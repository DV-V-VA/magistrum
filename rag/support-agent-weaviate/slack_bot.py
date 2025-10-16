import os
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from dotenv import load_dotenv
from graph.workflow import RAGWorkflow
import logging
import asyncio
import re

load_dotenv()

# Initialize Slack app
app = App(
    token=os.environ.get("SLACK_BOT_TOKEN"),
    signing_secret=os.environ.get("SLACK_SIGNING_SECRET")
)

# Initialize RAG workflow
rag_workflow = RAGWorkflow()

# Get bot user ID for mention handling
try:
    bot_user_id = app.client.auth_test()["user_id"]
except Exception as e:
    print(f"Warning: Could not get bot user ID: {e}")
    bot_user_id = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Helpers ----------

def get_user_email(client, user_id: str) -> str:
    try:
        user_info = client.users_info(user=user_id)
        return user_info.get('user', {}).get('profile', {}).get('email', '') or ''
    except Exception:
        return ''

def format_slack_response(response: str) -> str:
    """
    Clean up markdown-like tokens, normalize code fences and keep text short.
    (Does NOT touch Slack-style angle-bracket links like <url|text>.)
    """
    if not response:
        return "No response generated."

    # Truncate to avoid Slack’s 4000-char hard limit
    if len(response) > 3500:
        response = response[:3500] + "... (truncated)"

    # Normalize fenced code blocks: ```python -> ```
    response = re.sub(r"```[a-zA-Z]*\n", "```\n", response)

    # Remove markdown headers (##, ###, etc.)
    response = re.sub(r"^#{1,6}\s*", "", response, flags=re.MULTILINE)

    # Remove bold/italic markers (*, _, **, __)
    # (we leave angle-bracket links intact)
    response = response.replace("*", "")
    response = response.replace("_", "")

    return response

def get_thread_history(client, channel: str, parent_ts: str) -> str:
    """Fetch all messages in a thread (or DM convo) and build a conversation history string."""
    try:
        replies = client.conversations_replies(channel=channel, ts=parent_ts)
        messages = replies.get("messages", [])
        history = []
        for msg in messages:
            user = msg.get("user", "unknown")
            text = msg.get("text", "")
            if "🧠 Thinking" in text:
                continue
            history.append(f"<@{user}>: {text}")
        return "\n".join(history)
    except Exception:
        logger.error("Error fetching thread history", exc_info=True)
        return ""

def post_thinking(client, channel: str, thread_ts: str | None) -> str:
    """Post a 'thinking' placeholder and return its ts (message to be updated)."""
    res = client.chat_postMessage(
        channel=channel,
        text="🧠 Thinking…",
        thread_ts=thread_ts,
        link_names=True
    )
    return res["ts"]

def update_message(client, channel: str, ts: str, final_text: str):
    client.chat_update(channel=channel, ts=ts, text=final_text)

def run_rag_sync(query: str, user_email: str):
    return asyncio.run(
        rag_workflow.run_workflow(
            query=query,
            uploaded_files=[],
            user_email=user_email,
            search_limit=5
        )
    )

def safe_run_rag(query: str, user_email: str) -> str:
    """
    Run the workflow, sanitize errors, and append formatted source links if available.

    Returns a single string ready to send to Slack.
    """
    try:
        result_state = run_rag_sync(query, user_email)

        # Extract response depending on what workflow returns
        if hasattr(result_state, 'final_response'):
            response = result_state.final_response
            stats = getattr(result_state, "stats", {}) or {}
        else:
            # result_state might be a dict
            response = result_state.get('final_response', '')
            stats = result_state.get('stats', {}) or {}

        # Sanitize known error patterns (workflow might return an error string)
        if (not response) or ("Response generation failed" in response) or ("Error code" in response) or ("does not exist" in response):
            logger.warning("Sanitized backend error returned to user (hidden).")
            return "⚠️ Unable to answer your question currently. I’ll be available soon."

        # Format main response
        final_text = format_slack_response(response)

        # Attach sources if available in stats
        web_sources = stats.get("web_sources") or []
        # Some workflows may include sources at top-level (legacy)
        if not web_sources and isinstance(result_state, dict):
            web_sources = result_state.get("web_sources") or []

        # Build a concise sources block (limit to 5)
        if web_sources:
            sources_lines = []
            for s in web_sources[:5]:
                # Accept different possible key names
                title = s.get("title") or s.get("name") or s.get("source") or s.get("snippet") or "Source"
                url = s.get("url") or s.get("link") or s.get("href") or ""
                # Slack link formatting: <url|title>
                if url:
                    # sanitize title to avoid newlines
                    title_clean = title.replace("\n", " ").strip()
                    sources_lines.append(f"• <{url}|{title_clean}>")
                else:
                    sources_lines.append(f"• {title}")

            if sources_lines:
                sources_text = "\n\n*Sources:*\n" + "\n".join(sources_lines)
                # Ensure message stays within limits
                if len(final_text) + len(sources_text) > 3800:
                    # shorten sources list if needed
                    truncated = "\n".join(sources_lines[:2])
                    sources_text = "\n\n*Sources:*\n" + truncated + "\n• ... (more)"
                final_text = final_text + sources_text

        return final_text

    except Exception:
        logger.error("RAG workflow crashed", exc_info=True)
        return "⚠️ Unable to answer your question currently. I’ll be available soon."


# ---------- Event: @mention ----------
@app.event("app_mention")
def handle_app_mention(event, say, client):
    try:
        channel = event["channel"]
        parent_ts = event.get("thread_ts") or event["ts"]  # thread-aware
        user_id = event["user"]
        text = event.get("text", "")

        if bot_user_id:
            text = text.replace(f"<@{bot_user_id}>", "").strip()

        if not text:
            thinking_ts = post_thinking(client, channel, parent_ts)
            update_message(client, channel, thinking_ts,
                           "Hello! Please ask me a question after mentioning me.")
            return

        user_email = get_user_email(client, user_id)
        thinking_ts = post_thinking(client, channel, parent_ts)

        # Thread-based conversation context
        thread_history = get_thread_history(client, channel, parent_ts)
        full_query = f"Conversation so far:\n{thread_history}\n\nLatest user message:\n{text}"

        final_text = safe_run_rag(full_query, user_email)
        update_message(client, channel, thinking_ts, final_text)

    except Exception:
        logger.error("Error processing mention", exc_info=True)
        try:
            update_message(client, event["channel"], thinking_ts,
                           "❌ Sorry, something went wrong. The issue has been logged.")
        except Exception:
            say(text="❌ Sorry, something went wrong. The issue has been logged.",
                thread_ts=event.get("ts"))

@app.message(".*")
def handle_message(message, say, client):
    try:
        if "bot_id" in message or "subtype" in message:
            return
        if message.get("channel_type") != "im":
            return

        channel = message["channel"]
        parent_ts = message.get("thread_ts") or message["ts"]  # thread-aware in DMs
        user_id = message["user"]
        text = (message.get("text") or "").strip()

        if not text:
            thinking_ts = post_thinking(client, channel, parent_ts)
            update_message(client, channel, thinking_ts, "Hello! Please ask me a question.")
            return

        user_email = get_user_email(client, user_id)
        thinking_ts = post_thinking(client, channel, parent_ts)

        # DM thread-based context
        thread_history = get_thread_history(client, channel, parent_ts)
        full_query = f"Conversation so far:\n{thread_history}\n\nLatest user message:\n{text}"

        final_text = safe_run_rag(full_query, user_email)
        update_message(client, channel, thinking_ts, final_text)

    except Exception:
        logger.error("Error processing message", exc_info=True)
        try:
            update_message(client, channel, thinking_ts,
                           "❌ Sorry, something went wrong. The issue has been logged.")
        except Exception:
            say(text="❌ Sorry, something went wrong. The issue has been logged.",
                thread_ts=message.get("ts"))

# ---------- Slash command: /rag ----------
@app.command("/rag")
def handle_rag_command(ack, respond, command):
    ack()
    try:
        text = (command.get("text") or "").strip()
        channel = command["channel_id"]
        user_id = command["user_id"]

        if not text:
            respond("Please provide a question after the /rag command. Example: `/rag What is machine learning?`")
            return

        user_email = get_user_email(app.client, user_id)
        res = app.client.chat_postMessage(channel=channel, text="🧠 Thinking…", link_names=True)
        thinking_ts = res["ts"]

        final_text = safe_run_rag(text, user_email)
        update_message(app.client, channel, thinking_ts, final_text)

    except Exception:
        logger.error("Error processing command", exc_info=True)
        try:
            update_message(app.client, channel, thinking_ts, "❌ Sorry, something went wrong. The issue has been logged.")
        except Exception:
            respond("❌ Sorry, something went wrong while processing your command. The issue has been logged.")

if __name__ == "__main__":
    try:
        print("🚀 Starting Slack RAG Bot...")
        print(f"Bot User ID: {bot_user_id}")

        handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
        print("✅ Bot is running and ready to receive messages!")
        print("\nBot capabilities:")
        print("• Direct messages: Send DM to the bot (threaded replies)")
        print("• Channel mentions: @botname your question (threaded replies)")
        print("• Slash command: /rag your question (single message updated)")
        print("\nPress Ctrl+C to stop the bot")

        handler.start()

    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        print(f"❌ Failed to start bot: {e}")
