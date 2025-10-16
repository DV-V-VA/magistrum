import json
import time
import asyncio
from typing import List, Dict, Any
from openai import OpenAI
from pydantic import ValidationError
from config import Config
from tools.support_tools import SUPPORT_TOOLS, AVAILABLE_TOOLS
from langchain.memory import ConversationBufferMemory
from langchain.schema import HumanMessage, AIMessage


class LLMAgent:
    def __init__(self) -> None:
        self.client = OpenAI(base_url=Config.NEBIUS_BASE_URL, api_key=Config.NEBIUS_API_KEY)
        self.model = Config.LLM_MODEL
        self.last_generation_time = 0

        self.memory = ConversationBufferMemory(return_messages=True)

        self.system_prompt = """\
You are Nebius's internal support AI assistant. Only answer questions specifically related to Nebius — our products, services, infrastructure, APIs, pricing/billing, support processes, SLAs, security/compliance, onboarding, and internal tools/docs. All responses must be in Slack message format (no generic Markdown).

Scope & out‑of‑scope policy:
• If the user's request is NOT about Nebius or clearly out of context, say you cannot help with that because it is outside Nebius support scope. Do NOT use web search in this case. Optionally, ask if they want to create a ticket.
• If the request IS about Nebius but lacks enough detail, ask for the missing details. If still unresolved, propose raising a Notion ticket or booking a call.

Tools you can use:
• notion_append_entry – add a ticket to our Notion database.
• cal_create_booking  – schedule a support call with Cal.com.
• web_search          – only for Nebius‑related questions when local docs/context are insufficient.

Rules:
1. Only call a tool when it directly addresses the user's request. Otherwise, answer from your own knowledge and retrieved Nebius docs.
2. First, call the necessary tool. After the tool result is available, provide a final Slack response mentioning the action taken.
3. If the user asks to book a call, directly book it using the tool (no web search required).
4. Web search policy for Nebius‑related queries only: call `web_search` ONLY when avg_vector_relevance < min_vector_relevance_threshold (default 0.70). If avg_vector_relevance >= threshold, rely on retrieved documents/knowledge and DO NOT call web_search.
   • Never use web search for out‑of‑scope, non‑Nebius questions — reply that you cannot help.
5. When you call `web_search`, pass `num_results` exactly equal to `web_search_limit` provided in telemetry.
6. If the user is unsatisfied with the answer, ask for more details. If still unsatisfied, offer to raise a Notion ticket. If still unsatisfied, offer to book a call.
7. Note you are polite too.
"""


    async def generate_response(
        self,
        query: str,
        context: List[Dict[str, Any]] | None = None,
        retrieved_docs: List[Dict[str, Any]] | None = None,
        avg_vector_relevance: float = 0.0,
        min_vector_relevance: float | None = None,
        web_search_limit: int | None = None,
        temperature: float = 0.7,
        max_tokens: int = 10000,
        user_email: str | None = None,
    ) -> Dict[str, Any]:

        # Build memory context messages
        memory_msgs = self.memory.load_memory_variables({})["history"]

        # Convert LangChain messages to OpenAI format correctly
        history_dicts = []
        for msg in memory_msgs:
            if isinstance(msg, HumanMessage):
                history_dicts.append({"role": "user", "content": msg.content})
            elif isinstance(msg, AIMessage):
                history_dicts.append({"role": "assistant", "content": msg.content})
            else:
                # Fallback for other message types
                msg_type = getattr(msg, 'type', 'user')
                role = "user" if msg_type == "human" else "assistant" if msg_type == "ai" else "user"
                history_dicts.append({"role": role, "content": msg.content})

        # Compose initial messages
        # Determine threshold
        relevance_threshold = (
            min_vector_relevance if min_vector_relevance is not None else Config.DEFAULT_MIN_VECTOR_RELEVANCE
        )

        # Tools will be exposed to the model; the system prompt instructs it
        # to avoid tools for out-of-scope (non-Nebius) queries and only use
        # web_search for Nebius queries when relevance is low.
        tools_schema = SUPPORT_TOOLS

        user_prompt = (
            f"Question: {query}\n\n"
            f"Context:\n{self._build_context(context, retrieved_docs)}\n\n"
            f"Telemetry: avg_vector_relevance={avg_vector_relevance:.3f}, min_vector_relevance_threshold={relevance_threshold:.3f}, web_search_limit={web_search_limit if web_search_limit is not None else 'auto'}\n\n"
            f"User Email: {user_email or 'Not provided.'}\n\n"
            "Provide the best help you can. If this question is not about Nebius, reply that it's out of scope and do not call any tool. If it is about Nebius and relevance is low, you may consider web_search per policy."
        )

        messages = [{"role": "system", "content": self.system_prompt}] + history_dicts + [
            {"role": "user", "content": user_prompt},
        ]

        start = time.time()
        tool_blocks: list[str] = []
        tools_used: list[str] = []
        search_results_count: int = 0
        web_sources: list[dict[str, str]] = []

        while True:
            chat = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                tools=tools_schema,
                tool_choice="auto",
            )

            assistant = chat.choices[0].message
            self.last_generation_time = time.time() - start

            if not assistant.tool_calls:  # Final answer
                # Append current user and assistant response to memory
                final_response = (assistant.content or "")
                self.memory.save_context({"input": query}, {"output": final_response})

                return {
                    "content": final_response,
                    "query": query,
                    "tool_calls_made": bool(tool_blocks),
                    "tools_used": tools_used,
                    "search_results_count": search_results_count,
                    "web_sources": web_sources,
                    "generation_time": self.last_generation_time,
                }

            # Append assistant's message to history (tool call request)
            messages.append({
                "role": "assistant",
                "content": assistant.content,
                "tool_calls": [
                    {
                        "id": call.id,
                        "type": "function",
                        "function": {
                            "name": call.function.name,
                            "arguments": call.function.arguments
                        }
                    }
                    for call in assistant.tool_calls
                ]
            })

            # Execute requested tool(s)
            tool_msgs = []
            for call in assistant.tool_calls:
                name = call.function.name
                args = json.loads(call.function.arguments or "{}")
                try:
                    tool = AVAILABLE_TOOLS[name]
                except KeyError:
                    err = f"Tool '{name}' not available."
                    tool_msgs.append({"role": "tool", "tool_call_id": call.id, "content": err})
                    tool_blocks.append(f"\n\n❌ {err}")
                    continue

                try:
                    if hasattr(tool, 'ainvoke') and asyncio.iscoroutinefunction(getattr(tool, 'ainvoke')):
                        result = await tool.ainvoke(args)
                    else:
                        result = await asyncio.to_thread(tool.invoke, args)
                    tool_msgs.append({"role": "tool", "tool_call_id": call.id, "content": json.dumps(result)})
                    tools_used.append(name)
                    # Aggregate web search result counts for UI analytics and collect sources
                    if name == "web_search":
                        try:
                            if isinstance(result, list):
                                search_results_count += len(result)
                                for r in result:
                                    title = (r.get("title") or "Source").strip()
                                    url = r.get("url") or ""
                                    if url:
                                        web_sources.append({"title": title, "url": url})
                            elif isinstance(result, dict):
                                # common shapes: {"results": [...]} or {"items": [...]}
                                if "results" in result and isinstance(result["results"], list):
                                    arr = result["results"]
                                    search_results_count += len(arr)
                                    for r in arr:
                                        title = (r.get("title") or "Source").strip()
                                        url = r.get("url") or ""
                                        if url:
                                            web_sources.append({"title": title, "url": url})
                                elif "items" in result and isinstance(result["items"], list):
                                    arr = result["items"]
                                    search_results_count += len(arr)
                                    for r in arr:
                                        title = (r.get("title") or "Source").strip()
                                        url = r.get("url") or ""
                                        if url:
                                            web_sources.append({"title": title, "url": url})
                        except Exception:
                            pass
                except ValidationError as e:
                    err_msg = f"Tool call failed due to missing arguments. Details: {e.errors()}. Please ask the user for the missing information and then try calling the tool again."
                    tool_msgs.append({"role": "tool", "tool_call_id": call.id, "content": err_msg})
                except Exception as exc:
                    err = f"{type(exc).__name__}: {exc}"
                    tool_msgs.append({"role": "tool", "tool_call_id": call.id, "content": err})

            # Append tool results to message history
            messages.extend(tool_msgs)

    def _build_context(self, search: List[Dict[str, Any]] | None, docs: List[Dict[str, Any]] | None) -> str:
        """Helper method to build context string from search results and documents."""
        if not search and not docs:
            return "No additional context."
        
        context_parts = []
        
        if search:
            context_parts.append("Search results:")
            for i, result in enumerate(search[:3], 1):
                title = result.get('title', 'Untitled')
                content = result.get('content', '')[:200] + "..." if len(result.get('content', '')) > 200 else result.get('content', '')
                context_parts.append(f"{i}. {title}: {content}")
        
        if docs:
            context_parts.append("\nRetrieved documents:")
            for i, doc in enumerate(docs[:3], 1):
                title = doc.get('title', doc.get('filename', 'Untitled'))
                content = doc.get('content', '')[:200] + "..." if len(doc.get('content', '')) > 200 else doc.get('content', '')
                context_parts.append(f"{i}. {title}: {content}")
        
        return "\n".join(context_parts) if context_parts else "No additional context."

    # Removed hardcoded Nebius keyword detector; scope is decided contextually by the LLM per system prompt.

    def clear_memory(self):
        """Clear conversation memory."""
        self.memory.clear()

    def get_memory_summary(self):
        """Get a summary of current conversation history."""
        try:
            memory_vars = self.memory.load_memory_variables({})
            return len(memory_vars.get("history", []))
        except Exception:
            return 0
