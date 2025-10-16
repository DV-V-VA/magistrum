"""
LangChain-style tools

• notion_append_entry – add a row to a fixed-schema Notion database  
• cal_create_booking – create a booking link in Calendly
"""
from __future__ import annotations

from datetime import datetime
import os, requests, asyncio
from typing import Dict, Any, List
from langchain_core.tools import tool
from agents.search_agent import SearchAgent
from config import Config

# ── environment ──────────────────────────────────────────
NOTION_KEY      = os.environ["NOTION_API_KEY"]
NOTION_DB_ID    = os.environ["NOTION_DATABASE_ID"]
NOTION_VERSION  = os.getenv("NOTION_VERSION", "2022-06-28")

# Calendly
CALENDLY_API_KEY = getattr(Config, "CALENDLY_API_KEY", None) or os.getenv("CALENDLY_API_KEY") or os.getenv("CAL_API_KEY")
CALENDLY_EVENT_TYPE_ID = getattr(Config, "CALENDLY_EVENT_TYPE_ID", None) or os.getenv("CALENDLY_EVENT_TYPE_ID")

HEADERS_NOTION = {
    "Authorization": f"Bearer {NOTION_KEY}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

# ── helpers ──────────────────────────────────────────────
# tools/notion_and_cal.py
FIXED_SCHEMA: Dict[str, Any] = {
    "Name":       {"type": "title"},

    # single-select
    "Status":     {"type": "select", "options": ["Open", "Closed", "In Progress"]},
    "Priority":   {"type": "select", "options": ["High", "Medium", "Low"]},
    "Assignee":   {"type": "select", "options": []},          # provide empty list

    # rich-text (note the tab in Description\t from CSV export)
    "Description\t": {"type": "rich_text"},

    # dates
    "Created time": {"type": "date"},
    "Due Date":     {"type": "date"},

    # multi-select
    "Tags": {"type": "multi_select", "options": ["AI-Studio-Requests", "Other"]},
}

def _format_props(data: Dict[str, Any]) -> Dict[str, Any]:
    props: Dict[str, Any] = {}

    for col, val in data.items():
        if col not in FIXED_SCHEMA or val in (None, "", []):
            continue                      # ← skip missing values

        schema = FIXED_SCHEMA[col]
        typ = schema["type"]

        if typ == "title":
            props[col] = {"title": [{"text": {"content": str(val)}}]}

        elif typ == "rich_text":
            props[col] = {"rich_text": [{"text": {"content": str(val)}}]}

        elif typ == "select":
            opts = schema.get("options", [])
            if not opts or val in opts:
                props[col] = {"select": {"name": str(val)}}

        elif typ == "multi_select":
            vals = val if isinstance(val, list) else [val]
            opts = schema.get("options", [])
            accepted = vals if not opts else [v for v in vals if v in opts]
            if accepted:
                props[col] = {"multi_select": [{"name": v} for v in accepted]}

        elif typ == "date":
            # value guaranteed not None here
            props[col] = {"date": {"start": str(val)}}

    return props




# ── Notion tool ──────────────────────────────────────────
@tool
def notion_append_entry(
    Name: str,
    Description_: str,          # renamed to avoid Python identifier issue
    Priority: str,
    Status: str = "Open",
    Assignee: str | None = None,
    Due_Date: str | None = None,
    Tags: List[str] = ["AI-Studio-Requests"],
) -> str:
    """
    Append a row to the Support-Tickets database.
    Dates must be ISO-8601 (YYYY-MM-DD or full timestamp).
    """
    # Map Python-friendly names back to Notion keys
    props = {
        "Name": Name,
        "Description\t": Description_,   # keep the tab again
        "Priority": Priority,
        "Status": Status,
        "Assignee": Assignee,
        "Due Date": Due_Date,
        "Tags": Tags,
        "Created time": datetime.now().isoformat(),  # if you want to set it manually
    }
    payload = {
        "parent": {"database_id": NOTION_DB_ID},
        "properties": _format_props(props),
    }

    try:
        r = requests.post("https://api.notion.com/v1/pages",
                          json=payload, headers=HEADERS_NOTION, timeout=15)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"Notion API error: {e.response.text}") from e
    return f"✅ Notion row created (page id: {r.json()['id']})"


PRESET_EVENT_TYPE_ID = "29031588-4d93-4889-b714-df826bc756d2"  # Replace with your actual Event Type ID

@tool
def cal_create_booking() -> str:
    """
    Return a scheduling link for the pre-configured Calendly event.
    Uses CALENDLY_EVENT_TYPE_ID from env/config, falls back to preset.
    """
    try:
        if not CALENDLY_API_KEY:
            raise RuntimeError("Missing CALENDLY_API_KEY")
        event_type_id = CALENDLY_EVENT_TYPE_ID or PRESET_EVENT_TYPE_ID
        r = requests.post(
            "https://api.calendly.com/scheduling_links",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {CALENDLY_API_KEY}"
            },
            json={
                "max_event_count": 1,
                "owner": f"https://api.calendly.com/event_types/{event_type_id}",
                "owner_type": "EventType"
            },
            timeout=15,
        )
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"Calendly API error: {e.response.text}") from e
    except Exception as e:
        raise RuntimeError(str(e))

    link = r.json()["resource"]["booking_url"]
    return f"📅 Here is your booking link: {link}"


# ── Web search tool (Exa) ─────────────────────────────────
@tool
async def web_search(query: str, num_results: int | None = None) -> List[Dict[str, Any]]:
    """
    Perform a web search using Exa and return a list of results.
    Each result may include fields like title, url, snippet/content.
    """
    agent = SearchAgent()
    limit = num_results if num_results is not None else Config.DEFAULT_WEB_RESULTS
    results = await agent.search_web(query=query, num_results=limit)
    return results


# utility so LLMAgent can call tools sync or async
async def run_tool(tool_fn, args):
    if asyncio.iscoroutinefunction(tool_fn):
        return await tool_fn(**args)
    return await asyncio.to_thread(tool_fn, **args)
