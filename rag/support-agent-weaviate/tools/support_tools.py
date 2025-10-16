from langchain_core.utils.function_calling import convert_to_openai_function
from .tools_notion_and_cal import notion_append_entry, cal_create_booking, web_search

AVAILABLE_TOOLS = {
    "notion_append_entry": notion_append_entry,
    "cal_create_booking":  cal_create_booking,
    "web_search":         web_search,
}

SUPPORT_TOOLS = [
    {"type": "function", "function": convert_to_openai_function(t)}
    for t in AVAILABLE_TOOLS.values()
]
