import streamlit as st
import asyncio
from dotenv import load_dotenv
from typing import Dict, Any
from config import Config
from graph.workflow import RAGWorkflow
from services.vector_service import VectorService
from PIL import Image
import base64
from pathlib import Path

load_dotenv()

def validate_environment() -> bool:
    """Validate environment variables"""
    status = Config.validate_config()
    required = ["nebius_api_key", "exa_api_key", "weaviate_url", "weaviate_api_key"]
    missing = [k for k, v in status.items() if not v and k in required]
    
    if missing:
        st.error(f"🚨 Missing required environment variables: {', '.join(missing)}")
        with st.expander("📋 Configuration Guide", expanded=True):
            st.markdown("**Please add these to your `.env` file:**")
            st.code("""
NEBIUS_API_KEY=your_nebius_api_key
EXA_API_KEY=your_exa_api_key
WEAVIATE_URL=your_weaviate_url
WEAVIATE_API_KEY=your_weaviate_api_key

# Optional for support features
NOTION_API_KEY=your_notion_api_key
NOTION_DATABASE_ID=your_notion_database_id
CALENDLY_API_KEY=your_calendly_api_key
CALENDLY_EVENT_TYPE_ID=your_calendly_event_type_id
            """)
        return False
    return True

def render_retrieval_settings_sidebar():
    """Render retrieval settings early so values affect the current run"""
    with st.sidebar:
        # Branding at top of sidebar (from assets folder only)
        logo = None
        try:
            logo = Image.open("assets/logo.png")
        except Exception:
            logo = None
        if logo is not None:
            try:
                w, h = logo.size
                # display_w = max(48, int(w * 0.8))
                st.image(logo, width=150)
            except Exception:
                # Fallback render without size calc
                st.image(logo)
        st.markdown("# ⚙️ Control Panel")
        with st.expander("🔎 Retrieval Settings", expanded=True):
            st.markdown("Set limits for web results and vector document retrieval.")
            col_r1, col_r2 = st.columns(2)
            with col_r1:
                _web_limit = st.slider(
                    "Web results",
                    min_value=1,
                    max_value=10,
                    value=st.session_state.web_search_limit,
                    help="Number of web search results to fetch per query."
                )
            with col_r2:
                _docs_limit = st.slider(
                    "Vector docs",
                    min_value=1,
                    max_value=20,
                    value=st.session_state.doc_retrieval_limit,
                    help="Maximum number of documents retrieved from the vector DB."
                )
            # Relevance gating threshold
            _min_rel = st.slider(
                "Vector relevance threshold",
                min_value=0.0,
                max_value=1.0,
                value=float(st.session_state.min_vector_relevance),
                step=0.01,
                help="If avg vector relevance is below this threshold, the model may call web_search."
            )
            if _web_limit != st.session_state.web_search_limit:
                st.session_state.web_search_limit = _web_limit
            if _docs_limit != st.session_state.doc_retrieval_limit:
                st.session_state.doc_retrieval_limit = _docs_limit
            if _min_rel != st.session_state.min_vector_relevance:
                st.session_state.min_vector_relevance = _min_rel

def display_message_with_stats(message: Dict[str, Any], message_index: int):
    """Display a message with enhanced statistics and support information"""
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Enhanced stats display for assistant messages
        if message["role"] == "assistant" and "stats" in message:
            with st.expander("📊 Response Analytics", expanded=False):
                stats = message["stats"]
                
                # Main metrics in a clean grid
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric(
                        "🔍 Search Results", 
                        stats.get("search_results_count", 0),
                        help="Number of web search results found"
                    )
                with col2:
                    st.metric(
                        "📚 Documents", 
                        stats.get("retrieved_docs_count", 0),
                        help="Vector database documents retrieved"
                    )
                with col3:
                    st.metric(
                        "⚡ Generation Time", 
                        f"{stats.get('generation_time', 0):.2f}s",
                        help="Time taken to generate response"
                    )
                with col4:
                    tools_used = stats.get("tools_used", [])
                    st.metric(
                        "🛠️ Tools Used", 
                        len(tools_used),
                        help="Number of AI tools automatically triggered"
                    )
                
                # Tool details if any were used
                if tools_used:
                    st.markdown("**🔧 Automated Tools:**")
                    for tool in tools_used:
                        st.badge(tool)
                
                # Show effective limits used for this run
                limit_col1, limit_col2 = st.columns(2)
                with limit_col1:
                    st.metric(
                        "🌐 Web limit",
                        stats.get("web_search_limit", "-"),
                        help="Web results requested in this run"
                    )
                with limit_col2:
                    st.metric(
                        "📚 Docs limit",
                        stats.get("doc_retrieval_limit", "-"),
                        help="Vector documents requested in this run"
                    )

                # Relevance telemetry
                rel_col1, rel_col2 = st.columns(2)
                with rel_col1:
                    st.metric(
                        "📏 Avg vector relevance",
                        f"{stats.get('avg_vector_relevance', 0.0):.2f}",
                        help="Average relevance computed from vector DB (1 - distance)"
                    )
                with rel_col2:
                    st.metric(
                        "🎚️ Relevance threshold",
                        f"{stats.get('min_vector_relevance', 0.0):.2f}",
                        help="Threshold below which the model may call web_search"
                    )

                # Optional sources panel (only on demand)
                web_sources = stats.get("web_sources") or []
                if web_sources:
                    with st.expander("🔗 Show sources", expanded=False):
                        for s in web_sources:
                            title = s.get("title") or "Source"
                            url = s.get("url") or ""
                            if url:
                                st.markdown(f"- [{title}]({url})")
        
        # Support notification with better styling
        if message["role"] == "assistant" and message.get("tool_calls_made"):
            st.info("🤖 **Smart Support Active** - AI automatically assessed your query and provided additional assistance options above.")

def main():
    # Enhanced page configuration
    st.set_page_config(
        page_title="Internal Support Agent with LangGraph, Nebius & Weaviate",
        page_icon="🧠",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Fixed theme CSS: force identical visuals for light & dark modes
    st.markdown("""
    <style>
    /* Unified variables (force same palette for both themes) */
    :root {
        --bg-primary: #0e1117;
        --bg-secondary: #1a1d23;
        --bg-tertiary: #262730;
        --text-primary: #ffffff;
        --text-secondary: #e0e0e0;
        --text-muted: #b0b0b0;
        --accent-primary: #ff4b4b;
        --accent-secondary: #00cc88;
        --accent-blue: #0066cc;
        --accent-purple: #8b5fbf;
        --border-color: #3d4043;
        --shadow: rgba(0,0,0,0.3);
    }
    /* Streamlit sets data-theme; keep both identical */
    html[data-theme="dark"], body[data-theme="dark"],
    html[data-theme="light"], body[data-theme="light"] {
        --bg-primary: #0e1117;
        --bg-secondary: #1a1d23;
        --bg-tertiary: #262730;
        --text-primary: #ffffff;
        --text-secondary: #e0e0e0;
        --text-muted: #b0b0b0;
        --accent-primary: #ff4b4b;
        --accent-secondary: #00cc88;
        --accent-blue: #0066cc;
        --accent-purple: #8b5fbf;
        --border-color: #3d4043;
        --shadow: rgba(0,0,0,0.3);
        background-color: var(--bg-primary);
    }
    html, body { background: var(--bg-primary) !important; }
    [data-testid="stAppViewContainer"],
    [data-testid="stMain"] {
        background: var(--bg-primary) !important;
    }

    /* Override Streamlit's base using our variables */
    .stApp {
        background-color: var(--bg-primary);
        color: var(--text-primary);
    }
    /* Top header bar fix (Streamlit header) */
    [data-testid="stHeader"], .stAppHeader {
        background: var(--bg-primary) !important;
        color: var(--text-primary) !important;
        border-bottom: none !important;
    }
    [data-testid="stHeader"] * , .stAppHeader * { color: var(--text-primary) !important; }
    /* Streamlit top decoration/status bars */
    [data-testid="stDecoration"],
    [data-testid="stToolbar"],
    [data-testid="stStatusWidget"] {
        background: var(--bg-primary) !important;
        color: var(--text-primary) !important;
        border-bottom: none !important;
    }
    
    /* Specific overrides for different text elements */
    .stMarkdown, .stMarkdown *, 
    .stText, .stText *,
    p, span, div, h1, h2, h3, h4, h5, h6 {
        color: var(--text-primary) !important;
    }
    
    /* Main container styling */
    .block-container {
        max-width: none;
        padding-top: 1rem;
        padding-left: 2rem;
        padding-right: 2rem;
        padding-bottom: 6rem;
        background-color: var(--bg-primary);
    }
    
    /* Header styling - plain white text, keep emoji native colors */
    .main-header {
        color: var(--text-primary) !important;
        font-size: 2.5rem;
        font-weight: 800;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    
    .subtitle {
        text-align: center;
        color: var(--text-secondary) !important;
        font-size: 1.1rem;
        margin-bottom: 2rem;
        padding: 1rem;
        background: var(--bg-secondary);
        border-radius: 10px;
        border-left: none;
        box-shadow: 0 2px 8px var(--shadow);
    }
    
    /* Chat input styling - CENTERED TEXT */
    .stChatInput > div > div > input {
        background-color: var(--bg-secondary) !important;
        color: var(--text-primary) !important;
        border: 0 !important; /* remove inner input border to avoid double box */
        border-radius: 12px !important;
        text-align: center !important;
        font-size: 1rem !important;
        padding: 12px 20px !important;
        outline: none !important;
    }
    /* Chat input container background + wrapper (fix white bar) */
    [data-testid="stBottomBlockContainer"],
    [data-testid="stChatInput"],
    .stChatInput {
        background: var(--bg-primary) !important;
        border-top: none !important;
    }
    .stChatInput > div,
    .stChatInput > div > div,
    .stChatInput > div > div > div {
        background: var(--bg-secondary) !important;
        border: none !important; /* remove wrapper borders */
        border-radius: 12px !important;
        box-shadow: 0 2px 6px var(--shadow) !important;
    }
    /* Ensure no nested white backgrounds inside chat input */
    .stChatInput * {
        background-color: transparent;
    }
    .stChatInput button {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-color) !important;
    }
    /* New chat textarea + send button (BaseWeb wrappers) */
    [data-testid="stChatInputTextArea"],
    [data-baseweb="textarea"] {
        background: var(--bg-secondary) !important;
        color: var(--text-primary) !important;
        border: 0 !important; /* no border on textarea itself */
        border-radius: 12px !important;
        box-shadow: none !important;
        outline: none !important;
    }
    [data-baseweb="base-input"] {
        background: var(--bg-secondary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-color) !important; /* single visible border container */
        border-radius: 12px !important;
        box-shadow: none !important;
    }
    [data-testid="stChatInputTextArea"]::placeholder { color: var(--text-muted) !important; }
    [data-testid="stChatInputSubmitButton"] {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-color) !important;
        box-shadow: 0 1px 3px var(--shadow) !important;
    }
    [data-testid="stChatInputSubmitButton"][disabled] {
        opacity: 0.6 !important;
        background: var(--bg-tertiary) !important;
    }
    [data-testid="InputInstructions"] { color: var(--text-muted) !important; }
    
    .stChatInput > div > div > input:focus {
        border-color: var(--accent-secondary) !important;
        box-shadow: 0 0 0 2px rgba(0,204,136,0.2) !important;
        text-align: center !important;
        color: var(--text-primary) !important;
    }
    
    .stChatInput > div > div > input::placeholder {
        color: var(--text-muted) !important;
        text-align: center !important;
    }
    .stChatInput > div > div > input::-webkit-input-placeholder { color: var(--text-muted) !important; }
    .stChatInput > div > div > input::-moz-placeholder { color: var(--text-muted) !important; }
    .stChatInput > div > div > input:-ms-input-placeholder { color: var(--text-muted) !important; }
    .stChatInput > div > div > input::-ms-input-placeholder { color: var(--text-muted) !important; }
    /* Prevent white autofill background */
    .stChatInput input:-webkit-autofill,
    .stChatInput input:-webkit-autofill:hover,
    .stChatInput input:-webkit-autofill:focus {
        -webkit-text-fill-color: var(--text-primary) !important;
        -webkit-box-shadow: 0 0 0px 1000px var(--bg-secondary) inset !important;
        transition: background-color 5000s ease-in-out 0s !important;
        caret-color: var(--text-primary) !important;
    }
    
    /* Chat messages styling */
    .stChatMessage {
        background-color: var(--bg-secondary);
        border-radius: 10px;
        border: 1px solid var(--border-color);
        box-shadow: 0 2px 8px var(--shadow);
        margin-bottom: 1rem;
        color: var(--text-primary) !important;
    }
    
    .stChatMessage * {
        color: var(--text-primary) !important;
    }
    
    /* Sidebar styling - FIXED TEXT VISIBILITY */
    .stSidebar {
        background-color: var(--bg-secondary);
    }
    
    .stSidebar * {
        color: var(--text-primary) !important;
    }
    
    .stSidebar .stMarkdown,
    .stSidebar .stMarkdown *,
    .stSidebar h1, .stSidebar h2, .stSidebar h3,
    .stSidebar p, .stSidebar span, .stSidebar div {
        color: var(--text-primary) !important;
    }
    
    .sidebar-section {
        background: var(--bg-tertiary);
        padding: 1rem;
        border-radius: 10px;
        margin-bottom: 1rem;
        border-left: 4px solid var(--accent-secondary);
        box-shadow: 0 2px 8px var(--shadow);
        color: var(--text-primary) !important;
    }
    
    .sidebar-section * {
        color: var(--text-primary) !important;
    }
    
    .sidebar-header {
        font-weight: 700;
        color: var(--text-primary) !important;
        margin-bottom: 0.5rem;
        font-size: 1.1rem;
    }
    
    /* Metric styling for dark theme - FIXED TEXT */
    div[data-testid="metric-container"] {
        background: var(--bg-tertiary);
        border: 1px solid var(--border-color);
        padding: 1rem;
        border-radius: 8px;
        box-shadow: 0 2px 6px var(--shadow);
    }
    
    div[data-testid="metric-container"] * {
        color: var(--text-primary) !important;
    }
    
    div[data-testid="metric-container"] [data-testid="metric-label"] {
        color: var(--text-secondary) !important;
    }
    
    div[data-testid="metric-container"] [data-testid="metric-value"] {
        color: var(--text-primary) !important;
        font-weight: 600;
    }
    
    /* Button styling for dark theme */
    .stButton button {
        background: linear-gradient(135deg, var(--accent-secondary), var(--accent-blue));
        color: white !important;
        border-radius: 8px;
        border: none;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: 0 2px 6px var(--shadow);
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 12px var(--shadow);
        background: linear-gradient(135deg, var(--accent-blue), var(--accent-purple));
        color: white !important;
    }
    
    .stButton button[kind="secondary"] {
        background: var(--bg-tertiary);
        border: 2px solid var(--border-color);
        color: var(--text-primary) !important;
    }
    
    .stButton button[kind="secondary"]:hover {
        background: var(--bg-secondary);
        border-color: var(--accent-secondary);
        color: var(--text-primary) !important;
    }
    
    /* Status indicators for dark theme */
    .status-connected { 
        color: var(--accent-secondary) !important; 
        font-weight: 600; 
        background: rgba(0,204,136,0.15);
        padding: 4px 12px;
        border-radius: 6px;
        border: 1px solid rgba(0,204,136,0.3);
    }
    .status-disconnected { 
        color: var(--accent-primary) !important; 
        font-weight: 600; 
        background: rgba(255,75,75,0.15);
        padding: 4px 12px;
        border-radius: 6px;
        border: 1px solid rgba(255,75,75,0.3);
    }
    .status-warning { 
        color: #ffc107 !important; 
        font-weight: 600; 
        background: rgba(255,193,7,0.15);
        padding: 4px 12px;
        border-radius: 6px;
        border: 1px solid rgba(255,193,7,0.3);
    }
    
    /* Expandable sections for dark theme - FIXED TEXT */
    div[data-testid="stExpander"] {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: 10px;
        box-shadow: 0 2px 6px var(--shadow);
        margin-bottom: 1rem;
    }
    
    div[data-testid="stExpander"] * {
        color: var(--text-primary) !important;
    }
    
    div[data-testid="stExpander"] div[role="button"] {
        background: var(--bg-secondary) !important;
        color: var(--text-primary) !important;
        font-weight: 600;
        padding: 1rem;
        border-radius: 10px 10px 0 0;
        border-bottom: 1px solid var(--border-color) !important;
    }
    
    div[data-testid="stExpander"] div[role="button"]:hover {
        background: linear-gradient(90deg, var(--accent-secondary), var(--accent-blue));
        color: white !important;
    }
    
    /* File uploader styling for dark theme */
    .uploadedFile {
        background: var(--bg-secondary);
        border-radius: 8px;
        border: 2px dashed var(--accent-secondary);
        color: var(--text-primary) !important;
    }
    
    .uploadedFile * {
        color: var(--text-primary) !important;
    }
    
    /* Alert styling for dark theme - FIXED TEXT */
    .stAlert {
        border-radius: 8px;
        border-left: 0;
        background: var(--bg-secondary);
        color: var(--text-primary) !important;
    }
    
    .stAlert * {
        color: var(--text-primary) !important;
    }
    
    .stAlert > div {
        background: transparent;
        color: var(--text-primary) !important;
    }
    
    .stSuccess { 
        border-left-color: transparent; 
        background: rgba(0,204,136,0.1);
    }
    .stError { 
        border-left-color: transparent; 
        background: rgba(255,75,75,0.1);
    }
    .stWarning { 
        border-left-color: transparent; 
        background: rgba(255,193,7,0.1);
    }
    .stInfo { 
        border-left-color: transparent; 
        background: rgba(0,102,204,0.1);
    }
    
    /* Input fields styling - FIXED TEXT */
    .stTextInput input, .stSelectbox select, .stSelectbox > div > div {
        background-color: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        border: 2px solid var(--border-color) !important;
        border-radius: 8px !important;
    }
    
    .stTextInput input:focus, .stSelectbox select:focus {
        border-color: var(--accent-secondary) !important;
        box-shadow: 0 0 0 2px rgba(0,204,136,0.2) !important;
        color: var(--text-primary) !important;
    }
    
    .stTextInput label, .stSelectbox label {
        color: var(--text-primary) !important;
        font-weight: 600;
    }
    
    /* Selectbox dropdown styling */
    .stSelectbox > div > div > div {
        background-color: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-color) !important;
    }
    
    .stSelectbox > div > div > div > div {
        color: var(--text-primary) !important;
    }
    
    /* Code blocks */
    .stCodeBlock {
        background: var(--bg-tertiary);
        border: 1px solid var(--border-color);
        border-radius: 8px;
    }
    .stCodeBlock * { color: var(--text-primary) !important; }
    [data-testid="stCode"] {
        background: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 8px !important;
    }
    [data-testid="stCode"] pre,
    [data-testid="stCode"] code { background: transparent !important; color: var(--text-primary) !important; }
    [data-testid="stCodeCopyButton"] {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-color) !important;
        box-shadow: 0 1px 3px var(--shadow) !important;
    }
    code {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        padding: 2px 4px;
        border-radius: 4px;
        border: 1px solid var(--border-color);
    }
    
    /* Spinner for dark theme */
    .stSpinner {
        color: var(--accent-secondary);
    }
    
    /* File uploader */
    .stFileUploader * { color: var(--text-primary) !important; }
    .stFileUploader label { color: var(--text-primary) !important; font-weight: 600; }
    [data-testid="stFileUploaderDropzone"] {
        background: var(--bg-secondary) !important;
        border: 2px dashed var(--accent-secondary) !important;
        border-radius: 10px !important;
        box-shadow: 0 2px 6px var(--shadow) !important;
    }
    [data-testid="stFileUploaderDropzoneInstructions"] { background: transparent !important; }
    [data-testid="stBaseButton-secondary"] { 
        background: var(--bg-tertiary) !important; 
        color: var(--text-primary) !important; 
        border: 1px solid var(--border-color) !important; 
    }
    
    /* Help text */
    .stHelp {
        color: var(--text-muted) !important;
    }
    
    /* Caption text */
    .stCaption {
        color: var(--text-muted) !important;
    }
    
    /* Footer styling */
    .footer-text {
        text-align: center; 
        color: var(--text-primary) !important;
        font-size: 0.9rem;
        background: var(--bg-secondary);
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid var(--border-color);
    }
    
    /* Badge styling */
    .stBadge {
        background: var(--bg-tertiary) !important;
        color: var(--text-primary) !important;
        border: 1px solid var(--border-color);
    }
    
    /* Ensure all nested elements are visible */
    .stSidebar .stContainer * {
        color: var(--text-primary) !important;
    }
    
    /* Form elements */
    .stForm {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 1rem;
    }
    
    .stForm * {
        color: var(--text-primary) !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Prepare inline header logos from assets via base64 so they always load
    def _inline_img(filename: str, alt: str) -> str:
        try:
            p = Path(__file__).parent / "assets" / filename
            ext = (p.suffix or ".png").lstrip(".")
            with open(p, "rb") as f:
                data = base64.b64encode(f.read()).decode("utf-8")
            return f'<img src="data:image/{ext};base64,{data}" alt="{alt}" style="height:1.2em; vertical-align:middle; margin-left:4px; margin-right:6px;" />'
        except Exception:
            return alt

    langgraph_tag = _inline_img("langgraph_logo.png", "LangGraph")
    notion_tag = _inline_img("notion.png", "Notion")
    weaviate_tag = _inline_img("weavite.png", "Weaviate")

    # Enhanced header (LangGraph & Weaviate inline images, Nebius as text)
    st.markdown(
        f'<h1 class="main-header">Internal Support Agent with LangGraph {langgraph_tag}, Notion {notion_tag} & Weaviate {weaviate_tag}</h1>',
        unsafe_allow_html=True
    )
    st.markdown(
        '<div class="subtitle">🚀 Internal Support Agent with Web Search • 📄 Document Processing • 🔍 Vector Retrieval • 🤖 Intelligent Support</div>', 
        unsafe_allow_html=True
    )

    
    # Environment validation with better UX
    if not validate_environment():
        st.stop()
    
    # Initialize components with loading animation
    if 'workflow' not in st.session_state:
        with st.spinner("🔄 Initializing RAG workflow with AI support agents..."):
            try:
                st.session_state.workflow = RAGWorkflow()
                st.success("✅ System initialized successfully!")
            except Exception as e:
                st.error(f"❌ Initialization failed: {str(e)}")
                st.stop()
    
    # Initialize session state
    if 'messages' not in st.session_state:
        st.session_state.messages = []
    if 'user_email' not in st.session_state:
        st.session_state.user_email = ""
    # Retrieval settings (RAG): defaults from config
    if 'web_search_limit' not in st.session_state:
        st.session_state.web_search_limit = getattr(Config, 'DEFAULT_WEB_RESULTS', getattr(Config, 'DEFAULT_SEARCH_RESULTS', 5))
    if 'doc_retrieval_limit' not in st.session_state:
        st.session_state.doc_retrieval_limit = getattr(Config, 'DEFAULT_DOCS_RETRIEVAL', getattr(Config, 'DEFAULT_SEARCH_RESULTS', 5))
    # Relevance threshold default
    if 'min_vector_relevance' not in st.session_state:
        st.session_state.min_vector_relevance = getattr(Config, 'DEFAULT_MIN_VECTOR_RELEVANCE', 0.7)
    
    # Render retrieval settings early so current run uses updated values
    render_retrieval_settings_sidebar()

    # Main chat interface
    st.markdown("## 💬 Chat Interface")
    
    # Welcome message for new users
    if not st.session_state.messages:
        st.info("👋 **Welcome!** Ask me anything, upload documents, or request support. I'll automatically assess and provide the best assistance.")
    
    # Display chat messages with enhanced styling
    for i, message in enumerate(st.session_state.messages):
        display_message_with_stats(message, i)
    
    # Chat input with enhanced UX and centered text
    if prompt := st.chat_input("💭 Ask me anything... (I'll automatically provide support when needed)"):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("🧠 Processing your request with intelligent support assessment..."):
                try:
                    result_state = asyncio.run(
                        st.session_state.workflow.run_workflow(
                            query=prompt,
                            uploaded_files=[],
                            user_email=st.session_state.user_email,
                            web_search_limit=st.session_state.web_search_limit,
                            doc_retrieval_limit=st.session_state.doc_retrieval_limit,
                            min_vector_relevance=st.session_state.min_vector_relevance,
                            run_reason="chat",
                        )
                    )
                    
                    # Extract and display response
                    if hasattr(result_state, 'final_response'):
                        response = result_state.final_response
                        stats = getattr(result_state, 'stats', {})
                    else:
                        response = result_state.get('final_response', 'No response generated.')
                        stats = result_state.get('stats', {})
                    
                    st.markdown(response)
                    
                    # Add to chat history
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response,
                        "stats": stats,
                        "query": prompt,
                        "tool_calls_made": stats.get("tool_calls_made", False),
                        "tools_used": stats.get("tools_used", [])
                    })
                    
                except Exception as e:
                    error_msg = f"❌ **System Error:** {str(e)}"
                    st.error(error_msg)
                    
                    # Auto-create support ticket for errors
                    if st.session_state.user_email:
                        try:
                            support_result = asyncio.run(
                                st.session_state.workflow.llm_agent.handle_support_request(
                                    user_request="System error occurred, need assistance",
                                    original_query=prompt,
                                    ai_response=error_msg,
                                    user_email=st.session_state.user_email
                                )
                            )
                            error_msg += f"\n\n🎫 **Auto-Support Activated:** {support_result}"
                        except:
                            pass
                    
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": error_msg,
                        "is_error": True
                    })

    # Enhanced Sidebar with FIXED TEXT VISIBILITY (other sections)
    with st.sidebar:
        # Retrieval controls are rendered earlier; keep other tools below
        # User Profile removed per request
        
        # Document Processing Section
        with st.expander("📄 Document Processing", expanded=False):
            st.markdown("**Upload & Process Documents**")
            
            uploaded_files = st.file_uploader(
                "Choose files to add to knowledge base",
                type=['txt', 'pdf', 'docx', 'md'],
                accept_multiple_files=True,
                help="Supported formats: TXT, PDF, DOCX, MD"
            )
            
            col1, col2 = st.columns(2)
            with col1:
                chunk_size = st.selectbox("📏 Chunk Size", [500, 1000, 1500, 2000], index=1)
            with col2:
                chunk_overlap = st.selectbox("🔄 Overlap", [100, 200, 300, 400], index=1)
            
            if st.button("🚀 Process Documents", type="primary", disabled=not uploaded_files, use_container_width=True):
                with st.spinner("⚙️ Processing documents..."):
                    try:
                        file_data = []
                        for file in uploaded_files:
                            file_data.append({"filename": file.name, "content": file.read()})
                        
                        result_state = asyncio.run(
                            st.session_state.workflow.run_workflow(
                                query="Process uploaded documents",
                                uploaded_files=file_data,
                                chunk_size=chunk_size,
                                chunk_overlap=chunk_overlap,
                                user_email=st.session_state.user_email,
                                web_search_limit=st.session_state.web_search_limit,
                                doc_retrieval_limit=st.session_state.doc_retrieval_limit,
                                min_vector_relevance=st.session_state.min_vector_relevance,
                                run_reason="ingestion",
                            )
                        )
                        
                        if hasattr(result_state, 'error_message') and result_state.error_message:
                            st.error(f"❌ Processing failed: {result_state.error_message}")
                        else:
                            st.success("✅ Documents processed successfully!")
                            if hasattr(result_state, 'processed_docs'):
                                processed_info = result_state.processed_docs
                                col_a, col_b = st.columns(2)
                                with col_a:
                                    st.metric("📄 Documents", processed_info.get("total_documents", 0))
                                    st.metric("📏 Avg Size", f"{processed_info.get('average_chunk_size', 0):.0f}")
                                with col_b:
                                    st.metric("🧩 Chunks", processed_info.get("total_chunks", 0))
                                    st.metric("📁 Files", processed_info.get("files_processed", 0))
                                    
                    except Exception as e:
                        st.error(f"❌ Processing error: {str(e)}")
                        if st.session_state.user_email:
                            with st.spinner("Creating support ticket..."):
                                try:
                                    support_result = asyncio.run(
                                        st.session_state.workflow.llm_agent.handle_support_request(
                                            user_request="Document processing failed",
                                            original_query="Document upload",
                                            ai_response=f"Error: {str(e)}",
                                            user_email=st.session_state.user_email,
                                        )
                                    )
                                    st.info(f"🎫 Support ticket created: {support_result}")
                                except:
                                    pass
        
        # System Status Section
        with st.expander("🔧 System Status", expanded=False):
            st.markdown("**🤖 AI Components**")
            st.code(f"""
LLM Model: {Config.LLM_MODEL}
Embedding: {Config.EMBEDDING_MODEL}
Search: Exa.ai
Vector DB: Weaviate
Support: Always Active
            """)
            
            st.markdown("**🛠️ Integration Status**")
            notion_status = "connected" if Config.NOTION_API_KEY else "disconnected"
            calendly_status = "connected" if getattr(Config, 'CALENDLY_API_KEY', None) else "disconnected"
            
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f'**Notion:** <span class="status-{notion_status}">{"✅" if notion_status == "connected" else "❌"}</span>', unsafe_allow_html=True)
            with col2:
                st.markdown(f'**Calendly:** <span class="status-{calendly_status}">{"✅" if calendly_status == "connected" else "❌"}</span>', unsafe_allow_html=True)
        
        # Database Management
        with st.expander("💾 Database", expanded=False):
            if st.button("📊 Get Stats", use_container_width=True):
                with st.spinner("Fetching database statistics..."):
                    try:
                        vector_service = VectorService()
                        stats = vector_service.get_stats()
                        if "error" not in stats:
                            st.metric("📚 Total Documents", stats.get("total_documents", 0))
                            st.success("✅ Database healthy")
                        else:
                            st.error(f"❌ Error: {stats['error']}")
                    except Exception as e:
                        st.error(f"❌ Connection failed: {str(e)}")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🗑️ Clear DB", type="secondary", use_container_width=True):
                    with st.spinner("Clearing database..."):
                        try:
                            VectorService().wipe_collection()
                            st.success("✅ Database cleared")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
            with col2:
                if st.button("💬 Clear Chat", type="secondary", use_container_width=True):
                    st.session_state.messages = []
                    st.success("✅ Chat cleared")
                    st.rerun()
        
        # Chat Analytics
        if st.session_state.messages:
            with st.expander("📈 Session Analytics", expanded=False):
                assistant_messages = [m for m in st.session_state.messages if m.get("role") == "assistant"]
                tool_calls = sum(1 for m in assistant_messages if m.get("tool_calls_made", False))
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("💬 Responses", len(assistant_messages))
                with col2:
                    st.metric("🤖 Auto-Support", tool_calls)
                
                if tool_calls > 0:
                    st.success(f"🎯 AI provided automatic support {tool_calls} time(s)")
                
                # Recent tools used
                recent_tools = []
                for msg in reversed(assistant_messages[-3:]):  # Last 3 responses
                    recent_tools.extend(msg.get("tools_used", []))
                
                if recent_tools:
                    st.markdown("**Recent Tools:**")
                    for tool in set(recent_tools):
                        st.badge(tool)

                # Add sample document only on explicit user action
                sample_content = """
# LangGraph RAG System Documentation

## Overview
This intelligent RAG system combines advanced document processing with automatic support assessment.

## Key Features
- **Real-time Web Search**: Powered by Exa.ai
- **Document Processing**: Multi-format support
- **Vector Storage**: Efficient Weaviate integration  
- **Smart Support**: Automatic quality assessment
- **Tool Integration**: Seamless Notion and Cal.com

## Usage
1. Upload documents for processing
2. Ask questions naturally
3. Receive intelligent responses
4. Get automatic support when needed

The system continuously learns and improves support delivery.
                """

                if st.button("➕ Add sample document", use_container_width=True):
                    with st.spinner("Adding sample document..."):
                        try:
                            sample_files = [{
                                "filename": "system_documentation.md",
                                "content": sample_content.encode('utf-8')
                            }]
                            asyncio.run(
                                st.session_state.workflow.run_workflow(
                                    query="Process system documentation",
                                    uploaded_files=sample_files,
                                    user_email=st.session_state.user_email,
                                    min_vector_relevance=st.session_state.min_vector_relevance,
                                    web_search_limit=st.session_state.web_search_limit,
                                    doc_retrieval_limit=st.session_state.doc_retrieval_limit,
                                    run_reason="sample_ingestion",
                                )
                            )
                            st.success("✅ Sample document added!")
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
            
            # Manual support request
            if st.session_state.user_email:
                if st.button("🆘 Request Support", type="primary", use_container_width=True):
                    with st.spinner("Creating support request..."):
                        try:
                            support_result = asyncio.run(
                                st.session_state.workflow.llm_agent.handle_support_request(
                                    user_request="Manual support request from user",
                                    original_query="Direct support request",
                                    ai_response="User requested immediate assistance",
                                    user_email=st.session_state.user_email
                                )
                            )
                            st.success("🎫 Support request created!")
                            st.markdown(support_result)
                        except Exception as e:
                            st.error(f"❌ Error: {str(e)}")
        
        # Footer
        # Horizontal rule removed to avoid visible line
        st.markdown(
            '<div class="footer-text">🧠 LangGraph RAG System<br>Powered by AI</div>', 
            unsafe_allow_html=True
        )

if __name__ == "__main__":
    main()
