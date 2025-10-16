from typing import TypedDict, List, Dict, Any, Optional
from datetime import datetime

# This IS subscriptable and works with your existing graph nodes.
class WorkflowState(TypedDict):
    # Input
    query: str
    uploaded_files: List[Dict[str, Any]]
    user_email: Optional[str]
    
    # Processing options
    chunk_size: int
    chunk_overlap: int
    search_limit: int
    web_search_limit: int
    doc_retrieval_limit: int
    
    # Intermediate results
    search_results: List[Dict[str, Any]]
    retrieved_docs: List[Dict[str, Any]]
    processed_docs: Dict[str, Any]
    embeddings_generated: bool
    
    # Final output
    final_response: str
    
    # Metadata
    workflow_id: str
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    error_message: str
    
    # Statistics
    stats: Dict[str, Any]
    
    # Relevance gating
    min_vector_relevance: float
    avg_vector_relevance: float