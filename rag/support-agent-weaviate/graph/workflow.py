import uuid
from datetime import datetime
from typing import Dict, Any, List
from langgraph.graph import StateGraph, END
from graph.state import WorkflowState
from agents.search_agent import SearchAgent
from agents.embedding_agent import EmbeddingAgent
from agents.llm_agent import LLMAgent
from agents.document_agent import DocumentAgent
from agents.monitoring_agent import MonitoringAgent
from services.vector_service import VectorService

class RAGWorkflow:
    def __init__(self):
        self.search_agent = SearchAgent()
        self.embedding_agent = EmbeddingAgent()
        self.llm_agent = LLMAgent()
        self.document_agent = DocumentAgent()
        self.monitoring_agent = MonitoringAgent()
        self.vector_service = VectorService()
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph workflow"""
        # Create StateGraph with the state schema
        graph = StateGraph(WorkflowState)
        
        # Add nodes (search_web defined but not wired; LLMAgent will call web_search tool directly)
        graph.add_node("process_documents", self._process_documents_node)
        graph.add_node("retrieve_docs", self._retrieve_docs_node)
        graph.add_node("generate_response", self._generate_response_node)
        graph.add_node("monitor_request", self._monitor_request_node)

        # Always process documents (it's a no-op if none), then retrieve
        graph.set_entry_point("process_documents")
        graph.add_edge("process_documents", "retrieve_docs")

        # After retrieval, always go to generate_response. Web search is now a tool called by LLMAgent.
        graph.add_edge("retrieve_docs", "generate_response")
        
        # Continue to monitoring
        graph.add_edge("generate_response", "monitor_request")
        graph.add_edge("monitor_request", END)
        
        return graph.compile()
    
    async def _process_documents_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Process uploaded documents"""
        uploaded_files = state.get("uploaded_files", [])
        if not uploaded_files:
            return {"processed_docs": {}}
        
        print(f"📄 Processing {len(uploaded_files)} uploaded files...")
        try:
            result = await self.document_agent.process_documents(
                uploaded_files,
                state.get('chunk_size', 1000),
                state.get('chunk_overlap', 200)
            )
            
            # Store documents if processing was successful
            if result.get("success") and result.get("prepared_documents"):
                docs, embeddings = zip(*result["prepared_documents"])
                await self.vector_service.store_documents(list(docs), list(embeddings))
            
            return {
                "processed_docs": result, 
                "embeddings_generated": True
            }
            
        except Exception as e:
            print(f"❌ Document processing failed: {e}")
            return {
                "processed_docs": {},
                "error_message": str(e)
            }

    async def _retrieve_docs_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve relevant documents from vector database"""
        query = state.get("query", "")
        # Prefer granular limit; fall back to legacy search_limit
        search_limit = state.get("doc_retrieval_limit", state.get("search_limit", 5))
        
        print("🔎 Retrieving relevant documents from vector database...")
        print(f"   ↳ doc_retrieval_limit (node): {search_limit}")
        try:
            # Generate query embedding
            query_embeddings = await self.embedding_agent.generate_embeddings([query])
            if query_embeddings:
                query_embedding = query_embeddings[0]
                
                # Search for similar documents
                retrieved_docs = await self.vector_service.similarity_search(
                    query_embedding, 
                    limit=search_limit
                )
                # Compute average relevance from distances if available
                relevances = []
                for d in retrieved_docs:
                    dist = d.get("distance")
                    if dist is not None:
                        try:
                            rel = max(0.0, 1.0 - float(dist))
                        except Exception:
                            rel = 0.0
                        relevances.append(rel)
                avg_rel = sum(relevances) / len(relevances) if relevances else 0.0
                threshold = state.get("min_vector_relevance")
                if threshold is None:
                    from config import Config
                    threshold = Config.DEFAULT_MIN_VECTOR_RELEVANCE
                need_web = avg_rel < threshold
                print(f"📏 Vector relevance: avg={avg_rel:.3f}, threshold={threshold:.3f} -> need_web_search={need_web}")

                return {
                    "retrieved_docs": retrieved_docs,
                    "avg_vector_relevance": avg_rel,
                }
            else:
                print("❌ Failed to generate query embedding")
                # If we cannot embed, leave docs empty and avg relevance 0.0
                return {"retrieved_docs": [], "avg_vector_relevance": 0.0}
                
        except Exception as e:
            print(f"❌ Document retrieval failed: {e}")
            return {"retrieved_docs": [], "avg_vector_relevance": 0.0}
    async def _generate_response_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Generate final response with automatic tool access"""
        query = state.get("query", "")
        search_results = state.get("search_results", [])
        retrieved_docs = state.get("retrieved_docs", [])
        avg_vector_relevance = state.get("avg_vector_relevance", 0.0)
        min_vector_relevance = state.get("min_vector_relevance")
        web_search_limit = state.get("web_search_limit", state.get("search_limit", 2))
        user_email = state.get("user_email")  # Add user email to state
        
        print("🤖 Generating response with automatic support tools...")
        try:
            response_data = await self.llm_agent.generate_response(
                query=query,
                context=search_results,
                retrieved_docs=retrieved_docs,
                avg_vector_relevance=avg_vector_relevance,
                min_vector_relevance=min_vector_relevance,
                web_search_limit=web_search_limit,
                user_email=user_email
            )
            
            # Extract response content  
            final_response = response_data.get("content", "No response generated")
            
            # Calculate statistics including tool usage
            stats = {
                "search_results_count": response_data.get("search_results_count", 0),
                "retrieved_docs_count": len(retrieved_docs),
                "generation_time": response_data.get("generation_time", 0),
                "tool_calls_made": response_data.get("tool_calls_made", False),
                "tools_used": response_data.get("tools_used", []),
                "web_sources": response_data.get("web_sources", []),
                "total_processing_time": 0,
                # Expose effective limits used in this run
                "web_search_limit": state.get("web_search_limit", state.get("search_limit", 2)),
                "doc_retrieval_limit": state.get("doc_retrieval_limit", state.get("search_limit", 5)),
                # Relevance gating telemetry
                "avg_vector_relevance": state.get("avg_vector_relevance", 0.0),
                "min_vector_relevance": state.get("min_vector_relevance", 0.0),
                # Run metadata
                "run_reason": state.get("run_reason", "chat"),
            }
            
            # Calculate total processing time
            start_time = state.get('start_time')
            if start_time:
                if isinstance(start_time, str):
                    try:
                        start_time = datetime.fromisoformat(start_time)
                    except:
                        start_time = datetime.now()
                stats["total_processing_time"] = (datetime.now() - start_time).total_seconds()
            
            return {
                "final_response": final_response,
                "end_time": datetime.now().isoformat(),
                "stats": stats
            }
            
        except Exception as e:
            error_msg = f"Response generation failed: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                "final_response": error_msg,
                "error_message": str(e)
            }

    async def _monitor_request_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Log request for monitoring"""
        # Skip monitoring if there's an error
        if state.get('error_message'):
            return {}
        
        # Skip if no final response
        final_response = state.get('final_response')
        if not final_response:
            return {}
            
        try:
            # Determine context sources
            context_sources = []
            tools_used = state.get('stats', {}).get('tools_used', [])
            if isinstance(tools_used, list) and ('web_search' in tools_used):
                context_sources.append("web_search")
            if state.get('retrieved_docs'):
                context_sources.append("vector_database")
            if state.get('processed_docs', {}).get('success'):
                context_sources.append("uploaded_documents")
            
            # Log the request
            await self.monitoring_agent.log_request(
                query=state.get('query', ''),
                response=final_response,
                model_used=self.llm_agent.model,
                generation_time=state.get('stats', {}).get("generation_time", 0),
                context_sources=context_sources
            )
            
        except Exception as e:
            print(f"❌ Monitoring failed: {e}")
        
        return {}

    async def run_workflow(self, query: str, uploaded_files: List[Dict[str, Any]] = None, **options) -> Dict[str, Any]:
        """Run the complete RAG workflow"""
        # Prepare initial state as dictionary
        run_reason = options.get("run_reason", "chat")
        initial_state = {
            "query": query,
            "uploaded_files": uploaded_files or [],
            "user_email": options.get("user_email"),
            "workflow_id": str(uuid.uuid4()),
            "start_time": datetime.now(),
            "run_reason": run_reason,
            # Keep legacy search_limit but also set granular controls
            "search_limit": options.get("search_limit", 5),
            "web_search_limit": options.get("web_search_limit", options.get("search_limit", 2)),
            "doc_retrieval_limit": options.get("doc_retrieval_limit", options.get("search_limit", 5)),
            "chunk_size": options.get("chunk_size", 1000),
            "chunk_overlap": options.get("chunk_overlap", 200),
            # Relevance gating defaults
            "min_vector_relevance": options.get("min_vector_relevance"),
            "avg_vector_relevance": 0.0,
            # Initialize other fields
            "search_results": [],
            "retrieved_docs": [],
            "processed_docs": {},
            "final_response": "",
            "error_message": "",
            "embeddings_generated": False,
            "stats": {}
        }
        print(
            f"🚦 Workflow start (reason={run_reason}): web_search_limit={initial_state['web_search_limit']}, "
            f"doc_retrieval_limit={initial_state['doc_retrieval_limit']} (legacy search_limit={initial_state['search_limit']})"
        )
        
        try:
            # Execute the graph
            final_state = await self.graph.ainvoke(initial_state)
            return final_state
            
        except Exception as e:
            error_msg = f"Workflow error: {str(e)}"
            print(f"❌ {error_msg}")
            
            # Return state with error information
            initial_state.update({
                "error_message": str(e),
                "final_response": error_msg,
                "end_time": datetime.now()
            })
            return initial_state
