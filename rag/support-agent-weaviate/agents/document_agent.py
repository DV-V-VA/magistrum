import os
import tempfile
import uuid
from datetime import datetime
from typing import List, Dict, Any
from llama_index.core import SimpleDirectoryReader
from llama_index.core.node_parser import SentenceSplitter
from agents.embedding_agent import EmbeddingAgent

class DocumentAgent:
    def __init__(self):
        self.embedding_agent = EmbeddingAgent()
    
    def save_uploaded_files(self, uploaded_files: List[Dict], temp_dir: str) -> List[str]:
        """Save uploaded files to temporary directory"""
        input_files = []
        for file_data in uploaded_files:
            if not all(key in file_data for key in ['filename', 'content']):
                continue
                
            filename = file_data['filename']
            content = file_data['content']
            
            if len(content) == 0:
                print(f"⚠️ Skipping empty file: {filename}")
                continue
            
            file_path = os.path.join(temp_dir, filename)
            
            try:
                with open(file_path, "wb") as f:
                    f.write(content)
                input_files.append(file_path)
                print(f"💾 Saved file: {filename} ({len(content)} bytes)")
            except Exception as e:
                print(f"❌ Failed to save {filename}: {e}")
                continue
        
        return input_files
    
    def load_documents_with_llamaindex(self, input_files: List[str]) -> List:
        """Load documents using LlamaIndex"""
        try:
            documents = SimpleDirectoryReader(
                input_files=input_files,
                required_exts=[".txt", ".pdf", ".docx", ".md"]
            ).load_data(show_progress=False)
            
            if not documents:
                raise Exception("No documents were loaded - check file formats")
            
            # Add metadata
            batch_id = str(uuid.uuid4())
            for i, doc in enumerate(documents):
                doc.metadata.update({
                    "source": doc.metadata.get("file_name", f"Document_{i}"),
                    "ingestion_date": datetime.now().isoformat(),
                    "document_id": batch_id,
                    "batch_id": batch_id,
                    "file_index": i
                })
            
            print(f"📖 Loaded {len(documents)} documents with LlamaIndex")
            return documents
            
        except Exception as e:
            raise Exception(f"Error loading documents: {str(e)}")
    
    def create_chunks(self, documents: List, chunk_size: int, chunk_overlap: int) -> List:
        """Create chunks using SentenceSplitter"""
        try:
            parser = SentenceSplitter(
                chunk_size=chunk_size, 
                chunk_overlap=chunk_overlap,
                paragraph_separator="\n\n",
                secondary_chunking_regex="[.!?]+\\s+"
            )
            nodes = parser.get_nodes_from_documents(documents, show_progress=False)
            
            if not nodes:
                raise Exception("No chunks were created from documents")
            
            print(f"✂️ Created {len(nodes)} chunks")
            return nodes
            
        except Exception as e:
            raise Exception(f"Error chunking documents: {str(e)}")
    
    async def process_documents(
        self, 
        uploaded_files: List[Dict], 
        chunk_size: int = None, 
        chunk_overlap: int = None
    ) -> Dict[str, Any]:
        """Main processing function"""
        
        if chunk_size is None:
            chunk_size = Config.DEFAULT_CHUNK_SIZE
        if chunk_overlap is None:
            chunk_overlap = Config.DEFAULT_CHUNK_OVERLAP
            
        if not uploaded_files:
            raise Exception("No files provided for processing")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                # Step 1: Save files
                print("📥 Saving uploaded files...")
                input_files = self.save_uploaded_files(uploaded_files, temp_dir)
                
                if not input_files:
                    raise Exception("No valid files to process after validation")
                
                # Step 2: Load with LlamaIndex
                print("📖 Loading documents with LlamaIndex...")
                documents = self.load_documents_with_llamaindex(input_files)
                
                # Step 3: Create chunks
                print("✂️ Creating text chunks...")
                nodes = self.create_chunks(documents, chunk_size, chunk_overlap)
                
                # Step 4: Generate embeddings
                print("🧠 Generating embeddings...")
                contents = [node.text for node in nodes]
                embeddings = await self.embedding_agent.generate_embeddings_batch(contents)
                
                # Step 5: Prepare documents for storage
                prepared_docs = []
                for i, (node, embedding) in enumerate(zip(nodes, embeddings)):
                    source_file = node.metadata.get("source", "Unknown")
                    file_type = source_file.split('.')[-1].lower() if '.' in source_file else "unknown"
                    
                    doc_data = {
                        "content": node.text,
                        "source": source_file,
                        "document_id": node.metadata.get("document_id", str(uuid.uuid4())),
                        "chunk_index": i,
                        "file_type": file_type,
                        "total_chunks": len(nodes),
                        "ingestion_date": node.metadata.get("ingestion_date", datetime.now().isoformat()),
                        "batch_id": node.metadata.get("batch_id", "unknown"),
                        "chunk_size": len(node.text)
                    }
                    prepared_docs.append((doc_data, embedding))
                
                # Calculate statistics
                total_characters = sum(len(node.text) for node in nodes)
                avg_chunk_size = total_characters / len(nodes) if nodes else 0
                
                result = {
                    "success": True,
                    "prepared_documents": prepared_docs,
                    "total_documents": len(documents),
                    "total_chunks": len(nodes),
                    "total_characters": total_characters,
                    "average_chunk_size": avg_chunk_size,
                    "chunk_size_config": chunk_size,
                    "chunk_overlap_config": chunk_overlap,
                    "files_processed": len(input_files),
                    "processing_date": datetime.now().isoformat(),
                    "batch_id": documents[0].metadata.get("batch_id", "unknown") if documents else "unknown"
                }
                
                return result
                
            except Exception as e:
                raise Exception(f"Document processing error: {str(e)}")
