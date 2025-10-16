import weaviate
from weaviate.auth import AuthApiKey
from weaviate.classes import config, query
from weaviate.classes.data import DataObject
from typing import List, Dict, Any
from config import Config

class VectorService:
    def __init__(self):
        self.client = weaviate.connect_to_weaviate_cloud(
            cluster_url=Config.WEAVIATE_URL,
            auth_credentials=AuthApiKey(Config.WEAVIATE_API_KEY)
        )
        self.collection_name = Config.WEAVIATE_CLASS_NAME
        self._ensure_collection()
    
    def _ensure_collection(self):
        """Create the schema if it doesn't exist"""
        if self.client.collections.exists(self.collection_name):
            return
        
        self.client.collections.create(
            name=self.collection_name,
            description="Document chunks for RAG workflow",
            vectorizer_config=config.Configure.Vectorizer.none(),
            properties=[
                # FIX: Use keyword arguments 'name' and 'data_type'
                config.Property(name="content", data_type=config.DataType.TEXT),
                config.Property(name="source", data_type=config.DataType.TEXT),
                config.Property(name="document_id", data_type=config.DataType.TEXT),
                config.Property(name="chunk_index", data_type=config.DataType.INT),
                config.Property(name="file_type", data_type=config.DataType.TEXT),
                config.Property(name="total_chunks", data_type=config.DataType.INT),
                config.Property(name="ingestion_date", data_type=config.DataType.TEXT),
                config.Property(name="batch_id", data_type=config.DataType.TEXT),
                config.Property(name="chunk_size", data_type=config.DataType.INT),
            ],
        )
        print(f"✅ Created Weaviate collection: {self.collection_name}")
    
    async def store_documents(self, documents: List[Dict[str, Any]], embeddings: List[List[float]]):
        """Store documents with embeddings"""
        collection = self.client.collections.get(self.collection_name)
        
        data_objects = [
            DataObject(properties=doc, vector=embedding)
            for doc, embedding in zip(documents, embeddings)
        ]
        
        collection.data.insert_many(data_objects)
        print(f"✅ Stored {len(data_objects)} documents in Weaviate")
    
    async def similarity_search(self, query_embedding: List[float], limit: int = 5) -> List[Dict[str, Any]]:
        """Search for similar documents"""
        collection = self.client.collections.get(self.collection_name)
        
        try:
            # Ensure limit is an int and add debug logging
            try:
                requested = int(limit)
            except Exception:
                requested = 5
            print(f"📚 Weaviate near_vector: requested limit={requested}")

            response = collection.query.near_vector(
                near_vector=query_embedding,
                limit=requested,
                return_metadata=query.MetadataQuery(distance=True),
                return_properties=["content", "source", "document_id", "chunk_index", "file_type"]
            )
            
            objs = response.objects or []
            results = [
                {**obj.properties, "distance": obj.metadata.distance}
                for obj in objs
            ]
            print(f"📚 Weaviate near_vector: returned {len(results)} objects (requested {requested})")
            return results
        except Exception as e:
            print(f"Search error: {e}")
            return []
    
    def wipe_collection(self):
        """Delete all documents in collection"""
        if self.client.collections.exists(self.collection_name):
            self.client.collections.delete(self.collection_name)
        self._ensure_collection()
        print(f"✅ Wiped collection: {self.collection_name}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get collection statistics"""
        try:
            collection = self.client.collections.get(self.collection_name)
            response = collection.aggregate.over_all(total_count=True)
            return {
                "status": "healthy",
                "total_documents": response.total_count,
                "collection_exists": True
            }
        except Exception as e:
            return {"error": str(e), "collection_exists": False}
