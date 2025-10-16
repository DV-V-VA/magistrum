import httpx
import time
from typing import Dict, Any, List
from config import Config

class MonitoringAgent:
    def __init__(self):
        self.api_key = Config.KEYWORDS_AI_API_KEY
        self.base_url = "https://api.keywordsai.co/api/request-logs/create/"
        self.enabled = bool(self.api_key)
    
    async def log_request(
        self, 
        query: str, 
        response: str, 
        model_used: str, 
        generation_time: float = None,
        context_sources: List[str] = None,
        **kwargs
    ) -> bool:
        """Log request to Keywords AI for monitoring"""
        if not self.enabled:
            return False
            
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        prompt_messages = [
            {"role": "system", "content": "You are a helpful RAG assistant."},
            {"role": "user", "content": query}
        ]
        
        if context_sources:
            system_content = f"You are a helpful RAG assistant with access to: {', '.join(context_sources)}"
            prompt_messages[0]["content"] = system_content
        
        completion_message = {
            "role": "assistant",
            "content": response
        }
        
        payload = {
            "model": model_used,
            "prompt_messages": prompt_messages,
            "completion_message": completion_message,
            "generation_time": generation_time or 1.0,
            "prompt_tokens": len(query.split()),
            "completion_tokens": len(response.split()),
            "total_tokens": len(query.split()) + len(response.split())
        }
        
        try:
            async with httpx.AsyncClient() as client:
                response_obj = await client.post(
                    self.base_url,
                    headers=headers,
                    json=payload,
                    timeout=30.0
                )
                response_obj.raise_for_status()
                return True
        except Exception as e:
            print(f"Monitoring logging failed: {e}")
            return False
