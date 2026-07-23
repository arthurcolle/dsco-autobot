"""
Jina AI reranker for hybrid retrieval.

Based on existing Jina implementation.
"""

import os
from typing import List, Dict, Any, Optional
import httpx


class JinaReranker:
    """
    Jina AI reranker client.

    Uses jina-reranker-v2-base-multilingual or jina-reranker-v1-base-en.
    """

    ENDPOINT = "https://api.jina.ai/v1/rerank"

    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Jina reranker.

        Args:
            api_key: Jina AI API key (or set JINA_API_KEY env var)
        """
        self.api_key = api_key or os.getenv("JINA_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Jina API key required. Get it free at https://jina.ai/?sui=apikey\n"
                "Set JINA_API_KEY env var or pass api_key parameter."
            )

    def rerank(
        self,
        query: str,
        documents: List[str],
        model: str = "jina-reranker-v2-base-multilingual",
        top_n: Optional[int] = None,
        return_documents: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Rerank documents by relevance to query.

        Args:
            query: Search query
            documents: List of document texts to rerank
            model: Reranker model name
            top_n: Return top N results (default: all)
            return_documents: Include document text in results

        Returns:
            List of dicts with keys: index, document (optional), relevance_score
        """
        if not documents:
            return []

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": model,
            "query": query,
            "documents": documents,
            "return_documents": return_documents
        }

        if top_n is not None:
            payload["top_n"] = top_n

        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                self.ENDPOINT,
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            result = response.json()

        return result.get("results", [])

    def rerank_with_metadata(
        self,
        query: str,
        items: List[Dict[str, Any]],
        text_key: str = "text",
        model: str = "jina-reranker-v2-base-multilingual",
        top_n: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Rerank items with metadata preservation.

        Args:
            query: Search query
            items: List of dicts with text and metadata
            text_key: Key in dict containing text to rerank
            model: Reranker model name
            top_n: Return top N results

        Returns:
            List of items with added 'relevance_score' field, sorted by score
        """
        if not items:
            return []

        # Extract texts
        documents = [item.get(text_key, "") for item in items]

        # Rerank
        results = self.rerank(
            query=query,
            documents=documents,
            model=model,
            top_n=top_n,
            return_documents=False
        )

        # Map back to original items
        scored_items = []
        for result in results:
            idx = result["index"]
            score = result["relevance_score"]
            item = items[idx].copy()
            item["relevance_score"] = score
            scored_items.append(item)

        return scored_items
