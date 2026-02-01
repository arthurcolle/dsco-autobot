"""
Jina AI embedding provider with full feature support.

Supports:
- Text embeddings (jina-embeddings-v3/v4)
- Image embeddings (jina-clip-v2)
- Document reranking (jina-reranker-v2)
- Zero-shot classification
- Web search and grounded generation

Get your Jina AI API key for free: https://jina.ai/?sui=apikey
"""

import os
from typing import List, Optional, Union, Dict, Any
import httpx


class JinaProvider:
    """
    Comprehensive Jina AI provider for embeddings, reranking, and classification.
    """

    def __init__(self, api_key: Optional[str] = None, base_url: str = "https://api.jina.ai/v1"):
        """
        Initialize Jina provider.

        Args:
            api_key: Jina AI API key (or set JINA_API_KEY env var)
            base_url: API base URL
        """
        self.api_key = api_key or os.getenv("JINA_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Jina API key required. Get it free at https://jina.ai/?sui=apikey\n"
                "Set JINA_API_KEY env var or pass api_key parameter."
            )
        self.base_url = base_url.rstrip("/")
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create async HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=60.0)
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    async def embed_text(
        self,
        texts: Union[str, List[str]],
        model: str = "jina-embeddings-v3",
        dimensions: Optional[int] = None,
        normalized: bool = True,
        task: Optional[str] = None
    ) -> List[List[float]]:
        """
        Generate text embeddings.

        Args:
            texts: Single text or list of texts
            model: Model name (jina-embeddings-v3 or v4)
            dimensions: Output dimensions (v4: 2048/1024/512/256, v3: 1024/512/256)
            normalized: L2 normalize vectors for cosine similarity
            task: Task hint (retrieval.query, retrieval.passage, classification, etc.)

        Returns:
            List of embedding vectors
        """
        if isinstance(texts, str):
            texts = [texts]

        if not texts:
            return []

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload: Dict[str, Any] = {
            "model": model,
            "input": texts,
            "normalized": normalized,
        }

        if dimensions:
            payload["dimensions"] = dimensions
        if task:
            payload["task"] = task

        client = await self._get_client()
        response = await client.post(
            f"{self.base_url}/embeddings",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        result = response.json()

        data = result.get("data", [])
        if not data:
            raise ValueError("Jina API returned empty data")

        return [item["embedding"] for item in data]

    async def embed_images(
        self,
        images: Union[str, List[str]],
        model: str = "jina-clip-v2",
        dimensions: Optional[int] = None,
        normalized: bool = True
    ) -> List[List[float]]:
        """
        Generate image embeddings.

        Args:
            images: Single image or list of images (base64 strings or URLs)
            model: Model name (default: jina-clip-v2)
            dimensions: Output dimensions
            normalized: L2 normalize vectors

        Returns:
            List of embedding vectors
        """
        if isinstance(images, str):
            images = [images]

        if not images:
            return []

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload: Dict[str, Any] = {
            "model": model,
            "input": images,
            "normalized": normalized,
        }

        if dimensions:
            payload["dimensions"] = dimensions

        client = await self._get_client()
        response = await client.post(
            f"{self.base_url}/embeddings",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        result = response.json()

        data = result.get("data", [])
        if not data:
            raise ValueError("Jina API returned empty data")

        return [item["embedding"] for item in data]

    async def rerank(
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
            documents: List of document texts
            model: Reranker model name
            top_n: Return top N results
            return_documents: Include document text in results

        Returns:
            List of dicts with index, document (optional), relevance_score
        """
        if not documents:
            return []

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload: Dict[str, Any] = {
            "model": model,
            "query": query,
            "documents": documents,
            "return_documents": return_documents
        }

        if top_n is not None:
            payload["top_n"] = top_n

        client = await self._get_client()
        response = await client.post(
            f"{self.base_url}/rerank",
            headers=headers,
            json=payload
        )
        response.raise_for_status()
        result = response.json()

        return result.get("results", [])

    async def classify(
        self,
        input_data: Union[str, List[str]],
        labels: List[str],
        model: str = "jina-embeddings-v3"
    ) -> List[Dict[str, Any]]:
        """
        Zero-shot classification using embedding similarity.

        Args:
            input_data: Single text or list of texts to classify
            labels: List of candidate labels
            model: Model to use for classification

        Returns:
            List of classification results with label scores
        """
        if isinstance(input_data, str):
            input_data = [input_data]

        if not input_data or not labels:
            return []

        # Embed both inputs and labels, then compute similarities
        input_embeddings = await self.embed_text(input_data, model=model, task="classification")
        label_embeddings = await self.embed_text(labels, model=model, task="classification")

        results = []
        for input_emb in input_embeddings:
            scores = []
            for i, label_emb in enumerate(label_embeddings):
                # Cosine similarity (vectors are already normalized)
                score = sum(a * b for a, b in zip(input_emb, label_emb))
                scores.append({"label": labels[i], "score": score})

            # Sort by score descending
            scores.sort(key=lambda x: x["score"], reverse=True)
            results.append({"predictions": scores})

        return results

    async def search_web(
        self,
        query: str,
        num_results: int = 10,
        include_content: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Search the web using Jina's reader/search API.

        Args:
            query: Search query
            num_results: Number of results
            include_content: Include page content

        Returns:
            List of search results with title, url, content
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }

        client = await self._get_client()
        response = await client.get(
            f"https://s.jina.ai/{query}",
            headers=headers,
            params={"num": num_results}
        )
        response.raise_for_status()

        return response.json().get("results", [])

    async def read_url(
        self,
        url: str,
        include_images: bool = False
    ) -> Dict[str, Any]:
        """
        Read and parse a URL using Jina's reader API.

        Args:
            url: URL to read
            include_images: Include images in response

        Returns:
            Parsed content with title, content, etc.
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }

        if include_images:
            headers["X-With-Images-Summary"] = "true"

        client = await self._get_client()
        response = await client.get(
            f"https://r.jina.ai/{url}",
            headers=headers
        )
        response.raise_for_status()

        return response.json()

    # Legacy sync methods for compatibility
    def embed(self, text: str, model: str = "jina-embeddings-v3", dim: int = 1024) -> List[float]:
        """Synchronous embed (legacy)."""
        import asyncio
        return asyncio.run(self.embed_text([text], model=model, dimensions=dim))[0]

    def embed_batch(self, texts: List[str], model: str = "jina-embeddings-v3", dim: int = 1024) -> List[List[float]]:
        """Synchronous batch embed (legacy)."""
        import asyncio
        return asyncio.run(self.embed_text(texts, model=model, dimensions=dim))


# Convenience singleton
_jina: Optional[JinaProvider] = None


def get_jina() -> JinaProvider:
    """Get or create a Jina provider instance."""
    global _jina
    if _jina is None:
        _jina = JinaProvider()
    return _jina
