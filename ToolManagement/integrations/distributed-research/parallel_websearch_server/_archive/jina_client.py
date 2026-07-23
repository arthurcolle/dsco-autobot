"""Jina AI client for text processing: segment, classify, rerank, embed."""

import os
import logging
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum

import httpx

logger = logging.getLogger(__name__)

JINA_API_KEY = os.environ.get("JINA_API_KEY", "")
JINA_BASE_URL = "https://api.jina.ai/v1"


class JinaModel(str, Enum):
    """Available Jina models."""
    # Embeddings
    EMBED_V4 = "jina-embeddings-v4"
    EMBED_V3 = "jina-embeddings-v3"
    EMBED_V2_BASE = "jina-embeddings-v2-base-en"
    # Reranker
    RERANKER_V2 = "jina-reranker-v2-base-multilingual"
    RERANKER_V1 = "jina-reranker-v1-base-en"
    # Classifier
    CLASSIFIER_V1 = "jina-embeddings-v2-base-en"  # Used with classifier endpoint
    # Segmenter
    SEGMENTER = "jina-segmenter-api"


@dataclass
class SegmentResult:
    """Result from text segmentation."""
    chunks: List[str]
    chunk_positions: List[Dict[str, int]]
    num_tokens: int


@dataclass
class ClassifyResult:
    """Result from text classification."""
    text: str
    labels: List[Dict[str, Any]]  # [{label, score}]


@dataclass
class RerankResult:
    """Result from reranking."""
    index: int
    document: str
    relevance_score: float


@dataclass
class EmbedResult:
    """Result from embedding."""
    embedding: List[float]
    tokens_used: int


class JinaClient:
    """Client for Jina AI APIs."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: float = 60.0,
    ):
        self.api_key = api_key or JINA_API_KEY
        self.base_url = base_url or JINA_BASE_URL
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    # =========================================================================
    # Segment API - Split text into semantic chunks
    # =========================================================================

    async def segment(
        self,
        content: str,
        *,
        tokenizer: str = "cl100k_base",
        max_chunk_length: int = 1000,
        return_tokens: bool = False,
        return_chunks: bool = True,
    ) -> SegmentResult:
        """Segment text into semantic chunks.

        Args:
            content: Text to segment
            tokenizer: Tokenizer to use (cl100k_base, o200k_base, p50k_base)
            max_chunk_length: Maximum tokens per chunk
            return_tokens: Include token counts
            return_chunks: Include chunk text

        Returns:
            SegmentResult with chunks and positions
        """
        client = await self._get_client()

        response = await client.post(
            "/segmenter",
            json={
                "content": content,
                "tokenizer": tokenizer,
                "max_chunk_length": max_chunk_length,
                "return_tokens": return_tokens,
                "return_chunks": return_chunks,
            },
        )
        response.raise_for_status()
        data = response.json()

        chunks = data.get("chunks", [])
        positions = data.get("chunk_positions", [])
        num_tokens = data.get("num_tokens", 0)

        return SegmentResult(
            chunks=chunks,
            chunk_positions=positions,
            num_tokens=num_tokens,
        )

    # =========================================================================
    # Classify API - Zero-shot text classification
    # =========================================================================

    async def classify(
        self,
        texts: Union[str, List[str]],
        labels: List[str],
        *,
        model: str = JinaModel.CLASSIFIER_V1,
    ) -> List[ClassifyResult]:
        """Classify text into provided labels (zero-shot).

        Args:
            texts: Text or list of texts to classify
            labels: List of possible labels
            model: Model to use

        Returns:
            List of ClassifyResult with label scores
        """
        client = await self._get_client()

        if isinstance(texts, str):
            texts = [texts]

        response = await client.post(
            "/classify",
            json={
                "model": model,
                "input": texts,
                "labels": labels,
            },
        )
        response.raise_for_status()
        data = response.json()

        results = []
        for item in data.get("data", []):
            results.append(ClassifyResult(
                text=item.get("text", ""),
                labels=item.get("prediction", []),
            ))

        return results

    # =========================================================================
    # Rerank API - Rerank documents by relevance to query
    # =========================================================================

    async def rerank(
        self,
        query: str,
        documents: List[str],
        *,
        model: str = JinaModel.RERANKER_V2,
        top_n: Optional[int] = None,
        return_documents: bool = True,
    ) -> List[RerankResult]:
        """Rerank documents by relevance to query.

        Args:
            query: Query text
            documents: List of documents to rerank
            model: Reranker model
            top_n: Return only top N results
            return_documents: Include document text in results

        Returns:
            List of RerankResult sorted by relevance
        """
        client = await self._get_client()

        payload = {
            "model": model,
            "query": query,
            "documents": documents,
            "return_documents": return_documents,
        }
        if top_n:
            payload["top_n"] = top_n

        response = await client.post("/rerank", json=payload)
        response.raise_for_status()
        data = response.json()

        results = []
        for item in data.get("results", []):
            results.append(RerankResult(
                index=item.get("index", 0),
                document=item.get("document", {}).get("text", "") if return_documents else "",
                relevance_score=item.get("relevance_score", 0.0),
            ))

        return results

    # =========================================================================
    # Embed API - Generate embeddings
    # =========================================================================

    async def embed(
        self,
        texts: Union[str, List[str]],
        *,
        model: str = JinaModel.EMBED_V4,
        task: str = "retrieval.passage",
        dimensions: Optional[int] = None,
        late_chunking: bool = False,
    ) -> List[EmbedResult]:
        """Generate embeddings for texts.

        Args:
            texts: Text or list of texts to embed
            model: Embedding model
            task: Task type (retrieval.query, retrieval.passage, separation, classification, text-matching)
            dimensions: Output dimensions (for v3 model)
            late_chunking: Enable late chunking for long texts

        Returns:
            List of EmbedResult with embeddings
        """
        client = await self._get_client()

        if isinstance(texts, str):
            texts = [texts]

        payload = {
            "model": model,
            "input": texts,
            "task": task,
            "late_chunking": late_chunking,
        }
        if dimensions:
            payload["dimensions"] = dimensions

        response = await client.post("/embeddings", json=payload)
        response.raise_for_status()
        data = response.json()

        results = []
        usage = data.get("usage", {})
        tokens_used = usage.get("total_tokens", 0)

        for item in data.get("data", []):
            results.append(EmbedResult(
                embedding=item.get("embedding", []),
                tokens_used=tokens_used // len(texts) if texts else 0,
            ))

        return results

    # =========================================================================
    # Reader API - Extract content from URLs
    # =========================================================================

    async def read_url(
        self,
        url: str,
        *,
        return_format: str = "markdown",
        target_selector: Optional[str] = None,
        wait_for_selector: Optional[str] = None,
        timeout_ms: int = 30000,
    ) -> Dict[str, Any]:
        """Read and extract content from a URL.

        Args:
            url: URL to read
            return_format: Output format (markdown, html, text, screenshot)
            target_selector: CSS selector to extract specific content
            wait_for_selector: Wait for element before extracting
            timeout_ms: Page load timeout

        Returns:
            Dict with title, content, url, and metadata
        """
        client = await self._get_client()

        # Reader API uses a different endpoint pattern
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "X-Return-Format": return_format,
        }
        if target_selector:
            headers["X-Target-Selector"] = target_selector
        if wait_for_selector:
            headers["X-Wait-For-Selector"] = wait_for_selector

        response = await client.get(
            f"https://r.jina.ai/{url}",
            headers=headers,
            timeout=timeout_ms / 1000,
        )
        response.raise_for_status()

        return {
            "content": response.text,
            "url": url,
            "format": return_format,
        }

    # =========================================================================
    # Search API - Web search with content extraction
    # =========================================================================

    async def search(
        self,
        query: str,
        *,
        num_results: int = 5,
        return_format: str = "markdown",
    ) -> List[Dict[str, Any]]:
        """Search the web and extract content.

        Args:
            query: Search query
            num_results: Number of results
            return_format: Content format

        Returns:
            List of search results with extracted content
        """
        client = await self._get_client()

        response = await client.get(
            f"https://s.jina.ai/{query}",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "X-Return-Format": return_format,
            },
        )
        response.raise_for_status()

        # Parse the markdown response into structured results
        # Jina returns markdown with results separated by ---
        content = response.text
        return [{"content": content, "query": query}]


# Global client instance
_jina_client: Optional[JinaClient] = None


def get_jina_client() -> JinaClient:
    """Get or create the global Jina client."""
    global _jina_client
    if _jina_client is None:
        _jina_client = JinaClient()
    return _jina_client


async def close_jina_client():
    """Close the global Jina client."""
    global _jina_client
    if _jina_client:
        await _jina_client.close()
        _jina_client = None
