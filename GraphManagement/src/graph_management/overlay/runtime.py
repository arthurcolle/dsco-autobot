"""
Runtime support for graph overlay system.

Provides:
- GraphClient registry
- Embedding provider registry
- Enhanced GraphClient with vector operations
"""

from __future__ import annotations
from typing import Any, Callable, Dict, List, Optional
import httpx


# ========= Enhanced GraphClient =========


class GraphClient:
    """
    Enhanced graph client with vector and relationship support.

    Extends the basic client with:
    - Vector storage and search
    - Relationship merging
    - Traversal operations
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: float = 20.0
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        self.timeout = timeout
        self._client = None

    def _get_client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(timeout=self.timeout, headers=self.headers)
        return self._client

    def merge_node(
        self,
        label: str,
        key: Dict[str, Any],
        props: Optional[Dict[str, Any]] = None,
        vectors: Optional[Dict[str, List[float]]] = None
    ) -> Dict[str, Any]:
        """
        Merge (create or update) a node with optional vectors.

        Args:
            label: Node label
            key: Identity key dict
            props: Node properties
            vectors: Dict mapping space name -> embedding vector

        Returns:
            Backend response with node details
        """
        body = {
            "label": label,
            "key": key,
            "props": props or {},
            "vectors": vectors or {}
        }

        client = self._get_client()
        r = client.post(f"{self.base_url}/overlay/nodes/merge", json=body)
        r.raise_for_status()
        return r.json()

    def merge_rel(
        self,
        out_label: str,
        out_key: Dict[str, Any],
        rel_type: str,
        in_label: str,
        in_key: Dict[str, Any],
        props: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Merge (create or update) a relationship.

        Args:
            out_label: Source node label
            out_key: Source node key
            rel_type: Relationship type
            in_label: Target node label
            in_key: Target node key
            props: Relationship properties

        Returns:
            Backend response
        """
        body = {
            "out": {"label": out_label, "key": out_key},
            "rel": {"type": rel_type, "props": props or {}},
            "in": {"label": in_label, "key": in_key}
        }

        client = self._get_client()
        r = client.post(f"{self.base_url}/overlay/rels/merge", json=body)
        r.raise_for_status()
        return r.json()

    def vector_search(
        self,
        label: str,
        space: str,
        vector: List[float],
        k: int,
        threshold: float,
        similarity: str = "cosine"
    ) -> List[Dict[str, Any]]:
        """
        Perform k-NN vector search.

        Args:
            label: Node label to search
            space: Embedding space name
            vector: Query vector
            k: Number of results
            threshold: Minimum similarity score
            similarity: "cosine", "dot", or "euclidean"

        Returns:
            List of similar nodes with scores
        """
        body = {
            "label": label,
            "space": space,
            "vector": vector,
            "k": k,
            "threshold": threshold,
            "similarity": similarity
        }

        client = self._get_client()
        r = client.post(f"{self.base_url}/overlay/vectors/search", json=body)
        r.raise_for_status()
        return r.json().get("results", [])

    def traverse(
        self,
        start: Dict[str, Any],
        rel_type: Optional[str] = None,
        direction: str = "OUT",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Traverse from a starting node.

        Args:
            start: {"label": str, "key": dict}
            rel_type: Relationship type filter
            direction: "OUT", "IN", or "BOTH"
            limit: Max results

        Returns:
            List of neighbor nodes
        """
        body = {
            "start": start,
            "rel_type": rel_type,
            "direction": direction,
            "limit": limit
        }

        client = self._get_client()
        r = client.post(f"{self.base_url}/overlay/traverse", json=body)
        r.raise_for_status()
        return r.json().get("nodes", [])

    def get_node(
        self,
        label: str,
        key: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch a single node by key.

        Args:
            label: Node label
            key: Identity key dict

        Returns:
            Node data or None if not found
        """
        params = {"label": label, **key}

        import json as _json
        client = self._get_client()
        params["key"] = _json.dumps(key)
        r = client.get(f"{self.base_url}/overlay/nodes/get", params=params)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def delete_node(
        self,
        label: str,
        key: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Delete a node.

        Args:
            label: Node label
            key: Identity key dict

        Returns:
            Deletion confirmation
        """
        body = {"label": label, "key": key}

        client = self._get_client()
        r = client.post(f"{self.base_url}/overlay/nodes/delete", json=body)
        r.raise_for_status()
        return r.json()


# ========= Client Registry =========

_GRAPH_CLIENTS: Dict[str, GraphClient] = {}


def register_graph_client(
    service_id: str,
    base_url: str,
    api_key: Optional[str] = None
):
    """
    Register a GraphClient for a service.

    Args:
        service_id: Service identifier (e.g., "graph-svc")
        base_url: Base URL of graph service
        api_key: Optional API key for auth
    """
    _GRAPH_CLIENTS[service_id] = GraphClient(base_url, api_key=api_key)


def get_graph_client(service_id: str) -> GraphClient:
    """
    Get registered GraphClient by service ID.

    Args:
        service_id: Service identifier

    Returns:
        GraphClient instance

    Raises:
        RuntimeError: If service not registered
    """
    if service_id not in _GRAPH_CLIENTS:
        raise RuntimeError(
            f"No GraphClient registered for service_id='{service_id}'. "
            f"Call register_graph_client('{service_id}', base_url) first."
        )
    return _GRAPH_CLIENTS[service_id]


# ========= Embedding Provider Registry =========

_EMBED_PROVIDERS: Dict[str, Callable] = {}


def register_embedder(provider: str, fn: Callable):
    """
    Register an embedding provider.

    The callable should have signature:
        fn(text: str, model: str, dim: int) -> List[float]

    Args:
        provider: Provider name (e.g., "openai", "fastembed")
        fn: Embedding function
    """
    _EMBED_PROVIDERS[provider] = fn


def get_embedder(provider: str) -> Callable:
    """
    Get registered embedding provider.

    Args:
        provider: Provider name

    Returns:
        Embedding function

    Raises:
        RuntimeError: If provider not registered
    """
    if provider not in _EMBED_PROVIDERS:
        raise RuntimeError(
            f"No embedder registered for provider='{provider}'. "
            f"Available: {list(_EMBED_PROVIDERS.keys())}"
        )
    return _EMBED_PROVIDERS[provider]
