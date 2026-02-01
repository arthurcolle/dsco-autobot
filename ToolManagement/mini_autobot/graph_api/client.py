"""
Graph API Client - Client library for the Graph Management API.

Provides async methods for all graph operations: nodes, relationships,
search, and arbitrary queries.
"""
import httpx
import logging
import json
from typing import List, Dict, Any, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GraphAPIClient:
    """
    Async client for the Graph Management API.

    Usage:
        client = GraphAPIClient("http://localhost:2015")

        # Create a node
        node = await client.create_node("Tool", {"name": "my_tool", "description": "..."})

        # Create relationship
        await client.create_relationship(node["id"], schema["id"], "HAS_SCHEMA", {}, "Tool", "ToolSchema")

        # Search
        results = await client.vector_search("Tool", embedding_vector, limit=5)
    """

    def __init__(self, base_url: str, timeout: float = 30.0):
        """
        Initialize the Graph API client.

        Args:
            base_url: Base URL of the Graph Management API (e.g., "http://localhost:2015")
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """Make an HTTP request to the API."""
        client = await self._get_client()
        url = f"{self.base_url}{endpoint}"

        try:
            response = await client.request(method, url, **kwargs)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            raise

    # =========================================================================
    # Health Check
    # =========================================================================

    async def health_check(self) -> Dict[str, Any]:
        """Check the health of the Graph Management API."""
        return await self._request("GET", "/health")

    # =========================================================================
    # Schema Management
    # =========================================================================

    async def register_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """
        Register a graph schema with constraints and indexes.

        Args:
            schema: Schema definition with "nodes" and "relationships" keys

        Returns:
            Registration result with created constraints/indexes
        """
        return await self._request("POST", "/api/v1/schema/register", json={"schema": schema})

    # =========================================================================
    # Node Management
    # =========================================================================

    async def create_node(self, label: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a node with automatic metadata (id, timestamps).

        Args:
            label: Node label (e.g., "Tool", "ToolSchema")
            properties: Node properties

        Returns:
            Created node with id
        """
        return await self._request(
            "POST",
            "/api/v1/node-with-metadata",
            json={"label": label, "properties": properties}
        )

    async def create_node_with_metadata(self, label: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Alias for create_node for compatibility."""
        return await self.create_node(label, properties)

    async def get_node(self, label: str, node_id: str) -> Dict[str, Any]:
        """
        Get a node by label and ID.

        Args:
            label: Node label
            node_id: Node ID

        Returns:
            Node data
        """
        return await self._request("GET", f"/api/v1/node/{label}/{node_id}")

    async def update_node(self, label: str, node_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a node's properties.

        Args:
            label: Node label
            node_id: Node ID
            properties: Properties to update

        Returns:
            Updated node data
        """
        return await self._request(
            "PUT",
            "/api/v1/node",
            json={"label": label, "node_id": node_id, "properties": properties}
        )

    async def delete_node(self, label: str, node_id: str) -> Dict[str, Any]:
        """
        Delete a node and its relationships.

        Args:
            label: Node label
            node_id: Node ID

        Returns:
            Deletion result
        """
        return await self._request("DELETE", f"/api/v1/node/{label}/{node_id}")

    # =========================================================================
    # Relationship Management
    # =========================================================================

    async def create_relationship(
        self,
        start_id: str,
        end_id: str,
        relationship_type: str,
        properties: Dict[str, Any],
        start_label: str,
        end_label: str
    ) -> Dict[str, Any]:
        """
        Create a relationship between two nodes.

        Args:
            start_id: Starting node ID
            end_id: Ending node ID
            relationship_type: Type of relationship (e.g., "HAS_SCHEMA")
            properties: Relationship properties
            start_label: Label of starting node
            end_label: Label of ending node

        Returns:
            Created relationship data
        """
        return await self._request(
            "POST",
            "/api/v1/relationship-with-metadata",
            json={
                "start_id": start_id,
                "end_id": end_id,
                "relationship_type": relationship_type,
                "properties": properties,
                "start_label": start_label,
                "end_label": end_label
            }
        )

    async def create_relationship_with_metadata(
        self,
        start_id: str,
        end_id: str,
        relationship_type: str,
        properties: Dict[str, Any],
        start_label: str,
        end_label: str
    ) -> Dict[str, Any]:
        """Alias for create_relationship for compatibility."""
        return await self.create_relationship(
            start_id, end_id, relationship_type, properties, start_label, end_label
        )

    # =========================================================================
    # Search
    # =========================================================================

    async def vector_search(
        self,
        node_label: str,
        embedding: List[float],
        limit: int = 5,
        embedding_property: str = "embedding"
    ) -> List[Dict[str, Any]]:
        """
        Vector similarity search.

        Args:
            node_label: Label of nodes to search
            embedding: Query embedding vector
            limit: Maximum results to return
            embedding_property: Name of the embedding property

        Returns:
            List of matching nodes with similarity scores
        """
        return await self._request(
            "POST",
            "/api/v1/search/vector",
            json={
                "node_label": node_label,
                "embedding": embedding,
                "limit": limit,
                "embedding_property": embedding_property
            }
        )

    async def vector_similarity_search(
        self,
        node_label: str,
        embedding: List[float],
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Alias for vector_search for compatibility."""
        return await self.vector_search(node_label, embedding, limit)

    async def full_text_search(
        self,
        node_label: str,
        search_text: str,
        limit: int = 5,
        search_properties: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Full-text search across node properties.

        Args:
            node_label: Label of nodes to search
            search_text: Text to search for
            limit: Maximum results to return
            search_properties: Properties to search in

        Returns:
            List of matching nodes with scores
        """
        payload = {
            "node_label": node_label,
            "search_text": search_text,
            "limit": limit
        }
        if search_properties:
            payload["search_properties"] = search_properties

        return await self._request("POST", "/api/v1/search/fulltext", json=payload)

    # =========================================================================
    # Arbitrary Query Execution
    # =========================================================================

    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        strategy: str = "default"
    ) -> Dict[str, Any]:
        """
        Execute an arbitrary Cypher query.

        Args:
            query: Cypher query string
            params: Query parameters
            strategy: Result strategy ("default", "single", "all")

        Returns:
            Query result
        """
        return await self._request(
            "POST",
            "/api/v1/query/arbitrary",
            json={
                "query": query,
                "params": params or {},
                "strategy": strategy
            }
        )

    # =========================================================================
    # Batch Operations
    # =========================================================================

    async def batch_create_nodes(self, nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create multiple nodes in a single transaction.

        Args:
            nodes: List of {"label": str, "properties": dict}

        Returns:
            Created nodes summary
        """
        return await self._request("POST", "/api/v1/batch/nodes", json=nodes)

    # =========================================================================
    # Tool-Specific Helpers
    # =========================================================================

    async def create_tool_with_schema(
        self,
        name: str,
        description: str,
        input_schema: Dict[str, Any],
        output_schema: Dict[str, Any],
        action: Dict[str, Any],
        output: Dict[str, Any],
        embedding: Optional[List[float]] = None,
        user_settings: Optional[List[Dict[str, Any]]] = None,
        version: str = "1.0.0"
    ) -> Dict[str, Any]:
        """
        Create a tool with its schema, action, and output nodes.

        This is a convenience method that creates all related nodes and relationships
        following the Tool graph pattern.

        Returns:
            Dict with tool_id and all created node IDs
        """
        # Create Tool node
        tool_props = {
            "name": name,
            "description": description,
            "version": version,
            "status": "online"
        }
        if embedding:
            tool_props["embedding"] = embedding
        if user_settings:
            tool_props["user_settings"] = user_settings

        tool = await self.create_node("Tool", tool_props)
        tool_id = tool["id"]

        # Create ToolSchema node
        schema = await self.create_node("ToolSchema", {
            "input_schema": input_schema,
            "output_schema": output_schema
        })

        # Create ToolAction node
        action_node = await self.create_node("ToolAction", action)

        # Create ToolOutput node
        output_node = await self.create_node("ToolOutput", output)

        # Create relationships
        await self.create_relationship(tool_id, schema["id"], "HAS_SCHEMA", {}, "Tool", "ToolSchema")
        await self.create_relationship(tool_id, action_node["id"], "HAS_ACTION", {}, "Tool", "ToolAction")
        await self.create_relationship(tool_id, output_node["id"], "HAS_OUTPUT", {}, "Tool", "ToolOutput")

        return {
            "tool_id": tool_id,
            "schema_id": schema["id"],
            "action_id": action_node["id"],
            "output_id": output_node["id"]
        }

    async def get_tool_full(self, tool_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a tool with all its related nodes.

        Returns:
            Full tool data with schema, action, and output
        """
        result = await self.execute_query("""
            MATCH (t:Tool {id: $tool_id})
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            RETURN t, s, a, o
        """, {"tool_id": tool_id}, strategy="single")

        if result and result.get("result"):
            r = result["result"]
            return {
                "tool": r.get("t"),
                "schema": r.get("s"),
                "action": r.get("a"),
                "output": r.get("o")
            }
        return None

    async def list_tools(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List all tools with their schemas.

        Returns:
            List of tools with full data
        """
        result = await self.execute_query("""
            MATCH (t:Tool)
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            RETURN t, s, a, o
            LIMIT $limit
        """, {"limit": limit}, strategy="all")

        tools = []
        for r in result.get("result", []):
            tools.append({
                "tool": r.get("t"),
                "schema": r.get("s"),
                "action": r.get("a"),
                "output": r.get("o")
            })
        return tools

    async def delete_tool_full(self, tool_id: str) -> bool:
        """
        Delete a tool and all its related nodes.

        Returns:
            True if deleted successfully
        """
        result = await self.execute_query("""
            MATCH (t:Tool {id: $tool_id})
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            DETACH DELETE t, s, a, o
            RETURN count(t) as deleted
        """, {"tool_id": tool_id}, strategy="single")

        return result.get("result", {}).get("deleted", 0) > 0


# =============================================================================
# Convenience Function
# =============================================================================

def create_graph_client(base_url: str = "http://localhost:2015") -> GraphAPIClient:
    """Create a Graph API client with default settings."""
    return GraphAPIClient(base_url)
