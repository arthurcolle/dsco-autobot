"""
Graph Management API Extensions for Advanced Autonomous Agent Infrastructure

This module extends the Graph Management API with specialized endpoints for:
- Vector index management with custom embedding callbacks
- Hierarchical schema registration
- Change detection and smart redeployment
- Agent coordination patterns
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional, Union
import httpx
import hashlib
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

app = FastAPI(title="Graph Management Extensions", version="2.0.0")


# ==================== Request/Response Models ====================

class VectorIndexRequest(BaseModel):
    """Request to create a vector index with embedding configuration"""
    label: str
    property_name: str = "embedding"
    dimension: int = 1536
    similarity_function: str = "cosine"
    index_config: Dict[str, Any] = Field(default_factory=dict)
    embedding_callback_id: str
    embedding_callback_config: Dict[str, Any] = Field(default_factory=dict)


class SchemaWithEmbeddingRequest(BaseModel):
    """Request to register schema with embedding support"""
    schema_type: str  # node, relationship, subgraph
    schema_definition: Dict[str, Any]
    vector_indices: List[VectorIndexRequest] = Field(default_factory=list)
    embedding_fields: List[str] = Field(default_factory=list)
    skip_if_unchanged: bool = True


class HierarchicalSchemaRequest(BaseModel):
    """Request to register hierarchical schema structure"""
    root_schema_id: str
    child_schemas: List[Dict[str, Any]]
    relationships: List[Dict[str, Any]]
    traversal_patterns: List[str] = Field(default_factory=list)
    aggregation_rules: List[Dict[str, Any]] = Field(default_factory=list)


class EmbeddingCallbackRegistration(BaseModel):
    """Registration for embedding callback"""
    callback_id: str
    callback_type: str  # openai, local, composite
    config: Dict[str, Any]
    dimension: int


class ChangeDetectionRequest(BaseModel):
    """Request for change detection"""
    schema_id: str
    current_hash: Optional[str] = None
    check_dependencies: bool = True


class AgentRegistrationRequest(BaseModel):
    """Request to register an autonomous agent"""
    agent_id: str
    name: str
    type: str  # orchestrator, worker, specialist
    capabilities: List[str]
    model: str
    temperature: float = 0.7
    max_tokens: int = 4096
    tools: List[str] = Field(default_factory=list)
    embedding_config: Optional[Dict[str, Any]] = None


class AgentCoordinationRequest(BaseModel):
    """Request to create agent coordination"""
    coordinator_id: str
    worker_ids: List[str]
    coordination_type: str = "hierarchical"
    coordination_rules: Dict[str, Any] = Field(default_factory=dict)


# ==================== Core Services ====================

class GraphManagementExtensions:
    """Extended functionality for Graph Management API"""

    def __init__(self, neo4j_url: str = "http://localhost:2014"):
        self.neo4j_url = neo4j_url
        self.registered_callbacks: Dict[str, Dict[str, Any]] = {}
        self.schema_hashes: Dict[str, str] = {}
        self.vector_indices: Dict[str, VectorIndexRequest] = {}

    async def execute_neo4j_query(self, query: str, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Execute query against Neo4j via Graph Management API"""
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.neo4j_url}/v1/query",
                json={
                    "query": query,
                    "parameters": parameters or {}
                }
            )
            response.raise_for_status()
            return response.json()

    def compute_schema_hash(self, schema: Dict[str, Any]) -> str:
        """Compute hash of schema for change detection"""
        # Remove volatile fields
        clean_schema = {
            k: v for k, v in schema.items()
            if k not in ['created_at', 'updated_at', 'content_hash', 'code_hash']
        }
        schema_str = json.dumps(clean_schema, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()

    async def create_vector_index(self, request: VectorIndexRequest) -> Dict[str, Any]:
        """Create a vector index in Neo4j"""

        # Build index configuration
        index_config = {
            "vector.dimensions": request.dimension,
            "vector.similarity_function": request.similarity_function,
            **request.index_config
        }

        # Create index query
        index_name = f"{request.label}_{request.property_name}_vector_idx"
        query = f"""
        CREATE VECTOR INDEX {index_name} IF NOT EXISTS
        FOR (n:{request.label})
        ON n.{request.property_name}
        OPTIONS {{
            indexConfig: $config
        }}
        """

        try:
            await self.execute_neo4j_query(query, {"config": index_config})

            # Store index configuration
            self.vector_indices[index_name] = request

            # Register embedding callback
            self.registered_callbacks[request.embedding_callback_id] = {
                "type": request.embedding_callback_config.get("type", "openai"),
                "config": request.embedding_callback_config,
                "dimension": request.dimension
            }

            return {
                "status": "success",
                "index_name": index_name,
                "dimension": request.dimension,
                "callback_registered": True
            }

        except Exception as e:
            logger.error(f"Failed to create vector index: {str(e)}")
            raise HTTPException(status_code=500, detail=str(e))

    async def register_schema_with_embedding(
        self,
        request: SchemaWithEmbeddingRequest
    ) -> Dict[str, Any]:
        """Register schema with embedding support and change detection"""

        schema_id = request.schema_definition.get("schema_id")
        if not schema_id:
            raise HTTPException(status_code=400, detail="schema_id is required")

        # Compute hash for change detection
        new_hash = self.compute_schema_hash(request.schema_definition)

        # Check if unchanged
        if request.skip_if_unchanged:
            existing_hash = self.schema_hashes.get(schema_id)
            if existing_hash == new_hash:
                return {
                    "status": "unchanged",
                    "schema_id": schema_id,
                    "hash": new_hash
                }

        # Create vector indices
        created_indices = []
        for vector_req in request.vector_indices:
            index_result = await self.create_vector_index(vector_req)
            created_indices.append(index_result["index_name"])

        # Register schema based on type
        if request.schema_type == "node":
            result = await self._register_node_schema(request.schema_definition)
        elif request.schema_type == "relationship":
            result = await self._register_relationship_schema(request.schema_definition)
        elif request.schema_type == "subgraph":
            result = await self._register_subgraph_schema(request.schema_definition)
        else:
            raise HTTPException(status_code=400, detail=f"Unknown schema type: {request.schema_type}")

        # Update hash
        self.schema_hashes[schema_id] = new_hash

        return {
            "status": "registered",
            "schema_id": schema_id,
            "schema_type": request.schema_type,
            "hash": new_hash,
            "vector_indices": created_indices
        }

    async def _register_node_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Register node schema in graph"""
        query = """
        MERGE (s:Schema:NodeSchema {schema_id: $schema_id})
        SET s += $properties
        SET s.updated_at = datetime()
        RETURN s
        """

        result = await self.execute_neo4j_query(
            query,
            {
                "schema_id": schema["schema_id"],
                "properties": schema
            }
        )
        return result

    async def _register_relationship_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Register relationship schema in graph"""
        query = """
        MERGE (s:Schema:RelationshipSchema {schema_id: $schema_id})
        SET s += $properties
        SET s.updated_at = datetime()
        RETURN s
        """

        result = await self.execute_neo4j_query(
            query,
            {
                "schema_id": schema["schema_id"],
                "properties": schema
            }
        )
        return result

    async def _register_subgraph_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Register subgraph schema in graph"""
        query = """
        MERGE (s:Schema:SubgraphSchema {schema_id: $schema_id})
        SET s += $properties
        SET s.updated_at = datetime()

        // Link to root schema
        WITH s
        MATCH (root:Schema {schema_id: $root_schema_id})
        MERGE (s)-[:HAS_ROOT]->(root)

        RETURN s
        """

        result = await self.execute_neo4j_query(
            query,
            {
                "schema_id": schema["schema_id"],
                "properties": schema,
                "root_schema_id": schema.get("root_node_schema")
            }
        )

        # Link child schemas
        for child in schema.get("child_schemas", []):
            await self.execute_neo4j_query(
                """
                MATCH (s:SubgraphSchema {schema_id: $subgraph_id})
                MATCH (child:Schema {schema_id: $child_id})
                MERGE (s)-[:CONTAINS {level: $level}]->(child)
                """,
                {
                    "subgraph_id": schema["schema_id"],
                    "child_id": child["schema_id"],
                    "level": child.get("level", 0)
                }
            )

        return result

    async def register_hierarchical_schema(
        self,
        request: HierarchicalSchemaRequest
    ) -> Dict[str, Any]:
        """Register a hierarchical schema structure"""

        # Create subgraph schema
        subgraph_id = f"hierarchy_{request.root_schema_id}_{datetime.utcnow().timestamp()}"

        subgraph_schema = {
            "schema_id": subgraph_id,
            "root_node_schema": request.root_schema_id,
            "child_schemas": request.child_schemas,
            "relationships": request.relationships,
            "traversal_patterns": request.traversal_patterns,
            "aggregation_rules": request.aggregation_rules
        }

        # Register as subgraph
        result = await self._register_subgraph_schema(subgraph_schema)

        # Create relationships between schemas
        for rel in request.relationships:
            await self.execute_neo4j_query(
                """
                MATCH (start:Schema {schema_id: $start_id})
                MATCH (end:Schema {schema_id: $end_id})
                MERGE (start)-[:SCHEMA_RELATIONSHIP {
                    type: $rel_type,
                    cardinality: $cardinality
                }]->(end)
                """,
                {
                    "start_id": rel["start_schema"],
                    "end_id": rel["end_schema"],
                    "rel_type": rel["type"],
                    "cardinality": rel.get("cardinality", "many_to_many")
                }
            )

        return {
            "status": "success",
            "subgraph_id": subgraph_id,
            "root_schema": request.root_schema_id,
            "child_count": len(request.child_schemas),
            "relationship_count": len(request.relationships)
        }

    async def check_schema_changes(
        self,
        request: ChangeDetectionRequest
    ) -> Dict[str, Any]:
        """Check if schema has changed and get affected dependencies"""

        # Get current schema from graph
        query = """
        MATCH (s:Schema {schema_id: $schema_id})
        RETURN s
        """

        result = await self.execute_neo4j_query(
            query,
            {"schema_id": request.schema_id}
        )

        if not result.get("results"):
            raise HTTPException(status_code=404, detail="Schema not found")

        current_schema = result["results"][0]["s"]
        current_hash = self.compute_schema_hash(current_schema)

        # Check if changed
        changed = request.current_hash and request.current_hash != current_hash

        affected_schemas = []
        if changed and request.check_dependencies:
            # Find dependent schemas
            dep_query = """
            MATCH (s:Schema {schema_id: $schema_id})
            MATCH (dependent:Schema)-[:DEPENDS_ON|CONTAINS|HAS_ROOT*]->(s)
            RETURN DISTINCT dependent.schema_id as schema_id
            """

            dep_result = await self.execute_neo4j_query(
                dep_query,
                {"schema_id": request.schema_id}
            )

            affected_schemas = [r["schema_id"] for r in dep_result.get("results", [])]

        return {
            "schema_id": request.schema_id,
            "current_hash": current_hash,
            "changed": changed,
            "affected_schemas": affected_schemas
        }

    async def register_agent(
        self,
        request: AgentRegistrationRequest
    ) -> Dict[str, Any]:
        """Register an autonomous agent in the graph"""

        # Create agent node
        query = """
        CREATE (a:Agent:$type {
            agent_id: $agent_id,
            name: $name,
            type: $type,
            capabilities: $capabilities,
            model: $model,
            temperature: $temperature,
            max_tokens: $max_tokens,
            tools: $tools,
            state: 'initializing',
            created_at: datetime(),
            last_active: datetime()
        })
        RETURN a
        """

        result = await self.execute_neo4j_query(
            query,
            {
                "agent_id": request.agent_id,
                "name": request.name,
                "type": request.type.title(),
                "capabilities": request.capabilities,
                "model": request.model,
                "temperature": request.temperature,
                "max_tokens": request.max_tokens,
                "tools": request.tools
            }
        )

        # Create vector index for agent if embedding config provided
        if request.embedding_config:
            vector_req = VectorIndexRequest(
                label="Agent",
                property_name="embedding",
                dimension=request.embedding_config.get("dimension", 1536),
                embedding_callback_id=request.embedding_config.get("callback_id", "default"),
                embedding_callback_config=request.embedding_config
            )
            await self.create_vector_index(vector_req)

        return {
            "status": "success",
            "agent_id": request.agent_id,
            "type": request.type,
            "capabilities": request.capabilities
        }

    async def create_agent_coordination(
        self,
        request: AgentCoordinationRequest
    ) -> Dict[str, Any]:
        """Create coordination between agents"""

        # Create coordination relationships
        created_relationships = []

        for worker_id in request.worker_ids:
            query = """
            MATCH (coordinator:Agent {agent_id: $coordinator_id})
            MATCH (worker:Agent {agent_id: $worker_id})
            CREATE (coordinator)-[r:COORDINATES {
                type: $coordination_type,
                created_at: datetime(),
                rules: $rules
            }]->(worker)
            RETURN r
            """

            await self.execute_neo4j_query(
                query,
                {
                    "coordinator_id": request.coordinator_id,
                    "worker_id": worker_id,
                    "coordination_type": request.coordination_type,
                    "rules": json.dumps(request.coordination_rules)
                }
            )

            created_relationships.append({
                "from": request.coordinator_id,
                "to": worker_id,
                "type": request.coordination_type
            })

        return {
            "status": "success",
            "coordinator_id": request.coordinator_id,
            "worker_count": len(request.worker_ids),
            "coordination_type": request.coordination_type,
            "relationships": created_relationships
        }


# ==================== API Endpoints ====================

# Initialize service
service = GraphManagementExtensions()


@app.post("/v1/schemas/vector-index")
async def create_vector_index(request: VectorIndexRequest):
    """Create a vector index with embedding configuration"""
    return await service.create_vector_index(request)


@app.post("/v1/schemas/register-with-embedding")
async def register_schema_with_embedding(request: SchemaWithEmbeddingRequest):
    """Register schema with embedding support and change detection"""
    return await service.register_schema_with_embedding(request)


@app.post("/v1/schemas/hierarchical")
async def register_hierarchical_schema(request: HierarchicalSchemaRequest):
    """Register a hierarchical schema structure"""
    return await service.register_hierarchical_schema(request)


@app.post("/v1/schemas/check-changes")
async def check_schema_changes(request: ChangeDetectionRequest):
    """Check if schema has changed and get affected dependencies"""
    return await service.check_schema_changes(request)


@app.post("/v1/agents/register")
async def register_agent(request: AgentRegistrationRequest):
    """Register an autonomous agent"""
    return await service.register_agent(request)


@app.post("/v1/agents/coordination")
async def create_agent_coordination(request: AgentCoordinationRequest):
    """Create coordination between agents"""
    return await service.create_agent_coordination(request)


@app.post("/v1/embeddings/callbacks/register")
async def register_embedding_callback(request: EmbeddingCallbackRegistration):
    """Register an embedding callback"""
    service.registered_callbacks[request.callback_id] = {
        "type": request.callback_type,
        "config": request.config,
        "dimension": request.dimension
    }
    return {
        "status": "success",
        "callback_id": request.callback_id,
        "dimension": request.dimension
    }


@app.get("/v1/embeddings/callbacks")
async def list_embedding_callbacks():
    """List registered embedding callbacks"""
    return {
        "callbacks": service.registered_callbacks
    }


@app.post("/v1/schemas/search-vector")
async def search_schemas_by_vector(
    embedding: List[float],
    label: str = "Schema",
    property_name: str = "embedding",
    limit: int = 10,
    min_similarity: float = 0.7
):
    """Search schemas using vector similarity"""

    query = f"""
    MATCH (s:{label})
    WHERE s.{property_name} IS NOT NULL
    WITH s, gds.similarity.cosine(s.{property_name}, $embedding) AS similarity
    WHERE similarity >= $min_similarity
    RETURN s.schema_id as schema_id,
           s.name as name,
           s.type as type,
           similarity
    ORDER BY similarity DESC
    LIMIT $limit
    """

    result = await service.execute_neo4j_query(
        query,
        {
            "embedding": embedding,
            "min_similarity": min_similarity,
            "limit": limit
        }
    )

    return {
        "results": result.get("results", []),
        "count": len(result.get("results", []))
    }


@app.get("/v1/agents/{agent_id}/capabilities")
async def get_agent_capabilities(agent_id: str):
    """Get capabilities and tools for an agent"""

    query = """
    MATCH (a:Agent {agent_id: $agent_id})
    OPTIONAL MATCH (a)-[:HAS_TOOL]->(t:Tool)
    RETURN a.capabilities as capabilities,
           a.tools as tool_ids,
           collect(t.name) as tool_names
    """

    result = await service.execute_neo4j_query(
        query,
        {"agent_id": agent_id}
    )

    if not result.get("results"):
        raise HTTPException(status_code=404, detail="Agent not found")

    return result["results"][0]


@app.post("/v1/agents/find-similar")
async def find_similar_agents(
    capabilities: List[str],
    embedding: Optional[List[float]] = None,
    limit: int = 5
):
    """Find agents with similar capabilities"""

    if embedding:
        # Use vector similarity
        query = """
        MATCH (a:Agent)
        WHERE a.embedding IS NOT NULL
        WITH a, gds.similarity.cosine(a.embedding, $embedding) AS similarity
        WHERE similarity > 0.7
        RETURN a.agent_id as agent_id,
               a.name as name,
               a.capabilities as capabilities,
               similarity
        ORDER BY similarity DESC
        LIMIT $limit
        """

        result = await service.execute_neo4j_query(
            query,
            {"embedding": embedding, "limit": limit}
        )
    else:
        # Use capability matching
        query = """
        MATCH (a:Agent)
        WHERE any(cap IN $capabilities WHERE cap IN a.capabilities)
        WITH a, size([cap IN $capabilities WHERE cap IN a.capabilities]) as matches
        RETURN a.agent_id as agent_id,
               a.name as name,
               a.capabilities as capabilities,
               matches
        ORDER BY matches DESC
        LIMIT $limit
        """

        result = await service.execute_neo4j_query(
            query,
            {"capabilities": capabilities, "limit": limit}
        )

    return {
        "query_capabilities": capabilities,
        "results": result.get("results", [])
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Graph Management Extensions",
        "version": "2.0.0",
        "registered_callbacks": len(service.registered_callbacks),
        "registered_indices": len(service.vector_indices)
    }


# ==================== Background Tasks ====================

async def recompute_embeddings_for_schema(schema_id: str, callback_id: str):
    """Background task to recompute embeddings for all instances of a schema"""

    # Get all instances of the schema
    query = """
    MATCH (n)
    WHERE n.schema_id = $schema_id
    RETURN n
    """

    result = await service.execute_neo4j_query(
        query,
        {"schema_id": schema_id}
    )

    # Process each instance
    for record in result.get("results", []):
        node = record["n"]
        # Here you would call the actual embedding callback
        # For now, we'll just log it
        logger.info(f"Would recompute embedding for node with schema {schema_id}")


@app.post("/v1/schemas/{schema_id}/recompute-embeddings")
async def trigger_embedding_recomputation(
    schema_id: str,
    callback_id: str,
    background_tasks: BackgroundTasks
):
    """Trigger background recomputation of embeddings"""

    background_tasks.add_task(
        recompute_embeddings_for_schema,
        schema_id,
        callback_id
    )

    return {
        "status": "triggered",
        "schema_id": schema_id,
        "callback_id": callback_id
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)