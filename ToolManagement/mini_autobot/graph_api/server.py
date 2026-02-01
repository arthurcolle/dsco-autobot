"""
Graph Management API - Generic graph operations over Neo4j.

Provides REST endpoints for schema management, node/relationship CRUD,
vector similarity search, and arbitrary Cypher query execution.
"""
import os
import json
import uuid
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from neo4j import GraphDatabase, Driver

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models
# =============================================================================

class NodeProperty(BaseModel):
    type: str  # "string", "integer", "float", "boolean", "list", "object"
    required: bool = False


class NodeSchema(BaseModel):
    label: str
    properties: Dict[str, NodeProperty]


class RelationshipSchema(BaseModel):
    type: str
    start_node: str
    end_node: str
    properties: Dict[str, NodeProperty] = Field(default_factory=dict)


class GraphSchema(BaseModel):
    nodes: List[NodeSchema]
    relationships: List[RelationshipSchema]


class SchemaRegisterRequest(BaseModel):
    schema: GraphSchema


class CreateNodeRequest(BaseModel):
    label: str
    properties: Dict[str, Any]


class CreateRelationshipRequest(BaseModel):
    start_id: str
    end_id: str
    relationship_type: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    start_label: str
    end_label: str


class VectorSearchRequest(BaseModel):
    node_label: str
    embedding: List[float]
    limit: int = 5
    embedding_property: str = "embedding"


class FullTextSearchRequest(BaseModel):
    node_label: str
    search_text: str
    limit: int = 5
    search_properties: List[str] = Field(default_factory=lambda: ["name", "description"])


class ArbitraryQueryRequest(BaseModel):
    query: str
    params: Dict[str, Any] = Field(default_factory=dict)
    strategy: str = "default"  # "default", "all", "single"


class UpdateNodeRequest(BaseModel):
    label: str
    node_id: str
    properties: Dict[str, Any]


class DeleteNodeRequest(BaseModel):
    label: str
    node_id: str


# =============================================================================
# Neo4j Connection
# =============================================================================

class Neo4jConnection:
    """Manages Neo4j database connection."""

    def __init__(
        self,
        uri: str = "neo4j://127.0.0.1:7687",
        username: str = "neo4j",
        password: str = "password",
        database: str = "neo4j"
    ):
        self.uri = uri
        self.username = username
        self.password = password
        self.database = database
        self._driver: Optional[Driver] = None

    def connect(self) -> None:
        """Establish connection to Neo4j."""
        self._driver = GraphDatabase.driver(
            self.uri,
            auth=(self.username, self.password)
        )
        # Verify connection
        self._driver.verify_connectivity()
        logger.info(f"Connected to Neo4j at {self.uri}")

    def close(self) -> None:
        """Close the Neo4j connection."""
        if self._driver:
            self._driver.close()
            logger.info("Closed Neo4j connection")

    def get_session(self):
        """Get a Neo4j session."""
        return self._driver.session(database=self.database)

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a Cypher query and return results."""
        with self.get_session() as session:
            result = session.run(query, parameters=params or {})
            return [dict(record) for record in result]

    def execute_write(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a write query and return single result."""
        with self.get_session() as session:
            result = session.run(query, parameters=params or {})
            return result.single()


# Global connection instance
_neo4j: Optional[Neo4jConnection] = None


def get_neo4j() -> Neo4jConnection:
    """Dependency to get Neo4j connection."""
    global _neo4j
    if _neo4j is None:
        raise HTTPException(status_code=503, detail="Database not connected")
    return _neo4j


# =============================================================================
# FastAPI Application
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    global _neo4j

    # Startup
    _neo4j = Neo4jConnection(
        uri=os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687"),
        username=os.getenv("NEO4J_USER", "neo4j"),
        password=os.getenv("NEO4J_PASSWORD", "NEO4J2026!"),
        database=os.getenv("NEO4J_DATABASE", "neo4j")
    )
    _neo4j.connect()

    # Initialize schema indexes
    await init_indexes(_neo4j)

    yield

    # Shutdown
    if _neo4j:
        _neo4j.close()


app = FastAPI(
    title="Graph Management API",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def init_indexes(neo4j: Neo4jConnection) -> None:
    """Initialize database indexes."""
    try:
        # Create constraint for unique IDs on common node types
        for label in ["Tool", "ToolSchema", "ToolAction", "ToolOutput"]:
            try:
                neo4j.execute_query(f"""
                    CREATE CONSTRAINT {label.lower()}_id IF NOT EXISTS
                    FOR (n:{label}) REQUIRE n.id IS UNIQUE
                """)
            except Exception:
                pass  # Constraint may already exist

        logger.info("Database indexes initialized")
    except Exception as e:
        logger.warning(f"Could not initialize indexes: {e}")


# =============================================================================
# Health Check
# =============================================================================

@app.get("/health")
async def health_check(neo4j: Neo4jConnection = Depends(get_neo4j)):
    """Health check endpoint."""
    try:
        neo4j.execute_query("RETURN 1")
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unhealthy: {str(e)}")


# =============================================================================
# Schema Management
# =============================================================================

@app.post("/api/v1/schema/register")
async def register_schema(
    request: SchemaRegisterRequest,
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> Dict[str, Any]:
    """Register a graph schema with constraints and indexes."""
    schema = request.schema
    created_constraints = []
    created_indexes = []

    # Create constraints for required properties
    for node in schema.nodes:
        for prop_name, prop_def in node.properties.items():
            if prop_def.required:
                try:
                    constraint_name = f"{node.label.lower()}_{prop_name}_exists"
                    neo4j.execute_query(f"""
                        CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
                        FOR (n:{node.label}) REQUIRE n.{prop_name} IS NOT NULL
                    """)
                    created_constraints.append(constraint_name)
                except Exception as e:
                    logger.warning(f"Could not create constraint {constraint_name}: {e}")

        # Create index on label
        try:
            index_name = f"{node.label.lower()}_idx"
            neo4j.execute_query(f"""
                CREATE INDEX {index_name} IF NOT EXISTS
                FOR (n:{node.label}) ON (n.id)
            """)
            created_indexes.append(index_name)
        except Exception as e:
            logger.warning(f"Could not create index {index_name}: {e}")

    return {
        "status": "registered",
        "constraints_created": created_constraints,
        "indexes_created": created_indexes,
        "schema": schema.model_dump()
    }


# =============================================================================
# Node Management
# =============================================================================

@app.post("/api/v1/node-with-metadata")
async def create_node_with_metadata(
    request: CreateNodeRequest,
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> Dict[str, Any]:
    """Create a node with metadata (id, timestamps)."""
    node_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    # Build properties with metadata
    props = {
        "id": node_id,
        "created_at": now,
        "updated_at": now,
        **request.properties
    }

    # Serialize any dict/list properties to JSON
    serialized_props = {}
    for k, v in props.items():
        if isinstance(v, (dict, list)):
            serialized_props[k] = json.dumps(v)
        else:
            serialized_props[k] = v

    # Build SET clause
    set_clause = ", ".join([f"n.{k} = ${k}" for k in serialized_props.keys()])

    query = f"""
        CREATE (n:{request.label})
        SET {set_clause}
        RETURN n
    """

    result = neo4j.execute_write(query, serialized_props)

    if result:
        node = dict(result["n"])
        return {
            "id": node_id,
            "label": request.label,
            "properties": request.properties
        }

    raise HTTPException(status_code=500, detail="Failed to create node")


@app.get("/api/v1/node/{label}/{node_id}")
async def get_node(
    label: str,
    node_id: str,
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> Dict[str, Any]:
    """Get a node by label and ID."""
    result = neo4j.execute_query(f"""
        MATCH (n:{label} {{id: $node_id}})
        RETURN n
    """, {"node_id": node_id})

    if result:
        node = dict(result[0]["n"])
        return {
            "id": node_id,
            "label": label,
            "properties": node
        }

    raise HTTPException(status_code=404, detail=f"Node not found: {label}/{node_id}")


@app.put("/api/v1/node")
async def update_node(
    request: UpdateNodeRequest,
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> Dict[str, Any]:
    """Update a node's properties."""
    now = datetime.now(timezone.utc).isoformat()
    props = {"updated_at": now, **request.properties}

    # Serialize any dict/list properties
    serialized_props = {}
    for k, v in props.items():
        if isinstance(v, (dict, list)):
            serialized_props[k] = json.dumps(v)
        else:
            serialized_props[k] = v

    set_clause = ", ".join([f"n.{k} = ${k}" for k in serialized_props.keys()])

    query = f"""
        MATCH (n:{request.label} {{id: $node_id}})
        SET {set_clause}
        RETURN n
    """

    result = neo4j.execute_write(query, {"node_id": request.node_id, **serialized_props})

    if result:
        return {
            "id": request.node_id,
            "label": request.label,
            "properties": request.properties,
            "updated": True
        }

    raise HTTPException(status_code=404, detail=f"Node not found: {request.label}/{request.node_id}")


@app.delete("/api/v1/node/{label}/{node_id}")
async def delete_node(
    label: str,
    node_id: str,
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> Dict[str, Any]:
    """Delete a node and its relationships."""
    result = neo4j.execute_query(f"""
        MATCH (n:{label} {{id: $node_id}})
        DETACH DELETE n
        RETURN count(n) as deleted
    """, {"node_id": node_id})

    if result and result[0]["deleted"] > 0:
        return {"deleted": True, "id": node_id, "label": label}

    raise HTTPException(status_code=404, detail=f"Node not found: {label}/{node_id}")


# =============================================================================
# Relationship Management
# =============================================================================

@app.post("/api/v1/relationship-with-metadata")
async def create_relationship_with_metadata(
    request: CreateRelationshipRequest,
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> Dict[str, Any]:
    """Create a relationship between two nodes."""
    rel_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    props = {
        "id": rel_id,
        "created_at": now,
        **request.properties
    }

    # Serialize dict/list properties
    serialized_props = {}
    for k, v in props.items():
        if isinstance(v, (dict, list)):
            serialized_props[k] = json.dumps(v)
        else:
            serialized_props[k] = v

    set_clause = ", ".join([f"r.{k} = ${k}" for k in serialized_props.keys()])

    query = f"""
        MATCH (a:{request.start_label} {{id: $start_id}})
        MATCH (b:{request.end_label} {{id: $end_id}})
        CREATE (a)-[r:{request.relationship_type}]->(b)
        SET {set_clause}
        RETURN r
    """

    result = neo4j.execute_write(query, {
        "start_id": request.start_id,
        "end_id": request.end_id,
        **serialized_props
    })

    if result:
        return {
            "id": rel_id,
            "type": request.relationship_type,
            "start_id": request.start_id,
            "end_id": request.end_id,
            "properties": request.properties
        }

    raise HTTPException(status_code=404, detail="Could not create relationship - nodes not found")


# =============================================================================
# Search
# =============================================================================

@app.post("/api/v1/search/vector")
async def vector_similarity_search(
    request: VectorSearchRequest,
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> List[Dict[str, Any]]:
    """
    Vector similarity search using cosine similarity.

    Note: For production, use Neo4j's native vector index (5.11+).
    This fallback computes similarity in Cypher.
    """
    # Try native vector index first
    try:
        results = neo4j.execute_query(f"""
            CALL db.index.vector.queryNodes('{request.node_label.lower()}_embedding', $limit, $embedding)
            YIELD node, score
            RETURN node, score
            ORDER BY score DESC
        """, {"embedding": request.embedding, "limit": request.limit})

        return [
            {"node": dict(r["node"]), "score": r["score"]}
            for r in results
        ]
    except Exception:
        pass  # Vector index not available, fall back to Cypher

    # Fallback: compute cosine similarity in Cypher
    results = neo4j.execute_query(f"""
        MATCH (n:{request.node_label})
        WHERE n.{request.embedding_property} IS NOT NULL
        WITH n, n.{request.embedding_property} AS emb
        WITH n, emb,
             reduce(dot = 0.0, i IN range(0, size($embedding)-1) |
                    dot + emb[i] * $embedding[i]) AS dotProduct,
             reduce(norm1 = 0.0, x IN emb | norm1 + x*x) AS norm1,
             reduce(norm2 = 0.0, x IN $embedding | norm2 + x*x) AS norm2
        WITH n, dotProduct / (sqrt(norm1) * sqrt(norm2)) AS similarity
        WHERE similarity IS NOT NULL
        RETURN n AS node, similarity AS score
        ORDER BY score DESC
        LIMIT $limit
    """, {"embedding": request.embedding, "limit": request.limit})

    return [
        {"node": dict(r["node"]), "score": r["score"]}
        for r in results
    ]


@app.post("/api/v1/search/fulltext")
async def full_text_search(
    request: FullTextSearchRequest,
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> List[Dict[str, Any]]:
    """Full-text search across node properties."""
    # Try native full-text index first
    try:
        results = neo4j.execute_query(f"""
            CALL db.index.fulltext.queryNodes('{request.node_label.lower()}_search', $search_text)
            YIELD node, score
            RETURN node, score
            ORDER BY score DESC
            LIMIT $limit
        """, {"search_text": request.search_text, "limit": request.limit})

        return [
            {"node": dict(r["node"]), "score": r["score"]}
            for r in results
        ]
    except Exception:
        pass  # Full-text index not available

    # Fallback: CONTAINS search
    where_clauses = [
        f"toLower(n.{prop}) CONTAINS toLower($search_text)"
        for prop in request.search_properties
    ]
    where_clause = " OR ".join(where_clauses)

    results = neo4j.execute_query(f"""
        MATCH (n:{request.node_label})
        WHERE {where_clause}
        RETURN n AS node, 1.0 AS score
        LIMIT $limit
    """, {"search_text": request.search_text, "limit": request.limit})

    return [
        {"node": dict(r["node"]), "score": r["score"]}
        for r in results
    ]


# =============================================================================
# Arbitrary Query Execution
# =============================================================================

@app.post("/api/v1/query/arbitrary")
async def execute_arbitrary_query(
    request: ArbitraryQueryRequest,
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> Dict[str, Any]:
    """Execute an arbitrary Cypher query."""
    try:
        with neo4j.get_session() as session:
            result = session.run(request.query, parameters=request.params)

            if request.strategy == "single":
                record = result.single()
                return {"result": dict(record) if record else None}
            elif request.strategy == "all":
                records = [dict(r) for r in result]
                return {"result": records}
            else:  # default
                records = [dict(r) for r in result]
                return {
                    "result": records,
                    "count": len(records)
                }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")


# =============================================================================
# Batch Operations
# =============================================================================

@app.post("/api/v1/batch/nodes")
async def batch_create_nodes(
    nodes: List[CreateNodeRequest],
    neo4j: Neo4jConnection = Depends(get_neo4j)
) -> Dict[str, Any]:
    """Create multiple nodes in a single transaction."""
    created = []

    with neo4j.get_session() as session:
        with session.begin_transaction() as tx:
            for node_req in nodes:
                node_id = str(uuid.uuid4())
                now = datetime.now(timezone.utc).isoformat()

                props = {
                    "id": node_id,
                    "created_at": now,
                    "updated_at": now,
                    **node_req.properties
                }

                serialized = {
                    k: json.dumps(v) if isinstance(v, (dict, list)) else v
                    for k, v in props.items()
                }

                set_clause = ", ".join([f"n.{k} = ${k}" for k in serialized.keys()])
                query = f"CREATE (n:{node_req.label}) SET {set_clause} RETURN n"

                tx.run(query, serialized)
                created.append({"id": node_id, "label": node_req.label})

            tx.commit()

    return {"created": created, "count": len(created)}


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "2015")))
