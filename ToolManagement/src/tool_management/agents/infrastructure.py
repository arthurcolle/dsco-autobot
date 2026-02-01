"""
Autonomous Agent Infrastructure with Advanced Schema Management

This module provides graph-native infrastructure for autonomous agents with:
- Hierarchical schema registration (nodes, relationships, subgraphs)
- Custom embedding callbacks for vector indices
- Change detection for smart redeployment
- Agent orchestration and coordination
"""

from typing import Dict, List, Any, Optional, Callable, Union, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from pydantic import BaseModel, Field
import hashlib
import json
import asyncio
import logging
import inspect
from abc import ABC, abstractmethod
import numpy as np
from collections import defaultdict

logger = logging.getLogger(__name__)


# ==================== Schema Types ====================

class SchemaType(str, Enum):
    """Types of schemas that can be registered"""
    NODE = "node"
    RELATIONSHIP = "relationship"
    SUBGRAPH = "subgraph"
    PATTERN = "pattern"
    HIERARCHY = "hierarchy"


class EmbeddingDimension(int, Enum):
    """Standard embedding dimensions"""
    SMALL = 384    # MiniLM
    MEDIUM = 768   # BERT
    LARGE = 1536   # OpenAI Ada
    XLARGE = 3072  # OpenAI v3


class IndexType(str, Enum):
    """Types of indices supported"""
    BTREE = "btree"
    FULLTEXT = "fulltext"
    VECTOR = "vector"
    SPATIAL = "spatial"
    COMPOSITE = "composite"


# ==================== Core Models ====================

class EmbeddingConfig(BaseModel):
    """Configuration for embedding generation"""
    dimension: int = EmbeddingDimension.LARGE
    model: str = "text-embedding-ada-002"
    callback_id: str  # ID of registered callback function
    cache_embeddings: bool = True
    batch_size: int = 100
    update_trigger: List[str] = Field(default_factory=list)  # Properties that trigger re-embedding
    metadata: Dict[str, Any] = Field(default_factory=dict)


class VectorIndexConfig(BaseModel):
    """Configuration for vector indices in Neo4j"""
    name: str
    dimension: int
    similarity_function: str = "cosine"  # cosine, euclidean, dot_product
    m: int = 16  # HNSW parameter
    ef_construction: int = 200  # HNSW parameter
    ef_search: int = 100  # HNSW parameter
    embedding_property: str = "embedding"
    embedding_config: EmbeddingConfig


class SchemaDefinition(BaseModel):
    """Base schema definition"""
    schema_id: str
    schema_type: SchemaType
    name: str
    version: str = "1.0.0"
    description: Optional[str] = None
    properties: List[Dict[str, Any]] = Field(default_factory=list)
    constraints: List[Dict[str, Any]] = Field(default_factory=list)
    indices: List[Dict[str, Any]] = Field(default_factory=list)
    vector_indices: List[VectorIndexConfig] = Field(default_factory=list)
    validation_rules: List[Dict[str, Any]] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)

    # Change tracking
    content_hash: Optional[str] = None
    code_hash: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class NodeSchema(SchemaDefinition):
    """Schema for graph nodes"""
    labels: List[str] = Field(default_factory=list)
    embedding_fields: List[str] = Field(default_factory=list)  # Fields to include in embedding

    def __init__(self, **data):
        data['schema_type'] = SchemaType.NODE
        super().__init__(**data)


class RelationshipSchema(SchemaDefinition):
    """Schema for graph relationships"""
    relationship_type: str
    start_node_labels: List[str] = Field(default_factory=list)
    end_node_labels: List[str] = Field(default_factory=list)
    cardinality: str = "many_to_many"  # one_to_one, one_to_many, many_to_many

    def __init__(self, **data):
        data['schema_type'] = SchemaType.RELATIONSHIP
        super().__init__(**data)


class SubgraphSchema(SchemaDefinition):
    """Schema for hierarchical subgraph structures"""
    root_node_schema: str  # Reference to NodeSchema ID
    child_schemas: List[Dict[str, Any]] = Field(default_factory=list)
    relationships: List[Dict[str, Any]] = Field(default_factory=list)
    traversal_patterns: List[str] = Field(default_factory=list)
    aggregation_rules: List[Dict[str, Any]] = Field(default_factory=list)

    def __init__(self, **data):
        data['schema_type'] = SchemaType.SUBGRAPH
        super().__init__(**data)


# ==================== Embedding Callback System ====================

class EmbeddingCallback(ABC):
    """Abstract base class for embedding generation callbacks"""

    @abstractmethod
    async def generate(self, content: Union[str, Dict, List]) -> np.ndarray:
        """Generate embedding for content"""
        pass

    @abstractmethod
    def get_dimension(self) -> int:
        """Return embedding dimension"""
        pass

    @abstractmethod
    def get_model_info(self) -> Dict[str, Any]:
        """Return model information"""
        pass


class OpenAIEmbeddingCallback(EmbeddingCallback):
    """OpenAI embedding generation callback"""

    def __init__(self, model: str = "text-embedding-ada-002", api_key: Optional[str] = None):
        self.model = model
        self.api_key = api_key
        self.dimension = EmbeddingDimension.LARGE

    async def generate(self, content: Union[str, Dict, List]) -> np.ndarray:
        """Generate embedding using OpenAI API"""
        # Convert content to string if needed
        if isinstance(content, (dict, list)):
            text = json.dumps(content)
        else:
            text = str(content)

        # In production, call OpenAI API here
        # For now, return mock embedding
        import hashlib
        seed = int(hashlib.md5(text.encode()).hexdigest()[:8], 16)
        np.random.seed(seed)
        return np.random.randn(self.dimension).astype(np.float32)

    def get_dimension(self) -> int:
        return self.dimension

    def get_model_info(self) -> Dict[str, Any]:
        return {"model": self.model, "dimension": self.dimension}


class LocalEmbeddingCallback(EmbeddingCallback):
    """Local model embedding generation callback"""

    def __init__(self, model_path: str, dimension: int = 768):
        self.model_path = model_path
        self.dimension = dimension
        # In production, load actual model here

    async def generate(self, content: Union[str, Dict, List]) -> np.ndarray:
        """Generate embedding using local model"""
        if isinstance(content, (dict, list)):
            text = json.dumps(content)
        else:
            text = str(content)

        # In production, use actual model
        # For now, return mock embedding
        seed = int(hashlib.md5(text.encode()).hexdigest()[:8], 16)
        np.random.seed(seed)
        return np.random.randn(self.dimension).astype(np.float32)

    def get_dimension(self) -> int:
        return self.dimension

    def get_model_info(self) -> Dict[str, Any]:
        return {"model_path": self.model_path, "dimension": self.dimension}


class CompositeEmbeddingCallback(EmbeddingCallback):
    """Composite embedding from multiple sources"""

    def __init__(self, callbacks: List[EmbeddingCallback], strategy: str = "concatenate"):
        self.callbacks = callbacks
        self.strategy = strategy  # concatenate, average, weighted

        if strategy == "concatenate":
            self.dimension = sum(cb.get_dimension() for cb in callbacks)
        else:
            # For average/weighted, all must have same dimension
            dims = [cb.get_dimension() for cb in callbacks]
            assert len(set(dims)) == 1, "All callbacks must have same dimension for averaging"
            self.dimension = dims[0]

    async def generate(self, content: Union[str, Dict, List]) -> np.ndarray:
        """Generate composite embedding"""
        embeddings = await asyncio.gather(
            *[cb.generate(content) for cb in self.callbacks]
        )

        if self.strategy == "concatenate":
            return np.concatenate(embeddings)
        elif self.strategy == "average":
            return np.mean(embeddings, axis=0)
        else:
            raise ValueError(f"Unknown strategy: {self.strategy}")

    def get_dimension(self) -> int:
        return self.dimension

    def get_model_info(self) -> Dict[str, Any]:
        return {
            "type": "composite",
            "strategy": self.strategy,
            "models": [cb.get_model_info() for cb in self.callbacks]
        }


# ==================== Embedding Manager ====================

class EmbeddingManager:
    """Manages embedding callbacks and generation"""

    def __init__(self):
        self.callbacks: Dict[str, EmbeddingCallback] = {}
        self.embedding_cache: Dict[str, np.ndarray] = {}
        self.cache_ttl = timedelta(hours=24)
        self.cache_timestamps: Dict[str, datetime] = {}

    def register_callback(self, callback_id: str, callback: EmbeddingCallback):
        """Register an embedding callback"""
        self.callbacks[callback_id] = callback
        logger.info(f"Registered embedding callback: {callback_id}")

    async def generate_embedding(
        self,
        callback_id: str,
        content: Union[str, Dict, List],
        use_cache: bool = True
    ) -> np.ndarray:
        """Generate embedding using specified callback"""
        if callback_id not in self.callbacks:
            raise ValueError(f"Unknown callback: {callback_id}")

        # Create cache key
        content_str = json.dumps(content) if isinstance(content, (dict, list)) else str(content)
        cache_key = f"{callback_id}:{hashlib.md5(content_str.encode()).hexdigest()}"

        # Check cache
        if use_cache and cache_key in self.embedding_cache:
            cached_time = self.cache_timestamps.get(cache_key)
            if cached_time and datetime.utcnow() - cached_time < self.cache_ttl:
                logger.debug(f"Using cached embedding for {cache_key[:20]}...")
                return self.embedding_cache[cache_key]

        # Generate new embedding
        callback = self.callbacks[callback_id]
        embedding = await callback.generate(content)

        # Cache it
        if use_cache:
            self.embedding_cache[cache_key] = embedding
            self.cache_timestamps[cache_key] = datetime.utcnow()

        return embedding

    async def batch_generate(
        self,
        callback_id: str,
        contents: List[Union[str, Dict, List]],
        batch_size: int = 100
    ) -> List[np.ndarray]:
        """Generate embeddings in batches"""
        embeddings = []

        for i in range(0, len(contents), batch_size):
            batch = contents[i:i + batch_size]
            batch_embeddings = await asyncio.gather(
                *[self.generate_embedding(callback_id, content) for content in batch]
            )
            embeddings.extend(batch_embeddings)

        return embeddings

    def clear_cache(self, older_than: Optional[timedelta] = None):
        """Clear embedding cache"""
        if older_than:
            cutoff = datetime.utcnow() - older_than
            keys_to_remove = [
                key for key, timestamp in self.cache_timestamps.items()
                if timestamp < cutoff
            ]
            for key in keys_to_remove:
                del self.embedding_cache[key]
                del self.cache_timestamps[key]
            logger.info(f"Cleared {len(keys_to_remove)} cached embeddings")
        else:
            self.embedding_cache.clear()
            self.cache_timestamps.clear()
            logger.info("Cleared all cached embeddings")


# ==================== Change Detection System ====================

class ChangeDetector:
    """Detects changes in schemas and code for smart redeployment"""

    def __init__(self):
        self.schema_hashes: Dict[str, str] = {}
        self.code_hashes: Dict[str, str] = {}
        self.dependency_graph: Dict[str, Set[str]] = defaultdict(set)

    def compute_schema_hash(self, schema: SchemaDefinition) -> str:
        """Compute hash of schema definition"""
        # Serialize schema without timestamps
        schema_dict = schema.model_dump(exclude={'created_at', 'updated_at', 'content_hash', 'code_hash'})
        schema_str = json.dumps(schema_dict, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()

    def compute_code_hash(self, code: str) -> str:
        """Compute hash of code"""
        # Remove comments and normalize whitespace
        normalized = '\n'.join(line.strip() for line in code.split('\n') if line.strip() and not line.strip().startswith('#'))
        return hashlib.sha256(normalized.encode()).hexdigest()

    def has_schema_changed(self, schema_id: str, new_schema: SchemaDefinition) -> bool:
        """Check if schema has changed"""
        new_hash = self.compute_schema_hash(new_schema)
        old_hash = self.schema_hashes.get(schema_id)

        if old_hash is None:
            self.schema_hashes[schema_id] = new_hash
            return True

        changed = old_hash != new_hash
        if changed:
            self.schema_hashes[schema_id] = new_hash

        return changed

    def has_code_changed(self, function_id: str, code: str) -> bool:
        """Check if code has changed"""
        new_hash = self.compute_code_hash(code)
        old_hash = self.code_hashes.get(function_id)

        if old_hash is None:
            self.code_hashes[function_id] = new_hash
            return True

        changed = old_hash != new_hash
        if changed:
            self.code_hashes[function_id] = new_hash

        return changed

    def add_dependency(self, schema_id: str, depends_on: str):
        """Add schema dependency"""
        self.dependency_graph[schema_id].add(depends_on)

    def get_affected_schemas(self, changed_schema_id: str) -> Set[str]:
        """Get all schemas affected by a change"""
        affected = {changed_schema_id}

        # Find all schemas that depend on the changed one
        for schema_id, deps in self.dependency_graph.items():
            if changed_schema_id in deps:
                affected.add(schema_id)
                # Recursively add dependents
                affected.update(self.get_affected_schemas(schema_id))

        return affected


# ==================== Autonomous Agent Infrastructure ====================

class AgentCapability(str, Enum):
    """Types of agent capabilities"""
    REASONING = "reasoning"
    TOOL_USE = "tool_use"
    MEMORY = "memory"
    PLANNING = "planning"
    LEARNING = "learning"
    COORDINATION = "coordination"


class AgentState(str, Enum):
    """Agent lifecycle states"""
    INITIALIZING = "initializing"
    READY = "ready"
    WORKING = "working"
    WAITING = "waiting"
    SUSPENDED = "suspended"
    ERROR = "error"
    TERMINATED = "terminated"


@dataclass
class AgentSchema:
    """Schema for autonomous agents in the graph"""
    agent_id: str
    name: str
    type: str  # orchestrator, worker, specialist
    capabilities: List[AgentCapability]
    model: str  # LLM model
    temperature: float = 0.7
    max_tokens: int = 4096
    tools: List[str] = field(default_factory=list)
    memory_schema: Optional[str] = None
    coordination_schema: Optional[str] = None
    embedding_config: Optional[EmbeddingConfig] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConversationSchema:
    """Schema for conversation hierarchies"""
    conversation_id: str
    root_schema: NodeSchema  # Conversation node
    message_schema: NodeSchema  # Message nodes
    content_block_schema: NodeSchema  # Content blocks within messages
    relationships: List[RelationshipSchema]

    def to_subgraph_schema(self) -> SubgraphSchema:
        """Convert to subgraph schema"""
        return SubgraphSchema(
            schema_id=f"conversation_hierarchy_{self.conversation_id}",
            name="ConversationHierarchy",
            root_node_schema=self.root_schema.schema_id,
            child_schemas=[
                {"schema_id": self.message_schema.schema_id, "level": 1},
                {"schema_id": self.content_block_schema.schema_id, "level": 2}
            ],
            relationships=[rel.model_dump() for rel in self.relationships],
            traversal_patterns=[
                "(conv:Conversation)-[:HAS_MESSAGE]->(msg:Message)",
                "(msg:Message)-[:HAS_CONTENT]->(block:ContentBlock)"
            ]
        )


class AgentOrchestrator:
    """Orchestrates autonomous agents in the graph"""

    def __init__(self, graph_client, embedding_manager: EmbeddingManager):
        self.graph_client = graph_client
        self.embedding_manager = embedding_manager
        self.agents: Dict[str, AgentSchema] = {}
        self.agent_states: Dict[str, AgentState] = {}
        self.conversation_schemas: Dict[str, ConversationSchema] = {}

    async def register_agent(self, agent: AgentSchema) -> str:
        """Register an autonomous agent in the graph"""
        # Create agent node schema
        agent_node_schema = NodeSchema(
            schema_id=f"agent_{agent.agent_id}",
            name=f"Agent_{agent.name}",
            labels=["Agent", agent.type.title()],
            properties=[
                {"name": "agent_id", "type": "string", "required": True, "unique": True},
                {"name": "name", "type": "string", "required": True},
                {"name": "type", "type": "string", "required": True},
                {"name": "model", "type": "string", "required": True},
                {"name": "state", "type": "string", "required": True},
                {"name": "capabilities", "type": "array"},
                {"name": "temperature", "type": "float"},
                {"name": "max_tokens", "type": "integer"},
                {"name": "created_at", "type": "datetime"},
                {"name": "last_active", "type": "datetime"},
                {"name": "embedding", "type": "array"}
            ],
            embedding_fields=["name", "type", "capabilities"],
            vector_indices=[
                VectorIndexConfig(
                    name=f"agent_{agent.agent_id}_vector_idx",
                    dimension=1536,
                    embedding_config=agent.embedding_config or EmbeddingConfig(
                        callback_id="openai_default"
                    )
                )
            ] if agent.embedding_config else []
        )

        # Register in graph
        await self.graph_client.register_schema(agent_node_schema)

        # Create agent instance
        agent_data = {
            "agent_id": agent.agent_id,
            "name": agent.name,
            "type": agent.type,
            "model": agent.model,
            "state": AgentState.INITIALIZING.value,
            "capabilities": [cap.value for cap in agent.capabilities],
            "temperature": agent.temperature,
            "max_tokens": agent.max_tokens,
            "created_at": datetime.utcnow().isoformat(),
            "last_active": datetime.utcnow().isoformat()
        }

        # Generate embedding if configured
        if agent.embedding_config:
            embedding = await self.embedding_manager.generate_embedding(
                agent.embedding_config.callback_id,
                {
                    "name": agent.name,
                    "type": agent.type,
                    "capabilities": [cap.value for cap in agent.capabilities]
                }
            )
            agent_data["embedding"] = embedding.tolist()

        # Create agent node
        await self.graph_client.execute_query(
            """
            CREATE (a:Agent:$type {$properties})
            RETURN a.agent_id as agent_id
            """,
            {
                "type": agent.type.title(),
                "properties": agent_data
            }
        )

        # Store agent
        self.agents[agent.agent_id] = agent
        self.agent_states[agent.agent_id] = AgentState.READY

        logger.info(f"Registered agent: {agent.name} ({agent.agent_id})")
        return agent.agent_id

    async def register_conversation_hierarchy(
        self,
        conversation_id: str,
        embedding_config: Optional[EmbeddingConfig] = None
    ) -> ConversationSchema:
        """Register a conversation hierarchy schema"""

        # Define Conversation node schema
        conversation_schema = NodeSchema(
            schema_id=f"conversation_{conversation_id}",
            name="Conversation",
            labels=["Conversation"],
            properties=[
                {"name": "conversation_id", "type": "string", "required": True, "unique": True},
                {"name": "title", "type": "string"},
                {"name": "created_at", "type": "datetime"},
                {"name": "updated_at", "type": "datetime"},
                {"name": "agent_ids", "type": "array"},
                {"name": "status", "type": "string"},
                {"name": "metadata", "type": "object"},
                {"name": "summary_embedding", "type": "array"}
            ],
            embedding_fields=["title"],
            vector_indices=[
                VectorIndexConfig(
                    name=f"conversation_{conversation_id}_vector_idx",
                    dimension=1536,
                    embedding_property="summary_embedding",
                    embedding_config=embedding_config or EmbeddingConfig(
                        callback_id="openai_default"
                    )
                )
            ] if embedding_config else []
        )

        # Define Message node schema
        message_schema = NodeSchema(
            schema_id=f"message_{conversation_id}",
            name="Message",
            labels=["Message"],
            properties=[
                {"name": "message_id", "type": "string", "required": True, "unique": True},
                {"name": "conversation_id", "type": "string", "required": True, "indexed": True},
                {"name": "role", "type": "string", "required": True},
                {"name": "content", "type": "string"},
                {"name": "timestamp", "type": "datetime"},
                {"name": "agent_id", "type": "string"},
                {"name": "tokens", "type": "integer"},
                {"name": "embedding", "type": "array"}
            ],
            embedding_fields=["content"],
            vector_indices=[
                VectorIndexConfig(
                    name=f"message_{conversation_id}_vector_idx",
                    dimension=1536,
                    embedding_config=embedding_config or EmbeddingConfig(
                        callback_id="openai_default",
                        update_trigger=["content"]
                    )
                )
            ] if embedding_config else []
        )

        # Define ContentBlock schema
        content_block_schema = NodeSchema(
            schema_id=f"content_block_{conversation_id}",
            name="ContentBlock",
            labels=["ContentBlock"],
            properties=[
                {"name": "block_id", "type": "string", "required": True, "unique": True},
                {"name": "message_id", "type": "string", "required": True, "indexed": True},
                {"name": "block_type", "type": "string", "required": True},
                {"name": "content", "type": "object"},
                {"name": "order", "type": "integer"},
                {"name": "metadata", "type": "object"}
            ]
        )

        # Define relationships
        has_message = RelationshipSchema(
            schema_id=f"has_message_{conversation_id}",
            name="HAS_MESSAGE",
            relationship_type="HAS_MESSAGE",
            start_node_labels=["Conversation"],
            end_node_labels=["Message"],
            cardinality="one_to_many",
            properties=[
                {"name": "order", "type": "integer"}
            ]
        )

        has_content = RelationshipSchema(
            schema_id=f"has_content_{conversation_id}",
            name="HAS_CONTENT",
            relationship_type="HAS_CONTENT",
            start_node_labels=["Message"],
            end_node_labels=["ContentBlock"],
            cardinality="one_to_many",
            properties=[
                {"name": "order", "type": "integer"}
            ]
        )

        follows = RelationshipSchema(
            schema_id=f"follows_{conversation_id}",
            name="FOLLOWS",
            relationship_type="FOLLOWS",
            start_node_labels=["Message"],
            end_node_labels=["Message"],
            cardinality="one_to_one"
        )

        # Create conversation schema object
        conv_schema = ConversationSchema(
            conversation_id=conversation_id,
            root_schema=conversation_schema,
            message_schema=message_schema,
            content_block_schema=content_block_schema,
            relationships=[has_message, has_content, follows]
        )

        # Register all schemas in graph
        for schema in [conversation_schema, message_schema, content_block_schema]:
            await self.graph_client.register_schema(schema)

        for rel_schema in conv_schema.relationships:
            await self.graph_client.register_relationship_schema(rel_schema)

        # Register as subgraph
        subgraph = conv_schema.to_subgraph_schema()
        await self.graph_client.register_subgraph_schema(subgraph)

        # Store schema
        self.conversation_schemas[conversation_id] = conv_schema

        logger.info(f"Registered conversation hierarchy: {conversation_id}")
        return conv_schema

    async def create_agent_coordination(
        self,
        coordinator_id: str,
        worker_ids: List[str],
        coordination_type: str = "hierarchical"  # hierarchical, peer_to_peer, consensus
    ):
        """Create coordination structure between agents"""

        # Define coordination relationship
        coordinates = RelationshipSchema(
            schema_id=f"coordinates_{coordinator_id}",
            name="COORDINATES",
            relationship_type="COORDINATES",
            start_node_labels=["Agent", "Orchestrator"],
            end_node_labels=["Agent"],
            cardinality="one_to_many",
            properties=[
                {"name": "coordination_type", "type": "string"},
                {"name": "priority", "type": "integer"},
                {"name": "assigned_at", "type": "datetime"}
            ]
        )

        await self.graph_client.register_relationship_schema(coordinates)

        # Create coordination relationships
        for i, worker_id in enumerate(worker_ids):
            await self.graph_client.execute_query(
                """
                MATCH (coordinator:Agent {agent_id: $coordinator_id})
                MATCH (worker:Agent {agent_id: $worker_id})
                CREATE (coordinator)-[:COORDINATES {
                    coordination_type: $type,
                    priority: $priority,
                    assigned_at: datetime()
                }]->(worker)
                """,
                {
                    "coordinator_id": coordinator_id,
                    "worker_id": worker_id,
                    "type": coordination_type,
                    "priority": i
                }
            )

        logger.info(f"Created {coordination_type} coordination: {coordinator_id} -> {worker_ids}")

    async def find_similar_agents(
        self,
        capabilities: List[str],
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """Find agents with similar capabilities using vector search"""

        # Generate embedding for search
        search_embedding = await self.embedding_manager.generate_embedding(
            "openai_default",
            {"capabilities": capabilities}
        )

        # Vector similarity search in Neo4j
        query = """
        MATCH (a:Agent)
        WHERE a.embedding IS NOT NULL
        WITH a, gds.similarity.cosine(a.embedding, $search_embedding) AS similarity
        WHERE similarity > 0.7
        RETURN a.agent_id as agent_id,
               a.name as name,
               a.capabilities as capabilities,
               similarity
        ORDER BY similarity DESC
        LIMIT $limit
        """

        result = await self.graph_client.execute_query(
            query,
            {
                "search_embedding": search_embedding.tolist(),
                "limit": limit
            }
        )

        return result.get("results", [])


# ==================== Advanced Schema Registry ====================

class AdvancedSchemaRegistry:
    """Advanced schema registry with embedding support and change detection"""

    def __init__(self, graph_client, embedding_manager: EmbeddingManager):
        self.graph_client = graph_client
        self.embedding_manager = embedding_manager
        self.change_detector = ChangeDetector()
        self.schemas: Dict[str, SchemaDefinition] = {}
        self.vector_indices: Dict[str, VectorIndexConfig] = {}

    async def register_schema_with_embedding(
        self,
        schema: SchemaDefinition,
        skip_if_unchanged: bool = True
    ) -> Dict[str, Any]:
        """Register schema with embedding support and change detection"""

        # Check for changes
        if skip_if_unchanged and not self.change_detector.has_schema_changed(schema.schema_id, schema):
            logger.info(f"Schema {schema.schema_id} unchanged, skipping registration")
            return {"status": "unchanged", "schema_id": schema.schema_id}

        # Process vector indices
        for vector_config in schema.vector_indices:
            await self._create_vector_index(schema, vector_config)

        # Generate schema embedding if configured
        if schema.vector_indices:
            # Use first vector config for schema's own embedding
            config = schema.vector_indices[0].embedding_config
            schema_content = {
                "name": schema.name,
                "description": schema.description,
                "properties": [p["name"] for p in schema.properties]
            }

            embedding = await self.embedding_manager.generate_embedding(
                config.callback_id,
                schema_content
            )

            schema.metadata["schema_embedding"] = embedding.tolist()

        # Register based on type
        if schema.schema_type == SchemaType.NODE:
            result = await self._register_node_schema(schema)
        elif schema.schema_type == SchemaType.RELATIONSHIP:
            result = await self._register_relationship_schema(schema)
        elif schema.schema_type == SchemaType.SUBGRAPH:
            result = await self._register_subgraph_schema(schema)
        else:
            raise ValueError(f"Unsupported schema type: {schema.schema_type}")

        # Store schema
        self.schemas[schema.schema_id] = schema

        # Track dependencies
        if schema.schema_type == SchemaType.SUBGRAPH:
            subgraph = schema
            for child in subgraph.child_schemas:
                self.change_detector.add_dependency(
                    schema.schema_id,
                    child["schema_id"]
                )

        return result

    async def _create_vector_index(
        self,
        schema: SchemaDefinition,
        config: VectorIndexConfig
    ):
        """Create vector index in Neo4j"""

        # Determine label based on schema type
        if schema.schema_type == SchemaType.NODE:
            label = schema.labels[0] if schema.labels else schema.name
        else:
            label = schema.name

        # Create vector index
        index_query = f"""
        CREATE VECTOR INDEX {config.name} IF NOT EXISTS
        FOR (n:{label})
        ON n.{config.embedding_property}
        OPTIONS {{
            indexConfig: {{
                `vector.dimensions`: {config.dimension},
                `vector.similarity_function`: '{config.similarity_function}'
            }}
        }}
        """

        await self.graph_client.execute_query(index_query)

        # Store configuration
        self.vector_indices[config.name] = config

        # Register embedding callback if not exists
        if config.embedding_config.callback_id not in self.embedding_manager.callbacks:
            # Use default OpenAI callback
            self.embedding_manager.register_callback(
                config.embedding_config.callback_id,
                OpenAIEmbeddingCallback()
            )

        logger.info(f"Created vector index: {config.name}")

    async def _register_node_schema(self, schema: NodeSchema) -> Dict[str, Any]:
        """Register node schema in graph"""

        query = """
        MERGE (s:Schema:NodeSchema {schema_id: $schema_id})
        SET s += $properties
        RETURN s
        """

        properties = schema.model_dump(exclude={'schema_id'})
        properties['content_hash'] = self.change_detector.compute_schema_hash(schema)

        result = await self.graph_client.execute_query(
            query,
            {"schema_id": schema.schema_id, "properties": properties}
        )

        return {"status": "registered", "type": "node", "schema_id": schema.schema_id}

    async def _register_relationship_schema(self, schema: RelationshipSchema) -> Dict[str, Any]:
        """Register relationship schema in graph"""

        query = """
        MERGE (s:Schema:RelationshipSchema {schema_id: $schema_id})
        SET s += $properties
        RETURN s
        """

        properties = schema.model_dump(exclude={'schema_id'})
        properties['content_hash'] = self.change_detector.compute_schema_hash(schema)

        result = await self.graph_client.execute_query(
            query,
            {"schema_id": schema.schema_id, "properties": properties}
        )

        return {"status": "registered", "type": "relationship", "schema_id": schema.schema_id}

    async def _register_subgraph_schema(self, schema: SubgraphSchema) -> Dict[str, Any]:
        """Register subgraph schema in graph"""

        query = """
        MERGE (s:Schema:SubgraphSchema {schema_id: $schema_id})
        SET s += $properties

        // Link to root node schema
        WITH s
        MATCH (root:Schema:NodeSchema {schema_id: $root_schema_id})
        MERGE (s)-[:HAS_ROOT]->(root)

        RETURN s
        """

        properties = schema.model_dump(exclude={'schema_id', 'root_node_schema'})
        properties['content_hash'] = self.change_detector.compute_schema_hash(schema)

        result = await self.graph_client.execute_query(
            query,
            {
                "schema_id": schema.schema_id,
                "properties": properties,
                "root_schema_id": schema.root_node_schema
            }
        )

        # Link child schemas
        for child in schema.child_schemas:
            await self.graph_client.execute_query(
                """
                MATCH (s:SubgraphSchema {schema_id: $subgraph_id})
                MATCH (child:Schema {schema_id: $child_id})
                MERGE (s)-[:CONTAINS {level: $level}]->(child)
                """,
                {
                    "subgraph_id": schema.schema_id,
                    "child_id": child["schema_id"],
                    "level": child.get("level", 0)
                }
            )

        return {"status": "registered", "type": "subgraph", "schema_id": schema.schema_id}

    async def update_embeddings_if_changed(
        self,
        schema_id: str,
        data: Dict[str, Any]
    ) -> bool:
        """Update embeddings only if relevant fields changed"""

        schema = self.schemas.get(schema_id)
        if not schema or not schema.vector_indices:
            return False

        # Check each vector index configuration
        embeddings_updated = False

        for vector_config in schema.vector_indices:
            config = vector_config.embedding_config

            # Check if any trigger fields changed
            if config.update_trigger:
                changed_fields = set(data.keys()) & set(config.update_trigger)
                if not changed_fields:
                    continue

            # Generate new embedding
            if schema.schema_type == SchemaType.NODE and hasattr(schema, 'embedding_fields'):
                # Extract fields for embedding
                embedding_data = {
                    field: data.get(field)
                    for field in schema.embedding_fields
                    if field in data
                }

                if embedding_data:
                    new_embedding = await self.embedding_manager.generate_embedding(
                        config.callback_id,
                        embedding_data
                    )

                    # Update in graph
                    await self.graph_client.execute_query(
                        f"""
                        MATCH (n {{schema_id: $schema_id}})
                        SET n.{vector_config.embedding_property} = $embedding
                        """,
                        {
                            "schema_id": schema_id,
                            "embedding": new_embedding.tolist()
                        }
                    )

                    embeddings_updated = True
                    logger.info(f"Updated embedding for {schema_id}")

        return embeddings_updated


# ==================== Integration Example ====================

async def setup_autonomous_agent_infrastructure():
    """Example setup of autonomous agent infrastructure"""

    # Initialize components
    from graph_management_client import GraphManagementClient

    graph_client = GraphManagementClient(base_url="http://localhost:2014")
    embedding_manager = EmbeddingManager()

    # Register embedding callbacks
    embedding_manager.register_callback(
        "openai_default",
        OpenAIEmbeddingCallback()
    )

    embedding_manager.register_callback(
        "local_bert",
        LocalEmbeddingCallback(model_path="/models/bert", dimension=768)
    )

    embedding_manager.register_callback(
        "composite_multi",
        CompositeEmbeddingCallback(
            [
                OpenAIEmbeddingCallback(),
                LocalEmbeddingCallback(model_path="/models/bert")
            ],
            strategy="concatenate"
        )
    )

    # Initialize registry and orchestrator
    registry = AdvancedSchemaRegistry(graph_client, embedding_manager)
    orchestrator = AgentOrchestrator(graph_client, embedding_manager)

    # Register conversation hierarchy schema
    conversation_schema = await orchestrator.register_conversation_hierarchy(
        conversation_id="conv_001",
        embedding_config=EmbeddingConfig(
            callback_id="openai_default",
            dimension=1536,
            cache_embeddings=True,
            update_trigger=["content", "title"]
        )
    )

    # Register autonomous agents
    orchestrator_agent = AgentSchema(
        agent_id="orchestrator_001",
        name="MainOrchestrator",
        type="orchestrator",
        capabilities=[
            AgentCapability.PLANNING,
            AgentCapability.COORDINATION,
            AgentCapability.REASONING
        ],
        model="gpt-4",
        temperature=0.7,
        embedding_config=EmbeddingConfig(
            callback_id="openai_default",
            dimension=1536
        )
    )

    await orchestrator.register_agent(orchestrator_agent)

    # Register worker agents
    for i in range(3):
        worker_agent = AgentSchema(
            agent_id=f"worker_{i:03d}",
            name=f"Worker{i}",
            type="worker",
            capabilities=[
                AgentCapability.TOOL_USE,
                AgentCapability.MEMORY
            ],
            model="gpt-3.5-turbo",
            temperature=0.5
        )
        await orchestrator.register_agent(worker_agent)

    # Create coordination
    await orchestrator.create_agent_coordination(
        coordinator_id="orchestrator_001",
        worker_ids=[f"worker_{i:03d}" for i in range(3)],
        coordination_type="hierarchical"
    )

    # Register tool schema with embedding
    tool_schema = NodeSchema(
        schema_id="autonomous_tool_v1",
        name="AutonomousTool",
        labels=["Tool", "Autonomous"],
        properties=[
            {"name": "tool_id", "type": "string", "required": True, "unique": True},
            {"name": "name", "type": "string", "required": True},
            {"name": "description", "type": "string"},
            {"name": "capabilities", "type": "array"},
            {"name": "embedding", "type": "array"}
        ],
        embedding_fields=["name", "description", "capabilities"],
        vector_indices=[
            VectorIndexConfig(
                name="autonomous_tool_vector_idx",
                dimension=1536,
                similarity_function="cosine",
                embedding_config=EmbeddingConfig(
                    callback_id="openai_default",
                    cache_embeddings=True,
                    update_trigger=["name", "description", "capabilities"]
                )
            )
        ]
    )

    # Register with change detection
    result = await registry.register_schema_with_embedding(
        tool_schema,
        skip_if_unchanged=True
    )

    print(f"Schema registration result: {result}")

    # Find similar agents
    similar = await orchestrator.find_similar_agents(
        capabilities=["planning", "reasoning"],
        limit=5
    )
    print(f"Found {len(similar)} similar agents")

    return registry, orchestrator, embedding_manager


if __name__ == "__main__":
    import asyncio

    # Run setup
    asyncio.run(setup_autonomous_agent_infrastructure())