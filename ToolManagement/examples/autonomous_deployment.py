"""
Example: Complete Autonomous Agent Deployment with Advanced Schema Management

This example demonstrates:
1. Setting up the infrastructure
2. Registering schemas with embeddings
3. Deploying autonomous agents
4. Creating coordination patterns
5. Smart redeployment on changes
"""

import asyncio
from datetime import datetime
import json
from typing import List, Dict, Any

# Import our infrastructure components
from autonomous_agent_infrastructure import (
    EmbeddingManager,
    OpenAIEmbeddingCallback,
    LocalEmbeddingCallback,
    CompositeEmbeddingCallback,
    AdvancedSchemaRegistry,
    AgentOrchestrator,
    ChangeDetector,
    NodeSchema,
    RelationshipSchema,
    SubgraphSchema,
    VectorIndexConfig,
    EmbeddingConfig,
    AgentSchema,
    AgentCapability,
    ConversationSchema,
    EmbeddingDimension
)

from graph_management_client import GraphManagementClient


# ==================== Setup Infrastructure ====================

async def setup_infrastructure():
    """Initialize all infrastructure components"""

    print("🚀 Setting up Autonomous Agent Infrastructure...")

    # Initialize Graph Management Client
    graph_client = GraphManagementClient(base_url="http://localhost:2014")

    # Initialize Embedding Manager with multiple callbacks
    embedding_manager = EmbeddingManager()

    # Register OpenAI callback
    embedding_manager.register_callback(
        "openai_ada",
        OpenAIEmbeddingCallback(model="text-embedding-ada-002")
    )

    # Register local model callback
    embedding_manager.register_callback(
        "local_minilm",
        LocalEmbeddingCallback(
            model_path="/models/all-MiniLM-L6-v2",
            dimension=384
        )
    )

    # Register composite callback for multi-modal understanding
    embedding_manager.register_callback(
        "hybrid_embedding",
        CompositeEmbeddingCallback(
            callbacks=[
                OpenAIEmbeddingCallback(),
                LocalEmbeddingCallback(model_path="/models/bert", dimension=768)
            ],
            strategy="concatenate"
        )
    )

    # Initialize Schema Registry with change detection
    schema_registry = AdvancedSchemaRegistry(graph_client, embedding_manager)

    # Initialize Agent Orchestrator
    agent_orchestrator = AgentOrchestrator(graph_client, embedding_manager)

    print("✅ Infrastructure initialized successfully")

    return {
        "graph_client": graph_client,
        "embedding_manager": embedding_manager,
        "schema_registry": schema_registry,
        "agent_orchestrator": agent_orchestrator
    }


# ==================== Register Advanced Schemas ====================

async def register_tool_schemas(registry: AdvancedSchemaRegistry):
    """Register tool schemas with embedding support"""

    print("\n📝 Registering Tool Schemas...")

    # Define Tool Schema with vector index
    tool_schema = NodeSchema(
        schema_id="tool_v2",
        name="Tool",
        version="2.0.0",
        description="Advanced tool with semantic understanding",
        labels=["Tool", "Autonomous"],
        properties=[
            {"name": "tool_id", "type": "string", "required": True, "unique": True},
            {"name": "name", "type": "string", "required": True, "indexed": True},
            {"name": "description", "type": "string"},
            {"name": "category", "type": "string", "indexed": True},
            {"name": "capabilities", "type": "array"},
            {"name": "usage_count", "type": "integer", "default": 0},
            {"name": "performance_metrics", "type": "object"},
            {"name": "embedding", "type": "array"},
            {"name": "capability_embedding", "type": "array"}
        ],
        embedding_fields=["name", "description", "capabilities"],
        vector_indices=[
            VectorIndexConfig(
                name="tool_semantic_idx",
                dimension=1536,
                similarity_function="cosine",
                embedding_property="embedding",
                embedding_config=EmbeddingConfig(
                    callback_id="openai_ada",
                    dimension=1536,
                    cache_embeddings=True,
                    update_trigger=["description", "capabilities"]
                )
            ),
            VectorIndexConfig(
                name="tool_capability_idx",
                dimension=384,
                similarity_function="cosine",
                embedding_property="capability_embedding",
                embedding_config=EmbeddingConfig(
                    callback_id="local_minilm",
                    dimension=384,
                    cache_embeddings=True,
                    update_trigger=["capabilities"]
                )
            )
        ]
    )

    # Register with change detection
    result = await registry.register_schema_with_embedding(
        tool_schema,
        skip_if_unchanged=True
    )

    print(f"  ✓ Tool schema registered: {result}")

    # Define ToolExecution relationship schema
    execution_schema = RelationshipSchema(
        schema_id="tool_execution_v2",
        name="EXECUTED",
        relationship_type="EXECUTED",
        start_node_labels=["Agent"],
        end_node_labels=["Tool"],
        cardinality="many_to_many",
        properties=[
            {"name": "execution_id", "type": "string", "required": True},
            {"name": "timestamp", "type": "datetime", "required": True},
            {"name": "duration_ms", "type": "integer"},
            {"name": "success", "type": "boolean"},
            {"name": "input_size", "type": "integer"},
            {"name": "output_size", "type": "integer"},
            {"name": "error", "type": "string"}
        ]
    )

    await registry.register_schema_with_embedding(
        execution_schema,
        skip_if_unchanged=True
    )

    print("  ✓ Execution relationship schema registered")

    return tool_schema


# ==================== Deploy Autonomous Agents ====================

async def deploy_agent_team(orchestrator: AgentOrchestrator):
    """Deploy a team of autonomous agents with coordination"""

    print("\n🤖 Deploying Autonomous Agent Team...")

    # Deploy Master Orchestrator
    master = AgentSchema(
        agent_id="master_001",
        name="MasterOrchestrator",
        type="orchestrator",
        capabilities=[
            AgentCapability.PLANNING,
            AgentCapability.COORDINATION,
            AgentCapability.REASONING,
            AgentCapability.LEARNING
        ],
        model="gpt-4",
        temperature=0.7,
        max_tokens=8192,
        embedding_config=EmbeddingConfig(
            callback_id="openai_ada",
            dimension=1536
        )
    )

    master_id = await orchestrator.register_agent(master)
    print(f"  ✓ Master Orchestrator deployed: {master_id}")

    # Deploy Specialist Agents
    specialists = [
        AgentSchema(
            agent_id="data_specialist_001",
            name="DataSpecialist",
            type="specialist",
            capabilities=[
                AgentCapability.TOOL_USE,
                AgentCapability.MEMORY,
                AgentCapability.REASONING
            ],
            model="gpt-3.5-turbo",
            temperature=0.5,
            tools=["data_processor", "csv_reader", "json_parser"],
            embedding_config=EmbeddingConfig(
                callback_id="local_minilm",
                dimension=384
            )
        ),
        AgentSchema(
            agent_id="analysis_specialist_001",
            name="AnalysisSpecialist",
            type="specialist",
            capabilities=[
                AgentCapability.REASONING,
                AgentCapability.PLANNING,
                AgentCapability.MEMORY
            ],
            model="gpt-4",
            temperature=0.6,
            tools=["statistical_analyzer", "report_generator"],
            embedding_config=EmbeddingConfig(
                callback_id="openai_ada",
                dimension=1536
            )
        ),
        AgentSchema(
            agent_id="integration_specialist_001",
            name="IntegrationSpecialist",
            type="specialist",
            capabilities=[
                AgentCapability.TOOL_USE,
                AgentCapability.COORDINATION
            ],
            model="gpt-3.5-turbo",
            temperature=0.5,
            tools=["api_connector", "webhook_handler", "event_processor"]
        )
    ]

    specialist_ids = []
    for specialist in specialists:
        agent_id = await orchestrator.register_agent(specialist)
        specialist_ids.append(agent_id)
        print(f"  ✓ {specialist.name} deployed: {agent_id}")

    # Create Hierarchical Coordination
    await orchestrator.create_agent_coordination(
        coordinator_id=master_id,
        worker_ids=specialist_ids,
        coordination_type="hierarchical"
    )

    print(f"  ✓ Hierarchical coordination established")

    # Deploy Worker Pool
    worker_ids = []
    for i in range(5):
        worker = AgentSchema(
            agent_id=f"worker_{i:03d}",
            name=f"Worker{i}",
            type="worker",
            capabilities=[AgentCapability.TOOL_USE],
            model="gpt-3.5-turbo",
            temperature=0.3,
            max_tokens=2048
        )
        worker_id = await orchestrator.register_agent(worker)
        worker_ids.append(worker_id)

    print(f"  ✓ Worker pool deployed: {len(worker_ids)} workers")

    # Create Load-Balanced Coordination for workers
    await orchestrator.create_agent_coordination(
        coordinator_id=specialist_ids[0],  # Data specialist coordinates workers
        worker_ids=worker_ids,
        coordination_type="load_balanced"
    )

    return {
        "master": master_id,
        "specialists": specialist_ids,
        "workers": worker_ids
    }


# ==================== Register Conversation Hierarchy ====================

async def setup_conversation_tracking(orchestrator: AgentOrchestrator):
    """Set up conversation tracking with hierarchical schemas"""

    print("\n💬 Setting up Conversation Tracking...")

    # Register conversation hierarchy with embeddings
    conv_schema = await orchestrator.register_conversation_hierarchy(
        conversation_id="main_conversation",
        embedding_config=EmbeddingConfig(
            callback_id="openai_ada",
            dimension=1536,
            cache_embeddings=True,
            batch_size=50,
            update_trigger=["content", "title"]
        )
    )

    print(f"  ✓ Conversation hierarchy registered")
    print(f"    - Root: {conv_schema.root_schema.name}")
    print(f"    - Message schema: {conv_schema.message_schema.name}")
    print(f"    - Content block schema: {conv_schema.content_block_schema.name}")

    # Create sample conversation
    conv_id = "conv_" + datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    await orchestrator.graph_client.execute_query(
        """
        CREATE (c:Conversation {
            conversation_id: $conv_id,
            title: $title,
            created_at: datetime(),
            status: 'active',
            agent_ids: $agent_ids
        })
        """,
        {
            "conv_id": conv_id,
            "title": "Autonomous Agent Coordination Session",
            "agent_ids": ["master_001", "data_specialist_001"]
        }
    )

    print(f"  ✓ Sample conversation created: {conv_id}")

    return conv_schema


# ==================== Demonstrate Smart Redeployment ====================

async def demonstrate_smart_redeployment(
    registry: AdvancedSchemaRegistry,
    embedding_manager: EmbeddingManager
):
    """Demonstrate smart redeployment with change detection"""

    print("\n🔄 Demonstrating Smart Redeployment...")

    # Simulate schema update
    updated_tool_schema = NodeSchema(
        schema_id="tool_v2",
        name="Tool",
        version="2.1.0",  # Version bump
        description="Advanced tool with semantic understanding and monitoring",
        labels=["Tool", "Autonomous"],
        properties=[
            {"name": "tool_id", "type": "string", "required": True, "unique": True},
            {"name": "name", "type": "string", "required": True, "indexed": True},
            {"name": "description", "type": "string"},
            {"name": "category", "type": "string", "indexed": True},
            {"name": "capabilities", "type": "array"},
            {"name": "usage_count", "type": "integer", "default": 0},
            {"name": "performance_metrics", "type": "object"},
            {"name": "monitoring_data", "type": "object"},  # NEW FIELD
            {"name": "embedding", "type": "array"},
            {"name": "capability_embedding", "type": "array"}
        ],
        embedding_fields=["name", "description", "capabilities"],
        vector_indices=[
            VectorIndexConfig(
                name="tool_semantic_idx",
                dimension=1536,
                similarity_function="cosine",
                embedding_property="embedding",
                embedding_config=EmbeddingConfig(
                    callback_id="openai_ada",
                    dimension=1536,
                    cache_embeddings=True,
                    update_trigger=["description", "capabilities"]
                )
            )
        ]
    )

    # First registration - will detect change
    print("  → Updating schema (adding monitoring_data field)...")
    result1 = await registry.register_schema_with_embedding(
        updated_tool_schema,
        skip_if_unchanged=True
    )
    print(f"    Result: {result1['status']}")

    # Second registration - no change
    print("  → Re-registering same schema...")
    result2 = await registry.register_schema_with_embedding(
        updated_tool_schema,
        skip_if_unchanged=True
    )
    print(f"    Result: {result2['status']}")

    # Check affected schemas
    affected = registry.change_detector.get_affected_schemas("tool_v2")
    print(f"  ✓ Affected schemas: {affected}")

    # Clear old embeddings if needed
    embedding_manager.clear_cache(older_than=None)
    print("  ✓ Embedding cache cleared for recomputation")


# ==================== Search and Discovery ====================

async def demonstrate_semantic_search(
    orchestrator: AgentOrchestrator,
    embedding_manager: EmbeddingManager
):
    """Demonstrate semantic search capabilities"""

    print("\n🔍 Demonstrating Semantic Search...")

    # Find similar agents by capabilities
    print("  → Finding agents with planning and reasoning capabilities...")
    similar_agents = await orchestrator.find_similar_agents(
        capabilities=["planning", "reasoning"],
        limit=3
    )

    for agent in similar_agents:
        print(f"    - {agent['name']}: {agent['capabilities']} (similarity: {agent.get('similarity', 'N/A')})")

    # Semantic tool search using embeddings
    search_prompt = "Process CSV files and generate statistical reports"
    print(f"\n  → Searching for tools: '{search_prompt}'")

    # Generate search embedding
    search_embedding = await embedding_manager.generate_embedding(
        "openai_ada",
        search_prompt
    )

    # Search in graph (mock result for demo)
    print("    Found tools:")
    print("    - csv_processor (similarity: 0.92)")
    print("    - statistical_analyzer (similarity: 0.88)")
    print("    - report_generator (similarity: 0.85)")


# ==================== Monitor System Status ====================

async def monitor_system_status(infrastructure: Dict[str, Any]):
    """Monitor the deployed system"""

    print("\n📊 System Status Monitor...")

    graph_client = infrastructure["graph_client"]

    # Count deployed agents
    agent_count = await graph_client.execute_query(
        "MATCH (a:Agent) RETURN count(a) as count"
    )
    print(f"  • Active Agents: {agent_count['results'][0]['count']}")

    # Count registered schemas
    schema_count = await graph_client.execute_query(
        "MATCH (s:Schema) RETURN count(s) as count"
    )
    print(f"  • Registered Schemas: {schema_count['results'][0]['count']}")

    # Count vector indices
    index_count = await graph_client.execute_query(
        "CALL db.indexes() YIELD name WHERE name CONTAINS 'vector' RETURN count(*) as count"
    )
    print(f"  • Vector Indices: {index_count['results'][0]['count']}")

    # Check embedding cache
    embedding_manager = infrastructure["embedding_manager"]
    cache_size = len(embedding_manager.embedding_cache)
    print(f"  • Cached Embeddings: {cache_size}")

    # Agent coordination paths
    coord_paths = await graph_client.execute_query(
        """
        MATCH path = (:Agent {type: 'orchestrator'})-[:COORDINATES*]->(:Agent)
        RETURN count(path) as coordination_paths
        """
    )
    print(f"  • Coordination Paths: {coord_paths['results'][0]['coordination_paths']}")


# ==================== Main Deployment Flow ====================

async def main():
    """Main deployment flow"""

    print("=" * 60)
    print("AUTONOMOUS AGENT DEPLOYMENT WITH ADVANCED SCHEMA MANAGEMENT")
    print("=" * 60)

    try:
        # Step 1: Setup infrastructure
        infrastructure = await setup_infrastructure()

        # Step 2: Register tool schemas with embeddings
        tool_schema = await register_tool_schemas(
            infrastructure["schema_registry"]
        )

        # Step 3: Deploy autonomous agent team
        agents = await deploy_agent_team(
            infrastructure["agent_orchestrator"]
        )

        # Step 4: Setup conversation tracking
        conv_schema = await setup_conversation_tracking(
            infrastructure["agent_orchestrator"]
        )

        # Step 5: Demonstrate smart redeployment
        await demonstrate_smart_redeployment(
            infrastructure["schema_registry"],
            infrastructure["embedding_manager"]
        )

        # Step 6: Demonstrate semantic search
        await demonstrate_semantic_search(
            infrastructure["agent_orchestrator"],
            infrastructure["embedding_manager"]
        )

        # Step 7: Monitor system status
        await monitor_system_status(infrastructure)

        print("\n" + "=" * 60)
        print("✅ DEPLOYMENT COMPLETED SUCCESSFULLY")
        print("=" * 60)

        print("\n📋 Summary:")
        print(f"  • Master Orchestrator: {agents['master']}")
        print(f"  • Specialists: {len(agents['specialists'])} deployed")
        print(f"  • Workers: {len(agents['workers'])} deployed")
        print(f"  • Embedding Callbacks: {len(infrastructure['embedding_manager'].callbacks)} registered")
        print(f"  • Vector Indices: Created with smart caching")
        print(f"  • Change Detection: Enabled for all schemas")

        print("\n🚀 The system is now ready for autonomous operations!")

    except Exception as e:
        print(f"\n❌ Error during deployment: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Run the deployment
    asyncio.run(main())