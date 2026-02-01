"""
Tool Management Intelligence Module

SOTA intelligence features for tool management:
- Type algebra for safe tool composition
- Semantic embeddings for tool discovery
- Execution memory for learning from history
- Intent → DAG compilation
- Adaptive routing with circuit breaking
"""

from .algebra import (
    TypeKind,
    ToolType,
    Compatibility,
    CompatibilityResult,
    TypeChecker,
    ToolSignature,
    CompositionOp,
    ComposedTool,
    ToolAlgebra,
)

from .embeddings import (
    EmbeddingProvider,
    LocalEmbeddingProvider,
    ToolEmbedding,
    ToolIndex,
)

from .memory import (
    ExecutionRecord,
    ToolStats,
    SemanticCache,
    ExecutionMemory,
)

from .compiler import (
    NodeType,
    DAGNode,
    ExecutionDAG,
    IntentSlot,
    ParsedIntent,
    IntentParser,
    DAGCompiler,
    IntentDAGCompiler,
)

from .router import (
    RoutingStrategy,
    CircuitState,
    CircuitBreaker,
    ToolVariant,
    RoutingContext,
    RoutingDecision,
    AdaptiveRouter,
    LoadBalancer,
    RoutingMiddleware,
)


__all__ = [
    # Type Algebra
    "TypeKind",
    "ToolType",
    "Compatibility",
    "CompatibilityResult",
    "TypeChecker",
    "ToolSignature",
    "CompositionOp",
    "ComposedTool",
    "ToolAlgebra",
    # Embeddings
    "EmbeddingProvider",
    "LocalEmbeddingProvider",
    "ToolEmbedding",
    "ToolIndex",
    # Execution Memory
    "ExecutionRecord",
    "ToolStats",
    "SemanticCache",
    "ExecutionMemory",
    # DAG Compiler
    "NodeType",
    "DAGNode",
    "ExecutionDAG",
    "IntentSlot",
    "ParsedIntent",
    "IntentParser",
    "DAGCompiler",
    "IntentDAGCompiler",
    # Adaptive Router
    "RoutingStrategy",
    "CircuitState",
    "CircuitBreaker",
    "ToolVariant",
    "RoutingContext",
    "RoutingDecision",
    "AdaptiveRouter",
    "LoadBalancer",
    "RoutingMiddleware",
]
