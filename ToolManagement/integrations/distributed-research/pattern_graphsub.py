#!/usr/bin/env python3
"""
PATTERN INTELLIGENCE + GRAPHSUB INTEGRATION

Wires the sophisticated pattern intelligence system into GraphSub,
the high-performance Rust graph database.

This replaces the in-memory pattern graph with GraphSub for:
- 17B+ edges/sec traversal performance
- Persistent storage
- Distributed mode support
- Native SIMD-optimized operations
"""

import json
import sys
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path

# Try to import GraphSub
GRAPHSUB_PATH = Path("/Users/arthurcolle/Dsco/dsco-graphsub/python")
if GRAPHSUB_PATH.exists():
    sys.path.insert(0, str(GRAPHSUB_PATH))

try:
    import graphsub as gs
    GRAPHSUB_AVAILABLE = True
    print(f"GraphSub loaded successfully")
except ImportError as e:
    GRAPHSUB_AVAILABLE = False
    print(f"GraphSub not available: {e}")
    print("Falling back to in-memory mode")

from pattern_intelligence import (
    PatternKnowledgeBase,
    PatternDomain,
    PatternIntent,
    Pattern,
    PatternOntology,
    PatternGraph,
    PatternRelation,
    RelationType,
    PatternEmbedding,
    BayesianPatternMatcher,
    CausalReasoningEngine
)


# =============================================================================
# GRAPHSUB TYPE MAPPINGS
# =============================================================================

# Pattern-specific type IDs (extending GraphSub's ontology)
PATTERN_TYPE_IDS = {
    # Pattern categories
    "pattern": 100,
    "pattern_resilience": 101,
    "pattern_performance": 102,
    "pattern_data_flow": 103,
    "pattern_orchestration": 104,
    "pattern_observability": 105,
    "pattern_security": 106,
    "pattern_integration": 107,
    "pattern_governance": 108,

    # Meta types
    "domain": 110,
    "intent": 111,
    "cluster": 112,
    "composition": 113,
}

# Relation type IDs for pattern relationships
PATTERN_RELATION_IDS = {
    "requires": 200,
    "enhances": 201,
    "conflicts": 202,
    "alternative": 203,
    "composes": 204,
    "specializes": 205,
    "precedes": 206,
    "mitigates": 207,
    "has_domain": 210,
    "has_intent": 211,
    "in_cluster": 212,
    "similar_to": 213,
}


# =============================================================================
# GRAPHSUB PATTERN STORE
# =============================================================================

class GraphSubPatternStore:
    """
    Pattern storage backed by GraphSub.

    Maps the pattern intelligence model to GraphSub's graph structure:
    - Patterns become nodes with type based on domain
    - Relationships become typed edges
    - Embeddings stored as node payloads
    - Clusters as meta-nodes connecting patterns
    """

    def __init__(self, shard_id: int = 0, persistent: bool = False):
        self.shard_id = shard_id
        self.persistent = persistent

        if GRAPHSUB_AVAILABLE:
            self.graph = gs.Graph(shard_id=shard_id)
            self.ontology = self.graph.bootstrap_ontology()
            self.builder = self.graph.entity_builder()
            self._register_pattern_types()
        else:
            self.graph = None
            self.ontology = None
            self.builder = None

        # ID mappings
        self.pattern_id_to_node: Dict[str, int] = {}
        self.node_to_pattern_id: Dict[int, str] = {}
        self.domain_nodes: Dict[str, int] = {}
        self.intent_nodes: Dict[str, int] = {}
        self.cluster_nodes: Dict[int, int] = {}

        # Cache for fast lookups
        self._pattern_cache: Dict[str, Pattern] = {}

    def _register_pattern_types(self):
        """Register pattern-specific types in GraphSub ontology."""
        if not self.ontology:
            return

        # This would register custom types if GraphSub supports dynamic ontology
        # For now, we use the concept type (8) with payloads to distinguish
        pass

    def store_pattern(self, pattern: Pattern) -> int:
        """Store a pattern in GraphSub."""
        if not self.graph:
            return -1

        # Determine type based on domain
        type_id = PATTERN_TYPE_IDS.get("pattern", 100)
        if pattern.ontology and pattern.ontology.domain:
            domain_type = f"pattern_{pattern.ontology.domain.value}"
            type_id = PATTERN_TYPE_IDS.get(domain_type, 100)

        # Create node
        node_id = self.graph.add_node(type_id, pattern.name)

        # Store full pattern data as payload
        payload = {
            "id": pattern.id,
            "name": pattern.name,
            "description": pattern.description,
            "keywords": pattern.keywords,
            "category": pattern.category,
            "use_cases": pattern.use_cases,
            "anti_patterns": pattern.anti_patterns,
            "implementation_hints": pattern.implementation_hints,
            "trade_offs": pattern.trade_offs,
            "metrics": pattern.metrics,
            "embedding": pattern.embedding,
        }

        if pattern.ontology:
            payload["ontology"] = {
                "domain": pattern.ontology.domain.value,
                "intents": [i.value for i in pattern.ontology.intents],
                "complexity": pattern.ontology.complexity,
                "maturity": pattern.ontology.maturity,
                "prerequisites": pattern.ontology.prerequisites,
                "related_concepts": pattern.ontology.related_concepts,
            }

        self.graph.set_payload(node_id, json.dumps(payload).encode())

        # Update mappings
        self.pattern_id_to_node[pattern.id] = node_id
        self.node_to_pattern_id[node_id] = pattern.id
        self._pattern_cache[pattern.id] = pattern

        return node_id

    def get_pattern(self, pattern_id: str) -> Optional[Pattern]:
        """Retrieve a pattern from GraphSub."""
        # Check cache first
        if pattern_id in self._pattern_cache:
            return self._pattern_cache[pattern_id]

        if not self.graph:
            return None

        node_id = self.pattern_id_to_node.get(pattern_id)
        if node_id is None:
            return None

        node = self.graph.get_node(node_id)
        if not node:
            return None

        payload_bytes = self.graph.get_payload(node_id)
        if not payload_bytes:
            return None

        payload = json.loads(payload_bytes.decode())
        return self._payload_to_pattern(payload)

    def _payload_to_pattern(self, payload: Dict) -> Pattern:
        """Convert stored payload back to Pattern object."""
        ontology = None
        if "ontology" in payload:
            ont_data = payload["ontology"]
            ontology = PatternOntology(
                domain=PatternDomain(ont_data["domain"]),
                intents=[PatternIntent(i) for i in ont_data["intents"]],
                complexity=ont_data["complexity"],
                maturity=ont_data["maturity"],
                prerequisites=ont_data["prerequisites"],
                related_concepts=ont_data["related_concepts"],
            )

        return Pattern(
            id=payload["id"],
            name=payload["name"],
            description=payload["description"],
            keywords=payload["keywords"],
            category=payload["category"],
            ontology=ontology,
            use_cases=payload.get("use_cases", []),
            anti_patterns=payload.get("anti_patterns", []),
            implementation_hints=payload.get("implementation_hints", []),
            trade_offs=payload.get("trade_offs", {}),
            metrics=payload.get("metrics", []),
            embedding=payload.get("embedding", []),
        )

    def store_relationship(self, relation: PatternRelation) -> int:
        """Store a pattern relationship as GraphSub edge."""
        if not self.graph:
            return -1

        source_node = self.pattern_id_to_node.get(relation.source)
        target_node = self.pattern_id_to_node.get(relation.target)

        if source_node is None or target_node is None:
            return -1

        # Map relation type to edge type
        relation_type_id = PATTERN_RELATION_IDS.get(
            relation.relation_type.value, 200
        )

        edge_id = self.graph.add_edge(source_node, target_node, relation_type_id)

        # Store edge metadata as payload if supported
        # For now, weight/context stored in pattern cache

        if relation.bidirectional:
            self.graph.add_edge(target_node, source_node, relation_type_id)

        return edge_id

    def get_related(self, pattern_id: str,
                    relation_types: Optional[List[RelationType]] = None,
                    direction: str = "both") -> List[Tuple[str, RelationType, float]]:
        """Get patterns related to the given pattern."""
        if not self.graph:
            return []

        node_id = self.pattern_id_to_node.get(pattern_id)
        if node_id is None:
            return []

        results = []
        type_filter = None
        if relation_types:
            type_filter = {PATTERN_RELATION_IDS.get(rt.value) for rt in relation_types}

        if direction in ("outgoing", "both"):
            for edge in self.graph.outgoing_edges(node_id):
                if type_filter and edge.relation_type not in type_filter:
                    continue

                target_pattern_id = self.node_to_pattern_id.get(edge.target)
                if target_pattern_id:
                    rel_type = self._edge_type_to_relation(edge.relation_type)
                    results.append((target_pattern_id, rel_type, edge.weight))

        if direction in ("incoming", "both"):
            for edge in self.graph.incoming_edges(node_id):
                if type_filter and edge.relation_type not in type_filter:
                    continue

                source_pattern_id = self.node_to_pattern_id.get(edge.source)
                if source_pattern_id:
                    rel_type = self._edge_type_to_relation(edge.relation_type)
                    results.append((source_pattern_id, rel_type, edge.weight))

        return results

    def _edge_type_to_relation(self, edge_type: int) -> RelationType:
        """Map GraphSub edge type back to RelationType."""
        reverse_map = {v: k for k, v in PATTERN_RELATION_IDS.items()}
        rel_name = reverse_map.get(edge_type, "requires")
        return RelationType(rel_name)

    def traverse_patterns(self, start_pattern_id: str,
                          max_depth: int = 3,
                          relation_types: Optional[List[RelationType]] = None) -> List[Tuple[str, int]]:
        """BFS traversal from a pattern using GraphSub's fast traversal."""
        if not self.graph:
            return []

        start_node = self.pattern_id_to_node.get(start_pattern_id)
        if start_node is None:
            return []

        visited = set()
        results = []
        frontier = [(start_node, 0)]

        while frontier:
            node_id, depth = frontier.pop(0)

            if node_id in visited or depth > max_depth:
                continue
            visited.add(node_id)

            pattern_id = self.node_to_pattern_id.get(node_id)
            if pattern_id:
                results.append((pattern_id, depth))

            if depth < max_depth:
                for edge in self.graph.outgoing_edges(node_id):
                    if edge.target not in visited:
                        frontier.append((edge.target, depth + 1))

        return results

    def find_similar(self, pattern_id: str, top_k: int = 5) -> List[Tuple[str, float]]:
        """Find similar patterns using GraphSub traversal + embedding similarity."""
        if not self.graph:
            return []

        # Get the pattern's embedding
        pattern = self.get_pattern(pattern_id)
        if not pattern or not pattern.embedding:
            return []

        # First, find structurally related patterns (1-2 hops)
        candidates = set()
        for related_id, _, _ in self.get_related(pattern_id, direction="both"):
            candidates.add(related_id)

            # Add 2nd degree connections
            for related_id_2, _, _ in self.get_related(related_id, direction="both"):
                candidates.add(related_id_2)

        candidates.discard(pattern_id)

        # Score by embedding similarity
        scores = []
        for candidate_id in candidates:
            candidate = self.get_pattern(candidate_id)
            if candidate and candidate.embedding:
                similarity = self._cosine_similarity(pattern.embedding, candidate.embedding)
                scores.append((candidate_id, similarity))

        scores.sort(key=lambda x: -x[1])
        return scores[:top_k]

    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        if not vec1 or not vec2 or len(vec1) != len(vec2):
            return 0.0

        dot = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = sum(a * a for a in vec1) ** 0.5
        norm2 = sum(b * b for b in vec2) ** 0.5

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot / (norm1 * norm2)

    def get_context_window(self, pattern_ids: List[str],
                           token_budget: int = 4096) -> Dict:
        """Get LLM context window for patterns using GraphSub's context API."""
        if not self.graph:
            return {"context": "", "patterns": pattern_ids}

        ctx = self.graph.context_window(token_budget)

        # Add focus patterns
        for pid in pattern_ids:
            node_id = self.pattern_id_to_node.get(pid)
            if node_id:
                ctx.add_to_focus(node_id)

                # Add neighbors to peripheral
                for edge in self.graph.outgoing_edges(node_id):
                    ctx.add_to_peripheral(edge.target)
                for edge in self.graph.incoming_edges(node_id):
                    ctx.add_to_peripheral(edge.source)

        return {
            "capacity": ctx.capacity,
            "used": ctx.used,
            "remaining": ctx.remaining(),
            "context": ctx.render(),
            "patterns": pattern_ids,
        }

    def stats(self) -> Dict:
        """Get store statistics."""
        if not self.graph:
            return {"backend": "memory", "patterns": len(self._pattern_cache)}

        return {
            "backend": "graphsub",
            "shard_id": self.shard_id,
            "node_count": self.graph.node_count(),
            "edge_count": self.graph.edge_count(),
            "patterns": len(self.pattern_id_to_node),
            "cached": len(self._pattern_cache),
        }


# =============================================================================
# GRAPHSUB-BACKED PATTERN KNOWLEDGE BASE
# =============================================================================

class GraphSubPatternKB(PatternKnowledgeBase):
    """
    Pattern Knowledge Base backed by GraphSub.

    Inherits all the sophisticated features from PatternKnowledgeBase
    but stores patterns and relationships in GraphSub for:
    - High performance traversal
    - Persistence
    - Scalability
    """

    def __init__(self, shard_id: int = 0):
        # Initialize store BEFORE calling super().__init__
        self.store = GraphSubPatternStore(shard_id=shard_id)

        # Now initialize parent (which calls _load_patterns, _load_relationships)
        super().__init__()

        # Sync patterns to GraphSub
        self._sync_to_graphsub()

    def _sync_to_graphsub(self):
        """Sync all patterns and relationships to GraphSub."""
        if not self.store.graph:
            print("GraphSub not available, using in-memory mode")
            return

        print(f"Syncing {len(self.graph.nodes)} patterns to GraphSub...")

        # Store patterns
        for pattern_id, pattern in self.graph.nodes.items():
            self.store.store_pattern(pattern)

        # Store relationships
        for edge in self.graph.edges:
            self.store.store_relationship(edge)

        stats = self.store.stats()
        print(f"GraphSub sync complete: {stats['node_count']} nodes, {stats['edge_count']} edges")

    def search(self, query: str, k: int = 5,
               context: Optional[Dict] = None) -> List[Tuple[Pattern, float]]:
        """Search using embeddings + GraphSub traversal."""
        # Use parent's embedding search
        results = super().search(query, k=k, context=context)

        # If GraphSub available, boost patterns with more connections
        if self.store.graph:
            boosted_results = []
            for pattern, score in results:
                # Get connection count from GraphSub
                node_id = self.store.pattern_id_to_node.get(pattern.id)
                if node_id:
                    out_edges = len(list(self.store.graph.outgoing_edges(node_id)))
                    in_edges = len(list(self.store.graph.incoming_edges(node_id)))
                    connection_boost = min(0.1, (out_edges + in_edges) * 0.01)
                    boosted_results.append((pattern, score + connection_boost))
                else:
                    boosted_results.append((pattern, score))

            boosted_results.sort(key=lambda x: -x[1])
            return boosted_results

        return results

    def get_pattern_context(self, pattern_ids: List[str],
                            token_budget: int = 4096) -> Dict:
        """Get LLM context window for patterns."""
        return self.store.get_context_window(pattern_ids, token_budget)

    def find_path(self, from_pattern: str, to_pattern: str,
                  max_depth: int = 5) -> Optional[List[str]]:
        """Find path between two patterns using GraphSub."""
        if not self.store.graph:
            return None

        from_node = self.store.pattern_id_to_node.get(from_pattern)
        to_node = self.store.pattern_id_to_node.get(to_pattern)

        if from_node is None or to_node is None:
            return None

        # BFS to find path
        visited = {from_node: None}
        frontier = [from_node]

        while frontier:
            current = frontier.pop(0)

            if current == to_node:
                # Reconstruct path
                path = []
                node = to_node
                while node is not None:
                    pattern_id = self.store.node_to_pattern_id.get(node)
                    if pattern_id:
                        path.append(pattern_id)
                    node = visited.get(node)
                return list(reversed(path))

            for edge in self.store.graph.outgoing_edges(current):
                if edge.target not in visited:
                    visited[edge.target] = current
                    frontier.append(edge.target)

                    if len(visited) > 1000:  # Safety limit
                        return None

        return None

    def get_stats(self) -> Dict:
        """Get combined stats."""
        base_stats = {
            "patterns": len(self.graph.nodes),
            "relationships": len(self.graph.edges),
            "clusters": len(self.clusterer.clusters),
            "embedding_dims": self.embedder.EMBEDDING_DIM,
        }

        graphsub_stats = self.store.stats()

        return {**base_stats, "graphsub": graphsub_stats}


# =============================================================================
# PATTERN INTELLIGENCE SERVICE
# =============================================================================

class PatternIntelligenceService:
    """
    High-level service for pattern intelligence powered by GraphSub.

    Provides a unified API for:
    - Pattern search and recommendation
    - Causal reasoning
    - Multi-agent debate integration
    - LLM context generation
    """

    def __init__(self, shard_id: int = 0):
        self.kb = GraphSubPatternKB(shard_id=shard_id)
        self.bayesian = BayesianPatternMatcher(self.kb)
        self.causal = CausalReasoningEngine(self.kb.graph)

        # Import debate system if available
        try:
            from pattern_debate import PatternDebate
            self._debate_system = PatternDebate()
            self._debate_system.kb = self.kb  # Use GraphSub-backed KB
        except ImportError:
            self._debate_system = None

    def search(self, query: str, k: int = 5) -> List[Dict]:
        """Search for patterns."""
        results = self.kb.search(query, k=k)
        return [
            {
                "id": p.id,
                "name": p.name,
                "description": p.description,
                "score": score,
                "category": p.category,
            }
            for p, score in results
        ]

    def recommend(self, scenario: str,
                  existing_patterns: List[str] = None,
                  k: int = 5) -> List[Dict]:
        """Get pattern recommendations."""
        recs = self.kb.recommend(scenario, existing=existing_patterns or [], k=k)
        return [
            {
                "pattern_id": r.pattern_id,
                "pattern_name": r.pattern_name,
                "score": r.score,
                "explanation": r.explanation,
                "warnings": r.warnings,
                "alternatives": r.alternatives,
            }
            for r in recs
        ]

    def analyze(self, pattern_ids: List[str]) -> Dict:
        """Analyze a set of patterns."""
        # Composition analysis
        composed = self.kb.compose(pattern_ids)

        # Causal analysis
        causal = self.causal.explain_combination(pattern_ids)

        # Anti-pattern check
        warnings = self.kb.analyze_antipatterns(pattern_ids)

        return {
            "composition": {
                "name": composed.name,
                "type": composed.composition_type,
                "score": composed.score,
                "synergies": composed.synergies,
                "conflicts": composed.conflicts,
            },
            "causal": causal,
            "warnings": [
                {"severity": w.severity, "message": w.message, "suggestion": w.suggestion}
                for w in warnings
            ],
        }

    def get_context(self, pattern_ids: List[str],
                    token_budget: int = 4096) -> Dict:
        """Get LLM context for patterns."""
        return self.kb.get_pattern_context(pattern_ids, token_budget)

    def find_path(self, from_pattern: str, to_pattern: str) -> Optional[List[str]]:
        """Find path between patterns."""
        return self.kb.find_path(from_pattern, to_pattern)

    def debate(self, requirement: str,
               constraints: Dict = None,
               rounds: int = 2) -> Dict:
        """Run multi-agent debate."""
        if not self._debate_system:
            return {"error": "Debate system not available"}

        result = self._debate_system.debate(
            requirement=requirement,
            constraints=constraints,
            rounds=rounds,
            verbose=False
        )

        return {
            "recommendations": [
                {"pattern": r[0], "score": r[1], "rationale": r[2]}
                for r in result.recommended_patterns
            ],
            "consensus": result.consensus_level,
            "confidence": result.confidence,
            "tradeoffs": result.key_tradeoffs,
        }

    def stats(self) -> Dict:
        """Get service statistics."""
        return self.kb.get_stats()


# =============================================================================
# DEMONSTRATION
# =============================================================================

def demo():
    """Demonstrate GraphSub-backed pattern intelligence."""
    print("=" * 80)
    print("PATTERN INTELLIGENCE + GRAPHSUB INTEGRATION")
    print("=" * 80)

    # Initialize service
    print("\nInitializing Pattern Intelligence Service with GraphSub backend...")
    service = PatternIntelligenceService(shard_id=0)

    # Show stats
    stats = service.stats()
    print(f"\nService Stats:")
    print(f"  Patterns: {stats['patterns']}")
    print(f"  Relationships: {stats['relationships']}")
    print(f"  Clusters: {stats['clusters']}")
    if 'graphsub' in stats:
        gs_stats = stats['graphsub']
        print(f"  GraphSub Backend:")
        print(f"    Nodes: {gs_stats.get('node_count', 'N/A')}")
        print(f"    Edges: {gs_stats.get('edge_count', 'N/A')}")

    # Search
    print("\n" + "=" * 60)
    print("PATTERN SEARCH")
    print("=" * 60)

    queries = [
        "handle service failures gracefully",
        "high throughput data processing",
        "secure API design"
    ]

    for query in queries:
        print(f"\nQuery: \"{query}\"")
        results = service.search(query, k=3)
        for r in results:
            print(f"  [{r['score']:.3f}] {r['name']}")

    # Recommendation
    print("\n" + "=" * 60)
    print("PATTERN RECOMMENDATION")
    print("=" * 60)

    scenario = "Building a payment system with 99.99% availability"
    print(f"\nScenario: {scenario}")

    recs = service.recommend(scenario, existing_patterns=['timeout'], k=3)
    for r in recs:
        print(f"\n  [{r['score']:.2f}] {r['pattern_name']}")
        print(f"    {r['explanation']}")
        if r['warnings']:
            print(f"    Warnings: {r['warnings']}")

    # Analysis
    print("\n" + "=" * 60)
    print("PATTERN ANALYSIS")
    print("=" * 60)

    patterns_to_analyze = ['circuit_breaker', 'retry_backoff', 'fallback', 'bulkhead']
    print(f"\nAnalyzing: {', '.join(patterns_to_analyze)}")

    analysis = service.analyze(patterns_to_analyze)
    print(f"\n  Composition: {analysis['composition']['name']}")
    print(f"  Score: {analysis['composition']['score']:.2f}")
    if analysis['composition']['synergies']:
        syns = [f"{s[0]}+{s[1]}" for s in analysis['composition']['synergies'][:3]]
        print(f"  Synergies: {', '.join(syns)}")

    if analysis['warnings']:
        print(f"\n  Warnings:")
        for w in analysis['warnings'][:2]:
            print(f"    [{w['severity']}] {w['message']}")

    # Path finding
    print("\n" + "=" * 60)
    print("PATH FINDING")
    print("=" * 60)

    from_pattern = 'retry_backoff'
    to_pattern = 'saga'
    print(f"\nFinding path: {from_pattern} -> {to_pattern}")

    path = service.find_path(from_pattern, to_pattern)
    if path:
        print(f"  Path: {' -> '.join(path)}")
    else:
        print("  No path found")

    # Context generation
    print("\n" + "=" * 60)
    print("LLM CONTEXT GENERATION")
    print("=" * 60)

    context_patterns = ['circuit_breaker', 'retry_backoff']
    print(f"\nGenerating context for: {', '.join(context_patterns)}")

    context = service.get_context(context_patterns, token_budget=2048)
    print(f"  Token budget: {context.get('capacity', 'N/A')}")
    print(f"  Tokens used: {context.get('used', 'N/A')}")
    if context.get('context'):
        print(f"  Context preview: {context['context'][:200]}...")

    # Debate (if available)
    if service.debate:
        print("\n" + "=" * 60)
        print("MULTI-AGENT DEBATE")
        print("=" * 60)

        debate_result = service.debate(
            requirement="Real-time trading platform with sub-10ms latency",
            constraints={'max_latency_ms': 10},
            rounds=1
        )

        print(f"\n  Consensus: {debate_result.get('consensus', 0):.0%}")
        print(f"  Confidence: {debate_result.get('confidence', 0):.0%}")
        print(f"  Top recommendations:")
        for r in debate_result.get('recommendations', [])[:3]:
            print(f"    - {r['pattern']} ({r['score']:.2f})")

    print("\n" + "=" * 80)
    print("INTEGRATION COMPLETE")
    print("=" * 80)

    if GRAPHSUB_AVAILABLE:
        print("\n✓ GraphSub backend active - 17B+ edges/sec performance available")
    else:
        print("\n⚠ Running in fallback mode - install GraphSub for full performance")


if __name__ == "__main__":
    demo()
