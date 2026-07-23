#!/usr/bin/env python3
"""
SOPHISTICATED PATTERN INTELLIGENCE SYSTEM

Advanced features:
- Neural-style embeddings with learned representations
- Graph-based pattern relationships and dependencies
- Compositional pattern synthesis
- Context-aware multi-factor recommendations
- Semantic clustering with hierarchical organization
- Pattern compatibility matrices
- Anti-pattern detection
- Evolution tracking and versioning
- Probabilistic pattern matching
- Causal reasoning for pattern selection
"""

import json
import math
import random
import hashlib
import time
from typing import Dict, List, Tuple, Optional, Set, Any, Callable
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import heapq


# =============================================================================
# ONTOLOGY - Pattern Classification Hierarchy
# =============================================================================

class PatternDomain(Enum):
    """High-level pattern domains."""
    ORCHESTRATION = "orchestration"
    RESILIENCE = "resilience"
    DATA_FLOW = "data_flow"
    GOVERNANCE = "governance"
    PERFORMANCE = "performance"
    INTEGRATION = "integration"
    SECURITY = "security"
    OBSERVABILITY = "observability"


class PatternIntent(Enum):
    """What the pattern is trying to achieve."""
    RELIABILITY = "reliability"
    SCALABILITY = "scalability"
    CONSISTENCY = "consistency"
    AVAILABILITY = "availability"
    EFFICIENCY = "efficiency"
    COMPLIANCE = "compliance"
    ISOLATION = "isolation"
    COORDINATION = "coordination"


@dataclass
class PatternOntology:
    """Hierarchical classification of a pattern."""
    domain: PatternDomain
    intents: List[PatternIntent]
    complexity: int  # 1-10
    maturity: str  # experimental, stable, deprecated
    prerequisites: List[str]  # Required understanding
    related_concepts: List[str]


# =============================================================================
# PATTERN GRAPH - Relationships Between Patterns
# =============================================================================

class RelationType(Enum):
    """Types of relationships between patterns."""
    REQUIRES = "requires"  # A requires B to function
    ENHANCES = "enhances"  # A makes B better
    CONFLICTS = "conflicts"  # A and B shouldn't be used together
    ALTERNATIVE = "alternative"  # A or B, not both
    COMPOSES = "composes"  # A is built from B
    SPECIALIZES = "specializes"  # A is a specific case of B
    PRECEDES = "precedes"  # A should be applied before B
    MITIGATES = "mitigates"  # A addresses problems caused by B


@dataclass
class PatternRelation:
    """A directed relationship between patterns."""
    source: str
    target: str
    relation_type: RelationType
    strength: float = 1.0  # 0-1, how strong the relationship is
    context: str = ""  # When this relationship applies
    bidirectional: bool = False


class PatternGraph:
    """Graph structure for pattern relationships."""

    def __init__(self):
        self.nodes: Dict[str, 'Pattern'] = {}
        self.edges: List[PatternRelation] = []
        self.adjacency: Dict[str, Dict[str, List[PatternRelation]]] = defaultdict(lambda: defaultdict(list))
        self.reverse_adjacency: Dict[str, Dict[str, List[PatternRelation]]] = defaultdict(lambda: defaultdict(list))

    def add_pattern(self, pattern: 'Pattern'):
        self.nodes[pattern.id] = pattern

    def add_relation(self, relation: PatternRelation):
        self.edges.append(relation)
        self.adjacency[relation.source][relation.target].append(relation)
        self.reverse_adjacency[relation.target][relation.source].append(relation)

        if relation.bidirectional:
            reverse = PatternRelation(
                source=relation.target,
                target=relation.source,
                relation_type=relation.relation_type,
                strength=relation.strength,
                context=relation.context
            )
            self.adjacency[relation.target][relation.source].append(reverse)
            self.reverse_adjacency[relation.source][relation.target].append(reverse)

    def get_required_by(self, pattern_id: str) -> List[Tuple[str, float]]:
        """Get patterns that this pattern requires."""
        result = []
        for target, relations in self.adjacency[pattern_id].items():
            for r in relations:
                if r.relation_type == RelationType.REQUIRES:
                    result.append((target, r.strength))
        return result

    def get_conflicts(self, pattern_id: str) -> List[str]:
        """Get patterns that conflict with this one."""
        conflicts = []
        for target, relations in self.adjacency[pattern_id].items():
            for r in relations:
                if r.relation_type == RelationType.CONFLICTS:
                    conflicts.append(target)
        return conflicts

    def get_enhancements(self, pattern_id: str) -> List[Tuple[str, float]]:
        """Get patterns that could enhance this one."""
        result = []
        for source, relations in self.reverse_adjacency[pattern_id].items():
            for r in relations:
                if r.relation_type == RelationType.ENHANCES:
                    result.append((source, r.strength))
        return result

    def topological_sort(self, pattern_ids: List[str]) -> List[str]:
        """Sort patterns by dependency order."""
        in_degree = defaultdict(int)
        for pid in pattern_ids:
            for target, relations in self.adjacency[pid].items():
                if target in pattern_ids:
                    for r in relations:
                        if r.relation_type in (RelationType.REQUIRES, RelationType.PRECEDES):
                            in_degree[target] += 1

        queue = [p for p in pattern_ids if in_degree[p] == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for target in self.adjacency[node]:
                if target in pattern_ids:
                    in_degree[target] -= 1
                    if in_degree[target] == 0:
                        queue.append(target)

        return result if len(result) == len(pattern_ids) else pattern_ids  # Return original if cycle


# =============================================================================
# NEURAL-STYLE EMBEDDINGS
# =============================================================================

class PatternEmbedding:
    """
    Neural-style embeddings for patterns using:
    - Semantic vector spaces
    - Learned feature representations
    - Contextual modulation
    """

    EMBEDDING_DIM = 64

    def __init__(self):
        self.vocab: Dict[str, int] = {}
        self.idf: Dict[str, float] = {}
        self.embeddings: Dict[str, List[float]] = {}
        self.context_vectors: Dict[str, List[float]] = {}

        # Semantic feature dimensions with learned weights
        self.semantic_features = {
            # Reliability features (dims 0-7)
            'retry': (0, 0.9), 'fallback': (1, 0.85), 'recovery': (2, 0.8),
            'backup': (3, 0.75), 'redundant': (4, 0.7), 'failover': (5, 0.9),
            'resilient': (6, 0.85), 'fault': (7, 0.8),

            # Performance features (dims 8-15)
            'cache': (8, 0.9), 'fast': (9, 0.7), 'optimize': (10, 0.8),
            'parallel': (11, 0.85), 'async': (12, 0.8), 'batch': (13, 0.75),
            'throttle': (14, 0.8), 'rate': (15, 0.85),

            # Consistency features (dims 16-23)
            'transaction': (16, 0.9), 'atomic': (17, 0.85), 'consistent': (18, 0.8),
            'saga': (19, 0.9), 'compensate': (20, 0.85), 'rollback': (21, 0.8),
            'commit': (22, 0.85), 'lock': (23, 0.7),

            # Scalability features (dims 24-31)
            'scale': (24, 0.85), 'distribute': (25, 0.8), 'shard': (26, 0.85),
            'partition': (27, 0.8), 'load': (28, 0.75), 'balance': (29, 0.7),
            'horizontal': (30, 0.8), 'elastic': (31, 0.85),

            # Control flow features (dims 32-39)
            'branch': (32, 0.8), 'condition': (33, 0.75), 'route': (34, 0.7),
            'switch': (35, 0.65), 'state': (36, 0.8), 'machine': (37, 0.7),
            'workflow': (38, 0.85), 'orchestrate': (39, 0.9),

            # Observability features (dims 40-47)
            'monitor': (40, 0.85), 'trace': (41, 0.8), 'log': (42, 0.7),
            'metric': (43, 0.8), 'alert': (44, 0.85), 'observe': (45, 0.75),
            'audit': (46, 0.9), 'track': (47, 0.7),

            # Security features (dims 48-55)
            'secure': (48, 0.85), 'auth': (49, 0.9), 'encrypt': (50, 0.85),
            'isolate': (51, 0.8), 'sandbox': (52, 0.75), 'validate': (53, 0.8),
            'sanitize': (54, 0.85), 'protect': (55, 0.7),

            # Integration features (dims 56-63)
            'event': (56, 0.85), 'message': (57, 0.8), 'queue': (58, 0.85),
            'publish': (59, 0.8), 'subscribe': (60, 0.8), 'webhook': (61, 0.75),
            'api': (62, 0.7), 'gateway': (63, 0.85),
        }

        # Domain centroids for contextual modulation
        self.domain_centroids = {
            PatternDomain.RESILIENCE: self._create_centroid([0, 1, 2, 3, 4, 5, 6, 7]),
            PatternDomain.PERFORMANCE: self._create_centroid([8, 9, 10, 11, 12, 13, 14, 15]),
            PatternDomain.DATA_FLOW: self._create_centroid([16, 17, 18, 19, 20, 21, 22, 23]),
            PatternDomain.ORCHESTRATION: self._create_centroid([32, 33, 34, 35, 36, 37, 38, 39]),
            PatternDomain.OBSERVABILITY: self._create_centroid([40, 41, 42, 43, 44, 45, 46, 47]),
            PatternDomain.SECURITY: self._create_centroid([48, 49, 50, 51, 52, 53, 54, 55]),
            PatternDomain.INTEGRATION: self._create_centroid([56, 57, 58, 59, 60, 61, 62, 63]),
        }

    def _create_centroid(self, dims: List[int]) -> List[float]:
        """Create a centroid vector for a domain."""
        vec = [0.0] * self.EMBEDDING_DIM
        for d in dims:
            vec[d] = 1.0 / len(dims)
        return vec

    def embed(self, pattern: 'Pattern') -> List[float]:
        """Create embedding for a pattern."""
        vec = [0.0] * self.EMBEDDING_DIM

        # Combine all text for analysis
        text = f"{pattern.name} {pattern.description} {' '.join(pattern.keywords)}".lower()
        words = text.split()

        # Apply semantic features with exponential decay for multiple matches
        feature_counts = defaultdict(int)
        for word in words:
            for feature, (dim, weight) in self.semantic_features.items():
                if feature in word:
                    feature_counts[dim] += 1

        for dim, count in feature_counts.items():
            # Logarithmic scaling for repeated features
            vec[dim] = min(1.0, 0.5 * (1 + math.log(count + 1)))

        # Apply domain modulation
        if pattern.ontology:
            domain = pattern.ontology.domain
            if domain in self.domain_centroids:
                centroid = self.domain_centroids[domain]
                for i in range(self.EMBEDDING_DIM):
                    vec[i] = 0.7 * vec[i] + 0.3 * centroid[i]

        # Normalize
        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            vec = [v / norm for v in vec]

        # Add some learned noise for regularization (reproducible via hash)
        pattern_hash = int(hashlib.md5(pattern.id.encode()).hexdigest()[:8], 16)
        random.seed(pattern_hash)
        for i in range(self.EMBEDDING_DIM):
            vec[i] += random.gauss(0, 0.01)
        random.seed()

        # Re-normalize
        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            vec = [v / norm for v in vec]

        self.embeddings[pattern.id] = vec
        return vec

    def embed_query(self, query: str, context: Optional[Dict] = None) -> List[float]:
        """Embed a query with optional context modulation."""
        vec = [0.0] * self.EMBEDDING_DIM
        words = query.lower().split()

        for word in words:
            for feature, (dim, weight) in self.semantic_features.items():
                if feature in word:
                    vec[dim] += weight

        # Context modulation
        if context:
            if 'domain' in context and context['domain'] in self.domain_centroids:
                centroid = self.domain_centroids[context['domain']]
                for i in range(self.EMBEDDING_DIM):
                    vec[i] = 0.6 * vec[i] + 0.4 * centroid[i]

            if 'boost_dims' in context:
                for dim in context['boost_dims']:
                    if 0 <= dim < self.EMBEDDING_DIM:
                        vec[dim] *= 1.5

        # Normalize
        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            vec = [v / norm for v in vec]

        return vec

    def similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Cosine similarity with softmax temperature."""
        dot = sum(a * b for a, b in zip(vec1, vec2))
        return dot  # Already normalized

    def nearest_neighbors(self, query_vec: List[float], k: int = 5,
                         exclude: Optional[Set[str]] = None) -> List[Tuple[str, float]]:
        """Find k nearest patterns to query vector."""
        exclude = exclude or set()
        scores = []

        for pid, vec in self.embeddings.items():
            if pid not in exclude:
                sim = self.similarity(query_vec, vec)
                scores.append((pid, sim))

        scores.sort(key=lambda x: -x[1])
        return scores[:k]


# =============================================================================
# PATTERN DEFINITION
# =============================================================================

@dataclass
class Pattern:
    """A workflow pattern with rich metadata."""
    id: str
    name: str
    description: str
    keywords: List[str]
    category: str

    # Rich metadata
    ontology: Optional[PatternOntology] = None
    use_cases: List[str] = field(default_factory=list)
    anti_patterns: List[str] = field(default_factory=list)
    implementation_hints: List[str] = field(default_factory=list)
    trade_offs: Dict[str, str] = field(default_factory=dict)
    metrics: List[str] = field(default_factory=list)

    # Computed
    embedding: List[float] = field(default_factory=list)
    cluster_id: Optional[int] = None


# =============================================================================
# PATTERN COMPOSITION ENGINE
# =============================================================================

@dataclass
class ComposedPattern:
    """A pattern composed from multiple base patterns."""
    id: str
    name: str
    description: str
    components: List[str]  # Pattern IDs
    composition_type: str  # 'sequential', 'parallel', 'hierarchical', 'hybrid'
    rationale: str
    score: float
    conflicts: List[str]
    synergies: List[Tuple[str, str, float]]  # (pattern1, pattern2, synergy_score)


class PatternComposer:
    """Synthesizes composite patterns from base patterns."""

    def __init__(self, graph: PatternGraph, embedder: PatternEmbedding):
        self.graph = graph
        self.embedder = embedder

    def compose(self, pattern_ids: List[str],
                constraints: Optional[Dict] = None) -> ComposedPattern:
        """Compose multiple patterns into a coherent solution."""
        constraints = constraints or {}

        # Check for conflicts
        conflicts = []
        for i, p1 in enumerate(pattern_ids):
            for p2 in pattern_ids[i+1:]:
                if p2 in self.graph.get_conflicts(p1):
                    conflicts.append(f"{p1} conflicts with {p2}")

        # Find synergies
        synergies = []
        for i, p1 in enumerate(pattern_ids):
            enhancements = dict(self.graph.get_enhancements(p1))
            for p2 in pattern_ids:
                if p2 in enhancements:
                    synergies.append((p2, p1, enhancements[p2]))

        # Determine composition type
        comp_type = self._infer_composition_type(pattern_ids, constraints)

        # Sort by dependencies
        ordered = self.graph.topological_sort(pattern_ids)

        # Generate name and description
        patterns = [self.graph.nodes[pid] for pid in ordered if pid in self.graph.nodes]
        name = " + ".join(p.name for p in patterns[:3])
        if len(patterns) > 3:
            name += f" + {len(patterns) - 3} more"

        # Calculate composition score
        base_score = 0.5
        synergy_bonus = sum(s[2] for s in synergies) * 0.1
        conflict_penalty = len(conflicts) * 0.2
        score = min(1.0, max(0.0, base_score + synergy_bonus - conflict_penalty))

        return ComposedPattern(
            id=f"composed_{hashlib.md5('_'.join(ordered).encode()).hexdigest()[:8]}",
            name=name,
            description=self._generate_description(patterns, synergies),
            components=ordered,
            composition_type=comp_type,
            rationale=self._generate_rationale(patterns, synergies, conflicts),
            score=score,
            conflicts=conflicts,
            synergies=synergies
        )

    def _infer_composition_type(self, pattern_ids: List[str],
                                 constraints: Dict) -> str:
        """Infer how patterns should be composed."""
        if constraints.get('sequential'):
            return 'sequential'
        if constraints.get('parallel'):
            return 'parallel'

        # Check dependencies
        has_deps = False
        for pid in pattern_ids:
            if self.graph.get_required_by(pid):
                has_deps = True
                break

        if has_deps:
            return 'hierarchical'

        return 'hybrid'

    def _generate_description(self, patterns: List[Pattern],
                               synergies: List) -> str:
        """Generate a description for the composed pattern."""
        if not patterns:
            return "Empty composition"

        parts = [p.description.split('.')[0] for p in patterns[:3]]
        desc = ". ".join(parts)

        if synergies:
            desc += f". Includes {len(synergies)} synergistic combinations."

        return desc

    def _generate_rationale(self, patterns: List[Pattern],
                            synergies: List, conflicts: List) -> str:
        """Generate rationale for the composition."""
        lines = []

        if patterns:
            lines.append(f"Combines {len(patterns)} patterns for comprehensive solution.")

        if synergies:
            lines.append(f"Synergies: {', '.join(f'{s[0]}+{s[1]}' for s in synergies[:3])}")

        if conflicts:
            lines.append(f"Warning - conflicts detected: {'; '.join(conflicts[:2])}")

        return " ".join(lines)

    def suggest_additions(self, current_patterns: List[str],
                          goal: str, k: int = 3) -> List[Tuple[str, float, str]]:
        """Suggest patterns to add to current composition."""
        suggestions = []

        # Embed the goal
        goal_vec = self.embedder.embed_query(goal)

        # Find compatible patterns
        candidates = self.embedder.nearest_neighbors(
            goal_vec, k=k*3, exclude=set(current_patterns)
        )

        for pid, sim in candidates:
            # Check compatibility
            is_compatible = True
            reason = "Semantically aligned with goal"

            for existing in current_patterns:
                if pid in self.graph.get_conflicts(existing):
                    is_compatible = False
                    break

                # Check if it enhances existing patterns
                enhancements = dict(self.graph.get_enhancements(pid))
                if existing in enhancements:
                    reason = f"Enhances {existing}"
                    sim *= 1.2  # Boost score

            if is_compatible:
                suggestions.append((pid, min(1.0, sim), reason))

        suggestions.sort(key=lambda x: -x[1])
        return suggestions[:k]


# =============================================================================
# CONTEXT-AWARE RECOMMENDER
# =============================================================================

@dataclass
class RecommendationContext:
    """Context for pattern recommendations."""
    scenario: str
    constraints: List[str] = field(default_factory=list)
    existing_patterns: List[str] = field(default_factory=list)
    domain: Optional[PatternDomain] = None
    required_intents: List[PatternIntent] = field(default_factory=list)
    avoid_patterns: List[str] = field(default_factory=list)
    complexity_budget: int = 10  # Max total complexity
    prefer_mature: bool = True


@dataclass
class Recommendation:
    """A pattern recommendation with explanation."""
    pattern_id: str
    pattern_name: str
    score: float
    relevance_factors: Dict[str, float]
    explanation: str
    warnings: List[str]
    alternatives: List[str]


class ContextAwareRecommender:
    """Multi-factor recommendation engine."""

    def __init__(self, graph: PatternGraph, embedder: PatternEmbedding,
                 composer: PatternComposer):
        self.graph = graph
        self.embedder = embedder
        self.composer = composer

        # Factor weights (learnable)
        self.weights = {
            'semantic_similarity': 0.35,
            'intent_alignment': 0.20,
            'domain_match': 0.15,
            'complexity_fit': 0.10,
            'synergy_potential': 0.10,
            'maturity_preference': 0.10
        }

    def recommend(self, context: RecommendationContext,
                  k: int = 5) -> List[Recommendation]:
        """Generate context-aware recommendations."""
        candidates = []

        # Get semantic candidates
        query_vec = self.embedder.embed_query(
            context.scenario,
            context={'domain': context.domain} if context.domain else None
        )

        semantic_matches = self.embedder.nearest_neighbors(
            query_vec, k=k*4,
            exclude=set(context.existing_patterns + context.avoid_patterns)
        )

        for pid, sem_score in semantic_matches:
            if pid not in self.graph.nodes:
                continue

            pattern = self.graph.nodes[pid]
            factors = {}
            warnings = []

            # Semantic similarity
            factors['semantic_similarity'] = sem_score

            # Intent alignment
            if pattern.ontology and context.required_intents:
                matched = sum(1 for i in context.required_intents
                            if i in pattern.ontology.intents)
                factors['intent_alignment'] = matched / len(context.required_intents)
            else:
                factors['intent_alignment'] = 0.5

            # Domain match
            if pattern.ontology and context.domain:
                factors['domain_match'] = 1.0 if pattern.ontology.domain == context.domain else 0.3
            else:
                factors['domain_match'] = 0.5

            # Complexity fit
            if pattern.ontology:
                remaining = context.complexity_budget - sum(
                    self.graph.nodes[p].ontology.complexity
                    for p in context.existing_patterns
                    if p in self.graph.nodes and self.graph.nodes[p].ontology
                )
                if pattern.ontology.complexity <= remaining:
                    factors['complexity_fit'] = 1.0 - (pattern.ontology.complexity / 10)
                else:
                    factors['complexity_fit'] = 0.0
                    warnings.append(f"Complexity {pattern.ontology.complexity} exceeds budget")
            else:
                factors['complexity_fit'] = 0.5

            # Synergy potential
            synergy_score = 0.0
            for existing in context.existing_patterns:
                enhancements = dict(self.graph.get_enhancements(existing))
                if pid in enhancements:
                    synergy_score += enhancements[pid]
                # Also check if this pattern enhances existing
                for target, strength in self.graph.get_required_by(pid):
                    if target == existing:
                        synergy_score += strength * 0.5
            factors['synergy_potential'] = min(1.0, synergy_score)

            # Maturity preference
            if pattern.ontology and context.prefer_mature:
                maturity_scores = {'stable': 1.0, 'experimental': 0.4, 'deprecated': 0.1}
                factors['maturity_preference'] = maturity_scores.get(
                    pattern.ontology.maturity, 0.5
                )
            else:
                factors['maturity_preference'] = 0.5

            # Check conflicts
            for existing in context.existing_patterns:
                if pid in self.graph.get_conflicts(existing):
                    warnings.append(f"Conflicts with {existing}")
                    factors['synergy_potential'] *= 0.1

            # Calculate weighted score
            total_score = sum(
                self.weights[factor] * value
                for factor, value in factors.items()
            )

            # Find alternatives
            alternatives = []
            for alt_pid, _ in semantic_matches:
                if alt_pid != pid and alt_pid in self.graph.nodes:
                    alt = self.graph.nodes[alt_pid]
                    if alt.category == pattern.category:
                        alternatives.append(alt_pid)
                        if len(alternatives) >= 2:
                            break

            candidates.append(Recommendation(
                pattern_id=pid,
                pattern_name=pattern.name,
                score=total_score,
                relevance_factors=factors,
                explanation=self._generate_explanation(pattern, factors, context),
                warnings=warnings,
                alternatives=alternatives
            ))

        # Sort and return top k
        candidates.sort(key=lambda r: -r.score)
        return candidates[:k]

    def _generate_explanation(self, pattern: Pattern, factors: Dict,
                               context: RecommendationContext) -> str:
        """Generate human-readable explanation."""
        parts = []

        # Top factor
        top_factor = max(factors.items(), key=lambda x: x[1])
        factor_explanations = {
            'semantic_similarity': f"Matches '{context.scenario[:30]}...'",
            'intent_alignment': "Aligns with required intents",
            'domain_match': f"Fits {context.domain.value if context.domain else 'your'} domain",
            'complexity_fit': "Fits within complexity budget",
            'synergy_potential': "Synergizes with existing patterns",
            'maturity_preference': "Production-ready pattern"
        }
        parts.append(factor_explanations.get(top_factor[0], "Good overall fit"))

        # Add use case if available
        if pattern.use_cases:
            parts.append(f"Use cases: {pattern.use_cases[0]}")

        return ". ".join(parts)


# =============================================================================
# SEMANTIC CLUSTERING
# =============================================================================

class PatternClusterer:
    """Hierarchical clustering of patterns."""

    def __init__(self, embedder: PatternEmbedding):
        self.embedder = embedder
        self.clusters: Dict[int, List[str]] = {}
        self.cluster_centroids: Dict[int, List[float]] = {}
        self.hierarchy: Dict[int, List[int]] = {}  # parent -> children

    def cluster(self, patterns: List[Pattern], n_clusters: int = 6) -> Dict[int, List[str]]:
        """Cluster patterns using k-means style algorithm."""
        if not patterns:
            return {}

        # Initialize centroids from diverse patterns
        embeddings = {p.id: self.embedder.embed(p) for p in patterns}

        # Pick initial centroids
        centroid_ids = [patterns[0].id]
        for _ in range(1, min(n_clusters, len(patterns))):
            # Find pattern furthest from existing centroids
            max_min_dist = -1
            best_id = None
            for p in patterns:
                if p.id in centroid_ids:
                    continue
                min_dist = min(
                    1 - self.embedder.similarity(embeddings[p.id], embeddings[c])
                    for c in centroid_ids
                )
                if min_dist > max_min_dist:
                    max_min_dist = min_dist
                    best_id = p.id
            if best_id:
                centroid_ids.append(best_id)

        centroids = [embeddings[cid].copy() for cid in centroid_ids]

        # K-means iterations
        for _ in range(10):
            # Assign patterns to clusters
            assignments = defaultdict(list)
            for p in patterns:
                vec = embeddings[p.id]
                best_cluster = max(
                    range(len(centroids)),
                    key=lambda i: self.embedder.similarity(vec, centroids[i])
                )
                assignments[best_cluster].append(p.id)

            # Update centroids
            for cluster_id, pattern_ids in assignments.items():
                if pattern_ids:
                    new_centroid = [0.0] * self.embedder.EMBEDDING_DIM
                    for pid in pattern_ids:
                        for i, v in enumerate(embeddings[pid]):
                            new_centroid[i] += v
                    n = len(pattern_ids)
                    centroids[cluster_id] = [v / n for v in new_centroid]

        # Store results
        self.clusters = dict(assignments)
        self.cluster_centroids = {i: c for i, c in enumerate(centroids)}

        # Assign cluster IDs to patterns
        for cluster_id, pattern_ids in self.clusters.items():
            for pid in pattern_ids:
                for p in patterns:
                    if p.id == pid:
                        p.cluster_id = cluster_id
                        break

        return self.clusters

    def get_cluster_label(self, cluster_id: int) -> str:
        """Generate a label for a cluster based on its patterns."""
        if cluster_id not in self.clusters:
            return f"Cluster {cluster_id}"

        # Find dominant features in centroid
        centroid = self.cluster_centroids.get(cluster_id, [])
        if not centroid:
            return f"Cluster {cluster_id}"

        # Map dimensions back to features
        dim_to_feature = {v[0]: k for k, v in self.embedder.semantic_features.items()}

        top_dims = sorted(range(len(centroid)), key=lambda i: -centroid[i])[:3]
        labels = [dim_to_feature.get(d, f"dim{d}") for d in top_dims]

        return " / ".join(labels).title()


# =============================================================================
# ANTI-PATTERN DETECTOR
# =============================================================================

@dataclass
class AntiPatternWarning:
    """Warning about potential anti-pattern usage."""
    pattern_id: str
    warning_type: str  # 'misuse', 'overuse', 'conflict', 'complexity'
    severity: str  # 'info', 'warning', 'critical'
    message: str
    suggestion: str


class AntiPatternDetector:
    """Detects problematic pattern combinations and usages."""

    def __init__(self, graph: PatternGraph):
        self.graph = graph

        # Known anti-patterns
        self.anti_pattern_rules = [
            {
                'name': 'complexity_explosion',
                'condition': lambda patterns: sum(
                    p.ontology.complexity if p.ontology else 5 for p in patterns
                ) > 25,
                'severity': 'warning',
                'message': "Total complexity exceeds recommended threshold",
                'suggestion': "Consider simplifying or phasing implementation"
            },
            {
                'name': 'missing_observability',
                'condition': lambda patterns: (
                    len(patterns) > 3 and
                    not any(p.ontology and p.ontology.domain == PatternDomain.OBSERVABILITY
                           for p in patterns)
                ),
                'severity': 'warning',
                'message': "Complex system without observability patterns",
                'suggestion': "Add monitoring, logging, or tracing patterns"
            },
            {
                'name': 'retry_without_circuit_breaker',
                'condition': lambda patterns: (
                    any('retry' in p.id for p in patterns) and
                    not any('circuit' in p.id for p in patterns)
                ),
                'severity': 'info',
                'message': "Retry pattern without circuit breaker",
                'suggestion': "Consider adding circuit breaker to prevent cascading failures"
            }
        ]

    def analyze(self, patterns: List[Pattern]) -> List[AntiPatternWarning]:
        """Analyze patterns for anti-patterns."""
        warnings = []

        # Check rules
        for rule in self.anti_pattern_rules:
            if rule['condition'](patterns):
                warnings.append(AntiPatternWarning(
                    pattern_id='*',
                    warning_type=rule['name'],
                    severity=rule['severity'],
                    message=rule['message'],
                    suggestion=rule['suggestion']
                ))

        # Check pattern-specific anti-patterns
        for pattern in patterns:
            if pattern.anti_patterns:
                for ap in pattern.anti_patterns:
                    warnings.append(AntiPatternWarning(
                        pattern_id=pattern.id,
                        warning_type='documented_antipattern',
                        severity='info',
                        message=ap,
                        suggestion="Review pattern documentation"
                    ))

        # Check conflicts
        for i, p1 in enumerate(patterns):
            conflicts = self.graph.get_conflicts(p1.id)
            for p2 in patterns[i+1:]:
                if p2.id in conflicts:
                    warnings.append(AntiPatternWarning(
                        pattern_id=f"{p1.id}+{p2.id}",
                        warning_type='conflict',
                        severity='critical',
                        message=f"Conflicting patterns: {p1.name} and {p2.name}",
                        suggestion="Choose one or refactor to avoid conflict"
                    ))

        return warnings


# =============================================================================
# PATTERN KNOWLEDGE BASE
# =============================================================================

class PatternKnowledgeBase:
    """Central knowledge base for patterns."""

    def __init__(self):
        self.graph = PatternGraph()
        self.embedder = PatternEmbedding()
        self.composer = PatternComposer(self.graph, self.embedder)
        self.recommender = ContextAwareRecommender(
            self.graph, self.embedder, self.composer
        )
        self.clusterer = PatternClusterer(self.embedder)
        self.detector = AntiPatternDetector(self.graph)

        self._load_patterns()
        self._load_relationships()
        self._cluster_patterns()

    def _load_patterns(self):
        """Load comprehensive pattern catalog."""
        patterns = [
            # === RESILIENCE PATTERNS ===
            Pattern(
                id="retry_backoff",
                name="Retry with Exponential Backoff",
                description="Automatically retry failed operations with increasing delays. Prevents thundering herd and allows transient failures to resolve.",
                keywords=["retry", "backoff", "exponential", "fault", "tolerance", "transient"],
                category="resilience",
                ontology=PatternOntology(
                    domain=PatternDomain.RESILIENCE,
                    intents=[PatternIntent.RELIABILITY, PatternIntent.AVAILABILITY],
                    complexity=3,
                    maturity="stable",
                    prerequisites=["idempotency"],
                    related_concepts=["jitter", "max_retries", "retry_budget"]
                ),
                use_cases=[
                    "Network calls to external services",
                    "Database connections during failover",
                    "Message queue consumption"
                ],
                anti_patterns=["Retrying non-idempotent operations without safeguards"],
                trade_offs={"latency": "Increases during failures", "complexity": "Low"}
            ),

            Pattern(
                id="circuit_breaker",
                name="Circuit Breaker",
                description="Prevents cascade failures by stopping calls to failing services. Transitions through closed, open, and half-open states.",
                keywords=["circuit", "breaker", "fail", "fast", "cascade", "protection"],
                category="resilience",
                ontology=PatternOntology(
                    domain=PatternDomain.RESILIENCE,
                    intents=[PatternIntent.AVAILABILITY, PatternIntent.ISOLATION],
                    complexity=4,
                    maturity="stable",
                    prerequisites=["failure_detection", "fallback_strategy"],
                    related_concepts=["threshold", "reset_timeout", "half_open"]
                ),
                use_cases=[
                    "Microservice communication",
                    "Third-party API integration",
                    "Database connection pools"
                ],
                anti_patterns=["Setting thresholds too high/low"],
                metrics=["failure_rate", "state_transitions", "trip_count"]
            ),

            Pattern(
                id="bulkhead",
                name="Bulkhead Isolation",
                description="Isolates components to prevent failures from spreading. Like ship bulkheads, contains damage to one section.",
                keywords=["bulkhead", "isolate", "partition", "contain", "failure", "limit"],
                category="resilience",
                ontology=PatternOntology(
                    domain=PatternDomain.RESILIENCE,
                    intents=[PatternIntent.ISOLATION, PatternIntent.AVAILABILITY],
                    complexity=5,
                    maturity="stable",
                    prerequisites=["resource_management"],
                    related_concepts=["thread_pools", "connection_pools", "semaphores"]
                ),
                use_cases=[
                    "Multi-tenant systems",
                    "Resource-intensive operations",
                    "Critical vs non-critical paths"
                ]
            ),

            Pattern(
                id="fallback",
                name="Fallback Strategy",
                description="Provides alternative behavior when primary operation fails. Degrades gracefully rather than failing completely.",
                keywords=["fallback", "default", "graceful", "degradation", "alternative"],
                category="resilience",
                ontology=PatternOntology(
                    domain=PatternDomain.RESILIENCE,
                    intents=[PatternIntent.AVAILABILITY, PatternIntent.RELIABILITY],
                    complexity=2,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["cache", "default_value", "static_response"]
                ),
                use_cases=[
                    "Feature flags with safe defaults",
                    "Cached responses when live data unavailable",
                    "Simplified processing paths"
                ]
            ),

            Pattern(
                id="timeout",
                name="Timeout Control",
                description="Limits how long operations can take. Prevents resource exhaustion from hung operations.",
                keywords=["timeout", "deadline", "limit", "hung", "stuck", "cancel"],
                category="resilience",
                ontology=PatternOntology(
                    domain=PatternDomain.RESILIENCE,
                    intents=[PatternIntent.AVAILABILITY],
                    complexity=2,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["deadline_propagation", "cancellation"]
                )
            ),

            Pattern(
                id="health_check",
                name="Health Check Endpoint",
                description="Exposes service health for orchestrators and load balancers. Enables automated recovery and routing.",
                keywords=["health", "check", "liveness", "readiness", "probe", "status"],
                category="resilience",
                ontology=PatternOntology(
                    domain=PatternDomain.OBSERVABILITY,
                    intents=[PatternIntent.AVAILABILITY],
                    complexity=2,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["kubernetes_probes", "load_balancer"]
                )
            ),

            # === DATA FLOW PATTERNS ===
            Pattern(
                id="saga",
                name="Saga Pattern",
                description="Manages distributed transactions through a sequence of local transactions with compensating actions for rollback.",
                keywords=["saga", "transaction", "distributed", "compensate", "rollback", "consistency"],
                category="transactions",
                ontology=PatternOntology(
                    domain=PatternDomain.DATA_FLOW,
                    intents=[PatternIntent.CONSISTENCY, PatternIntent.RELIABILITY],
                    complexity=8,
                    maturity="stable",
                    prerequisites=["idempotency", "compensation_logic"],
                    related_concepts=["choreography", "orchestration", "eventual_consistency"]
                ),
                use_cases=[
                    "Order processing across microservices",
                    "Financial transactions",
                    "Travel booking systems"
                ],
                anti_patterns=["Partial compensation", "Missing idempotency keys"]
            ),

            Pattern(
                id="two_phase_commit",
                name="Two-Phase Commit",
                description="Ensures atomic commits across distributed resources through prepare and commit phases.",
                keywords=["2pc", "atomic", "commit", "prepare", "coordinator", "transaction"],
                category="transactions",
                ontology=PatternOntology(
                    domain=PatternDomain.DATA_FLOW,
                    intents=[PatternIntent.CONSISTENCY],
                    complexity=9,
                    maturity="stable",
                    prerequisites=["xa_support", "transaction_coordinator"],
                    related_concepts=["xa", "distributed_lock", "consensus"]
                ),
                trade_offs={"consistency": "Strong", "availability": "Reduced during failures"}
            ),

            Pattern(
                id="outbox",
                name="Transactional Outbox",
                description="Ensures reliable message publishing by writing to an outbox table in the same transaction as business data.",
                keywords=["outbox", "reliable", "messaging", "publish", "transaction", "event"],
                category="integration",
                ontology=PatternOntology(
                    domain=PatternDomain.INTEGRATION,
                    intents=[PatternIntent.RELIABILITY, PatternIntent.CONSISTENCY],
                    complexity=5,
                    maturity="stable",
                    prerequisites=["database", "message_broker"],
                    related_concepts=["cdc", "polling_publisher", "log_tailing"]
                )
            ),

            Pattern(
                id="cqrs",
                name="CQRS",
                description="Separates read and write models for optimized performance and scalability.",
                keywords=["cqrs", "read", "write", "separate", "query", "command"],
                category="architecture",
                ontology=PatternOntology(
                    domain=PatternDomain.DATA_FLOW,
                    intents=[PatternIntent.SCALABILITY, PatternIntent.EFFICIENCY],
                    complexity=7,
                    maturity="stable",
                    prerequisites=["event_sourcing_optional"],
                    related_concepts=["event_sourcing", "materialized_views"]
                )
            ),

            Pattern(
                id="event_sourcing",
                name="Event Sourcing",
                description="Stores state as a sequence of events rather than current state. Enables complete audit trail and temporal queries.",
                keywords=["event", "sourcing", "append", "history", "audit", "replay"],
                category="architecture",
                ontology=PatternOntology(
                    domain=PatternDomain.DATA_FLOW,
                    intents=[PatternIntent.CONSISTENCY, PatternIntent.COMPLIANCE],
                    complexity=8,
                    maturity="stable",
                    prerequisites=["event_store", "event_schema"],
                    related_concepts=["projections", "snapshots", "temporal_queries"]
                )
            ),

            # === ORCHESTRATION PATTERNS ===
            Pattern(
                id="state_machine",
                name="State Machine",
                description="Models complex workflows as explicit states and transitions. Ensures valid state progression.",
                keywords=["state", "machine", "transition", "workflow", "fsm", "status"],
                category="control_flow",
                ontology=PatternOntology(
                    domain=PatternDomain.ORCHESTRATION,
                    intents=[PatternIntent.CONSISTENCY, PatternIntent.COORDINATION],
                    complexity=5,
                    maturity="stable",
                    prerequisites=["state_definition"],
                    related_concepts=["transitions", "guards", "actions"]
                )
            ),

            Pattern(
                id="choreography",
                name="Event Choreography",
                description="Services react to events without central coordination. Loose coupling through event-driven architecture.",
                keywords=["choreography", "event", "driven", "decouple", "publish", "react"],
                category="integration",
                ontology=PatternOntology(
                    domain=PatternDomain.INTEGRATION,
                    intents=[PatternIntent.SCALABILITY, PatternIntent.ISOLATION],
                    complexity=6,
                    maturity="stable",
                    prerequisites=["event_bus", "event_schema"],
                    related_concepts=["pub_sub", "event_driven"]
                )
            ),

            Pattern(
                id="orchestration",
                name="Central Orchestration",
                description="A central orchestrator coordinates service calls and manages workflow state.",
                keywords=["orchestration", "coordinator", "central", "workflow", "sequence"],
                category="integration",
                ontology=PatternOntology(
                    domain=PatternDomain.ORCHESTRATION,
                    intents=[PatternIntent.COORDINATION, PatternIntent.CONSISTENCY],
                    complexity=5,
                    maturity="stable",
                    prerequisites=["orchestrator_service"],
                    related_concepts=["workflow_engine", "step_functions"]
                )
            ),

            Pattern(
                id="fan_out_fan_in",
                name="Fan-Out/Fan-In",
                description="Distributes work across parallel workers then aggregates results. Enables parallel processing.",
                keywords=["fan", "out", "in", "parallel", "scatter", "gather", "aggregate"],
                category="parallelism",
                ontology=PatternOntology(
                    domain=PatternDomain.PERFORMANCE,
                    intents=[PatternIntent.EFFICIENCY, PatternIntent.SCALABILITY],
                    complexity=4,
                    maturity="stable",
                    prerequisites=["work_partitioning"],
                    related_concepts=["map_reduce", "scatter_gather"]
                )
            ),

            Pattern(
                id="map_reduce",
                name="Map-Reduce",
                description="Maps operations across data partitions then reduces to final result. Classic parallel processing pattern.",
                keywords=["map", "reduce", "parallel", "partition", "aggregate", "batch"],
                category="parallelism",
                ontology=PatternOntology(
                    domain=PatternDomain.PERFORMANCE,
                    intents=[PatternIntent.SCALABILITY, PatternIntent.EFFICIENCY],
                    complexity=5,
                    maturity="stable",
                    prerequisites=["data_partitioning", "associative_reduce"],
                    related_concepts=["hadoop", "spark", "parallel_aggregate"]
                )
            ),

            # === PERFORMANCE PATTERNS ===
            Pattern(
                id="cache_aside",
                name="Cache-Aside",
                description="Application manages cache population on cache miss. Simple and widely applicable caching strategy.",
                keywords=["cache", "aside", "lazy", "load", "read", "through"],
                category="performance",
                ontology=PatternOntology(
                    domain=PatternDomain.PERFORMANCE,
                    intents=[PatternIntent.EFFICIENCY],
                    complexity=3,
                    maturity="stable",
                    prerequisites=["cache_infrastructure"],
                    related_concepts=["ttl", "eviction", "invalidation"]
                )
            ),

            Pattern(
                id="write_behind",
                name="Write-Behind Cache",
                description="Writes to cache first, asynchronously persists to storage. Improves write performance.",
                keywords=["write", "behind", "async", "cache", "persist", "buffer"],
                category="performance",
                ontology=PatternOntology(
                    domain=PatternDomain.PERFORMANCE,
                    intents=[PatternIntent.EFFICIENCY],
                    complexity=6,
                    maturity="stable",
                    prerequisites=["durable_cache", "async_persistence"],
                    related_concepts=["write_coalescing", "batch_writes"]
                )
            ),

            Pattern(
                id="rate_limiter",
                name="Rate Limiting",
                description="Controls the rate of requests to prevent overload. Protects services from abuse and ensures fair usage.",
                keywords=["rate", "limit", "throttle", "quota", "backpressure", "control"],
                category="performance",
                ontology=PatternOntology(
                    domain=PatternDomain.PERFORMANCE,
                    intents=[PatternIntent.AVAILABILITY, PatternIntent.ISOLATION],
                    complexity=4,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["token_bucket", "leaky_bucket", "sliding_window"]
                )
            ),

            Pattern(
                id="load_shedding",
                name="Load Shedding",
                description="Drops excess load during overload to maintain service for remaining requests.",
                keywords=["load", "shed", "drop", "overload", "protect", "capacity"],
                category="performance",
                ontology=PatternOntology(
                    domain=PatternDomain.PERFORMANCE,
                    intents=[PatternIntent.AVAILABILITY],
                    complexity=5,
                    maturity="stable",
                    prerequisites=["load_detection"],
                    related_concepts=["priority_queues", "graceful_degradation"]
                )
            ),

            Pattern(
                id="connection_pool",
                name="Connection Pooling",
                description="Reuses expensive connections instead of creating new ones. Reduces latency and resource usage.",
                keywords=["connection", "pool", "reuse", "resource", "efficient"],
                category="performance",
                ontology=PatternOntology(
                    domain=PatternDomain.PERFORMANCE,
                    intents=[PatternIntent.EFFICIENCY],
                    complexity=3,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["max_size", "idle_timeout", "health_check"]
                )
            ),

            # === OBSERVABILITY PATTERNS ===
            Pattern(
                id="distributed_tracing",
                name="Distributed Tracing",
                description="Tracks requests across service boundaries with correlation IDs. Enables debugging distributed systems.",
                keywords=["trace", "span", "correlation", "distributed", "debug", "request"],
                category="observability",
                ontology=PatternOntology(
                    domain=PatternDomain.OBSERVABILITY,
                    intents=[PatternIntent.EFFICIENCY],  # Efficiency in debugging
                    complexity=5,
                    maturity="stable",
                    prerequisites=["trace_context_propagation"],
                    related_concepts=["opentelemetry", "jaeger", "zipkin"]
                )
            ),

            Pattern(
                id="structured_logging",
                name="Structured Logging",
                description="Logs as structured data (JSON) rather than text. Enables querying and analysis at scale.",
                keywords=["log", "structured", "json", "query", "analyze", "search"],
                category="observability",
                ontology=PatternOntology(
                    domain=PatternDomain.OBSERVABILITY,
                    intents=[PatternIntent.EFFICIENCY],
                    complexity=2,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["log_levels", "correlation_id", "elk_stack"]
                )
            ),

            Pattern(
                id="metrics_collection",
                name="Metrics Collection",
                description="Collects and aggregates numerical metrics for monitoring and alerting.",
                keywords=["metrics", "prometheus", "gauge", "counter", "histogram", "monitor"],
                category="observability",
                ontology=PatternOntology(
                    domain=PatternDomain.OBSERVABILITY,
                    intents=[PatternIntent.AVAILABILITY],
                    complexity=3,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["prometheus", "grafana", "percentiles"]
                )
            ),

            Pattern(
                id="audit_trail",
                name="Audit Trail",
                description="Records all significant actions for compliance and debugging. Immutable history of changes.",
                keywords=["audit", "trail", "log", "compliance", "history", "record"],
                category="governance",
                ontology=PatternOntology(
                    domain=PatternDomain.GOVERNANCE,
                    intents=[PatternIntent.COMPLIANCE],
                    complexity=4,
                    maturity="stable",
                    prerequisites=["immutable_storage"],
                    related_concepts=["event_sourcing", "append_only"]
                )
            ),

            # === SECURITY PATTERNS ===
            Pattern(
                id="api_gateway",
                name="API Gateway",
                description="Single entry point for all API calls. Handles auth, routing, rate limiting, and cross-cutting concerns.",
                keywords=["api", "gateway", "entry", "point", "route", "auth"],
                category="security",
                ontology=PatternOntology(
                    domain=PatternDomain.SECURITY,
                    intents=[PatternIntent.ISOLATION],
                    complexity=6,
                    maturity="stable",
                    prerequisites=["authentication_provider"],
                    related_concepts=["reverse_proxy", "load_balancer"]
                )
            ),

            Pattern(
                id="token_bucket",
                name="Token Bucket",
                description="Rate limiting using tokens that replenish over time. Allows bursts while maintaining average rate.",
                keywords=["token", "bucket", "rate", "burst", "replenish", "limit"],
                category="performance",
                ontology=PatternOntology(
                    domain=PatternDomain.PERFORMANCE,
                    intents=[PatternIntent.AVAILABILITY],
                    complexity=3,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["leaky_bucket", "sliding_window"]
                )
            ),

            Pattern(
                id="service_mesh",
                name="Service Mesh",
                description="Infrastructure layer handling service-to-service communication with observability, security, and reliability.",
                keywords=["mesh", "sidecar", "proxy", "istio", "linkerd", "envoy"],
                category="infrastructure",
                ontology=PatternOntology(
                    domain=PatternDomain.INTEGRATION,
                    intents=[PatternIntent.ISOLATION, PatternIntent.RELIABILITY],
                    complexity=8,
                    maturity="stable",
                    prerequisites=["container_orchestration"],
                    related_concepts=["sidecar", "mTLS", "traffic_management"]
                )
            ),

            # === DEPLOYMENT PATTERNS ===
            Pattern(
                id="blue_green",
                name="Blue-Green Deployment",
                description="Maintains two identical environments, switching traffic between them for zero-downtime deployments.",
                keywords=["blue", "green", "deploy", "switch", "zero", "downtime"],
                category="deployment",
                ontology=PatternOntology(
                    domain=PatternDomain.ORCHESTRATION,
                    intents=[PatternIntent.AVAILABILITY, PatternIntent.RELIABILITY],
                    complexity=5,
                    maturity="stable",
                    prerequisites=["duplicate_infrastructure"],
                    related_concepts=["rollback", "traffic_routing"]
                )
            ),

            Pattern(
                id="canary",
                name="Canary Deployment",
                description="Gradually rolls out changes to a subset of users before full deployment. Limits blast radius.",
                keywords=["canary", "gradual", "rollout", "percentage", "traffic", "split"],
                category="deployment",
                ontology=PatternOntology(
                    domain=PatternDomain.ORCHESTRATION,
                    intents=[PatternIntent.RELIABILITY],
                    complexity=6,
                    maturity="stable",
                    prerequisites=["traffic_splitting"],
                    related_concepts=["feature_flags", "a_b_testing"]
                )
            ),

            Pattern(
                id="feature_flag",
                name="Feature Flags",
                description="Toggles features on/off without deployment. Enables gradual rollouts and quick rollbacks.",
                keywords=["feature", "flag", "toggle", "release", "control", "dynamic"],
                category="deployment",
                ontology=PatternOntology(
                    domain=PatternDomain.ORCHESTRATION,
                    intents=[PatternIntent.AVAILABILITY, PatternIntent.ISOLATION],
                    complexity=4,
                    maturity="stable",
                    prerequisites=["flag_management"],
                    related_concepts=["percentage_rollout", "user_targeting"]
                )
            ),

            # === SCHEDULING PATTERNS ===
            Pattern(
                id="priority_queue",
                name="Priority Queue",
                description="Processes high-priority items before low-priority ones. Ensures critical work is handled first.",
                keywords=["priority", "queue", "order", "important", "urgent", "rank"],
                category="scheduling",
                ontology=PatternOntology(
                    domain=PatternDomain.ORCHESTRATION,
                    intents=[PatternIntent.EFFICIENCY],
                    complexity=3,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["heap", "priority_levels"]
                )
            ),

            Pattern(
                id="scheduler",
                name="Job Scheduler",
                description="Schedules tasks to run at specific times or intervals. Manages recurring operations.",
                keywords=["schedule", "cron", "timer", "recurring", "job", "batch"],
                category="scheduling",
                ontology=PatternOntology(
                    domain=PatternDomain.ORCHESTRATION,
                    intents=[PatternIntent.COORDINATION],
                    complexity=4,
                    maturity="stable",
                    prerequisites=[],
                    related_concepts=["cron", "at_most_once", "at_least_once"]
                )
            ),

            Pattern(
                id="work_stealing",
                name="Work Stealing",
                description="Idle workers steal tasks from busy workers' queues. Balances load automatically.",
                keywords=["work", "steal", "balance", "idle", "queue", "load"],
                category="parallelism",
                ontology=PatternOntology(
                    domain=PatternDomain.PERFORMANCE,
                    intents=[PatternIntent.EFFICIENCY, PatternIntent.SCALABILITY],
                    complexity=6,
                    maturity="stable",
                    prerequisites=["work_queue_per_worker"],
                    related_concepts=["fork_join", "deque"]
                )
            ),

            # === MESSAGING PATTERNS ===
            Pattern(
                id="dead_letter_queue",
                name="Dead Letter Queue",
                description="Captures messages that can't be processed for later analysis and retry.",
                keywords=["dead", "letter", "queue", "dlq", "failed", "poison"],
                category="integration",
                ontology=PatternOntology(
                    domain=PatternDomain.INTEGRATION,
                    intents=[PatternIntent.RELIABILITY],
                    complexity=3,
                    maturity="stable",
                    prerequisites=["message_queue"],
                    related_concepts=["poison_message", "retry_queue"]
                )
            ),

            Pattern(
                id="idempotent_receiver",
                name="Idempotent Receiver",
                description="Handles duplicate messages safely by tracking processed message IDs.",
                keywords=["idempotent", "duplicate", "exactly", "once", "dedup", "key"],
                category="integration",
                ontology=PatternOntology(
                    domain=PatternDomain.INTEGRATION,
                    intents=[PatternIntent.CONSISTENCY, PatternIntent.RELIABILITY],
                    complexity=4,
                    maturity="stable",
                    prerequisites=["message_id_tracking"],
                    related_concepts=["idempotency_key", "deduplication"]
                )
            ),

            Pattern(
                id="competing_consumers",
                name="Competing Consumers",
                description="Multiple consumers compete for messages from a queue. Enables horizontal scaling.",
                keywords=["competing", "consumer", "parallel", "scale", "worker", "queue"],
                category="integration",
                ontology=PatternOntology(
                    domain=PatternDomain.INTEGRATION,
                    intents=[PatternIntent.SCALABILITY],
                    complexity=3,
                    maturity="stable",
                    prerequisites=["message_queue"],
                    related_concepts=["prefetch", "acknowledgment"]
                )
            ),

            # === APPROVAL AND GOVERNANCE ===
            Pattern(
                id="approval_gate",
                name="Approval Gate",
                description="Requires explicit approval before proceeding. Ensures human oversight at critical points.",
                keywords=["approval", "gate", "manual", "review", "authorize", "checkpoint"],
                category="governance",
                ontology=PatternOntology(
                    domain=PatternDomain.GOVERNANCE,
                    intents=[PatternIntent.COMPLIANCE, PatternIntent.CONSISTENCY],
                    complexity=4,
                    maturity="stable",
                    prerequisites=["notification_system"],
                    related_concepts=["workflow", "authorization"]
                )
            ),

            Pattern(
                id="checkpoint",
                name="Checkpoint/Restart",
                description="Saves progress at checkpoints to enable restart from last good state after failures.",
                keywords=["checkpoint", "restart", "resume", "progress", "save", "state"],
                category="resilience",
                ontology=PatternOntology(
                    domain=PatternDomain.RESILIENCE,
                    intents=[PatternIntent.RELIABILITY],
                    complexity=5,
                    maturity="stable",
                    prerequisites=["state_serialization"],
                    related_concepts=["snapshot", "recovery"]
                )
            ),
        ]

        for pattern in patterns:
            self.graph.add_pattern(pattern)
            self.embedder.embed(pattern)

    def _load_relationships(self):
        """Load pattern relationships."""
        relationships = [
            # Retry + Circuit Breaker synergy
            PatternRelation("retry_backoff", "circuit_breaker", RelationType.ENHANCES, 0.9,
                          "Retry handles transient failures, circuit breaker prevents cascade"),
            PatternRelation("circuit_breaker", "retry_backoff", RelationType.ENHANCES, 0.8,
                          "Circuit breaker protects retry from overwhelming failing service"),

            # Resilience chain
            PatternRelation("timeout", "retry_backoff", RelationType.PRECEDES, 0.9,
                          "Set timeout before retry logic"),
            PatternRelation("fallback", "circuit_breaker", RelationType.ENHANCES, 0.85,
                          "Fallback provides response when circuit is open"),
            PatternRelation("bulkhead", "circuit_breaker", RelationType.ENHANCES, 0.8,
                          "Bulkhead isolates failures, circuit breaker stops cascades"),

            # Saga relationships
            PatternRelation("saga", "idempotent_receiver", RelationType.REQUIRES, 0.95,
                          "Saga requires idempotent operations for safe retry"),
            PatternRelation("saga", "two_phase_commit", RelationType.ALTERNATIVE, 0.8,
                          "Choose saga or 2PC for distributed transactions"),
            PatternRelation("saga", "outbox", RelationType.ENHANCES, 0.85,
                          "Outbox ensures reliable event publishing in saga"),

            # Event architecture
            PatternRelation("event_sourcing", "cqrs", RelationType.ENHANCES, 0.9,
                          "Event sourcing naturally supports CQRS"),
            PatternRelation("choreography", "event_sourcing", RelationType.ENHANCES, 0.7,
                          "Events from sourcing drive choreography"),
            PatternRelation("choreography", "orchestration", RelationType.ALTERNATIVE, 0.9,
                          "Choose choreography or orchestration"),

            # Caching
            PatternRelation("cache_aside", "write_behind", RelationType.ALTERNATIVE, 0.7,
                          "Different caching strategies"),
            PatternRelation("cache_aside", "fallback", RelationType.ENHANCES, 0.6,
                          "Cache can serve as fallback"),

            # Rate limiting family
            PatternRelation("rate_limiter", "token_bucket", RelationType.COMPOSES, 0.9,
                          "Token bucket is an implementation of rate limiting"),
            PatternRelation("rate_limiter", "load_shedding", RelationType.ENHANCES, 0.8,
                          "Rate limiting before load shedding kicks in"),
            PatternRelation("rate_limiter", "circuit_breaker", RelationType.ENHANCES, 0.7,
                          "Rate limit before circuit breaks"),

            # Observability synergies
            PatternRelation("distributed_tracing", "structured_logging", RelationType.ENHANCES, 0.85,
                          "Correlation IDs link traces and logs"),
            PatternRelation("metrics_collection", "health_check", RelationType.ENHANCES, 0.8,
                          "Metrics inform health check logic"),
            PatternRelation("audit_trail", "event_sourcing", RelationType.ENHANCES, 0.9,
                          "Event log provides audit trail"),

            # Infrastructure
            PatternRelation("service_mesh", "distributed_tracing", RelationType.COMPOSES, 0.8,
                          "Service mesh provides tracing"),
            PatternRelation("service_mesh", "rate_limiter", RelationType.COMPOSES, 0.8,
                          "Service mesh handles rate limiting"),
            PatternRelation("api_gateway", "rate_limiter", RelationType.COMPOSES, 0.85,
                          "Gateway applies rate limits"),

            # Deployment
            PatternRelation("blue_green", "canary", RelationType.ALTERNATIVE, 0.7,
                          "Different deployment strategies"),
            PatternRelation("feature_flag", "canary", RelationType.ENHANCES, 0.85,
                          "Feature flags enable canary-style rollouts"),

            # Messaging
            PatternRelation("dead_letter_queue", "competing_consumers", RelationType.ENHANCES, 0.8,
                          "DLQ handles consumer failures"),
            PatternRelation("idempotent_receiver", "competing_consumers", RelationType.ENHANCES, 0.9,
                          "Idempotency needed for competing consumers"),

            # Scheduling
            PatternRelation("priority_queue", "load_shedding", RelationType.ENHANCES, 0.7,
                          "Priority determines what gets shed"),
            PatternRelation("scheduler", "checkpoint", RelationType.ENHANCES, 0.8,
                          "Checkpoints enable job restart"),

            # Conflicts
            PatternRelation("two_phase_commit", "saga", RelationType.CONFLICTS, 0.3,
                          "Don't mix 2PC and saga for same transaction"),
        ]

        for rel in relationships:
            self.graph.add_relation(rel)

    def _cluster_patterns(self):
        """Cluster patterns for organization."""
        patterns = list(self.graph.nodes.values())
        self.clusterer.cluster(patterns, n_clusters=6)

    def search(self, query: str, k: int = 5,
               context: Optional[Dict] = None) -> List[Tuple[Pattern, float]]:
        """Search for patterns matching query."""
        query_vec = self.embedder.embed_query(query, context)
        neighbors = self.embedder.nearest_neighbors(query_vec, k=k)

        results = []
        for pid, score in neighbors:
            if pid in self.graph.nodes:
                results.append((self.graph.nodes[pid], score))

        return results

    def recommend(self, scenario: str,
                  existing: List[str] = None,
                  domain: Optional[PatternDomain] = None,
                  k: int = 5) -> List[Recommendation]:
        """Get context-aware recommendations."""
        context = RecommendationContext(
            scenario=scenario,
            existing_patterns=existing or [],
            domain=domain
        )
        return self.recommender.recommend(context, k)

    def compose(self, pattern_ids: List[str]) -> ComposedPattern:
        """Compose patterns into a solution."""
        return self.composer.compose(pattern_ids)

    def analyze_antipatterns(self, pattern_ids: List[str]) -> List[AntiPatternWarning]:
        """Check for anti-patterns."""
        patterns = [self.graph.nodes[pid] for pid in pattern_ids if pid in self.graph.nodes]
        return self.detector.analyze(patterns)

    def get_related(self, pattern_id: str) -> Dict[str, List]:
        """Get related patterns."""
        return {
            'requires': self.graph.get_required_by(pattern_id),
            'enhances': self.graph.get_enhancements(pattern_id),
            'conflicts': self.graph.get_conflicts(pattern_id)
        }

    def get_cluster_info(self) -> Dict[int, Dict]:
        """Get cluster information."""
        info = {}
        for cluster_id, pattern_ids in self.clusterer.clusters.items():
            patterns = [self.graph.nodes[pid] for pid in pattern_ids if pid in self.graph.nodes]
            info[cluster_id] = {
                'label': self.clusterer.get_cluster_label(cluster_id),
                'size': len(pattern_ids),
                'patterns': [p.name for p in patterns]
            }
        return info


# =============================================================================
# PROBABILISTIC REASONING ENGINE
# =============================================================================

@dataclass
class ProbabilisticMatch:
    """A match with probability distribution."""
    pattern_id: str
    mean_score: float
    confidence_interval: Tuple[float, float]  # 95% CI
    variance: float
    factors: Dict[str, float]


class BayesianPatternMatcher:
    """
    Bayesian inference for pattern matching.

    Uses prior beliefs + evidence to compute posterior probabilities.
    """

    def __init__(self, kb: 'PatternKnowledgeBase'):
        self.kb = kb

        # Prior probabilities (base rates of pattern usefulness)
        self.priors: Dict[str, float] = {}
        self._compute_priors()

        # Likelihood tables P(evidence | pattern)
        self.likelihoods: Dict[str, Dict[str, float]] = {}
        self._build_likelihood_tables()

        # Usage statistics for learning
        self.usage_counts: Dict[str, int] = defaultdict(int)
        self.success_counts: Dict[str, int] = defaultdict(int)
        self.co_occurrence: Dict[Tuple[str, str], int] = defaultdict(int)

    def _compute_priors(self):
        """Compute prior probabilities based on pattern characteristics."""
        for pid, pattern in self.kb.graph.nodes.items():
            # Base prior on maturity and complexity
            base = 0.5

            if pattern.ontology:
                # Stable patterns get higher priors
                maturity_bonus = {'stable': 0.2, 'experimental': -0.1, 'deprecated': -0.3}
                base += maturity_bonus.get(pattern.ontology.maturity, 0)

                # Lower complexity = higher prior (easier to use)
                complexity_factor = (10 - pattern.ontology.complexity) / 20
                base += complexity_factor

            # Patterns with more relationships are more useful
            rel_count = len(self.kb.graph.adjacency.get(pid, {}))
            rel_bonus = min(0.1, rel_count * 0.02)
            base += rel_bonus

            self.priors[pid] = max(0.1, min(0.9, base))

    def _build_likelihood_tables(self):
        """Build P(evidence | pattern) tables."""
        # Evidence types: domain match, keyword match, intent match
        for pid, pattern in self.kb.graph.nodes.items():
            self.likelihoods[pid] = {}

            # Domain evidence
            if pattern.ontology:
                domain = pattern.ontology.domain.value
                self.likelihoods[pid][f'domain_{domain}'] = 0.9

                # Intent evidence
                for intent in pattern.ontology.intents:
                    self.likelihoods[pid][f'intent_{intent.value}'] = 0.8

            # Keyword evidence
            for kw in pattern.keywords:
                self.likelihoods[pid][f'keyword_{kw}'] = 0.85

            # Category evidence
            self.likelihoods[pid][f'category_{pattern.category}'] = 0.75

    def infer(self, evidence: Dict[str, bool],
              top_k: int = 5) -> List[ProbabilisticMatch]:
        """
        Bayesian inference given evidence.

        P(pattern | evidence) = P(evidence | pattern) * P(pattern) / P(evidence)
        """
        posteriors = []

        for pid, prior in self.priors.items():
            # Compute likelihood of evidence given pattern
            likelihood = 1.0
            factors = {}

            for ev, present in evidence.items():
                ev_likelihood = self.likelihoods[pid].get(ev, 0.3)
                if present:
                    likelihood *= ev_likelihood
                    factors[ev] = ev_likelihood
                else:
                    likelihood *= (1 - ev_likelihood * 0.5)

            # Posterior (unnormalized)
            posterior = likelihood * prior

            # Estimate variance based on evidence coverage
            evidence_coverage = sum(1 for ev in evidence if ev in self.likelihoods[pid])
            variance = 0.1 / (1 + evidence_coverage)

            # 95% confidence interval (assuming normal approximation)
            ci_width = 1.96 * math.sqrt(variance)
            ci = (max(0, posterior - ci_width), min(1, posterior + ci_width))

            posteriors.append(ProbabilisticMatch(
                pattern_id=pid,
                mean_score=posterior,
                confidence_interval=ci,
                variance=variance,
                factors=factors
            ))

        # Normalize
        total = sum(p.mean_score for p in posteriors)
        if total > 0:
            for p in posteriors:
                p.mean_score /= total

        # Sort and return top k
        posteriors.sort(key=lambda x: -x.mean_score)
        return posteriors[:top_k]

    def extract_evidence(self, query: str,
                         context: Optional[Dict] = None) -> Dict[str, bool]:
        """Extract evidence from query and context."""
        evidence = {}
        query_lower = query.lower()

        # Domain evidence
        for domain in PatternDomain:
            if domain.value in query_lower:
                evidence[f'domain_{domain.value}'] = True

        # Intent evidence
        intent_keywords = {
            PatternIntent.RELIABILITY: ['reliable', 'dependable', 'stable'],
            PatternIntent.SCALABILITY: ['scale', 'grow', 'expand', 'horizontal'],
            PatternIntent.CONSISTENCY: ['consistent', 'accurate', 'correct'],
            PatternIntent.AVAILABILITY: ['available', 'uptime', '99.9'],
            PatternIntent.EFFICIENCY: ['fast', 'efficient', 'performance'],
            PatternIntent.COMPLIANCE: ['audit', 'compliance', 'regulation'],
            PatternIntent.ISOLATION: ['isolate', 'separate', 'contain'],
            PatternIntent.COORDINATION: ['coordinate', 'orchestrate', 'manage'],
        }

        for intent, keywords in intent_keywords.items():
            if any(kw in query_lower for kw in keywords):
                evidence[f'intent_{intent.value}'] = True

        # Keyword evidence from patterns
        for pid, pattern in self.kb.graph.nodes.items():
            for kw in pattern.keywords:
                if kw in query_lower:
                    evidence[f'keyword_{kw}'] = True

        # Context-based evidence
        if context:
            if 'domain' in context:
                evidence[f'domain_{context["domain"].value}'] = True
            if 'category' in context:
                evidence[f'category_{context["category"]}'] = True

        return evidence

    def learn_from_usage(self, pattern_id: str, success: bool,
                         co_used_with: Optional[List[str]] = None):
        """Learn from pattern usage to update priors."""
        self.usage_counts[pattern_id] += 1
        if success:
            self.success_counts[pattern_id] += 1

        # Update prior using beta distribution conjugate
        successes = self.success_counts[pattern_id]
        failures = self.usage_counts[pattern_id] - successes
        # Bayesian update: posterior = Beta(prior_a + successes, prior_b + failures)
        prior_strength = 10  # How much we trust the initial prior
        new_prior = (prior_strength * self.priors[pattern_id] + successes) / \
                   (prior_strength + self.usage_counts[pattern_id])
        self.priors[pattern_id] = new_prior

        # Track co-occurrences
        if co_used_with:
            for other in co_used_with:
                pair = tuple(sorted([pattern_id, other]))
                self.co_occurrence[pair] += 1


# =============================================================================
# CAUSAL REASONING ENGINE
# =============================================================================

@dataclass
class CausalEffect:
    """A causal relationship between patterns."""
    cause: str
    effect: str
    effect_type: str  # 'enables', 'prevents', 'improves', 'degrades'
    strength: float  # -1 to 1
    mechanism: str  # Explanation of causal mechanism
    conditions: List[str]  # When this causal effect applies


class CausalReasoningEngine:
    """
    Causal inference for understanding why patterns work together.
    """

    def __init__(self, graph: PatternGraph):
        self.graph = graph
        self.causal_graph: Dict[str, List[CausalEffect]] = defaultdict(list)
        self._build_causal_graph()

    def _build_causal_graph(self):
        """Build causal relationships from pattern graph."""
        # Convert graph relationships to causal effects
        causal_mappings = {
            RelationType.REQUIRES: ('enables', 0.9, 'Prerequisite for functionality'),
            RelationType.ENHANCES: ('improves', 0.7, 'Synergistic improvement'),
            RelationType.CONFLICTS: ('degrades', -0.8, 'Negative interaction'),
            RelationType.MITIGATES: ('prevents', 0.8, 'Addresses weakness'),
            RelationType.PRECEDES: ('enables', 0.6, 'Sequential dependency'),
        }

        for edge in self.graph.edges:
            if edge.relation_type in causal_mappings:
                effect_type, strength, mechanism = causal_mappings[edge.relation_type]

                effect = CausalEffect(
                    cause=edge.source,
                    effect=edge.target,
                    effect_type=effect_type,
                    strength=strength * edge.strength,
                    mechanism=f"{mechanism}: {edge.context}" if edge.context else mechanism,
                    conditions=[]
                )
                self.causal_graph[edge.source].append(effect)

        # Add domain-specific causal knowledge
        self._add_domain_knowledge()

    def _add_domain_knowledge(self):
        """Add expert knowledge about causal relationships."""
        domain_effects = [
            CausalEffect(
                cause='circuit_breaker',
                effect='system_stability',
                effect_type='improves',
                strength=0.85,
                mechanism='Prevents cascade failures by failing fast',
                conditions=['dependent services exist']
            ),
            CausalEffect(
                cause='retry_backoff',
                effect='latency',
                effect_type='degrades',
                strength=-0.3,
                mechanism='Retries add latency during failures',
                conditions=['transient failures present']
            ),
            CausalEffect(
                cause='saga',
                effect='complexity',
                effect_type='degrades',
                strength=-0.5,
                mechanism='Compensating transactions add implementation complexity',
                conditions=[]
            ),
            CausalEffect(
                cause='caching',
                effect='consistency',
                effect_type='degrades',
                strength=-0.4,
                mechanism='Cached data may be stale',
                conditions=['cache_invalidation_imperfect']
            ),
            CausalEffect(
                cause='event_sourcing',
                effect='auditability',
                effect_type='improves',
                strength=0.95,
                mechanism='Complete history of all changes',
                conditions=[]
            ),
        ]

        for effect in domain_effects:
            self.causal_graph[effect.cause].append(effect)

    def trace_effects(self, pattern_id: str,
                      depth: int = 3) -> Dict[str, List[Tuple[str, float, List[str]]]]:
        """
        Trace causal effects of adding a pattern.

        Returns effects categorized by type with accumulated strength and path.
        """
        effects = {
            'improves': [],
            'degrades': [],
            'enables': [],
            'prevents': []
        }

        visited = set()

        def trace(pid: str, current_strength: float, path: List[str], d: int):
            if d <= 0 or pid in visited:
                return
            visited.add(pid)

            for effect in self.causal_graph.get(pid, []):
                accumulated = current_strength * effect.strength
                new_path = path + [f"{pid}->{effect.effect}"]

                if abs(accumulated) > 0.1:  # Threshold for significance
                    effects[effect.effect_type].append(
                        (effect.effect, accumulated, new_path)
                    )

                # Propagate to next level
                if effect.effect in self.graph.nodes:
                    trace(effect.effect, accumulated * 0.7, new_path, d - 1)

        trace(pattern_id, 1.0, [], depth)

        # Sort by absolute strength
        for effect_type in effects:
            effects[effect_type].sort(key=lambda x: -abs(x[1]))

        return effects

    def explain_combination(self, pattern_ids: List[str]) -> Dict:
        """Explain why a combination of patterns works (or doesn't)."""
        explanation = {
            'synergies': [],
            'conflicts': [],
            'causal_chains': [],
            'net_effect': {}
        }

        # Find synergies
        for i, p1 in enumerate(pattern_ids):
            for p2 in pattern_ids[i+1:]:
                # Check if p1 enhances p2 or vice versa
                for effect in self.causal_graph.get(p1, []):
                    if effect.effect == p2 and effect.strength > 0:
                        explanation['synergies'].append({
                            'patterns': [p1, p2],
                            'type': effect.effect_type,
                            'strength': effect.strength,
                            'mechanism': effect.mechanism
                        })

                for effect in self.causal_graph.get(p2, []):
                    if effect.effect == p1 and effect.strength > 0:
                        explanation['synergies'].append({
                            'patterns': [p2, p1],
                            'type': effect.effect_type,
                            'strength': effect.strength,
                            'mechanism': effect.mechanism
                        })

        # Find conflicts
        for i, p1 in enumerate(pattern_ids):
            for p2 in pattern_ids[i+1:]:
                for effect in self.causal_graph.get(p1, []):
                    if effect.effect == p2 and effect.strength < 0:
                        explanation['conflicts'].append({
                            'patterns': [p1, p2],
                            'type': effect.effect_type,
                            'severity': abs(effect.strength),
                            'mechanism': effect.mechanism
                        })

        # Calculate net effect on key metrics
        metrics = ['reliability', 'performance', 'complexity', 'consistency']
        for metric in metrics:
            total = 0
            for pid in pattern_ids:
                for effect in self.causal_graph.get(pid, []):
                    if metric in effect.effect.lower():
                        total += effect.strength
            explanation['net_effect'][metric] = total

        return explanation

    def counterfactual(self, current_patterns: List[str],
                       hypothetical_pattern: str) -> Dict:
        """
        What would happen if we added/removed a pattern?
        Counterfactual reasoning.
        """
        result = {
            'pattern': hypothetical_pattern,
            'action': 'add' if hypothetical_pattern not in current_patterns else 'remove',
            'predicted_changes': [],
            'risk_assessment': 'low'
        }

        if hypothetical_pattern not in current_patterns:
            # Adding pattern
            effects = self.trace_effects(hypothetical_pattern, depth=2)

            for effect_type, items in effects.items():
                for target, strength, path in items:
                    result['predicted_changes'].append({
                        'target': target,
                        'effect': effect_type,
                        'strength': strength,
                        'path': ' -> '.join(path)
                    })

            # Check for new conflicts
            for current in current_patterns:
                if current in self.graph.get_conflicts(hypothetical_pattern):
                    result['predicted_changes'].append({
                        'target': current,
                        'effect': 'conflict',
                        'strength': -0.8,
                        'path': f"{hypothetical_pattern} conflicts {current}"
                    })
                    result['risk_assessment'] = 'high'
        else:
            # Removing pattern
            dependents = []
            for pid in current_patterns:
                for target, strength in self.graph.get_required_by(pid):
                    if target == hypothetical_pattern:
                        dependents.append(pid)

            if dependents:
                result['predicted_changes'].append({
                    'target': 'functionality',
                    'effect': 'degrades',
                    'strength': -0.9,
                    'path': f"Breaking dependency for: {', '.join(dependents)}"
                })
                result['risk_assessment'] = 'critical'

        return result


# =============================================================================
# PATTERN EVOLUTION TRACKER
# =============================================================================

@dataclass
class PatternVersion:
    """A version of a pattern over time."""
    version: str
    timestamp: float
    changes: List[str]
    deprecated_by: Optional[str] = None
    breaking_changes: bool = False


class PatternEvolutionTracker:
    """Tracks pattern evolution and provides migration guidance."""

    def __init__(self):
        self.versions: Dict[str, List[PatternVersion]] = defaultdict(list)
        self.migrations: Dict[Tuple[str, str], List[str]] = {}  # (from, to) -> steps

    def record_version(self, pattern_id: str, version: PatternVersion):
        """Record a new version of a pattern."""
        self.versions[pattern_id].append(version)
        self.versions[pattern_id].sort(key=lambda v: v.timestamp)

    def get_history(self, pattern_id: str) -> List[PatternVersion]:
        """Get version history for a pattern."""
        return self.versions.get(pattern_id, [])

    def suggest_migration(self, from_pattern: str,
                          to_pattern: str) -> List[str]:
        """Suggest migration steps between patterns."""
        key = (from_pattern, to_pattern)
        if key in self.migrations:
            return self.migrations[key]

        # Generate migration steps based on pattern differences
        steps = [
            f"1. Identify all uses of {from_pattern}",
            f"2. Evaluate compatibility with {to_pattern}",
            f"3. Implement adapter if needed",
            f"4. Gradually migrate traffic",
            f"5. Remove {from_pattern} after validation"
        ]

        return steps

    def check_deprecation(self, pattern_ids: List[str]) -> List[Dict]:
        """Check if any patterns are deprecated."""
        warnings = []
        for pid in pattern_ids:
            versions = self.versions.get(pid, [])
            if versions:
                latest = versions[-1]
                if latest.deprecated_by:
                    warnings.append({
                        'pattern': pid,
                        'deprecated_by': latest.deprecated_by,
                        'migration_steps': self.suggest_migration(pid, latest.deprecated_by)
                    })
        return warnings


# =============================================================================
# INTERACTIVE QUERY REFINEMENT
# =============================================================================

@dataclass
class RefinementSuggestion:
    """A suggestion for refining a query."""
    type: str  # 'add_constraint', 'specify_domain', 'clarify_intent'
    suggestion: str
    reason: str
    example: str


class InteractiveQueryRefiner:
    """
    Helps users refine ambiguous queries through interactive suggestions.
    """

    def __init__(self, kb: 'PatternKnowledgeBase'):
        self.kb = kb

    def analyze_query(self, query: str,
                      results: List[Tuple[Pattern, float]]) -> List[RefinementSuggestion]:
        """Analyze query and suggest refinements."""
        suggestions = []

        # Check if results are diverse (might indicate ambiguity)
        if len(results) >= 2:
            categories = set(r[0].category for r in results[:5])
            if len(categories) >= 3:
                suggestions.append(RefinementSuggestion(
                    type='specify_domain',
                    suggestion='Your query matches patterns from multiple domains',
                    reason=f'Found patterns in: {", ".join(categories)}',
                    example=f'Try: "{query} for {list(categories)[0]}"'
                ))

        # Check if scores are low (might need more context)
        if results and results[0][1] < 0.3:
            suggestions.append(RefinementSuggestion(
                type='add_keywords',
                suggestion='Consider adding more specific keywords',
                reason='Top match has low confidence score',
                example=f'Try adding terms like: reliability, performance, consistency'
            ))

        # Check if query lacks intent
        intent_keywords = ['need', 'want', 'require', 'must', 'should']
        if not any(kw in query.lower() for kw in intent_keywords):
            suggestions.append(RefinementSuggestion(
                type='clarify_intent',
                suggestion='Specify what you need to achieve',
                reason='Query describes a situation but not the goal',
                example=f'Try: "I need to {query} with high reliability"'
            ))

        # Suggest constraints based on common refinements
        constraint_suggestions = {
            'scale': 'Add expected load (e.g., "10k requests/sec")',
            'reliable': 'Specify availability target (e.g., "99.99% uptime")',
            'fast': 'Add latency requirements (e.g., "under 100ms")',
            'consistent': 'Specify consistency model (e.g., "eventual" or "strong")'
        }

        for keyword, suggestion in constraint_suggestions.items():
            if keyword in query.lower():
                suggestions.append(RefinementSuggestion(
                    type='add_constraint',
                    suggestion=suggestion,
                    reason=f'"{keyword}" is ambiguous without quantification',
                    example=f'{query} with specific requirements'
                ))
                break

        return suggestions

    def expand_query(self, query: str) -> List[str]:
        """Generate query expansions for broader search."""
        expansions = [query]

        # Synonym expansion
        synonyms = {
            'fast': ['performance', 'efficient', 'low-latency'],
            'reliable': ['dependable', 'fault-tolerant', 'resilient'],
            'scale': ['distributed', 'horizontal', 'parallel'],
            'consistent': ['accurate', 'synchronized', 'coherent']
        }

        for word, syns in synonyms.items():
            if word in query.lower():
                for syn in syns:
                    expansions.append(query.lower().replace(word, syn))

        return list(set(expansions))


# =============================================================================
# DEMONSTRATION
# =============================================================================

def demo():
    """Demonstrate the sophisticated pattern intelligence system."""
    print("=" * 80)
    print("SOPHISTICATED PATTERN INTELLIGENCE SYSTEM")
    print("=" * 80)

    kb = PatternKnowledgeBase()

    print(f"\nLoaded {len(kb.graph.nodes)} patterns with {len(kb.graph.edges)} relationships")

    # Show clusters
    print("\n" + "=" * 60)
    print("SEMANTIC CLUSTERS")
    print("=" * 60)
    for cluster_id, info in kb.get_cluster_info().items():
        print(f"\n[Cluster {cluster_id}] {info['label']}")
        print(f"  Patterns ({info['size']}): {', '.join(info['patterns'][:5])}")
        if info['size'] > 5:
            print(f"  ... and {info['size'] - 5} more")

    # Semantic search with context
    print("\n" + "=" * 60)
    print("CONTEXTUAL SEMANTIC SEARCH")
    print("=" * 60)

    queries = [
        ("handle failures gracefully", None),
        ("transaction consistency", PatternDomain.DATA_FLOW),
        ("scale horizontally", PatternDomain.PERFORMANCE),
        ("monitor distributed system", PatternDomain.OBSERVABILITY),
        ("secure API", PatternDomain.SECURITY)
    ]

    for query, domain in queries:
        context = {'domain': domain} if domain else None
        results = kb.search(query, k=3, context=context)
        domain_str = f" [{domain.value}]" if domain else ""
        print(f"\n\"{query}\"{domain_str}:")
        for pattern, score in results:
            print(f"  {score:.3f} - {pattern.name}")
            if pattern.use_cases:
                print(f"         Use: {pattern.use_cases[0][:50]}...")

    # Context-aware recommendations
    print("\n" + "=" * 60)
    print("CONTEXT-AWARE RECOMMENDATIONS")
    print("=" * 60)

    scenarios = [
        {
            "scenario": "Building a payment processing system that must never lose transactions",
            "existing": [],
            "domain": PatternDomain.DATA_FLOW
        },
        {
            "scenario": "API that calls multiple external services and needs to be resilient",
            "existing": ["retry_backoff"],
            "domain": PatternDomain.RESILIENCE
        },
        {
            "scenario": "High-throughput event processing pipeline",
            "existing": ["fan_out_fan_in", "competing_consumers"],
            "domain": PatternDomain.PERFORMANCE
        }
    ]

    for ctx in scenarios:
        print(f"\nScenario: {ctx['scenario'][:60]}...")
        if ctx['existing']:
            print(f"Existing: {', '.join(ctx['existing'])}")

        recs = kb.recommend(
            ctx['scenario'],
            existing=ctx['existing'],
            domain=ctx['domain'],
            k=3
        )

        for rec in recs:
            print(f"\n  [{rec.score:.2f}] {rec.pattern_name}")
            print(f"    {rec.explanation}")
            if rec.warnings:
                print(f"    WARNINGS: {', '.join(rec.warnings)}")

            # Show relevance breakdown
            top_factors = sorted(rec.relevance_factors.items(), key=lambda x: -x[1])[:2]
            factors_str = ", ".join(f"{f[0]}={f[1]:.2f}" for f in top_factors)
            print(f"    Factors: {factors_str}")

    # Pattern composition
    print("\n" + "=" * 60)
    print("PATTERN COMPOSITION")
    print("=" * 60)

    composition_sets = [
        ["retry_backoff", "circuit_breaker", "fallback", "timeout"],
        ["saga", "outbox", "idempotent_receiver"],
        ["distributed_tracing", "structured_logging", "metrics_collection"]
    ]

    for patterns in composition_sets:
        composed = kb.compose(patterns)
        print(f"\nComposing: {', '.join(patterns)}")
        print(f"  Name: {composed.name}")
        print(f"  Type: {composed.composition_type}")
        print(f"  Score: {composed.score:.2f}")
        if composed.synergies:
            syns = [f"{s[0]}+{s[1]}" for s in composed.synergies[:3]]
            print(f"  Synergies: {', '.join(syns)}")
        if composed.conflicts:
            print(f"  CONFLICTS: {composed.conflicts}")
        print(f"  Rationale: {composed.rationale}")

    # Anti-pattern detection
    print("\n" + "=" * 60)
    print("ANTI-PATTERN DETECTION")
    print("=" * 60)

    test_sets = [
        ["saga", "two_phase_commit"],  # Conflict
        ["retry_backoff"],  # Missing circuit breaker
        ["saga", "outbox", "event_sourcing", "cqrs", "distributed_tracing",
         "service_mesh", "api_gateway"]  # High complexity
    ]

    for patterns in test_sets:
        print(f"\nAnalyzing: {', '.join(patterns)}")
        warnings = kb.analyze_antipatterns(patterns)
        if warnings:
            for w in warnings:
                icon = {"critical": "X", "warning": "!", "info": "i"}[w.severity]
                print(f"  [{icon}] {w.severity.upper()}: {w.message}")
                print(f"      Suggestion: {w.suggestion}")
        else:
            print("  No anti-patterns detected")

    # Pattern relationships
    print("\n" + "=" * 60)
    print("PATTERN RELATIONSHIPS")
    print("=" * 60)

    for pid in ["circuit_breaker", "saga", "rate_limiter"]:
        print(f"\n{kb.graph.nodes[pid].name}:")
        related = kb.get_related(pid)

        if related['requires']:
            reqs = [f"{r[0]} ({r[1]:.1f})" for r in related['requires']]
            print(f"  Requires: {', '.join(reqs)}")

        if related['enhances']:
            enh = [f"{e[0]} ({e[1]:.1f})" for e in related['enhances']]
            print(f"  Enhanced by: {', '.join(enh)}")

        if related['conflicts']:
            print(f"  Conflicts: {', '.join(related['conflicts'])}")

    # Suggest additions
    print("\n" + "=" * 60)
    print("PATTERN ADDITION SUGGESTIONS")
    print("=" * 60)

    current = ["retry_backoff", "timeout"]
    goal = "build a resilient microservice that handles external API failures"

    print(f"Current patterns: {', '.join(current)}")
    print(f"Goal: {goal}")
    print("\nSuggested additions:")

    suggestions = kb.composer.suggest_additions(current, goal, k=5)
    for pid, score, reason in suggestions:
        pattern = kb.graph.nodes[pid]
        print(f"  [{score:.2f}] {pattern.name}")
        print(f"    Reason: {reason}")

    # =========================================================================
    # NEW SOPHISTICATED FEATURES
    # =========================================================================

    print("\n" + "=" * 80)
    print("BAYESIAN PROBABILISTIC INFERENCE")
    print("=" * 80)

    bayesian = BayesianPatternMatcher(kb)

    test_queries = [
        "I need reliable transaction processing with audit compliance",
        "High performance caching with eventual consistency",
        "Fault tolerant distributed system with circuit breakers"
    ]

    for query in test_queries:
        print(f"\nQuery: \"{query}\"")

        # Extract evidence
        evidence = bayesian.extract_evidence(query)
        print(f"  Evidence extracted: {len(evidence)} signals")

        # Bayesian inference
        matches = bayesian.infer(evidence, top_k=3)

        for match in matches:
            pattern = kb.graph.nodes.get(match.pattern_id)
            if pattern:
                ci_low, ci_high = match.confidence_interval
                print(f"\n  [{match.mean_score:.3f}] {pattern.name}")
                print(f"    95% CI: [{ci_low:.3f}, {ci_high:.3f}]")
                print(f"    Variance: {match.variance:.4f}")
                if match.factors:
                    top_factors = list(match.factors.items())[:3]
                    factors_str = ", ".join(f"{k}={v:.2f}" for k, v in top_factors)
                    print(f"    Key factors: {factors_str}")

    # Learning demonstration
    print("\n" + "-" * 40)
    print("ONLINE LEARNING")
    print("-" * 40)

    print("\nSimulating usage feedback...")
    initial_prior = bayesian.priors['circuit_breaker']
    print(f"  Initial prior for circuit_breaker: {initial_prior:.3f}")

    # Simulate successful usage
    for i in range(5):
        bayesian.learn_from_usage('circuit_breaker', success=True,
                                   co_used_with=['retry_backoff', 'fallback'])

    updated_prior = bayesian.priors['circuit_breaker']
    print(f"  After 5 successful uses: {updated_prior:.3f} (+{updated_prior - initial_prior:.3f})")

    print(f"  Co-occurrence (circuit_breaker, retry_backoff): {bayesian.co_occurrence[('circuit_breaker', 'retry_backoff')]}")

    # Causal Reasoning
    print("\n" + "=" * 80)
    print("CAUSAL REASONING ENGINE")
    print("=" * 80)

    causal = CausalReasoningEngine(kb.graph)

    print("\nTracing causal effects of adding 'circuit_breaker':")
    effects = causal.trace_effects('circuit_breaker', depth=2)

    for effect_type, items in effects.items():
        if items:
            print(f"\n  {effect_type.upper()}:")
            for target, strength, path in items[:3]:
                print(f"    {target}: {strength:+.2f}")
                print(f"      Path: {' -> '.join(path)}")

    # Explain combination
    print("\n" + "-" * 40)
    print("CAUSAL EXPLANATION OF PATTERN COMBINATION")
    print("-" * 40)

    combo = ['retry_backoff', 'circuit_breaker', 'fallback', 'bulkhead']
    print(f"\nExplaining: {', '.join(combo)}")

    explanation = causal.explain_combination(combo)

    if explanation['synergies']:
        print("\n  SYNERGIES:")
        for syn in explanation['synergies'][:4]:
            print(f"    {syn['patterns'][0]} -> {syn['patterns'][1]}: {syn['type']} ({syn['strength']:.2f})")
            print(f"      {syn['mechanism'][:60]}...")

    if explanation['conflicts']:
        print("\n  CONFLICTS:")
        for con in explanation['conflicts']:
            print(f"    {con['patterns']}: {con['mechanism']}")

    if explanation['net_effect']:
        print("\n  NET EFFECTS:")
        for metric, value in explanation['net_effect'].items():
            if value != 0:
                indicator = "+" if value > 0 else ""
                print(f"    {metric}: {indicator}{value:.2f}")

    # Counterfactual reasoning
    print("\n" + "-" * 40)
    print("COUNTERFACTUAL REASONING")
    print("-" * 40)

    current_system = ['retry_backoff', 'timeout']
    hypothetical = 'circuit_breaker'

    print(f"\nCurrent system: {', '.join(current_system)}")
    print(f"What if we add: {hypothetical}?")

    counterfactual = causal.counterfactual(current_system, hypothetical)

    print(f"\n  Action: {counterfactual['action']}")
    print(f"  Risk assessment: {counterfactual['risk_assessment']}")

    if counterfactual['predicted_changes']:
        print("\n  Predicted changes:")
        for change in counterfactual['predicted_changes'][:5]:
            indicator = "+" if change['strength'] > 0 else ""
            print(f"    {change['target']}: {change['effect']} ({indicator}{change['strength']:.2f})")

    # Interactive Query Refinement
    print("\n" + "=" * 80)
    print("INTERACTIVE QUERY REFINEMENT")
    print("=" * 80)

    refiner = InteractiveQueryRefiner(kb)

    ambiguous_queries = [
        "make it fast and reliable",
        "handle errors",
        "scale the system"
    ]

    for query in ambiguous_queries:
        print(f"\nQuery: \"{query}\"")

        # Get initial results
        results = kb.search(query, k=5)

        # Get refinement suggestions
        suggestions = refiner.analyze_query(query, results)

        if suggestions:
            print("  Refinement suggestions:")
            for sug in suggestions[:2]:
                print(f"    [{sug.type}] {sug.suggestion}")
                print(f"      Reason: {sug.reason}")
                print(f"      Example: {sug.example}")

        # Show query expansions
        expansions = refiner.expand_query(query)
        if len(expansions) > 1:
            print(f"  Query expansions: {', '.join(expansions[1:3])}")

    # Summary Statistics
    print("\n" + "=" * 80)
    print("SYSTEM SUMMARY")
    print("=" * 80)

    print(f"""
  Patterns:           {len(kb.graph.nodes)}
  Relationships:      {len(kb.graph.edges)}
  Semantic clusters:  {len(kb.clusterer.clusters)}
  Embedding dims:     {kb.embedder.EMBEDDING_DIM}
  Causal effects:     {sum(len(v) for v in causal.causal_graph.values())}
  Bayesian priors:    {len(bayesian.priors)}
  Likelihood entries: {sum(len(v) for v in bayesian.likelihoods.values())}

  Capabilities:
    - Neural-style semantic embeddings
    - Graph-based pattern relationships
    - Compositional pattern synthesis
    - Context-aware multi-factor recommendations
    - Semantic clustering
    - Anti-pattern detection
    - Bayesian probabilistic inference
    - Online learning from usage
    - Causal reasoning & counterfactuals
    - Interactive query refinement
    """)

    print("=" * 80)
    print("DEMO COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    demo()
