#!/usr/bin/env python3
"""
Advanced Tool Discovery Engine

Provides intelligent tool discovery, auto-registration, and adaptive routing
for the dynamic tool registry system. Includes semantic search, capability
matching, and self-learning recommendation systems.
"""

import asyncio
import json
import hashlib
import inspect
import ast
import importlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union, Callable, Set, Tuple
from dataclasses import dataclass
from enum import Enum
import re
import logging

from pydantic import BaseModel, Field

# Optional ML dependencies
try:
    import numpy as np
    from sentence_transformers import SentenceTransformer
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    from sklearn.cluster import KMeans
    import networkx as nx
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    np = None
    SentenceTransformer = None
    TfidfVectorizer = None
    cosine_similarity = None
    KMeans = None
    nx = None

# Optional dynamic registry - stub if not available
try:
    from dynamic_tool_registry import (
        DynamicToolRegistry, ToolDefinition, ToolCategory, ToolStatus,
        ToolCapability, ExecutionMode, ToolMetrics
    )
except ImportError:
    # Create stub classes for type hints
    class DynamicToolRegistry:
        pass
    class ToolDefinition:
        pass
    class ToolCategory:
        pass
    class ToolStatus:
        pass
    class ToolCapability:
        pass
    class ExecutionMode:
        pass
    class ToolMetrics:
        pass

class DiscoveryMethod(Enum):
    """Methods for tool discovery"""
    SEMANTIC_SEARCH = "semantic_search"
    CAPABILITY_MATCHING = "capability_matching"
    USAGE_PATTERN = "usage_pattern"
    SIMILARITY_CLUSTERING = "similarity_clustering"
    GRAPH_TRAVERSAL = "graph_traversal"
    HYBRID = "hybrid"

class ToolRecommendation(BaseModel):
    """Represents a tool recommendation with confidence scoring"""
    tool_id: str
    tool_name: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    relevance_score: float = Field(ge=0.0, le=1.0)
    reasoning: str
    method_used: DiscoveryMethod
    alternative_tools: List[str] = Field(default_factory=list)
    execution_context_suggestion: Dict[str, Any] = Field(default_factory=dict)

class CapabilityQuery(BaseModel):
    """Query for finding tools with specific capabilities"""
    required_capabilities: List[str]
    optional_capabilities: List[str] = Field(default_factory=list)
    input_schema: Optional[Dict[str, Any]] = None
    output_schema: Optional[Dict[str, Any]] = None
    performance_requirements: Dict[str, float] = Field(default_factory=dict)
    context_constraints: Dict[str, Any] = Field(default_factory=dict)

class ToolUsagePattern(BaseModel):
    """Represents patterns in tool usage"""
    tool_combinations: List[List[str]]  # Commonly used tool sequences
    user_preferences: Dict[str, float]  # User preference weights
    temporal_patterns: Dict[str, List[int]]  # Usage patterns by time
    context_associations: Dict[str, List[str]]  # Context -> tools mapping

class AdvancedToolDiscoveryEngine:
    """Intelligent tool discovery and recommendation engine"""

    def __init__(self, tool_registry: DynamicToolRegistry):
        self.registry = tool_registry
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # Initialize ML models
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2')
        self.tfidf_vectorizer = TfidfVectorizer(max_features=1000, stop_words='english')

        # Tool relationship graph
        self.tool_graph = nx.DiGraph()

        # Usage patterns and learning
        self.usage_patterns = ToolUsagePattern(
            tool_combinations=[],
            user_preferences={},
            temporal_patterns={},
            context_associations={}
        )

        # Caches
        self.capability_cache: Dict[str, List[str]] = {}
        self.similarity_cache: Dict[Tuple[str, str], float] = {}
        self.recommendation_cache: Dict[str, List[ToolRecommendation]] = {}

        # Discovery statistics
        self.discovery_stats = {
            'total_queries': 0,
            'successful_recommendations': 0,
            'method_usage': {method.value: 0 for method in DiscoveryMethod}
        }

        # Initialize discovery systems
        self._initialize_discovery_systems()

    def _initialize_discovery_systems(self):
        """Initialize discovery and learning systems"""
        # Build tool relationship graph
        self._build_tool_graph()

        # Initialize capability indexing
        self._index_tool_capabilities()

        # Load usage patterns from historical data
        self._load_usage_patterns()

    def _build_tool_graph(self):
        """Build a graph representing tool relationships"""
        tools = self.registry.list_tools()

        # Add nodes for all tools
        for tool in tools:
            self.tool_graph.add_node(
                tool.id,
                name=tool.name,
                category=tool.category.value,
                capabilities=[cap.name for cap in tool.capabilities]
            )

        # Add edges based on relationships
        for tool in tools:
            for other_tool in tools:
                if tool.id != other_tool.id:
                    # Calculate relationship strength
                    similarity = self._calculate_tool_similarity(tool, other_tool)

                    if similarity > 0.3:  # Threshold for creating edge
                        self.tool_graph.add_edge(
                            tool.id,
                            other_tool.id,
                            weight=similarity,
                            relationship_type=self._determine_relationship_type(tool, other_tool)
                        )

    def _calculate_tool_similarity(self, tool1: ToolDefinition, tool2: ToolDefinition) -> float:
        """Calculate similarity between two tools"""
        cache_key = (tool1.id, tool2.id)
        if cache_key in self.similarity_cache:
            return self.similarity_cache[cache_key]

        # Factors for similarity calculation
        factors = []

        # Category similarity
        if tool1.category == tool2.category:
            factors.append(0.4)
        else:
            factors.append(0.0)

        # Tag overlap
        tags1 = set(tool1.tags)
        tags2 = set(tool2.tags)
        if tags1 and tags2:
            tag_similarity = len(tags1.intersection(tags2)) / len(tags1.union(tags2))
            factors.append(tag_similarity * 0.3)

        # Capability overlap
        caps1 = set(cap.name for cap in tool1.capabilities)
        caps2 = set(cap.name for cap in tool2.capabilities)
        if caps1 and caps2:
            cap_similarity = len(caps1.intersection(caps2)) / len(caps1.union(caps2))
            factors.append(cap_similarity * 0.3)

        # Semantic similarity of descriptions
        desc_similarity = self._calculate_semantic_similarity(
            tool1.description, tool2.description
        )
        factors.append(desc_similarity * 0.2)

        similarity = sum(factors) / len(factors) if factors else 0.0
        self.similarity_cache[cache_key] = similarity

        return similarity

    def _calculate_semantic_similarity(self, text1: str, text2: str) -> float:
        """Calculate semantic similarity between two texts"""
        try:
            embeddings = self.embedder.encode([text1, text2])
            similarity = cosine_similarity([embeddings[0]], [embeddings[1]])[0][0]
            return float(similarity)
        except Exception:
            return 0.0

    def _determine_relationship_type(self, tool1: ToolDefinition, tool2: ToolDefinition) -> str:
        """Determine the type of relationship between tools"""
        # Check if tools are commonly used together
        if self._are_tools_complementary(tool1, tool2):
            return "complementary"
        elif self._are_tools_alternatives(tool1, tool2):
            return "alternative"
        elif self._is_tool_prerequisite(tool1, tool2):
            return "prerequisite"
        else:
            return "related"

    def _are_tools_complementary(self, tool1: ToolDefinition, tool2: ToolDefinition) -> bool:
        """Check if tools complement each other"""
        # Look for output-input compatibility
        for cap1 in tool1.capabilities:
            for cap2 in tool2.capabilities:
                if self._outputs_match_inputs(cap1.output_schema, cap2.input_schema):
                    return True
        return False

    def _are_tools_alternatives(self, tool1: ToolDefinition, tool2: ToolDefinition) -> bool:
        """Check if tools are alternatives to each other"""
        # Same category and similar capabilities
        if tool1.category == tool2.category:
            caps1 = set(cap.name for cap in tool1.capabilities)
            caps2 = set(cap.name for cap in tool2.capabilities)
            overlap = len(caps1.intersection(caps2)) / len(caps1.union(caps2)) if caps1.union(caps2) else 0
            return overlap > 0.7
        return False

    def _is_tool_prerequisite(self, tool1: ToolDefinition, tool2: ToolDefinition) -> bool:
        """Check if tool1 is a prerequisite for tool2"""
        # Check if tool2 depends on outputs from tool1's category
        return tool1.category.value in [dep.lower() for dep in tool2.dependencies]

    def _outputs_match_inputs(self, output_schema: Dict[str, Any], input_schema: Dict[str, Any]) -> bool:
        """Check if output schema matches input schema"""
        if not output_schema or not input_schema:
            return False

        # Simple type matching
        for out_key, out_type in output_schema.items():
            if out_key in input_schema:
                if str(out_type).lower() == str(input_schema[out_key]).lower():
                    return True
        return False

    def _index_tool_capabilities(self):
        """Create capability index for fast lookup"""
        for tool in self.registry.list_tools():
            capabilities = []
            for cap in tool.capabilities:
                capabilities.extend([
                    cap.name,
                    cap.description,
                    *cap.input_schema.keys(),
                    *cap.output_schema.keys()
                ])
            self.capability_cache[tool.id] = capabilities

    def _load_usage_patterns(self):
        """Load historical usage patterns"""
        # In a real implementation, this would load from database
        # For now, initialize with empty patterns
        pass

    async def discover_tools_by_query(self,
                                     query: str,
                                     method: DiscoveryMethod = DiscoveryMethod.HYBRID,
                                     limit: int = 10,
                                     context: Optional[Dict[str, Any]] = None) -> List[ToolRecommendation]:
        """Discover tools using various discovery methods"""

        self.discovery_stats['total_queries'] += 1
        self.discovery_stats['method_usage'][method.value] += 1

        # Check cache first
        cache_key = f"{query}:{method.value}:{limit}"
        if cache_key in self.recommendation_cache:
            return self.recommendation_cache[cache_key]

        recommendations = []

        if method == DiscoveryMethod.SEMANTIC_SEARCH:
            recommendations = await self._semantic_search_discovery(query, limit, context)

        elif method == DiscoveryMethod.CAPABILITY_MATCHING:
            capability_query = self._parse_capability_query(query)
            recommendations = await self._capability_matching_discovery(capability_query, limit)

        elif method == DiscoveryMethod.USAGE_PATTERN:
            recommendations = await self._usage_pattern_discovery(query, limit, context)

        elif method == DiscoveryMethod.SIMILARITY_CLUSTERING:
            recommendations = await self._clustering_discovery(query, limit)

        elif method == DiscoveryMethod.GRAPH_TRAVERSAL:
            recommendations = await self._graph_traversal_discovery(query, limit)

        elif method == DiscoveryMethod.HYBRID:
            recommendations = await self._hybrid_discovery(query, limit, context)

        # Sort by confidence score
        recommendations.sort(key=lambda x: x.confidence_score, reverse=True)

        # Cache results
        self.recommendation_cache[cache_key] = recommendations[:limit]

        return recommendations[:limit]

    async def _semantic_search_discovery(self,
                                        query: str,
                                        limit: int,
                                        context: Optional[Dict[str, Any]] = None) -> List[ToolRecommendation]:
        """Discover tools using semantic search"""

        # Get semantic embeddings
        query_embedding = self.embedder.encode(query)

        # Search through tool descriptions and capabilities
        similarities = []
        for tool in self.registry.list_tools():
            # Combine tool information for search
            search_text = f"{tool.name} {tool.description} {' '.join(tool.tags)}"
            for cap in tool.capabilities:
                search_text += f" {cap.name} {cap.description}"

            # Calculate similarity
            tool_embedding = self.embedder.encode(search_text)
            similarity = cosine_similarity([query_embedding], [tool_embedding])[0][0]

            similarities.append((tool, float(similarity)))

        # Sort by similarity
        similarities.sort(key=lambda x: x[1], reverse=True)

        # Create recommendations
        recommendations = []
        for tool, similarity in similarities[:limit]:
            recommendations.append(ToolRecommendation(
                tool_id=tool.id,
                tool_name=tool.name,
                confidence_score=similarity,
                relevance_score=similarity,
                reasoning=f"Semantic similarity to query: {similarity:.3f}",
                method_used=DiscoveryMethod.SEMANTIC_SEARCH
            ))

        return recommendations

    async def _capability_matching_discovery(self,
                                           capability_query: CapabilityQuery,
                                           limit: int) -> List[ToolRecommendation]:
        """Discover tools by matching capabilities"""

        scored_tools = []

        for tool in self.registry.list_tools():
            score = 0.0
            matches = []

            # Check required capabilities
            tool_capabilities = [cap.name.lower() for cap in tool.capabilities]
            for req_cap in capability_query.required_capabilities:
                if any(req_cap.lower() in tool_cap for tool_cap in tool_capabilities):
                    score += 1.0
                    matches.append(req_cap)

            # Check optional capabilities
            for opt_cap in capability_query.optional_capabilities:
                if any(opt_cap.lower() in tool_cap for tool_cap in tool_capabilities):
                    score += 0.5
                    matches.append(opt_cap)

            # Normalize score
            total_capabilities = len(capability_query.required_capabilities) + len(capability_query.optional_capabilities)
            if total_capabilities > 0:
                score = score / total_capabilities

            # Check performance requirements
            if capability_query.performance_requirements:
                metrics = self.registry.get_tool_metrics(tool.id)
                if metrics:
                    perf_score = self._evaluate_performance_requirements(
                        metrics, capability_query.performance_requirements
                    )
                    score = (score + perf_score) / 2

            if score > 0:
                scored_tools.append((tool, score, matches))

        # Sort by score
        scored_tools.sort(key=lambda x: x[1], reverse=True)

        # Create recommendations
        recommendations = []
        for tool, score, matches in scored_tools[:limit]:
            recommendations.append(ToolRecommendation(
                tool_id=tool.id,
                tool_name=tool.name,
                confidence_score=score,
                relevance_score=score,
                reasoning=f"Capability matches: {', '.join(matches)}",
                method_used=DiscoveryMethod.CAPABILITY_MATCHING
            ))

        return recommendations

    async def _usage_pattern_discovery(self,
                                      query: str,
                                      limit: int,
                                      context: Optional[Dict[str, Any]] = None) -> List[ToolRecommendation]:
        """Discover tools based on usage patterns"""

        recommendations = []

        # Analyze context for pattern matching
        if context:
            # Check for context associations
            for ctx_key, ctx_value in context.items():
                if str(ctx_value).lower() in self.usage_patterns.context_associations:
                    associated_tools = self.usage_patterns.context_associations[str(ctx_value).lower()]

                    for tool_id in associated_tools:
                        if tool_id in self.registry.tools:
                            tool = self.registry.tools[tool_id]
                            # Calculate confidence based on usage frequency
                            usage_count = self.usage_patterns.user_preferences.get(tool_id, 0)
                            confidence = min(usage_count / 10.0, 1.0)  # Normalize

                            recommendations.append(ToolRecommendation(
                                tool_id=tool.id,
                                tool_name=tool.name,
                                confidence_score=confidence,
                                relevance_score=confidence,
                                reasoning=f"Used in similar context: {ctx_key}={ctx_value}",
                                method_used=DiscoveryMethod.USAGE_PATTERN
                            ))

        # Look for tool combinations
        query_words = set(query.lower().split())
        for combination in self.usage_patterns.tool_combinations:
            for tool_id in combination:
                if tool_id in self.registry.tools:
                    tool = self.registry.tools[tool_id]
                    # Check if query words match tool keywords
                    tool_words = set(tool.name.lower().split() + tool.description.lower().split())
                    overlap = len(query_words.intersection(tool_words))

                    if overlap > 0:
                        confidence = overlap / len(query_words)
                        recommendations.append(ToolRecommendation(
                            tool_id=tool.id,
                            tool_name=tool.name,
                            confidence_score=confidence,
                            relevance_score=confidence,
                            reasoning=f"Part of common tool combination",
                            method_used=DiscoveryMethod.USAGE_PATTERN,
                            alternative_tools=[tid for tid in combination if tid != tool_id]
                        ))

        # Remove duplicates and sort
        seen = set()
        unique_recommendations = []
        for rec in recommendations:
            if rec.tool_id not in seen:
                seen.add(rec.tool_id)
                unique_recommendations.append(rec)

        unique_recommendations.sort(key=lambda x: x.confidence_score, reverse=True)
        return unique_recommendations[:limit]

    async def _clustering_discovery(self, query: str, limit: int) -> List[ToolRecommendation]:
        """Discover tools using clustering analysis"""

        tools = self.registry.list_tools()
        if len(tools) < 3:  # Need minimum tools for clustering
            return []

        # Create feature vectors for all tools
        tool_texts = []
        tool_objects = []

        for tool in tools:
            tool_text = f"{tool.name} {tool.description} {' '.join(tool.tags)}"
            for cap in tool.capabilities:
                tool_text += f" {cap.name} {cap.description}"

            tool_texts.append(tool_text)
            tool_objects.append(tool)

        # Add query to the texts
        tool_texts.append(query)

        # Vectorize using TF-IDF
        try:
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(tool_texts)

            # Perform clustering
            n_clusters = min(5, len(tools))
            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            cluster_labels = kmeans.fit_predict(tfidf_matrix)

            # Find which cluster the query belongs to
            query_cluster = cluster_labels[-1]

            # Get tools in the same cluster as the query
            recommendations = []
            for i, (tool, label) in enumerate(zip(tool_objects, cluster_labels[:-1])):
                if label == query_cluster:
                    # Calculate distance to cluster center
                    tool_vector = tfidf_matrix[i]
                    cluster_center = kmeans.cluster_centers_[query_cluster]
                    distance = cosine_similarity(tool_vector, cluster_center.reshape(1, -1))[0][0]

                    recommendations.append(ToolRecommendation(
                        tool_id=tool.id,
                        tool_name=tool.name,
                        confidence_score=float(distance),
                        relevance_score=float(distance),
                        reasoning=f"In same cluster (#{query_cluster}) as query",
                        method_used=DiscoveryMethod.SIMILARITY_CLUSTERING
                    ))

            recommendations.sort(key=lambda x: x.confidence_score, reverse=True)
            return recommendations[:limit]

        except Exception as e:
            self.logger.error(f"Clustering discovery failed: {e}")
            return []

    async def _graph_traversal_discovery(self, query: str, limit: int) -> List[ToolRecommendation]:
        """Discover tools using graph traversal algorithms"""

        recommendations = []

        # First, find tools that match the query directly
        seed_tools = []
        query_words = set(query.lower().split())

        for tool in self.registry.list_tools():
            tool_words = set(tool.name.lower().split() + tool.description.lower().split())
            overlap = len(query_words.intersection(tool_words))

            if overlap > 0:
                seed_tools.append((tool.id, overlap / len(query_words)))

        # Sort seed tools by relevance
        seed_tools.sort(key=lambda x: x[1], reverse=True)

        # Traverse graph from seed tools
        visited = set()
        for seed_tool_id, seed_score in seed_tools[:3]:  # Use top 3 as seeds
            if seed_tool_id in self.tool_graph:
                # BFS traversal
                queue = [(seed_tool_id, seed_score, 0)]  # (tool_id, score, depth)

                while queue and len(recommendations) < limit:
                    current_tool_id, current_score, depth = queue.pop(0)

                    if current_tool_id in visited or depth > 2:  # Limit traversal depth
                        continue

                    visited.add(current_tool_id)

                    if current_tool_id in self.registry.tools:
                        tool = self.registry.tools[current_tool_id]

                        # Calculate confidence based on score and depth
                        confidence = current_score * (0.8 ** depth)

                        recommendations.append(ToolRecommendation(
                            tool_id=tool.id,
                            tool_name=tool.name,
                            confidence_score=confidence,
                            relevance_score=confidence,
                            reasoning=f"Graph traversal from seed (depth {depth})",
                            method_used=DiscoveryMethod.GRAPH_TRAVERSAL
                        ))

                    # Add neighbors to queue
                    if current_tool_id in self.tool_graph:
                        for neighbor_id in self.tool_graph.neighbors(current_tool_id):
                            edge_weight = self.tool_graph[current_tool_id][neighbor_id].get('weight', 0.5)
                            neighbor_score = current_score * edge_weight
                            queue.append((neighbor_id, neighbor_score, depth + 1))

        # Remove duplicates and sort
        seen = set()
        unique_recommendations = []
        for rec in recommendations:
            if rec.tool_id not in seen:
                seen.add(rec.tool_id)
                unique_recommendations.append(rec)

        unique_recommendations.sort(key=lambda x: x.confidence_score, reverse=True)
        return unique_recommendations[:limit]

    async def _hybrid_discovery(self,
                               query: str,
                               limit: int,
                               context: Optional[Dict[str, Any]] = None) -> List[ToolRecommendation]:
        """Combine multiple discovery methods for best results"""

        # Run multiple discovery methods
        methods = [
            DiscoveryMethod.SEMANTIC_SEARCH,
            DiscoveryMethod.CAPABILITY_MATCHING,
            DiscoveryMethod.USAGE_PATTERN,
            DiscoveryMethod.GRAPH_TRAVERSAL
        ]

        all_recommendations = []

        for method in methods:
            try:
                if method == DiscoveryMethod.CAPABILITY_MATCHING:
                    capability_query = self._parse_capability_query(query)
                    method_recs = await self._capability_matching_discovery(capability_query, limit)
                else:
                    method_recs = await self.discover_tools_by_query(query, method, limit, context)

                all_recommendations.extend(method_recs)
            except Exception as e:
                self.logger.error(f"Error in {method.value} discovery: {e}")

        # Combine and score recommendations
        tool_scores = {}
        for rec in all_recommendations:
            if rec.tool_id not in tool_scores:
                tool_scores[rec.tool_id] = {
                    'recommendation': rec,
                    'total_score': 0.0,
                    'method_count': 0,
                    'methods': []
                }

            tool_scores[rec.tool_id]['total_score'] += rec.confidence_score
            tool_scores[rec.tool_id]['method_count'] += 1
            tool_scores[rec.tool_id]['methods'].append(rec.method_used.value)

        # Create final recommendations with combined scores
        final_recommendations = []
        for tool_id, score_data in tool_scores.items():
            # Average score weighted by number of methods
            avg_score = score_data['total_score'] / score_data['method_count']
            # Boost score for tools found by multiple methods
            method_boost = min(score_data['method_count'] * 0.1, 0.3)
            final_score = min(avg_score + method_boost, 1.0)

            rec = score_data['recommendation']
            rec.confidence_score = final_score
            rec.relevance_score = final_score
            rec.method_used = DiscoveryMethod.HYBRID
            rec.reasoning = f"Found by {score_data['method_count']} methods: {', '.join(score_data['methods'])}"

            final_recommendations.append(rec)

        # Sort by final score
        final_recommendations.sort(key=lambda x: x.confidence_score, reverse=True)
        return final_recommendations[:limit]

    def _parse_capability_query(self, query: str) -> CapabilityQuery:
        """Parse natural language query into capability query"""

        # Simple keyword-based parsing (could be enhanced with NLP)
        words = query.lower().split()

        required_capabilities = []
        optional_capabilities = []

        # Look for capability keywords
        capability_keywords = {
            'analyze': ['analysis', 'analyze', 'examine'],
            'generate': ['generate', 'create', 'produce'],
            'transform': ['transform', 'convert', 'change'],
            'process': ['process', 'handle', 'manage'],
            'classify': ['classify', 'categorize', 'label'],
            'predict': ['predict', 'forecast', 'estimate']
        }

        for word in words:
            for cap, keywords in capability_keywords.items():
                if word in keywords:
                    required_capabilities.append(cap)

        # If no specific capabilities found, use query words as is
        if not required_capabilities:
            required_capabilities = [word for word in words if len(word) > 3]

        return CapabilityQuery(
            required_capabilities=required_capabilities,
            optional_capabilities=optional_capabilities
        )

    def _evaluate_performance_requirements(self,
                                         metrics: ToolMetrics,
                                         requirements: Dict[str, float]) -> float:
        """Evaluate if tool meets performance requirements"""

        score = 0.0
        checks = 0

        for req_name, req_value in requirements.items():
            checks += 1

            if req_name == 'max_execution_time':
                if metrics.average_execution_time <= req_value:
                    score += 1.0
                else:
                    score += max(0, 1.0 - (metrics.average_execution_time - req_value) / req_value)

            elif req_name == 'min_success_rate':
                if metrics.success_rate >= req_value:
                    score += 1.0
                else:
                    score += metrics.success_rate / req_value

            elif req_name == 'max_memory_usage':
                if metrics.memory_usage <= req_value:
                    score += 1.0
                else:
                    score += max(0, 1.0 - (metrics.memory_usage - req_value) / req_value)

        return score / checks if checks > 0 else 1.0

    def learn_from_execution(self, tool_id: str, context: Dict[str, Any], success: bool):
        """Learn from tool execution results to improve recommendations"""

        # Update usage patterns
        if success:
            self.usage_patterns.user_preferences[tool_id] = (
                self.usage_patterns.user_preferences.get(tool_id, 0) + 1
            )

            # Learn context associations
            for key, value in context.items():
                context_key = f"{key}:{value}".lower()
                if context_key not in self.usage_patterns.context_associations:
                    self.usage_patterns.context_associations[context_key] = []

                if tool_id not in self.usage_patterns.context_associations[context_key]:
                    self.usage_patterns.context_associations[context_key].append(tool_id)

        # Update success tracking
        if success:
            self.discovery_stats['successful_recommendations'] += 1

    def get_tool_alternatives(self, tool_id: str, limit: int = 5) -> List[ToolRecommendation]:
        """Get alternative tools for a given tool"""

        if tool_id not in self.registry.tools:
            return []

        original_tool = self.registry.tools[tool_id]
        alternatives = []

        # Find tools in same category with high similarity
        for tool in self.registry.list_tools():
            if tool.id != tool_id and tool.category == original_tool.category:
                similarity = self._calculate_tool_similarity(original_tool, tool)

                if similarity > 0.5:  # Threshold for alternatives
                    alternatives.append(ToolRecommendation(
                        tool_id=tool.id,
                        tool_name=tool.name,
                        confidence_score=similarity,
                        relevance_score=similarity,
                        reasoning=f"Alternative in {tool.category.value} category",
                        method_used=DiscoveryMethod.SIMILARITY_CLUSTERING
                    ))

        alternatives.sort(key=lambda x: x.confidence_score, reverse=True)
        return alternatives[:limit]

    def suggest_tool_composition(self, goal: str) -> List[Dict[str, Any]]:
        """Suggest a composition of tools to achieve a goal"""

        # Parse goal into steps
        steps = self._decompose_goal(goal)

        composition_steps = []
        for i, step in enumerate(steps):
            # Find tools for each step
            step_tools = asyncio.run(
                self.discover_tools_by_query(step, limit=3)
            )

            if step_tools:
                best_tool = step_tools[0]
                composition_steps.append({
                    'step': i + 1,
                    'description': step,
                    'tool_id': best_tool.tool_id,
                    'tool_name': best_tool.tool_name,
                    'parameters': {},
                    'alternatives': [t.tool_id for t in step_tools[1:]]
                })

        return composition_steps

    def _decompose_goal(self, goal: str) -> List[str]:
        """Decompose a complex goal into simpler steps"""

        # Simple keyword-based decomposition
        if 'analyze' in goal.lower() and 'generate' in goal.lower():
            return [
                f"analyze {goal.split('analyze')[1].split('generate')[0].strip()}",
                f"generate {goal.split('generate')[1].strip()}"
            ]
        elif 'process' in goal.lower() and 'output' in goal.lower():
            return [
                f"process {goal.split('process')[1].split('output')[0].strip()}",
                f"output {goal.split('output')[1].strip()}"
            ]
        else:
            # Return as single step
            return [goal]

    def export_discovery_insights(self) -> Dict[str, Any]:
        """Export insights about tool discovery patterns"""

        return {
            'discovery_statistics': self.discovery_stats,
            'tool_graph_metrics': {
                'nodes': self.tool_graph.number_of_nodes(),
                'edges': self.tool_graph.number_of_edges(),
                'density': nx.density(self.tool_graph),
                'connected_components': nx.number_weakly_connected_components(self.tool_graph)
            },
            'usage_patterns': {
                'top_tools': sorted(
                    self.usage_patterns.user_preferences.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:10],
                'context_associations': len(self.usage_patterns.context_associations),
                'tool_combinations': len(self.usage_patterns.tool_combinations)
            },
            'cache_performance': {
                'similarity_cache_size': len(self.similarity_cache),
                'recommendation_cache_size': len(self.recommendation_cache),
                'capability_cache_size': len(self.capability_cache)
            }
        }

# === Integration Functions ===

def setup_auto_discovery(registry: DynamicToolRegistry) -> AdvancedToolDiscoveryEngine:
    """Setup automatic tool discovery for a registry"""

    discovery_engine = AdvancedToolDiscoveryEngine(registry)

    # Auto-discover tools from code directories
    discovery_engine._auto_discover_from_directory(Path("./tools"))

    return discovery_engine

def create_discovery_cli(discovery_engine: AdvancedToolDiscoveryEngine):
    """Create CLI interface for tool discovery"""

    import click

    @click.group()
    def discovery_cli():
        """Tool Discovery CLI"""
        pass

    @discovery_cli.command()
    @click.argument('query')
    @click.option('--method', type=click.Choice([m.value for m in DiscoveryMethod]), default='hybrid')
    @click.option('--limit', default=10)
    def search(query, method, limit):
        """Search for tools"""
        method_enum = DiscoveryMethod(method)
        recommendations = asyncio.run(
            discovery_engine.discover_tools_by_query(query, method_enum, limit)
        )

        for i, rec in enumerate(recommendations, 1):
            click.echo(f"{i}. {rec.tool_name} (confidence: {rec.confidence_score:.3f})")
            click.echo(f"   {rec.reasoning}")
            click.echo()

    @discovery_cli.command()
    @click.argument('tool_id')
    def alternatives(tool_id):
        """Find alternatives for a tool"""
        alts = discovery_engine.get_tool_alternatives(tool_id)

        for alt in alts:
            click.echo(f"- {alt.tool_name} (similarity: {alt.confidence_score:.3f})")

    @discovery_cli.command()
    @click.argument('goal')
    def compose(goal):
        """Suggest tool composition for a goal"""
        composition = discovery_engine.suggest_tool_composition(goal)

        click.echo(f"Suggested composition for: {goal}")
        for step in composition:
            click.echo(f"Step {step['step']}: {step['description']}")
            click.echo(f"  Tool: {step['tool_name']}")
            if step['alternatives']:
                click.echo(f"  Alternatives: {', '.join(step['alternatives'])}")
            click.echo()

    return discovery_cli

if __name__ == "__main__":
    # Example usage
    from dynamic_tool_registry import DynamicToolRegistry

    async def main():
        # Initialize systems
        registry = DynamicToolRegistry()
        discovery_engine = AdvancedToolDiscoveryEngine(registry)

        # Test discovery
        recommendations = await discovery_engine.discover_tools_by_query(
            "analyze sentiment of text",
            method=DiscoveryMethod.HYBRID,
            limit=5
        )

        print("Tool Recommendations:")
        for rec in recommendations:
            print(f"- {rec.tool_name}: {rec.confidence_score:.3f}")
            print(f"  {rec.reasoning}")
            print()

        # Test composition
        composition = discovery_engine.suggest_tool_composition(
            "analyze customer feedback and generate summary report"
        )

        print("Suggested Tool Composition:")
        for step in composition:
            print(f"Step {step['step']}: {step['tool_name']}")

    asyncio.run(main())