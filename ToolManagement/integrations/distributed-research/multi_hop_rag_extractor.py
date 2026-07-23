"""
Multi-hop RAG Extractor for Context Arenas
Sophisticated retrieval and reasoning across isolated context spaces
"""

import re
import json
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import numpy as np
from collections import defaultdict


class QueryType(Enum):
    """Types of queries for different extraction strategies"""
    FACTUAL = "factual"  # Direct fact retrieval
    COMPARATIVE = "comparative"  # Compare across sources
    AGGREGATIVE = "aggregative"  # Aggregate information
    EXPLORATORY = "exploratory"  # Discover related info
    DIAGNOSTIC = "diagnostic"  # Find problems/errors
    PROCEDURAL = "procedural"  # Step-by-step instructions


class HopStrategy(Enum):
    """Strategies for multi-hop reasoning"""
    BREADTH_FIRST = "breadth"  # Explore all arenas equally
    DEPTH_FIRST = "depth"  # Deep dive into most relevant
    GUIDED = "guided"  # Follow explicit path
    ADAPTIVE = "adaptive"  # Adjust based on findings


@dataclass
class ExtractionPath:
    """Represents a path through the extraction process"""
    query: str
    hops: List['HopResult'] = field(default_factory=list)
    strategy: HopStrategy = HopStrategy.ADAPTIVE
    max_tokens: int = 2000
    confidence: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HopResult:
    """Result from a single hop in the extraction"""
    hop_number: int
    query: str
    arena: str
    documents: List[Dict[str, Any]]
    synthesis: str
    confidence: float
    tokens_used: int
    reasoning: str


class MultiHopRAGExtractor:
    """
    Advanced multi-hop RAG system for extracting from context arenas
    """

    def __init__(self, arena_manager: Any):  # Any = ContextArenaManager
        self.arena_manager = arena_manager
        self.query_cache = {}  # Cache recent queries
        self.extraction_history = []  # Track extraction paths

        # Query understanding patterns
        self.query_patterns = {
            QueryType.FACTUAL: [
                r"what is", r"who is", r"where is", r"when did", r"how many"
            ],
            QueryType.COMPARATIVE: [
                r"compare", r"difference between", r"versus", r"vs\.", r"better than"
            ],
            QueryType.AGGREGATIVE: [
                r"all", r"list of", r"summarize", r"overview", r"collection"
            ],
            QueryType.EXPLORATORY: [
                r"related to", r"about", r"concerning", r"regarding"
            ],
            QueryType.DIAGNOSTIC: [
                r"error", r"problem", r"issue", r"bug", r"wrong", r"failing"
            ],
            QueryType.PROCEDURAL: [
                r"how to", r"steps to", r"process for", r"implement", r"setup"
            ]
        }

        # Arena affinity matrix (which query types prefer which arenas)
        self.arena_affinity = {
            QueryType.FACTUAL: ['docs', 'search', 'memory'],
            QueryType.COMPARATIVE: ['code', 'docs', 'search'],
            QueryType.AGGREGATIVE: ['search', 'memory', 'docs'],
            QueryType.EXPLORATORY: ['search', 'docs', 'code'],
            QueryType.DIAGNOSTIC: ['logs', 'code', 'memory'],
            QueryType.PROCEDURAL: ['docs', 'code', 'search']
        }

    def extract(
        self,
        query: str,
        strategy: HopStrategy = HopStrategy.ADAPTIVE,
        max_hops: int = 4,
        max_tokens: int = 2000
    ) -> Dict[str, Any]:
        """
        Main extraction method with multi-hop reasoning
        """
        # Check cache
        cache_key = f"{query}:{strategy.value}"
        if cache_key in self.query_cache:
            cached = self.query_cache[cache_key]
            if (datetime.now() - cached['timestamp']).seconds < 300:  # 5 min cache
                return cached['result']

        # Create extraction path
        path = ExtractionPath(
            query=query,
            strategy=strategy,
            max_tokens=max_tokens
        )

        # Understand query
        query_type = self._understand_query(query)
        path.metadata['query_type'] = query_type.value

        # Execute multi-hop extraction
        result = self._execute_multi_hop(
            path, query_type, max_hops
        )

        # Cache result
        self.query_cache[cache_key] = {
            'result': result,
            'timestamp': datetime.now()
        }

        # Track in history
        self.extraction_history.append(path)

        return result

    def _understand_query(self, query: str) -> QueryType:
        """Understand the type of query"""
        query_lower = query.lower()

        for query_type, patterns in self.query_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    return query_type

        return QueryType.EXPLORATORY  # Default

    def _execute_multi_hop(
        self,
        path: ExtractionPath,
        query_type: QueryType,
        max_hops: int
    ) -> Dict[str, Any]:
        """Execute multi-hop extraction based on strategy"""
        current_query = path.query
        total_tokens = 0
        all_findings = []

        for hop_num in range(max_hops):
            # Determine target arenas for this hop
            target_arenas = self._select_arenas(
                current_query,
                query_type,
                path.strategy,
                hop_num
            )

            # Extract from each arena
            hop_documents = []
            for arena in target_arenas:
                docs = self._extract_from_arena(
                    current_query,
                    arena,
                    max_docs=3
                )
                hop_documents.extend(docs)

            if not hop_documents:
                break  # No more relevant documents

            # Synthesize hop results
            synthesis = self._synthesize_hop(
                current_query,
                hop_documents,
                query_type
            )

            # Create hop result
            hop_result = HopResult(
                hop_number=hop_num + 1,
                query=current_query,
                arena=','.join(target_arenas),
                documents=hop_documents,
                synthesis=synthesis,
                confidence=self._calculate_confidence(hop_documents),
                tokens_used=len(synthesis) // 4,
                reasoning=self._generate_reasoning(hop_documents, query_type)
            )

            path.hops.append(hop_result)
            all_findings.append(synthesis)
            total_tokens += hop_result.tokens_used

            # Check if we have enough information
            if self._should_stop(path, total_tokens):
                break

            # Generate next query
            current_query = self._generate_next_query(
                path,
                hop_result,
                query_type
            )

        # Final synthesis across all hops
        final_result = self._synthesize_final(
            path,
            all_findings,
            query_type
        )

        return final_result

    def _select_arenas(
        self,
        query: str,
        query_type: QueryType,
        strategy: HopStrategy,
        hop_number: int
    ) -> List[str]:
        """Select which arenas to search based on strategy"""
        available_arenas = list(self.arena_manager.arenas.keys())

        if strategy == HopStrategy.BREADTH_FIRST:
            # Search all arenas equally
            return available_arenas

        elif strategy == HopStrategy.DEPTH_FIRST:
            # Focus on most relevant arena
            preferred = self.arena_affinity.get(query_type, available_arenas)
            if hop_number == 0:
                return preferred[:1] if preferred else available_arenas[:1]
            else:
                return preferred[1:2] if len(preferred) > 1 else available_arenas[:1]

        elif strategy == HopStrategy.Adaptive:
            # Adapt based on query type and hop number
            preferred = self.arena_affinity.get(query_type, available_arenas)
            if hop_number == 0:
                return preferred[:2] if len(preferred) > 1 else available_arenas
            else:
                # Expand search in later hops
                return preferred[:3] if len(preferred) > 2 else available_arenas

        else:  # GUIDED
            # Use explicit arena ordering
            return available_arenas

    def _extract_from_arena(
        self,
        query: str,
        arena_name: str,
        max_docs: int = 5
    ) -> List[Dict[str, Any]]:
        """Extract documents from a specific arena"""
        if arena_name not in self.arena_manager.arenas:
            return []

        arena = self.arena_manager.arenas[arena_name]
        results = arena.search(query, k=max_docs)

        documents = []
        for doc in results:
            documents.append({
                'arena': arena_name,
                'content': doc.content,
                'metadata': doc.metadata,
                'relevance': doc.access_count,
                'timestamp': doc.timestamp.isoformat()
            })

        return documents

    def _synthesize_hop(
        self,
        query: str,
        documents: List[Dict[str, Any]],
        query_type: QueryType
    ) -> str:
        """Synthesize findings from a single hop"""
        if not documents:
            return f"No relevant information found for: {query}"

        # Different synthesis strategies based on query type
        if query_type == QueryType.FACTUAL:
            # Extract specific facts
            return self._extract_facts(documents)

        elif query_type == QueryType.COMPARATIVE:
            # Compare and contrast
            return self._compare_information(documents)

        elif query_type == QueryType.AGGREGATIVE:
            # Aggregate all information
            return self._aggregate_information(documents)

        elif query_type == QueryType.DIAGNOSTIC:
            # Focus on problems/errors
            return self._extract_diagnostics(documents)

        elif query_type == QueryType.PROCEDURAL:
            # Extract steps/procedures
            return self._extract_procedures(documents)

        else:  # EXPLORATORY
            # General synthesis
            return self._general_synthesis(documents)

    def _extract_facts(self, documents: List[Dict[str, Any]]) -> str:
        """Extract specific facts from documents"""
        facts = []
        for doc in documents[:3]:  # Limit to top 3
            content = doc['content'][:500]  # First 500 chars
            # Extract sentences that look like facts
            sentences = content.split('.')
            for sentence in sentences:
                if len(sentence) > 20 and len(sentence) < 200:
                    facts.append(sentence.strip())

        return "Facts found:\n" + "\n- ".join(facts[:5])

    def _compare_information(self, documents: List[Dict[str, Any]]) -> str:
        """Compare information across documents"""
        comparison = "Comparison across sources:\n"

        # Group by arena
        by_arena = defaultdict(list)
        for doc in documents:
            by_arena[doc['arena']].append(doc['content'][:200])

        for arena, contents in by_arena.items():
            comparison += f"\nFrom {arena}:\n"
            for content in contents[:2]:
                comparison += f"  - {content}\n"

        return comparison

    def _aggregate_information(self, documents: List[Dict[str, Any]]) -> str:
        """Aggregate all relevant information"""
        aggregated = "Aggregated information:\n"
        seen_content = set()

        for doc in documents:
            preview = doc['content'][:300]
            # Avoid duplicates
            content_hash = hash(preview)
            if content_hash not in seen_content:
                seen_content.add(content_hash)
                aggregated += f"\n- {preview}..."

        return aggregated

    def _extract_diagnostics(self, documents: List[Dict[str, Any]]) -> str:
        """Extract diagnostic information (errors, issues)"""
        diagnostics = "Diagnostic findings:\n"
        error_patterns = ['error', 'exception', 'failed', 'issue', 'problem']

        for doc in documents:
            content = doc['content']
            for pattern in error_patterns:
                if pattern.lower() in content.lower():
                    # Extract context around error
                    idx = content.lower().find(pattern.lower())
                    start = max(0, idx - 50)
                    end = min(len(content), idx + 150)
                    diagnostics += f"\n- {content[start:end]}\n"
                    break

        return diagnostics

    def _extract_procedures(self, documents: List[Dict[str, Any]]) -> str:
        """Extract step-by-step procedures"""
        procedures = "Procedural steps found:\n"
        step_patterns = [r'\d+\.', r'step \d+', r'first', r'then', r'finally']

        for doc in documents:
            content = doc['content']
            # Look for numbered lists or step indicators
            for pattern in step_patterns:
                if re.search(pattern, content, re.IGNORECASE):
                    procedures += f"\nFrom {doc['arena']}:\n{content[:400]}\n"
                    break

        return procedures

    def _general_synthesis(self, documents: List[Dict[str, Any]]) -> str:
        """General synthesis for exploratory queries"""
        synthesis = "Relevant information:\n"

        for doc in documents[:3]:
            synthesis += f"\nFrom {doc['arena']} arena:\n"
            synthesis += f"{doc['content'][:300]}...\n"

        return synthesis

    def _calculate_confidence(self, documents: List[Dict[str, Any]]) -> float:
        """Calculate confidence score for extraction"""
        if not documents:
            return 0.0

        # Factors: document count, relevance scores, arena diversity
        doc_count_score = min(len(documents) / 5, 1.0)

        avg_relevance = sum(d.get('relevance', 0) for d in documents) / len(documents)
        relevance_score = min(avg_relevance / 10, 1.0)

        unique_arenas = len(set(d['arena'] for d in documents))
        diversity_score = min(unique_arenas / 3, 1.0)

        return (doc_count_score + relevance_score + diversity_score) / 3

    def _generate_reasoning(
        self,
        documents: List[Dict[str, Any]],
        query_type: QueryType
    ) -> str:
        """Generate reasoning about the extraction"""
        reasoning = f"Query type: {query_type.value}\n"
        reasoning += f"Found {len(documents)} relevant documents\n"

        arenas = set(d['arena'] for d in documents)
        reasoning += f"Sources: {', '.join(arenas)}\n"

        return reasoning

    def _should_stop(
        self,
        path: ExtractionPath,
        total_tokens: int
    ) -> bool:
        """Determine if we should stop hopping"""
        # Stop conditions
        if total_tokens >= path.max_tokens * 0.9:
            return True

        if len(path.hops) >= 2:
            # Check if we're getting diminishing returns
            if path.hops[-1].confidence < 0.3:
                return True

            # Check if content is repetitive
            if len(path.hops) > 1:
                last_synthesis = path.hops[-1].synthesis
                prev_synthesis = path.hops[-2].synthesis
                if self._similarity(last_synthesis, prev_synthesis) > 0.8:
                    return True

        return False

    def _similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two texts (simplified)"""
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())

        if not words1 or not words2:
            return 0.0

        intersection = words1 & words2
        union = words1 | words2

        return len(intersection) / len(union)

    def _generate_next_query(
        self,
        path: ExtractionPath,
        last_hop: HopResult,
        query_type: QueryType
    ) -> str:
        """Generate the next query based on findings"""
        # Different strategies based on query type
        if query_type == QueryType.PROCEDURAL:
            return f"More details about: {path.query}"

        elif query_type == QueryType.DIAGNOSTIC:
            # Look for root causes
            if 'error' in last_hop.synthesis.lower():
                return f"Root cause of: {path.query}"
            else:
                return f"Solutions for: {path.query}"

        elif query_type == QueryType.COMPARATIVE:
            return f"Additional differences in: {path.query}"

        else:
            # Generic follow-up
            return f"Specific examples of: {path.query}"

    def _synthesize_final(
        self,
        path: ExtractionPath,
        all_findings: List[str],
        query_type: QueryType
    ) -> Dict[str, Any]:
        """Create final synthesis across all hops"""
        # Combine all findings
        combined_text = "\n\n".join(all_findings)

        # Create structured result
        result = {
            'query': path.query,
            'query_type': query_type.value,
            'strategy': path.strategy.value,
            'total_hops': len(path.hops),
            'confidence': sum(h.confidence for h in path.hops) / max(len(path.hops), 1),
            'synthesis': self._create_final_synthesis(combined_text, query_type),
            'sources': self._extract_sources(path),
            'tokens_used': sum(h.tokens_used for h in path.hops),
            'reasoning_chain': [h.reasoning for h in path.hops]
        }

        # Add query-specific results
        if query_type == QueryType.DIAGNOSTIC:
            result['issues_found'] = self._extract_issues(path)
        elif query_type == QueryType.PROCEDURAL:
            result['steps'] = self._extract_steps(path)

        return result

    def _create_final_synthesis(
        self,
        combined_text: str,
        query_type: QueryType
    ) -> str:
        """Create final synthesis based on query type"""
        max_length = 1500

        if len(combined_text) <= max_length:
            return combined_text

        # Truncate intelligently based on query type
        if query_type in [QueryType.FACTUAL, QueryType.DIAGNOSTIC]:
            # Keep the most important parts
            return combined_text[:max_length] + "\n... [truncated]"
        else:
            # Keep beginning and end
            half = max_length // 2
            return (
                combined_text[:half] +
                "\n... [content omitted] ...\n" +
                combined_text[-half:]
            )

    def _extract_sources(self, path: ExtractionPath) -> List[str]:
        """Extract unique sources from path"""
        sources = set()
        for hop in path.hops:
            for doc in hop.documents:
                sources.add(f"{doc['arena']}: {doc.get('metadata', {}).get('source', 'unknown')}")
        return list(sources)

    def _extract_issues(self, path: ExtractionPath) -> List[str]:
        """Extract specific issues for diagnostic queries"""
        issues = []
        error_keywords = ['error', 'exception', 'failed', 'issue', 'problem', 'bug']

        for hop in path.hops:
            for keyword in error_keywords:
                if keyword in hop.synthesis.lower():
                    # Extract sentence containing the keyword
                    sentences = hop.synthesis.split('.')
                    for sentence in sentences:
                        if keyword in sentence.lower():
                            issues.append(sentence.strip())

        return issues[:10]  # Limit to 10 issues

    def _extract_steps(self, path: ExtractionPath) -> List[str]:
        """Extract steps for procedural queries"""
        steps = []
        step_patterns = [r'(\d+)\.([^.]+)', r'step (\d+):([^.]+)']

        for hop in path.hops:
            for pattern in step_patterns:
                matches = re.findall(pattern, hop.synthesis, re.IGNORECASE)
                for match in matches:
                    if len(match) >= 2:
                        steps.append(f"Step {match[0]}: {match[1].strip()}")

        return steps

    def explain_extraction(self, extraction_id: int = -1) -> str:
        """Explain the reasoning behind an extraction"""
        if not self.extraction_history:
            return "No extraction history available"

        path = self.extraction_history[extraction_id]

        explanation = f"Extraction Explanation for: {path.query}\n"
        explanation += f"Strategy: {path.strategy.value}\n\n"

        for hop in path.hops:
            explanation += f"Hop {hop.hop_number}:\n"
            explanation += f"  Query: {hop.query}\n"
            explanation += f"  Arena: {hop.arena}\n"
            explanation += f"  Documents found: {len(hop.documents)}\n"
            explanation += f"  Confidence: {hop.confidence:.2f}\n"
            explanation += f"  Reasoning: {hop.reasoning}\n\n"

        return explanation


# Example usage
if __name__ == "__main__":
    # Mock arena manager for testing
    class MockArenaManager:
        def __init__(self):
            self.arenas = {
                'search': type('Arena', (), {
                    'search': lambda self, q, k: [
                        type('Doc', (), {
                            'content': f"Search result about {q}",
                            'metadata': {'source': 'web'},
                            'access_count': 1,
                            'timestamp': datetime.now()
                        })() for _ in range(k)
                    ]
                })(),
                'code': type('Arena', (), {
                    'search': lambda self, q, k: [
                        type('Doc', (), {
                            'content': f"Code implementation for {q}",
                            'metadata': {'file': 'main.py'},
                            'access_count': 2,
                            'timestamp': datetime.now()
                        })() for _ in range(k)
                    ]
                })()
            }

    # Create extractor
    manager = MockArenaManager()
    extractor = MultiHopRAGExtractor(manager)

    # Test different query types
    queries = [
        "How to implement async functions in Python?",
        "Compare REST API vs GraphQL",
        "List all authentication methods",
        "Error: ImportError in module loading",
        "What is machine learning?"
    ]

    for query in queries:
        print(f"\nQuery: {query}")
        print("-" * 50)

        result = extractor.extract(
            query,
            strategy=HopStrategy.ADAPTIVE,
            max_hops=3
        )

        print(f"Query Type: {result['query_type']}")
        print(f"Confidence: {result['confidence']:.2f}")
        print(f"Hops: {result['total_hops']}")
        print(f"Tokens Used: {result['tokens_used']}")
        print(f"\nSynthesis:\n{result['synthesis'][:500]}...")

        if 'issues_found' in result:
            print(f"\nIssues Found: {result['issues_found']}")
        if 'steps' in result:
            print(f"\nSteps: {result['steps']}")