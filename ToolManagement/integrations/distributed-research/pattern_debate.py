#!/usr/bin/env python3
"""
MULTI-AGENT PATTERN DEBATE SYSTEM

A sophisticated debate framework where specialized advocate agents argue for
different architectural concerns, with an arbitrator synthesizing the final
recommendation through structured deliberation.

Features:
- Multiple advocate agents (reliability, performance, simplicity, security, cost)
- Structured debate rounds (opening, rebuttal, evidence, closing)
- Evidence-based argumentation using pattern knowledge base
- Arbitrator with weighted resolution
- Debate transcript and reasoning chain
- Consensus detection and deadlock resolution
"""

import json
import time
import math
import random
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
from abc import ABC, abstractmethod

from pattern_intelligence import (
    PatternKnowledgeBase,
    PatternDomain,
    PatternIntent,
    Pattern,
    CausalReasoningEngine,
    BayesianPatternMatcher
)


# =============================================================================
# DEBATE STRUCTURES
# =============================================================================

class DebatePhase(Enum):
    """Phases of the debate."""
    OPENING = "opening"
    REBUTTAL = "rebuttal"
    EVIDENCE = "evidence"
    CROSS_EXAMINATION = "cross_examination"
    CLOSING = "closing"
    DELIBERATION = "deliberation"


class ArgumentStrength(Enum):
    """Strength of an argument."""
    WEAK = 1
    MODERATE = 2
    STRONG = 3
    COMPELLING = 4
    DECISIVE = 5


@dataclass
class Argument:
    """An argument made by an advocate."""
    advocate: str
    phase: DebatePhase
    claim: str
    reasoning: str
    evidence: List[str]
    patterns_proposed: List[str]
    patterns_opposed: List[str]
    strength: ArgumentStrength
    rebuts: Optional[str] = None  # ID of argument this rebuts
    confidence: float = 0.8
    timestamp: float = field(default_factory=time.time)

    @property
    def id(self) -> str:
        return f"{self.advocate}_{self.phase.value}_{int(self.timestamp * 1000) % 10000}"


@dataclass
class DebateContext:
    """Context for the debate."""
    requirement: str
    constraints: Dict[str, Any]
    existing_patterns: List[str]
    priority_weights: Dict[str, float]  # advocate -> weight
    max_complexity: int = 10
    budget_tier: str = "moderate"  # low, moderate, high, unlimited


@dataclass
class DebateResult:
    """Result of the debate."""
    recommended_patterns: List[Tuple[str, float, str]]  # (pattern, score, rationale)
    consensus_level: float  # 0-1, how much advocates agreed
    key_tradeoffs: List[Dict[str, str]]
    dissenting_opinions: List[Dict[str, str]]
    transcript: List[Argument]
    deliberation_notes: str
    confidence: float


# =============================================================================
# ADVOCATE AGENTS
# =============================================================================

class AdvocateAgent(ABC):
    """Base class for advocate agents."""

    def __init__(self, kb: PatternKnowledgeBase):
        self.kb = kb
        self.causal = CausalReasoningEngine(kb.graph)
        self.name = "base_advocate"
        self.concern = "general"
        self.icon = "?"

    @abstractmethod
    def evaluate_pattern(self, pattern: Pattern, context: DebateContext) -> Tuple[float, str]:
        """Evaluate a pattern from this advocate's perspective. Returns (score, reasoning)."""
        pass

    @abstractmethod
    def make_opening(self, context: DebateContext) -> Argument:
        """Make opening argument."""
        pass

    @abstractmethod
    def make_rebuttal(self, context: DebateContext,
                      opponent_args: List[Argument]) -> Argument:
        """Rebut opponent arguments."""
        pass

    def gather_evidence(self, pattern_ids: List[str],
                        context: DebateContext) -> List[str]:
        """Gather evidence for/against patterns."""
        evidence = []
        for pid in pattern_ids:
            if pid in self.kb.graph.nodes:
                pattern = self.kb.graph.nodes[pid]
                score, reasoning = self.evaluate_pattern(pattern, context)
                if score > 0.6:
                    evidence.append(f"[+] {pattern.name}: {reasoning}")
                elif score < 0.4:
                    evidence.append(f"[-] {pattern.name}: {reasoning}")
        return evidence

    def make_closing(self, context: DebateContext,
                     all_arguments: List[Argument]) -> Argument:
        """Make closing argument summarizing position."""
        # Find patterns this advocate has consistently supported
        supported = defaultdict(int)
        opposed = defaultdict(int)

        for arg in all_arguments:
            if arg.advocate == self.name:
                for p in arg.patterns_proposed:
                    supported[p] += 1
                for p in arg.patterns_opposed:
                    opposed[p] += 1

        top_supported = sorted(supported.keys(), key=lambda x: -supported[x])[:3]
        top_opposed = sorted(opposed.keys(), key=lambda x: -opposed[x])[:2]

        claim = f"For {self.concern}, I strongly recommend: {', '.join(top_supported)}"
        if top_opposed:
            claim += f". I caution against: {', '.join(top_opposed)}"

        return Argument(
            advocate=self.name,
            phase=DebatePhase.CLOSING,
            claim=claim,
            reasoning=f"Throughout this debate, I've consistently advocated for patterns that maximize {self.concern}.",
            evidence=[f"Supported {p} {supported[p]} times" for p in top_supported],
            patterns_proposed=top_supported,
            patterns_opposed=top_opposed,
            strength=ArgumentStrength.STRONG,
            confidence=0.85
        )


class ReliabilityAdvocate(AdvocateAgent):
    """Advocates for reliability, fault tolerance, and availability."""

    def __init__(self, kb: PatternKnowledgeBase):
        super().__init__(kb)
        self.name = "reliability_advocate"
        self.concern = "reliability"
        self.icon = "🛡️"

        self.preferred_patterns = [
            'circuit_breaker', 'retry_backoff', 'bulkhead', 'fallback',
            'timeout', 'health_check', 'saga', 'idempotent_receiver',
            'dead_letter_queue', 'checkpoint'
        ]

        self.scoring_factors = {
            PatternIntent.RELIABILITY: 1.0,
            PatternIntent.AVAILABILITY: 0.9,
            PatternIntent.CONSISTENCY: 0.7,
            PatternIntent.ISOLATION: 0.6
        }

    def evaluate_pattern(self, pattern: Pattern, context: DebateContext) -> Tuple[float, str]:
        score = 0.3  # Base score

        # Check if pattern is in preferred list
        if pattern.id in self.preferred_patterns:
            score += 0.4

        # Check ontology intents
        if pattern.ontology:
            for intent in pattern.ontology.intents:
                if intent in self.scoring_factors:
                    score += self.scoring_factors[intent] * 0.2

            # Resilience domain is our home turf
            if pattern.ontology.domain == PatternDomain.RESILIENCE:
                score += 0.2

        # Check for availability constraint
        if 'availability' in context.constraints:
            target = context.constraints['availability']
            if target >= 0.999:  # High availability requirement
                score += 0.15

        reasoning = self._generate_reasoning(pattern, score, context)
        return min(1.0, score), reasoning

    def _generate_reasoning(self, pattern: Pattern, score: float,
                            context: DebateContext) -> str:
        if score > 0.7:
            return f"{pattern.name} is essential for system reliability - it prevents cascade failures and ensures graceful degradation."
        elif score > 0.5:
            return f"{pattern.name} contributes to reliability by providing fault isolation."
        else:
            return f"{pattern.name} has limited reliability benefits in this context."

    def make_opening(self, context: DebateContext) -> Argument:
        # Analyze requirement for reliability signals
        req_lower = context.requirement.lower()
        reliability_keywords = ['reliable', 'fault', 'fail', 'available', 'uptime',
                                'resilient', 'recover', 'stable']
        keyword_matches = sum(1 for kw in reliability_keywords if kw in req_lower)

        # Recommend patterns
        recommended = []
        reasoning_parts = []

        # Always recommend circuit breaker for distributed systems
        if any(kw in req_lower for kw in ['service', 'api', 'distributed', 'microservice']):
            recommended.append('circuit_breaker')
            reasoning_parts.append("Circuit breaker prevents cascade failures across services")

        # Recommend retry for external calls
        if any(kw in req_lower for kw in ['external', 'call', 'network', 'api']):
            recommended.append('retry_backoff')
            reasoning_parts.append("Retry with backoff handles transient failures gracefully")

        # Recommend bulkhead for isolation
        if any(kw in req_lower for kw in ['isolate', 'critical', 'important', 'priority']):
            recommended.append('bulkhead')
            reasoning_parts.append("Bulkhead isolation contains failures to prevent system-wide impact")

        # High availability requirement
        if context.constraints.get('availability', 0) >= 0.999:
            recommended.extend(['fallback', 'health_check'])
            reasoning_parts.append("99.9%+ availability requires fallback strategies and health monitoring")

        # Transaction reliability
        if any(kw in req_lower for kw in ['transaction', 'payment', 'order', 'money']):
            recommended.append('saga')
            recommended.append('idempotent_receiver')
            reasoning_parts.append("Financial operations require saga pattern for reliable distributed transactions")

        # Ensure we have recommendations
        if not recommended:
            recommended = ['circuit_breaker', 'retry_backoff', 'timeout']
            reasoning_parts.append("Basic reliability patterns provide a foundation for fault tolerance")

        strength = ArgumentStrength.STRONG if keyword_matches >= 2 else ArgumentStrength.MODERATE

        return Argument(
            advocate=self.name,
            phase=DebatePhase.OPENING,
            claim=f"System reliability must be our primary concern. I propose: {', '.join(recommended[:4])}",
            reasoning=" ".join(reasoning_parts),
            evidence=[
                "Studies show 99.9% of outages are caused by cascade failures",
                "Circuit breaker reduces mean time to recovery by 60%",
                "Retry with backoff handles 80% of transient failures automatically"
            ],
            patterns_proposed=recommended[:4],
            patterns_opposed=[],
            strength=strength,
            confidence=0.85 + (keyword_matches * 0.03)
        )

    def make_rebuttal(self, context: DebateContext,
                      opponent_args: List[Argument]) -> Argument:
        rebuttals = []
        patterns_to_defend = []
        patterns_to_oppose = []

        for arg in opponent_args:
            if arg.advocate == self.name:
                continue

            # Rebut performance concerns about reliability patterns
            if arg.advocate == "performance_advocate":
                for pattern in arg.patterns_opposed:
                    if pattern in self.preferred_patterns:
                        patterns_to_defend.append(pattern)
                        rebuttals.append(
                            f"While {pattern} adds some latency, the alternative is complete "
                            f"system failure. A 10ms overhead is better than a 10-minute outage."
                        )

            # Rebut simplicity concerns
            if arg.advocate == "simplicity_advocate":
                for pattern in arg.patterns_opposed:
                    if pattern in self.preferred_patterns:
                        patterns_to_defend.append(pattern)
                        rebuttals.append(
                            f"The complexity of {pattern} is justified by the reliability it provides. "
                            f"Simple systems fail simply - completely."
                        )

            # Counter-propose against risky patterns
            for pattern in arg.patterns_proposed:
                if pattern in ['two_phase_commit']:  # 2PC has availability issues
                    patterns_to_oppose.append(pattern)
                    rebuttals.append(
                        f"{pattern} reduces availability during failures. Consider saga pattern instead."
                    )

        rebuttal_text = " ".join(rebuttals) if rebuttals else \
            "I maintain that reliability patterns are essential and their overhead is justified."

        return Argument(
            advocate=self.name,
            phase=DebatePhase.REBUTTAL,
            claim="Reliability cannot be compromised for marginal gains elsewhere.",
            reasoning=rebuttal_text,
            evidence=[
                "Amazon estimates each minute of downtime costs $220,000",
                "The cost of implementing reliability patterns is far less than outage costs"
            ],
            patterns_proposed=list(set(patterns_to_defend)),
            patterns_opposed=list(set(patterns_to_oppose)),
            strength=ArgumentStrength.STRONG,
            rebuts=opponent_args[0].id if opponent_args else None,
            confidence=0.8
        )


class PerformanceAdvocate(AdvocateAgent):
    """Advocates for performance, speed, and efficiency."""

    def __init__(self, kb: PatternKnowledgeBase):
        super().__init__(kb)
        self.name = "performance_advocate"
        self.concern = "performance"
        self.icon = "⚡"

        self.preferred_patterns = [
            'cache_aside', 'connection_pool', 'fan_out_fan_in', 'map_reduce',
            'rate_limiter', 'load_shedding', 'write_behind', 'competing_consumers'
        ]

        self.latency_concerns = [
            'circuit_breaker',  # Adds state checks
            'saga',  # Multiple round trips
            'two_phase_commit',  # Blocking waits
            'distributed_tracing'  # Instrumentation overhead
        ]

    def evaluate_pattern(self, pattern: Pattern, context: DebateContext) -> Tuple[float, str]:
        score = 0.5  # Base score

        if pattern.id in self.preferred_patterns:
            score += 0.3

        if pattern.id in self.latency_concerns:
            score -= 0.3

        if pattern.ontology:
            if pattern.ontology.domain == PatternDomain.PERFORMANCE:
                score += 0.25
            if PatternIntent.EFFICIENCY in pattern.ontology.intents:
                score += 0.2
            if PatternIntent.SCALABILITY in pattern.ontology.intents:
                score += 0.15

        # Latency constraint
        if 'max_latency_ms' in context.constraints:
            target = context.constraints['max_latency_ms']
            if target <= 50:  # Very strict latency
                if pattern.id in self.latency_concerns:
                    score -= 0.2
                if pattern.id in self.preferred_patterns:
                    score += 0.1

        reasoning = self._generate_reasoning(pattern, score)
        return max(0, min(1.0, score)), reasoning

    def _generate_reasoning(self, pattern: Pattern, score: float) -> str:
        if score > 0.7:
            return f"{pattern.name} directly improves system throughput and reduces latency."
        elif score < 0.4:
            return f"{pattern.name} introduces latency overhead that may impact user experience."
        else:
            return f"{pattern.name} has neutral performance characteristics."

    def make_opening(self, context: DebateContext) -> Argument:
        req_lower = context.requirement.lower()
        perf_keywords = ['fast', 'performance', 'latency', 'throughput', 'speed',
                         'efficient', 'scale', 'load', 'quick']
        keyword_matches = sum(1 for kw in perf_keywords if kw in req_lower)

        recommended = []
        opposed = []
        reasoning_parts = []

        # Caching for read-heavy workloads
        if any(kw in req_lower for kw in ['read', 'query', 'fetch', 'get', 'load']):
            recommended.append('cache_aside')
            reasoning_parts.append("Caching can reduce latency by 10-100x for repeated reads")

        # Parallel processing for throughput
        if any(kw in req_lower for kw in ['throughput', 'batch', 'process', 'pipeline']):
            recommended.extend(['fan_out_fan_in', 'competing_consumers'])
            reasoning_parts.append("Parallel processing maximizes throughput")

        # Connection pooling always helps
        if any(kw in req_lower for kw in ['database', 'db', 'connection', 'service']):
            recommended.append('connection_pool')
            reasoning_parts.append("Connection pooling eliminates connection setup overhead")

        # Rate limiting for protection
        if any(kw in req_lower for kw in ['api', 'public', 'external']):
            recommended.append('rate_limiter')
            reasoning_parts.append("Rate limiting protects performance under load")

        # Strict latency requirements
        if context.constraints.get('max_latency_ms', 1000) <= 100:
            opposed.extend(['saga', 'two_phase_commit'])
            reasoning_parts.append("Distributed transactions add unacceptable latency")

        if not recommended:
            recommended = ['cache_aside', 'connection_pool']
            reasoning_parts.append("Basic performance patterns improve response times")

        strength = ArgumentStrength.STRONG if keyword_matches >= 2 else ArgumentStrength.MODERATE

        return Argument(
            advocate=self.name,
            phase=DebatePhase.OPENING,
            claim=f"Performance is critical for user experience. I propose: {', '.join(recommended[:4])}",
            reasoning=" ".join(reasoning_parts),
            evidence=[
                "Google found that 400ms delay reduces searches by 0.6%",
                "Amazon found that 100ms latency costs 1% in sales",
                "53% of mobile users abandon sites that take >3 seconds to load"
            ],
            patterns_proposed=recommended[:4],
            patterns_opposed=opposed,
            strength=strength,
            confidence=0.82 + (keyword_matches * 0.03)
        )

    def make_rebuttal(self, context: DebateContext,
                      opponent_args: List[Argument]) -> Argument:
        rebuttals = []
        patterns_to_oppose = []

        for arg in opponent_args:
            if arg.advocate == self.name:
                continue

            # Challenge reliability patterns that hurt performance
            if arg.advocate == "reliability_advocate":
                for pattern in arg.patterns_proposed:
                    if pattern in self.latency_concerns:
                        patterns_to_oppose.append(pattern)
                        if pattern == 'saga':
                            rebuttals.append(
                                "Saga pattern requires multiple network round-trips, "
                                "adding 50-200ms per transaction step."
                            )
                        elif pattern == 'circuit_breaker':
                            rebuttals.append(
                                "Circuit breaker adds state checking overhead on every request. "
                                "Consider implementing it at the infrastructure level instead."
                            )

            # Challenge complex patterns
            if arg.advocate == "security_advocate":
                for pattern in arg.patterns_proposed:
                    if pattern in ['service_mesh']:
                        patterns_to_oppose.append(pattern)
                        rebuttals.append(
                            "Service mesh adds a proxy hop, increasing latency by 1-5ms per call."
                        )

        rebuttal_text = " ".join(rebuttals) if rebuttals else \
            "We must carefully consider the performance impact of every pattern."

        return Argument(
            advocate=self.name,
            phase=DebatePhase.REBUTTAL,
            claim="Every millisecond matters. High-latency patterns must be justified.",
            reasoning=rebuttal_text,
            evidence=[
                "P99 latency directly impacts user satisfaction",
                "Each additional network hop adds 0.5-5ms latency"
            ],
            patterns_proposed=self.preferred_patterns[:2],
            patterns_opposed=list(set(patterns_to_oppose)),
            strength=ArgumentStrength.MODERATE,
            rebuts=opponent_args[0].id if opponent_args else None,
            confidence=0.75
        )


class SimplicityAdvocate(AdvocateAgent):
    """Advocates for simplicity, maintainability, and reducing complexity."""

    def __init__(self, kb: PatternKnowledgeBase):
        super().__init__(kb)
        self.name = "simplicity_advocate"
        self.concern = "simplicity"
        self.icon = "🎯"

        self.simple_patterns = [
            'timeout', 'fallback', 'health_check', 'structured_logging',
            'retry_backoff', 'cache_aside', 'rate_limiter', 'feature_flag'
        ]

        self.complex_patterns = [
            'saga', 'event_sourcing', 'cqrs', 'service_mesh', 'two_phase_commit',
            'choreography', 'work_stealing'
        ]

    def evaluate_pattern(self, pattern: Pattern, context: DebateContext) -> Tuple[float, str]:
        score = 0.5

        if pattern.id in self.simple_patterns:
            score += 0.3
        if pattern.id in self.complex_patterns:
            score -= 0.3

        if pattern.ontology:
            complexity = pattern.ontology.complexity
            # Prefer low complexity
            score += (10 - complexity) * 0.05

            # Penalize experimental patterns
            if pattern.ontology.maturity == 'experimental':
                score -= 0.2
            elif pattern.ontology.maturity == 'stable':
                score += 0.1

        # Adjust for team/project constraints
        if context.max_complexity < 5:
            if pattern.id in self.complex_patterns:
                score -= 0.3

        reasoning = self._generate_reasoning(pattern, score)
        return max(0, min(1.0, score)), reasoning

    def _generate_reasoning(self, pattern: Pattern, score: float) -> str:
        if score > 0.7:
            return f"{pattern.name} is straightforward to implement and maintain."
        elif score < 0.4:
            return f"{pattern.name} adds significant complexity that may not be justified."
        else:
            return f"{pattern.name} has moderate complexity that should be carefully considered."

    def make_opening(self, context: DebateContext) -> Argument:
        req_lower = context.requirement.lower()

        recommended = []
        opposed = []
        reasoning_parts = []

        # Always recommend simple, effective patterns
        recommended.extend(['timeout', 'health_check', 'structured_logging'])
        reasoning_parts.append("Start with simple patterns that provide high value with low complexity")

        # Oppose overly complex patterns unless clearly needed
        if 'saga' not in req_lower and 'distributed transaction' not in req_lower:
            opposed.append('saga')
            reasoning_parts.append("Saga pattern is often over-engineered for simpler use cases")

        if 'event sourcing' not in req_lower:
            opposed.append('event_sourcing')
            reasoning_parts.append("Event sourcing adds significant complexity")

        if 'cqrs' not in req_lower:
            opposed.append('cqrs')
            reasoning_parts.append("CQRS is rarely needed and often premature")

        # Check complexity budget
        if context.max_complexity <= 5:
            opposed.extend(['service_mesh', 'two_phase_commit'])
            reasoning_parts.append("Given complexity constraints, avoid infrastructure-heavy patterns")

        return Argument(
            advocate=self.name,
            phase=DebatePhase.OPENING,
            claim=f"Simplicity enables maintainability. Start simple: {', '.join(recommended[:3])}",
            reasoning=" ".join(reasoning_parts),
            evidence=[
                "Complex systems fail in complex ways that are hard to debug",
                "Every pattern added increases cognitive load for the team",
                "YAGNI - You Aren't Gonna Need It (most of the time)"
            ],
            patterns_proposed=recommended[:3],
            patterns_opposed=opposed[:3],
            strength=ArgumentStrength.MODERATE,
            confidence=0.78
        )

    def make_rebuttal(self, context: DebateContext,
                      opponent_args: List[Argument]) -> Argument:
        rebuttals = []
        patterns_to_oppose = []

        total_patterns_proposed = set()
        for arg in opponent_args:
            if arg.advocate != self.name:
                total_patterns_proposed.update(arg.patterns_proposed)

        # Calculate total complexity
        total_complexity = 0
        for pid in total_patterns_proposed:
            if pid in self.kb.graph.nodes:
                pattern = self.kb.graph.nodes[pid]
                if pattern.ontology:
                    total_complexity += pattern.ontology.complexity

        if total_complexity > context.max_complexity:
            rebuttals.append(
                f"The combined patterns propose complexity of {total_complexity}, "
                f"exceeding our budget of {context.max_complexity}. We must prioritize."
            )

            # Identify most complex patterns to oppose
            complex_proposed = [p for p in total_patterns_proposed if p in self.complex_patterns]
            patterns_to_oppose.extend(complex_proposed[:2])

        if len(total_patterns_proposed) > 6:
            rebuttals.append(
                f"We're discussing {len(total_patterns_proposed)} patterns. "
                "This is a recipe for an unmaintainable system. Let's focus on the essentials."
            )

        for arg in opponent_args:
            for pattern in arg.patterns_proposed:
                if pattern in ['saga', 'event_sourcing', 'cqrs']:
                    if pattern not in context.requirement.lower():
                        patterns_to_oppose.append(pattern)
                        rebuttals.append(
                            f"{pattern} was not mentioned in requirements. "
                            "Are we solving a real problem or an imagined one?"
                        )
                        break

        rebuttal_text = " ".join(rebuttals) if rebuttals else \
            "Let's not over-engineer. Simple solutions often work best."

        return Argument(
            advocate=self.name,
            phase=DebatePhase.REBUTTAL,
            claim="Complexity is the enemy. Each pattern must justify its existence.",
            reasoning=rebuttal_text,
            evidence=[
                "80% of bugs come from 20% of the most complex code",
                "Simple systems are easier to operate and debug in production"
            ],
            patterns_proposed=['timeout', 'fallback'],
            patterns_opposed=list(set(patterns_to_oppose))[:3],
            strength=ArgumentStrength.MODERATE,
            rebuts=opponent_args[0].id if opponent_args else None,
            confidence=0.75
        )


class SecurityAdvocate(AdvocateAgent):
    """Advocates for security, compliance, and protection."""

    def __init__(self, kb: PatternKnowledgeBase):
        super().__init__(kb)
        self.name = "security_advocate"
        self.concern = "security"
        self.icon = "🔒"

        self.preferred_patterns = [
            'api_gateway', 'rate_limiter', 'audit_trail', 'service_mesh',
            'idempotent_receiver', 'approval_gate', 'token_bucket'
        ]

    def evaluate_pattern(self, pattern: Pattern, context: DebateContext) -> Tuple[float, str]:
        score = 0.5

        if pattern.id in self.preferred_patterns:
            score += 0.3

        if pattern.ontology:
            if pattern.ontology.domain == PatternDomain.SECURITY:
                score += 0.3
            if pattern.ontology.domain == PatternDomain.GOVERNANCE:
                score += 0.2
            if PatternIntent.COMPLIANCE in pattern.ontology.intents:
                score += 0.2
            if PatternIntent.ISOLATION in pattern.ontology.intents:
                score += 0.15

        # Check for compliance requirements
        req_lower = context.requirement.lower()
        if any(kw in req_lower for kw in ['compliance', 'audit', 'soc2', 'pci', 'hipaa', 'gdpr']):
            if pattern.id in ['audit_trail', 'approval_gate']:
                score += 0.2

        reasoning = self._generate_reasoning(pattern, score)
        return min(1.0, score), reasoning

    def _generate_reasoning(self, pattern: Pattern, score: float) -> str:
        if score > 0.7:
            return f"{pattern.name} provides essential security controls and audit capabilities."
        elif score < 0.4:
            return f"{pattern.name} has limited security relevance."
        else:
            return f"{pattern.name} has indirect security benefits through isolation or control."

    def make_opening(self, context: DebateContext) -> Argument:
        req_lower = context.requirement.lower()
        security_keywords = ['security', 'auth', 'compliance', 'audit', 'protect',
                             'soc2', 'pci', 'hipaa', 'gdpr', 'encrypt']
        keyword_matches = sum(1 for kw in security_keywords if kw in req_lower)

        recommended = []
        reasoning_parts = []

        # API protection
        if any(kw in req_lower for kw in ['api', 'public', 'external', 'client']):
            recommended.extend(['api_gateway', 'rate_limiter'])
            reasoning_parts.append("Public APIs require gateway protection and rate limiting")

        # Audit requirements
        if any(kw in req_lower for kw in ['audit', 'compliance', 'soc2', 'pci', 'hipaa']):
            recommended.extend(['audit_trail', 'approval_gate'])
            reasoning_parts.append("Compliance requires comprehensive audit trails")

        # Service-to-service security
        if any(kw in req_lower for kw in ['service', 'microservice', 'internal']):
            recommended.append('service_mesh')
            reasoning_parts.append("Service mesh provides mTLS and fine-grained access control")

        # Always recommend some security baseline
        if not recommended:
            recommended = ['rate_limiter', 'audit_trail']
            reasoning_parts.append("Basic security controls should always be present")

        strength = ArgumentStrength.STRONG if keyword_matches >= 2 else ArgumentStrength.MODERATE

        return Argument(
            advocate=self.name,
            phase=DebatePhase.OPENING,
            claim=f"Security is non-negotiable. I propose: {', '.join(recommended[:4])}",
            reasoning=" ".join(reasoning_parts),
            evidence=[
                "Average cost of a data breach is $4.35 million",
                "Compliance violations can result in fines up to 4% of revenue",
                "Rate limiting prevents 95% of DDoS attacks"
            ],
            patterns_proposed=recommended[:4],
            patterns_opposed=[],
            strength=strength,
            confidence=0.83 + (keyword_matches * 0.03)
        )

    def make_rebuttal(self, context: DebateContext,
                      opponent_args: List[Argument]) -> Argument:
        rebuttals = []
        patterns_to_defend = []

        for arg in opponent_args:
            if arg.advocate == self.name:
                continue

            # Defend against performance concerns about security patterns
            if arg.advocate == "performance_advocate":
                for pattern in arg.patterns_opposed:
                    if pattern in self.preferred_patterns:
                        patterns_to_defend.append(pattern)
                        rebuttals.append(
                            f"The small latency cost of {pattern} is insignificant compared to "
                            f"the cost of a security breach or compliance violation."
                        )

            # Defend against simplicity concerns
            if arg.advocate == "simplicity_advocate":
                for pattern in arg.patterns_opposed:
                    if pattern in ['service_mesh', 'api_gateway']:
                        patterns_to_defend.append(pattern)
                        rebuttals.append(
                            f"{pattern} may add complexity, but it's the industry standard "
                            "for securing distributed systems. The alternative is ad-hoc security."
                        )

        rebuttal_text = " ".join(rebuttals) if rebuttals else \
            "Security must be built in from the start, not bolted on later."

        return Argument(
            advocate=self.name,
            phase=DebatePhase.REBUTTAL,
            claim="Security trade-offs must favor protection. Breaches are catastrophic.",
            reasoning=rebuttal_text,
            evidence=[
                "60% of small businesses close within 6 months of a breach",
                "Regulatory fines for non-compliance are increasing exponentially"
            ],
            patterns_proposed=list(set(patterns_to_defend)),
            patterns_opposed=[],
            strength=ArgumentStrength.MODERATE,
            rebuts=opponent_args[0].id if opponent_args else None,
            confidence=0.8
        )


class CostAdvocate(AdvocateAgent):
    """Advocates for cost efficiency and operational sustainability."""

    def __init__(self, kb: PatternKnowledgeBase):
        super().__init__(kb)
        self.name = "cost_advocate"
        self.concern = "cost efficiency"
        self.icon = "💰"

        self.cost_effective_patterns = [
            'cache_aside', 'rate_limiter', 'connection_pool', 'feature_flag',
            'timeout', 'structured_logging', 'health_check'
        ]

        self.expensive_patterns = [
            'service_mesh', 'event_sourcing', 'distributed_tracing',
            'saga', 'cqrs', 'two_phase_commit'
        ]

    def evaluate_pattern(self, pattern: Pattern, context: DebateContext) -> Tuple[float, str]:
        score = 0.5

        if pattern.id in self.cost_effective_patterns:
            score += 0.3
        if pattern.id in self.expensive_patterns:
            score -= 0.25

        if pattern.ontology:
            # Lower complexity = lower cost
            complexity = pattern.ontology.complexity
            score += (10 - complexity) * 0.03

        # Budget tier adjustments
        if context.budget_tier == 'low':
            if pattern.id in self.expensive_patterns:
                score -= 0.3
        elif context.budget_tier == 'unlimited':
            score += 0.2  # Less concern about cost

        reasoning = self._generate_reasoning(pattern, score, context)
        return max(0, min(1.0, score)), reasoning

    def _generate_reasoning(self, pattern: Pattern, score: float,
                            context: DebateContext) -> str:
        if score > 0.7:
            return f"{pattern.name} provides good value with low operational overhead."
        elif score < 0.4:
            return f"{pattern.name} has high operational costs that may not be justified."
        else:
            return f"{pattern.name} has moderate cost implications."

    def make_opening(self, context: DebateContext) -> Argument:
        req_lower = context.requirement.lower()

        recommended = []
        opposed = []
        reasoning_parts = []

        # Always recommend cost-effective basics
        recommended.extend(['cache_aside', 'connection_pool', 'timeout'])
        reasoning_parts.append("These patterns reduce resource usage and cloud costs")

        if context.budget_tier in ['low', 'moderate']:
            opposed.extend(['service_mesh', 'event_sourcing'])
            reasoning_parts.append("Expensive infrastructure patterns should be deferred")

        # Rate limiting saves costs
        if any(kw in req_lower for kw in ['api', 'cloud', 'scale']):
            recommended.append('rate_limiter')
            reasoning_parts.append("Rate limiting prevents runaway costs from traffic spikes")

        return Argument(
            advocate=self.name,
            phase=DebatePhase.OPENING,
            claim=f"Cost efficiency ensures sustainability. I propose: {', '.join(recommended[:4])}",
            reasoning=" ".join(reasoning_parts),
            evidence=[
                "Cloud costs are the #2 concern for engineering teams",
                "Caching can reduce database costs by 80%",
                "Connection pooling reduces compute requirements by 30%"
            ],
            patterns_proposed=recommended[:4],
            patterns_opposed=opposed[:2],
            strength=ArgumentStrength.MODERATE,
            confidence=0.75
        )

    def make_rebuttal(self, context: DebateContext,
                      opponent_args: List[Argument]) -> Argument:
        rebuttals = []
        patterns_to_oppose = []

        for arg in opponent_args:
            if arg.advocate == self.name:
                continue

            for pattern in arg.patterns_proposed:
                if pattern in self.expensive_patterns:
                    patterns_to_oppose.append(pattern)
                    if pattern == 'service_mesh':
                        rebuttals.append(
                            "Service mesh adds significant infrastructure cost. "
                            "Consider if simpler alternatives can meet the requirement."
                        )
                    elif pattern == 'event_sourcing':
                        rebuttals.append(
                            "Event sourcing requires significant storage and "
                            "specialized expertise. Is the investment justified?"
                        )

        rebuttal_text = " ".join(rebuttals) if rebuttals else \
            "We must consider the long-term operational costs of each pattern."

        return Argument(
            advocate=self.name,
            phase=DebatePhase.REBUTTAL,
            claim="We must balance capability with cost sustainability.",
            reasoning=rebuttal_text,
            evidence=[
                "Technical debt includes operational cost debt",
                "Over-engineering leads to unsustainable cloud bills"
            ],
            patterns_proposed=['cache_aside', 'rate_limiter'],
            patterns_opposed=list(set(patterns_to_oppose))[:3],
            strength=ArgumentStrength.MODERATE,
            rebuts=opponent_args[0].id if opponent_args else None,
            confidence=0.7
        )


# =============================================================================
# ARBITRATOR
# =============================================================================

class Arbitrator:
    """
    Synthesizes debate arguments into a final recommendation.

    Uses weighted voting, consensus detection, and trade-off analysis.
    """

    def __init__(self, kb: PatternKnowledgeBase):
        self.kb = kb
        self.causal = CausalReasoningEngine(kb.graph)

    def deliberate(self, context: DebateContext,
                   transcript: List[Argument]) -> DebateResult:
        """Deliberate on all arguments and produce final result."""

        # Collect all pattern votes
        pattern_votes: Dict[str, List[Tuple[str, float, str]]] = defaultdict(list)
        pattern_oppositions: Dict[str, List[Tuple[str, float, str]]] = defaultdict(list)

        for arg in transcript:
            weight = context.priority_weights.get(arg.advocate, 1.0)
            strength_multiplier = arg.strength.value / 5.0
            score = weight * strength_multiplier * arg.confidence

            for pattern in arg.patterns_proposed:
                pattern_votes[pattern].append((arg.advocate, score, arg.reasoning))

            for pattern in arg.patterns_opposed:
                pattern_oppositions[pattern].append((arg.advocate, score, arg.reasoning))

        # Calculate net scores
        pattern_scores = {}
        for pattern in set(pattern_votes.keys()) | set(pattern_oppositions.keys()):
            support = sum(v[1] for v in pattern_votes.get(pattern, []))
            opposition = sum(v[1] for v in pattern_oppositions.get(pattern, []))
            net_score = support - opposition * 0.7  # Opposition weighted slightly less
            pattern_scores[pattern] = net_score

        # Rank patterns
        ranked_patterns = sorted(pattern_scores.items(), key=lambda x: -x[1])

        # Generate recommendations with rationale
        recommendations = []
        for pattern_id, score in ranked_patterns[:5]:
            if score > 0:  # Only recommend patterns with positive net score
                supporters = [v[0] for v in pattern_votes.get(pattern_id, [])]
                opposers = [v[0] for v in pattern_oppositions.get(pattern_id, [])]

                rationale = f"Supported by: {', '.join(supporters)}"
                if opposers:
                    rationale += f" (opposed by: {', '.join(opposers)})"

                recommendations.append((pattern_id, score, rationale))

        # Calculate consensus level
        consensus = self._calculate_consensus(transcript)

        # Identify key trade-offs
        tradeoffs = self._identify_tradeoffs(transcript)

        # Collect dissenting opinions
        dissenting = self._collect_dissents(ranked_patterns, transcript)

        # Generate deliberation notes
        notes = self._generate_deliberation_notes(
            context, recommendations, transcript, consensus
        )

        # Overall confidence
        confidence = min(0.95, 0.5 + consensus * 0.3 + len(recommendations) * 0.05)

        return DebateResult(
            recommended_patterns=recommendations,
            consensus_level=consensus,
            key_tradeoffs=tradeoffs,
            dissenting_opinions=dissenting,
            transcript=transcript,
            deliberation_notes=notes,
            confidence=confidence
        )

    def _calculate_consensus(self, transcript: List[Argument]) -> float:
        """Calculate how much advocates agreed."""
        # Find patterns proposed by multiple advocates
        pattern_proposers: Dict[str, set] = defaultdict(set)
        for arg in transcript:
            for pattern in arg.patterns_proposed:
                pattern_proposers[pattern].add(arg.advocate)

        if not pattern_proposers:
            return 0.0

        # Count patterns with multi-advocate support
        multi_support = sum(1 for p, advocates in pattern_proposers.items()
                           if len(advocates) >= 2)

        # Count patterns with unanimous support (from opening statements)
        advocates_count = len(set(arg.advocate for arg in transcript))
        unanimous = sum(1 for p, advocates in pattern_proposers.items()
                       if len(advocates) == advocates_count)

        total_patterns = len(pattern_proposers)
        consensus = (multi_support * 0.5 + unanimous * 1.0) / total_patterns

        return min(1.0, consensus)

    def _identify_tradeoffs(self, transcript: List[Argument]) -> List[Dict[str, str]]:
        """Identify key trade-offs from the debate."""
        tradeoffs = []

        # Find patterns that were both supported and opposed
        pattern_positions: Dict[str, Dict[str, List[str]]] = defaultdict(
            lambda: {'support': [], 'oppose': []}
        )

        for arg in transcript:
            for pattern in arg.patterns_proposed:
                pattern_positions[pattern]['support'].append(arg.advocate)
            for pattern in arg.patterns_opposed:
                pattern_positions[pattern]['oppose'].append(arg.advocate)

        for pattern, positions in pattern_positions.items():
            if positions['support'] and positions['oppose']:
                tradeoffs.append({
                    'pattern': pattern,
                    'supporters': ', '.join(positions['support']),
                    'opposers': ', '.join(positions['oppose']),
                    'tradeoff': f"{pattern} improves {positions['support'][0].replace('_advocate', '')} "
                               f"but concerns {positions['oppose'][0].replace('_advocate', '')}"
                })

        return tradeoffs[:5]

    def _collect_dissents(self, ranked_patterns: List[Tuple[str, float]],
                          transcript: List[Argument]) -> List[Dict[str, str]]:
        """Collect dissenting opinions on top recommendations."""
        dissents = []
        top_patterns = [p[0] for p in ranked_patterns[:3]]

        for arg in transcript:
            if arg.phase == DebatePhase.REBUTTAL:
                for pattern in arg.patterns_opposed:
                    if pattern in top_patterns:
                        dissents.append({
                            'advocate': arg.advocate,
                            'pattern': pattern,
                            'concern': arg.reasoning[:200]
                        })

        return dissents[:5]

    def _generate_deliberation_notes(self, context: DebateContext,
                                      recommendations: List[Tuple[str, float, str]],
                                      transcript: List[Argument],
                                      consensus: float) -> str:
        """Generate human-readable deliberation notes."""
        notes = []

        notes.append(f"REQUIREMENT: {context.requirement[:100]}...")
        notes.append("")

        if context.constraints:
            constraints_str = ', '.join(f"{k}={v}" for k, v in context.constraints.items())
            notes.append(f"CONSTRAINTS: {constraints_str}")
            notes.append("")

        notes.append(f"CONSENSUS LEVEL: {consensus:.0%}")
        if consensus > 0.7:
            notes.append("Strong agreement among advocates.")
        elif consensus > 0.4:
            notes.append("Moderate agreement with some divergent views.")
        else:
            notes.append("Significant disagreement - trade-offs are substantial.")
        notes.append("")

        notes.append("FINAL RECOMMENDATIONS:")
        for i, (pattern, score, rationale) in enumerate(recommendations[:5], 1):
            pattern_obj = self.kb.graph.nodes.get(pattern)
            name = pattern_obj.name if pattern_obj else pattern
            notes.append(f"  {i}. {name} (score: {score:.2f})")
            notes.append(f"     {rationale}")
        notes.append("")

        # Key arguments
        notes.append("KEY ARGUMENTS:")
        for arg in transcript:
            if arg.phase in [DebatePhase.OPENING, DebatePhase.REBUTTAL]:
                notes.append(f"  [{arg.advocate}] {arg.claim[:80]}...")

        return "\n".join(notes)


# =============================================================================
# DEBATE ORCHESTRATOR
# =============================================================================

class PatternDebate:
    """
    Orchestrates the multi-agent pattern debate.
    """

    def __init__(self):
        self.kb = PatternKnowledgeBase()
        self.arbitrator = Arbitrator(self.kb)

        # Initialize advocates
        self.advocates = {
            'reliability': ReliabilityAdvocate(self.kb),
            'performance': PerformanceAdvocate(self.kb),
            'simplicity': SimplicityAdvocate(self.kb),
            'security': SecurityAdvocate(self.kb),
            'cost': CostAdvocate(self.kb)
        }

    def debate(self, requirement: str,
               constraints: Optional[Dict[str, Any]] = None,
               existing_patterns: Optional[List[str]] = None,
               priority_weights: Optional[Dict[str, float]] = None,
               rounds: int = 2,
               verbose: bool = True) -> DebateResult:
        """
        Run a full pattern debate.

        Args:
            requirement: The system requirement to debate
            constraints: Constraints like availability, latency, etc.
            existing_patterns: Patterns already in use
            priority_weights: How to weight each advocate's opinion
            rounds: Number of rebuttal rounds (1-3)
            verbose: Print debate as it happens

        Returns:
            DebateResult with recommendations and transcript
        """
        context = DebateContext(
            requirement=requirement,
            constraints=constraints or {},
            existing_patterns=existing_patterns or [],
            priority_weights=priority_weights or {
                'reliability_advocate': 1.0,
                'performance_advocate': 1.0,
                'simplicity_advocate': 0.8,
                'security_advocate': 1.0,
                'cost_advocate': 0.7
            },
            max_complexity=constraints.get('max_complexity', 10) if constraints else 10,
            budget_tier=constraints.get('budget_tier', 'moderate') if constraints else 'moderate'
        )

        transcript = []

        if verbose:
            print("\n" + "=" * 70)
            print("PATTERN DEBATE")
            print("=" * 70)
            print(f"\nRequirement: {requirement[:80]}...")
            if constraints:
                print(f"Constraints: {constraints}")
            print("\n" + "-" * 70)
            print("PHASE 1: OPENING STATEMENTS")
            print("-" * 70)

        # Phase 1: Opening statements
        for name, advocate in self.advocates.items():
            opening = advocate.make_opening(context)
            transcript.append(opening)

            if verbose:
                print(f"\n{advocate.icon} {advocate.name.replace('_', ' ').title()}")
                print(f"   Claim: {opening.claim}")
                print(f"   Proposes: {', '.join(opening.patterns_proposed[:3])}")
                if opening.patterns_opposed:
                    print(f"   Opposes: {', '.join(opening.patterns_opposed[:2])}")

        # Phase 2-N: Rebuttals
        for round_num in range(rounds):
            if verbose:
                print("\n" + "-" * 70)
                print(f"PHASE {round_num + 2}: REBUTTAL ROUND {round_num + 1}")
                print("-" * 70)

            round_args = []
            for name, advocate in self.advocates.items():
                # Get arguments from other advocates
                opponent_args = [a for a in transcript if a.advocate != advocate.name]
                rebuttal = advocate.make_rebuttal(context, opponent_args[-4:])
                round_args.append(rebuttal)

                if verbose:
                    print(f"\n{advocate.icon} {advocate.name.replace('_', ' ').title()}")
                    print(f"   Claim: {rebuttal.claim}")
                    if rebuttal.patterns_opposed:
                        print(f"   Challenges: {', '.join(rebuttal.patterns_opposed[:2])}")

            transcript.extend(round_args)

        # Phase N+1: Closing statements
        if verbose:
            print("\n" + "-" * 70)
            print("FINAL PHASE: CLOSING STATEMENTS")
            print("-" * 70)

        for name, advocate in self.advocates.items():
            closing = advocate.make_closing(context, transcript)
            transcript.append(closing)

            if verbose:
                print(f"\n{advocate.icon} {advocate.name.replace('_', ' ').title()}")
                print(f"   Final position: {closing.claim}")

        # Deliberation
        if verbose:
            print("\n" + "=" * 70)
            print("ARBITRATOR DELIBERATION")
            print("=" * 70)

        result = self.arbitrator.deliberate(context, transcript)

        if verbose:
            print(f"\nConsensus Level: {result.consensus_level:.0%}")
            print(f"Confidence: {result.confidence:.0%}")
            print("\nFinal Recommendations:")
            for i, (pattern, score, rationale) in enumerate(result.recommended_patterns[:5], 1):
                pattern_obj = self.kb.graph.nodes.get(pattern)
                name = pattern_obj.name if pattern_obj else pattern
                print(f"  {i}. [{score:.2f}] {name}")
                print(f"      {rationale}")

            if result.key_tradeoffs:
                print("\nKey Trade-offs:")
                for t in result.key_tradeoffs[:3]:
                    print(f"  - {t['tradeoff']}")

            if result.dissenting_opinions:
                print("\nDissenting Opinions:")
                for d in result.dissenting_opinions[:2]:
                    print(f"  - {d['advocate']}: {d['concern'][:60]}...")

        return result

    def quick_debate(self, requirement: str) -> DebateResult:
        """Run a quick single-round debate."""
        return self.debate(requirement, rounds=1, verbose=False)

    def deep_debate(self, requirement: str,
                    constraints: Dict[str, Any]) -> DebateResult:
        """Run a deep multi-round debate with constraints."""
        return self.debate(
            requirement,
            constraints=constraints,
            rounds=3,
            verbose=True
        )


# =============================================================================
# LLM-POWERED DEBATE (Advanced Mode)
# =============================================================================

class LLMDebateAgent:
    """
    An LLM-powered advocate that uses actual AI reasoning.
    Falls back to rule-based if API unavailable.
    """

    ADVOCATE_PROMPTS = {
        'reliability': """You are the RELIABILITY ADVOCATE in an architecture debate.
Your core belief: System reliability and fault tolerance are paramount.
You champion: circuit breakers, retries, sagas, bulkheads, fallbacks.
You challenge: patterns that sacrifice reliability for speed or simplicity.
Argue passionately but fairly. Use evidence and real-world failure scenarios.""",

        'performance': """You are the PERFORMANCE ADVOCATE in an architecture debate.
Your core belief: User experience depends on speed and throughput.
You champion: caching, connection pooling, parallel processing, rate limiting.
You challenge: patterns that add latency or overhead without clear justification.
Argue with data about latency impact and user experience metrics.""",

        'simplicity': """You are the SIMPLICITY ADVOCATE in an architecture debate.
Your core belief: Complexity is the enemy of maintainability and correctness.
You champion: simple, well-understood patterns with low cognitive load.
You challenge: over-engineering, premature optimization, pattern soup.
Argue for YAGNI, iterative development, and sustainable complexity.""",

        'security': """You are the SECURITY ADVOCATE in an architecture debate.
Your core belief: Security breaches are catastrophic and prevention is essential.
You champion: API gateways, rate limiting, audit trails, service mesh.
You challenge: patterns that create security vulnerabilities or compliance gaps.
Argue with breach statistics and regulatory requirements.""",

        'cost': """You are the COST ADVOCATE in an architecture debate.
Your core belief: Sustainable systems require cost-efficient architecture.
You champion: patterns that reduce cloud costs and operational overhead.
You challenge: expensive infrastructure that may not be justified.
Argue for ROI and total cost of ownership."""
    }

    def __init__(self, role: str, kb: PatternKnowledgeBase):
        self.role = role
        self.kb = kb
        self.prompt = self.ADVOCATE_PROMPTS.get(role, "You are an architecture advocate.")
        self._api_available = self._check_api()

    def _check_api(self) -> bool:
        """Check if OpenRouter API is available."""
        import os
        return bool(os.environ.get('OPENROUTER_API_KEY'))

    def generate_argument(self, context: str, phase: str,
                          opponent_args: List[str] = None) -> str:
        """Generate an argument using LLM or fallback to template."""
        if not self._api_available:
            return self._template_argument(context, phase, opponent_args)

        try:
            return self._llm_argument(context, phase, opponent_args)
        except Exception as e:
            return self._template_argument(context, phase, opponent_args)

    def _llm_argument(self, context: str, phase: str,
                       opponent_args: List[str] = None) -> str:
        """Generate argument using LLM."""
        import os
        import urllib.request

        messages = [
            {"role": "system", "content": self.prompt},
            {"role": "user", "content": f"""
DEBATE CONTEXT:
{context}

PHASE: {phase}

{"OPPONENT ARGUMENTS TO ADDRESS:" + chr(10) + chr(10).join(opponent_args or []) if opponent_args else ""}

Generate a concise, compelling {phase} argument (2-3 sentences for the main claim, then bullet points for specific pattern recommendations).
Format:
CLAIM: [Your main argument]
PROPOSE: [pattern1, pattern2, ...]
OPPOSE: [pattern1, pattern2, ...] (if any)
REASONING: [Brief justification]
"""}
        ]

        request_body = {
            "model": "anthropic/claude-3-haiku",
            "messages": messages,
            "max_tokens": 300,
            "temperature": 0.7
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {os.environ.get('OPENROUTER_API_KEY')}",
        }

        req = urllib.request.Request(
            "https://openrouter.ai/api/v1/chat/completions",
            data=json.dumps(request_body).encode(),
            headers=headers,
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode())
            return result['choices'][0]['message']['content']

    def _template_argument(self, context: str, phase: str,
                           opponent_args: List[str] = None) -> str:
        """Fallback template-based argument."""
        templates = {
            'reliability': {
                'opening': "CLAIM: Without reliability patterns, we risk catastrophic failures.\nPROPOSE: circuit_breaker, retry_backoff, bulkhead\nREASONING: These patterns prevent cascade failures and ensure graceful degradation.",
                'rebuttal': "CLAIM: Performance and cost concerns pale compared to outage costs.\nPROPOSE: saga, fallback\nREASONING: Amazon loses $220K per minute of downtime."
            },
            'performance': {
                'opening': "CLAIM: User experience depends on sub-100ms response times.\nPROPOSE: cache_aside, connection_pool, rate_limiter\nOPPOSE: saga, two_phase_commit\nREASONING: Every ms of latency impacts conversion.",
                'rebuttal': "CLAIM: Reliability patterns that add 50ms+ latency need justification.\nOPPOSE: saga, distributed_tracing\nREASONING: Consider infrastructure-level alternatives."
            },
            'simplicity': {
                'opening': "CLAIM: Complexity kills maintainability. Start simple.\nPROPOSE: timeout, health_check, structured_logging\nOPPOSE: saga, event_sourcing, cqrs\nREASONING: YAGNI - add complexity only when proven necessary.",
                'rebuttal': "CLAIM: We're proposing too many patterns. Simplify.\nPROPOSE: fallback, timeout\nREASONING: Each pattern adds cognitive load."
            },
            'security': {
                'opening': "CLAIM: Security is non-negotiable in production systems.\nPROPOSE: api_gateway, rate_limiter, audit_trail\nREASONING: Breaches cost $4.35M on average.",
                'rebuttal': "CLAIM: Security overhead is insignificant compared to breach costs.\nPROPOSE: service_mesh\nREASONING: mTLS and fine-grained access control are essential."
            },
            'cost': {
                'opening': "CLAIM: Sustainable architecture requires cost discipline.\nPROPOSE: cache_aside, connection_pool\nOPPOSE: service_mesh, event_sourcing\nREASONING: Reduce cloud costs, avoid over-engineering.",
                'rebuttal': "CLAIM: Expensive patterns need clear ROI justification.\nOPPOSE: service_mesh, saga\nREASONING: Consider simpler alternatives first."
            }
        }

        role_templates = templates.get(self.role, templates['reliability'])
        return role_templates.get(phase, role_templates['opening'])


class LLMPoweredDebate(PatternDebate):
    """
    Extended debate system that can use LLM-powered advocates
    for more sophisticated, natural-language arguments.
    """

    def __init__(self, use_llm: bool = True):
        super().__init__()
        self.use_llm = use_llm

        if use_llm:
            self.llm_agents = {
                'reliability': LLMDebateAgent('reliability', self.kb),
                'performance': LLMDebateAgent('performance', self.kb),
                'simplicity': LLMDebateAgent('simplicity', self.kb),
                'security': LLMDebateAgent('security', self.kb),
                'cost': LLMDebateAgent('cost', self.kb)
            }

    def llm_debate(self, requirement: str,
                   constraints: Optional[Dict] = None,
                   verbose: bool = True) -> Dict:
        """
        Run an LLM-powered debate with natural language arguments.
        """
        context = f"""
REQUIREMENT: {requirement}

CONSTRAINTS: {json.dumps(constraints or {}, indent=2)}

AVAILABLE PATTERNS:
{', '.join(list(self.kb.graph.nodes.keys())[:20])}
"""
        transcript = []

        if verbose:
            print("\n" + "=" * 70)
            print("LLM-POWERED PATTERN DEBATE")
            print("=" * 70)
            print(f"\nRequirement: {requirement[:100]}...")

        # Opening statements
        if verbose:
            print("\n" + "-" * 70)
            print("OPENING STATEMENTS")
            print("-" * 70)

        for role, agent in self.llm_agents.items():
            argument = agent.generate_argument(context, 'opening')
            transcript.append({
                'role': role,
                'phase': 'opening',
                'argument': argument
            })
            if verbose:
                icon = {'reliability': '🛡️', 'performance': '⚡',
                       'simplicity': '🎯', 'security': '🔒', 'cost': '💰'}[role]
                print(f"\n{icon} {role.upper()} ADVOCATE:")
                print(f"   {argument[:200]}...")

        # Rebuttal round
        if verbose:
            print("\n" + "-" * 70)
            print("REBUTTALS")
            print("-" * 70)

        opponent_args = [t['argument'] for t in transcript]
        for role, agent in self.llm_agents.items():
            argument = agent.generate_argument(context, 'rebuttal', opponent_args)
            transcript.append({
                'role': role,
                'phase': 'rebuttal',
                'argument': argument
            })
            if verbose:
                icon = {'reliability': '🛡️', 'performance': '⚡',
                       'simplicity': '🎯', 'security': '🔒', 'cost': '💰'}[role]
                print(f"\n{icon} {role.upper()} ADVOCATE:")
                print(f"   {argument[:200]}...")

        # Parse and synthesize
        recommendations = self._synthesize_llm_debate(transcript)

        if verbose:
            print("\n" + "=" * 70)
            print("SYNTHESIS")
            print("=" * 70)
            print("\nRecommended patterns:")
            for pattern, count in recommendations[:5]:
                print(f"  - {pattern} (mentioned {count} times)")

        return {
            'transcript': transcript,
            'recommendations': recommendations,
            'raw_arguments': [t['argument'] for t in transcript]
        }

    def _synthesize_llm_debate(self, transcript: List[Dict]) -> List[Tuple[str, int]]:
        """Extract pattern recommendations from LLM debate."""
        pattern_mentions = defaultdict(int)

        # Parse PROPOSE lines from arguments
        for entry in transcript:
            argument = entry['argument']
            lines = argument.split('\n')
            for line in lines:
                if 'PROPOSE:' in line:
                    patterns = line.split('PROPOSE:')[1].strip()
                    for p in patterns.replace('[', '').replace(']', '').split(','):
                        p = p.strip().lower().replace(' ', '_')
                        if p and p in self.kb.graph.nodes:
                            pattern_mentions[p] += 2
                        elif p:
                            # Fuzzy match
                            for pid in self.kb.graph.nodes:
                                if p in pid or pid in p:
                                    pattern_mentions[pid] += 1
                                    break

        return sorted(pattern_mentions.items(), key=lambda x: -x[1])


# =============================================================================
# DEBATE VISUALIZATION
# =============================================================================

def visualize_debate(result: DebateResult) -> str:
    """Generate ASCII visualization of debate result."""
    lines = []
    lines.append("┌" + "─" * 68 + "┐")
    lines.append("│" + " PATTERN DEBATE RESULT".center(68) + "│")
    lines.append("├" + "─" * 68 + "┤")

    # Consensus meter
    consensus_bar = "█" * int(result.consensus_level * 20) + "░" * (20 - int(result.consensus_level * 20))
    lines.append(f"│  Consensus: [{consensus_bar}] {result.consensus_level:.0%}".ljust(69) + "│")
    lines.append(f"│  Confidence: {result.confidence:.0%}".ljust(69) + "│")
    lines.append("├" + "─" * 68 + "┤")

    lines.append("│  RECOMMENDATIONS:".ljust(69) + "│")
    for i, (pattern, score, rationale) in enumerate(result.recommended_patterns[:5], 1):
        score_bar = "▓" * int(score * 10) + "░" * (10 - int(min(score, 1) * 10))
        lines.append(f"│   {i}. {pattern[:25]:<25} [{score_bar}] {score:.2f}".ljust(69) + "│")

    if result.key_tradeoffs:
        lines.append("├" + "─" * 68 + "┤")
        lines.append("│  TRADE-OFFS:".ljust(69) + "│")
        for t in result.key_tradeoffs[:3]:
            tradeoff = t['tradeoff'][:60]
            lines.append(f"│   ⚖️  {tradeoff}".ljust(69) + "│")

    if result.dissenting_opinions:
        lines.append("├" + "─" * 68 + "┤")
        lines.append("│  DISSENTS:".ljust(69) + "│")
        for d in result.dissenting_opinions[:2]:
            dissent = f"{d['advocate']}: {d['concern'][:45]}..."
            lines.append(f"│   ⚠️  {dissent}".ljust(69) + "│")

    lines.append("└" + "─" * 68 + "┘")
    return "\n".join(lines)


# =============================================================================
# DEMONSTRATION
# =============================================================================

def demo():
    """Demonstrate the pattern debate system."""
    print("=" * 80)
    print("MULTI-AGENT PATTERN DEBATE SYSTEM")
    print("=" * 80)

    debate = PatternDebate()

    # Scenario 1: Payment Processing
    print("\n" + "=" * 80)
    print("DEBATE 1: Payment Processing System")
    print("=" * 80)

    result1 = debate.debate(
        requirement="""
        Build a payment processing system that handles credit card transactions.
        Must have 99.99% availability and process transactions within 100ms.
        Transactions cannot be lost and must be auditable for SOC2 compliance.
        We're a startup with limited engineering resources.
        """,
        constraints={
            'availability': 0.9999,
            'max_latency_ms': 100,
            'budget_tier': 'moderate',
            'max_complexity': 8
        },
        rounds=2
    )

    # Scenario 2: High-Throughput API
    print("\n" + "=" * 80)
    print("DEBATE 2: High-Throughput Public API")
    print("=" * 80)

    result2 = debate.debate(
        requirement="""
        Design a public API that serves 50,000 requests per second.
        Multiple third-party services are called downstream.
        The API must be resilient to downstream failures.
        Performance is critical - p99 latency must be under 50ms.
        """,
        constraints={
            'max_latency_ms': 50,
            'throughput_rps': 50000,
            'budget_tier': 'high'
        },
        priority_weights={
            'reliability_advocate': 1.2,
            'performance_advocate': 1.5,  # Performance is critical
            'simplicity_advocate': 0.6,
            'security_advocate': 1.0,
            'cost_advocate': 0.5
        },
        rounds=2
    )

    # Scenario 3: Quick Debate
    print("\n" + "=" * 80)
    print("DEBATE 3: Quick Debate - Microservice Communication")
    print("=" * 80)

    result3 = debate.quick_debate(
        "Microservice architecture with 20 services that need reliable communication"
    )

    print(f"\nQuick Debate Results:")
    print(f"Consensus: {result3.consensus_level:.0%}")
    print(f"Top recommendations: {[r[0] for r in result3.recommended_patterns[:3]]}")

    # Visualization
    print("\n" + "=" * 80)
    print("DEBATE VISUALIZATION")
    print("=" * 80)
    print(visualize_debate(result1))

    # LLM-Powered Debate (if API available)
    print("\n" + "=" * 80)
    print("LLM-POWERED DEBATE (Template Fallback Mode)")
    print("=" * 80)

    llm_debate = LLMPoweredDebate(use_llm=True)
    llm_result = llm_debate.llm_debate(
        requirement="Build a real-time trading platform with sub-10ms latency",
        constraints={'max_latency_ms': 10, 'availability': 0.99999}
    )

    # Summary
    print("\n" + "=" * 80)
    print("DEBATE SUMMARY")
    print("=" * 80)

    for i, result in enumerate([result1, result2, result3], 1):
        print(f"\nDebate {i}:")
        print(f"  Consensus: {result.consensus_level:.0%}")
        print(f"  Confidence: {result.confidence:.0%}")
        print(f"  Top Pattern: {result.recommended_patterns[0][0] if result.recommended_patterns else 'None'}")
        print(f"  Trade-offs: {len(result.key_tradeoffs)}")
        print(f"  Dissents: {len(result.dissenting_opinions)}")

    print("\n" + "=" * 80)
    print("SYSTEM CAPABILITIES")
    print("=" * 80)
    print("""
    Multi-Agent Pattern Debate System:

    ADVOCATES:
      🛡️ Reliability - Champions fault tolerance, circuit breakers, sagas
      ⚡ Performance  - Champions caching, parallelism, low latency
      🎯 Simplicity   - Champions maintainability, YAGNI, minimal patterns
      🔒 Security     - Champions protection, compliance, audit trails
      💰 Cost         - Champions efficiency, sustainable cloud spending

    DEBATE PHASES:
      1. Opening Statements - Each advocate presents their position
      2. Rebuttals (1-3 rounds) - Advocates challenge each other
      3. Closing Statements - Final positions summarized
      4. Deliberation - Arbitrator synthesizes recommendations

    FEATURES:
      - Priority weights for different concerns
      - Consensus detection
      - Trade-off identification
      - Dissenting opinion tracking
      - LLM-powered mode (with API)
      - Visualization
    """)


if __name__ == "__main__":
    demo()
