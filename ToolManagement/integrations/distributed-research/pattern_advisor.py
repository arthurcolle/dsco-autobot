#!/usr/bin/env python3
"""
PATTERN ADVISOR - Agent-Integrated Architectural Guidance

Integrates the Pattern Intelligence system with the AdvancedAgent,
providing real-time architectural recommendations during task execution.
"""

import json
import time
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

from pattern_intelligence import (
    PatternKnowledgeBase,
    PatternDomain,
    PatternIntent,
    BayesianPatternMatcher,
    CausalReasoningEngine,
    RecommendationContext,
    Pattern
)


# =============================================================================
# CONTEXT DETECTION
# =============================================================================

@dataclass
class TaskContext:
    """Detected context from agent task."""
    task_description: str
    detected_domains: List[PatternDomain]
    detected_intents: List[PatternIntent]
    code_signals: Dict[str, float]  # signal -> confidence
    existing_patterns: List[str]
    constraints: Dict[str, Any]
    confidence: float


class ContextDetector:
    """Detects architectural context from task descriptions and code."""

    DOMAIN_SIGNALS = {
        PatternDomain.RESILIENCE: [
            'fail', 'error', 'retry', 'timeout', 'circuit', 'fallback',
            'recover', 'fault', 'resilient', 'reliable'
        ],
        PatternDomain.PERFORMANCE: [
            'fast', 'slow', 'latency', 'throughput', 'cache', 'optimize',
            'performance', 'speed', 'efficient', 'scale'
        ],
        PatternDomain.DATA_FLOW: [
            'transaction', 'consistent', 'saga', 'event', 'message',
            'queue', 'stream', 'data', 'sync', 'async'
        ],
        PatternDomain.OBSERVABILITY: [
            'monitor', 'log', 'trace', 'metric', 'alert', 'debug',
            'observe', 'track', 'audit', 'dashboard'
        ],
        PatternDomain.SECURITY: [
            'auth', 'secure', 'encrypt', 'token', 'permission', 'access',
            'validate', 'sanitize', 'protect', 'credential'
        ],
        PatternDomain.INTEGRATION: [
            'api', 'service', 'endpoint', 'webhook', 'integrate',
            'external', 'third-party', 'connect', 'gateway'
        ],
        PatternDomain.ORCHESTRATION: [
            'workflow', 'orchestrate', 'coordinate', 'schedule', 'job',
            'pipeline', 'sequence', 'parallel', 'batch'
        ],
        PatternDomain.GOVERNANCE: [
            'approve', 'review', 'compliance', 'policy', 'rule',
            'regulation', 'audit', 'control', 'governance'
        ]
    }

    INTENT_SIGNALS = {
        PatternIntent.RELIABILITY: ['reliable', 'dependable', 'stable', 'uptime', 'available'],
        PatternIntent.SCALABILITY: ['scale', 'grow', 'horizontal', 'distributed', 'elastic'],
        PatternIntent.CONSISTENCY: ['consistent', 'accurate', 'correct', 'atomic', 'transaction'],
        PatternIntent.AVAILABILITY: ['available', '99.9', 'uptime', 'always', 'redundant'],
        PatternIntent.EFFICIENCY: ['efficient', 'fast', 'optimize', 'performance', 'quick'],
        PatternIntent.COMPLIANCE: ['compliant', 'audit', 'regulation', 'soc2', 'hipaa', 'gdpr'],
        PatternIntent.ISOLATION: ['isolate', 'separate', 'contain', 'sandbox', 'boundary'],
        PatternIntent.COORDINATION: ['coordinate', 'orchestrate', 'manage', 'control', 'sequence']
    }

    CODE_PATTERN_SIGNALS = {
        'retry_pattern': [
            'retry', 'attempt', 'max_retries', 'backoff', 'exponential'
        ],
        'circuit_breaker': [
            'circuit', 'breaker', 'open', 'closed', 'half_open', 'trip'
        ],
        'rate_limiting': [
            'rate_limit', 'throttle', 'quota', 'bucket', 'window'
        ],
        'caching': [
            'cache', 'redis', 'memcached', 'ttl', 'invalidate'
        ],
        'messaging': [
            'queue', 'kafka', 'rabbitmq', 'pubsub', 'subscriber', 'publisher'
        ],
        'saga': [
            'saga', 'compensate', 'rollback', 'step', 'orchestrator'
        ]
    }

    def detect(self, task: str, code: Optional[str] = None) -> TaskContext:
        """Detect context from task and optional code."""
        task_lower = task.lower()
        code_lower = (code or '').lower()
        combined = task_lower + ' ' + code_lower

        # Detect domains
        domain_scores = {}
        for domain, signals in self.DOMAIN_SIGNALS.items():
            score = sum(1 for s in signals if s in combined)
            if score > 0:
                domain_scores[domain] = score

        detected_domains = sorted(domain_scores.keys(),
                                   key=lambda d: -domain_scores[d])[:3]

        # Detect intents
        intent_scores = {}
        for intent, signals in self.INTENT_SIGNALS.items():
            score = sum(1 for s in signals if s in combined)
            if score > 0:
                intent_scores[intent] = score

        detected_intents = sorted(intent_scores.keys(),
                                   key=lambda i: -intent_scores[i])[:3]

        # Detect existing patterns from code
        code_signals = {}
        existing_patterns = []
        if code:
            for pattern, signals in self.CODE_PATTERN_SIGNALS.items():
                matches = sum(1 for s in signals if s in code_lower)
                if matches >= 2:  # Need at least 2 signals
                    confidence = min(1.0, matches / len(signals))
                    code_signals[pattern] = confidence
                    existing_patterns.append(pattern)

        # Extract constraints (basic heuristics)
        constraints = {}
        if '99.9' in task_lower or '99.99' in task_lower:
            constraints['availability'] = 0.9999 if '99.99' in task_lower else 0.999
        if 'ms' in task_lower:
            # Try to extract latency requirement
            import re
            match = re.search(r'(\d+)\s*ms', task_lower)
            if match:
                constraints['max_latency_ms'] = int(match.group(1))

        # Calculate overall confidence
        total_signals = sum(domain_scores.values()) + sum(intent_scores.values())
        confidence = min(1.0, total_signals / 10)

        return TaskContext(
            task_description=task,
            detected_domains=detected_domains,
            detected_intents=detected_intents,
            code_signals=code_signals,
            existing_patterns=existing_patterns,
            constraints=constraints,
            confidence=confidence
        )


# =============================================================================
# PATTERN ADVISOR
# =============================================================================

@dataclass
class PatternAdvice:
    """Advice from the pattern advisor."""
    recommended_patterns: List[Tuple[str, float, str]]  # (id, score, reason)
    warnings: List[str]
    synergies: List[str]
    causal_effects: Dict[str, List[str]]
    confidence: float
    context_summary: str


class PatternAdvisor:
    """
    Provides architectural pattern recommendations integrated with agent tasks.
    """

    def __init__(self):
        self.kb = PatternKnowledgeBase()
        self.bayesian = BayesianPatternMatcher(self.kb)
        self.causal = CausalReasoningEngine(self.kb.graph)
        self.detector = ContextDetector()

        # Track usage for learning
        self.session_patterns: List[str] = []
        self.advice_history: List[Dict] = []

    def advise(self, task: str, code: Optional[str] = None,
               existing_patterns: Optional[List[str]] = None) -> PatternAdvice:
        """Get pattern advice for a task."""
        # Detect context
        context = self.detector.detect(task, code)

        # Merge existing patterns
        all_existing = list(set(
            (existing_patterns or []) +
            context.existing_patterns +
            self.session_patterns
        ))

        # Get recommendations
        recommendations = []

        # Method 1: Bayesian inference
        evidence = self.bayesian.extract_evidence(task)
        if context.detected_domains:
            evidence[f'domain_{context.detected_domains[0].value}'] = True
        bayesian_matches = self.bayesian.infer(evidence, top_k=5)

        for match in bayesian_matches:
            if match.pattern_id not in all_existing:
                pattern = self.kb.graph.nodes.get(match.pattern_id)
                if pattern:
                    recommendations.append((
                        match.pattern_id,
                        match.mean_score,
                        f"Bayesian match (CI: {match.confidence_interval[0]:.2f}-{match.confidence_interval[1]:.2f})"
                    ))

        # Method 2: Context-aware recommender
        rec_context = RecommendationContext(
            scenario=task,
            existing_patterns=all_existing,
            domain=context.detected_domains[0] if context.detected_domains else None,
            required_intents=context.detected_intents
        )
        context_recs = self.kb.recommender.recommend(rec_context, k=5)

        for rec in context_recs:
            if rec.pattern_id not in all_existing:
                # Check if already in recommendations
                existing_rec = next(
                    (r for r in recommendations if r[0] == rec.pattern_id), None
                )
                if existing_rec:
                    # Boost score if found by both methods
                    idx = recommendations.index(existing_rec)
                    recommendations[idx] = (
                        rec.pattern_id,
                        (existing_rec[1] + rec.score) / 2 * 1.2,  # Boost
                        existing_rec[2] + " + context match"
                    )
                else:
                    recommendations.append((
                        rec.pattern_id,
                        rec.score,
                        rec.explanation
                    ))

        # Sort and deduplicate
        recommendations.sort(key=lambda x: -x[1])
        recommendations = recommendations[:5]

        # Check for warnings (anti-patterns, missing patterns)
        warnings = []
        proposed = [r[0] for r in recommendations[:3]]
        all_patterns = all_existing + proposed

        anti_warnings = self.kb.analyze_antipatterns(all_patterns)
        for w in anti_warnings:
            warnings.append(f"[{w.severity}] {w.message}")

        # Find synergies
        synergies = []
        for i, p1 in enumerate(proposed):
            for p2 in proposed[i+1:]:
                enhancements = dict(self.kb.graph.get_enhancements(p1))
                if p2 in enhancements:
                    synergies.append(f"{p1} + {p2} = synergy ({enhancements[p2]:.1f})")

        # Trace causal effects of adding top recommendation
        causal_effects = {'improves': [], 'degrades': []}
        if recommendations:
            top_pattern = recommendations[0][0]
            effects = self.causal.trace_effects(top_pattern, depth=2)
            for target, strength, _ in effects.get('improves', [])[:3]:
                causal_effects['improves'].append(f"{target} (+{strength:.2f})")
            for target, strength, _ in effects.get('degrades', [])[:2]:
                causal_effects['degrades'].append(f"{target} ({strength:.2f})")

        # Build context summary
        domain_str = ', '.join(d.value for d in context.detected_domains[:2]) or 'general'
        intent_str = ', '.join(i.value for i in context.detected_intents[:2]) or 'unspecified'
        context_summary = f"Domain: {domain_str} | Intent: {intent_str}"
        if context.constraints:
            constraints_str = ', '.join(f"{k}={v}" for k, v in context.constraints.items())
            context_summary += f" | Constraints: {constraints_str}"

        advice = PatternAdvice(
            recommended_patterns=recommendations,
            warnings=warnings,
            synergies=synergies,
            causal_effects=causal_effects,
            confidence=context.confidence,
            context_summary=context_summary
        )

        # Record for history
        self.advice_history.append({
            'task': task,
            'advice': advice,
            'timestamp': time.time()
        })

        return advice

    def accept_pattern(self, pattern_id: str, success: bool = True):
        """Record that a pattern recommendation was accepted."""
        self.session_patterns.append(pattern_id)
        self.bayesian.learn_from_usage(
            pattern_id,
            success=success,
            co_used_with=self.session_patterns[-5:]  # Last 5 patterns
        )

    def get_composition(self) -> Dict:
        """Get composition analysis of current session patterns."""
        if not self.session_patterns:
            return {'patterns': [], 'composition': None}

        composed = self.kb.compose(self.session_patterns)
        explanation = self.causal.explain_combination(self.session_patterns)

        return {
            'patterns': self.session_patterns,
            'composition': {
                'name': composed.name,
                'type': composed.composition_type,
                'score': composed.score,
                'synergies': composed.synergies,
                'conflicts': composed.conflicts
            },
            'explanation': explanation
        }

    def what_if(self, pattern_id: str) -> Dict:
        """Counterfactual: what if we add this pattern?"""
        return self.causal.counterfactual(self.session_patterns, pattern_id)

    def suggest_next(self, goal: str) -> List[Tuple[str, float, str]]:
        """Suggest next patterns to add given a goal."""
        return self.kb.composer.suggest_additions(
            self.session_patterns, goal, k=5
        )


# =============================================================================
# AGENT TOOL INTEGRATION
# =============================================================================

class PatternAdvisorTool:
    """
    Tool wrapper for integration with AdvancedAgent.

    Add to agent's meta_tools:
        {
            "name": "pattern_advisor",
            "description": "Get architectural pattern recommendations for the current task",
            "input_schema": {
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["advise", "accept", "what_if", "composition", "suggest_next"]},
                    "task": {"type": "string", "description": "Task description for advise action"},
                    "pattern_id": {"type": "string", "description": "Pattern ID for accept/what_if actions"},
                    "goal": {"type": "string", "description": "Goal for suggest_next action"}
                }
            }
        }
    """

    def __init__(self):
        self.advisor = PatternAdvisor()

    def execute(self, params: Dict) -> Dict:
        """Execute pattern advisor action."""
        action = params.get('action', 'advise')

        if action == 'advise':
            task = params.get('task', '')
            code = params.get('code')
            existing = params.get('existing_patterns', [])

            advice = self.advisor.advise(task, code, existing)

            return {
                'success': True,
                'recommendations': [
                    {'pattern': p[0], 'score': p[1], 'reason': p[2]}
                    for p in advice.recommended_patterns
                ],
                'warnings': advice.warnings,
                'synergies': advice.synergies,
                'causal_effects': advice.causal_effects,
                'confidence': advice.confidence,
                'context': advice.context_summary
            }

        elif action == 'accept':
            pattern_id = params.get('pattern_id', '')
            success = params.get('success', True)
            self.advisor.accept_pattern(pattern_id, success)
            return {
                'success': True,
                'message': f"Recorded acceptance of {pattern_id}",
                'session_patterns': self.advisor.session_patterns
            }

        elif action == 'what_if':
            pattern_id = params.get('pattern_id', '')
            result = self.advisor.what_if(pattern_id)
            return {'success': True, 'analysis': result}

        elif action == 'composition':
            result = self.advisor.get_composition()
            return {'success': True, 'composition': result}

        elif action == 'suggest_next':
            goal = params.get('goal', '')
            suggestions = self.advisor.suggest_next(goal)
            return {
                'success': True,
                'suggestions': [
                    {'pattern': s[0], 'score': s[1], 'reason': s[2]}
                    for s in suggestions
                ]
            }

        else:
            return {'success': False, 'error': f"Unknown action: {action}"}


# =============================================================================
# DEMONSTRATION
# =============================================================================

def demo():
    """Demonstrate the pattern advisor."""
    print("=" * 80)
    print("PATTERN ADVISOR - Agent-Integrated Architectural Guidance")
    print("=" * 80)

    advisor = PatternAdvisor()

    # Scenario 1: Payment processing task
    print("\n" + "=" * 60)
    print("SCENARIO 1: Payment Processing")
    print("=" * 60)

    task1 = """
    Build a payment processing service that handles credit card transactions.
    Must have 99.99% availability and process transactions within 100ms.
    Transactions cannot be lost and must be auditable for SOC2 compliance.
    """

    advice1 = advisor.advise(task1)

    print(f"\nTask: {task1.strip()[:80]}...")
    print(f"\nContext: {advice1.context_summary}")
    print(f"Confidence: {advice1.confidence:.2f}")

    print("\nRecommended Patterns:")
    for pid, score, reason in advice1.recommended_patterns[:5]:
        pattern = advisor.kb.graph.nodes.get(pid)
        print(f"  [{score:.2f}] {pattern.name if pattern else pid}")
        print(f"         {reason}")

    if advice1.causal_effects['improves']:
        print(f"\nAdding top pattern improves: {', '.join(advice1.causal_effects['improves'][:3])}")

    if advice1.warnings:
        print("\nWarnings:")
        for w in advice1.warnings:
            print(f"  {w}")

    # Accept some recommendations
    print("\n--- Accepting patterns ---")
    advisor.accept_pattern('saga')
    advisor.accept_pattern('audit_trail')
    advisor.accept_pattern('circuit_breaker')
    print(f"Session patterns: {advisor.session_patterns}")

    # Scenario 2: API resilience
    print("\n" + "=" * 60)
    print("SCENARIO 2: API Resilience (with existing patterns)")
    print("=" * 60)

    task2 = """
    Add resilience to our API gateway that calls 5 external services.
    Some services are flaky and timeout frequently.
    Need graceful degradation when services fail.
    """

    advice2 = advisor.advise(task2)

    print(f"\nTask: {task2.strip()[:80]}...")
    print(f"Context: {advice2.context_summary}")

    print("\nRecommended Patterns:")
    for pid, score, reason in advice2.recommended_patterns[:4]:
        pattern = advisor.kb.graph.nodes.get(pid)
        print(f"  [{score:.2f}] {pattern.name if pattern else pid}")
        print(f"         {reason}")

    if advice2.synergies:
        print(f"\nSynergies: {', '.join(advice2.synergies[:3])}")

    # What-if analysis
    print("\n--- What-if Analysis ---")
    what_if = advisor.what_if('bulkhead')
    print(f"\nWhat if we add 'bulkhead'?")
    print(f"  Action: {what_if['action']}")
    print(f"  Risk: {what_if['risk_assessment']}")
    for change in what_if['predicted_changes'][:3]:
        print(f"  - {change['target']}: {change['effect']} ({change['strength']:+.2f})")

    # Get composition
    print("\n--- Current Architecture Composition ---")
    advisor.accept_pattern('retry_backoff')
    advisor.accept_pattern('fallback')

    composition = advisor.get_composition()
    print(f"\nPatterns: {', '.join(composition['patterns'])}")
    print(f"Composition: {composition['composition']['name']}")
    print(f"Type: {composition['composition']['type']}")
    print(f"Score: {composition['composition']['score']:.2f}")

    if composition['composition']['synergies']:
        syns = [f"{s[0]}+{s[1]}" for s in composition['composition']['synergies'][:3]]
        print(f"Synergies: {', '.join(syns)}")

    # Suggest next
    print("\n--- Suggest Next Patterns ---")
    goal = "complete the resilience stack for production readiness"
    suggestions = advisor.suggest_next(goal)
    print(f"\nGoal: {goal}")
    print("Suggestions:")
    for pid, score, reason in suggestions[:3]:
        pattern = advisor.kb.graph.nodes.get(pid)
        print(f"  [{score:.2f}] {pattern.name if pattern else pid}")
        print(f"         {reason}")

    # Show tool integration
    print("\n" + "=" * 60)
    print("TOOL INTEGRATION EXAMPLE")
    print("=" * 60)

    tool = PatternAdvisorTool()

    result = tool.execute({
        'action': 'advise',
        'task': 'Build a high-throughput event streaming pipeline'
    })

    print("\nAgent Tool Call: pattern_advisor(action='advise', task='...')")
    print(f"\nResult:")
    print(f"  Success: {result['success']}")
    print(f"  Context: {result['context']}")
    print(f"  Top recommendations:")
    for rec in result['recommendations'][:3]:
        print(f"    - {rec['pattern']}: {rec['score']:.2f}")

    print("\n" + "=" * 80)
    print("DEMO COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    demo()
