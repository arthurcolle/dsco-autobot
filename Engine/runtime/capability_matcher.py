"""
Capability Matching Engine for Agent Task Assignment

This module implements skill matching algorithms to determine if an agent
can execute a task and find optimal agent-task pairings based on capabilities.

Uses GraphSub to query agent capabilities and maintains confidence scores
based on execution history.
"""

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from substrate.graphsub_client import GraphSubClient

logger = logging.getLogger(__name__)


class MatchStrategy(Enum):
    """Capability matching strategy."""
    EXACT = "exact"  # Agent must have all required capabilities
    FUZZY = "fuzzy"  # Agent can execute with partial capabilities
    BEST_EFFORT = "best_effort"  # Use whatever capabilities available


class CapabilityTier(Enum):
    """Confidence tier for capability execution."""
    EXPERT = 0.9  # Very confident (0.85-1.0)
    PROFICIENT = 0.75  # Good confidence (0.70-0.84)
    COMPETENT = 0.55  # Can execute (0.50-0.69)
    NOVICE = 0.35  # Might struggle (0.30-0.49)
    UNTRAINED = 0.0  # Cannot execute (<0.30)


@dataclass
class CapabilityScore:
    """Score for a single capability."""
    name: str
    confidence: float  # 0.0 to 1.0
    times_used: int = 0
    times_succeeded: int = 0
    times_failed: int = 0
    last_used_at: Optional[int] = None  # timestamp

    @property
    def success_rate(self) -> float:
        """Calculate success rate from execution history."""
        if self.times_used == 0:
            return self.confidence  # Use base confidence if untested
        return self.times_succeeded / self.times_used

    @property
    def effective_confidence(self) -> float:
        """Blend base confidence with empirical success rate."""
        if self.times_used < 3:
            # Trust base confidence when few trials
            return self.confidence
        # Weighted average: 30% base, 70% empirical after 3+ trials
        return 0.3 * self.confidence + 0.7 * self.success_rate


@dataclass
class CapabilityProfile:
    """Agent's complete capability profile."""
    agent_id: str
    capabilities: Dict[str, CapabilityScore] = field(default_factory=dict)

    def add_capability(
        self,
        name: str,
        confidence: float,
        times_used: int = 0,
        times_succeeded: int = 0,
        times_failed: int = 0,
    ) -> None:
        """Add or update a capability."""
        self.capabilities[name] = CapabilityScore(
            name=name,
            confidence=confidence,
            times_used=times_used,
            times_succeeded=times_succeeded,
            times_failed=times_failed,
        )

    def get_capability(self, name: str) -> Optional[CapabilityScore]:
        """Get capability by name."""
        return self.capabilities.get(name)

    def has_capability(self, name: str, min_confidence: float = 0.3) -> bool:
        """Check if agent has capability above threshold."""
        cap = self.capabilities.get(name)
        if not cap:
            return False
        return cap.effective_confidence >= min_confidence

    def get_matched_capabilities(
        self,
        required: Set[str],
        min_confidence: float = 0.3,
    ) -> Set[str]:
        """Get intersection of required and available capabilities."""
        matched = set()
        for req_cap in required:
            if self.has_capability(req_cap, min_confidence):
                matched.add(req_cap)
        return matched

    def match_score(
        self,
        required: Set[str],
        min_confidence: float = 0.3,
        strategy: MatchStrategy = MatchStrategy.FUZZY,
    ) -> float:
        """Calculate match score (0.0 to 1.0)."""
        if not required:
            return 1.0  # No requirements = perfect match

        matched = self.get_matched_capabilities(required, min_confidence)

        if strategy == MatchStrategy.EXACT:
            # Must have all capabilities
            if len(matched) < len(required):
                return 0.0
            # All matched: average confidence of matched capabilities
            avg_conf = sum(
                self.capabilities[c].effective_confidence for c in matched
            ) / len(matched)
            return avg_conf

        elif strategy == MatchStrategy.FUZZY:
            # Partial credit for partial matching
            # Score = (matched / required) * average_confidence
            match_ratio = len(matched) / len(required)
            if match_ratio == 0:
                return 0.0
            avg_conf = sum(
                self.capabilities[c].effective_confidence for c in matched
            ) / len(matched)
            return match_ratio * avg_conf

        else:  # BEST_EFFORT
            # Use whatever capabilities available
            if not self.capabilities:
                return 0.0
            avg_conf = sum(
                c.effective_confidence for c in self.capabilities.values()
            ) / len(self.capabilities)
            # Apply match ratio as bonus multiplier
            match_ratio = len(matched) / len(required) if required else 1.0
            return (1.0 + match_ratio) / 2.0 * avg_conf


@dataclass
class MatchResult:
    """Result of capability matching."""
    agent_id: str
    match_score: float
    matched_capabilities: Set[str]
    unmatched_capabilities: Set[str]
    confidence_tier: CapabilityTier
    can_execute: bool
    reasoning: str

    @staticmethod
    def tier_from_score(score: float) -> CapabilityTier:
        """Determine tier from match score."""
        if score >= 0.85:
            return CapabilityTier.EXPERT
        elif score >= 0.70:
            return CapabilityTier.PROFICIENT
        elif score >= 0.50:
            return CapabilityTier.COMPETENT
        elif score >= 0.30:
            return CapabilityTier.NOVICE
        else:
            return CapabilityTier.UNTRAINED


class CapabilityMatcher:
    """
    Matches tasks to agents based on capability profiles.

    Maintains agent capability profiles with confidence scores derived from
    both declared capabilities and execution history. Supports multiple
    matching strategies (exact, fuzzy, best-effort).
    """

    def __init__(
        self,
        graphsub_client: GraphSubClient,
        cache_size: int = 1000,
        cache_ttl_seconds: int = 300,
    ):
        """
        Initialize capability matcher.

        Args:
            graphsub_client: GraphSub client for querying agent capabilities
            cache_size: Max cached profiles
            cache_ttl_seconds: Cache entry lifetime in seconds
        """
        self.graphsub = graphsub_client
        self.cache_size = cache_size
        self.cache_ttl_seconds = cache_ttl_seconds
        self._profile_cache: Dict[str, Tuple[int, CapabilityProfile]] = {}
        self._confidence_history: Dict[str, Dict[str, List[float]]] = {}

    async def load_agent_profile(
        self,
        agent_id: str,
        use_cache: bool = True,
    ) -> CapabilityProfile:
        """
        Load agent capability profile from GraphSub.

        Args:
            agent_id: Agent ID to load
            use_cache: Use cached profile if available

        Returns:
            CapabilityProfile with agent's capabilities
        """
        # Check cache
        if use_cache and agent_id in self._profile_cache:
            timestamp, profile = self._profile_cache[agent_id]
            # TODO: Check if cache expired based on timestamp
            if profile:
                return profile

        # Query GraphSub for capabilities
        try:
            results = await self.graphsub.query(
                """
                MATCH (a:Agent {id: $agent_id})-[cap:HAS_CAPABILITY]->(c:Capability)
                RETURN c.name AS capability, cap.level AS confidence,
                       cap.times_used AS times_used,
                       cap.times_succeeded AS times_succeeded,
                       cap.times_failed AS times_failed,
                       cap.last_used_at AS last_used_at
                """,
                {"agent_id": agent_id},
            )

            profile = CapabilityProfile(agent_id=agent_id)

            for row in results:
                capability_name = row.get("capability", "unknown")
                profile.add_capability(
                    name=capability_name,
                    confidence=float(row.get("confidence", 0.5)),
                    times_used=int(row.get("times_used", 0)),
                    times_succeeded=int(row.get("times_succeeded", 0)),
                    times_failed=int(row.get("times_failed", 0)),
                )

            # Cache profile
            self._profile_cache[agent_id] = (int(asyncio.get_event_loop().time()), profile)

            # Evict oldest if cache full
            if len(self._profile_cache) > self.cache_size:
                oldest_key = min(
                    self._profile_cache.keys(),
                    key=lambda k: self._profile_cache[k][0],
                )
                del self._profile_cache[oldest_key]

            return profile

        except Exception as e:
            logger.error(f"Failed to load profile for agent {agent_id}: {e}")
            # Return empty profile on error
            return CapabilityProfile(agent_id=agent_id)

    async def can_execute(
        self,
        agent_id: str,
        required_capabilities: List[str],
        min_confidence: float = 0.3,
        strategy: MatchStrategy = MatchStrategy.FUZZY,
    ) -> Tuple[bool, MatchResult]:
        """
        Check if agent can execute task with required capabilities.

        Args:
            agent_id: Agent ID to check
            required_capabilities: List of required capability names
            min_confidence: Minimum confidence threshold
            strategy: Matching strategy (exact, fuzzy, best-effort)

        Returns:
            Tuple of (can_execute: bool, match_result: MatchResult)
        """
        profile = await self.load_agent_profile(agent_id)
        required = set(required_capabilities or [])

        match_score = profile.match_score(required, min_confidence, strategy)
        matched = profile.get_matched_capabilities(required, min_confidence)
        unmatched = required - matched

        # Determine executability
        can_execute = False
        if strategy == MatchStrategy.EXACT:
            can_execute = len(unmatched) == 0 and match_score >= min_confidence
        elif strategy == MatchStrategy.FUZZY:
            can_execute = match_score >= min_confidence
        else:  # BEST_EFFORT
            can_execute = match_score > 0.0

        tier = MatchResult.tier_from_score(match_score)

        reasoning = self._generate_reasoning(
            match_score,
            len(matched),
            len(required),
            tier,
            strategy,
        )

        result = MatchResult(
            agent_id=agent_id,
            match_score=match_score,
            matched_capabilities=matched,
            unmatched_capabilities=unmatched,
            confidence_tier=tier,
            can_execute=can_execute,
            reasoning=reasoning,
        )

        logger.debug(
            f"Agent {agent_id} can_execute={can_execute} "
            f"score={match_score:.2f} matched={len(matched)}/{len(required)}"
        )

        return can_execute, result

    async def find_best_agent(
        self,
        agent_ids: List[str],
        required_capabilities: List[str],
        min_confidence: float = 0.3,
        strategy: MatchStrategy = MatchStrategy.FUZZY,
    ) -> Optional[Tuple[str, MatchResult]]:
        """
        Find the best agent for a task from a list of candidates.

        Args:
            agent_ids: Candidate agent IDs
            required_capabilities: Task's required capabilities
            min_confidence: Minimum confidence threshold
            strategy: Matching strategy

        Returns:
            Tuple of (best_agent_id, match_result) or None if no match
        """
        if not agent_ids:
            return None

        best_agent = None
        best_score = -1.0
        best_result = None

        for agent_id in agent_ids:
            can_exec, result = await self.can_execute(
                agent_id,
                required_capabilities,
                min_confidence,
                strategy,
            )

            if can_exec and result.match_score > best_score:
                best_agent = agent_id
                best_score = result.match_score
                best_result = result

        if best_result:
            logger.info(
                f"Selected agent {best_agent} with score {best_score:.2f} "
                f"from {len(agent_ids)} candidates"
            )

        return (best_agent, best_result) if best_agent else None

    async def find_agents_for_capability(
        self,
        capability_name: str,
        min_confidence: float = 0.3,
        limit: Optional[int] = None,
    ) -> List[Tuple[str, float]]:
        """
        Find all agents with a specific capability above threshold.

        Args:
            capability_name: Capability to search for
            min_confidence: Minimum confidence threshold
            limit: Maximum number of agents to return

        Returns:
            List of (agent_id, confidence_score) tuples, sorted by confidence desc
        """
        try:
            results = await self.graphsub.query(
                """
                MATCH (a:Agent)-[cap:HAS_CAPABILITY]->(c:Capability {name: $capability})
                WHERE cap.level >= $min_confidence
                RETURN a.id AS agent_id, cap.level AS confidence
                ORDER BY cap.level DESC
                """,
                {
                    "capability": capability_name,
                    "min_confidence": min_confidence,
                },
            )

            agents = [
                (row["agent_id"], float(row["confidence"])) for row in results
            ]

            if limit:
                agents = agents[:limit]

            logger.debug(
                f"Found {len(agents)} agents for capability '{capability_name}'"
            )

            return agents

        except Exception as e:
            logger.error(
                f"Failed to find agents for capability {capability_name}: {e}"
            )
            return []

    async def update_capability_score(
        self,
        agent_id: str,
        capability_name: str,
        success: bool,
        new_confidence: Optional[float] = None,
    ) -> None:
        """
        Update capability score based on execution result.

        Args:
            agent_id: Agent ID
            capability_name: Capability name
            success: Whether execution succeeded
            new_confidence: Optional base confidence adjustment
        """
        try:
            # Update in GraphSub
            if success:
                increment_succeeded = 1
                increment_failed = 0
            else:
                increment_succeeded = 0
                increment_failed = 1

            cypher = """
            MATCH (a:Agent {id: $agent_id})-[cap:HAS_CAPABILITY]->(c:Capability {name: $capability})
            SET cap.times_used = cap.times_used + 1,
                cap.times_succeeded = cap.times_succeeded + $succeeded,
                cap.times_failed = cap.times_failed + $failed,
                cap.last_used_at = timestamp()
            """

            if new_confidence is not None:
                cypher += ", cap.level = $new_confidence"

            await self.graphsub.query(
                cypher,
                {
                    "agent_id": agent_id,
                    "capability": capability_name,
                    "succeeded": increment_succeeded,
                    "failed": increment_failed,
                    "new_confidence": new_confidence,
                },
            )

            # Invalidate cache for this agent
            if agent_id in self._profile_cache:
                del self._profile_cache[agent_id]

            logger.debug(
                f"Updated {capability_name} for agent {agent_id}: "
                f"success={success}"
            )

        except Exception as e:
            logger.error(
                f"Failed to update capability score: {e}"
            )

    async def get_capability_summary(
        self,
        agent_id: str,
    ) -> Dict[str, Dict]:
        """
        Get summary of agent's capabilities for reporting.

        Args:
            agent_id: Agent ID

        Returns:
            Dict mapping capability_name -> {
                'confidence': float,
                'effective_confidence': float,
                'success_rate': float,
                'times_used': int,
            }
        """
        profile = await self.load_agent_profile(agent_id)
        summary = {}

        for name, score in profile.capabilities.items():
            summary[name] = {
                "confidence": score.confidence,
                "effective_confidence": score.effective_confidence,
                "success_rate": score.success_rate,
                "times_used": score.times_used,
                "times_succeeded": score.times_succeeded,
                "times_failed": score.times_failed,
            }

        return summary

    def _generate_reasoning(
        self,
        score: float,
        matched_count: int,
        required_count: int,
        tier: CapabilityTier,
        strategy: MatchStrategy,
    ) -> str:
        """Generate human-readable reasoning for match result."""
        match_pct = (matched_count / required_count * 100) if required_count else 100
        tier_name = tier.name

        if tier == CapabilityTier.EXPERT:
            return (
                f"{tier_name}: Agent has strong mastery ({match_pct:.0f}% capabilities "
                f"matched, score {score:.2f})"
            )
        elif tier == CapabilityTier.PROFICIENT:
            return (
                f"{tier_name}: Agent is well-qualified ({match_pct:.0f}% capabilities "
                f"matched, score {score:.2f})"
            )
        elif tier == CapabilityTier.COMPETENT:
            return (
                f"{tier_name}: Agent can execute with {match_pct:.0f}% capability "
                f"coverage (score {score:.2f})"
            )
        elif tier == CapabilityTier.NOVICE:
            return (
                f"{tier_name}: Agent lacks sufficient expertise ({match_pct:.0f}% "
                f"coverage, score {score:.2f}) - consider with caution"
            )
        else:
            return (
                f"{tier_name}: Agent lacks required capabilities ({match_pct:.0f}% "
                f"coverage, score {score:.2f}) - not recommended"
            )

    def cache_stats(self) -> Dict[str, int]:
        """Get cache statistics."""
        return {
            "cached_profiles": len(self._profile_cache),
            "max_cache_size": self.cache_size,
        }
