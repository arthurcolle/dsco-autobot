"""Matching Engine - Intelligent routing of jobs to optimal workers."""

from datetime import datetime
from typing import List, Optional, Tuple
import math
from parallel_websearch_server.matching_models import (
    JobInput,
    JobType,
    DomainAgent,
    MatchScore,
    MatchResult
)
from parallel_websearch_server.worker_registry import WorkerRegistry


class MatchingEngine:
    """
    Intelligent job-to-worker matching engine.

    Uses multi-dimensional scoring to match jobs with the most appropriate
    workers based on:
    - Capability match (does the worker have the required skills?)
    - Domain expertise (does the worker specialize in this area?)
    - Availability (can the worker take on more work?)
    - Cost efficiency (best value for money)
    - Quality (historical performance)
    - Load balancing (distribute work evenly)
    """

    def __init__(self, registry: WorkerRegistry):
        """
        Initialize matching engine with a worker registry.

        Args:
            registry: WorkerRegistry containing available agents
        """
        self.registry = registry
        self.match_history: List[MatchResult] = []

    def match_job(self, job: JobInput, job_id: str) -> MatchResult:
        """
        Match a job to the best available worker(s).

        Args:
            job: Job specification
            job_id: Unique job identifier

        Returns:
            MatchResult containing selected worker and match details
        """
        # Step 1: Extract requirements from job
        required_capabilities = self._extract_required_capabilities(job)
        preferred_domains = self._extract_preferred_domains(job)

        # Step 2: Find candidate agents
        candidates = self._find_candidates(
            job=job,
            required_capabilities=required_capabilities,
            preferred_domains=preferred_domains
        )

        if not candidates:
            raise ValueError(f"No available workers found for job type: {job.job_type}")

        # Step 3: Score all candidates
        match_scores = [
            self._score_match(job, agent)
            for agent in candidates
        ]

        # Sort by overall score
        match_scores.sort(key=lambda x: x.overall_score, reverse=True)

        # Step 4: Select best match
        best_match = match_scores[0]

        # Step 5: Estimate cost and duration
        agent = self.registry.get_agent(best_match.worker_id)
        estimated_cost = self._estimate_cost(job, agent)
        estimated_duration = self._estimate_duration(job, agent)

        # Step 6: Generate reasoning
        reasoning = self._generate_reasoning(job, agent, best_match)

        # Create match result
        result = MatchResult(
            job_id=job_id,
            matches=match_scores[:10],  # Top 10 matches
            selected_worker_id=best_match.worker_id,
            reasoning=reasoning,
            estimated_cost_usd=estimated_cost,
            estimated_duration_seconds=estimated_duration,
            matched_at=datetime.now()
        )

        # Store in history
        self.match_history.append(result)

        return result

    def _extract_required_capabilities(self, job: JobInput) -> List[str]:
        """Extract required capabilities from job specification."""
        capabilities = []

        # Job type implies certain capabilities
        type_capabilities = {
            JobType.RESEARCH: ["research", "analysis", "web_search"],
            JobType.DATA_LABELING: ["analysis", "classification", "attention_to_detail"],
            JobType.CONTENT_CREATION: ["creative_writing", "communication"],
            JobType.ANALYSIS: ["data_analysis", "critical_thinking", "statistical_analysis"],
            JobType.CODE_GENERATION: ["code_execution", "software_engineering"],
            JobType.COMPUTER_USE: ["computer_use"],
            JobType.GENERAL: ["research", "analysis"]
        }

        capabilities.extend(type_capabilities.get(job.job_type, []))

        # Add explicit requirements
        if job.requirements and job.requirements.required_capabilities:
            capabilities.extend(job.requirements.required_capabilities)

        # Add computer use if required
        if job.requirements and job.requirements.computer_use_required:
            capabilities.append("computer_use")

        return list(set(capabilities))  # Deduplicate

    def _extract_preferred_domains(self, job: JobInput) -> List[str]:
        """Extract preferred domains from job specification."""
        domains = []

        # Use explicit preferences
        if job.requirements and job.requirements.preferred_domains:
            domains.extend(job.requirements.preferred_domains)

        # Try to infer domain from description
        description_lower = job.description.lower()

        domain_keywords = {
            "finance": ["financial", "investment", "trading", "market", "portfolio", "stock", "revenue", "profit"],
            "medicine": ["medical", "health", "patient", "diagnosis", "treatment", "clinical", "disease"],
            "law": ["legal", "contract", "compliance", "regulation", "litigation", "lawsuit"],
            "technology": ["software", "code", "api", "algorithm", "system", "database", "cloud"],
            "science": ["research", "experiment", "hypothesis", "scientific", "study", "analysis"],
            "marketing": ["campaign", "brand", "customer", "marketing", "seo", "content", "social"],
            "data_science": ["data", "model", "prediction", "machine learning", "analytics", "statistics"]
        }

        for domain, keywords in domain_keywords.items():
            if any(keyword in description_lower for keyword in keywords):
                domains.append(domain)

        return domains

    def _find_candidates(
        self,
        job: JobInput,
        required_capabilities: List[str],
        preferred_domains: List[str]
    ) -> List[DomainAgent]:
        """
        Find candidate agents for a job.

        Args:
            job: Job specification
            required_capabilities: List of required capabilities
            preferred_domains: List of preferred domains

        Returns:
            List of candidate agents
        """
        # Start with basic search
        max_cost = job.requirements.max_cost_usd if job.requirements else None
        min_quality = job.requirements.min_quality_score if job.requirements else 0.0
        computer_use = job.requirements.computer_use_required if job.requirements else False

        # Search with requirements
        candidates = self.registry.search_agents(
            domains=preferred_domains if preferred_domains else None,
            capabilities=required_capabilities if required_capabilities else None,
            min_quality=min_quality,
            max_cost=max_cost,
            computer_use_required=computer_use,
            limit=50  # Get top 50 candidates for scoring
        )

        # If we don't have enough candidates, relax constraints
        if len(candidates) < 10:
            candidates = self.registry.search_agents(
                capabilities=required_capabilities if required_capabilities else None,
                min_quality=max(0.0, min_quality - 0.1),
                computer_use_required=computer_use,
                limit=50
            )

        # If still not enough, get any available agents
        if len(candidates) < 5:
            candidates = self.registry.find_available_agents(max_load_pct=0.9)[:50]

        return candidates

    def _score_match(self, job: JobInput, agent: DomainAgent) -> MatchScore:
        """
        Score how well an agent matches a job.

        Uses weighted multi-dimensional scoring:
        - Capability match: 30%
        - Domain match: 25%
        - Availability: 20%
        - Cost: 15%
        - Quality: 10%

        Args:
            job: Job specification
            agent: Worker agent

        Returns:
            MatchScore with detailed breakdown
        """
        # Extract requirements
        required_caps = self._extract_required_capabilities(job)
        preferred_domains = self._extract_preferred_domains(job)

        # 1. Capability Match (0-1)
        capability_score = self._score_capabilities(required_caps, agent)

        # 2. Domain Match (0-1)
        domain_score = self._score_domain(preferred_domains, agent)

        # 3. Availability (0-1)
        availability_score = self._score_availability(agent)

        # 4. Cost (0-1)
        cost_score = self._score_cost(job, agent)

        # 5. Quality (0-1)
        quality_score = agent.avg_quality_score

        # Calculate weighted overall score
        weights = {
            "capability": 0.30,
            "domain": 0.25,
            "availability": 0.20,
            "cost": 0.15,
            "quality": 0.10
        }

        overall_score = (
            capability_score * weights["capability"] +
            domain_score * weights["domain"] +
            availability_score * weights["availability"] +
            cost_score * weights["cost"] +
            quality_score * weights["quality"]
        )

        # Generate explanation
        explanation = self._generate_score_explanation(
            capability_score, domain_score, availability_score, cost_score, quality_score
        )

        return MatchScore(
            worker_id=agent.agent_id,
            overall_score=round(overall_score, 3),
            capability_match=round(capability_score, 3),
            domain_match=round(domain_score, 3),
            availability_score=round(availability_score, 3),
            cost_score=round(cost_score, 3),
            quality_score=round(quality_score, 3),
            explanation=explanation
        )

    def _score_capabilities(self, required_caps: List[str], agent: DomainAgent) -> float:
        """Score how well agent's capabilities match requirements."""
        if not required_caps:
            return 0.8  # Neutral score if no specific requirements

        # Get agent's capability names and proficiencies
        agent_caps = {cap.name: cap.proficiency for cap in agent.capabilities}

        # Calculate match score
        matched_caps = 0
        total_proficiency = 0

        for req_cap in required_caps:
            # Check for exact match or partial match
            for agent_cap_name, proficiency in agent_caps.items():
                if req_cap.lower() in agent_cap_name.lower() or agent_cap_name.lower() in req_cap.lower():
                    matched_caps += 1
                    total_proficiency += proficiency
                    break

        if not required_caps:
            return 0.8

        # Calculate score based on coverage and proficiency
        coverage = matched_caps / len(required_caps)
        avg_proficiency = total_proficiency / matched_caps if matched_caps > 0 else 0

        return (coverage * 0.7) + (avg_proficiency * 0.3)

    def _score_domain(self, preferred_domains: List[str], agent: DomainAgent) -> float:
        """Score how well agent's domain matches preferences."""
        if not preferred_domains:
            return 0.7  # Neutral score if no preferences

        agent_domain_lower = agent.domain.lower()

        # Check for matches
        matches = sum(
            1 for domain in preferred_domains
            if domain.lower() in agent_domain_lower
        )

        return min(1.0, matches / len(preferred_domains) + 0.2)  # Bonus for any match

    def _score_availability(self, agent: DomainAgent) -> float:
        """Score based on agent's current load."""
        if agent.max_concurrent_tasks == 0:
            return 0.0

        load_pct = agent.current_load / agent.max_concurrent_tasks
        # Prefer agents with lower load
        return 1.0 - load_pct

    def _score_cost(self, job: JobInput, agent: DomainAgent) -> float:
        """Score based on cost efficiency."""
        if not job.requirements or not job.requirements.max_cost_usd:
            # If no budget constraint, prefer cheaper but not too cheap
            # Normalize cost to 0-1 range (assuming $0.05 to $5.00 range)
            normalized_cost = min(1.0, agent.cost_per_task_usd / 5.0)
            return 1.0 - (normalized_cost * 0.5)  # Don't penalize cost too much

        max_cost = job.requirements.max_cost_usd
        if agent.cost_per_task_usd > max_cost:
            return 0.0  # Over budget

        # Score based on how much budget is left
        cost_ratio = agent.cost_per_task_usd / max_cost
        return 1.0 - cost_ratio

    def _generate_score_explanation(
        self,
        capability: float,
        domain: float,
        availability: float,
        cost: float,
        quality: float
    ) -> str:
        """Generate human-readable explanation of match score."""
        explanations = []

        if capability >= 0.8:
            explanations.append("excellent capability match")
        elif capability >= 0.6:
            explanations.append("good capability match")
        else:
            explanations.append("partial capability match")

        if domain >= 0.8:
            explanations.append("strong domain expertise")
        elif domain >= 0.6:
            explanations.append("relevant domain experience")

        if availability >= 0.8:
            explanations.append("high availability")
        elif availability >= 0.5:
            explanations.append("moderate availability")

        if cost >= 0.8:
            explanations.append("cost-effective")

        if quality >= 0.9:
            explanations.append("excellent quality history")

        return ", ".join(explanations)

    def _estimate_cost(self, job: JobInput, agent: DomainAgent) -> float:
        """Estimate total cost for the job."""
        base_cost = agent.cost_per_task_usd

        # Adjust for complexity
        complexity_multiplier = 1.0
        if job.requirements and job.requirements.computer_use_required:
            complexity_multiplier *= 1.5
        if job.priority.value == "urgent":
            complexity_multiplier *= 1.3

        return round(base_cost * complexity_multiplier, 2)

    def _estimate_duration(self, job: JobInput, agent: DomainAgent) -> int:
        """Estimate duration in seconds for the job."""
        base_duration = agent.avg_response_time_seconds

        # Adjust for complexity and priority
        if job.job_type == JobType.RESEARCH:
            base_duration *= 2.0
        elif job.job_type == JobType.CODE_GENERATION:
            base_duration *= 1.5

        if job.requirements and job.requirements.computer_use_required:
            base_duration *= 1.3

        return int(base_duration)

    def _generate_reasoning(self, job: JobInput, agent: DomainAgent, match: MatchScore) -> str:
        """Generate detailed reasoning for the match."""
        lines = [
            f"Selected {agent.name} (ID: {agent.agent_id})",
            f"Domain: {agent.domain}",
            f"Overall match score: {match.overall_score:.2f}",
            f"",
            f"Breakdown:",
            f"  • Capability match: {match.capability_match:.2f}",
            f"  • Domain expertise: {match.domain_match:.2f}",
            f"  • Availability: {match.availability_score:.2f}",
            f"  • Cost efficiency: {match.cost_score:.2f}",
            f"  • Quality rating: {match.quality_score:.2f}",
            f"",
            f"Agent capabilities: {', '.join(c.name for c in agent.capabilities[:5])}",
            f"Backend: {agent.backend_type} ({agent.backend_model})",
            f"",
            f"{match.explanation}"
        ]

        return "\n".join(lines)

    def get_match_stats(self) -> dict:
        """Get statistics about matching performance."""
        if not self.match_history:
            return {"total_matches": 0}

        return {
            "total_matches": len(self.match_history),
            "avg_match_score": sum(m.matches[0].overall_score for m in self.match_history) / len(self.match_history),
            "avg_estimated_cost": sum(m.estimated_cost_usd for m in self.match_history) / len(self.match_history),
            "avg_estimated_duration": sum(m.estimated_duration_seconds for m in self.match_history) / len(self.match_history),
            "most_used_workers": self._get_most_used_workers(5)
        }

    def _get_most_used_workers(self, limit: int = 5) -> List[Tuple[str, int]]:
        """Get most frequently matched workers."""
        from collections import Counter

        worker_counts = Counter(m.selected_worker_id for m in self.match_history)
        return worker_counts.most_common(limit)
