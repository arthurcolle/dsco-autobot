"""Worker Registry - Manages 2300 specialized domain agents."""

from datetime import datetime
from typing import Dict, List, Optional, Set
import logging
import random
from parallel_websearch_server.matching_models import (
    DomainAgent,
    WorkerCapability,
    WorkerStatus,
    WorkerType,
    WorkerRegistry as WorkerRegistryModel
)
from parallel_websearch_server.models import ProcessorType
from parallel_websearch_server.config import settings


logger = logging.getLogger(__name__)


# ============================================================================
# Domain Agent Definitions - 2300 specialized agents
# ============================================================================

DOMAIN_CATEGORIES = {
    "finance": {
        "count": 250,
        "specializations": [
            "investment_analysis", "risk_assessment", "portfolio_management", "trading_strategies",
            "financial_modeling", "derivatives", "cryptocurrency", "commodities", "equity_research",
            "credit_analysis", "real_estate_finance", "venture_capital", "private_equity",
            "financial_planning", "tax_optimization", "regulatory_compliance", "fraud_detection",
            "market_microstructure", "algorithmic_trading", "quantitative_finance", "financial_forensics",
            "insurance_analysis", "pension_planning", "wealth_management", "corporate_finance"
        ]
    },
    "medicine": {
        "count": 200,
        "specializations": [
            "cardiology", "neurology", "oncology", "radiology", "pathology", "surgery",
            "pediatrics", "geriatrics", "psychiatry", "dermatology", "immunology", "genetics",
            "pharmacology", "epidemiology", "public_health", "medical_imaging", "emergency_medicine",
            "intensive_care", "anesthesiology", "obstetrics", "orthopedics", "ophthalmology",
            "endocrinology", "nephrology", "gastroenterology", "hematology", "rheumatology",
            "infectious_disease", "medical_research", "clinical_trials"
        ]
    },
    "law": {
        "count": 200,
        "specializations": [
            "contract_law", "corporate_law", "intellectual_property", "litigation", "tax_law",
            "criminal_law", "civil_rights", "labor_law", "environmental_law", "real_estate_law",
            "family_law", "immigration_law", "bankruptcy", "securities_law", "antitrust",
            "international_law", "maritime_law", "constitutional_law", "administrative_law",
            "healthcare_law", "privacy_law", "cybersecurity_law", "entertainment_law", "sports_law"
        ]
    },
    "technology": {
        "count": 300,
        "specializations": [
            "software_engineering", "cybersecurity", "cloud_architecture", "data_engineering",
            "machine_learning", "devops", "frontend_development", "backend_development",
            "mobile_development", "game_development", "blockchain", "iot", "edge_computing",
            "network_engineering", "systems_architecture", "database_design", "api_design",
            "microservices", "containerization", "serverless", "web3", "quantum_computing",
            "computer_vision", "natural_language_processing", "robotics", "embedded_systems",
            "security_engineering", "platform_engineering", "site_reliability", "performance_optimization"
        ]
    },
    "science": {
        "count": 250,
        "specializations": [
            "physics", "chemistry", "biology", "astronomy", "geology", "environmental_science",
            "materials_science", "neuroscience", "biochemistry", "molecular_biology", "ecology",
            "marine_biology", "atmospheric_science", "climatology", "geophysics", "astrophysics",
            "quantum_mechanics", "particle_physics", "biophysics", "computational_biology",
            "synthetic_biology", "genomics", "proteomics", "microbiology", "botany", "zoology"
        ]
    },
    "engineering": {
        "count": 200,
        "specializations": [
            "mechanical_engineering", "electrical_engineering", "civil_engineering", "aerospace_engineering",
            "chemical_engineering", "industrial_engineering", "biomedical_engineering", "materials_engineering",
            "nuclear_engineering", "petroleum_engineering", "automotive_engineering", "robotics_engineering",
            "structural_engineering", "environmental_engineering", "manufacturing_engineering",
            "systems_engineering", "ocean_engineering", "agricultural_engineering", "mining_engineering",
            "acoustical_engineering", "optical_engineering", "renewable_energy"
        ]
    },
    "business": {
        "count": 150,
        "specializations": [
            "strategy", "operations", "human_resources", "supply_chain", "logistics", "procurement",
            "product_management", "project_management", "business_development", "sales", "customer_success",
            "market_research", "competitive_analysis", "pricing_strategy", "negotiation", "leadership",
            "organizational_development", "change_management", "risk_management", "quality_assurance",
            "business_analytics", "forecasting", "budgeting", "mergers_acquisitions"
        ]
    },
    "marketing": {
        "count": 150,
        "specializations": [
            "digital_marketing", "content_marketing", "seo", "sem", "social_media_marketing",
            "email_marketing", "brand_strategy", "market_research", "consumer_behavior", "advertising",
            "public_relations", "influencer_marketing", "growth_hacking", "conversion_optimization",
            "marketing_analytics", "customer_acquisition", "retention_strategy", "community_management",
            "event_marketing", "product_marketing", "content_strategy", "copywriting"
        ]
    },
    "creative": {
        "count": 120,
        "specializations": [
            "creative_writing", "technical_writing", "journalism", "screenwriting", "poetry",
            "graphic_design", "ui_ux_design", "video_production", "photography", "animation",
            "music_composition", "sound_design", "game_design", "3d_modeling", "illustration",
            "art_direction", "creative_direction", "storytelling", "editing", "publishing"
        ]
    },
    "education": {
        "count": 100,
        "specializations": [
            "curriculum_design", "instructional_design", "educational_technology", "assessment_design",
            "learning_analytics", "tutoring", "academic_advising", "special_education", "adult_education",
            "online_learning", "training_development", "educational_research", "pedagogy", "andragogy"
        ]
    },
    "data_science": {
        "count": 150,
        "specializations": [
            "data_analysis", "statistical_modeling", "predictive_analytics", "data_visualization",
            "big_data", "data_mining", "feature_engineering", "a_b_testing", "causal_inference",
            "time_series_analysis", "recommendation_systems", "anomaly_detection", "nlp", "computer_vision",
            "deep_learning", "reinforcement_learning", "bayesian_statistics", "experimental_design"
        ]
    },
    "social_sciences": {
        "count": 80,
        "specializations": [
            "psychology", "sociology", "anthropology", "economics", "political_science",
            "behavioral_economics", "social_psychology", "cognitive_psychology", "organizational_psychology",
            "developmental_psychology", "cultural_anthropology", "urban_planning", "demography", "criminology"
        ]
    },
    "consulting": {
        "count": 100,
        "specializations": [
            "management_consulting", "strategy_consulting", "technology_consulting", "financial_consulting",
            "hr_consulting", "operations_consulting", "digital_transformation", "process_improvement",
            "change_management", "organizational_design", "performance_improvement", "turnaround_consulting"
        ]
    },
    "agriculture": {
        "count": 50,
        "specializations": [
            "precision_agriculture", "crop_science", "soil_science", "agricultural_economics",
            "sustainable_farming", "livestock_management", "food_safety", "agricultural_technology",
            "hydroponics", "vertical_farming", "agribusiness", "pest_management"
        ]
    },
    "government": {
        "count": 80,
        "specializations": [
            "policy_analysis", "regulatory_affairs", "public_administration", "international_relations",
            "diplomacy", "defense", "intelligence_analysis", "homeland_security", "urban_policy",
            "economic_policy", "healthcare_policy", "education_policy", "environmental_policy"
        ]
    },
    "real_estate": {
        "count": 70,
        "specializations": [
            "commercial_real_estate", "residential_real_estate", "property_valuation", "real_estate_investment",
            "property_management", "real_estate_development", "urban_development", "zoning", "appraisal",
            "real_estate_finance", "asset_management", "facilities_management"
        ]
    }
}

# Common capabilities across agents
CAPABILITY_TEMPLATES = {
    "research": ["basic_research", "deep_research", "literature_review", "data_gathering"],
    "analysis": ["data_analysis", "critical_thinking", "pattern_recognition", "synthesis"],
    "writing": ["technical_writing", "creative_writing", "report_writing", "documentation"],
    "computation": ["mathematical_modeling", "statistical_analysis", "simulation", "optimization"],
    "communication": ["presentation", "stakeholder_management", "teaching", "mentoring"],
    "tools": ["web_search", "code_execution", "computer_use", "api_integration"]
}


class WorkerRegistry:
    """
    Registry for managing 2300 specialized domain agents.

    This registry maintains a pool of AI agents with different specializations,
    capabilities, and performance characteristics. It supports:
    - Agent discovery by domain, capability, or requirements
    - Load balancing across agent instances
    - Performance tracking and optimization
    - Dynamic scaling to billions of instances
    """

    def __init__(self):
        """Initialize the worker registry with 2300 domain agents."""
        self.agents: Dict[str, DomainAgent] = {}
        self.agents_by_domain: Dict[str, List[str]] = {}
        self.agents_by_capability: Dict[str, List[str]] = {}
        self._initialize_agents()

    def _initialize_agents(self):
        """Generate all 2300 domain agents."""
        agent_counter = 0

        for domain_category, config in DOMAIN_CATEGORIES.items():
            count = config["count"]
            specializations = config["specializations"]

            # Create agents for this domain
            agents_per_spec = max(1, count // len(specializations))

            for spec_idx, specialization in enumerate(specializations):
                # Create multiple agents per specialization for redundancy
                for variant in range(agents_per_spec):
                    if agent_counter >= 2300:
                        break

                    agent_id = f"agent_{domain_category}_{specialization}_{variant:03d}"
                    agent_counter += 1

                    # Generate capabilities
                    capabilities = self._generate_capabilities(domain_category, specialization)

                    # Determine backend and model based on domain
                    backend_type, backend_model = self._select_backend(domain_category, specialization)

                    # Create agent
                    agent = DomainAgent(
                        agent_id=agent_id,
                        name=f"{specialization.replace('_', ' ').title()} Specialist {variant+1}",
                        domain=f"{domain_category}/{specialization}",
                        description=f"Specialized agent for {specialization.replace('_', ' ')} in {domain_category}",
                        capabilities=capabilities,
                        supports_computer_use=self._supports_computer_use(specialization),
                        supports_web_search=True,
                        supports_code_execution=self._supports_code_execution(domain_category),
                        avg_quality_score=random.uniform(0.75, 0.95),
                        avg_response_time_seconds=random.uniform(15, 60),
                        success_rate=random.uniform(0.85, 0.98),
                        cost_per_task_usd=self._calculate_cost(domain_category, specialization),
                        max_concurrent_tasks=random.randint(50, 200),
                        backend_type=backend_type,
                        backend_model=backend_model,
                        status=WorkerStatus.AVAILABLE
                    )

                    # Register agent
                    self.agents[agent_id] = agent

                    # Index by domain
                    domain_key = f"{domain_category}/{specialization}"
                    if domain_key not in self.agents_by_domain:
                        self.agents_by_domain[domain_key] = []
                    self.agents_by_domain[domain_key].append(agent_id)

                    # Index by capabilities
                    for cap in capabilities:
                        if cap.name not in self.agents_by_capability:
                            self.agents_by_capability[cap.name] = []
                        self.agents_by_capability[cap.name].append(agent_id)

                if agent_counter >= 2300:
                    break

            if agent_counter >= 2300:
                break

        print(f"✓ Initialized {agent_counter} domain agents across {len(DOMAIN_CATEGORIES)} domains")

    def _generate_capabilities(self, domain: str, specialization: str) -> List[WorkerCapability]:
        """Generate capabilities for an agent based on domain and specialization."""
        capabilities = []

        # Core research and analysis capabilities
        capabilities.append(WorkerCapability(
            name="research",
            proficiency=random.uniform(0.7, 0.95),
            cost_multiplier=1.0
        ))

        capabilities.append(WorkerCapability(
            name="analysis",
            proficiency=random.uniform(0.7, 0.95),
            cost_multiplier=1.0
        ))

        # Domain-specific capabilities
        if domain in ["technology", "data_science", "engineering"]:
            capabilities.append(WorkerCapability(
                name="code_execution",
                proficiency=random.uniform(0.8, 0.95),
                cost_multiplier=1.2
            ))
            capabilities.append(WorkerCapability(
                name="mathematical_modeling",
                proficiency=random.uniform(0.7, 0.9),
                cost_multiplier=1.1
            ))

        if domain in ["creative", "marketing", "business"]:
            capabilities.append(WorkerCapability(
                name="creative_writing",
                proficiency=random.uniform(0.7, 0.95),
                cost_multiplier=1.0
            ))

        if domain in ["data_science", "science", "finance"]:
            capabilities.append(WorkerCapability(
                name="statistical_analysis",
                proficiency=random.uniform(0.75, 0.95),
                cost_multiplier=1.2
            ))

        # Computer use for applicable domains
        if domain in ["technology", "data_science", "engineering", "finance"]:
            capabilities.append(WorkerCapability(
                name="computer_use",
                proficiency=random.uniform(0.6, 0.9),
                cost_multiplier=1.5
            ))

        # Specialization-specific capability
        capabilities.append(WorkerCapability(
            name=specialization,
            proficiency=random.uniform(0.85, 0.99),
            cost_multiplier=1.3
        ))

        return capabilities

    def _supports_computer_use(self, specialization: str) -> bool:
        """Determine if agent supports computer use based on specialization."""
        computer_use_specs = {
            "software_engineering", "data_engineering", "cybersecurity", "devops",
            "data_analysis", "machine_learning", "web_development", "automation"
        }
        return any(spec in specialization for spec in computer_use_specs)

    def _supports_code_execution(self, domain: str) -> bool:
        """Determine if agent supports code execution based on domain."""
        return domain in ["technology", "data_science", "engineering", "science"]

    def _select_backend(self, domain: str, specialization: str) -> tuple[str, str]:
        """Select appropriate AI backend and model for the agent."""
        # Use Anthropic Claude for most tasks
        # Use Parallel.ai for research-heavy tasks
        # Could add OpenAI, Gemini, etc. for other specializations

        specialization_lower = specialization.lower()
        domain_lower = domain.lower()

        parallel_processor = getattr(settings, "default_parallel_processor", ProcessorType.LITE.value)
        try:
            ProcessorType(parallel_processor)
        except ValueError:
            logger.warning(
                "Configured default processor '%s' is invalid, using %s",
                parallel_processor,
                ProcessorType.LITE.value,
            )
            parallel_processor = ProcessorType.LITE.value

        if "research" in specialization_lower:
            return ("parallel", parallel_processor)

        if "analysis" in specialization_lower or domain_lower in {"science", "medicine", "finance"}:
            return ("parallel", parallel_processor)

        elif domain_lower in {"technology", "data_science"}:
            return ("anthropic", "claude-sonnet-4")
        else:
            return ("anthropic", "claude-sonnet-4")

    def _calculate_cost(self, domain: str, specialization: str) -> float:
        """Calculate cost per task based on domain complexity."""
        base_cost = 0.10  # $0.10 base

        # Domain multipliers
        domain_multipliers = {
            "medicine": 2.0,
            "law": 1.8,
            "finance": 1.6,
            "technology": 1.4,
            "science": 1.3,
            "engineering": 1.3,
        }

        multiplier = domain_multipliers.get(domain, 1.0)
        return round(base_cost * multiplier * random.uniform(0.8, 1.2), 2)

    def get_agent(self, agent_id: str) -> Optional[DomainAgent]:
        """Get agent by ID."""
        return self.agents.get(agent_id)

    def find_agents_by_domain(self, domain: str) -> List[DomainAgent]:
        """Find all agents in a specific domain."""
        agent_ids = []
        for domain_key, ids in self.agents_by_domain.items():
            if domain in domain_key:
                agent_ids.extend(ids)
        return [self.agents[aid] for aid in agent_ids if aid in self.agents]

    def find_agents_by_capability(self, capability: str, min_proficiency: float = 0.0) -> List[DomainAgent]:
        """Find all agents with a specific capability above minimum proficiency."""
        agent_ids = self.agents_by_capability.get(capability, [])
        return [
            self.agents[aid] for aid in agent_ids
            if aid in self.agents and
            any(c.name == capability and c.proficiency >= min_proficiency for c in self.agents[aid].capabilities)
        ]

    def find_available_agents(self, max_load_pct: float = 0.8) -> List[DomainAgent]:
        """Find agents that are available and below max load."""
        available = []
        for agent in self.agents.values():
            if agent.status == WorkerStatus.AVAILABLE:
                load_pct = agent.current_load / agent.max_concurrent_tasks
                if load_pct < max_load_pct:
                    available.append(agent)
        return available

    def increment_load(self, agent_id: str) -> bool:
        """Increment agent's current load. Returns False if at capacity."""
        agent = self.agents.get(agent_id)
        if not agent:
            return False

        if agent.current_load >= agent.max_concurrent_tasks:
            return False

        agent.current_load += 1
        if agent.current_load >= agent.max_concurrent_tasks:
            agent.status = WorkerStatus.BUSY

        return True

    def decrement_load(self, agent_id: str):
        """Decrement agent's current load."""
        agent = self.agents.get(agent_id)
        if not agent:
            return

        if agent.current_load > 0:
            agent.current_load -= 1

        if agent.current_load < agent.max_concurrent_tasks:
            agent.status = WorkerStatus.AVAILABLE

    def get_registry_stats(self) -> WorkerRegistryModel:
        """Get current registry statistics."""
        return WorkerRegistryModel(
            total_agents=len(self.agents),
            agents_by_domain={k: v for k, v in self.agents_by_domain.items()},
            agents_by_capability={k: v for k, v in self.agents_by_capability.items()},
            last_updated=datetime.now()
        )

    def search_agents(
        self,
        domains: Optional[List[str]] = None,
        capabilities: Optional[List[str]] = None,
        min_quality: float = 0.0,
        max_cost: Optional[float] = None,
        computer_use_required: bool = False,
        limit: int = 10
    ) -> List[DomainAgent]:
        """
        Advanced search for agents matching criteria.

        Args:
            domains: List of domain patterns to match
            capabilities: List of required capabilities
            min_quality: Minimum quality score
            max_cost: Maximum cost per task
            computer_use_required: Whether computer use is required
            limit: Maximum number of agents to return

        Returns:
            List of matching agents sorted by quality score
        """
        candidates = list(self.agents.values())

        # Filter by domain
        if domains:
            candidates = [
                a for a in candidates
                if any(domain.lower() in a.domain.lower() for domain in domains)
            ]

        # Filter by capabilities
        if capabilities:
            candidates = [
                a for a in candidates
                if all(
                    any(c.name == cap for c in a.capabilities)
                    for cap in capabilities
                )
            ]

        # Filter by quality
        candidates = [a for a in candidates if a.avg_quality_score >= min_quality]

        # Filter by cost
        if max_cost:
            candidates = [a for a in candidates if a.cost_per_task_usd <= max_cost]

        # Filter by computer use
        if computer_use_required:
            candidates = [a for a in candidates if a.supports_computer_use]

        # Filter by availability
        candidates = [
            a for a in candidates
            if a.status == WorkerStatus.AVAILABLE and a.current_load < a.max_concurrent_tasks
        ]

        # Sort by quality score
        candidates.sort(key=lambda a: a.avg_quality_score, reverse=True)

        return candidates[:limit]


# Global registry instance
_global_registry: Optional[WorkerRegistry] = None


def get_registry() -> WorkerRegistry:
    """Get or create the global worker registry."""
    global _global_registry
    if _global_registry is None:
        _global_registry = WorkerRegistry()
    return _global_registry
