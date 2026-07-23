"""Data models for the Matching Engine - connecting jobs with AI/human workers."""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel, Field, validator


# ============================================================================
# Job Models
# ============================================================================

class JobType(str, Enum):
    """Types of jobs the matching engine can handle."""
    RESEARCH = "research"
    DATA_LABELING = "data_labeling"
    CONTENT_CREATION = "content_creation"
    ANALYSIS = "analysis"
    CODE_GENERATION = "code_generation"
    COMPUTER_USE = "computer_use"
    GENERAL = "general"


class JobPriority(str, Enum):
    """Job priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class JobStatus(str, Enum):
    """Job lifecycle status."""
    PENDING = "pending"
    QUEUED = "queued"
    MATCHED = "matched"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobRequirements(BaseModel):
    """Requirements and constraints for a job."""
    min_quality_score: Optional[float] = Field(None, ge=0, le=1, description="Minimum quality threshold (0-1)")
    max_cost_usd: Optional[float] = Field(None, ge=0, description="Maximum cost in USD")
    max_duration_seconds: Optional[int] = Field(None, ge=1, description="Maximum time allowed")
    required_capabilities: Optional[List[str]] = Field(default_factory=list, description="Required worker capabilities")
    preferred_domains: Optional[List[str]] = Field(default_factory=list, description="Preferred knowledge domains")
    allow_multi_agent: bool = Field(default=True, description="Allow multiple agents to collaborate")
    computer_use_required: bool = Field(default=False, description="Requires computer use capability")


class JobInput(BaseModel):
    """Input specification for a job."""
    description: str = Field(..., description="Natural language description of the task")
    job_type: JobType
    priority: JobPriority = JobPriority.NORMAL
    requirements: Optional[JobRequirements] = None
    context: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional context data")
    output_schema: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    notification_channel: Optional[str] = Field(
        default=None,
        description="Mailbox channel to receive asynchronous job notifications",
        max_length=128,
    )


class JobSubmission(BaseModel):
    """Job submission response."""
    job_id: str
    status: JobStatus
    created_at: datetime
    estimated_cost_usd: Optional[float] = None
    estimated_duration_seconds: Optional[int] = None
    matched_workers: Optional[List[str]] = None


class JobResult(BaseModel):
    """Result from a completed job."""
    job_id: str
    status: JobStatus
    output: Dict[str, Any]
    worker_id: str
    quality_score: Optional[float] = None
    actual_cost_usd: Optional[float] = None
    actual_duration_seconds: Optional[int] = None
    completed_at: datetime
    metadata: Optional[Dict[str, Any]] = None


# ============================================================================
# Worker Models
# ============================================================================

class WorkerType(str, Enum):
    """Type of worker."""
    DOMAIN_AGENT = "domain_agent"
    GENERAL_AGENT = "general_agent"
    HUMAN = "human"
    HYBRID = "hybrid"


class WorkerStatus(str, Enum):
    """Worker availability status."""
    AVAILABLE = "available"
    BUSY = "busy"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"


class WorkerCapability(BaseModel):
    """A specific capability of a worker."""
    name: str
    proficiency: float = Field(..., ge=0, le=1, description="Proficiency level (0-1)")
    cost_multiplier: float = Field(default=1.0, ge=0, description="Cost adjustment for this capability")


class DomainAgent(BaseModel):
    """A specialized domain agent with specific knowledge and capabilities."""
    agent_id: str
    name: str
    domain: str = Field(..., description="Primary knowledge domain (e.g., 'finance', 'medicine', 'law')")
    description: str
    capabilities: List[WorkerCapability]
    supports_computer_use: bool = Field(default=False)
    supports_web_search: bool = Field(default=True)
    supports_code_execution: bool = Field(default=False)

    # Performance metrics
    avg_quality_score: float = Field(default=0.8, ge=0, le=1)
    avg_response_time_seconds: float = Field(default=30.0, ge=0)
    success_rate: float = Field(default=0.95, ge=0, le=1)
    total_jobs_completed: int = Field(default=0, ge=0)

    # Cost and capacity
    cost_per_task_usd: float = Field(default=0.10, ge=0)
    max_concurrent_tasks: int = Field(default=100, ge=1)
    current_load: int = Field(default=0, ge=0)

    # Backend configuration
    backend_type: str = Field(default="anthropic", description="AI backend (anthropic, parallel, openai, etc.)")
    backend_model: str = Field(default="claude-sonnet-4")
    mcp_servers: Optional[List[str]] = Field(default_factory=list, description="Associated MCP server endpoints")

    status: WorkerStatus = WorkerStatus.AVAILABLE
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

    @validator('current_load')
    def validate_load(cls, v, values):
        """Ensure current load doesn't exceed capacity."""
        if 'max_concurrent_tasks' in values and v > values['max_concurrent_tasks']:
            raise ValueError(f"Current load {v} exceeds capacity {values['max_concurrent_tasks']}")
        return v


class WorkerRegistry(BaseModel):
    """Registry of all available workers."""
    total_agents: int
    agents_by_domain: Dict[str, List[str]] = Field(default_factory=dict)
    agents_by_capability: Dict[str, List[str]] = Field(default_factory=dict)
    last_updated: datetime


# ============================================================================
# Matching Models
# ============================================================================

class MatchScore(BaseModel):
    """Score for a job-worker match."""
    worker_id: str
    overall_score: float = Field(..., ge=0, le=1)
    capability_match: float = Field(..., ge=0, le=1)
    domain_match: float = Field(..., ge=0, le=1)
    availability_score: float = Field(..., ge=0, le=1)
    cost_score: float = Field(..., ge=0, le=1)
    quality_score: float = Field(..., ge=0, le=1)
    explanation: Optional[str] = None


class MatchResult(BaseModel):
    """Result of matching a job to workers."""
    job_id: str
    matches: List[MatchScore]
    selected_worker_id: str
    reasoning: str
    estimated_cost_usd: float
    estimated_duration_seconds: int
    matched_at: datetime


# ============================================================================
# Execution Models
# ============================================================================

class ExecutionBackend(str, Enum):
    """Available execution backends."""
    ANTHROPIC = "anthropic"
    PARALLEL = "parallel"
    OPENAI = "openai"
    CUSTOM_MCP = "custom_mcp"


class ExecutionRequest(BaseModel):
    """Request to execute a job on a specific worker."""
    job_id: str
    worker_id: str
    backend: ExecutionBackend
    input_data: Dict[str, Any]
    timeout_seconds: Optional[int] = None
    enable_streaming: bool = False


class ExecutionStatus(BaseModel):
    """Status of a job execution."""
    job_id: str
    worker_id: str
    status: JobStatus
    progress_pct: Optional[float] = Field(None, ge=0, le=100)
    current_step: Optional[str] = None
    started_at: datetime
    updated_at: datetime
    result: Optional[JobResult] = None


# ============================================================================
# Analytics Models
# ============================================================================

class WorkerMetrics(BaseModel):
    """Performance metrics for a worker."""
    worker_id: str
    period_start: datetime
    period_end: datetime
    total_jobs: int
    completed_jobs: int
    failed_jobs: int
    avg_quality_score: float
    avg_response_time: float
    total_revenue_usd: float
    utilization_rate: float = Field(..., ge=0, le=1)


class SystemMetrics(BaseModel):
    """Overall system metrics."""
    timestamp: datetime
    total_jobs_pending: int
    total_jobs_in_progress: int
    total_jobs_completed_today: int
    total_workers_available: int
    total_workers_busy: int
    avg_match_time_ms: float
    avg_job_duration_seconds: float
    total_cost_today_usd: float
