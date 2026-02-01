"""
Tool Contracts - Enforced standards for all tools.

Every tool in the registry MUST support these base contracts.
This ensures consistent behavior, budgeting, and resource management.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, TypeVar, Generic, Union
from pydantic import BaseModel, Field, field_validator, model_validator


# =============================================================================
# Budget Specifications
# =============================================================================

class BudgetType(str, Enum):
    """Types of budget constraints."""
    TOKENS = "tokens"           # Token count limit
    TIME = "time"               # Wall-clock time limit
    COST = "cost"               # Monetary cost limit
    CALLS = "calls"             # Number of sub-calls/iterations
    DEPTH = "depth"             # Recursion/nesting depth
    MEMORY = "memory"           # Memory usage limit
    BANDWIDTH = "bandwidth"     # Network bandwidth limit


class BudgetUnit(str, Enum):
    """Units for budget measurements."""
    # Token units
    TOKENS = "tokens"
    K_TOKENS = "k_tokens"       # 1000 tokens

    # Time units
    MILLISECONDS = "ms"
    SECONDS = "s"
    MINUTES = "m"

    # Cost units
    USD_CENTS = "cents"
    USD = "usd"
    CREDITS = "credits"

    # Size units
    BYTES = "bytes"
    KB = "kb"
    MB = "mb"


class BudgetConstraint(BaseModel):
    """A single budget constraint."""
    type: BudgetType
    limit: float = Field(..., gt=0, description="Maximum allowed value")
    unit: BudgetUnit
    soft_limit: Optional[float] = Field(default=None, description="Warning threshold (< limit)")
    priority: int = Field(default=1, ge=1, le=10, description="Priority when multiple constraints conflict")

    @field_validator('soft_limit')
    @classmethod
    def soft_must_be_less_than_limit(cls, v, info):
        if v is not None and 'limit' in info.data and v >= info.data['limit']:
            raise ValueError('soft_limit must be less than limit')
        return v


class TokenBudget(BaseModel):
    """Token budget specification."""
    max_tokens: int = Field(default=4096, ge=1, le=1_000_000, description="Maximum tokens for response")
    max_input_tokens: Optional[int] = Field(default=None, description="Maximum tokens in input")
    max_output_tokens: Optional[int] = Field(default=None, description="Maximum tokens in output")
    reserve_tokens: int = Field(default=0, ge=0, description="Tokens to reserve for system use")

    @property
    def effective_max(self) -> int:
        """Effective maximum after reservations."""
        return self.max_tokens - self.reserve_tokens


class TimeBudget(BaseModel):
    """Time budget specification."""
    timeout_ms: int = Field(default=30_000, ge=100, le=600_000, description="Hard timeout in milliseconds")
    soft_timeout_ms: Optional[int] = Field(default=None, description="Soft timeout for warnings")
    min_time_ms: int = Field(default=0, ge=0, description="Minimum execution time (for rate limiting)")

    # Sequential/streaming time budgets
    first_token_timeout_ms: Optional[int] = Field(default=None, description="Max time to first token/chunk")
    inter_chunk_timeout_ms: Optional[int] = Field(default=None, description="Max time between chunks")

    @property
    def timeout(self) -> timedelta:
        return timedelta(milliseconds=self.timeout_ms)


class CostBudget(BaseModel):
    """Cost budget specification."""
    max_cost_cents: int = Field(default=100, ge=0, description="Maximum cost in cents")
    warn_at_cents: Optional[int] = Field(default=None, description="Warning threshold")

    # Per-resource costs for estimation
    cost_per_1k_input_tokens: float = Field(default=0.0, ge=0)
    cost_per_1k_output_tokens: float = Field(default=0.0, ge=0)
    cost_per_call: float = Field(default=0.0, ge=0)


class CallBudget(BaseModel):
    """Call/iteration budget specification."""
    max_calls: int = Field(default=10, ge=1, le=1000, description="Maximum sub-calls or iterations")
    max_depth: int = Field(default=5, ge=1, le=100, description="Maximum recursion/nesting depth")
    max_parallel: int = Field(default=10, ge=1, le=100, description="Maximum parallel operations")
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts")


class ResourceBudget(BaseModel):
    """Resource usage budget."""
    max_memory_mb: Optional[int] = Field(default=None, ge=1, description="Maximum memory in MB")
    max_bandwidth_mbps: Optional[float] = Field(default=None, ge=0.1, description="Maximum bandwidth in Mbps")
    max_file_size_mb: Optional[int] = Field(default=None, ge=1, description="Maximum file size in MB")


# =============================================================================
# Execution Context
# =============================================================================

class ExecutionPriority(str, Enum):
    """Execution priority levels."""
    CRITICAL = "critical"       # Highest priority, minimal constraints
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"
    BACKGROUND = "background"   # Lowest priority, strictest constraints


class ExecutionMode(str, Enum):
    """Execution mode affecting behavior."""
    SYNC = "sync"               # Synchronous, wait for result
    ASYNC = "async"             # Asynchronous, return immediately
    STREAM = "stream"           # Streaming response
    BATCH = "batch"             # Batch processing mode


# =============================================================================
# Standard Tool Input Contract
# =============================================================================

class ToolInputBase(BaseModel):
    """
    Base input contract that ALL tools must accept.

    Tools extend this with their specific parameters.
    These fields are ALWAYS available and MUST be respected.
    """

    # === Token Budget (REQUIRED for all tools) ===
    max_tokens: int = Field(
        default=4096,
        ge=1,
        le=1_000_000,
        description="Maximum tokens for this execution"
    )

    # === Time Budget ===
    timeout_ms: int = Field(
        default=30_000,
        ge=100,
        le=600_000,
        description="Timeout in milliseconds"
    )

    # === Cost Budget ===
    max_cost_cents: Optional[int] = Field(
        default=None,
        ge=0,
        description="Maximum cost in cents (None = no limit)"
    )

    # === Execution Control ===
    priority: ExecutionPriority = Field(
        default=ExecutionPriority.NORMAL,
        description="Execution priority"
    )

    mode: ExecutionMode = Field(
        default=ExecutionMode.SYNC,
        description="Execution mode"
    )

    # === Call Budget ===
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum retry attempts on failure"
    )

    max_iterations: Optional[int] = Field(
        default=None,
        ge=1,
        description="Maximum iterations for iterative tools"
    )

    # === Streaming Budget ===
    first_chunk_timeout_ms: Optional[int] = Field(
        default=5000,
        ge=100,
        description="Max time to first chunk in streaming mode"
    )

    # === Caching ===
    use_cache: bool = Field(
        default=True,
        description="Whether to use cached results if available"
    )

    cache_ttl_seconds: Optional[int] = Field(
        default=None,
        ge=0,
        description="Cache TTL override (None = use default)"
    )

    # === Tracing/Observability ===
    trace_id: Optional[str] = Field(
        default=None,
        description="Trace ID for distributed tracing"
    )

    span_id: Optional[str] = Field(
        default=None,
        description="Span ID for distributed tracing"
    )

    # === Metadata ===
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata passed through execution"
    )

    class Config:
        extra = "allow"  # Allow additional fields for tool-specific params


# =============================================================================
# Standard Tool Output Contract
# =============================================================================

class UsageMetrics(BaseModel):
    """Metrics about resource usage during execution."""
    tokens_used: Optional[int] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None

    duration_ms: float = 0
    time_to_first_token_ms: Optional[float] = None

    cost_cents: Optional[float] = None

    calls_made: int = 0
    retries: int = 0
    cache_hit: bool = False

    memory_peak_mb: Optional[float] = None


class ToolOutputBase(BaseModel):
    """
    Base output contract that ALL tools must return.

    Tools extend this with their specific result fields.
    """

    # === Status ===
    success: bool = Field(..., description="Whether execution succeeded")

    error: Optional[str] = Field(
        default=None,
        description="Error message if failed"
    )

    error_code: Optional[str] = Field(
        default=None,
        description="Machine-readable error code"
    )

    # === Result ===
    result: Any = Field(
        default=None,
        description="The actual result data"
    )

    # === Usage/Budget Tracking ===
    usage: UsageMetrics = Field(
        default_factory=UsageMetrics,
        description="Resource usage metrics"
    )

    # === Budget Warnings ===
    budget_warnings: List[str] = Field(
        default_factory=list,
        description="Warnings about approaching budget limits"
    )

    truncated: bool = Field(
        default=False,
        description="Whether result was truncated due to limits"
    )

    # === Tracing ===
    trace_id: Optional[str] = None
    span_id: Optional[str] = None

    # === Timing ===
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def duration_ms(self) -> float:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds() * 1000
        return self.usage.duration_ms


# =============================================================================
# Budget Enforcement
# =============================================================================

class BudgetEnforcer:
    """
    Enforces budget constraints during tool execution.

    Usage:
        enforcer = BudgetEnforcer(input_contract)

        # Check before operations
        if not enforcer.can_continue():
            return enforcer.budget_exceeded_response()

        # Track usage
        enforcer.add_tokens(100)
        enforcer.add_time_ms(50)

        # Get warnings
        warnings = enforcer.get_warnings()
    """

    def __init__(self, input: ToolInputBase):
        self.input = input
        self.tokens_used = 0
        self.time_used_ms = 0
        self.cost_used_cents = 0.0
        self.calls_made = 0
        self.start_time = datetime.utcnow()
        self._warnings: List[str] = []

    def add_tokens(self, count: int) -> bool:
        """Add token usage. Returns False if budget exceeded."""
        self.tokens_used += count
        if self.tokens_used >= self.input.max_tokens:
            return False
        if self.tokens_used >= self.input.max_tokens * 0.9:
            self._warnings.append(f"Token usage at {self.tokens_used}/{self.input.max_tokens} (90%+)")
        return True

    def add_time_ms(self, ms: float) -> bool:
        """Add time usage. Returns False if budget exceeded."""
        self.time_used_ms += ms
        if self.time_used_ms >= self.input.timeout_ms:
            return False
        if self.time_used_ms >= self.input.timeout_ms * 0.8:
            self._warnings.append(f"Time usage at {self.time_used_ms:.0f}/{self.input.timeout_ms}ms (80%+)")
        return True

    def add_cost_cents(self, cents: float) -> bool:
        """Add cost. Returns False if budget exceeded."""
        if self.input.max_cost_cents is None:
            return True
        self.cost_used_cents += cents
        if self.cost_used_cents >= self.input.max_cost_cents:
            return False
        return True

    def add_call(self) -> bool:
        """Add a sub-call. Returns False if budget exceeded."""
        self.calls_made += 1
        if self.input.max_iterations and self.calls_made >= self.input.max_iterations:
            return False
        return True

    def elapsed_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        return (datetime.utcnow() - self.start_time).total_seconds() * 1000

    def can_continue(self) -> bool:
        """Check if execution can continue within all budgets."""
        if self.tokens_used >= self.input.max_tokens:
            return False
        if self.elapsed_ms() >= self.input.timeout_ms:
            return False
        if self.input.max_cost_cents and self.cost_used_cents >= self.input.max_cost_cents:
            return False
        if self.input.max_iterations and self.calls_made >= self.input.max_iterations:
            return False
        return True

    def remaining_tokens(self) -> int:
        """Get remaining token budget."""
        return max(0, self.input.max_tokens - self.tokens_used)

    def remaining_time_ms(self) -> float:
        """Get remaining time budget."""
        return max(0, self.input.timeout_ms - self.elapsed_ms())

    def get_warnings(self) -> List[str]:
        """Get budget warnings."""
        return self._warnings.copy()

    def get_usage(self) -> UsageMetrics:
        """Get current usage metrics."""
        return UsageMetrics(
            tokens_used=self.tokens_used,
            duration_ms=self.elapsed_ms(),
            cost_cents=self.cost_used_cents if self.cost_used_cents > 0 else None,
            calls_made=self.calls_made,
        )

    def budget_exceeded_response(self, partial_result: Any = None) -> ToolOutputBase:
        """Generate response for budget exceeded."""
        reason = []
        if self.tokens_used >= self.input.max_tokens:
            reason.append(f"token limit ({self.input.max_tokens})")
        if self.elapsed_ms() >= self.input.timeout_ms:
            reason.append(f"timeout ({self.input.timeout_ms}ms)")
        if self.input.max_cost_cents and self.cost_used_cents >= self.input.max_cost_cents:
            reason.append(f"cost limit ({self.input.max_cost_cents} cents)")

        return ToolOutputBase(
            success=False,
            error=f"Budget exceeded: {', '.join(reason)}",
            error_code="BUDGET_EXCEEDED",
            result=partial_result,
            usage=self.get_usage(),
            budget_warnings=self._warnings,
            truncated=partial_result is not None,
        )


# =============================================================================
# Contract Validation
# =============================================================================

def validate_tool_input(input_data: Dict[str, Any]) -> ToolInputBase:
    """
    Validate that input data conforms to the base contract.

    Raises ValidationError if contract is violated.
    """
    return ToolInputBase.model_validate(input_data)


def ensure_base_fields(input_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure all base contract fields have values (use defaults if missing).

    This allows tools to receive partial input while guaranteeing
    all budget fields are present.
    """
    base = ToolInputBase()
    merged = base.model_dump()
    merged.update(input_data)
    return merged


# =============================================================================
# Type Helpers
# =============================================================================

T_Input = TypeVar("T_Input", bound=ToolInputBase)
T_Output = TypeVar("T_Output", bound=ToolOutputBase)


class TypedTool(Generic[T_Input, T_Output], ABC):
    """
    Base class for typed tools with enforced contracts.

    Usage:
        class SearchInput(ToolInputBase):
            query: str
            num_results: int = 10

        class SearchOutput(ToolOutputBase):
            results: List[SearchResult]

        class SearchTool(TypedTool[SearchInput, SearchOutput]):
            async def execute(self, input: SearchInput) -> SearchOutput:
                enforcer = BudgetEnforcer(input)
                ...
    """

    @abstractmethod
    async def execute(self, input: T_Input) -> T_Output:
        """Execute the tool with typed input/output."""
        pass
