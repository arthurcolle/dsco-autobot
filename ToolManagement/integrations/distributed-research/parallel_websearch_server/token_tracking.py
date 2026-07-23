"""Token tracking system for context window visibility.

This module provides comprehensive token counting for all requests/responses
to give visibility into context window consumption.

Features:
- Multi-model token counting (Claude, GPT, fallback)
- Request/response token tracking
- Integration with audit logging
- Metrics collection for histograms/counters
- Automatic size bucketing (small/medium/large/huge)
"""

import json
import logging
from typing import Any, Dict, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)

# Try to import tokenizers
try:
    import tiktoken
    HAS_TIKTOKEN = True
except ImportError:
    HAS_TIKTOKEN = False
    logger.warning("tiktoken not installed, token counting will use character estimates")

try:
    from anthropic import Anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False
    logger.warning("anthropic not installed, Claude token counting will use estimates")


class TokenModel(str, Enum):
    """Supported token models."""
    CLAUDE = "claude"
    GPT4 = "gpt-4"
    GPT35 = "gpt-3.5-turbo"
    FALLBACK = "fallback"


class SizeBucket(str, Enum):
    """Size categorization for requests."""
    TINY = "tiny"           # < 500 tokens
    SMALL = "small"         # 500-2K tokens
    MEDIUM = "medium"       # 2K-10K tokens
    LARGE = "large"         # 10K-50K tokens
    HUGE = "huge"           # 50K-100K tokens
    MASSIVE = "massive"     # > 100K tokens


class TokenCounter:
    """Token counter with multi-model support."""

    def __init__(self):
        """Initialize token counter with available encoders."""
        self.gpt4_encoder = None
        self.gpt35_encoder = None
        self.anthropic_client = None

        if HAS_TIKTOKEN:
            try:
                self.gpt4_encoder = tiktoken.encoding_for_model("gpt-4")
                self.gpt35_encoder = tiktoken.encoding_for_model("gpt-3.5-turbo")
                logger.info("Initialized GPT tokenizers")
            except Exception as e:
                logger.warning(f"Failed to initialize tiktoken encoders: {e}")

        if HAS_ANTHROPIC:
            try:
                self.anthropic_client = Anthropic()
                logger.info("Initialized Anthropic client for token counting")
            except Exception as e:
                logger.warning(f"Failed to initialize Anthropic client: {e}")

    def count_tokens(
        self,
        text: str,
        model: TokenModel = TokenModel.CLAUDE
    ) -> int:
        """Count tokens in text using specified model.

        Args:
            text: Text to count tokens in
            model: Token model to use (claude, gpt-4, gpt-3.5-turbo, fallback)

        Returns:
            Estimated token count
        """
        if not text:
            return 0

        # Try model-specific counting
        if model == TokenModel.CLAUDE and self.anthropic_client:
            return self._count_claude_tokens(text)
        elif model == TokenModel.GPT4 and self.gpt4_encoder:
            return len(self.gpt4_encoder.encode(text))
        elif model == TokenModel.GPT35 and self.gpt35_encoder:
            return len(self.gpt35_encoder.encode(text))

        # Fallback to character-based estimation
        return self._estimate_tokens_from_chars(text)

    def _count_claude_tokens(self, text: str) -> int:
        """Count tokens using Claude-compatible estimation.

        Claude uses a similar tokenizer to GPT-4, so we use GPT-4's
        tokenizer as a close approximation when available.
        """
        if self.gpt4_encoder:
            return len(self.gpt4_encoder.encode(text))
        return self._estimate_tokens_from_chars(text)

    def _estimate_tokens_from_chars(self, text: str) -> int:
        """Fallback token estimation from character count.

        Claude: ~3.5 chars per token
        GPT: ~4 chars per token

        We use 3.5 as a conservative estimate.
        """
        return int(len(text) / 3.5)

    def count_json_tokens(
        self,
        data: Any,
        model: TokenModel = TokenModel.CLAUDE
    ) -> int:
        """Count tokens in JSON-serializable data.

        Args:
            data: Data to serialize and count
            model: Token model to use

        Returns:
            Estimated token count
        """
        if data is None:
            return 0

        try:
            if isinstance(data, (str, bytes)):
                text = data if isinstance(data, str) else data.decode('utf-8', errors='replace')
            else:
                # Serialize to compact JSON
                text = json.dumps(data, separators=(',', ':'), ensure_ascii=False)

            return self.count_tokens(text, model)
        except Exception as e:
            logger.warning(f"Failed to count tokens for JSON data: {e}")
            return 0

    def categorize_size(self, token_count: int) -> SizeBucket:
        """Categorize token count into size bucket.

        Args:
            token_count: Number of tokens

        Returns:
            Size bucket classification
        """
        if token_count < 500:
            return SizeBucket.TINY
        elif token_count < 2000:
            return SizeBucket.SMALL
        elif token_count < 10000:
            return SizeBucket.MEDIUM
        elif token_count < 50000:
            return SizeBucket.LARGE
        elif token_count < 100000:
            return SizeBucket.HUGE
        else:
            return SizeBucket.MASSIVE


class TokenMetrics:
    """Token metrics for a single request/response pair."""

    def __init__(
        self,
        request_tokens: int,
        response_tokens: int,
        request_bytes: int,
        response_bytes: int,
        model: TokenModel = TokenModel.CLAUDE
    ):
        """Initialize token metrics.

        Args:
            request_tokens: Tokens in request body
            response_tokens: Tokens in response body
            request_bytes: Bytes in request body
            response_bytes: Bytes in response body
            model: Token model used for counting
        """
        self.request_tokens = request_tokens
        self.response_tokens = response_tokens
        self.total_tokens = request_tokens + response_tokens
        self.request_bytes = request_bytes
        self.response_bytes = response_bytes
        self.total_bytes = request_bytes + response_bytes
        self.model = model

        # Categorize size
        counter = TokenCounter()
        self.size_bucket = counter.categorize_size(self.total_tokens)
        self.request_size_bucket = counter.categorize_size(request_tokens)
        self.response_size_bucket = counter.categorize_size(response_tokens)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage/serialization."""
        return {
            "request_tokens": self.request_tokens,
            "response_tokens": self.response_tokens,
            "total_tokens": self.total_tokens,
            "request_bytes": self.request_bytes,
            "response_bytes": self.response_bytes,
            "total_bytes": self.total_bytes,
            "model": self.model.value,
            "size_bucket": self.size_bucket.value,
            "request_size_bucket": self.request_size_bucket.value,
            "response_size_bucket": self.response_size_bucket.value,
        }


# Global token counter instance
_token_counter: Optional[TokenCounter] = None


def get_token_counter() -> TokenCounter:
    """Get or create global token counter instance."""
    global _token_counter
    if _token_counter is None:
        _token_counter = TokenCounter()
    return _token_counter


def count_request_response_tokens(
    request_body: Any,
    response_body: Any,
    model: TokenModel = TokenModel.CLAUDE
) -> TokenMetrics:
    """Count tokens in request and response bodies.

    Args:
        request_body: Request body (str, bytes, dict, list, or None)
        response_body: Response body (str, bytes, dict, list, or None)
        model: Token model to use for counting

    Returns:
        TokenMetrics with detailed token/byte counts
    """
    counter = get_token_counter()

    # Count request tokens
    request_tokens = counter.count_json_tokens(request_body, model)
    request_bytes = _calculate_bytes(request_body)

    # Count response tokens
    response_tokens = counter.count_json_tokens(response_body, model)
    response_bytes = _calculate_bytes(response_body)

    return TokenMetrics(
        request_tokens=request_tokens,
        response_tokens=response_tokens,
        request_bytes=request_bytes,
        response_bytes=response_bytes,
        model=model
    )


def _calculate_bytes(data: Any) -> int:
    """Calculate byte size of data."""
    if data is None:
        return 0

    try:
        if isinstance(data, bytes):
            return len(data)
        elif isinstance(data, str):
            return len(data.encode('utf-8'))
        else:
            # Serialize to JSON and measure
            json_str = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
            return len(json_str.encode('utf-8'))
    except Exception as e:
        logger.warning(f"Failed to calculate bytes: {e}")
        return 0


async def record_token_metrics_async(
    metrics: TokenMetrics,
    endpoint: str,
    operation_type: str
) -> None:
    """Record token metrics to the telemetry system (async).

    Args:
        metrics: TokenMetrics object with token counts
        endpoint: API endpoint path
        operation_type: Type of operation (search, task.run, etc.)
    """
    try:
        from parallel_websearch_server.telemetry.metrics import get_metrics_registry

        registry = get_metrics_registry()

        # Record total tokens histogram
        total_tokens_histogram = await registry.get_or_create_histogram(
            "parallel_request_total_tokens",
            "Total tokens (request + response) per API call",
            buckets=[100, 500, 1000, 2000, 5000, 10000, 25000, 50000, 100000, float('inf')],
            labels=["endpoint", "operation_type", "size_bucket"]
        )
        await total_tokens_histogram.observe(
            float(metrics.total_tokens),
            label_values={
                "endpoint": endpoint,
                "operation_type": operation_type,
                "size_bucket": metrics.size_bucket.value
            }
        )

        # Record request tokens histogram
        request_tokens_histogram = await registry.get_or_create_histogram(
            "parallel_request_input_tokens",
            "Input tokens per API call",
            buckets=[100, 500, 1000, 2000, 5000, 10000, 25000, 50000, 100000, float('inf')],
            labels=["endpoint", "operation_type"]
        )
        await request_tokens_histogram.observe(
            float(metrics.request_tokens),
            label_values={
                "endpoint": endpoint,
                "operation_type": operation_type
            }
        )

        # Record response tokens histogram
        response_tokens_histogram = await registry.get_or_create_histogram(
            "parallel_request_output_tokens",
            "Output tokens per API call",
            buckets=[100, 500, 1000, 2000, 5000, 10000, 25000, 50000, 100000, float('inf')],
            labels=["endpoint", "operation_type"]
        )
        await response_tokens_histogram.observe(
            float(metrics.response_tokens),
            label_values={
                "endpoint": endpoint,
                "operation_type": operation_type
            }
        )

        # Increment total tokens counter
        total_tokens_counter = await registry.get_or_create_counter(
            "parallel_tokens_total",
            "Cumulative total tokens processed",
            labels=["operation_type"]
        )
        await total_tokens_counter.inc(
            float(metrics.total_tokens),
            label_values={"operation_type": operation_type}
        )

        # Increment requests by size bucket
        size_bucket_counter = await registry.get_or_create_counter(
            "parallel_requests_by_size",
            "Number of requests by token size bucket",
            labels=["size_bucket", "operation_type"]
        )
        await size_bucket_counter.inc(
            1.0,
            label_values={
                "size_bucket": metrics.size_bucket.value,
                "operation_type": operation_type
            }
        )

    except Exception as e:
        logger.warning(f"Failed to record token metrics: {e}")


__all__ = [
    "TokenCounter",
    "TokenMetrics",
    "TokenModel",
    "SizeBucket",
    "get_token_counter",
    "count_request_response_tokens",
    "record_token_metrics_async",
]
