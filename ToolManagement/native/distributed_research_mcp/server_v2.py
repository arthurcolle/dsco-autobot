#!/usr/bin/env python3
"""
Distributed Research MCP v2 - Parallel AI Integration.

This MCP provides deep research and task execution capabilities
powered by Parallel AI's API (https://api.parallel.ai).

Capabilities:
- Web search with structured extraction
- Autonomous task execution (lite, turbo, premium processors)
- Task groups for parallel batch processing
- FindAll for entity prospecting
- Web monitors for scheduled tracking

Run with:
    python -m native.distributed_research_mcp.server_v2 --port 4002

Environment:
    PARALLEL_API_KEY: Parallel AI API key (required for full functionality)
    REGISTRY_URL: Central registry URL (optional)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

import httpx

from native.base import NativeMCPBase, CapabilityType, EmbeddingMixin
from .storage import ResearchStorage
from .dataframes import DataFrameStorage

logger = logging.getLogger(__name__)


# =============================================================================
# MODELS
# =============================================================================

class ProcessorType(str, Enum):
    """Parallel AI processor types."""
    LITE = "lite"
    TURBO = "turbo"
    PREMIUM = "premium"


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StrategyType(str, Enum):
    COST = "cost"
    SPEED = "speed"
    BALANCED = "balanced"
    QUALITY = "quality"


@dataclass
class LocalTask:
    """Local task tracking (mirrors Parallel AI tasks)."""
    id: str
    parallel_id: Optional[str] = None  # Parallel AI's task ID
    processor: ProcessorType = ProcessorType.LITE
    input: str = ""
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None


# =============================================================================
# RESEARCH MCP - PARALLEL AI INTEGRATION
# =============================================================================

class DistributedResearchMCPv2(NativeMCPBase, EmbeddingMixin):
    """
    Native MCP server for distributed research capabilities.

    Powered by Parallel AI's API for:
    - Deep web search with AI synthesis
    - Autonomous task execution
    - Parallel batch processing
    - Entity prospecting
    - Real-time web monitoring
    """

    SERVICE_ID = "distributed-research-mcp"
    SERVICE_NAME = "Distributed Research MCP"
    VERSION = "2.0.0"

    # Parallel AI API
    PARALLEL_API_URL = "https://api.parallel.ai"

    # Capabilities provided
    CAPABILITIES = [
        CapabilityType.TASK,
        CapabilityType.EXECUTE,
        "research",
        "synthesis",
        "parallel_ai",
    ]

    # We can use embeddings if available (for semantic search of local tasks)
    REQUIRES = [CapabilityType.EMBED]
    DEPENDS = []

    # Tool definitions matching Parallel AI's API
    TOOLS = [
        {
            "name": "deep_search",
            "description": "Search the web and extract structured information using Parallel AI. Returns synthesized results with sources.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "objective": {
                        "type": "string",
                        "description": "Research objective or search query"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum number of results (1-20)",
                        "default": 10
                    }
                },
                "required": ["objective"]
            }
        },
        {
            "name": "extract_data",
            "description": "Extract structured data from a URL based on a JSON schema using Parallel AI.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "url": {
                        "type": "string",
                        "description": "URL to extract data from"
                    },
                    "schema": {
                        "type": "object",
                        "description": "JSON schema defining what to extract"
                    },
                    "instructions": {
                        "type": "string",
                        "description": "Additional extraction instructions"
                    }
                },
                "required": ["url"]
            }
        },
        {
            "name": "run_task",
            "description": "Execute an autonomous task using Parallel AI's processors (lite, turbo, premium).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "input": {
                        "type": "string",
                        "description": "Task input/prompt"
                    },
                    "processor": {
                        "type": "string",
                        "enum": ["lite", "turbo", "premium"],
                        "description": "Processor type (lite=fast/cheap, turbo=balanced, premium=best quality)",
                        "default": "lite"
                    },
                    "output_schema": {
                        "type": "object",
                        "description": "Optional JSON schema for structured output"
                    },
                    "instructions": {
                        "type": "string",
                        "description": "Additional task instructions"
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds (max 600)",
                        "default": 300
                    }
                },
                "required": ["input"]
            }
        },
        {
            "name": "create_task_group",
            "description": "Create a batch of tasks to run in parallel via Parallel AI.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Group name for tracking"
                    },
                    "tasks": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "input": {"type": "string"},
                                "processor": {
                                    "type": "string",
                                    "enum": ["lite", "turbo", "premium"]
                                }
                            },
                            "required": ["input"]
                        },
                        "description": "List of tasks to execute"
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["cost", "speed", "balanced", "quality"],
                        "description": "Optimization strategy",
                        "default": "balanced"
                    }
                },
                "required": ["tasks"]
            }
        },
        {
            "name": "find_all",
            "description": "Find all entities matching criteria across the web (prospecting) using Parallel AI.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "objective": {
                        "type": "string",
                        "description": "What to find (e.g., 'AI startups in San Francisco')"
                    },
                    "schema": {
                        "type": "object",
                        "description": "Schema for extracted entities"
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum entities to return",
                        "default": 50
                    }
                },
                "required": ["objective"]
            }
        },
        {
            "name": "create_monitor",
            "description": "Create a scheduled web monitor to track changes at a URL.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Monitor name"
                    },
                    "url": {
                        "type": "string",
                        "description": "URL to monitor"
                    },
                    "schedule": {
                        "type": "string",
                        "description": "Cron schedule (e.g., '0 * * * *' for hourly)"
                    },
                    "notify_on_change": {
                        "type": "boolean",
                        "description": "Send notification on changes",
                        "default": True
                    }
                },
                "required": ["name", "url", "schedule"]
            }
        },
        {
            "name": "list_tasks",
            "description": "List tasks from local history and Parallel AI storage.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["pending", "running", "completed", "failed", "all"],
                        "default": "all"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 20
                    },
                    "include_remote": {
                        "type": "boolean",
                        "description": "Also fetch from Parallel AI storage",
                        "default": False
                    }
                }
            }
        },
        {
            "name": "get_task",
            "description": "Get details of a specific task by ID.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Task ID (local or Parallel AI)"
                    }
                },
                "required": ["task_id"]
            }
        },
        {
            "name": "synthesize",
            "description": "Synthesize information from multiple sources into a coherent summary.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sources": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of texts or URLs to synthesize"
                    },
                    "objective": {
                        "type": "string",
                        "description": "What to focus on in synthesis"
                    },
                    "format": {
                        "type": "string",
                        "enum": ["summary", "bullets", "report", "analysis"],
                        "default": "summary"
                    }
                },
                "required": ["sources"]
            }
        },
        {
            "name": "search_citations",
            "description": "Search stored citations from past research tasks.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query for citation titles/URLs"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "Filter by specific task ID"
                    },
                    "url_pattern": {
                        "type": "string",
                        "description": "Filter by URL pattern"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 50
                    }
                }
            }
        },
        {
            "name": "get_usage_stats",
            "description": "Get usage statistics and analytics for research tasks.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "start_date": {
                        "type": "string",
                        "description": "Start date (YYYY-MM-DD)"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date (YYYY-MM-DD)"
                    }
                }
            }
        },
        # =====================================================================
        # DATAFRAME-POWERED TOOLS
        # =====================================================================
        {
            "name": "batch_research",
            "description": "Execute multiple research queries in parallel, collecting results into a structured DataFrame. Perfect for comparative analysis, market research, or systematic data collection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name for this research batch"
                    },
                    "queries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of research queries to execute in parallel"
                    },
                    "output_schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "type": {"type": "string", "enum": ["string", "number", "boolean", "url", "list", "json"]},
                                "description": {"type": "string"}
                            }
                        },
                        "description": "Schema for structured output columns"
                    },
                    "processor": {
                        "type": "string",
                        "enum": ["lite", "turbo", "premium"],
                        "default": "lite"
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["cost", "speed", "balanced", "quality"],
                        "default": "balanced"
                    }
                },
                "required": ["name", "queries"]
            }
        },
        {
            "name": "prospect_entities",
            "description": "Find all entities matching criteria across the web and collect into a DataFrame. Ideal for lead generation, competitor analysis, or building databases.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "objective": {
                        "type": "string",
                        "description": "What to find (e.g., 'AI startups in healthcare', 'Python open source projects')"
                    },
                    "entity_schema": {
                        "type": "object",
                        "description": "JSON Schema defining entity fields to extract",
                        "properties": {
                            "properties": {
                                "type": "object",
                                "description": "Field definitions"
                            }
                        }
                    },
                    "max_entities": {
                        "type": "integer",
                        "default": 50,
                        "description": "Maximum entities to find"
                    },
                    "deduplicate": {
                        "type": "boolean",
                        "default": True,
                        "description": "Remove duplicate entities"
                    }
                },
                "required": ["objective"]
            }
        },
        {
            "name": "get_dataframe",
            "description": "Retrieve a DataFrame with its rows and metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dataframe_id": {
                        "type": "string",
                        "description": "DataFrame ID"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 100
                    },
                    "offset": {
                        "type": "integer",
                        "default": 0
                    },
                    "filters": {
                        "type": "object",
                        "description": "Filter rows by field values"
                    }
                },
                "required": ["dataframe_id"]
            }
        },
        {
            "name": "list_dataframes",
            "description": "List all DataFrames from research batches and entity prospecting.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_type": {
                        "type": "string",
                        "enum": ["task_group", "findall", "manual"],
                        "description": "Filter by source type"
                    },
                    "limit": {
                        "type": "integer",
                        "default": 50
                    }
                }
            }
        },
        {
            "name": "export_dataframe",
            "description": "Export a DataFrame to CSV, JSON, or Markdown format.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dataframe_id": {
                        "type": "string",
                        "description": "DataFrame ID"
                    },
                    "format": {
                        "type": "string",
                        "enum": ["csv", "json", "markdown"],
                        "default": "json"
                    },
                    "max_rows": {
                        "type": "integer",
                        "default": 1000
                    }
                },
                "required": ["dataframe_id"]
            }
        },
        {
            "name": "aggregate_dataframe",
            "description": "Perform aggregations on DataFrame data (count, sum, avg, min, max, distinct).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "dataframe_id": {
                        "type": "string",
                        "description": "DataFrame ID"
                    },
                    "group_by": {
                        "type": "string",
                        "description": "Field to group by"
                    },
                    "aggregations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "field": {"type": "string"},
                                "function": {"type": "string", "enum": ["count", "sum", "avg", "min", "max", "distinct"]}
                            }
                        },
                        "description": "Aggregations to perform"
                    }
                },
                "required": ["dataframe_id"]
            }
        },
        {
            "name": "get_batch_status",
            "description": "Get status and progress of a running research batch or entity prospecting job.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "batch_id": {
                        "type": "string",
                        "description": "Batch/group ID"
                    },
                    "batch_type": {
                        "type": "string",
                        "enum": ["task_group", "findall"],
                        "default": "task_group"
                    }
                },
                "required": ["batch_id"]
            }
        },
    ]

    def __init__(
        self,
        port: int = 4002,
        api_key: Optional[str] = None,
        db_path: Optional[str] = None,
        **kwargs
    ):
        super().__init__(port=port, **kwargs)

        self.api_key = api_key or os.getenv("PARALLEL_API_KEY")
        self._client: Optional[httpx.AsyncClient] = None

        # SQLite storage for durable persistence
        db_path = db_path or os.getenv("RESEARCH_DB_PATH", str(self.data_dir / "research_tasks.db"))
        self.storage = ResearchStorage(db_path)
        logger.info(f"Research storage initialized at {db_path}")

        # DataFrame storage for structured batch results
        df_path = os.getenv("RESEARCH_DF_PATH", str(self.data_dir / "research_dataframes.db"))
        self.df_storage = DataFrameStorage(df_path)
        logger.info(f"DataFrame storage initialized at {df_path}")

        # In-memory cache (mirrors storage for fast access)
        self._tasks: Dict[str, LocalTask] = {}
        self._task_groups: Dict[str, List[str]] = {}
        self._task_embeddings: Dict[str, List[float]] = {}

        # Lifecycle hooks
        self.on_start(self._on_start)
        self.on_stop(self._on_stop)
        self.on_capability_available(CapabilityType.EMBED, self._on_embeddings_available)

    async def _on_start(self):
        """Initialize HTTP client."""
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        self._client = httpx.AsyncClient(
            timeout=120.0,
            headers=headers
        )
        logger.info(f"Parallel AI client initialized (key configured: {bool(self.api_key)})")

    async def _on_stop(self):
        """Close HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _on_embeddings_available(self):
        """Called when embedding capability becomes available."""
        logger.info("Embedding capability available - semantic task search enabled")

    # =========================================================================
    # PARALLEL AI API METHODS
    # =========================================================================

    async def _parallel_search(self, objective: str, max_results: int = 10) -> Dict[str, Any]:
        """Execute search via Parallel AI task run.

        Parallel AI processes search queries through the task run API,
        returning results with citations and sources.
        """
        if not self.api_key:
            return {"error": "PARALLEL_API_KEY not configured", "success": False}

        # Use task run for search - Parallel AI handles web search internally
        search_prompt = f"""Search the web for: {objective}

Return up to {max_results} relevant results with:
- Title
- URL
- Brief excerpt/summary

Focus on recent, authoritative sources."""

        return await self._parallel_task_run(
            input_text=search_prompt,
            processor="lite",
            timeout=120
        )

    async def _parallel_extract(self, url: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        """Execute extraction via Parallel AI task run.

        Uses the task API to extract structured data from URLs.
        """
        if not self.api_key:
            return {"error": "PARALLEL_API_KEY not configured", "success": False}

        # Build extraction prompt
        schema_instruction = ""
        if schema:
            schema_instruction = f"\n\nExtract data according to this schema:\n{json.dumps(schema, indent=2)}"

        extract_prompt = f"""Extract structured data from this URL: {url}
{schema_instruction}

Return the extracted information in a clear, structured format."""

        return await self._parallel_task_run(
            input_text=extract_prompt,
            processor="lite",
            timeout=120
        )

    async def _parallel_task_run(
        self,
        input_text: str,
        processor: str = "lite",
        task_spec: Optional[Dict] = None,
        timeout: int = 300,
        _retry_with_lite: bool = True,
    ) -> Dict[str, Any]:
        """Execute a task via Parallel AI with polling for completion."""
        if not self.api_key:
            return {"error": "PARALLEL_API_KEY not configured", "success": False}

        if not self._client:
            return {"error": "HTTP client not initialized", "success": False}

        try:
            # Create task
            payload = {
                "processor": processor,
                "input": input_text
            }
            if task_spec:
                payload["task_spec"] = task_spec

            response = await self._client.post(
                f"{self.PARALLEL_API_URL}/v1/tasks/runs",
                json=payload
            )

            # Handle 403 - may be processor permission issue, fallback to lite
            if response.status_code == 403 and processor != "lite" and _retry_with_lite:
                logger.warning(f"Processor '{processor}' returned 403, falling back to 'lite'")
                return await self._parallel_task_run(
                    input_text=input_text,
                    processor="lite",
                    task_spec=task_spec,
                    timeout=timeout,
                    _retry_with_lite=False,
                )

            if response.status_code not in (200, 201, 202):
                error_detail = ""
                try:
                    error_detail = response.json().get("detail", response.text[:200])
                except Exception:
                    error_detail = response.text[:200]
                return {"error": f"Task creation failed: {response.status_code} - {error_detail}", "success": False}

            task_data = response.json()
            run_id = task_data.get("run_id") or task_data.get("id") or task_data.get("task_id")

            if not run_id:
                # Synchronous response - task completed immediately
                return {**task_data, "success": True}

            # Poll for completion
            poll_interval = 2.0
            elapsed = 0

            while elapsed < timeout:
                await asyncio.sleep(poll_interval)
                elapsed += poll_interval

                status_response = await self._client.get(
                    f"{self.PARALLEL_API_URL}/v1/tasks/runs/{run_id}"
                )

                if status_response.status_code == 200:
                    status_data = status_response.json()
                    status = status_data.get("status", "")

                    if status in ("completed", "failed", "cancelled"):
                        # Fetch full result with output from /result endpoint
                        result_response = await self._client.get(
                            f"{self.PARALLEL_API_URL}/v1/tasks/runs/{run_id}/result"
                        )

                        if result_response.status_code == 200:
                            result = result_response.json()
                            return {
                                "run_id": run_id,
                                "status": status,
                                "run": result.get("run", {}),
                                "output": result.get("output", {}),
                                "success": status == "completed"
                            }
                        else:
                            return {
                                **status_data,
                                "success": status == "completed"
                            }

            return {"error": f"Task {run_id} timed out after {timeout}s", "success": False}

        except Exception as e:
            return {"error": str(e), "success": False}

    async def _parallel_task_group(
        self,
        tasks: List[Dict],
        name: Optional[str] = None,
        strategy: str = "balanced"
    ) -> Dict[str, Any]:
        """Create a task group via Parallel AI."""
        if not self.api_key:
            return {"error": "PARALLEL_API_KEY not configured", "success": False}

        try:
            payload = {
                "tasks": tasks,
                "strategy": strategy
            }
            if name:
                payload["name"] = name

            response = await self._client.post(
                f"{self.PARALLEL_API_URL}/v1beta/tasks/groups",
                json=payload,
                timeout=120.0
            )

            if response.status_code in (200, 201, 202):
                return {**response.json(), "success": True}
            else:
                return {"error": f"Task group creation failed: {response.status_code}", "success": False}
        except Exception as e:
            return {"error": str(e), "success": False}

    async def _parallel_findall(
        self,
        objective: str,
        schema: Optional[Dict] = None,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """Execute findall via Parallel AI."""
        if not self.api_key:
            return {"error": "PARALLEL_API_KEY not configured", "success": False}

        try:
            payload = {
                "objective": objective,
                "max_results": max_results
            }
            if schema:
                payload["schema"] = schema

            response = await self._client.post(
                f"{self.PARALLEL_API_URL}/v1beta/findall/runs",
                json=payload,
                timeout=120.0
            )

            if response.status_code in (200, 201, 202):
                return {**response.json(), "success": True}
            else:
                return {"error": f"FindAll failed: {response.status_code}", "success": False}
        except Exception as e:
            return {"error": str(e), "success": False}

    async def _parallel_create_monitor(
        self,
        name: str,
        url: str,
        schedule: str,
        notify_on_change: bool = True
    ) -> Dict[str, Any]:
        """Create a monitor via Parallel AI."""
        if not self.api_key:
            return {"error": "PARALLEL_API_KEY not configured", "success": False}

        try:
            response = await self._client.post(
                f"{self.PARALLEL_API_URL}/v1alpha/monitors",
                json={
                    "name": name,
                    "url": url,
                    "schedule": schedule,
                    "notify_on_change": notify_on_change
                }
            )

            if response.status_code in (200, 201):
                return {**response.json(), "success": True}
            else:
                return {"error": f"Monitor creation failed: {response.status_code}", "success": False}
        except Exception as e:
            return {"error": str(e), "success": False}

    async def _parallel_get_stored_tasks(
        self,
        limit: int = 20,
        status: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get stored tasks from Parallel AI."""
        if not self.api_key:
            return {"error": "PARALLEL_API_KEY not configured", "success": False}

        try:
            params = {"limit": limit}
            if status and status != "all":
                params["status"] = status

            response = await self._client.get(
                f"{self.PARALLEL_API_URL}/v1/stored/tasks",
                params=params
            )

            if response.status_code == 200:
                return {**response.json(), "success": True}
            else:
                return {"error": f"Get stored tasks failed: {response.status_code}", "success": False}
        except Exception as e:
            return {"error": str(e), "success": False}

    # =========================================================================
    # TOOL HANDLERS
    # =========================================================================

    async def _tool_deep_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute deep search via Parallel AI."""
        objective = args.get("objective")
        if not objective:
            return self._error("objective is required")

        max_results = min(args.get("max_results", 10), 20)

        # Create task ID early for tracking
        task_id = str(uuid.uuid4())

        # Try Parallel AI first
        result = await self._parallel_search(objective, max_results)

        if result.get("success") is False and "PARALLEL_API_KEY" in result.get("error", ""):
            # Fallback to mesh search if available
            if self.has_capability(CapabilityType.SEARCH):
                search_results = await self.mesh.search(objective, num_results=max_results)
                return self._text_result({
                    "objective": objective,
                    "source": "mesh_fallback",
                    "results": search_results,
                    "success": True
                })

        # Track locally
        task = LocalTask(
            id=task_id,
            processor=ProcessorType.LITE,
            input=objective,
            status=TaskStatus.COMPLETED if result.get("success") else TaskStatus.FAILED,
            result=result,
            completed_at=datetime.now(timezone.utc)
        )
        task.parallel_id = result.get("run_id")
        self._tasks[task.id] = task

        # Persist to SQLite storage
        self.storage.create_task(task.id, objective, "lite")
        self.storage.update_task_status(
            task.id,
            task.status.value,
            parallel_id=task.parallel_id,
            error=result.get("error") if not result.get("success") else None
        )
        if result.get("success"):
            self.storage.store_result(task.id, result)

        return self._text_result({
            "objective": objective,
            "task_id": task.id,
            "source": "parallel_ai",
            **result
        })

    async def _tool_extract_data(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from URL via Parallel AI."""
        url = args.get("url")
        if not url:
            return self._error("url is required")

        schema = args.get("schema")
        result = await self._parallel_extract(url, schema)

        # Track locally
        task = LocalTask(
            id=str(uuid.uuid4()),
            processor=ProcessorType.LITE,
            input=f"Extract from: {url}",
            status=TaskStatus.COMPLETED if result.get("success") else TaskStatus.FAILED,
            result=result,
            completed_at=datetime.now(timezone.utc)
        )
        task.parallel_id = result.get("run_id")
        self._tasks[task.id] = task

        # Persist to SQLite storage
        self.storage.create_task(task.id, f"Extract from: {url}", "lite")
        self.storage.update_task_status(
            task.id,
            task.status.value,
            parallel_id=task.parallel_id,
            error=result.get("error") if not result.get("success") else None
        )
        if result.get("success"):
            self.storage.store_result(task.id, result)

        return self._text_result({
            "url": url,
            "task_id": task.id,
            **result
        })

    async def _tool_run_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute autonomous task via Parallel AI."""
        input_text = args.get("input")
        if not input_text:
            return self._error("input is required")

        processor = args.get("processor", "lite")
        timeout = min(args.get("timeout", 300), 600)

        task_spec = None
        if args.get("output_schema") or args.get("instructions"):
            task_spec = {}
            if args.get("output_schema"):
                task_spec["output_schema"] = args["output_schema"]
            if args.get("instructions"):
                task_spec["instructions"] = args["instructions"]

        # Create local tracking
        local_task = LocalTask(
            id=str(uuid.uuid4()),
            processor=ProcessorType(processor),
            input=input_text,
            status=TaskStatus.RUNNING
        )
        self._tasks[local_task.id] = local_task

        # Persist to SQLite storage
        self.storage.create_task(local_task.id, input_text, processor)
        self.storage.update_task_status(local_task.id, "running")

        # Execute via Parallel AI
        result = await self._parallel_task_run(input_text, processor, task_spec, timeout)

        # Update local tracking
        local_task.parallel_id = result.get("run_id") or result.get("id") or result.get("task_id")
        local_task.status = TaskStatus.COMPLETED if result.get("success") else TaskStatus.FAILED
        local_task.result = result
        local_task.completed_at = datetime.now(timezone.utc)

        if not result.get("success"):
            local_task.error = result.get("error")

        # Persist completion to SQLite
        self.storage.update_task_status(
            local_task.id,
            local_task.status.value,
            parallel_id=local_task.parallel_id,
            error=local_task.error
        )
        if result.get("success"):
            self.storage.store_result(local_task.id, result)

        # Store embedding for semantic search
        if self.has_capability(CapabilityType.EMBED):
            embedding = await self.get_embedding(input_text)
            if embedding:
                self._task_embeddings[local_task.id] = embedding

        return self._text_result({
            "task_id": local_task.id,
            "parallel_task_id": local_task.parallel_id,
            "processor": processor,
            **result
        })

    async def _tool_create_task_group(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create task group via Parallel AI."""
        tasks_config = args.get("tasks", [])
        if not tasks_config:
            return self._error("tasks list is required")

        name = args.get("name", f"group-{uuid.uuid4().hex[:8]}")
        strategy = args.get("strategy", "balanced")

        # Format tasks for Parallel AI
        parallel_tasks = []
        local_task_ids = []

        for task_config in tasks_config:
            # Create local tracking
            local_task = LocalTask(
                id=str(uuid.uuid4()),
                processor=ProcessorType(task_config.get("processor", "lite")),
                input=task_config.get("input", ""),
                status=TaskStatus.PENDING
            )
            self._tasks[local_task.id] = local_task
            local_task_ids.append(local_task.id)

            parallel_tasks.append({
                "input": task_config.get("input", ""),
                "processor": task_config.get("processor", "lite")
            })

        # Store group
        self._task_groups[name] = local_task_ids

        # Execute via Parallel AI
        result = await self._parallel_task_group(parallel_tasks, name, strategy)

        # Update local tracking based on result
        if result.get("success"):
            for local_id in local_task_ids:
                self._tasks[local_id].status = TaskStatus.RUNNING

        return self._text_result({
            "group_name": name,
            "strategy": strategy,
            "task_count": len(tasks_config),
            "local_task_ids": local_task_ids,
            **result
        })

    async def _tool_find_all(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Find all matching entities via Parallel AI."""
        objective = args.get("objective")
        if not objective:
            return self._error("objective is required")

        schema = args.get("schema")
        max_results = args.get("max_results", 50)

        result = await self._parallel_findall(objective, schema, max_results)

        # Track locally
        task = LocalTask(
            id=str(uuid.uuid4()),
            processor=ProcessorType.TURBO,
            input=f"FindAll: {objective}",
            status=TaskStatus.COMPLETED if result.get("success") else TaskStatus.FAILED,
            result=result,
            completed_at=datetime.now(timezone.utc)
        )
        self._tasks[task.id] = task

        return self._text_result({
            "objective": objective,
            "task_id": task.id,
            **result
        })

    async def _tool_create_monitor(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create web monitor via Parallel AI."""
        name = args.get("name")
        url = args.get("url")
        schedule = args.get("schedule")

        if not all([name, url, schedule]):
            return self._error("name, url, and schedule are required")

        notify_on_change = args.get("notify_on_change", True)

        result = await self._parallel_create_monitor(name, url, schedule, notify_on_change)

        return self._text_result({
            "name": name,
            "url": url,
            "schedule": schedule,
            **result
        })

    async def _tool_list_tasks(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List tasks from SQLite storage and optionally Parallel AI."""
        status_filter = args.get("status", "all")
        limit = args.get("limit", 20)
        include_remote = args.get("include_remote", False)

        # Get tasks from SQLite storage
        stored_tasks, total = self.storage.list_tasks(
            status=status_filter if status_filter != "all" else None,
            limit=limit
        )

        result = {
            "tasks": [
                {
                    "id": t.id,
                    "parallel_id": t.parallel_id,
                    "processor": t.processor,
                    "status": t.status,
                    "input_preview": t.input[:100] + "..." if len(t.input) > 100 else t.input,
                    "citations_count": t.citations_count,
                    "created_at": t.created_at.isoformat(),
                    "completed_at": t.completed_at.isoformat() if t.completed_at else None,
                }
                for t in stored_tasks
            ],
            "total": total,
            "success": True
        }

        # Include remote if requested
        if include_remote:
            remote_result = await self._parallel_get_stored_tasks(limit, status_filter)
            if remote_result.get("success"):
                result["remote_tasks"] = remote_result.get("tasks", [])

        return self._text_result(result)

    async def _tool_get_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get task details from SQLite storage."""
        task_id = args.get("task_id")
        if not task_id:
            return self._error("task_id is required")

        # Check SQLite storage first
        stored_task = self.storage.get_task(task_id)
        if stored_task:
            # Get citations for this task
            citations = self.storage.get_citations(task_id=task_id, limit=50)
            return self._text_result({
                "task": {
                    "id": stored_task.id,
                    "parallel_id": stored_task.parallel_id,
                    "processor": stored_task.processor,
                    "input": stored_task.input,
                    "status": stored_task.status,
                    "result": stored_task.result,
                    "error": stored_task.error,
                    "citations_count": stored_task.citations_count,
                    "created_at": stored_task.created_at.isoformat(),
                    "completed_at": stored_task.completed_at.isoformat() if stored_task.completed_at else None,
                },
                "citations": citations,
                "source": "storage",
                "success": True
            })

        # Try to fetch from Parallel AI
        if self.api_key and self._client:
            try:
                response = await self._client.get(
                    f"{self.PARALLEL_API_URL}/v1/tasks/runs/{task_id}"
                )
                if response.status_code == 200:
                    return self._text_result({
                        "task": response.json(),
                        "source": "parallel_ai",
                        "success": True
                    })
            except Exception:
                pass

        return self._error(f"Task not found: {task_id}")

    async def _tool_synthesize(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Synthesize information from multiple sources."""
        sources = args.get("sources", [])
        if not sources:
            return self._error("sources list is required")

        objective = args.get("objective", "")
        format_type = args.get("format", "summary")

        # Read content from URLs using mesh if available
        contents = []
        for source in sources:
            if source.startswith("http"):
                if self.has_capability(CapabilityType.READ):
                    result = await self.mesh.call(
                        tool="read",
                        args={"url": source},
                        capability=CapabilityType.READ
                    )
                    try:
                        data = json.loads(result.get("content", [{}])[0].get("text", "{}"))
                        contents.append(data.get("content", source)[:2000])
                    except Exception:
                        contents.append(source)
                else:
                    # Try Parallel extraction
                    extract_result = await self._parallel_extract(source)
                    if extract_result.get("success"):
                        contents.append(json.dumps(extract_result.get("data", {}))[:2000])
                    else:
                        contents.append(source)
            else:
                contents.append(source)

        # Synthesize using a task run
        synthesis_prompt = f"""Synthesize the following {len(contents)} sources into a {format_type}.
{"Focus on: " + objective if objective else ""}

Sources:
{chr(10).join(f"[{i+1}] {c[:1000]}" for i, c in enumerate(contents))}

Provide a clear, coherent {format_type} combining insights from all sources."""

        result = await self._parallel_task_run(
            synthesis_prompt,
            processor="turbo",
            timeout=120
        )

        return self._text_result({
            "sources_processed": len(sources),
            "format": format_type,
            "objective": objective,
            **result
        })

    async def _tool_search_citations(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Search stored citations from past research tasks."""
        query = args.get("query")
        task_id = args.get("task_id")
        url_pattern = args.get("url_pattern")
        limit = args.get("limit", 50)

        if query:
            # Full-text search
            citations = self.storage.search_citations(query, limit=limit)
        else:
            # Filter-based retrieval
            citations = self.storage.get_citations(
                task_id=task_id,
                url_pattern=url_pattern,
                limit=limit
            )

        return self._text_result({
            "citations": citations,
            "count": len(citations),
            "query": query,
            "task_id": task_id,
            "url_pattern": url_pattern,
            "success": True
        })

    async def _tool_get_usage_stats(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get usage statistics and analytics for research tasks."""
        start_date = args.get("start_date")
        end_date = args.get("end_date")

        # Get usage stats from storage
        usage_stats = self.storage.get_usage_stats(start_date, end_date)

        # Get quick counts
        counts = self.storage.get_total_counts()

        return self._text_result({
            "usage": usage_stats,
            "totals": counts,
            "period": {
                "start_date": start_date,
                "end_date": end_date
            },
            "success": True
        })

    # =========================================================================
    # DATAFRAME-POWERED TOOL HANDLERS
    # =========================================================================

    async def _tool_batch_research(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute batch research queries in parallel with DataFrame collection."""
        name = args.get("name", f"batch-{uuid.uuid4().hex[:8]}")
        queries = args.get("queries", [])
        output_schema = args.get("output_schema")
        processor = args.get("processor", "lite")
        strategy = args.get("strategy", "balanced")

        if not queries:
            return self._error("queries list is required")

        # Build tasks from queries
        tasks = [{"input": q, "processor": processor} for q in queries]

        # Default schema if not provided
        if not output_schema:
            output_schema = [
                {"name": "query", "type": "string", "description": "Original query"},
                {"name": "answer", "type": "string", "description": "Research answer"},
                {"name": "confidence", "type": "string", "description": "Confidence level"},
                {"name": "sources", "type": "list", "description": "Source URLs"},
            ]

        # Create task group with DataFrame
        group_id, dataframe_id = self.df_storage.create_task_group(
            name=name,
            tasks=tasks,
            output_schema=output_schema,
            strategy=strategy,
        )

        # Execute tasks via Parallel AI (async background execution)
        asyncio.create_task(self._execute_batch_research(
            group_id, dataframe_id, queries, processor
        ))

        return self._text_result({
            "batch_id": group_id,
            "dataframe_id": dataframe_id,
            "name": name,
            "query_count": len(queries),
            "status": "started",
            "message": f"Batch research started with {len(queries)} queries. Use get_batch_status to track progress.",
            "success": True
        })

    async def _execute_batch_research(
        self,
        group_id: str,
        dataframe_id: str,
        queries: List[str],
        processor: str,
    ):
        """Background execution of batch research queries."""
        group = self.df_storage.get_task_group(group_id)
        if not group:
            return

        for i, item in enumerate(group["items"]):
            task_id = item["task_id"]
            query = queries[i] if i < len(queries) else item["input"]

            # Add delay between requests to avoid rate limiting
            if i > 0:
                await asyncio.sleep(1.0)

            # Execute via Parallel AI (with automatic fallback to lite)
            result = await self._parallel_task_run(
                input_text=query,
                processor=processor,
                timeout=180
            )

            # Update task item
            status = "completed" if result.get("success") else "failed"
            self.df_storage.update_task_group_item(
                group_id=group_id,
                task_id=task_id,
                status=status,
                parallel_task_id=result.get("run_id"),
                result=result,
                error=result.get("error"),
            )

            # Add row to DataFrame if successful
            if result.get("success") and dataframe_id:
                output = result.get("output", {})
                content = output.get("content", {})
                basis = output.get("basis", [])

                # Extract sources from citations
                sources = []
                for b in basis:
                    for c in b.get("citations", []):
                        sources.append(c.get("url"))

                row_data = {
                    "query": query,
                    "answer": content.get("output", str(content)) if isinstance(content, dict) else str(content),
                    "confidence": basis[0].get("confidence", "unknown") if basis else "unknown",
                    "sources": sources,
                }

                # Extract citations for the row
                citations = []
                for b in basis:
                    citations.extend(b.get("citations", []))

                self.df_storage.add_row(
                    dataframe_id=dataframe_id,
                    data=row_data,
                    task_id=task_id,
                    confidence=0.8 if basis and basis[0].get("confidence") == "high" else 0.5,
                    citations=citations,
                )

            logger.info(f"Batch {group_id}: completed {i+1}/{len(queries)}")

    async def _tool_prospect_entities(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Find all entities matching criteria and collect into DataFrame."""
        objective = args.get("objective")
        if not objective:
            return self._error("objective is required")

        entity_schema = args.get("entity_schema")
        max_entities = args.get("max_entities", 50)
        deduplicate = args.get("deduplicate", True)

        # Create FindAll run with DataFrame
        run_id, dataframe_id = self.df_storage.create_findall_run(
            objective=objective,
            schema=entity_schema,
            max_results=max_entities,
        )

        # Execute FindAll via Parallel AI (async)
        asyncio.create_task(self._execute_prospect_entities(
            run_id, dataframe_id, objective, entity_schema, max_entities, deduplicate
        ))

        return self._text_result({
            "run_id": run_id,
            "dataframe_id": dataframe_id,
            "objective": objective,
            "max_entities": max_entities,
            "status": "started",
            "message": f"Entity prospecting started. Use get_batch_status to track progress.",
            "success": True
        })

    async def _execute_prospect_entities(
        self,
        run_id: str,
        dataframe_id: str,
        objective: str,
        schema: Optional[Dict],
        max_entities: int,
        deduplicate: bool,
    ):
        """Background execution of entity prospecting."""
        # Build prospecting prompt
        schema_instruction = ""
        if schema and schema.get("properties"):
            fields = list(schema["properties"].keys())
            schema_instruction = f"\n\nFor each entity, extract these fields: {', '.join(fields)}"

        prospect_prompt = f"""Find up to {max_entities} entities matching: {objective}
{schema_instruction}

Return a JSON array of entities, each with relevant fields extracted.
Include URLs and sources where found."""

        # Execute via task run (will auto-fallback to lite if turbo not available)
        result = await self._parallel_task_run(
            input_text=prospect_prompt,
            processor="lite",  # Use lite for broader compatibility
            timeout=300
        )

        # Update run status
        status = "completed" if result.get("success") else "failed"
        self.df_storage.update_findall_run(
            run_id=run_id,
            status=status,
            parallel_run_id=result.get("run_id"),
        )

        # Extract entities and add to DataFrame
        if result.get("success") and dataframe_id:
            output = result.get("output", {})
            content = output.get("content", {})
            basis = output.get("basis", [])

            # Try to parse entities from output
            entities = []
            if isinstance(content, dict) and "entities" in content:
                entities = content["entities"]
            elif isinstance(content, list):
                entities = content
            elif isinstance(content, dict) and "output" in content:
                # Try to parse JSON from output string
                try:
                    parsed = json.loads(content["output"])
                    if isinstance(parsed, list):
                        entities = parsed
                except (json.JSONDecodeError, TypeError):
                    # Create single entity from output
                    entities = [{"description": str(content.get("output", content))}]

            # Deduplicate if requested
            if deduplicate and entities:
                seen = set()
                unique_entities = []
                for e in entities:
                    key = str(e.get("name", e.get("url", str(e))))
                    if key not in seen:
                        seen.add(key)
                        unique_entities.append(e)
                entities = unique_entities

            # Add rows to DataFrame
            added = self.df_storage.add_rows_batch(
                dataframe_id=dataframe_id,
                rows=[{"data": e} for e in entities],
            )

            self.df_storage.update_findall_run(
                run_id=run_id,
                status="completed",
                entities_found=added,
            )

            logger.info(f"FindAll {run_id}: found {added} entities")

    async def _tool_get_dataframe(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Retrieve a DataFrame with its rows."""
        dataframe_id = args.get("dataframe_id")
        if not dataframe_id:
            return self._error("dataframe_id is required")

        limit = args.get("limit", 100)
        offset = args.get("offset", 0)
        filters = args.get("filters")

        df = self.df_storage.get_dataframe(dataframe_id)
        if not df:
            return self._error(f"DataFrame not found: {dataframe_id}")

        rows, total = self.df_storage.query_rows(
            dataframe_id=dataframe_id,
            filters=filters,
            limit=limit,
            offset=offset,
        )

        return self._text_result({
            "dataframe": {
                "id": df.id,
                "name": df.name,
                "description": df.description,
                "columns": [{"name": c.name, "type": c.type.value} for c in df.columns],
                "source_type": df.source_type,
                "source_id": df.source_id,
                "row_count": df.row_count,
                "created_at": df.created_at.isoformat(),
            },
            "rows": [
                {
                    "id": r.id,
                    "data": r.data,
                    "confidence": r.confidence,
                    "citations_count": len(r.citations),
                }
                for r in rows
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
            "success": True
        })

    async def _tool_list_dataframes(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List all DataFrames."""
        source_type = args.get("source_type")
        limit = args.get("limit", 50)

        dataframes = self.df_storage.list_dataframes(
            source_type=source_type,
            limit=limit,
        )

        return self._text_result({
            "dataframes": [
                {
                    "id": df.id,
                    "name": df.name,
                    "description": df.description,
                    "source_type": df.source_type,
                    "row_count": df.row_count,
                    "columns": [c.name for c in df.columns],
                    "created_at": df.created_at.isoformat(),
                    "updated_at": df.updated_at.isoformat(),
                }
                for df in dataframes
            ],
            "count": len(dataframes),
            "success": True
        })

    async def _tool_export_dataframe(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Export DataFrame to various formats."""
        dataframe_id = args.get("dataframe_id")
        if not dataframe_id:
            return self._error("dataframe_id is required")

        export_format = args.get("format", "json")
        max_rows = args.get("max_rows", 1000)

        df = self.df_storage.get_dataframe(dataframe_id)
        if not df:
            return self._error(f"DataFrame not found: {dataframe_id}")

        if export_format == "csv":
            content = self.df_storage.export_to_csv(dataframe_id)
            return self._text_result({
                "format": "csv",
                "dataframe_id": dataframe_id,
                "dataframe_name": df.name,
                "content": content,
                "success": True
            })
        elif export_format == "markdown":
            content = self.df_storage.export_to_markdown(dataframe_id, max_rows=max_rows)
            return self._text_result({
                "format": "markdown",
                "dataframe_id": dataframe_id,
                "dataframe_name": df.name,
                "content": content,
                "success": True
            })
        else:  # json
            data = self.df_storage.export_to_json(dataframe_id)
            return self._text_result({
                "format": "json",
                "dataframe_id": dataframe_id,
                "dataframe_name": df.name,
                **data,
                "success": True
            })

    async def _tool_aggregate_dataframe(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Perform aggregations on DataFrame data."""
        dataframe_id = args.get("dataframe_id")
        if not dataframe_id:
            return self._error("dataframe_id is required")

        group_by = args.get("group_by")
        aggregations = args.get("aggregations", [])

        df = self.df_storage.get_dataframe(dataframe_id)
        if not df:
            return self._error(f"DataFrame not found: {dataframe_id}")

        results = self.df_storage.aggregate(
            dataframe_id=dataframe_id,
            group_by=group_by,
            aggregations=aggregations,
        )

        return self._text_result({
            "dataframe_id": dataframe_id,
            "dataframe_name": df.name,
            "group_by": group_by,
            "aggregations": aggregations,
            "results": results,
            "success": True
        })

    async def _tool_get_batch_status(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get status of a running batch or findall job."""
        batch_id = args.get("batch_id")
        if not batch_id:
            return self._error("batch_id is required")

        batch_type = args.get("batch_type", "task_group")

        if batch_type == "findall":
            run = self.df_storage.get_findall_run(batch_id)
            if not run:
                return self._error(f"FindAll run not found: {batch_id}")

            # Get DataFrame info if available
            df_info = None
            if run["dataframe_id"]:
                df = self.df_storage.get_dataframe(run["dataframe_id"])
                if df:
                    df_info = {
                        "id": df.id,
                        "name": df.name,
                        "row_count": df.row_count,
                    }

            return self._text_result({
                "type": "findall",
                "run": run,
                "dataframe": df_info,
                "success": True
            })
        else:  # task_group
            group = self.df_storage.get_task_group(batch_id)
            if not group:
                return self._error(f"Task group not found: {batch_id}")

            # Get DataFrame info if available
            df_info = None
            if group["dataframe_id"]:
                df = self.df_storage.get_dataframe(group["dataframe_id"])
                if df:
                    df_info = {
                        "id": df.id,
                        "name": df.name,
                        "row_count": df.row_count,
                    }

            return self._text_result({
                "type": "task_group",
                "group": {
                    "id": group["id"],
                    "name": group["name"],
                    "status": group["status"],
                    "progress": group["progress"],
                    "total_tasks": group["total_tasks"],
                    "completed_tasks": group["completed_tasks"],
                    "failed_tasks": group["failed_tasks"],
                    "created_at": group["created_at"],
                },
                "items": [
                    {
                        "task_id": item["task_id"],
                        "status": item["status"],
                        "input_preview": item["input"][:100] + "..." if len(item["input"]) > 100 else item["input"],
                        "error": item["error"],
                    }
                    for item in group["items"]
                ],
                "dataframe": df_info,
                "success": True
            })

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _text_result(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Format a text result."""
        return {
            "content": [{
                "type": "text",
                "text": json.dumps(data, indent=2, default=str)
            }]
        }

    def _error(self, message: str) -> Dict[str, Any]:
        """Format an error result."""
        return {
            "content": [{
                "type": "text",
                "text": json.dumps({"error": message, "success": False})
            }],
            "isError": True
        }


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=4002)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    api_configured = bool(os.getenv("PARALLEL_API_KEY"))

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║         DISTRIBUTED RESEARCH MCP v2 - Parallel AI                ║
╠══════════════════════════════════════════════════════════════════╣
║  Service ID: distributed-research-mcp                            ║
║  Port: {args.port:<57} ║
║  Protocol: MCP 2024-11-05                                        ║
║  Parallel AI: {'✓ Configured' if api_configured else '✗ Not configured (set PARALLEL_API_KEY)':<43} ║
╠══════════════════════════════════════════════════════════════════╣
║  Capabilities: task, execute, research, synthesis, parallel_ai   ║
║  Requires: embed (optional, for semantic task search)            ║
╠══════════════════════════════════════════════════════════════════╣
║  Parallel AI Integration:                                        ║
║  • POST /v1beta/search      - Deep web search                    ║
║  • POST /v1beta/extract     - Structured extraction              ║
║  • POST /v1/tasks/runs      - Task execution                     ║
║  • POST /v1beta/tasks/groups - Parallel batch tasks              ║
║  • POST /v1beta/findall/runs - Entity prospecting                ║
║  • POST /v1alpha/monitors   - Web monitoring                     ║
╠══════════════════════════════════════════════════════════════════╣
║  Tools: deep_search, extract_data, run_task, create_task_group,  ║
║         find_all, create_monitor, list_tasks, get_task,          ║
║         synthesize                                               ║
╠══════════════════════════════════════════════════════════════════╣
║  Built on NativeMCPBase with cross-MCP mesh communication        ║
╚══════════════════════════════════════════════════════════════════╝
""")

    server = DistributedResearchMCPv2(port=args.port)
    server.run()
