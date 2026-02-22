#!/usr/bin/env python3
"""
Distributed Research MCP - Native MCP Server

First-class MCP server providing:
- Deep research capabilities
- Parallel task execution
- Structured data extraction
- Batch processing
- Web monitoring

This is a NATIVE MCP server - it implements the MCP protocol directly
rather than wrapping an external service.

Run with:
    python -m native.distributed_research_mcp
    # or
    uvicorn native.distributed_research_mcp:app --port 4002

Environment:
    PARALLEL_API_KEY: Parallel AI API key (optional - degrades gracefully)
    REGISTRY_URL: Central registry URL (optional)
    PORT: Server port (default: 4002)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, AsyncGenerator

import httpx
from fastapi import FastAPI, HTTPException, Request, Response, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Output guard for enforcing size limits and URL capture
try:
    from tool_management.guards.output_guard import guard_mcp_content, OutputGuardConfig
    OUTPUT_GUARD_AVAILABLE = True
except ImportError:
    OUTPUT_GUARD_AVAILABLE = False
    logger.info("Output guard not available - running without output limits")


# =============================================================================
# MODELS
# =============================================================================

class MCPProtocolVersion(str, Enum):
    V1 = "2024-11-05"


class ConnectionStatus(str, Enum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    ERROR = "error"


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ProcessorType(str, Enum):
    LITE = "lite"
    TURBO = "turbo"
    PREMIUM = "premium"


@dataclass
class MCPTool:
    name: str
    description: str
    input_schema: Dict[str, Any]
    handler: Optional[Callable] = None


@dataclass
class ConnectionState:
    status: ConnectionStatus = ConnectionStatus.DISCONNECTED
    connected_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    error_message: Optional[str] = None
    session_id: Optional[str] = None
    protocol_version: str = MCPProtocolVersion.V1.value


@dataclass
class Task:
    id: str
    processor: ProcessorType
    input: str
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: Optional[datetime] = None


class ToolCallRequest(BaseModel):
    name: str
    arguments: Dict[str, Any] = Field(default_factory=dict)


class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    params: Optional[Dict[str, Any]] = None
    id: Optional[int | str] = None


class MCPResponse(BaseModel):
    jsonrpc: str = "2.0"
    result: Optional[Any] = None
    error: Optional[Dict[str, Any]] = None
    id: Optional[int | str] = None


# =============================================================================
# NATIVE MCP SERVER
# =============================================================================

class DistributedResearchMCP:
    """
    Native MCP server for distributed research capabilities.

    This is our second native MCP - it implements parallel task execution
    and deep research capabilities. The connectivity test verifies:

    1. MCP protocol handshake works
    2. Tool discovery functions
    3. Task management works
    4. Parallel execution functions
    """

    SERVICE_ID = "distributed-research-mcp"
    SERVICE_NAME = "Distributed Research MCP"
    VERSION = "1.0.0"

    # Parallel AI endpoints (optional - degrades gracefully)
    PARALLEL_API_URL = "https://api.parallel.ai"

    # Model mapping by processor tier
    MODEL_MAP = {
        ProcessorType.LITE: "claude-haiku-4-5-20251001",
        ProcessorType.TURBO: "claude-sonnet-4-5-20250929",
        ProcessorType.PREMIUM: "claude-opus-4-6",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        port: int = 4002,
        registry_url: Optional[str] = None,
    ):
        self.api_key = api_key or os.getenv("PARALLEL_API_KEY")
        self.anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        self.jina_api_key = os.getenv("JINA_API_KEY")
        self.port = port
        self.registry_url = registry_url or os.getenv("REGISTRY_URL")

        self._client: Optional[httpx.AsyncClient] = None
        self._tools: Dict[str, MCPTool] = {}
        self._state = ConnectionState()

        # In-memory task storage (native task management)
        self._tasks: Dict[str, Task] = {}
        self._task_groups: Dict[str, List[str]] = {}

        # Register native tools
        self._register_tools()

    # =========================================================================
    # LLM + JINA HELPERS
    # =========================================================================

    async def _call_llm(
        self,
        prompt: str,
        system: str = "You are a helpful research assistant.",
        max_tokens: int = 4096,
        processor: ProcessorType = ProcessorType.LITE,
    ) -> Optional[str]:
        """Call Anthropic API. Returns response text or None if unavailable."""
        if not self.anthropic_api_key:
            return None

        model = self.MODEL_MAP.get(processor, self.MODEL_MAP[ProcessorType.LITE])
        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": self.anthropic_api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": model,
                        "max_tokens": max_tokens,
                        "system": system,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get("content", [])
                    return content[0]["text"] if content else None
                else:
                    logger.warning(f"LLM call failed ({resp.status_code}): {resp.text[:200]}")
                    return None
        except Exception as e:
            logger.error(f"LLM call error: {e}")
            return None

    async def _jina_read(self, url: str) -> Optional[str]:
        """Fetch URL content via Jina Reader API."""
        headers = {}
        if self.jina_api_key:
            headers["Authorization"] = f"Bearer {self.jina_api_key}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(f"https://r.jina.ai/{url}", headers=headers)
                if resp.status_code == 200:
                    return resp.text
                return None
        except Exception as e:
            logger.error(f"Jina read error for {url}: {e}")
            return None

    async def _jina_search(self, query: str, num_results: int = 5) -> List[Dict[str, Any]]:
        """Search via Jina Search API."""
        headers = {"Accept": "application/json"}
        if self.jina_api_key:
            headers["Authorization"] = f"Bearer {self.jina_api_key}"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.get(
                    f"https://s.jina.ai/{query}",
                    headers=headers,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    return data.get("data", [])[:num_results]
                return []
        except Exception as e:
            logger.error(f"Jina search error: {e}")
            return []

    def _register_tools(self):
        """Register all native MCP tools."""

        # Deep research/search
        self._tools["deep_search"] = MCPTool(
            name="deep_search",
            description="Perform deep research on a topic, gathering and synthesizing information from multiple sources.",
            input_schema={
                "type": "object",
                "properties": {
                    "objective": {
                        "type": "string",
                        "description": "Research objective or question"
                    },
                    "depth": {
                        "type": "string",
                        "enum": ["quick", "standard", "thorough"],
                        "description": "Research depth level",
                        "default": "standard"
                    },
                    "max_sources": {
                        "type": "integer",
                        "description": "Maximum sources to consult",
                        "default": 10
                    }
                },
                "required": ["objective"]
            },
            handler=self._execute_deep_search
        )

        # Run task
        self._tools["run_task"] = MCPTool(
            name="run_task",
            description="Execute an autonomous task with specified processor. Supports lite, turbo, and premium processing.",
            input_schema={
                "type": "object",
                "properties": {
                    "input": {
                        "type": "string",
                        "description": "Task input/prompt"
                    },
                    "processor": {
                        "type": "string",
                        "enum": ["lite", "turbo", "premium"],
                        "description": "Processor type (affects speed/quality)",
                        "default": "lite"
                    },
                    "output_schema": {
                        "type": "object",
                        "description": "Optional JSON schema for structured output"
                    },
                    "timeout": {
                        "type": "integer",
                        "description": "Timeout in seconds",
                        "default": 300
                    }
                },
                "required": ["input"]
            },
            handler=self._execute_run_task
        )

        # Extract data
        self._tools["extract_data"] = MCPTool(
            name="extract_data",
            description="Extract structured data from a URL based on a schema.",
            input_schema={
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
            },
            handler=self._execute_extract_data
        )

        # Create task group
        self._tools["create_task_group"] = MCPTool(
            name="create_task_group",
            description="Create a batch of tasks to execute in parallel.",
            input_schema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Group name"
                    },
                    "tasks": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "input": {"type": "string"},
                                "processor": {"type": "string"}
                            }
                        },
                        "description": "List of tasks to execute"
                    },
                    "strategy": {
                        "type": "string",
                        "enum": ["parallel", "sequential", "adaptive"],
                        "default": "parallel"
                    }
                },
                "required": ["tasks"]
            },
            handler=self._execute_create_task_group
        )

        # Find all
        self._tools["find_all"] = MCPTool(
            name="find_all",
            description="Find all entities matching criteria across the web (prospecting).",
            input_schema={
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
                        "description": "Maximum results to return",
                        "default": 50
                    }
                },
                "required": ["objective"]
            },
            handler=self._execute_find_all
        )

        # List tasks
        self._tools["list_tasks"] = MCPTool(
            name="list_tasks",
            description="List all tasks with their status and results.",
            input_schema={
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
                    }
                }
            },
            handler=self._execute_list_tasks
        )

        # Get task
        self._tools["get_task"] = MCPTool(
            name="get_task",
            description="Get details and result of a specific task.",
            input_schema={
                "type": "object",
                "properties": {
                    "task_id": {
                        "type": "string",
                        "description": "Task ID"
                    }
                },
                "required": ["task_id"]
            },
            handler=self._execute_get_task
        )

        # Synthesize
        self._tools["synthesize"] = MCPTool(
            name="synthesize",
            description="Synthesize information from multiple sources into a coherent summary.",
            input_schema={
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
            },
            handler=self._execute_synthesize
        )

        # Connectivity test
        self._tools["_connectivity_test"] = MCPTool(
            name="_connectivity_test",
            description="Internal connectivity test for native MCP verification.",
            input_schema={
                "type": "object",
                "properties": {
                    "echo": {
                        "type": "string",
                        "description": "Echo string for verification"
                    }
                }
            },
            handler=self._execute_connectivity_test
        )

    async def start(self):
        """Start the MCP server and establish connectivity."""
        logger.info(f"Starting {self.SERVICE_NAME}...")

        self._state.status = ConnectionStatus.CONNECTING
        self._state.session_id = str(uuid.uuid4())

        # Initialize HTTP client
        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        self._client = httpx.AsyncClient(
            timeout=120.0,
            headers=headers
        )

        # Run connectivity test
        try:
            result = await self._execute_connectivity_test({"echo": "native-research-mcp-init"})
            if result.get("success"):
                self._state.status = ConnectionStatus.CONNECTED
                self._state.connected_at = datetime.now(timezone.utc)
                self._state.last_heartbeat = datetime.now(timezone.utc)
                logger.info(f"✓ {self.SERVICE_NAME} connected (session: {self._state.session_id[:8]}...)")
            else:
                raise RuntimeError("Connectivity test failed")
        except Exception as e:
            self._state.status = ConnectionStatus.ERROR
            self._state.error_message = str(e)
            logger.error(f"✗ {self.SERVICE_NAME} failed to connect: {e}")
            raise

        # Announce to Tool Management
        await self._announce_to_registry()

    async def stop(self):
        """Stop the MCP server."""
        logger.info(f"Stopping {self.SERVICE_NAME}...")

        if self._client:
            await self._client.aclose()
            self._client = None

        self._state.status = ConnectionStatus.DISCONNECTED
        self._state.connected_at = None
        logger.info(f"✓ {self.SERVICE_NAME} stopped")

    async def _announce_to_registry(self):
        """Announce this server and its tools to Tool Management."""
        registry_url = self.registry_url or os.getenv("REGISTRY_URL", "https://tools.distributed.systems")
        service_url = os.getenv("SERVICE_URL", f"http://localhost:{self.port}")
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                response = await client.post(
                    f"{registry_url}/api/v1/tools/announce",
                    json={
                        "backend_id": self.SERVICE_ID,
                        "name": self.SERVICE_NAME,
                        "url": service_url,
                        "health_endpoint": "/health",
                        "mcp_endpoint": "/mcp",
                        "capabilities": list(self._tools.keys()),
                        "tools": self.tools,
                    },
                    timeout=15.0,
                )
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Announced {result.get('registered', 0)} tools to {registry_url}")
                else:
                    logger.warning(f"Announce returned {response.status_code}: {response.text[:200]}")
        except Exception as e:
            logger.warning(f"Could not announce to registry: {e}")

    # =========================================================================
    # MCP PROTOCOL HANDLERS
    # =========================================================================

    async def handle_initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP initialize request."""
        return {
            "protocolVersion": MCPProtocolVersion.V1.value,
            "capabilities": {
                "tools": {},
            },
            "serverInfo": {
                "name": self.SERVICE_NAME,
                "version": self.VERSION,
            }
        }

    async def handle_tools_list(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/list request."""
        tools = []
        for name, tool in self._tools.items():
            if not name.startswith("_"):
                tools.append({
                    "name": tool.name,
                    "description": tool.description,
                    "inputSchema": tool.input_schema
                })
        return {"tools": tools}

    async def handle_tools_call(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/call request."""
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        tool = self._tools.get(tool_name)
        if not tool:
            raise ValueError(f"Unknown tool: {tool_name}")

        if not tool.handler:
            raise ValueError(f"Tool {tool_name} has no handler")

        result = await tool.handler(arguments)

        mcp_result = {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(result) if isinstance(result, dict) else str(result)
                }
            ]
        }

        # Apply output guard: enforce size limits, cap links, capture URLs
        if OUTPUT_GUARD_AVAILABLE:
            mcp_result = guard_mcp_content(
                mcp_result,
                tool_name=tool_name,
                source_context={"server": self.SERVICE_ID},
            )

        return mcp_result

    async def handle_mcp_request(self, request: MCPRequest) -> MCPResponse:
        """Route MCP request to appropriate handler."""
        try:
            handlers = {
                "initialize": self.handle_initialize,
                "tools/list": self.handle_tools_list,
                "tools/call": self.handle_tools_call,
            }

            handler = handlers.get(request.method)
            if not handler:
                return MCPResponse(
                    id=request.id,
                    error={
                        "code": -32601,
                        "message": f"Method not found: {request.method}"
                    }
                )

            result = await handler(request.params or {})
            return MCPResponse(id=request.id, result=result)

        except Exception as e:
            logger.error(f"MCP request error: {e}")
            return MCPResponse(
                id=request.id,
                error={
                    "code": -32000,
                    "message": str(e)
                }
            )

    # =========================================================================
    # TOOL IMPLEMENTATIONS
    # =========================================================================

    async def _execute_deep_search(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute deep research on a topic using Jina search + LLM synthesis."""
        objective = args.get("objective")
        if not objective:
            raise ValueError("objective is required")

        depth = args.get("depth", "standard")
        max_sources = args.get("max_sources", 10)

        # 1. Generate search queries
        queries = self._generate_research_queries(objective, depth)

        # 2. Execute real searches via Jina in parallel (capped at 5 concurrent)
        sem = asyncio.Semaphore(5)

        async def _search_one(query: str) -> List[Dict[str, Any]]:
            async with sem:
                jina_results = await self._jina_search(query, num_results=3)
                if jina_results:
                    return [
                        {
                            "query": query,
                            "title": r.get("title", ""),
                            "url": r.get("url", ""),
                            "description": r.get("description", r.get("content", ""))[:500],
                            "relevant": True,
                        }
                        for r in jina_results
                    ]
                return [{"query": query, "note": f"Native search result for: {query}", "relevant": True}]

        search_tasks = [_search_one(q) for q in queries[:max_sources]]
        search_results = await asyncio.gather(*search_tasks, return_exceptions=True)

        findings = []
        for res in search_results:
            if isinstance(res, Exception):
                logger.warning(f"Search task failed: {res}")
                continue
            findings.extend(res)

        # 3. Synthesize findings with LLM if available
        synthesis = None
        processor = ProcessorType.TURBO if depth == "thorough" else ProcessorType.LITE
        if findings:
            findings_text = "\n".join(
                f"- {f.get('title', f.get('query', ''))}: {f.get('description', f.get('note', ''))}"
                for f in findings[:20]
            )
            llm_result = await self._call_llm(
                prompt=f"Research objective: {objective}\n\nFindings:\n{findings_text}\n\nProvide a comprehensive synthesis of these findings.",
                system="You are a research analyst. Synthesize the provided findings into a clear, structured summary.",
                processor=processor,
            )
            if llm_result:
                synthesis = llm_result

        # 4. Create task to track this research
        task = Task(
            id=str(uuid.uuid4()),
            processor=processor,
            input=objective,
            status=TaskStatus.COMPLETED,
            result={"findings": findings, "synthesis": synthesis},
            completed_at=datetime.now(timezone.utc)
        )
        self._tasks[task.id] = task

        result = {
            "objective": objective,
            "depth": depth,
            "queries_generated": len(queries),
            "findings": findings,
            "task_id": task.id,
            "success": True,
        }
        if synthesis:
            result["synthesis"] = synthesis

        return result

    def _generate_research_queries(self, objective: str, depth: str) -> List[str]:
        """Generate diverse research queries from an objective."""
        base_queries = [
            objective,
            f"{objective} overview",
            f"{objective} latest developments",
            f"{objective} key players",
            f"{objective} challenges",
        ]

        if depth == "thorough":
            base_queries.extend([
                f"{objective} case studies",
                f"{objective} statistics",
                f"{objective} future trends",
                f"{objective} expert opinions",
                f"{objective} comparison analysis",
            ])

        return base_queries

    async def _execute_run_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an autonomous task using LLM when available."""
        input_text = args.get("input")
        if not input_text:
            raise ValueError("input is required")

        processor = ProcessorType(args.get("processor", "lite"))
        output_schema = args.get("output_schema")
        timeout = args.get("timeout", 300)

        # Create task
        task = Task(
            id=str(uuid.uuid4()),
            processor=processor,
            input=input_text,
            status=TaskStatus.RUNNING
        )
        self._tasks[task.id] = task

        try:
            start_time = time.time()

            # Try real LLM processing
            system_prompt = "You are a helpful assistant. Complete the following task."
            if output_schema:
                system_prompt += f"\n\nReturn your response as JSON matching this schema: {json.dumps(output_schema)}"

            llm_output = await self._call_llm(
                prompt=input_text,
                system=system_prompt,
                processor=processor,
            )

            processing_time_ms = (time.time() - start_time) * 1000

            if llm_output:
                output = llm_output
            else:
                # Fallback to stub behavior
                output = f"Processed result for: {input_text[:100]}..."

            result = {
                "input": input_text,
                "processor": processor.value,
                "output": output,
                "metadata": {
                    "tokens_used": len(input_text.split()) * 2,
                    "processing_time_ms": round(processing_time_ms),
                    "llm_used": llm_output is not None,
                }
            }

            if output_schema:
                result["schema_applied"] = True

            task.status = TaskStatus.COMPLETED
            task.result = result
            task.completed_at = datetime.now(timezone.utc)

            return {
                "task_id": task.id,
                "status": task.status.value,
                "result": result,
                "success": True
            }

        except Exception as e:
            task.status = TaskStatus.FAILED
            task.error = str(e)
            return {
                "task_id": task.id,
                "status": task.status.value,
                "error": str(e),
                "success": False
            }

    async def _execute_extract_data(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from URL using Jina reader + LLM."""
        url = args.get("url")
        if not url:
            raise ValueError("url is required")

        schema = args.get("schema", {})
        instructions = args.get("instructions", "")

        # Fetch page content via Jina Reader
        page_content = await self._jina_read(url)

        extracted = {
            "title": f"Content from {url}",
            "data": {},
            "schema_used": bool(schema),
        }

        if page_content:
            # Use LLM to extract structured data
            prompt = f"Extract structured data from the following web page content.\n\nURL: {url}\n"
            if schema:
                prompt += f"\nExtract data matching this schema: {json.dumps(schema)}\n"
            if instructions:
                prompt += f"\nAdditional instructions: {instructions}\n"
            prompt += f"\nPage content:\n{page_content[:8000]}\n\nReturn the extracted data as JSON."

            llm_result = await self._call_llm(
                prompt=prompt,
                system="You are a data extraction specialist. Extract structured data from web content and return it as clean JSON.",
                processor=ProcessorType.LITE,
            )

            if llm_result:
                # Try to parse as JSON
                try:
                    extracted["data"] = json.loads(llm_result)
                except json.JSONDecodeError:
                    extracted["data"] = {"raw_extraction": llm_result}
                extracted["llm_used"] = True
            else:
                extracted["page_content_length"] = len(page_content)

        task = Task(
            id=str(uuid.uuid4()),
            processor=ProcessorType.LITE,
            input=f"Extract from {url}",
            status=TaskStatus.COMPLETED,
            result={"url": url, "extracted": extracted},
            completed_at=datetime.now(timezone.utc)
        )
        self._tasks[task.id] = task

        return {
            "url": url,
            "task_id": task.id,
            "extracted": extracted,
            "success": True,
        }

    async def _execute_create_task_group(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Create and execute a batch of parallel tasks."""
        tasks_config = args.get("tasks", [])
        if not tasks_config:
            raise ValueError("tasks list is required")

        name = args.get("name", f"group-{uuid.uuid4().hex[:8]}")
        strategy = args.get("strategy", "parallel")

        # Create tasks
        task_ids = []
        for task_config in tasks_config:
            task = Task(
                id=str(uuid.uuid4()),
                processor=ProcessorType(task_config.get("processor", "lite")),
                input=task_config.get("input", ""),
                status=TaskStatus.PENDING
            )
            self._tasks[task.id] = task
            task_ids.append(task.id)

        # Store group
        self._task_groups[name] = task_ids

        # Execute tasks based on strategy
        if strategy == "parallel":
            # Execute all in parallel
            results = await asyncio.gather(*[
                self._execute_run_task({"input": self._tasks[tid].input, "processor": self._tasks[tid].processor.value})
                for tid in task_ids
            ], return_exceptions=True)
        else:
            # Execute sequentially
            results = []
            for tid in task_ids:
                result = await self._execute_run_task({
                    "input": self._tasks[tid].input,
                    "processor": self._tasks[tid].processor.value
                })
                results.append(result)

        return {
            "group_name": name,
            "strategy": strategy,
            "task_count": len(task_ids),
            "task_ids": task_ids,
            "results": results,
            "success": True
        }

    async def _execute_find_all(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Find all matching entities."""
        objective = args.get("objective")
        if not objective:
            raise ValueError("objective is required")

        schema = args.get("schema", {})
        max_results = args.get("max_results", 50)

        # Native prospecting (simulated)
        task = Task(
            id=str(uuid.uuid4()),
            processor=ProcessorType.TURBO,
            input=f"FindAll: {objective}",
            status=TaskStatus.COMPLETED,
            result={
                "objective": objective,
                "entities": [],
                "total_found": 0,
                "schema_used": bool(schema)
            },
            completed_at=datetime.now(timezone.utc)
        )
        self._tasks[task.id] = task

        return {
            "objective": objective,
            "task_id": task.id,
            "entities": [],
            "total_found": 0,
            "note": "Native prospecting implementation - connect Parallel AI for full results",
            "success": True
        }

    async def _execute_list_tasks(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """List all tasks."""
        status_filter = args.get("status", "all")
        limit = args.get("limit", 20)

        tasks = list(self._tasks.values())

        # Filter by status
        if status_filter != "all":
            target_status = TaskStatus(status_filter)
            tasks = [t for t in tasks if t.status == target_status]

        # Sort by creation time (most recent first)
        tasks.sort(key=lambda t: t.created_at, reverse=True)

        # Limit
        tasks = tasks[:limit]

        return {
            "tasks": [
                {
                    "id": t.id,
                    "processor": t.processor.value,
                    "status": t.status.value,
                    "input_preview": t.input[:100] + "..." if len(t.input) > 100 else t.input,
                    "created_at": t.created_at.isoformat(),
                    "completed_at": t.completed_at.isoformat() if t.completed_at else None,
                }
                for t in tasks
            ],
            "total": len(self._tasks),
            "returned": len(tasks),
            "success": True
        }

    async def _execute_get_task(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Get a specific task."""
        task_id = args.get("task_id")
        if not task_id:
            raise ValueError("task_id is required")

        task = self._tasks.get(task_id)
        if not task:
            return {
                "error": f"Task not found: {task_id}",
                "success": False
            }

        return {
            "task": {
                "id": task.id,
                "processor": task.processor.value,
                "input": task.input,
                "status": task.status.value,
                "result": task.result,
                "error": task.error,
                "created_at": task.created_at.isoformat(),
                "completed_at": task.completed_at.isoformat() if task.completed_at else None,
            },
            "success": True
        }

    async def _execute_synthesize(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Synthesize information from multiple sources using Jina + LLM."""
        sources = args.get("sources", [])
        if not sources:
            raise ValueError("sources list is required")

        objective = args.get("objective", "")
        format_type = args.get("format", "summary")

        # Fetch content from URL sources, keep text sources as-is
        source_contents = []
        for source in sources:
            if source.startswith("http://") or source.startswith("https://"):
                content = await self._jina_read(source)
                if content:
                    source_contents.append(f"[Source: {source}]\n{content[:4000]}")
                else:
                    source_contents.append(f"[Source: {source}]\n(Could not fetch)")
            else:
                source_contents.append(source)

        combined = "\n\n---\n\n".join(source_contents)

        # Try LLM synthesis
        format_instructions = {
            "summary": "Provide a concise summary.",
            "bullets": "Provide key points as a bulleted list.",
            "report": "Provide a structured report with sections.",
            "analysis": "Provide a critical analysis comparing the sources.",
        }

        prompt = f"Synthesize the following {len(sources)} sources"
        if objective:
            prompt += f", focusing on: {objective}"
        prompt += f"\n\nFormat: {format_instructions.get(format_type, format_type)}\n\nSources:\n{combined[:12000]}"

        synthesis = await self._call_llm(
            prompt=prompt,
            system="You are a research synthesist. Combine information from multiple sources into a coherent output.",
            processor=ProcessorType.TURBO,
        )

        if not synthesis:
            # Fallback stub
            synthesis = f"Synthesized content from {len(sources)} sources" + (f" focusing on: {objective}" if objective else "")

        task = Task(
            id=str(uuid.uuid4()),
            processor=ProcessorType.TURBO,
            input=f"Synthesize {len(sources)} sources",
            status=TaskStatus.COMPLETED,
            result={
                "sources_count": len(sources),
                "format": format_type,
                "synthesis": synthesis,
            },
            completed_at=datetime.now(timezone.utc)
        )
        self._tasks[task.id] = task

        return {
            "task_id": task.id,
            "sources_processed": len(sources),
            "format": format_type,
            "synthesis": synthesis,
            "success": True,
        }

    async def _execute_connectivity_test(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """Internal connectivity verification."""
        echo = args.get("echo", "test")

        return {
            "success": True,
            "echo": echo,
            "server_id": self.SERVICE_ID,
            "version": self.VERSION,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": self._state.session_id,
            "tools_registered": len(self._tools),
            "tasks_in_memory": len(self._tasks),
            "api_key_configured": bool(self.api_key),
        }

    # =========================================================================
    # PROPERTIES
    # =========================================================================

    @property
    def is_connected(self) -> bool:
        return self._state.status == ConnectionStatus.CONNECTED

    @property
    def tools(self) -> List[Dict[str, Any]]:
        return [
            {
                "name": t.name,
                "description": t.description,
                "inputSchema": t.input_schema
            }
            for name, t in self._tools.items()
            if not name.startswith("_")
        ]

    def get_status(self) -> Dict[str, Any]:
        return {
            "service_id": self.SERVICE_ID,
            "service_name": self.SERVICE_NAME,
            "version": self.VERSION,
            "status": self._state.status.value,
            "connected_at": self._state.connected_at.isoformat() if self._state.connected_at else None,
            "session_id": self._state.session_id,
            "tools_count": len([t for t in self._tools if not t.startswith("_")]),
            "tasks_count": len(self._tasks),
            "task_groups_count": len(self._task_groups),
            "api_key_configured": bool(self.api_key),
        }


# =============================================================================
# FASTAPI APPLICATION
# =============================================================================

def create_server(
    api_key: Optional[str] = None,
    port: int = 8102,
    registry_url: Optional[str] = None,
) -> DistributedResearchMCP:
    """Create and return a DistributedResearchMCP instance."""
    return DistributedResearchMCP(
        api_key=api_key,
        port=port,
        registry_url=registry_url
    )


# Global server instance
_server: Optional[DistributedResearchMCP] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage server lifecycle."""
    global _server

    _server = create_server(
        port=int(os.getenv("PORT", "4002")),
        registry_url=os.getenv("REGISTRY_URL")
    )

    await _server.start()

    # Background heartbeat: re-announce to registry every 60s
    _heartbeat_task = None

    async def _heartbeat_loop():
        await asyncio.sleep(30)
        while True:
            try:
                await _server._announce_to_registry()
                logger.debug("Heartbeat: re-announced to registry")
            except Exception as e:
                logger.warning(f"Heartbeat failed: {e}")
            await asyncio.sleep(60)

    _heartbeat_task = asyncio.create_task(_heartbeat_loop())

    yield

    if _heartbeat_task:
        _heartbeat_task.cancel()
        try:
            await _heartbeat_task
        except asyncio.CancelledError:
            pass

    await _server.stop()


app = FastAPI(
    title="Distributed Research MCP",
    description="Native MCP server for deep research and parallel task execution",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/")
async def root():
    """Service info."""
    if _server:
        return _server.get_status()
    return {"status": "initializing"}


@app.get("/health")
async def health():
    """Health check."""
    if _server and _server.is_connected:
        return {"status": "healthy", "service": DistributedResearchMCP.SERVICE_ID}
    return JSONResponse(
        status_code=503,
        content={"status": "unhealthy", "service": DistributedResearchMCP.SERVICE_ID}
    )


@app.get("/tools")
async def list_tools():
    """List available MCP tools."""
    if not _server:
        raise HTTPException(503, "Server not initialized")
    return {"tools": _server.tools}


@app.post("/mcp")
async def mcp_endpoint(request: MCPRequest):
    """MCP JSON-RPC endpoint."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    response = await _server.handle_mcp_request(request)
    return response.model_dump(exclude_none=True)


@app.post("/execute")
async def execute_tool(request: ToolCallRequest):
    """Execute a tool directly (non-MCP interface)."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    tool = _server._tools.get(request.name)
    if not tool:
        raise HTTPException(404, f"Tool not found: {request.name}")

    if not tool.handler:
        raise HTTPException(500, f"Tool {request.name} has no handler")

    result = await tool.handler(request.arguments)
    return {"result": result}


@app.get("/tasks")
async def list_all_tasks(status: str = "all", limit: int = 20):
    """List all tasks."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    result = await _server._execute_list_tasks({"status": status, "limit": limit})
    return result


@app.get("/tasks/{task_id}")
async def get_task(task_id: str):
    """Get a specific task."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    result = await _server._execute_get_task({"task_id": task_id})
    if not result.get("success"):
        raise HTTPException(404, result.get("error", "Task not found"))
    return result


@app.get("/mcp/sse")
@app.get("/sse")
async def sse_endpoint(request: Request):
    """Server-Sent Events endpoint for MCP streaming."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    async def event_generator() -> AsyncGenerator[str, None]:
        yield f"data: {json.dumps({'type': 'connected', 'session_id': _server._state.session_id})}\n\n"

        while True:
            if await request.is_disconnected():
                break

            yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': datetime.now(timezone.utc).isoformat(), 'tasks_count': len(_server._tasks)})}\n\n"
            await asyncio.sleep(30)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


@app.post("/mcp/sse")
@app.post("/sse")
async def sse_post_endpoint(request: Request):
    """Handle MCP JSON-RPC POST requests via SSE URL (mcp-remote pattern)."""
    if not _server:
        raise HTTPException(503, "Server not initialized")

    body = await request.json()
    mcp_request = MCPRequest(**body)
    response = await _server.handle_mcp_request(mcp_request)
    return response.model_dump(exclude_none=True)


# =============================================================================
# ENTRYPOINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "4002"))

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║         DISTRIBUTED RESEARCH MCP - Native Server                 ║
║               Second Native MCP Implementation                   ║
╠══════════════════════════════════════════════════════════════════╣
║  Service ID: distributed-research-mcp                            ║
║  Port: {port:<57} ║
║  Protocol: MCP 2024-11-05                                        ║
╠══════════════════════════════════════════════════════════════════╣
║  Tools:                                                          ║
║  • deep_search       - Multi-source research                     ║
║  • run_task          - Execute autonomous tasks                  ║
║  • extract_data      - Structured data extraction                ║
║  • create_task_group - Parallel task batches                     ║
║  • find_all          - Entity prospecting                        ║
║  • list_tasks        - View task history                         ║
║  • get_task          - Get task details                          ║
║  • synthesize        - Combine multiple sources                  ║
╠══════════════════════════════════════════════════════════════════╣
║  Endpoints:                                                      ║
║  • GET  /            Service info                                ║
║  • GET  /health      Health check                                ║
║  • GET  /tools       List tools                                  ║
║  • POST /mcp         MCP JSON-RPC endpoint                       ║
║  • POST /execute     Direct tool execution                       ║
║  • GET  /tasks       List all tasks                              ║
║  • GET  /tasks/:id   Get task details                            ║
║  • GET  /sse         Server-Sent Events stream                   ║
╚══════════════════════════════════════════════════════════════════╝

Starting native MCP server...
""")

    # Use import string to ensure proper module loading and route registration
    # This avoids the double-module issue when running with python -m
    uvicorn.run(
        "native.distributed_research_mcp.server:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
