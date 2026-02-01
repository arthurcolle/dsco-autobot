"""
Parallel AI MCP Adapter - Integration with Parallel's task execution API.

Parallel provides:
- Web search with structured extraction
- Task runs with different processors (lite, turbo, premium)
- Task groups for batch processing
- FindAll for prospecting
- Monitors for scheduled web monitoring
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional
from enum import Enum

import httpx

from .mcp_adapter import MCPAdapter, MCPServerConfig, MCPTransport, MCPTool

logger = logging.getLogger(__name__)


class ParallelProcessor(str, Enum):
    """Parallel AI processor types."""
    LITE = "lite"
    TURBO = "turbo"
    PREMIUM = "premium"


# Parallel's known tools based on their API
PARALLEL_TOOLS = [
    {
        "name": "search",
        "description": "Search the web and extract structured information. Returns results with titles, URLs, and excerpts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "objective": {
                    "type": "string",
                    "description": "Search query or research objective"
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results",
                    "default": 10
                }
            },
            "required": ["objective"]
        }
    },
    {
        "name": "extract",
        "description": "Extract structured data from a URL based on a schema.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "URL to extract from"
                },
                "schema": {
                    "type": "object",
                    "description": "JSON schema for extraction"
                }
            },
            "required": ["url"]
        }
    },
    {
        "name": "task_run",
        "description": "Run a task with Parallel's AI processors. Supports lite, turbo, and premium processors.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "processor": {
                    "type": "string",
                    "enum": ["lite", "turbo", "premium"],
                    "description": "Processor type",
                    "default": "lite"
                },
                "input": {
                    "type": "string",
                    "description": "Task input/prompt"
                },
                "task_spec": {
                    "type": "object",
                    "description": "Optional task specification with output schema",
                    "properties": {
                        "output_schema": {"type": "object"},
                        "instructions": {"type": "string"}
                    }
                },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in seconds",
                    "default": 300
                }
            },
            "required": ["input"]
        }
    },
    {
        "name": "task_group_create",
        "description": "Create a batch of tasks to run in parallel.",
        "inputSchema": {
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
                            "processor": {"type": "string"},
                            "input": {"type": "string"}
                        }
                    },
                    "description": "List of tasks to run"
                },
                "strategy": {
                    "type": "string",
                    "enum": ["cost", "speed", "balanced", "quality"],
                    "default": "balanced"
                }
            },
            "required": ["tasks"]
        }
    },
    {
        "name": "findall",
        "description": "Find all entities matching criteria across the web (prospecting).",
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
                    "default": 50
                }
            },
            "required": ["objective"]
        }
    },
    {
        "name": "monitor_create",
        "description": "Create a scheduled web monitor to track changes.",
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
                    "description": "Cron schedule or interval"
                },
                "notify_on_change": {
                    "type": "boolean",
                    "default": True
                }
            },
            "required": ["name", "url", "schedule"]
        }
    },
    {
        "name": "get_stored_tasks",
        "description": "Retrieve past task results from storage.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "default": 20
                },
                "status": {
                    "type": "string",
                    "enum": ["completed", "failed", "running", "all"],
                    "default": "all"
                }
            }
        }
    }
]


class ParallelAdapter(MCPAdapter):
    """
    Adapter for Parallel AI's API.

    Usage:
        adapter = ParallelAdapter(api_key="your-parallel-key")
        await adapter.connect()
        result = await adapter.search("AI startups funding 2024")
        result = await adapter.task_run("Summarize this article", processor="turbo")
    """

    DEFAULT_URL = "https://api.parallel.ai"
    MCP_URL = "http://localhost:8100"  # Local MCP server if running

    def __init__(
        self,
        api_key: Optional[str] = None,
        url: Optional[str] = None,
        use_local_mcp: bool = True,
        local_mcp_port: int = 8100,
        tools_registry=None,
        **kwargs
    ):
        api_key = api_key or os.getenv("PARALLEL_API_KEY")

        # Use local MCP server if available, otherwise direct API
        if use_local_mcp:
            base_url = url or f"http://localhost:{local_mcp_port}"
        else:
            base_url = url or self.DEFAULT_URL

        config = MCPServerConfig(
            name="parallel-ai",
            url=base_url,
            transport=MCPTransport.HTTP,
            api_key=api_key,
            api_key_header="X-API-Key",
            api_key_prefix="",  # No prefix for Parallel
            tags=["search", "extraction", "tasks", "parallel"],
            category="research",
            metadata={
                "provider": "Parallel AI",
                "api_url": self.DEFAULT_URL,
                "use_local_mcp": use_local_mcp,
            },
            **kwargs
        )

        super().__init__(config, tools_registry)
        self._use_local_mcp = use_local_mcp

    async def _discover_tools(self):
        """Discover Parallel tools."""
        # Start with known tools
        for tool_data in PARALLEL_TOOLS:
            tool = MCPTool(
                name=tool_data["name"],
                description=tool_data["description"],
                input_schema=tool_data["inputSchema"],
                server_name=self.name,
                server_url=self.config.url,
            )
            self._discovered_tools[tool.name] = tool

        # Try to get tools from local MCP server if running
        if self._use_local_mcp:
            try:
                response = await self._client.get(
                    f"{self.config.url}/tools",
                    headers=self._headers,
                    timeout=5.0,
                )
                if response.status_code == 200:
                    data = response.json()
                    tools_list = data.get("tools", [])
                    for tool_data in tools_list:
                        name = tool_data.get("name")
                        if name and name not in self._discovered_tools:
                            tool = MCPTool(
                                name=name,
                                description=tool_data.get("description", ""),
                                input_schema=tool_data.get("inputSchema", {}),
                                server_name=self.name,
                                server_url=self.config.url,
                            )
                            self._discovered_tools[name] = tool
            except Exception as e:
                logger.debug(f"Local MCP discovery failed (using known tools): {e}")

    async def execute_tool(self, tool_name: str, inputs: Dict[str, Any]) -> Any:
        """Execute a Parallel tool."""
        if not self._connected:
            raise RuntimeError("Not connected to Parallel AI")

        mcp_tool = self._discovered_tools.get(tool_name)
        if not mcp_tool:
            raise ValueError(f"Tool not found: {tool_name}")

        # Route to appropriate endpoint
        try:
            if tool_name == "search":
                return await self._execute_search(inputs)
            elif tool_name == "extract":
                return await self._execute_extract(inputs)
            elif tool_name == "task_run":
                return await self._execute_task_run(inputs)
            elif tool_name == "task_group_create":
                return await self._execute_task_group(inputs)
            elif tool_name == "findall":
                return await self._execute_findall(inputs)
            elif tool_name == "monitor_create":
                return await self._execute_monitor(inputs)
            elif tool_name == "get_stored_tasks":
                return await self._execute_get_stored(inputs)
            else:
                # Generic execution
                return await self._execute_generic(tool_name, inputs)

        except Exception as e:
            logger.error(f"Failed to execute {tool_name}: {e}")
            raise

    async def _execute_search(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute search endpoint."""
        response = await self._client.post(
            f"{self.config.url}/v1beta/search",
            json=inputs,
            headers=self._headers,
            timeout=60.0,
        )
        response.raise_for_status()
        return response.json()

    async def _execute_extract(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute extract endpoint."""
        response = await self._client.post(
            f"{self.config.url}/v1beta/extract",
            json=inputs,
            headers=self._headers,
            timeout=60.0,
        )
        response.raise_for_status()
        return response.json()

    async def _execute_task_run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a managed task run with polling."""
        # Create task
        response = await self._client.post(
            f"{self.config.url}/v1/tasks/runs",
            json=inputs,
            headers=self._headers,
        )
        response.raise_for_status()
        task_data = response.json()

        task_id = task_data.get("id") or task_data.get("task_id")
        if not task_id:
            return task_data

        # Poll for completion
        timeout = inputs.get("timeout", 300)
        poll_interval = inputs.get("poll_interval", 2.0)
        elapsed = 0

        while elapsed < timeout:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

            status_response = await self._client.get(
                f"{self.config.url}/v1/tasks/runs/{task_id}",
                headers=self._headers,
            )

            if status_response.status_code == 200:
                result = status_response.json()
                status = result.get("status", "")

                if status in ("completed", "failed", "cancelled"):
                    return result

        raise TimeoutError(f"Task {task_id} did not complete within {timeout}s")

    async def _execute_task_group(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Create and monitor a task group."""
        response = await self._client.post(
            f"{self.config.url}/v1beta/tasks/groups",
            json=inputs,
            headers=self._headers,
        )
        response.raise_for_status()
        return response.json()

    async def _execute_findall(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute findall prospecting."""
        response = await self._client.post(
            f"{self.config.url}/v1beta/findall/runs",
            json=inputs,
            headers=self._headers,
            timeout=120.0,
        )
        response.raise_for_status()
        return response.json()

    async def _execute_monitor(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Create a monitor."""
        response = await self._client.post(
            f"{self.config.url}/v1alpha/monitors",
            json=inputs,
            headers=self._headers,
        )
        response.raise_for_status()
        return response.json()

    async def _execute_get_stored(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Get stored tasks."""
        params = {k: v for k, v in inputs.items() if v is not None}
        response = await self._client.get(
            f"{self.config.url}/v1/stored/tasks",
            params=params,
            headers=self._headers,
        )
        response.raise_for_status()
        return response.json()

    async def _execute_generic(self, tool_name: str, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Generic tool execution via MCP protocol."""
        if self._use_local_mcp:
            response = await self._client.post(
                f"{self.config.url}/mcp/tools/{tool_name}",
                json=inputs,
                headers=self._headers,
            )
        else:
            response = await self._client.post(
                f"{self.config.url}/v1/tools/{tool_name}",
                json=inputs,
                headers=self._headers,
            )
        response.raise_for_status()
        return response.json()

    # Convenience methods
    async def search(self, objective: str, max_results: int = 10) -> Dict[str, Any]:
        """Search the web."""
        return await self.execute_tool("search", {
            "objective": objective,
            "max_results": max_results
        })

    async def extract(self, url: str, schema: Optional[Dict] = None) -> Dict[str, Any]:
        """Extract data from URL."""
        inputs = {"url": url}
        if schema:
            inputs["schema"] = schema
        return await self.execute_tool("extract", inputs)

    async def task_run(
        self,
        input_text: str,
        processor: str = "lite",
        task_spec: Optional[Dict] = None,
        timeout: int = 300
    ) -> Dict[str, Any]:
        """Run a task with specified processor."""
        inputs = {
            "processor": processor,
            "input": input_text,
            "timeout": timeout
        }
        if task_spec:
            inputs["task_spec"] = task_spec
        return await self.execute_tool("task_run", inputs)

    async def findall(
        self,
        objective: str,
        schema: Optional[Dict] = None,
        max_results: int = 50
    ) -> Dict[str, Any]:
        """Find all matching entities."""
        inputs = {
            "objective": objective,
            "max_results": max_results
        }
        if schema:
            inputs["schema"] = schema
        return await self.execute_tool("findall", inputs)
