"""
Integration Server - Unified MCP integration server.

Combines multiple MCP adapters (Jina, Parallel, etc.) into a single
FastAPI server that:
1. Discovers tools from all connected MCP servers
2. Registers them with our tool registry
3. Provides a unified API for tool discovery and execution
4. Supports real-time SSE for tool status updates
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Type

from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field

from ..tools_api import Tools, ToolDefinition, ToolType
from .mcp_adapter import MCPAdapter, MCPServerConfig
from .jina_adapter import JinaAdapter
from .parallel_adapter import ParallelAdapter

logger = logging.getLogger(__name__)


# =============================================================================
# REQUEST/RESPONSE MODELS
# =============================================================================

class ToolExecuteRequest(BaseModel):
    """Request to execute a tool."""
    tool_name: str = Field(..., description="Name of the tool to execute")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Tool inputs")
    adapter: Optional[str] = Field(None, description="Specific adapter to use")


class ToolExecuteResponse(BaseModel):
    """Response from tool execution."""
    tool_name: str
    adapter: str
    status: str
    result: Optional[Any] = None
    error: Optional[str] = None
    execution_time_ms: float


class AdapterStatus(BaseModel):
    """Status of an MCP adapter."""
    name: str
    url: str
    connected: bool
    tool_count: int
    tags: List[str]


class ServerStatus(BaseModel):
    """Overall server status."""
    status: str
    adapters: List[AdapterStatus]
    total_tools: int
    uptime_seconds: float


# =============================================================================
# INTEGRATION SERVER
# =============================================================================

class IntegrationServer:
    """
    Unified integration server for multiple MCP providers.

    Usage:
        server = IntegrationServer()

        # Add adapters
        server.add_adapter(JinaAdapter(api_key="..."))
        server.add_adapter(ParallelAdapter(api_key="..."))

        # Get FastAPI app
        app = server.create_app()

        # Run
        uvicorn.run(app, host="0.0.0.0", port=8200)
    """

    def __init__(
        self,
        tools_registry: Optional[Tools] = None,
        auto_register: bool = True,
    ):
        self.tools_registry = tools_registry or Tools(
            service_name="mcp-integration",
            service_url="http://localhost:8200",
        )
        self.auto_register = auto_register

        self._adapters: Dict[str, MCPAdapter] = {}
        self._tool_to_adapter: Dict[str, str] = {}  # tool_name -> adapter_name
        self._start_time = datetime.utcnow()
        self._app: Optional[FastAPI] = None

    def add_adapter(self, adapter: MCPAdapter) -> None:
        """Add an MCP adapter."""
        adapter.tools_registry = self.tools_registry
        self._adapters[adapter.name] = adapter
        logger.info(f"Added adapter: {adapter.name}")

    def remove_adapter(self, name: str) -> None:
        """Remove an adapter."""
        if name in self._adapters:
            del self._adapters[name]
            # Remove tool mappings
            self._tool_to_adapter = {
                k: v for k, v in self._tool_to_adapter.items() if v != name
            }

    async def connect_all(self) -> Dict[str, bool]:
        """Connect all adapters and discover tools."""
        results = {}

        for name, adapter in self._adapters.items():
            try:
                success = await adapter.connect()
                results[name] = success

                if success:
                    # Map tools to adapter
                    for tool in adapter.get_discovered_tools():
                        self._tool_to_adapter[tool.name] = name

                    # Register with our registry
                    if self.auto_register:
                        await adapter.register_tools()

                    logger.info(f"Connected {name}: {len(adapter.get_discovered_tools())} tools")

            except Exception as e:
                logger.error(f"Failed to connect {name}: {e}")
                results[name] = False

        return results

    async def disconnect_all(self) -> None:
        """Disconnect all adapters."""
        for adapter in self._adapters.values():
            try:
                await adapter.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting {adapter.name}: {e}")

    def get_all_tools(self) -> List[ToolDefinition]:
        """Get all tools from all adapters."""
        return list(self.tools_registry._tools.values())

    def get_tools_by_adapter(self, adapter_name: str) -> List[ToolDefinition]:
        """Get tools from a specific adapter."""
        tool_names = [
            name for name, adapter in self._tool_to_adapter.items()
            if adapter == adapter_name
        ]
        return [
            t for t in self.tools_registry._tools.values()
            if t.name in tool_names
        ]

    async def execute_tool(
        self,
        tool_name: str,
        inputs: Dict[str, Any],
        adapter_name: Optional[str] = None
    ) -> Any:
        """Execute a tool, routing to the appropriate adapter."""
        # Find the adapter
        if adapter_name:
            if adapter_name not in self._adapters:
                raise ValueError(f"Unknown adapter: {adapter_name}")
            adapter = self._adapters[adapter_name]
        else:
            adapter_name = self._tool_to_adapter.get(tool_name)
            if not adapter_name:
                raise ValueError(f"Unknown tool: {tool_name}")
            adapter = self._adapters[adapter_name]

        # Execute
        return await adapter.execute_tool(tool_name, inputs)

    def get_status(self) -> ServerStatus:
        """Get server status."""
        adapters = []
        total_tools = 0

        for name, adapter in self._adapters.items():
            tool_count = len(adapter.get_discovered_tools())
            total_tools += tool_count
            adapters.append(AdapterStatus(
                name=name,
                url=adapter.config.url,
                connected=adapter._connected,
                tool_count=tool_count,
                tags=adapter.config.tags,
            ))

        uptime = (datetime.utcnow() - self._start_time).total_seconds()

        return ServerStatus(
            status="healthy" if any(a.connected for a in adapters) else "degraded",
            adapters=adapters,
            total_tools=total_tools,
            uptime_seconds=uptime,
        )

    def create_app(self) -> FastAPI:
        """Create FastAPI application."""

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            logger.info("Starting Integration Server...")
            await self.connect_all()
            yield
            # Shutdown
            logger.info("Shutting down Integration Server...")
            await self.disconnect_all()

        app = FastAPI(
            title="MCP Integration Server",
            description="Unified API for multiple MCP tool providers (Jina, Parallel, etc.)",
            version="1.0.0",
            lifespan=lifespan,
        )

        # CORS
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        # Store reference
        self._app = app

        # Routes
        @app.get("/health")
        async def health():
            return self.get_status().model_dump()

        @app.get("/")
        async def root():
            status = self.get_status()
            return {
                "name": "MCP Integration Server",
                "version": "1.0.0",
                "status": status.status,
                "adapters": [a.name for a in status.adapters],
                "total_tools": status.total_tools,
                "endpoints": {
                    "tools": "/tools",
                    "tools_mcp": "/tools/mcp",
                    "execute": "/execute",
                    "adapters": "/adapters",
                }
            }

        @app.get("/adapters")
        async def list_adapters():
            return {
                "adapters": [
                    AdapterStatus(
                        name=name,
                        url=adapter.config.url,
                        connected=adapter._connected,
                        tool_count=len(adapter.get_discovered_tools()),
                        tags=adapter.config.tags,
                    ).model_dump()
                    for name, adapter in self._adapters.items()
                ]
            }

        @app.get("/adapters/{name}")
        async def get_adapter(name: str):
            if name not in self._adapters:
                raise HTTPException(404, f"Adapter not found: {name}")

            adapter = self._adapters[name]
            return {
                "name": name,
                "url": adapter.config.url,
                "connected": adapter._connected,
                "tools": [
                    {
                        "name": t.name,
                        "description": t.description,
                    }
                    for t in adapter.get_discovered_tools()
                ]
            }

        @app.get("/tools")
        async def list_tools(adapter: Optional[str] = None):
            if adapter:
                tools = self.get_tools_by_adapter(adapter)
            else:
                tools = self.get_all_tools()

            return {
                "tools": [
                    {
                        "name": t.name,
                        "description": t.description,
                        "category": t.category,
                        "tags": t.tags,
                        "adapter": self._tool_to_adapter.get(t.name),
                        "input_schema": t.input_schema.model_dump(),
                    }
                    for t in tools
                ]
            }

        @app.get("/tools/mcp")
        async def get_mcp_tools():
            """Get all tools in MCP format."""
            tools = []
            for adapter in self._adapters.values():
                tools.extend(await adapter.list_tools_mcp_format())
            return {"tools": tools}

        @app.get("/tools/{tool_name}")
        async def get_tool(tool_name: str):
            tool = self.tools_registry.get_by_name(tool_name)
            if not tool:
                raise HTTPException(404, f"Tool not found: {tool_name}")

            return {
                "tool": tool.model_dump(),
                "adapter": self._tool_to_adapter.get(tool_name),
            }

        @app.post("/execute", response_model=ToolExecuteResponse)
        async def execute_tool(request: ToolExecuteRequest):
            start_time = datetime.utcnow()

            try:
                result = await self.execute_tool(
                    request.tool_name,
                    request.inputs,
                    request.adapter,
                )

                elapsed = (datetime.utcnow() - start_time).total_seconds() * 1000

                return ToolExecuteResponse(
                    tool_name=request.tool_name,
                    adapter=request.adapter or self._tool_to_adapter.get(request.tool_name, "unknown"),
                    status="success",
                    result=result,
                    execution_time_ms=elapsed,
                )

            except Exception as e:
                elapsed = (datetime.utcnow() - start_time).total_seconds() * 1000
                logger.error(f"Tool execution failed: {e}")

                return ToolExecuteResponse(
                    tool_name=request.tool_name,
                    adapter=request.adapter or self._tool_to_adapter.get(request.tool_name, "unknown"),
                    status="error",
                    error=str(e),
                    execution_time_ms=elapsed,
                )

        @app.post("/tools/{tool_name}/execute")
        async def execute_tool_by_name(tool_name: str, request: Request):
            body = await request.json()
            inputs = body.get("inputs", body.get("arguments", body))

            start_time = datetime.utcnow()

            try:
                result = await self.execute_tool(tool_name, inputs)
                elapsed = (datetime.utcnow() - start_time).total_seconds() * 1000

                return {
                    "tool_name": tool_name,
                    "status": "success",
                    "result": result,
                    "execution_time_ms": elapsed,
                }

            except Exception as e:
                elapsed = (datetime.utcnow() - start_time).total_seconds() * 1000
                raise HTTPException(500, detail=str(e))

        @app.post("/adapters/{name}/reconnect")
        async def reconnect_adapter(name: str):
            if name not in self._adapters:
                raise HTTPException(404, f"Adapter not found: {name}")

            adapter = self._adapters[name]
            await adapter.disconnect()
            success = await adapter.connect()

            if success and self.auto_register:
                await adapter.register_tools()
                for tool in adapter.get_discovered_tools():
                    self._tool_to_adapter[tool.name] = name

            return {
                "name": name,
                "reconnected": success,
                "tool_count": len(adapter.get_discovered_tools()),
            }

        return app


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def create_integration_server(
    jina_api_key: Optional[str] = None,
    parallel_api_key: Optional[str] = None,
    parallel_use_local: bool = True,
    parallel_port: int = 8100,
    auto_register: bool = True,
    additional_adapters: Optional[List[MCPAdapter]] = None,
) -> IntegrationServer:
    """
    Factory function to create a configured integration server.

    Args:
        jina_api_key: Jina AI API key (or set JINA_API_KEY env var)
        parallel_api_key: Parallel AI API key (or set PARALLEL_API_KEY env var)
        parallel_use_local: Use local Parallel MCP server
        parallel_port: Port for local Parallel MCP server
        auto_register: Auto-register tools on connect
        additional_adapters: Additional MCP adapters to add

    Returns:
        Configured IntegrationServer
    """
    server = IntegrationServer(auto_register=auto_register)

    # Add Jina adapter
    if jina_api_key or os.getenv("JINA_API_KEY"):
        server.add_adapter(JinaAdapter(api_key=jina_api_key))

    # Add Parallel adapter
    if parallel_api_key or os.getenv("PARALLEL_API_KEY"):
        server.add_adapter(ParallelAdapter(
            api_key=parallel_api_key,
            use_local_mcp=parallel_use_local,
            local_mcp_port=parallel_port,
        ))

    # Add any additional adapters
    if additional_adapters:
        for adapter in additional_adapters:
            server.add_adapter(adapter)

    return server
