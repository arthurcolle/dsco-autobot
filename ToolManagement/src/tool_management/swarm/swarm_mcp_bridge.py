"""
Swarm-MCP Bridge - Routes swarm tasks to MCP backends.

Core service that connects the Swarm Economy with distributed MCP backends,
enabling autonomous agent execution, skill learning, and event coordination.

Includes OpenRouter integration for LLM-powered task processing.
"""

from __future__ import annotations

import asyncio
import os
import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import httpx

# OpenRouter configuration
OPENROUTER_API_KEY = os.environ.get(
    "OPENROUTER_API_KEY",
    "[REDACTED-OPENROUTER-KEY]"
)
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
DEFAULT_MODEL = "moonshotai/kimi-k2.5"  # Kimi K2.5 - better than Claude per user

from .mcp_task_types import MCPTaskType, MCP_TASK_ROUTING, MCP_BACKEND_CAPABILITIES, is_mcp_task_type
from .agent_capabilities import AgentMCPProfile, MCPCapability
from .swarm_events import (
    SwarmEvent,
    SwarmEventType,
    task_completed_event,
    skill_learned_event,
    workflow_completed_event,
    mcp_skill_captured_event,
)
from .persistence import get_swarm_db

logger = logging.getLogger(__name__)


class OpenRouterClient:
    """Client for OpenRouter LLM API calls."""

    def __init__(
        self,
        api_key: str = OPENROUTER_API_KEY,
        base_url: str = OPENROUTER_BASE_URL,
        default_model: str = DEFAULT_MODEL,
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.default_model = default_model
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=120.0)
        return self._client

    async def chat(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 4096,
        **kwargs,
    ) -> Dict[str, Any]:
        """Send a chat completion request to OpenRouter."""
        client = await self._get_client()

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://tools.distributed.systems",
            "X-Title": "Swarm MCP Bridge",
        }

        payload = {
            "model": model or self.default_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            **kwargs,
        }

        try:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"OpenRouter HTTP error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"OpenRouter request error: {e}")
            raise

    async def complete_task(
        self,
        task_description: str,
        context: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
    ) -> str:
        """Use LLM to complete a task and return the result."""
        system_prompt = """You are an autonomous agent in a swarm economy.
Your task is to execute the given task and return a structured result.
Be concise and focused on completing the task effectively."""

        user_content = f"Task: {task_description}"
        if context:
            user_content += f"\n\nContext: {context}"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ]

        result = await self.chat(messages, model=model)
        return result.get("choices", [{}])[0].get("message", {}).get("content", "")

    async def analyze_and_route(
        self,
        query: str,
        available_tools: List[str],
    ) -> Dict[str, Any]:
        """Use LLM to analyze a query and determine which tools to use."""
        system_prompt = f"""You are a task router for an MCP swarm system.
Given a user query, determine which tools should be used and in what order.

Available tools: {', '.join(available_tools)}

Respond with JSON:
{{"tools": ["tool1", "tool2"], "reasoning": "...", "parameters": {{}}}}"""

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": query},
        ]

        result = await self.chat(messages, temperature=0.3)
        content = result.get("choices", [{}])[0].get("message", {}).get("content", "{}")

        # Parse JSON from response
        try:
            import json
            # Handle markdown code blocks
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            return json.loads(content.strip())
        except:
            return {"tools": [], "reasoning": content, "parameters": {}}

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


class MCPBackendClient:
    """Client for communicating with MCP backends."""

    def __init__(self, backend_id: str, base_url: str, timeout: float = 30.0):
        self.backend_id = backend_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call an MCP tool via JSON-RPC."""
        client = await self._get_client()

        request_id = str(uuid4())
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments,
            },
        }

        try:
            response = await client.post(
                f"{self.base_url}/mcp",
                json=payload,
            )
            response.raise_for_status()
            result = response.json()

            if "error" in result:
                raise MCPError(
                    result["error"].get("code", -1),
                    result["error"].get("message", "Unknown error"),
                )

            return result.get("result", {})

        except httpx.HTTPStatusError as e:
            raise MCPError(-32000, f"HTTP error: {e.response.status_code}")
        except httpx.RequestError as e:
            raise MCPError(-32001, f"Request error: {str(e)}")

    async def list_tools(self) -> List[Dict[str, Any]]:
        """List available tools from the backend."""
        client = await self._get_client()

        payload = {
            "jsonrpc": "2.0",
            "id": str(uuid4()),
            "method": "tools/list",
            "params": {},
        }

        try:
            response = await client.post(f"{self.base_url}/mcp", json=payload)
            response.raise_for_status()
            result = response.json()
            return result.get("result", {}).get("tools", [])
        except Exception as e:
            logger.warning(f"Failed to list tools from {self.backend_id}: {e}")
            return []

    async def health_check(self) -> bool:
        """Check if the backend is healthy."""
        try:
            client = await self._get_client()
            response = await client.get(f"{self.base_url}/mcp/health", timeout=5.0)
            return response.status_code == 200
        except Exception:
            return False

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()


class MCPError(Exception):
    """Error from MCP backend."""

    def __init__(self, code: int, message: str):
        self.code = code
        self.message = message
        super().__init__(f"MCP Error {code}: {message}")


class SwarmMCPBridge:
    """
    Bridge between Swarm Economy and MCP backends.

    Handles:
    - Task routing to appropriate MCP backends
    - LLM-powered task execution via OpenRouter
    - Multi-step workflow execution
    - Skill pattern learning and storage
    - Event publishing for coordination
    """

    def __init__(
        self,
        backend_urls: Optional[Dict[str, str]] = None,
        openrouter_api_key: Optional[str] = None,
        default_model: str = DEFAULT_MODEL,
    ):
        """
        Initialize the bridge.

        Args:
            backend_urls: Map of backend_id to base URL.
                         If not provided, uses defaults.
            openrouter_api_key: OpenRouter API key for LLM calls.
            default_model: Default model for LLM tasks.
        """
        self.backend_urls = backend_urls or {
            "distributed-search-mcp": "https://tools.distributed.systems",
            "distributed-research-mcp": "https://tools.distributed.systems",
            "distributed-memory-mcp": "https://tools.distributed.systems",
            "distributed-publish-mcp": "https://tools.distributed.systems",
        }

        self._clients: Dict[str, MCPBackendClient] = {}
        self._event_subscribers: List[Dict[str, Any]] = []
        self._skill_threshold = 0.8  # Minimum score to learn a skill

        # OpenRouter client for LLM-powered tasks
        self._openrouter = OpenRouterClient(
            api_key=openrouter_api_key or OPENROUTER_API_KEY,
            default_model=default_model,
        )

    def _get_client(self, backend_id: str) -> MCPBackendClient:
        """Get or create a client for a backend."""
        if backend_id not in self._clients:
            if backend_id not in self.backend_urls:
                raise ValueError(f"Unknown backend: {backend_id}")

            self._clients[backend_id] = MCPBackendClient(
                backend_id=backend_id,
                base_url=self.backend_urls[backend_id],
            )

        return self._clients[backend_id]

    async def execute_mcp_task(
        self,
        task_type: str,
        input_data: Dict[str, Any],
        agent_id: Optional[str] = None,
        swarm_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a single MCP task.

        Args:
            task_type: MCPTaskType value (e.g., "search_web")
            input_data: Arguments for the MCP tool
            agent_id: Optional agent executing the task
            swarm_id: Optional swarm context

        Returns:
            Task execution result
        """
        if not is_mcp_task_type(task_type):
            raise ValueError(f"Invalid MCP task type: {task_type}")

        backend_id, tool_name = MCP_TASK_ROUTING[MCPTaskType(task_type)]
        client = self._get_client(backend_id)

        start_time = datetime.now(timezone.utc)

        try:
            result = await client.call_tool(tool_name, input_data)

            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

            return {
                "success": True,
                "task_type": task_type,
                "backend_id": backend_id,
                "tool": tool_name,
                "result": result,
                "execution_time": execution_time,
                "agent_id": agent_id,
            }

        except MCPError as e:
            return {
                "success": False,
                "task_type": task_type,
                "backend_id": backend_id,
                "tool": tool_name,
                "error": e.message,
                "error_code": e.code,
                "agent_id": agent_id,
            }

        except Exception as e:
            return {
                "success": False,
                "task_type": task_type,
                "backend_id": backend_id,
                "tool": tool_name,
                "error": str(e),
                "agent_id": agent_id,
            }

    async def execute_llm_task(
        self,
        task_description: str,
        context: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        swarm_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a task using LLM (OpenRouter).

        Args:
            task_description: Natural language description of the task
            context: Additional context for the task
            model: Override the default model
            agent_id: Optional agent executing the task
            swarm_id: Optional swarm context

        Returns:
            Task execution result with LLM response
        """
        import time
        start_time = time.time()
        used_model = model or self._openrouter.default_model

        try:
            result = await self._openrouter.complete_task(
                task_description=task_description,
                context=context,
                model=model,
            )

            latency_ms = int((time.time() - start_time) * 1000)

            # Track usage in database
            try:
                db = get_swarm_db()
                # Estimate tokens (rough: 4 chars per token)
                prompt_tokens = len(task_description) // 4 + (len(str(context or "")) // 4)
                completion_tokens = len(result) // 4

                # Estimate cost based on model (very rough)
                if "kimi" in used_model.lower():
                    cost_per_1k = 0.0003  # Kimi K2.5 is cheap
                elif "opus" in used_model.lower():
                    cost_per_1k = 0.015  # Claude Opus
                elif "sonnet" in used_model.lower():
                    cost_per_1k = 0.003  # Claude Sonnet
                else:
                    cost_per_1k = 0.001  # Default

                total_tokens = prompt_tokens + completion_tokens
                cost_usd = (total_tokens / 1000) * cost_per_1k

                db.track_llm_usage(
                    swarm_id=swarm_id,
                    agent_id=agent_id,
                    model=used_model,
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    cost_usd=cost_usd,
                    task_type="llm_task",
                    success=True,
                    latency_ms=latency_ms,
                )
            except Exception as track_err:
                logger.warning(f"Failed to track LLM usage: {track_err}")

            return {
                "success": True,
                "task_type": "llm_task",
                "model": used_model,
                "result": result,
                "execution_time": latency_ms / 1000,
                "agent_id": agent_id,
                "swarm_id": swarm_id,
            }

        except Exception as e:
            # Track failed request
            try:
                latency_ms = int((time.time() - start_time) * 1000)
                db = get_swarm_db()
                db.track_llm_usage(
                    swarm_id=swarm_id,
                    agent_id=agent_id,
                    model=used_model,
                    prompt_tokens=len(task_description) // 4,
                    completion_tokens=0,
                    cost_usd=0.0,
                    task_type="llm_task",
                    success=False,
                    latency_ms=latency_ms,
                )
            except:
                pass

            return {
                "success": False,
                "task_type": "llm_task",
                "error": str(e),
                "agent_id": agent_id,
                "swarm_id": swarm_id,
            }

    async def smart_route_and_execute(
        self,
        query: str,
        swarm_id: str,
        agent_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Use LLM to intelligently route a query to appropriate tools.

        The LLM analyzes the query and determines which MCP tools to use,
        then executes them in the appropriate order.

        Args:
            query: Natural language query/task
            swarm_id: Swarm context
            agent_id: Optional agent executing

        Returns:
            Combined results from routed tools
        """
        # Get available tools from all backends
        all_tools = []
        for backend_id in self.backend_urls:
            try:
                client = self._get_client(backend_id)
                tools = await client.list_tools()
                for tool in tools:
                    all_tools.append(f"{backend_id}:{tool.get('name', 'unknown')}")
            except Exception:
                pass

        # Add MCP task types as available tools
        from .mcp_task_types import MCPTaskType
        all_tools.extend([t.value for t in MCPTaskType])

        # Use LLM to determine routing
        routing = await self._openrouter.analyze_and_route(query, all_tools)

        results = []
        prev_result = None

        for tool in routing.get("tools", []):
            try:
                # Check if it's an MCP task type
                if is_mcp_task_type(tool):
                    params = routing.get("parameters", {}).get(tool, {})
                    if prev_result:
                        params["context"] = prev_result
                    result = await self.execute_mcp_task(tool, params, agent_id, swarm_id)
                # Check if it's a backend:tool format
                elif ":" in tool:
                    backend_id, tool_name = tool.split(":", 1)
                    client = self._get_client(backend_id)
                    params = routing.get("parameters", {}).get(tool, {})
                    result = {"success": True, "result": await client.call_tool(tool_name, params)}
                else:
                    # Fallback to LLM task
                    result = await self.execute_llm_task(f"Execute: {tool}", {"query": query})

                results.append({"tool": tool, **result})
                if result.get("success"):
                    prev_result = result.get("result")

            except Exception as e:
                results.append({"tool": tool, "success": False, "error": str(e)})

        return {
            "success": all(r.get("success") for r in results),
            "query": query,
            "routing": routing,
            "results": results,
            "swarm_id": swarm_id,
            "agent_id": agent_id,
        }

    async def execute_workflow(
        self,
        steps: List[Dict[str, Any]],
        swarm_id: str,
        workflow_type: str = "sequential",
        agent_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a multi-step MCP workflow.

        Args:
            steps: List of task steps, each with:
                   - task_type: MCPTaskType value
                   - input_data: Tool arguments (can use $prev for previous result)
            swarm_id: Swarm context
            workflow_type: "sequential" or "parallel"
            agent_id: Optional agent executing the workflow

        Returns:
            Workflow execution result
        """
        workflow_id = str(uuid4())
        results = []
        prev_result = None

        if workflow_type == "parallel":
            # Execute all steps in parallel
            tasks = []
            for step in steps:
                task_type = step["task_type"]
                input_data = self._substitute_variables(step.get("input_data", {}), prev_result)
                tasks.append(self.execute_mcp_task(task_type, input_data, agent_id, swarm_id))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            results = [
                r if isinstance(r, dict) else {"success": False, "error": str(r)}
                for r in results
            ]

        else:  # sequential
            for i, step in enumerate(steps):
                task_type = step["task_type"]
                input_data = self._substitute_variables(step.get("input_data", {}), prev_result)

                result = await self.execute_mcp_task(task_type, input_data, agent_id, swarm_id)
                results.append(result)

                if result.get("success"):
                    prev_result = result.get("result")
                else:
                    # Stop on failure in sequential mode
                    break

        # Publish workflow completed event
        success = all(r.get("success") for r in results)
        await self.publish_event(
            workflow_completed_event(
                swarm_id=swarm_id,
                workflow_id=workflow_id,
                steps_completed=len(results),
                total_steps=len(steps),
                results=results,
                success=success,
            )
        )

        return {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "success": success,
            "steps_completed": len(results),
            "total_steps": len(steps),
            "results": results,
        }

    def _substitute_variables(self, obj: Any, prev_result: Any) -> Any:
        """Substitute $prev references with actual values."""
        if isinstance(obj, str):
            if obj == "$prev":
                return prev_result
            if obj.startswith("$prev."):
                path = obj[6:]  # Remove "$prev."
                return self._extract_path(prev_result, path)
            return obj

        elif isinstance(obj, dict):
            return {k: self._substitute_variables(v, prev_result) for k, v in obj.items()}

        elif isinstance(obj, list):
            return [self._substitute_variables(v, prev_result) for v in obj]

        return obj

    def _extract_path(self, obj: Any, path: str) -> Any:
        """Extract a value from a nested object using dot/bracket notation."""
        if obj is None:
            return None

        parts = re.split(r'\.|\[|\]', path)
        value = obj

        for part in parts:
            if not part:
                continue

            if part.isdigit():
                if isinstance(value, list) and int(part) < len(value):
                    value = value[int(part)]
                else:
                    return None
            elif isinstance(value, dict):
                value = value.get(part)
            else:
                return None

        return value

    async def maybe_learn_skill(
        self,
        task_type: str,
        input_data: Dict[str, Any],
        result: Dict[str, Any],
        validation_score: float,
        agent_id: str,
        swarm_id: str,
    ) -> Optional[str]:
        """
        Capture a successful task pattern as a learnable skill.

        Only learns skills when validation_score >= threshold.

        Returns:
            skill_id if skill was learned, None otherwise
        """
        if validation_score < self._skill_threshold:
            return None

        # Create skill pattern
        skill_id = str(uuid4())
        skill_pattern = {
            "id": skill_id,
            "task_type": task_type,
            "input_pattern": self._extract_pattern(input_data),
            "output_pattern": self._extract_pattern(result),
            "score": validation_score,
            "created_by": agent_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Store in Memory MCP
        try:
            client = self._get_client("distributed-memory-mcp")
            await client.call_tool("learn_skill", {
                "skill_id": skill_id,
                "skill_type": task_type,
                "pattern": skill_pattern,
                "context": {
                    "swarm_id": swarm_id,
                    "agent_id": agent_id,
                },
            })

            # Publish skill learned event
            await self.publish_event(
                mcp_skill_captured_event(
                    swarm_id=swarm_id,
                    agent_id=agent_id,
                    skill_id=skill_id,
                    task_type=task_type,
                    input_pattern=skill_pattern["input_pattern"],
                    output_pattern=skill_pattern["output_pattern"],
                    score=validation_score,
                )
            )

            logger.info(f"Learned skill {skill_id} from agent {agent_id}")
            return skill_id

        except Exception as e:
            logger.warning(f"Failed to learn skill: {e}")
            return None

    def _extract_pattern(self, data: Any) -> Dict[str, Any]:
        """Extract a generalizable pattern from data."""
        if isinstance(data, dict):
            return {
                "type": "object",
                "keys": list(data.keys()),
                "structure": {k: self._get_type_hint(v) for k, v in data.items()},
            }
        elif isinstance(data, list):
            return {
                "type": "array",
                "length": len(data),
                "item_type": self._get_type_hint(data[0]) if data else "unknown",
            }
        else:
            return {
                "type": type(data).__name__,
            }

    def _get_type_hint(self, value: Any) -> str:
        """Get a type hint for a value."""
        if isinstance(value, dict):
            return f"object({len(value)} keys)"
        elif isinstance(value, list):
            return f"array({len(value)} items)"
        elif isinstance(value, str):
            return f"string({len(value)} chars)"
        elif isinstance(value, (int, float)):
            return "number"
        elif isinstance(value, bool):
            return "boolean"
        else:
            return type(value).__name__

    async def publish_event(self, event: SwarmEvent):
        """
        Publish an event via Publish MCP.

        Also notifies any local subscribers.
        """
        try:
            # Publish to MCP backend
            client = self._get_client("distributed-publish-mcp")
            await client.call_tool("publish_event", {
                "event_type": event.event_type.value,
                "event_id": event.id,
                "data": event.to_dict(),
            })

        except Exception as e:
            logger.warning(f"Failed to publish event to MCP: {e}")

        # Notify local subscribers
        for sub in self._event_subscribers:
            if event.event_type.value in sub.get("event_types", []):
                if sub.get("webhook_url"):
                    asyncio.create_task(self._send_webhook(sub["webhook_url"], event))

    async def _send_webhook(self, url: str, event: SwarmEvent):
        """Send event to a webhook URL."""
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    url,
                    json=event.to_dict(),
                    timeout=10.0,
                )
        except Exception as e:
            logger.warning(f"Failed to send webhook to {url}: {e}")

    def subscribe(
        self,
        swarm_id: str,
        event_types: List[str],
        webhook_url: Optional[str] = None,
        subscriber_id: Optional[str] = None,
    ) -> str:
        """
        Subscribe to swarm events.

        Returns:
            Subscription ID
        """
        sub_id = str(uuid4())
        self._event_subscribers.append({
            "id": sub_id,
            "swarm_id": swarm_id,
            "event_types": event_types,
            "webhook_url": webhook_url,
            "subscriber_id": subscriber_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        })
        return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        for i, sub in enumerate(self._event_subscribers):
            if sub["id"] == subscription_id:
                self._event_subscribers.pop(i)
                return True
        return False

    async def get_backend_health(self) -> Dict[str, Any]:
        """Get health status of all MCP backends."""
        results = {}

        for backend_id in self.backend_urls:
            client = self._get_client(backend_id)
            healthy = await client.health_check()
            tools = await client.list_tools() if healthy else []

            results[backend_id] = {
                "healthy": healthy,
                "url": self.backend_urls[backend_id],
                "tools_count": len(tools),
                "tools": [t.get("name") for t in tools],
            }

        return results

    async def execute_mcp_task_async(
        self,
        task_type: str,
        input_data: Dict[str, Any],
        agent_id: Optional[str] = None,
        swarm_id: Optional[str] = None,
        timeout: float = 300.0,
        poll_interval: float = 2.0,
    ) -> Dict[str, Any]:
        """
        Execute a long-running MCP task via the Jobs API.

        Submits the task as an async job, polls for completion, and returns the result.
        Avoids HTTP timeout issues with tools like deep_search.

        Args:
            task_type: MCPTaskType value
            input_data: Arguments for the MCP tool
            agent_id: Optional agent executing the task
            swarm_id: Optional swarm context
            timeout: Max wait time in seconds
            poll_interval: Seconds between polls

        Returns:
            Task execution result
        """
        if not is_mcp_task_type(task_type):
            raise ValueError(f"Invalid MCP task type: {task_type}")

        backend_id, tool_name = MCP_TASK_ROUTING[MCPTaskType(task_type)]
        start_time = datetime.now(timezone.utc)

        try:
            # Submit via Jobs API
            async with httpx.AsyncClient(timeout=10.0) as client:
                submit_resp = await client.post(
                    f"{self.backend_urls.get(backend_id, 'http://localhost:2016')}/api/v1/jobs/submit",
                    json={
                        "tool": f"{backend_id}:{tool_name}",
                        "arguments": input_data,
                        "timeout": timeout,
                        "client_id": agent_id,
                    },
                )
                if submit_resp.status_code != 200:
                    # Fallback to synchronous execution
                    return await self.execute_mcp_task(task_type, input_data, agent_id, swarm_id)

                job_data = submit_resp.json()
                job_id = job_data["job_id"]

            # Poll for completion
            import time
            deadline = time.time() + timeout
            base_url = self.backend_urls.get(backend_id, "http://localhost:2016")

            async with httpx.AsyncClient(timeout=10.0) as client:
                while time.time() < deadline:
                    await asyncio.sleep(poll_interval)
                    status_resp = await client.get(f"{base_url}/api/v1/jobs/{job_id}")
                    if status_resp.status_code != 200:
                        continue

                    status = status_resp.json()
                    if status["status"] in ("completed", "error", "cancelled"):
                        execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
                        if status["status"] == "completed":
                            return {
                                "success": True,
                                "task_type": task_type,
                                "backend_id": backend_id,
                                "tool": tool_name,
                                "result": status.get("result"),
                                "job_id": job_id,
                                "execution_time": execution_time,
                                "agent_id": agent_id,
                            }
                        else:
                            return {
                                "success": False,
                                "task_type": task_type,
                                "backend_id": backend_id,
                                "tool": tool_name,
                                "error": status.get("error", status["status"]),
                                "job_id": job_id,
                                "execution_time": execution_time,
                                "agent_id": agent_id,
                            }

            return {
                "success": False,
                "task_type": task_type,
                "error": f"Job {job_id} timed out after {timeout}s",
                "job_id": job_id,
                "agent_id": agent_id,
            }

        except Exception as e:
            logger.warning(f"Async job execution failed, falling back to sync: {e}")
            return await self.execute_mcp_task(task_type, input_data, agent_id, swarm_id)

    async def close(self):
        """Close all backend clients and OpenRouter."""
        for client in self._clients.values():
            await client.close()
        await self._openrouter.close()

    @property
    def openrouter(self) -> OpenRouterClient:
        """Access the OpenRouter client directly."""
        return self._openrouter


# Singleton instance
_bridge: Optional[SwarmMCPBridge] = None


def get_swarm_mcp_bridge(
    backend_urls: Optional[Dict[str, str]] = None,
    openrouter_api_key: Optional[str] = None,
) -> SwarmMCPBridge:
    """Get or create the SwarmMCPBridge singleton."""
    global _bridge
    if _bridge is None:
        _bridge = SwarmMCPBridge(
            backend_urls=backend_urls,
            openrouter_api_key=openrouter_api_key,
        )
    return _bridge
