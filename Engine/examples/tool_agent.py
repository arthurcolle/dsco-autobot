#!/usr/bin/env python3
"""
Tool Agent Example

An agent that can invoke tools from the Tool Gateway.

Usage:
    python tool_agent.py --agent-id tool-agent-1 --tool-gateway http://localhost:8000
"""

import asyncio
import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from sdk import Agent, AgentConfig, ClientConfig
from models import Tick, TickType, Task

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class ToolAgent(Agent):
    """
    An agent that can search for and invoke tools.

    Capabilities:
    - Search tools by semantic query
    - Execute tools with parameters
    - Chain tool invocations
    """

    def __init__(
        self,
        config: AgentConfig,
        client_config: ClientConfig = None,
        tool_gateway_url: str = "http://localhost:8000",
    ):
        super().__init__(config, client_config)
        self.tool_gateway_url = tool_gateway_url
        self._tool_cache: Dict[str, Dict] = {}

    async def on_start(self) -> None:
        """Initialize and cache available tools."""
        logger.info(f"Tool Agent {self.config.agent_id} starting")
        logger.info(f"Tool Gateway: {self.tool_gateway_url}")

        # Pre-fetch available tools
        await self._refresh_tool_cache()

    async def _refresh_tool_cache(self) -> None:
        """Refresh the local tool cache."""
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.tool_gateway_url}/tools",
                    timeout=10.0,
                )
                if response.status_code == 200:
                    tools = response.json()
                    self._tool_cache = {t["tool_id"]: t for t in tools}
                    logger.info(f"Cached {len(self._tool_cache)} tools")
        except Exception as e:
            logger.warning(f"Failed to refresh tool cache: {e}")

    async def search_tools(self, query: str, limit: int = 5) -> List[Dict]:
        """Search for tools matching a query."""
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.tool_gateway_url}/unified/search",
                    params={"q": query, "limit": limit},
                    timeout=10.0,
                )
                if response.status_code == 200:
                    return response.json().get("tools", [])
        except Exception as e:
            logger.error(f"Tool search failed: {e}")
        return []

    async def execute_tool(
        self,
        tool_id: str,
        params: Dict[str, Any],
    ) -> Any:
        """Execute a tool and return the result."""
        logger.info(f"Executing tool: {tool_id}")
        logger.debug(f"Parameters: {params}")

        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.tool_gateway_url}/unified/execute",
                    json={
                        "tool_id": tool_id,
                        "input_data": params,
                        "agent_id": self.config.agent_id,
                    },
                    timeout=60.0,
                )
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Tool executed successfully")
                    return result
                else:
                    logger.error(f"Tool execution failed: {response.status_code}")
                    return {"error": response.text}
        except Exception as e:
            logger.error(f"Tool execution error: {e}")
            return {"error": str(e)}

    async def on_tick(self, tick: Tick) -> Any:
        """Process ticks and execute tool-based tasks."""
        if tick.tick_type != TickType.TASK or not tick.scheduled_tasks:
            return None

        task = tick.scheduled_tasks[0]
        return await self.process_task(task)

    async def process_task(self, task: Task) -> Dict:
        """
        Process a tool execution task.

        Expected input_data format:
        {
            "action": "search" | "execute" | "chain",
            "query": str,  # for search
            "tool_id": str,  # for execute
            "params": dict,  # for execute
            "chain": [{"tool_id": str, "params": dict}, ...]  # for chain
        }
        """
        action = task.input_data.get("action", "execute")

        if action == "search":
            query = task.input_data.get("query", "")
            tools = await self.search_tools(query)
            return {
                "task_id": task.task_id,
                "action": "search",
                "query": query,
                "tools": tools,
            }

        elif action == "execute":
            tool_id = task.input_data.get("tool_id")
            params = task.input_data.get("params", {})

            if not tool_id:
                return {"error": "tool_id required"}

            result = await self.execute_tool(tool_id, params)
            return {
                "task_id": task.task_id,
                "action": "execute",
                "tool_id": tool_id,
                "result": result,
            }

        elif action == "chain":
            chain = task.input_data.get("chain", [])
            results = []

            for i, step in enumerate(chain):
                tool_id = step.get("tool_id")
                params = step.get("params", {})

                # Substitute results from previous steps
                for key, value in params.items():
                    if isinstance(value, str) and value.startswith("$"):
                        step_ref = int(value[1:])
                        if step_ref < len(results):
                            params[key] = results[step_ref]

                result = await self.execute_tool(tool_id, params)
                results.append(result)

                if "error" in result:
                    return {
                        "task_id": task.task_id,
                        "action": "chain",
                        "failed_at": i,
                        "results": results,
                    }

            return {
                "task_id": task.task_id,
                "action": "chain",
                "results": results,
            }

        else:
            return {"error": f"Unknown action: {action}"}


async def main():
    parser = argparse.ArgumentParser(description="Tool Agent")
    parser.add_argument("--agent-id", default="tool-agent-1")
    parser.add_argument("--engine-url", default="ws://localhost:8080")
    parser.add_argument("--tool-gateway", default="http://localhost:8000")
    parser.add_argument("--tick-interval", type=int, default=1000)

    args = parser.parse_args()

    config = AgentConfig(
        agent_id=args.agent_id,
        agent_type="tool-executor",
        tick_interval_ms=args.tick_interval,
        capabilities={"tools", "search", "execute", "chain"},
    )

    client_config = ClientConfig(
        engine_url=args.engine_url,
        auto_reconnect=True,
    )

    agent = ToolAgent(
        config,
        client_config,
        tool_gateway_url=args.tool_gateway,
    )

    try:
        await agent.run()
    except KeyboardInterrupt:
        logger.info("Interrupted")


if __name__ == "__main__":
    asyncio.run(main())
