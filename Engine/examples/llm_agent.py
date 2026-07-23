#!/usr/bin/env python3
"""
LLM Agent Example

An agent that uses LLMs to process tasks intelligently.

Usage:
    python llm_agent.py --agent-id llm-agent-1 --model claude-sonnet-5
"""

import asyncio
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from sdk import Agent, AgentConfig, ClientConfig
from models import Tick, TickType, Task, Signal, SignalType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


class LLMAgent(Agent):
    """
    An agent that uses LLMs for intelligent task processing.

    Features:
    - Conversation memory
    - Tool use
    - Multi-turn reasoning
    - Task decomposition
    """

    def __init__(
        self,
        config: AgentConfig,
        client_config: ClientConfig = None,
        model: str = "claude-sonnet-5",
        api_key: Optional[str] = None,
        tool_gateway_url: str = "http://localhost:8000",
    ):
        super().__init__(config, client_config)
        self.model = model
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self.tool_gateway_url = tool_gateway_url

        # Conversation memory
        self.conversation_history: List[Dict] = []
        self.max_history = 20

        # Available tools (fetched from gateway)
        self.available_tools: List[Dict] = []

    async def on_start(self) -> None:
        """Initialize the LLM agent."""
        logger.info(f"LLM Agent {self.config.agent_id} starting")
        logger.info(f"Model: {self.model}")

        # Fetch available tools
        await self._fetch_tools()

    async def _fetch_tools(self) -> None:
        """Fetch available tools from the gateway."""
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.tool_gateway_url}/tools",
                    timeout=10.0,
                )
                if response.status_code == 200:
                    self.available_tools = response.json()
                    logger.info(f"Loaded {len(self.available_tools)} tools")
        except Exception as e:
            logger.warning(f"Failed to fetch tools: {e}")

    def _format_tools_for_claude(self) -> List[Dict]:
        """Format tools for Claude's tool_use format."""
        formatted = []
        for tool in self.available_tools[:20]:  # Limit to 20 tools
            formatted.append({
                "name": tool.get("name", tool.get("tool_id")),
                "description": tool.get("description", ""),
                "input_schema": tool.get("input_schema", {
                    "type": "object",
                    "properties": {},
                }),
            })
        return formatted

    async def _call_claude(
        self,
        messages: List[Dict],
        system: Optional[str] = None,
        tools: Optional[List[Dict]] = None,
    ) -> Dict:
        """Call Claude API."""
        try:
            import httpx

            headers = {
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            }

            body = {
                "model": self.model,
                "max_tokens": 4096,
                "messages": messages,
            }

            if system:
                body["system"] = system

            if tools:
                body["tools"] = tools

            async with httpx.AsyncClient() as client:
                response = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers=headers,
                    json=body,
                    timeout=60.0,
                )

                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"Claude API error: {response.status_code}")
                    return {"error": response.text}

        except Exception as e:
            logger.error(f"Claude API call failed: {e}")
            return {"error": str(e)}

    async def _execute_tool(self, tool_name: str, tool_input: Dict) -> Any:
        """Execute a tool via the gateway."""
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.tool_gateway_url}/unified/execute",
                    json={
                        "tool_id": tool_name,
                        "input_data": tool_input,
                        "agent_id": self.config.agent_id,
                    },
                    timeout=60.0,
                )
                if response.status_code == 200:
                    return response.json()
                return {"error": f"Tool execution failed: {response.status_code}"}
        except Exception as e:
            return {"error": str(e)}

    async def process_with_llm(
        self,
        user_message: str,
        context: Optional[Dict] = None,
    ) -> str:
        """
        Process a message using the LLM with tool use.

        Implements a tool use loop:
        1. Send message to Claude
        2. If Claude wants to use a tool, execute it
        3. Send tool result back to Claude
        4. Repeat until Claude provides a final response
        """
        # Build messages with history
        messages = list(self.conversation_history)
        messages.append({"role": "user", "content": user_message})

        # System prompt
        system = f"""You are a DSI Engine agent (ID: {self.config.agent_id}) operating within
Distributed Systems, Inc. tick-based execution infrastructure.

You have access to MCP tools registered through the Tool Gateway. Use them to
gather evidence and take actions — prefer tool-driven results over reasoning
from priors alone. Be concise and direct.

If you cannot complete a task, state the specific blocker and what resources
or permissions would unblock it.

Current context: {context or {}}"""

        # Tool use loop
        max_iterations = 10
        tools = self._format_tools_for_claude() if self.available_tools else None

        for iteration in range(max_iterations):
            response = await self._call_claude(messages, system=system, tools=tools)

            if "error" in response:
                return f"Error: {response['error']}"

            # Check stop reason
            stop_reason = response.get("stop_reason")

            # Extract content
            content_blocks = response.get("content", [])

            if stop_reason == "tool_use":
                # Claude wants to use tools
                tool_uses = [
                    block for block in content_blocks
                    if block.get("type") == "tool_use"
                ]

                # Add assistant message
                messages.append({"role": "assistant", "content": content_blocks})

                # Execute tools and collect results
                tool_results = []
                for tool_use in tool_uses:
                    tool_name = tool_use["name"]
                    tool_input = tool_use["input"]
                    tool_id = tool_use["id"]

                    logger.info(f"Executing tool: {tool_name}")
                    result = await self._execute_tool(tool_name, tool_input)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": str(result),
                    })

                # Add tool results
                messages.append({"role": "user", "content": tool_results})

            else:
                # Claude is done - extract text response
                text_blocks = [
                    block.get("text", "")
                    for block in content_blocks
                    if block.get("type") == "text"
                ]
                final_response = "\n".join(text_blocks)

                # Update conversation history
                self.conversation_history.append(
                    {"role": "user", "content": user_message}
                )
                self.conversation_history.append(
                    {"role": "assistant", "content": final_response}
                )

                # Trim history if needed
                if len(self.conversation_history) > self.max_history * 2:
                    self.conversation_history = self.conversation_history[-self.max_history * 2:]

                return final_response

        return "Error: Max iterations reached"

    async def on_tick(self, tick: Tick) -> Any:
        """Process ticks."""
        if tick.tick_type != TickType.TASK or not tick.scheduled_tasks:
            return None

        task = tick.scheduled_tasks[0]
        return await self.process_task(task)

    async def process_task(self, task: Task) -> Dict:
        """
        Process a task using the LLM.

        Expected input_data format:
        {
            "message": str,  # User message or task description
            "context": dict,  # Optional context
            "clear_history": bool,  # Optional, clears conversation
        }
        """
        message = task.input_data.get("message", "")
        context = task.input_data.get("context", {})
        clear_history = task.input_data.get("clear_history", False)

        if clear_history:
            self.conversation_history = []

        if not message:
            return {"error": "No message provided"}

        # Add task context
        context["task_id"] = task.task_id
        context["task_type"] = task.task_type

        # Process with LLM
        response = await self.process_with_llm(message, context)

        return {
            "task_id": task.task_id,
            "response": response,
            "history_length": len(self.conversation_history),
        }

    async def on_signal(self, signal: Signal) -> None:
        """Handle signals."""
        if signal.signal_type == SignalType.INTERRUPT:
            # Clear history on interrupt
            if signal.payload and signal.payload.get("clear_history"):
                self.conversation_history = []
                logger.info("Conversation history cleared")


async def main():
    parser = argparse.ArgumentParser(description="LLM Agent")
    parser.add_argument("--agent-id", default="llm-agent-1")
    parser.add_argument("--engine-url", default="ws://localhost:8080")
    parser.add_argument("--model", default="claude-sonnet-5")
    parser.add_argument("--tool-gateway", default="http://localhost:8000")
    parser.add_argument("--tick-interval", type=int, default=1000)

    args = parser.parse_args()

    config = AgentConfig(
        agent_id=args.agent_id,
        agent_type="llm",
        tick_interval_ms=args.tick_interval,
        capabilities={"llm", "reasoning", "tools", "conversation"},
    )

    client_config = ClientConfig(
        engine_url=args.engine_url,
        auto_reconnect=True,
    )

    agent = LLMAgent(
        config,
        client_config,
        model=args.model,
        tool_gateway_url=args.tool_gateway,
    )

    try:
        await agent.run()
    except KeyboardInterrupt:
        logger.info("Interrupted")


if __name__ == "__main__":
    asyncio.run(main())
