"""
Example MCP - Shows how easy it is to create a new native MCP.

This is a minimal example demonstrating the NativeMCPBase pattern.
A complete MCP in ~50 lines of code.
"""

import json
from typing import Any, Dict
from native.base import NativeMCPBase, EmbeddingMixin, CapabilityType


class ExampleMCP(NativeMCPBase, EmbeddingMixin):
    """
    Minimal example MCP.

    To create a new MCP:
    1. Inherit from NativeMCPBase (and EmbeddingMixin if you need embeddings)
    2. Define SERVICE_ID, SERVICE_NAME, VERSION
    3. Define CAPABILITIES (what you provide)
    4. Define REQUIRES (soft dependencies) and DEPENDS (hard dependencies)
    5. Define TOOLS list
    6. Implement _tool_{name} methods for each tool
    """

    SERVICE_ID = "example-mcp"
    SERVICE_NAME = "Example MCP"
    VERSION = "1.0.0"

    CAPABILITIES = ["example", "demo"]
    REQUIRES = [CapabilityType.EMBED]  # Optional: we'd like embeddings

    TOOLS = [
        {
            "name": "greet",
            "description": "Say hello",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "default": "World"},
                },
            },
        },
        {
            "name": "embed_text",
            "description": "Get embedding for text (uses Search MCP)",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                },
                "required": ["text"],
            },
        },
        {
            "name": "call_other_mcp",
            "description": "Demonstrate calling another MCP",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tool": {"type": "string"},
                    "args": {"type": "object"},
                },
                "required": ["tool"],
            },
        },
    ]

    # Tool handlers - just implement _tool_{name} methods

    async def _tool_greet(self, args: Dict[str, Any]) -> Dict[str, Any]:
        name = args.get("name", "World")
        return {
            "content": [{
                "type": "text",
                "text": f"Hello, {name}! I'm {self.SERVICE_NAME}.",
            }]
        }

    async def _tool_embed_text(self, args: Dict[str, Any]) -> Dict[str, Any]:
        text = args["text"]

        # EmbeddingMixin provides get_embedding()
        # It automatically routes to any MCP with 'embed' capability
        embedding = await self.get_embedding(text)

        if embedding:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "text": text,
                        "embedding_dim": len(embedding),
                        "embedding_preview": embedding[:5],
                    }),
                }]
            }
        else:
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "error": "Embedding not available",
                        "hint": "Make sure distributed-search-mcp is running",
                    }),
                }],
                "isError": True,
            }

    async def _tool_call_other_mcp(self, args: Dict[str, Any]) -> Dict[str, Any]:
        tool = args["tool"]
        tool_args = args.get("args", {})

        # self.mesh provides cross-MCP communication
        result = await self.mesh.call(tool=tool, args=tool_args)

        return {
            "content": [{
                "type": "text",
                "text": json.dumps(result, indent=2, default=str),
            }]
        }


# Run with: python -m native.example_mcp --port 4099
if __name__ == "__main__":
    import argparse
    import logging

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=4099)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    server = ExampleMCP(port=args.port)
    print(f"Starting {server.SERVICE_NAME} on port {args.port}")
    print(f"Capabilities: {server.CAPABILITIES}")
    print(f"Tools: {[t['name'] for t in server.TOOLS]}")
    server.run()
