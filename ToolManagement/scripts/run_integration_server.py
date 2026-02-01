#!/usr/bin/env python3
"""
Run the MCP Integration Server.

This server provides a unified API for:
- Jina AI (search, extraction, embeddings)
- Parallel AI (tasks, search, findall, monitors)

All tools are automatically discovered and registered with our tool registry.

Usage:
    # Set API keys
    export JINA_API_KEY=your-jina-key
    export PARALLEL_API_KEY=your-parallel-key

    # Run the server
    python run_integration_server.py

    # Or with custom settings
    python run_integration_server.py --port 8200 --parallel-local

Endpoints:
    GET  /           - Server info
    GET  /health     - Health check with adapter status
    GET  /adapters   - List all adapters
    GET  /tools      - List all discovered tools
    GET  /tools/mcp  - Get tools in MCP format
    POST /execute    - Execute a tool
"""

import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, "src")

import uvicorn


def main():
    parser = argparse.ArgumentParser(description="MCP Integration Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8200, help="Port to bind to")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    # Adapter options
    parser.add_argument("--jina-key", help="Jina API key (or use JINA_API_KEY env)")
    parser.add_argument("--parallel-key", help="Parallel API key (or use PARALLEL_API_KEY env)")
    parser.add_argument("--parallel-local", action="store_true", help="Use local Parallel MCP server")
    parser.add_argument("--parallel-port", type=int, default=8100, help="Local Parallel MCP port")

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Import here to avoid circular imports
    from tool_management.integrations import (
        IntegrationServer,
        JinaAdapter,
        ParallelAdapter,
        create_integration_server,
    )

    # Create server
    server = create_integration_server(
        jina_api_key=args.jina_key,
        parallel_api_key=args.parallel_key,
        parallel_use_local=args.parallel_local,
        parallel_port=args.parallel_port,
    )

    # Create app
    app = server.create_app()

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                  MCP Integration Server                       ║
╠══════════════════════════════════════════════════════════════╣
║  Adapters:                                                    ║
║    • Jina AI    - {"✓ Enabled" if os.getenv("JINA_API_KEY") or args.jina_key else "✗ No API key":12}                           ║
║    • Parallel   - {"✓ Enabled" if os.getenv("PARALLEL_API_KEY") or args.parallel_key else "✗ No API key":12}                           ║
╠══════════════════════════════════════════════════════════════╣
║  Server: http://{args.host}:{args.port:<5}                                  ║
║  Docs:   http://{args.host}:{args.port}/docs                               ║
╚══════════════════════════════════════════════════════════════╝
    """)

    # Run
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
