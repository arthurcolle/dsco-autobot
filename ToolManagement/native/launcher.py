#!/usr/bin/env python3
"""
Native MCP Launcher - Unified Startup for Native MCPs

Starts native MCP servers as background processes:
- distributed-search-mcp (port 4001)
- distributed-research-mcp (port 4002)
- distributed-publish-mcp (port 4003)
- distributed-memory-mcp (port 4004)
- distributed-video-qa (port 4005)
- distributed-document-qa (port 4006)

Run with:
    python -m native.launcher
    # or
    python native/launcher.py

Options:
    --search-only       Only start the search MCP
    --research-only     Only start the research MCP
    --publish-only      Only start the publish MCP
    --memory-only       Only start the memory MCP
    --video-only        Only start the video Q&A MCP
    --document-only     Only start the document Q&A MCP
    --wait-for-ready    Block until all servers are healthy
    --test              Run connectivity test after startup
"""

from __future__ import annotations

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import httpx


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    END = "\033[0m"


NATIVE_ROOT = Path(__file__).parent
PROJECT_ROOT = NATIVE_ROOT.parent

SERVERS = {
    "search": {
        "name": "distributed-search-mcp",
        "module": "native.distributed_search_mcp.server",
        "port": int(os.getenv("SEARCH_MCP_PORT", "4001")),
        "env_key": "JINA_API_KEY"
    },
    "research": {
        "name": "distributed-research-mcp",
        "module": "native.distributed_research_mcp.server",
        "port": int(os.getenv("RESEARCH_MCP_PORT", "4002")),
        "env_key": "PARALLEL_API_KEY"
    },
    "publish": {
        "name": "distributed-publish-mcp",
        "module": "native.distributed_publish_mcp.server",
        "port": int(os.getenv("PUBLISH_MCP_PORT", "4003")),
        "env_key": None
    },
    "memory": {
        "name": "distributed-memory-mcp",
        "module": "native.distributed_memory_mcp.server",
        "port": int(os.getenv("MEMORY_MCP_PORT", "4004")),
        "env_key": "MEMORY_DATA_DIR"
    },
    "video": {
        "name": "distributed-video-qa",
        "module": "native.distributed_video_qa",
        "port": int(os.getenv("VIDEO_MCP_PORT", "4005")),
        "env_key": "OPENAI_API_KEY"
    },
    "document": {
        "name": "distributed-document-qa",
        "module": "native.distributed_document_qa",
        "port": int(os.getenv("DOCUMENT_MCP_PORT", "4006")),
        "env_key": "OPENAI_API_KEY"
    },
    "codex-apps": {
        "name": "codex-apps-mcp",
        "module": "native.codex_apps_mcp.server",
        "port": int(os.getenv("CODEX_APPS_MCP_PORT", "4011")),
        "env_key": "TOOLMANAGEMENT_API_URL"
    },
    "timeline": {
        "name": "distributed-timeline",
        "module": "native.timeline_mcp.server",
        "port": int(os.getenv("TIMELINE_MCP_PORT", "4012")),
        "env_key": "JINA_API_KEY"
    }
}


class NativeMCPLauncher:
    """Manages native MCP server processes."""

    def __init__(self):
        self.processes: dict[str, subprocess.Popen] = {}
        self._shutdown_requested = False

    def start_server(self, server_key: str) -> Optional[subprocess.Popen]:
        """Start a native MCP server."""
        server = SERVERS.get(server_key)
        if not server:
            print(f"{Colors.RED}Unknown server: {server_key}{Colors.END}")
            return None

        env = os.environ.copy()
        env["PORT"] = str(server["port"])
        env["PYTHONPATH"] = f"{PROJECT_ROOT}:{PROJECT_ROOT}/src:{env.get('PYTHONPATH', '')}"

        print(f"{Colors.CYAN}Starting {server['name']} on port {server['port']}...{Colors.END}")

        try:
            process = subprocess.Popen(
                [sys.executable, "-m", server["module"]],
                cwd=str(PROJECT_ROOT),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            self.processes[server_key] = process
            return process
        except Exception as e:
            print(f"{Colors.RED}Failed to start {server['name']}: {e}{Colors.END}")
            return None

    async def wait_for_healthy(self, server_key: str, timeout: float = 30.0) -> bool:
        """Wait for a server to become healthy."""
        server = SERVERS.get(server_key)
        if not server:
            return False

        url = f"http://localhost:{server['port']}/health"
        start_time = time.time()

        async with httpx.AsyncClient() as client:
            while time.time() - start_time < timeout:
                try:
                    response = await client.get(url, timeout=2.0)
                    if response.status_code == 200:
                        return True
                except (httpx.ConnectError, httpx.TimeoutException):
                    pass
                await asyncio.sleep(0.5)

        return False

    async def wait_all_healthy(self, servers: list[str], timeout: float = 30.0) -> dict[str, bool]:
        """Wait for multiple servers to become healthy."""
        tasks = [self.wait_for_healthy(s, timeout) for s in servers]
        results = await asyncio.gather(*tasks)
        return dict(zip(servers, results))

    def stop_server(self, server_key: str):
        """Stop a native MCP server."""
        process = self.processes.get(server_key)
        if process:
            server = SERVERS.get(server_key)
            print(f"{Colors.YELLOW}Stopping {server['name'] if server else server_key}...{Colors.END}")
            process.terminate()
            try:
                process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                process.kill()
            del self.processes[server_key]

    def stop_all(self):
        """Stop all running servers."""
        for key in list(self.processes.keys()):
            self.stop_server(key)

    def handle_signal(self, signum, frame):
        """Handle shutdown signals."""
        if not self._shutdown_requested:
            self._shutdown_requested = True
            print(f"\n{Colors.YELLOW}Shutting down native MCPs...{Colors.END}")
            self.stop_all()
            sys.exit(0)


async def run_launcher(args: argparse.Namespace):
    """Main launcher function."""
    launcher = NativeMCPLauncher()

    # Register signal handlers
    signal.signal(signal.SIGINT, launcher.handle_signal)
    signal.signal(signal.SIGTERM, launcher.handle_signal)

    print(f"""
{Colors.BOLD}╔══════════════════════════════════════════════════════════════════╗
║              Native MCP Launcher                                 ║
║             Starting Native MCP Servers                          ║
╚══════════════════════════════════════════════════════════════════╝{Colors.END}
""")

    # Determine which servers to start
    servers_to_start = []
    if args.search_only:
        servers_to_start = ["search"]
    elif args.research_only:
        servers_to_start = ["research"]
    elif args.publish_only:
        servers_to_start = ["publish"]
    elif args.memory_only:
        servers_to_start = ["memory"]
    elif args.video_only:
        servers_to_start = ["video"]
    elif args.document_only:
        servers_to_start = ["document"]
    else:
        servers_to_start = ["search", "research", "publish", "memory", "video", "document"]

    # Start servers
    for server_key in servers_to_start:
        server = SERVERS[server_key]
        api_key = os.getenv(server["env_key"]) if server["env_key"] else None
        print(f"  {Colors.BLUE}•{Colors.END} {server['name']}")
        print(f"    Port: {server['port']}")
        print(f"    API Key: {'configured' if api_key else 'not set'}")
        print()

    for server_key in servers_to_start:
        launcher.start_server(server_key)

    # Wait for servers to be healthy
    if args.wait_for_ready or args.test:
        print(f"\n{Colors.CYAN}Waiting for servers to be ready...{Colors.END}")
        results = await launcher.wait_all_healthy(servers_to_start, timeout=30.0)

        all_healthy = True
        for server_key, healthy in results.items():
            server = SERVERS[server_key]
            if healthy:
                print(f"  {Colors.GREEN}✓{Colors.END} {server['name']} is ready")
            else:
                print(f"  {Colors.RED}✗{Colors.END} {server['name']} failed to start")
                all_healthy = False

        if not all_healthy:
            print(f"\n{Colors.RED}Some servers failed to start.{Colors.END}")
            launcher.stop_all()
            return 1

    # Run connectivity test
    if args.test:
        print(f"\n{Colors.CYAN}Running connectivity test...{Colors.END}\n")
        from native.connectivity_test import main as run_test
        test_result = await run_test()
        if test_result != 0:
            launcher.stop_all()
            return test_result

    # Print summary
    print(f"""
{Colors.BOLD}══════════════════════════════════════════════════════════════════{Colors.END}

{Colors.GREEN}Native MCPs are running:{Colors.END}
""")

    for server_key in servers_to_start:
        server = SERVERS[server_key]
        print(f"  • {server['name']:<30} http://localhost:{server['port']}")

    print(f"""
{Colors.BLUE}Endpoints:{Colors.END}
  • GET  /         Service info
  • GET  /health   Health check
  • GET  /tools    List tools
  • POST /mcp      MCP JSON-RPC endpoint
  • POST /execute  Direct tool execution
  • GET  /sse      Server-Sent Events

{Colors.YELLOW}Press Ctrl+C to stop all servers{Colors.END}
""")

    # Keep running until interrupted
    try:
        while True:
            # Check if processes are still running
            for server_key, process in list(launcher.processes.items()):
                if process.poll() is not None:
                    server = SERVERS[server_key]
                    print(f"{Colors.RED}{server['name']} exited unexpectedly{Colors.END}")

                    # Print any output
                    if process.stdout:
                        output = process.stdout.read()
                        if output:
                            print(output)

            await asyncio.sleep(1.0)

    except KeyboardInterrupt:
        pass
    finally:
        launcher.stop_all()

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Native MCP Launcher - Start native MCP servers"
    )
    parser.add_argument(
        "--search-only",
        action="store_true",
        help="Only start the search MCP"
    )
    parser.add_argument(
        "--research-only",
        action="store_true",
        help="Only start the research MCP"
    )
    parser.add_argument(
        "--publish-only",
        action="store_true",
        help="Only start the publish MCP"
    )
    parser.add_argument(
        "--memory-only",
        action="store_true",
        help="Only start the memory MCP"
    )
    parser.add_argument(
        "--video-only",
        action="store_true",
        help="Only start the video Q&A MCP"
    )
    parser.add_argument(
        "--document-only",
        action="store_true",
        help="Only start the document Q&A MCP"
    )
    parser.add_argument(
        "--wait-for-ready",
        action="store_true",
        help="Wait for servers to be healthy before returning"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run connectivity test after startup"
    )

    args = parser.parse_args()

    return asyncio.run(run_launcher(args))


if __name__ == "__main__":
    sys.exit(main())
