#!/usr/bin/env python3
"""
Native MCP Connectivity Test

Verifies both native MCP servers are running and functional:
- distributed-search-mcp (port 8101)
- distributed-research-mcp (port 8102)

Run with:
    python -m native.connectivity_test
    # or
    python native/connectivity_test.py
"""

import asyncio
import json
import sys
from datetime import datetime

import httpx


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


def ok(msg: str) -> str:
    return f"{Colors.GREEN}✓{Colors.END} {msg}"


def fail(msg: str) -> str:
    return f"{Colors.RED}✗{Colors.END} {msg}"


def info(msg: str) -> str:
    return f"{Colors.BLUE}→{Colors.END} {msg}"


def header(msg: str) -> str:
    return f"\n{Colors.BOLD}{msg}{Colors.END}"


async def test_server(
    name: str,
    base_url: str,
    expected_tools: list[str]
) -> dict:
    """Test a native MCP server."""
    results = {
        "name": name,
        "base_url": base_url,
        "tests": [],
        "passed": 0,
        "failed": 0
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Test 1: Health check
        try:
            response = await client.get(f"{base_url}/health")
            if response.status_code == 200:
                results["tests"].append(("Health check", True, None))
                results["passed"] += 1
            else:
                results["tests"].append(("Health check", False, f"Status {response.status_code}"))
                results["failed"] += 1
        except Exception as e:
            results["tests"].append(("Health check", False, str(e)))
            results["failed"] += 1
            return results  # Can't continue if server is down

        # Test 2: Service info
        try:
            response = await client.get(f"{base_url}/")
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "connected":
                    results["tests"].append(("Service info", True, None))
                    results["passed"] += 1
                else:
                    results["tests"].append(("Service info", False, f"Status: {data.get('status')}"))
                    results["failed"] += 1
            else:
                results["tests"].append(("Service info", False, f"Status {response.status_code}"))
                results["failed"] += 1
        except Exception as e:
            results["tests"].append(("Service info", False, str(e)))
            results["failed"] += 1

        # Test 3: Tools list
        try:
            response = await client.get(f"{base_url}/tools")
            if response.status_code == 200:
                data = response.json()
                tools = data.get("tools", [])
                tool_names = [t.get("name") for t in tools]

                missing = [t for t in expected_tools if t not in tool_names]
                if not missing:
                    results["tests"].append(("Tools list", True, f"{len(tools)} tools"))
                    results["passed"] += 1
                else:
                    results["tests"].append(("Tools list", False, f"Missing: {missing}"))
                    results["failed"] += 1
            else:
                results["tests"].append(("Tools list", False, f"Status {response.status_code}"))
                results["failed"] += 1
        except Exception as e:
            results["tests"].append(("Tools list", False, str(e)))
            results["failed"] += 1

        # Test 4: MCP protocol - initialize
        try:
            response = await client.post(
                f"{base_url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "method": "initialize",
                    "params": {},
                    "id": 1
                }
            )
            if response.status_code == 200:
                data = response.json()
                if "result" in data and "protocolVersion" in data["result"]:
                    results["tests"].append(("MCP initialize", True, data["result"]["protocolVersion"]))
                    results["passed"] += 1
                else:
                    results["tests"].append(("MCP initialize", False, "Invalid response"))
                    results["failed"] += 1
            else:
                results["tests"].append(("MCP initialize", False, f"Status {response.status_code}"))
                results["failed"] += 1
        except Exception as e:
            results["tests"].append(("MCP initialize", False, str(e)))
            results["failed"] += 1

        # Test 5: MCP protocol - tools/list
        try:
            response = await client.post(
                f"{base_url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "method": "tools/list",
                    "params": {},
                    "id": 2
                }
            )
            if response.status_code == 200:
                data = response.json()
                if "result" in data and "tools" in data["result"]:
                    results["tests"].append(("MCP tools/list", True, f"{len(data['result']['tools'])} tools"))
                    results["passed"] += 1
                else:
                    results["tests"].append(("MCP tools/list", False, "Invalid response"))
                    results["failed"] += 1
            else:
                results["tests"].append(("MCP tools/list", False, f"Status {response.status_code}"))
                results["failed"] += 1
        except Exception as e:
            results["tests"].append(("MCP tools/list", False, str(e)))
            results["failed"] += 1

        # Test 6: MCP protocol - tools/call (connectivity test)
        try:
            response = await client.post(
                f"{base_url}/mcp",
                json={
                    "jsonrpc": "2.0",
                    "method": "tools/call",
                    "params": {
                        "name": "_connectivity_test",
                        "arguments": {"echo": "native-test"}
                    },
                    "id": 3
                }
            )
            if response.status_code == 200:
                data = response.json()
                if "result" in data:
                    content = data["result"].get("content", [])
                    if content:
                        text = content[0].get("text", "")
                        parsed = json.loads(text)
                        if parsed.get("success") and parsed.get("echo") == "native-test":
                            results["tests"].append(("MCP tools/call", True, "Echo verified"))
                            results["passed"] += 1
                        else:
                            results["tests"].append(("MCP tools/call", False, "Echo mismatch"))
                            results["failed"] += 1
                    else:
                        results["tests"].append(("MCP tools/call", False, "No content"))
                        results["failed"] += 1
                else:
                    results["tests"].append(("MCP tools/call", False, data.get("error", {}).get("message", "Unknown")))
                    results["failed"] += 1
            else:
                results["tests"].append(("MCP tools/call", False, f"Status {response.status_code}"))
                results["failed"] += 1
        except Exception as e:
            results["tests"].append(("MCP tools/call", False, str(e)))
            results["failed"] += 1

    return results


async def main():
    print(f"""
{Colors.BOLD}╔══════════════════════════════════════════════════════════════════╗
║            Native MCP Connectivity Test                          ║
║              Verifying All Three Native MCPs                     ║
╚══════════════════════════════════════════════════════════════════╝{Colors.END}
""")

    servers = [
        {
            "name": "distributed-search-mcp",
            "url": "http://localhost:4001",
            "expected_tools": ["search", "read", "verify", "rerank", "embed"]
        },
        {
            "name": "distributed-research-mcp",
            "url": "http://localhost:4002",
            "expected_tools": ["deep_search", "run_task", "extract_data", "create_task_group", "find_all", "list_tasks", "get_task", "synthesize"]
        },
        {
            "name": "distributed-publish-mcp",
            "url": "http://localhost:4003",
            "expected_tools": ["create_webhook", "trigger_webhook", "list_webhooks", "delete_webhook", "publish_event", "list_events", "queue_message", "get_queue_messages", "send_notification", "syndicate_content"]
        }
    ]

    all_passed = True

    for server in servers:
        print(header(f"Testing {server['name']}"))
        print(info(f"URL: {server['url']}"))
        print()

        results = await test_server(
            server["name"],
            server["url"],
            server["expected_tools"]
        )

        for test_name, passed, detail in results["tests"]:
            if passed:
                msg = ok(test_name)
                if detail:
                    msg += f" ({detail})"
            else:
                msg = fail(f"{test_name}: {detail}")
                all_passed = False
            print(f"  {msg}")

        print()
        print(f"  {Colors.BOLD}Results:{Colors.END} {results['passed']} passed, {results['failed']} failed")
        print()

    # Summary
    print(f"""
{Colors.BOLD}══════════════════════════════════════════════════════════════════{Colors.END}
""")

    if all_passed:
        print(f"{Colors.GREEN}{Colors.BOLD}All connectivity tests passed!{Colors.END}")
        print()
        print("Native MCPs are ready:")
        print(f"  • distributed-search-mcp   → http://localhost:4001")
        print(f"  • distributed-research-mcp → http://localhost:4002")
        print(f"  • distributed-publish-mcp  → http://localhost:4003")
        return 0
    else:
        print(f"{Colors.RED}{Colors.BOLD}Some tests failed.{Colors.END}")
        print()
        print("Make sure both servers are running:")
        print("  python -m native.distributed_search_mcp.server")
        print("  python -m native.distributed_research_mcp.server")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
