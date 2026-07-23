"""
Distributed Dsco Bridge MCP

Fronts the standalone dsco-mcp-integrations REST servers (shell, code-execution,
orchestration, analytics) behind the standard native MCP protocol so their tools
route through the Tool Management registry over HTTP. Each tool maps to an HTTP
call against the upstream FastAPI server.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

import httpx

from native.base import NativeMCPBase


SHELL_URL = os.getenv("DSCO_SHELL_URL", "http://localhost:8000").rstrip("/")
CODE_URL = os.getenv("DSCO_CODE_URL", "http://localhost:8001").rstrip("/")
ORCH_URL = os.getenv("DSCO_ORCH_URL", "http://localhost:2050").rstrip("/")
ANALYTICS_URL = os.getenv("DSCO_ANALYTICS_URL", "http://localhost:2060").rstrip("/")


# tool -> (http_method, base_url, path_template, arg_mode)
# arg_mode: "body" sends remaining args as JSON; "path" substitutes {name} from
# args; "none" sends no payload.
ROUTES: Dict[str, tuple] = {
    "shell_execute_command": ("POST", SHELL_URL, "/tools/execute_shell_command", "body"),
    "shell_file_operation": ("POST", SHELL_URL, "/tools/file_operation", "body"),
    "code_execute_cli_tool": ("POST", CODE_URL, "/tools/execute_tool", "body"),
    "code_execute_python": ("POST", CODE_URL, "/tools/execute_python", "body"),
    "orchestration_create_task": ("POST", ORCH_URL, "/tasks/create", "body"),
    "orchestration_get_task": ("GET", ORCH_URL, "/tasks/{task_id}", "path"),
    "orchestration_list_tasks": ("GET", ORCH_URL, "/tasks", "none"),
    "orchestration_queue_status": ("GET", ORCH_URL, "/queue/status", "none"),
    "analytics_record_metric": ("POST", ANALYTICS_URL, "/metrics/record", "body"),
    "analytics_get_metrics": ("GET", ANALYTICS_URL, "/metrics/{metric_name}", "path"),
    "analytics_create_alert": ("POST", ANALYTICS_URL, "/alerts/create", "body"),
    "analytics_active_alerts": ("GET", ANALYTICS_URL, "/alerts/active", "none"),
}


class DistributedDscoBridgeMCP(NativeMCPBase):
    SERVICE_ID = "distributed-dsco-bridge"
    SERVICE_NAME = "Distributed Dsco Bridge MCP"
    VERSION = "1.0.0"

    CAPABILITIES = ["execute", "task", "metrics", "pipeline"]

    TOOLS = [
        {
            "name": "shell_execute_command",
            "description": "Run a shell command on the dsco shell-tools server.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to execute"},
                    "working_dir": {"type": "string", "description": "Working directory"},
                    "timeout": {"type": "integer", "description": "Timeout seconds", "default": 30},
                    "env_vars": {"type": "object", "description": "Environment variables"},
                },
                "required": ["command"],
            },
        },
        {
            "name": "shell_file_operation",
            "description": "Read/write/append/delete/list/copy/move files via shell-tools.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "operation": {"type": "string", "description": "read|write|append|delete|list|copy|move"},
                    "path": {"type": "string", "description": "File or directory path"},
                    "content": {"type": "string", "description": "Content for write/append"},
                    "target_path": {"type": "string", "description": "Target path for copy/move"},
                },
                "required": ["operation", "path"],
            },
        },
        {
            "name": "code_execute_cli_tool",
            "description": "Run a CLI tool (ripgrep, fd, bat, etc.) via code-execution server.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "tool": {"type": "string", "description": "CLI tool name"},
                    "args": {"type": "array", "items": {"type": "string"}, "description": "Tool arguments"},
                    "input_data": {"type": "string", "description": "Data to pipe to stdin"},
                },
                "required": ["tool"],
            },
        },
        {
            "name": "code_execute_python",
            "description": "Execute Python code via the code-execution server.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "code": {"type": "string", "description": "Python code to execute"},
                    "timeout": {"type": "integer", "description": "Timeout seconds", "default": 30},
                },
                "required": ["code"],
            },
        },
        {
            "name": "orchestration_create_task",
            "description": "Create a task on the orchestration server.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "priority": {"type": "integer", "default": 0},
                    "description": {"type": "string"},
                },
                "required": ["name"],
            },
        },
        {
            "name": "orchestration_get_task",
            "description": "Get a task by id from the orchestration server.",
            "inputSchema": {
                "type": "object",
                "properties": {"task_id": {"type": "string"}},
                "required": ["task_id"],
            },
        },
        {
            "name": "orchestration_list_tasks",
            "description": "List all orchestration tasks.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "orchestration_queue_status",
            "description": "Get orchestration queue status.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "analytics_record_metric",
            "description": "Record a metric value on the analytics server.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "value": {"type": "number"},
                    "tags": {"type": "object"},
                },
                "required": ["name", "value"],
            },
        },
        {
            "name": "analytics_get_metrics",
            "description": "Get aggregate stats for a metric (mean/median/p95/p99/...).",
            "inputSchema": {
                "type": "object",
                "properties": {"metric_name": {"type": "string"}},
                "required": ["metric_name"],
            },
        },
        {
            "name": "analytics_create_alert",
            "description": "Create an alert rule on the analytics server.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "metric": {"type": "string"},
                    "condition": {"type": "string", "description": "e.g. gt, lt"},
                    "threshold": {"type": "number"},
                    "enabled": {"type": "boolean", "default": True},
                },
                "required": ["name", "metric", "condition", "threshold"],
            },
        },
        {
            "name": "analytics_active_alerts",
            "description": "List currently active alerts.",
            "inputSchema": {"type": "object", "properties": {}},
        },
    ]

    async def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        route = ROUTES.get(tool_name)
        if route is None:
            return await super()._execute_tool(tool_name, arguments)

        method, base_url, path_template, arg_mode = route
        args = dict(arguments or {})

        url = base_url + path_template
        json_body: Optional[Dict[str, Any]] = None

        if arg_mode == "path":
            for key in list(args.keys()):
                token = "{" + key + "}"
                if token in url:
                    url = url.replace(token, str(args.pop(key)))
        elif arg_mode == "body":
            json_body = args

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                resp = await client.request(method, url, json=json_body)
            payload: Any
            try:
                payload = resp.json()
            except Exception:
                payload = {"status_code": resp.status_code, "text": resp.text}
            is_error = resp.status_code >= 400 or (
                isinstance(payload, dict) and payload.get("success") is False
            )
            return {
                "content": [{"type": "text", "text": json.dumps(payload, indent=2, default=str)}],
                "isError": bool(is_error),
            }
        except Exception as e:
            return {
                "content": [{"type": "text", "text": f"Bridge call to {url} failed: {e}"}],
                "isError": True,
            }


def create_server(port: Optional[int] = None) -> DistributedDscoBridgeMCP:
    port = port or int(os.environ.get("PORT", "4007"))
    return DistributedDscoBridgeMCP(port=port)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Distributed Dsco Bridge MCP")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "4007")))
    args = parser.parse_args()
    server = create_server(args.port)
    server.run()
