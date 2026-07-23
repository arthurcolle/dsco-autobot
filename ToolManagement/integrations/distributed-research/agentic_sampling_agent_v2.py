#!/usr/bin/env python3
"""
Agentic Sampling Agent V2 - Native Tool Use with Sub-Agent Orchestration

An advanced agentic architecture implementing:
- Native Anthropic tool use (structured tool calling)
- Sub-agent spawning (agents calling agents)
- Async/streaming support
- Hierarchical agent orchestration
- Tool permission scoping

This is the "Claude Code in 200 lines" expanded to production-grade
with proper async patterns and agent composition.
"""

import asyncio
import json
import os
import subprocess
import sys
import tempfile
import socket
import ssl
import urllib.request
import urllib.error
import shutil
import uuid
import re
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, AsyncIterator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

try:
    import anthropic
except ImportError:
    print("Please install anthropic: pip install anthropic")
    sys.exit(1)

# =============================================================================
# Configuration
# =============================================================================

DEFAULT_MODEL = "claude-sonnet-5"
ORCHESTRATOR_MODEL = "claude-sonnet-5"
MAX_TOKENS = 8192
DEFAULT_TIMEOUT = 30

ANSI = {
    "reset": "\u001b[0m",
    "user": "\u001b[94m",
    "assistant": "\u001b[93m",
    "tool": "\u001b[92m",
    "error": "\u001b[91m",
    "info": "\u001b[96m",
    "agent": "\u001b[95m",
    "dim": "\u001b[90m",
}

# =============================================================================
# Result Types
# =============================================================================

class ResultStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"
    PARTIAL = "partial"

@dataclass
class ToolResult:
    status: ResultStatus
    data: Dict[str, Any]
    message: Optional[str] = None

    def to_content(self) -> str:
        """Convert to content string for tool_result."""
        result = {"status": self.status.value, "data": self.data}
        if self.message:
            result["message"] = self.message
        return json.dumps(result, indent=2, default=str)

# =============================================================================
# Path Utilities
# =============================================================================

def resolve_path(path_str: str) -> Path:
    path = Path(path_str).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path

# =============================================================================
# TOOL IMPLEMENTATIONS
# =============================================================================

def read_file(filename: str, encoding: str = "utf-8",
              start_line: Optional[int] = None,
              end_line: Optional[int] = None) -> ToolResult:
    """Read the contents of a file."""
    try:
        full_path = resolve_path(filename)
        if not full_path.exists():
            return ToolResult(ResultStatus.ERROR, {"path": str(full_path)},
                            f"File not found: {full_path}")
        if not full_path.is_file():
            return ToolResult(ResultStatus.ERROR, {"path": str(full_path)},
                            "Path is not a file")

        content = full_path.read_text(encoding=encoding)
        lines = content.splitlines(keepends=True)
        total_lines = len(lines)

        if start_line or end_line:
            start_idx = (start_line - 1) if start_line else 0
            end_idx = end_line if end_line else total_lines
            lines = lines[start_idx:end_idx]
            content = "".join(lines)

        return ToolResult(ResultStatus.SUCCESS, {
            "path": str(full_path),
            "content": content,
            "total_lines": total_lines,
            "lines_returned": len(lines)
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"path": filename}, str(e))


def write_file(path: str, content: str, create_dirs: bool = True) -> ToolResult:
    """Write content to a file."""
    try:
        full_path = resolve_path(path)
        action = "created" if not full_path.exists() else "overwritten"

        if create_dirs:
            full_path.parent.mkdir(parents=True, exist_ok=True)

        full_path.write_text(content, encoding="utf-8")

        return ToolResult(ResultStatus.SUCCESS, {
            "path": str(full_path),
            "action": action,
            "size_bytes": len(content.encode("utf-8"))
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"path": path}, str(e))


def edit_file(path: str, old_str: str, new_str: str,
              replace_all: bool = False) -> ToolResult:
    """Edit a file by replacing text. Empty old_str creates new file."""
    try:
        full_path = resolve_path(path)

        if old_str == "":
            return write_file(path, new_str)

        if not full_path.exists():
            return ToolResult(ResultStatus.ERROR, {"path": str(full_path)},
                            "File not found")

        original = full_path.read_text(encoding="utf-8")
        if old_str not in original:
            return ToolResult(ResultStatus.ERROR,
                            {"path": str(full_path)},
                            "Search string not found")

        count = original.count(old_str) if replace_all else 1
        edited = original.replace(old_str, new_str, -1 if replace_all else 1)
        full_path.write_text(edited, encoding="utf-8")

        return ToolResult(ResultStatus.SUCCESS, {
            "path": str(full_path),
            "replacements": count
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"path": path}, str(e))


def list_files(path: str = ".", pattern: Optional[str] = None,
               recursive: bool = False) -> ToolResult:
    """List files in a directory."""
    try:
        full_path = resolve_path(path)
        if not full_path.exists():
            return ToolResult(ResultStatus.ERROR, {"path": str(full_path)},
                            "Directory not found")

        files = []
        iterator = full_path.rglob(pattern or "*") if recursive else full_path.glob(pattern or "*")

        for item in iterator:
            if item.name.startswith("."):
                continue
            try:
                files.append({
                    "name": item.name,
                    "path": str(item.relative_to(full_path)),
                    "type": "file" if item.is_file() else "directory",
                    "size": item.stat().st_size if item.is_file() else None
                })
            except (PermissionError, OSError):
                continue

        files.sort(key=lambda x: (x["type"] == "file", x["name"].lower()))

        return ToolResult(ResultStatus.SUCCESS, {
            "path": str(full_path),
            "files": files,
            "count": len(files)
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"path": path}, str(e))


def glob_search(pattern: str, path: str = ".", max_results: int = 100) -> ToolResult:
    """Find files matching a glob pattern."""
    try:
        base_path = resolve_path(path)
        if not base_path.exists():
            return ToolResult(ResultStatus.ERROR, {"path": str(base_path)},
                            "Base directory not found")

        matches = []
        for match in base_path.glob(pattern):
            if len(matches) >= max_results:
                break
            matches.append({
                "path": str(match.relative_to(base_path)),
                "type": "file" if match.is_file() else "directory"
            })

        return ToolResult(ResultStatus.SUCCESS, {
            "pattern": pattern,
            "matches": matches,
            "count": len(matches)
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"pattern": pattern}, str(e))


def grep_search(pattern: str, path: str = ".",
                file_pattern: Optional[str] = None,
                context_lines: int = 0,
                max_results: int = 100) -> ToolResult:
    """Search file contents for a regex pattern."""
    try:
        base_path = resolve_path(path)
        regex = re.compile(pattern)
        matches = []
        files_searched = 0

        def search_file(file_path: Path):
            nonlocal files_searched
            try:
                content = file_path.read_text(encoding="utf-8", errors="ignore")
                lines = content.splitlines()
                files_searched += 1

                for i, line in enumerate(lines):
                    if len(matches) >= max_results:
                        return
                    if regex.search(line):
                        matches.append({
                            "file": str(file_path.relative_to(base_path) if base_path.is_dir() else file_path.name),
                            "line_number": i + 1,
                            "line": line,
                            "context_before": lines[max(0, i-context_lines):i] if context_lines else None,
                            "context_after": lines[i+1:i+1+context_lines] if context_lines else None
                        })
            except (PermissionError, OSError, UnicodeDecodeError):
                pass

        if base_path.is_file():
            search_file(base_path)
        else:
            for fp in base_path.glob(file_pattern or "**/*"):
                if fp.is_file() and len(matches) < max_results:
                    search_file(fp)

        return ToolResult(ResultStatus.SUCCESS, {
            "pattern": pattern,
            "matches": matches,
            "count": len(matches),
            "files_searched": files_searched
        })
    except re.error as e:
        return ToolResult(ResultStatus.ERROR, {"pattern": pattern},
                         f"Invalid regex: {str(e)}")
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"pattern": pattern}, str(e))


def execute_bash(command: str, working_dir: Optional[str] = None,
                 timeout: int = DEFAULT_TIMEOUT) -> ToolResult:
    """Execute a bash command."""
    try:
        cwd = resolve_path(working_dir) if working_dir else Path.cwd()

        result = subprocess.run(
            command, shell=True, capture_output=True, text=True,
            timeout=timeout, cwd=str(cwd)
        )

        return ToolResult(
            ResultStatus.SUCCESS if result.returncode == 0 else ResultStatus.ERROR,
            {
                "command": command,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.returncode
            }
        )
    except subprocess.TimeoutExpired:
        return ToolResult(ResultStatus.ERROR, {"command": command},
                         f"Timed out after {timeout}s")
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"command": command}, str(e))


def execute_python(code: str, timeout: int = DEFAULT_TIMEOUT) -> ToolResult:
    """Execute Python code in a subprocess."""
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            temp_path = f.name

        try:
            result = subprocess.run(
                [sys.executable, temp_path],
                capture_output=True, text=True, timeout=timeout
            )
            return ToolResult(
                ResultStatus.SUCCESS if result.returncode == 0 else ResultStatus.ERROR,
                {
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "exit_code": result.returncode
                }
            )
        finally:
            os.unlink(temp_path)
    except subprocess.TimeoutExpired:
        return ToolResult(ResultStatus.ERROR, {}, f"Timed out after {timeout}s")
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {}, str(e))


def http_fetch(url: str, method: str = "GET",
               headers: Optional[Dict[str, str]] = None,
               body: Optional[str] = None,
               timeout: int = DEFAULT_TIMEOUT) -> ToolResult:
    """Fetch content from a URL."""
    try:
        req = urllib.request.Request(url, method=method)
        req.add_header("User-Agent", "AgenticAgent/2.0")

        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        if body:
            req.data = body.encode("utf-8")

        context = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            content = resp.read()
            try:
                decoded = content.decode("utf-8")
            except UnicodeDecodeError:
                decoded = content.decode("latin-1")

            return ToolResult(ResultStatus.SUCCESS, {
                "url": url,
                "status_code": resp.status,
                "headers": dict(resp.headers),
                "content": decoded[:50000],
                "truncated": len(content) > 50000
            })
    except urllib.error.HTTPError as e:
        return ToolResult(ResultStatus.ERROR,
                         {"url": url, "status_code": e.code},
                         f"HTTP {e.code}: {e.reason}")
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"url": url}, str(e))


def tcp_connect(host: str, port: int, send_data: Optional[str] = None,
                timeout: int = 10, use_ssl: bool = False) -> ToolResult:
    """Open a TCP connection."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)

        if use_ssl:
            context = ssl.create_default_context()
            sock = context.wrap_socket(sock, server_hostname=host)

        sock.connect((host, port))

        received = None
        if send_data:
            sock.sendall(send_data.encode("utf-8"))
            received = sock.recv(4096).decode("utf-8", errors="ignore")

        sock.close()
        return ToolResult(ResultStatus.SUCCESS, {
            "host": host, "port": port, "connected": True,
            "ssl": use_ssl, "received": received
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"host": host, "port": port}, str(e))


def git_status(repo_path: str = ".") -> ToolResult:
    """Get git status."""
    result = execute_bash("git status --porcelain", working_dir=repo_path)
    if result.status == ResultStatus.SUCCESS:
        lines = result.data["stdout"].strip().split("\n") if result.data["stdout"].strip() else []
        files = [{"status": l[:2].strip(), "file": l[3:]} for l in lines if len(l) >= 3]
        result.data["parsed_files"] = files
        result.data["is_clean"] = len(files) == 0
    return result


def git_diff(path: str = ".", staged: bool = False) -> ToolResult:
    """Get git diff."""
    cmd = "git diff --staged" if staged else "git diff"
    return execute_bash(cmd, working_dir=path)


def git_commit(message: str, repo_path: str = ".", add_all: bool = False) -> ToolResult:
    """Create a git commit."""
    if add_all:
        add_result = execute_bash("git add -A", working_dir=repo_path)
        if add_result.status == ResultStatus.ERROR:
            return add_result

    escaped_msg = message.replace('"', '\\"').replace("$", "\\$")
    return execute_bash(f'git commit -m "{escaped_msg}"', working_dir=repo_path)

# =============================================================================
# TOOL SCHEMA DEFINITIONS (Anthropic Native Format)
# =============================================================================

TOOL_SCHEMAS = [
    {
        "name": "read_file",
        "description": "Read the contents of a file from the filesystem.",
        "input_schema": {
            "type": "object",
            "properties": {
                "filename": {"type": "string", "description": "Path to file to read"},
                "encoding": {"type": "string", "description": "Character encoding", "default": "utf-8"},
                "start_line": {"type": "integer", "description": "Starting line (1-indexed)"},
                "end_line": {"type": "integer", "description": "Ending line (1-indexed, inclusive)"}
            },
            "required": ["filename"]
        }
    },
    {
        "name": "write_file",
        "description": "Write content to a file, creating it if it doesn't exist.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path where file should be written"},
                "content": {"type": "string", "description": "Content to write"},
                "create_dirs": {"type": "boolean", "description": "Create parent directories", "default": True}
            },
            "required": ["path", "content"]
        }
    },
    {
        "name": "edit_file",
        "description": "Edit a file by replacing text. Empty old_str creates new file.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to file"},
                "old_str": {"type": "string", "description": "Text to find (empty = create file)"},
                "new_str": {"type": "string", "description": "Text to replace with"},
                "replace_all": {"type": "boolean", "description": "Replace all occurrences", "default": False}
            },
            "required": ["path", "old_str", "new_str"]
        }
    },
    {
        "name": "list_files",
        "description": "List files and directories at the given path.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Directory to list", "default": "."},
                "pattern": {"type": "string", "description": "Glob pattern filter"},
                "recursive": {"type": "boolean", "description": "Include subdirectories", "default": False}
            }
        }
    },
    {
        "name": "glob_search",
        "description": "Find files matching a glob pattern.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Glob pattern (e.g., **/*.py)"},
                "path": {"type": "string", "description": "Base directory", "default": "."},
                "max_results": {"type": "integer", "description": "Max results", "default": 100}
            },
            "required": ["pattern"]
        }
    },
    {
        "name": "grep_search",
        "description": "Search file contents for a regex pattern.",
        "input_schema": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Regex pattern"},
                "path": {"type": "string", "description": "Directory or file", "default": "."},
                "file_pattern": {"type": "string", "description": "Glob pattern for files"},
                "context_lines": {"type": "integer", "description": "Context lines around match", "default": 0},
                "max_results": {"type": "integer", "description": "Max matches", "default": 100}
            },
            "required": ["pattern"]
        }
    },
    {
        "name": "execute_bash",
        "description": "Execute a bash command and return output.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {"type": "string", "description": "Command to execute"},
                "working_dir": {"type": "string", "description": "Working directory"},
                "timeout": {"type": "integer", "description": "Timeout in seconds", "default": 30}
            },
            "required": ["command"]
        }
    },
    {
        "name": "execute_python",
        "description": "Execute Python code in a subprocess.",
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "Python code to execute"},
                "timeout": {"type": "integer", "description": "Timeout in seconds", "default": 30}
            },
            "required": ["code"]
        }
    },
    {
        "name": "http_fetch",
        "description": "Fetch content from a URL using HTTP/HTTPS.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "URL to fetch"},
                "method": {"type": "string", "description": "HTTP method", "default": "GET"},
                "headers": {"type": "object", "description": "Request headers"},
                "body": {"type": "string", "description": "Request body"},
                "timeout": {"type": "integer", "description": "Timeout in seconds", "default": 30}
            },
            "required": ["url"]
        }
    },
    {
        "name": "tcp_connect",
        "description": "Open a TCP connection and optionally send/receive data.",
        "input_schema": {
            "type": "object",
            "properties": {
                "host": {"type": "string", "description": "Hostname or IP"},
                "port": {"type": "integer", "description": "Port number"},
                "send_data": {"type": "string", "description": "Data to send"},
                "timeout": {"type": "integer", "description": "Timeout", "default": 10},
                "use_ssl": {"type": "boolean", "description": "Use SSL/TLS", "default": False}
            },
            "required": ["host", "port"]
        }
    },
    {
        "name": "git_status",
        "description": "Get git status of a repository.",
        "input_schema": {
            "type": "object",
            "properties": {
                "repo_path": {"type": "string", "description": "Repository path", "default": "."}
            }
        }
    },
    {
        "name": "git_diff",
        "description": "Get git diff of changes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Repository path", "default": "."},
                "staged": {"type": "boolean", "description": "Show staged changes", "default": False}
            }
        }
    },
    {
        "name": "git_commit",
        "description": "Create a git commit.",
        "input_schema": {
            "type": "object",
            "properties": {
                "message": {"type": "string", "description": "Commit message"},
                "repo_path": {"type": "string", "description": "Repository path", "default": "."},
                "add_all": {"type": "boolean", "description": "Stage all changes first", "default": False}
            },
            "required": ["message"]
        }
    },
    # SUB-AGENT TOOL - This is the key addition for agents calling agents
    {
        "name": "spawn_agent",
        "description": """Spawn a specialized sub-agent to handle a complex task autonomously.
Use this when a task would benefit from focused, autonomous execution with its own context.
The sub-agent has access to all tools and can work independently.

Common use cases:
- Research tasks (exploring codebase, gathering information)
- Refactoring tasks (analyzing and modifying multiple files)
- Testing tasks (running and analyzing test results)
- Documentation tasks (reading code and generating docs)""",
        "input_schema": {
            "type": "object",
            "properties": {
                "task": {"type": "string", "description": "Detailed description of what the sub-agent should accomplish"},
                "agent_type": {
                    "type": "string",
                    "enum": ["researcher", "coder", "tester", "reviewer", "general"],
                    "description": "Type of agent to spawn (affects system prompt)",
                    "default": "general"
                },
                "context": {"type": "string", "description": "Additional context to provide to the agent"},
                "max_iterations": {"type": "integer", "description": "Maximum tool iterations", "default": 10},
                "allowed_tools": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Specific tools this agent can use (default: all)"
                }
            },
            "required": ["task"]
        }
    },
    {
        "name": "spawn_parallel_agents",
        "description": """Spawn multiple sub-agents to work on tasks in parallel.
Each agent works independently and results are collected when all complete.

Use this for:
- Searching multiple directories simultaneously
- Running independent analyses in parallel
- Performing multiple refactoring tasks concurrently""",
        "input_schema": {
            "type": "object",
            "properties": {
                "agents": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "task": {"type": "string", "description": "Task for this agent"},
                            "agent_type": {"type": "string", "enum": ["researcher", "coder", "tester", "reviewer", "general"]},
                            "context": {"type": "string"}
                        },
                        "required": ["task"]
                    },
                    "description": "List of agent configurations to spawn"
                },
                "max_iterations_each": {"type": "integer", "description": "Max iterations per agent", "default": 5}
            },
            "required": ["agents"]
        }
    }
]

# Tool function registry
TOOL_FUNCTIONS = {
    "read_file": read_file,
    "write_file": write_file,
    "edit_file": edit_file,
    "list_files": list_files,
    "glob_search": glob_search,
    "grep_search": grep_search,
    "execute_bash": execute_bash,
    "execute_python": execute_python,
    "http_fetch": http_fetch,
    "tcp_connect": tcp_connect,
    "git_status": git_status,
    "git_diff": git_diff,
    "git_commit": git_commit,
}

# =============================================================================
# SUB-AGENT SYSTEM PROMPTS
# =============================================================================

AGENT_PROMPTS = {
    "general": """You are a capable coding assistant. Complete the task assigned to you efficiently.
Use tools to read files, search code, and make changes as needed.
Be thorough but focused - accomplish the specific goal you were given.""",

    "researcher": """You are a research-focused agent. Your job is to explore, analyze, and gather information.
Focus on understanding code structure, finding relevant files, and synthesizing information.
Do NOT make changes - only read and analyze. Report your findings comprehensively.""",

    "coder": """You are a coding agent focused on writing and modifying code.
Write clean, well-structured code following existing patterns in the codebase.
Read files before editing. Test your changes when possible.""",

    "tester": """You are a testing agent. Your job is to run tests, analyze results, and identify issues.
Use execute_bash and execute_python to run test suites.
Report test failures clearly with context about what failed and why.""",

    "reviewer": """You are a code review agent. Analyze code for:
- Bugs and potential issues
- Code style and best practices
- Security vulnerabilities
- Performance concerns
Provide specific, actionable feedback with file and line references."""
}

# =============================================================================
# ASYNC AGENT IMPLEMENTATION
# =============================================================================

class SubAgentResult:
    """Result from a sub-agent execution."""
    def __init__(self, agent_id: str, task: str, response: str,
                 tool_calls: int, status: str):
        self.agent_id = agent_id
        self.task = task
        self.response = response
        self.tool_calls = tool_calls
        self.status = status

    def to_dict(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "task": self.task,
            "response": self.response,
            "tool_calls": self.tool_calls,
            "status": self.status
        }


class AgenticSamplingAgentV2:
    """
    Advanced agent with native Anthropic tool use and sub-agent spawning.

    Key features:
    - Uses Anthropic's structured tool_use instead of text parsing
    - Can spawn sub-agents for complex tasks
    - Supports parallel agent execution
    - Async/streaming support
    - Hierarchical agent composition
    """

    def __init__(self,
                 api_key: Optional[str] = None,
                 model: str = DEFAULT_MODEL,
                 allowed_tools: Optional[List[str]] = None,
                 system_prompt: Optional[str] = None,
                 agent_id: Optional[str] = None,
                 parent_agent: Optional['AgenticSamplingAgentV2'] = None,
                 depth: int = 0,
                 max_depth: int = 3,
                 verbose: bool = True):

        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY required")

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = model
        self.system_prompt = system_prompt or AGENT_PROMPTS["general"]
        self.agent_id = agent_id or str(uuid.uuid4())[:8]
        self.parent_agent = parent_agent
        self.depth = depth
        self.max_depth = max_depth
        self.verbose = verbose

        # Filter allowed tools
        if allowed_tools:
            self.allowed_tools = set(allowed_tools)
            self.tools = [t for t in TOOL_SCHEMAS if t["name"] in allowed_tools or t["name"].startswith("spawn")]
        else:
            self.allowed_tools = set(TOOL_FUNCTIONS.keys())
            self.tools = TOOL_SCHEMAS.copy()

        # Add spawn tools only if not at max depth
        if depth < max_depth:
            if "spawn_agent" not in [t["name"] for t in self.tools]:
                self.tools.extend([t for t in TOOL_SCHEMAS if t["name"].startswith("spawn")])

        self.conversation: List[dict] = []
        self.tool_call_count = 0

    def _log(self, message: str, color: str = "dim"):
        """Print with color if verbose."""
        if self.verbose:
            prefix = f"[Agent-{self.agent_id}]" if self.depth > 0 else ""
            print(f"{ANSI.get(color, '')}{prefix} {message}{ANSI['reset']}")

    def _execute_tool(self, name: str, args: dict) -> ToolResult:
        """Execute a single tool."""
        self.tool_call_count += 1

        if name == "spawn_agent":
            return self._spawn_sub_agent(**args)
        elif name == "spawn_parallel_agents":
            return self._spawn_parallel_agents(**args)

        if name not in TOOL_FUNCTIONS:
            return ToolResult(ResultStatus.ERROR, {"tool": name},
                            f"Unknown tool: {name}")

        if name not in self.allowed_tools:
            return ToolResult(ResultStatus.ERROR, {"tool": name},
                            f"Tool not allowed: {name}")

        try:
            return TOOL_FUNCTIONS[name](**args)
        except TypeError as e:
            return ToolResult(ResultStatus.ERROR, {"tool": name, "args": args},
                            f"Invalid arguments: {e}")
        except Exception as e:
            return ToolResult(ResultStatus.ERROR, {"tool": name}, str(e))

    def _spawn_sub_agent(self, task: str, agent_type: str = "general",
                        context: Optional[str] = None,
                        max_iterations: int = 10,
                        allowed_tools: Optional[List[str]] = None) -> ToolResult:
        """Spawn a sub-agent to handle a task."""

        if self.depth >= self.max_depth:
            return ToolResult(ResultStatus.ERROR, {},
                            f"Max agent depth ({self.max_depth}) reached")

        self._log(f"Spawning {agent_type} sub-agent for: {task[:50]}...", "agent")

        system = AGENT_PROMPTS.get(agent_type, AGENT_PROMPTS["general"])
        if context:
            system += f"\n\nAdditional context:\n{context}"

        sub_agent = AgenticSamplingAgentV2(
            api_key=self.api_key,
            model=self.model,
            allowed_tools=allowed_tools,
            system_prompt=system,
            parent_agent=self,
            depth=self.depth + 1,
            max_depth=self.max_depth,
            verbose=self.verbose
        )

        try:
            response = sub_agent.process_message(task, max_iterations=max_iterations)

            result = SubAgentResult(
                agent_id=sub_agent.agent_id,
                task=task,
                response=response,
                tool_calls=sub_agent.tool_call_count,
                status="success"
            )

            return ToolResult(ResultStatus.SUCCESS, result.to_dict())

        except Exception as e:
            return ToolResult(ResultStatus.ERROR,
                            {"agent_id": sub_agent.agent_id, "task": task},
                            str(e))

    def _spawn_parallel_agents(self, agents: List[dict],
                               max_iterations_each: int = 5) -> ToolResult:
        """Spawn multiple agents in parallel."""

        if self.depth >= self.max_depth:
            return ToolResult(ResultStatus.ERROR, {},
                            f"Max agent depth ({self.max_depth}) reached")

        self._log(f"Spawning {len(agents)} parallel agents...", "agent")

        async def run_agent(agent_config: dict) -> SubAgentResult:
            task = agent_config["task"]
            agent_type = agent_config.get("agent_type", "general")
            context = agent_config.get("context")

            system = AGENT_PROMPTS.get(agent_type, AGENT_PROMPTS["general"])
            if context:
                system += f"\n\nAdditional context:\n{context}"

            sub_agent = AgenticSamplingAgentV2(
                api_key=self.api_key,
                model=self.model,
                system_prompt=system,
                parent_agent=self,
                depth=self.depth + 1,
                max_depth=self.max_depth,
                verbose=self.verbose
            )

            try:
                # Run in thread pool since we're mixing sync/async
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: sub_agent.process_message(task, max_iterations=max_iterations_each)
                )

                return SubAgentResult(
                    agent_id=sub_agent.agent_id,
                    task=task,
                    response=response,
                    tool_calls=sub_agent.tool_call_count,
                    status="success"
                )
            except Exception as e:
                return SubAgentResult(
                    agent_id=sub_agent.agent_id,
                    task=task,
                    response=str(e),
                    tool_calls=sub_agent.tool_call_count,
                    status="error"
                )

        async def run_all():
            tasks = [run_agent(cfg) for cfg in agents]
            return await asyncio.gather(*tasks)

        # Run the async agents
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        results = loop.run_until_complete(run_all())

        return ToolResult(ResultStatus.SUCCESS, {
            "agents_completed": len(results),
            "results": [r.to_dict() for r in results]
        })

    def process_message(self, user_message: str,
                       max_iterations: int = 20) -> str:
        """
        Process a message through the agentic loop.

        Uses Anthropic's native tool_use for structured tool calling.
        """
        self.conversation.append({
            "role": "user",
            "content": user_message
        })

        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            # Call the API with tools
            response = self.client.messages.create(
                model=self.model,
                max_tokens=MAX_TOKENS,
                system=self.system_prompt,
                tools=self.tools,
                messages=self.conversation
            )

            # Process the response
            assistant_content = []
            tool_results = []
            has_tool_use = False

            for block in response.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    has_tool_use = True
                    tool_name = block.name
                    tool_args = block.input
                    tool_id = block.id

                    assistant_content.append({
                        "type": "tool_use",
                        "id": tool_id,
                        "name": tool_name,
                        "input": tool_args
                    })

                    self._log(f"Tool: {tool_name}({json.dumps(tool_args, default=str)[:100]}...)", "tool")

                    result = self._execute_tool(tool_name, tool_args)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": result.to_content()
                    })

            # Add assistant message to conversation
            self.conversation.append({
                "role": "assistant",
                "content": assistant_content
            })

            # If no tool use, we're done
            if not has_tool_use:
                # Extract text response
                text_parts = [b["text"] for b in assistant_content if b["type"] == "text"]
                return "\n".join(text_parts)

            # Add tool results and continue
            self.conversation.append({
                "role": "user",
                "content": tool_results
            })

        return "Maximum iterations reached."

    def stream_message(self, user_message: str,
                      max_iterations: int = 20) -> AsyncIterator[str]:
        """Stream a message response."""
        self.conversation.append({
            "role": "user",
            "content": user_message
        })

        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            # Stream the response
            full_response = ""
            tool_uses = []

            with self.client.messages.stream(
                model=self.model,
                max_tokens=MAX_TOKENS,
                system=self.system_prompt,
                tools=self.tools,
                messages=self.conversation
            ) as stream:
                for event in stream:
                    if hasattr(event, 'type'):
                        if event.type == "content_block_delta":
                            if hasattr(event.delta, 'text'):
                                yield event.delta.text
                                full_response += event.delta.text
                        elif event.type == "content_block_start":
                            if hasattr(event.content_block, 'type') and event.content_block.type == "tool_use":
                                tool_uses.append({
                                    "id": event.content_block.id,
                                    "name": event.content_block.name,
                                    "input": {}
                                })
                        elif event.type == "content_block_delta":
                            if hasattr(event.delta, 'partial_json') and tool_uses:
                                # Accumulate tool input
                                pass

                # Get the final message
                final = stream.get_final_message()

            # Process tool uses from final message
            assistant_content = []
            tool_results = []
            has_tool_use = False

            for block in final.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    has_tool_use = True
                    assistant_content.append({
                        "type": "tool_use",
                        "id": block.id,
                        "name": block.name,
                        "input": block.input
                    })

                    yield f"\n[Executing {block.name}...]\n"

                    result = self._execute_tool(block.name, block.input)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result.to_content()
                    })

            self.conversation.append({
                "role": "assistant",
                "content": assistant_content
            })

            if not has_tool_use:
                return

            self.conversation.append({
                "role": "user",
                "content": tool_results
            })

            yield "\n[Continuing...]\n"

    def run_interactive(self):
        """Run in interactive mode."""
        print(f"\n{ANSI['info']}Agentic Sampling Agent V2{ANSI['reset']}")
        print(f"{ANSI['dim']}Model: {self.model}")
        print(f"Tools: {len(self.tools)} available")
        print(f"Max agent depth: {self.max_depth}")
        print(f"Commands: exit, clear, tools{ANSI['reset']}\n")

        while True:
            try:
                user_input = input(f"{ANSI['user']}You: {ANSI['reset']}")
            except (KeyboardInterrupt, EOFError):
                print("\nGoodbye!")
                break

            if not user_input.strip():
                continue

            cmd = user_input.strip().lower()
            if cmd in ("exit", "quit"):
                print("\nGoodbye!")
                break
            elif cmd == "clear":
                self.conversation = []
                print("Conversation cleared.\n")
                continue
            elif cmd == "tools":
                print("\nAvailable tools:")
                for tool in self.tools:
                    print(f"  - {tool['name']}: {tool['description'][:60]}...")
                print()
                continue

            print(f"\n{ANSI['assistant']}Assistant: {ANSI['reset']}", end="", flush=True)

            # Use streaming
            for chunk in self.stream_message(user_input):
                print(chunk, end="", flush=True)

            print("\n")


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Agentic Sampling Agent V2")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Model to use")
    parser.add_argument("--tools", nargs="+", help="Specific tools to enable")
    parser.add_argument("--prompt", help="Single prompt (non-interactive)")
    parser.add_argument("--max-depth", type=int, default=3, help="Max agent nesting depth")
    parser.add_argument("--quiet", action="store_true", help="Less verbose output")
    parser.add_argument("--list-tools", action="store_true", help="List tools and exit")

    args = parser.parse_args()

    if args.list_tools:
        print("\nAvailable tools:\n")
        for tool in TOOL_SCHEMAS:
            print(f"{tool['name']}:")
            print(f"  {tool['description'][:200]}")
            print()
        return

    try:
        agent = AgenticSamplingAgentV2(
            model=args.model,
            allowed_tools=args.tools,
            max_depth=args.max_depth,
            verbose=not args.quiet
        )

        if args.prompt:
            response = agent.process_message(args.prompt)
            print(response)
        else:
            agent.run_interactive()

    except ValueError as e:
        print(f"Error: {e}")
        print("Please set ANTHROPIC_API_KEY environment variable")
        sys.exit(1)


if __name__ == "__main__":
    main()
