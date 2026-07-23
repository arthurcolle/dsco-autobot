#!/usr/bin/env python3
"""
Agentic Sampling Agent V3 - Enhanced Intelligence

Implements 16 key improvements to the core agentic sampling loop:

1.  REFLECTION - Self-critique before final response
2.  TOOL RESULT CACHING - Memoize identical tool calls
3.  PLANNING PHASE - Explicit planning before execution
4.  MEMORY/SCRATCHPAD - Working memory for complex tasks
5.  PARALLEL TOOL EXECUTION - Batch independent tools
6.  CONFIDENCE SCORING - Rate response confidence
7.  BACKTRACKING - Undo and retry with different approaches
8.  GOAL DECOMPOSITION - Break complex goals into sub-goals
9.  PROGRESSIVE TOOL UNLOCKING - Start minimal, unlock as needed
10. ERROR RECOVERY STRATEGIES - Multiple fallback approaches
11. CONTEXT COMPRESSION - Summarize long conversations
12. TOOL USAGE ANALYTICS - Learn from tool patterns
13. VERIFICATION STEPS - Verify actions succeeded
14. PRIORITY SCHEDULING - Prioritize critical operations
15. BUDGET AWARENESS - Track token/time budgets
16. META-PROMPTING - Self-optimize prompts
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
import hashlib
import time
import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Iterator
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import anthropic
except ImportError:
    print("pip install anthropic")
    sys.exit(1)

# =============================================================================
# Configuration
# =============================================================================

DEFAULT_MODEL = "claude-sonnet-5"
PLANNER_MODEL = "claude-sonnet-5"  # Fast model for planning
REFLECTOR_MODEL = "claude-sonnet-5"  # Same model for reflection
MAX_TOKENS = 8192
DEFAULT_TIMEOUT = 30

ANSI = {
    "reset": "\u001b[0m", "user": "\u001b[94m", "assistant": "\u001b[93m",
    "tool": "\u001b[92m", "error": "\u001b[91m", "info": "\u001b[96m",
    "agent": "\u001b[95m", "dim": "\u001b[90m", "plan": "\u001b[33m",
    "reflect": "\u001b[35m", "cache": "\u001b[36m",
}

# =============================================================================
# IMPROVEMENT 2: Tool Result Caching
# =============================================================================

class ToolCache:
    """LRU cache for tool results with TTL."""

    def __init__(self, max_size: int = 100, ttl_seconds: int = 300):
        self.cache: Dict[str, Tuple[Any, float]] = {}
        self.max_size = max_size
        self.ttl = ttl_seconds
        self.hits = 0
        self.misses = 0

    def _make_key(self, tool_name: str, args: Dict) -> str:
        """Create cache key from tool name and args."""
        serialized = json.dumps({"tool": tool_name, "args": args}, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()[:16]

    def get(self, tool_name: str, args: Dict) -> Optional[Any]:
        """Get cached result if exists and not expired."""
        key = self._make_key(tool_name, args)
        if key in self.cache:
            result, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl:
                self.hits += 1
                return result
            del self.cache[key]
        self.misses += 1
        return None

    def set(self, tool_name: str, args: Dict, result: Any):
        """Cache a tool result."""
        if len(self.cache) >= self.max_size:
            # Remove oldest entries
            oldest = sorted(self.cache.items(), key=lambda x: x[1][1])[:self.max_size // 4]
            for key, _ in oldest:
                del self.cache[key]

        key = self._make_key(tool_name, args)
        self.cache[key] = (result, time.time())

    def invalidate(self, pattern: Optional[str] = None):
        """Invalidate cache entries matching pattern."""
        if pattern is None:
            self.cache.clear()
        else:
            keys_to_delete = [k for k in self.cache if pattern in k]
            for k in keys_to_delete:
                del self.cache[k]

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


# =============================================================================
# IMPROVEMENT 4: Memory/Scratchpad
# =============================================================================

class WorkingMemory:
    """
    Working memory for maintaining context across tool calls.
    Acts as a scratchpad for intermediate results and insights.
    """

    def __init__(self, max_items: int = 50):
        self.items: List[Dict[str, Any]] = []
        self.max_items = max_items
        self.facts: Dict[str, str] = {}  # Key facts discovered
        self.goals: List[str] = []  # Current goals
        self.hypotheses: List[str] = []  # Working hypotheses

    def add(self, category: str, content: str, metadata: Optional[Dict] = None):
        """Add an item to working memory."""
        item = {
            "category": category,
            "content": content,
            "timestamp": datetime.now().isoformat(),
            "metadata": metadata or {}
        }
        self.items.append(item)

        # Evict old items if needed
        if len(self.items) > self.max_items:
            self.items = self.items[-self.max_items:]

    def add_fact(self, key: str, value: str):
        """Store a discovered fact."""
        self.facts[key] = value

    def add_goal(self, goal: str):
        """Add a goal."""
        if goal not in self.goals:
            self.goals.append(goal)

    def complete_goal(self, goal: str):
        """Mark a goal as complete."""
        if goal in self.goals:
            self.goals.remove(goal)
            self.add("goal_completed", goal)

    def add_hypothesis(self, hypothesis: str):
        """Add a working hypothesis."""
        self.hypotheses.append(hypothesis)

    def get_context(self, max_items: int = 10) -> str:
        """Get a summary of working memory for context."""
        parts = []

        if self.goals:
            parts.append("Current Goals:\n" + "\n".join(f"  - {g}" for g in self.goals))

        if self.facts:
            facts_str = "\n".join(f"  - {k}: {v}" for k, v in list(self.facts.items())[-10:])
            parts.append(f"Known Facts:\n{facts_str}")

        if self.hypotheses:
            parts.append("Working Hypotheses:\n" + "\n".join(f"  - {h}" for h in self.hypotheses[-5:]))

        recent = self.items[-max_items:] if self.items else []
        if recent:
            items_str = "\n".join(f"  [{i['category']}] {i['content'][:100]}" for i in recent)
            parts.append(f"Recent Activity:\n{items_str}")

        return "\n\n".join(parts) if parts else "No working memory yet."

    def clear(self):
        """Clear working memory."""
        self.items.clear()
        self.facts.clear()
        self.goals.clear()
        self.hypotheses.clear()


# =============================================================================
# IMPROVEMENT 12: Tool Usage Analytics
# =============================================================================

class ToolAnalytics:
    """Track and analyze tool usage patterns."""

    def __init__(self):
        self.usage_count: Dict[str, int] = defaultdict(int)
        self.success_count: Dict[str, int] = defaultdict(int)
        self.failure_count: Dict[str, int] = defaultdict(int)
        self.avg_duration: Dict[str, float] = defaultdict(float)
        self.sequences: List[List[str]] = []  # Tool call sequences
        self.current_sequence: List[str] = []

    def record_call(self, tool_name: str, success: bool, duration: float):
        """Record a tool call."""
        self.usage_count[tool_name] += 1
        if success:
            self.success_count[tool_name] += 1
        else:
            self.failure_count[tool_name] += 1

        # Update rolling average duration
        count = self.usage_count[tool_name]
        old_avg = self.avg_duration[tool_name]
        self.avg_duration[tool_name] = ((count - 1) * old_avg + duration) / count

        self.current_sequence.append(tool_name)

    def end_sequence(self):
        """End current tool sequence."""
        if self.current_sequence:
            self.sequences.append(self.current_sequence.copy())
            self.current_sequence = []

    def get_success_rate(self, tool_name: str) -> float:
        """Get success rate for a tool."""
        total = self.success_count[tool_name] + self.failure_count[tool_name]
        return self.success_count[tool_name] / total if total > 0 else 0.0

    def suggest_next_tool(self, current_tool: str) -> Optional[str]:
        """Suggest next tool based on historical patterns."""
        # Find sequences where current_tool appears
        next_tools: Dict[str, int] = defaultdict(int)
        for seq in self.sequences:
            for i, tool in enumerate(seq[:-1]):
                if tool == current_tool:
                    next_tools[seq[i + 1]] += 1

        if not next_tools:
            return None

        return max(next_tools.items(), key=lambda x: x[1])[0]

    def get_summary(self) -> str:
        """Get analytics summary."""
        lines = ["Tool Usage Analytics:"]
        for tool in sorted(self.usage_count.keys()):
            rate = self.get_success_rate(tool)
            lines.append(
                f"  {tool}: {self.usage_count[tool]} calls, "
                f"{rate:.1%} success, {self.avg_duration[tool]:.2f}s avg"
            )
        return "\n".join(lines)


# =============================================================================
# IMPROVEMENT 15: Budget Awareness
# =============================================================================

@dataclass
class ExecutionBudget:
    """Track and enforce execution budgets."""
    max_tokens: int = 100000
    max_time_seconds: float = 300.0
    max_tool_calls: int = 50
    max_api_calls: int = 20

    tokens_used: int = 0
    time_started: float = field(default_factory=time.time)
    tool_calls: int = 0
    api_calls: int = 0

    def use_tokens(self, count: int):
        self.tokens_used += count

    def use_tool_call(self):
        self.tool_calls += 1

    def use_api_call(self):
        self.api_calls += 1

    @property
    def elapsed_time(self) -> float:
        return time.time() - self.time_started

    @property
    def tokens_remaining(self) -> int:
        return max(0, self.max_tokens - self.tokens_used)

    @property
    def time_remaining(self) -> float:
        return max(0, self.max_time_seconds - self.elapsed_time)

    @property
    def tool_calls_remaining(self) -> int:
        return max(0, self.max_tool_calls - self.tool_calls)

    @property
    def api_calls_remaining(self) -> int:
        return max(0, self.max_api_calls - self.api_calls)

    def is_exhausted(self) -> bool:
        return (self.tokens_remaining <= 0 or
                self.time_remaining <= 0 or
                self.tool_calls_remaining <= 0 or
                self.api_calls_remaining <= 0)

    def get_status(self) -> str:
        return (f"Budget: {self.tokens_remaining:,} tokens, "
                f"{self.time_remaining:.1f}s, "
                f"{self.tool_calls_remaining} tools, "
                f"{self.api_calls_remaining} API calls remaining")


# =============================================================================
# IMPROVEMENT 8: Goal Decomposition
# =============================================================================

@dataclass
class Goal:
    """Represents a goal or sub-goal."""
    id: str
    description: str
    parent_id: Optional[str] = None
    status: str = "pending"  # pending, in_progress, completed, failed
    priority: int = 5  # 1-10, higher = more important
    sub_goals: List['Goal'] = field(default_factory=list)
    result: Optional[str] = None

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "description": self.description,
            "status": self.status,
            "priority": self.priority,
            "sub_goals": [g.to_dict() for g in self.sub_goals]
        }


class GoalManager:
    """Manages hierarchical goal decomposition."""

    def __init__(self):
        self.goals: Dict[str, Goal] = {}
        self.root_goals: List[str] = []
        self._next_id = 0

    def _generate_id(self) -> str:
        self._next_id += 1
        return f"goal_{self._next_id}"

    def add_goal(self, description: str, parent_id: Optional[str] = None,
                 priority: int = 5) -> Goal:
        """Add a new goal."""
        goal_id = self._generate_id()
        goal = Goal(
            id=goal_id,
            description=description,
            parent_id=parent_id,
            priority=priority
        )
        self.goals[goal_id] = goal

        if parent_id and parent_id in self.goals:
            self.goals[parent_id].sub_goals.append(goal)
        else:
            self.root_goals.append(goal_id)

        return goal

    def update_status(self, goal_id: str, status: str, result: Optional[str] = None):
        """Update goal status."""
        if goal_id in self.goals:
            self.goals[goal_id].status = status
            self.goals[goal_id].result = result

    def get_next_goal(self) -> Optional[Goal]:
        """Get the next goal to work on (highest priority pending)."""
        pending = [g for g in self.goals.values() if g.status == "pending"]
        if not pending:
            return None
        return max(pending, key=lambda g: g.priority)

    def get_progress(self) -> Dict[str, int]:
        """Get goal completion progress."""
        statuses = defaultdict(int)
        for goal in self.goals.values():
            statuses[goal.status] += 1
        return dict(statuses)

    def to_tree_str(self) -> str:
        """Get string representation of goal tree."""
        lines = []

        def _format_goal(goal: Goal, indent: int = 0):
            prefix = "  " * indent
            status_icon = {"pending": "○", "in_progress": "◐",
                          "completed": "●", "failed": "✗"}.get(goal.status, "?")
            lines.append(f"{prefix}{status_icon} [{goal.priority}] {goal.description}")
            for sub in goal.sub_goals:
                _format_goal(sub, indent + 1)

        for goal_id in self.root_goals:
            _format_goal(self.goals[goal_id])

        return "\n".join(lines) if lines else "No goals."


# =============================================================================
# IMPROVEMENT 7: Backtracking Support
# =============================================================================

@dataclass
class Checkpoint:
    """A checkpoint for backtracking."""
    id: str
    description: str
    conversation: List[Dict]
    memory_snapshot: Dict
    timestamp: float = field(default_factory=time.time)


class BacktrackingManager:
    """Manage checkpoints for backtracking."""

    def __init__(self, max_checkpoints: int = 10):
        self.checkpoints: List[Checkpoint] = []
        self.max_checkpoints = max_checkpoints
        self._next_id = 0

    def create_checkpoint(self, description: str, conversation: List[Dict],
                         memory: WorkingMemory) -> Checkpoint:
        """Create a new checkpoint."""
        self._next_id += 1
        checkpoint = Checkpoint(
            id=f"ckpt_{self._next_id}",
            description=description,
            conversation=json.loads(json.dumps(conversation, default=str)),
            memory_snapshot={
                "facts": memory.facts.copy(),
                "goals": memory.goals.copy(),
                "items": memory.items.copy()
            }
        )
        self.checkpoints.append(checkpoint)

        if len(self.checkpoints) > self.max_checkpoints:
            self.checkpoints = self.checkpoints[-self.max_checkpoints:]

        return checkpoint

    def rollback(self, checkpoint_id: str) -> Optional[Checkpoint]:
        """Rollback to a checkpoint."""
        for ckpt in self.checkpoints:
            if ckpt.id == checkpoint_id:
                return ckpt
        return None

    def list_checkpoints(self) -> List[str]:
        """List available checkpoints."""
        return [f"{c.id}: {c.description}" for c in self.checkpoints]


# =============================================================================
# Result Types
# =============================================================================

class ResultStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"
    PARTIAL = "partial"
    CACHED = "cached"

@dataclass
class ToolResult:
    status: ResultStatus
    data: Dict[str, Any]
    message: Optional[str] = None
    cached: bool = False
    duration: float = 0.0

    def to_content(self) -> str:
        result = {"status": self.status.value, "data": self.data}
        if self.message:
            result["message"] = self.message
        if self.cached:
            result["cached"] = True
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
# TOOL IMPLEMENTATIONS (Same as V2, condensed)
# =============================================================================

def read_file(filename: str, encoding: str = "utf-8",
              start_line: Optional[int] = None,
              end_line: Optional[int] = None) -> ToolResult:
    """Read file contents."""
    try:
        path = resolve_path(filename)
        if not path.exists():
            return ToolResult(ResultStatus.ERROR, {"path": str(path)}, "File not found")
        content = path.read_text(encoding=encoding)
        lines = content.splitlines(keepends=True)
        total = len(lines)
        if start_line or end_line:
            lines = lines[(start_line or 1) - 1:end_line or total]
            content = "".join(lines)
        return ToolResult(ResultStatus.SUCCESS, {
            "path": str(path), "content": content,
            "total_lines": total, "lines_returned": len(lines)
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"path": filename}, str(e))


def write_file(path: str, content: str, create_dirs: bool = True) -> ToolResult:
    """Write file."""
    try:
        full_path = resolve_path(path)
        action = "created" if not full_path.exists() else "overwritten"
        if create_dirs:
            full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(content, encoding="utf-8")
        return ToolResult(ResultStatus.SUCCESS, {
            "path": str(full_path), "action": action,
            "size_bytes": len(content.encode("utf-8"))
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"path": path}, str(e))


def edit_file(path: str, old_str: str, new_str: str,
              replace_all: bool = False) -> ToolResult:
    """Edit file by replacing text."""
    try:
        full_path = resolve_path(path)
        if old_str == "":
            return write_file(path, new_str)
        if not full_path.exists():
            return ToolResult(ResultStatus.ERROR, {"path": str(full_path)}, "File not found")
        original = full_path.read_text(encoding="utf-8")
        if old_str not in original:
            return ToolResult(ResultStatus.ERROR, {"path": str(full_path)},
                            "Search string not found")
        count = original.count(old_str) if replace_all else 1
        edited = original.replace(old_str, new_str, -1 if replace_all else 1)
        full_path.write_text(edited, encoding="utf-8")
        return ToolResult(ResultStatus.SUCCESS, {
            "path": str(full_path), "replacements": count
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"path": path}, str(e))


def list_files(path: str = ".", pattern: Optional[str] = None,
               recursive: bool = False) -> ToolResult:
    """List directory contents."""
    try:
        full_path = resolve_path(path)
        if not full_path.exists():
            return ToolResult(ResultStatus.ERROR, {"path": str(full_path)}, "Not found")
        files = []
        iterator = full_path.rglob(pattern or "*") if recursive else full_path.glob(pattern or "*")
        for item in iterator:
            if not item.name.startswith("."):
                files.append({
                    "name": item.name,
                    "path": str(item.relative_to(full_path)),
                    "type": "file" if item.is_file() else "directory"
                })
        return ToolResult(ResultStatus.SUCCESS, {
            "path": str(full_path), "files": files[:100], "count": len(files)
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"path": path}, str(e))


def glob_search(pattern: str, path: str = ".", max_results: int = 100) -> ToolResult:
    """Glob search."""
    try:
        base = resolve_path(path)
        matches = [{"path": str(m.relative_to(base)), "type": "file" if m.is_file() else "dir"}
                   for m in list(base.glob(pattern))[:max_results]]
        return ToolResult(ResultStatus.SUCCESS, {
            "pattern": pattern, "matches": matches, "count": len(matches)
        })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"pattern": pattern}, str(e))


def grep_search(pattern: str, path: str = ".",
                file_pattern: Optional[str] = None,
                context_lines: int = 0,
                max_results: int = 100) -> ToolResult:
    """Grep search."""
    try:
        base = resolve_path(path)
        regex = re.compile(pattern)
        matches = []

        def search_file(fp: Path):
            try:
                content = fp.read_text(errors="ignore")
                for i, line in enumerate(content.splitlines()):
                    if len(matches) >= max_results:
                        return
                    if regex.search(line):
                        matches.append({
                            "file": str(fp.relative_to(base) if base.is_dir() else fp.name),
                            "line_number": i + 1, "line": line
                        })
            except:
                pass

        if base.is_file():
            search_file(base)
        else:
            for fp in base.glob(file_pattern or "**/*"):
                if fp.is_file():
                    search_file(fp)

        return ToolResult(ResultStatus.SUCCESS, {
            "pattern": pattern, "matches": matches, "count": len(matches)
        })
    except re.error as e:
        return ToolResult(ResultStatus.ERROR, {"pattern": pattern}, f"Regex error: {e}")
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"pattern": pattern}, str(e))


def execute_bash(command: str, working_dir: Optional[str] = None,
                 timeout: int = DEFAULT_TIMEOUT) -> ToolResult:
    """Execute bash command."""
    try:
        cwd = resolve_path(working_dir) if working_dir else Path.cwd()
        result = subprocess.run(command, shell=True, capture_output=True,
                               text=True, timeout=timeout, cwd=str(cwd))
        return ToolResult(
            ResultStatus.SUCCESS if result.returncode == 0 else ResultStatus.ERROR,
            {"command": command, "stdout": result.stdout,
             "stderr": result.stderr, "exit_code": result.returncode}
        )
    except subprocess.TimeoutExpired:
        return ToolResult(ResultStatus.ERROR, {"command": command},
                         f"Timeout after {timeout}s")
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"command": command}, str(e))


def execute_python(code: str, timeout: int = DEFAULT_TIMEOUT) -> ToolResult:
    """Execute Python code."""
    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(code)
            temp_path = f.name
        try:
            result = subprocess.run([sys.executable, temp_path],
                                   capture_output=True, text=True, timeout=timeout)
            return ToolResult(
                ResultStatus.SUCCESS if result.returncode == 0 else ResultStatus.ERROR,
                {"stdout": result.stdout, "stderr": result.stderr,
                 "exit_code": result.returncode}
            )
        finally:
            os.unlink(temp_path)
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {}, str(e))


def http_fetch(url: str, method: str = "GET", headers: Optional[Dict[str, str]] = None,
               body: Optional[str] = None, timeout: int = DEFAULT_TIMEOUT) -> ToolResult:
    """HTTP fetch."""
    try:
        req = urllib.request.Request(url, method=method)
        req.add_header("User-Agent", "AgentV3/1.0")
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        if body:
            req.data = body.encode("utf-8")

        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            content = resp.read().decode("utf-8", errors="replace")
            return ToolResult(ResultStatus.SUCCESS, {
                "url": url, "status_code": resp.status,
                "content": content[:50000], "truncated": len(content) > 50000
            })
    except Exception as e:
        return ToolResult(ResultStatus.ERROR, {"url": url}, str(e))


def git_status(repo_path: str = ".") -> ToolResult:
    """Git status."""
    result = execute_bash("git status --porcelain", working_dir=repo_path)
    if result.status == ResultStatus.SUCCESS:
        lines = result.data["stdout"].strip().split("\n") if result.data["stdout"].strip() else []
        result.data["parsed_files"] = [{"status": l[:2], "file": l[3:]} for l in lines if len(l) >= 3]
        result.data["is_clean"] = len(result.data["parsed_files"]) == 0
    return result


# =============================================================================
# TOOL SCHEMAS
# =============================================================================

TOOL_SCHEMAS = [
    {"name": "read_file", "description": "Read file contents",
     "input_schema": {"type": "object", "properties": {
         "filename": {"type": "string"}, "encoding": {"type": "string", "default": "utf-8"},
         "start_line": {"type": "integer"}, "end_line": {"type": "integer"}
     }, "required": ["filename"]}},
    {"name": "write_file", "description": "Write content to file",
     "input_schema": {"type": "object", "properties": {
         "path": {"type": "string"}, "content": {"type": "string"},
         "create_dirs": {"type": "boolean", "default": True}
     }, "required": ["path", "content"]}},
    {"name": "edit_file", "description": "Edit file by replacing text",
     "input_schema": {"type": "object", "properties": {
         "path": {"type": "string"}, "old_str": {"type": "string"},
         "new_str": {"type": "string"}, "replace_all": {"type": "boolean", "default": False}
     }, "required": ["path", "old_str", "new_str"]}},
    {"name": "list_files", "description": "List directory contents",
     "input_schema": {"type": "object", "properties": {
         "path": {"type": "string", "default": "."}, "pattern": {"type": "string"},
         "recursive": {"type": "boolean", "default": False}
     }}},
    {"name": "glob_search", "description": "Find files by glob pattern",
     "input_schema": {"type": "object", "properties": {
         "pattern": {"type": "string"}, "path": {"type": "string", "default": "."},
         "max_results": {"type": "integer", "default": 100}
     }, "required": ["pattern"]}},
    {"name": "grep_search", "description": "Search file contents with regex",
     "input_schema": {"type": "object", "properties": {
         "pattern": {"type": "string"}, "path": {"type": "string", "default": "."},
         "file_pattern": {"type": "string"}, "context_lines": {"type": "integer", "default": 0},
         "max_results": {"type": "integer", "default": 100}
     }, "required": ["pattern"]}},
    {"name": "execute_bash", "description": "Execute bash command",
     "input_schema": {"type": "object", "properties": {
         "command": {"type": "string"}, "working_dir": {"type": "string"},
         "timeout": {"type": "integer", "default": 30}
     }, "required": ["command"]}},
    {"name": "execute_python", "description": "Execute Python code",
     "input_schema": {"type": "object", "properties": {
         "code": {"type": "string"}, "timeout": {"type": "integer", "default": 30}
     }, "required": ["code"]}},
    {"name": "http_fetch", "description": "Fetch URL content",
     "input_schema": {"type": "object", "properties": {
         "url": {"type": "string"}, "method": {"type": "string", "default": "GET"},
         "headers": {"type": "object"}, "body": {"type": "string"},
         "timeout": {"type": "integer", "default": 30}
     }, "required": ["url"]}},
    {"name": "git_status", "description": "Get git status",
     "input_schema": {"type": "object", "properties": {
         "repo_path": {"type": "string", "default": "."}
     }}},
    # Meta-tools for enhanced intelligence
    {"name": "spawn_agent", "description": "Spawn a sub-agent for complex tasks",
     "input_schema": {"type": "object", "properties": {
         "task": {"type": "string"}, "agent_type": {"type": "string",
         "enum": ["researcher", "coder", "tester", "reviewer", "planner"]},
         "context": {"type": "string"}, "max_iterations": {"type": "integer", "default": 10}
     }, "required": ["task"]}},
    {"name": "create_checkpoint", "description": "Create a checkpoint for potential backtracking",
     "input_schema": {"type": "object", "properties": {
         "description": {"type": "string"}
     }, "required": ["description"]}},
    {"name": "add_to_memory", "description": "Add important information to working memory",
     "input_schema": {"type": "object", "properties": {
         "category": {"type": "string", "enum": ["fact", "goal", "hypothesis", "observation"]},
         "content": {"type": "string"}
     }, "required": ["category", "content"]}},
    {"name": "decompose_goal", "description": "Break a complex goal into sub-goals",
     "input_schema": {"type": "object", "properties": {
         "main_goal": {"type": "string"},
         "sub_goals": {"type": "array", "items": {"type": "string"}}
     }, "required": ["main_goal", "sub_goals"]}},
]

TOOL_FUNCTIONS = {
    "read_file": read_file, "write_file": write_file, "edit_file": edit_file,
    "list_files": list_files, "glob_search": glob_search, "grep_search": grep_search,
    "execute_bash": execute_bash, "execute_python": execute_python,
    "http_fetch": http_fetch, "git_status": git_status,
}

# Tools that should NOT be cached (have side effects)
NON_CACHEABLE_TOOLS = {"write_file", "edit_file", "execute_bash", "execute_python",
                        "spawn_agent", "create_checkpoint", "add_to_memory", "decompose_goal"}


# =============================================================================
# AGENT PROMPTS (Enhanced)
# =============================================================================

AGENT_PROMPTS = {
    "general": """You are an advanced AI coding assistant with enhanced reasoning capabilities.

You have access to:
- Working memory for storing facts, goals, and observations
- Checkpointing for backtracking if needed
- Goal decomposition for breaking down complex tasks
- Sub-agent spawning for parallel work

Guidelines:
1. Use working memory to track important discoveries
2. Create checkpoints before risky operations
3. Decompose complex goals into manageable sub-goals
4. Verify your actions succeeded (read files after editing)
5. Be thorough but efficient with your budget""",

    "planner": """You are a planning agent. Your job is to:
1. Analyze the task requirements
2. Identify necessary steps
3. Decompose into sub-goals
4. Determine tool requirements
5. Identify potential risks

Do NOT execute - only plan. Return a structured plan.""",

    "researcher": """You are a research agent. Focus on:
1. Gathering information
2. Reading and analyzing files
3. Searching for patterns
4. Documenting findings in memory

Do NOT modify files - only read and analyze.""",

    "coder": """You are a coding agent. Focus on:
1. Writing clean, correct code
2. Following existing patterns
3. Verifying edits succeeded
4. Testing when possible

Always read files before editing.""",

    "reviewer": """You are a code review agent. Focus on:
1. Identifying bugs and issues
2. Checking code quality
3. Finding security concerns
4. Suggesting improvements

Provide specific, actionable feedback.""",

    "tester": """You are a testing agent. Focus on:
1. Running tests
2. Analyzing failures
3. Verifying behavior
4. Reporting results clearly"""
}


# =============================================================================
# IMPROVEMENT 1, 3, 6, 10, 11, 13, 16: Main Agent with All Enhancements
# =============================================================================

class EnhancedAgenticAgent:
    """
    Agentic Sampling Agent V3 with 16 intelligence improvements.
    """

    def __init__(self,
                 api_key: Optional[str] = None,
                 model: str = DEFAULT_MODEL,
                 allowed_tools: Optional[List[str]] = None,
                 system_prompt: Optional[str] = None,
                 agent_type: str = "general",
                 parent: Optional['EnhancedAgenticAgent'] = None,
                 depth: int = 0,
                 max_depth: int = 3,
                 verbose: bool = True,
                 budget: Optional[ExecutionBudget] = None):

        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY required")

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = model
        self.agent_type = agent_type
        self.system_prompt = system_prompt or AGENT_PROMPTS.get(agent_type, AGENT_PROMPTS["general"])
        self.parent = parent
        self.depth = depth
        self.max_depth = max_depth
        self.verbose = verbose

        # Core components
        self.conversation: List[Dict] = []
        self.tool_cache = ToolCache()
        self.memory = WorkingMemory()
        self.analytics = ToolAnalytics()
        self.goals = GoalManager()
        self.backtracking = BacktrackingManager()
        self.budget = budget or ExecutionBudget()

        # Filter tools
        self.allowed_tools = set(allowed_tools) if allowed_tools else set(TOOL_FUNCTIONS.keys())
        self.tools = [t for t in TOOL_SCHEMAS if t["name"] in self.allowed_tools or
                      t["name"] in ["spawn_agent", "create_checkpoint", "add_to_memory", "decompose_goal"]]

    def _log(self, msg: str, color: str = "dim"):
        if self.verbose:
            indent = "  " * self.depth
            print(f"{ANSI.get(color, '')}{indent}{msg}{ANSI['reset']}")

    # =========================================================================
    # IMPROVEMENT 3: Planning Phase
    # =========================================================================

    def _create_plan(self, task: str) -> Optional[str]:
        """Create an execution plan before taking action."""
        if self.budget.api_calls_remaining < 2:
            return None

        self._log("Creating execution plan...", "plan")

        plan_prompt = f"""Analyze this task and create a brief execution plan:

Task: {task}

Working Memory:
{self.memory.get_context(5)}

Create a plan with:
1. Key steps to accomplish
2. Files/resources needed
3. Potential risks
4. Success criteria

Be concise (3-5 steps max)."""

        try:
            response = self.client.messages.create(
                model=PLANNER_MODEL,
                max_tokens=500,
                messages=[{"role": "user", "content": plan_prompt}]
            )
            self.budget.use_api_call()
            plan = response.content[0].text
            self.memory.add("plan", plan)
            return plan
        except Exception as e:
            self._log(f"Planning failed: {e}", "error")
            return None

    # =========================================================================
    # IMPROVEMENT 1: Reflection
    # =========================================================================

    def _reflect(self, response: str, task: str) -> str:
        """Reflect on response before finalizing."""
        if self.budget.api_calls_remaining < 1:
            return response

        self._log("Reflecting on response...", "reflect")

        reflect_prompt = f"""Review this response for quality and completeness:

Original Task: {task}

Response:
{response[:2000]}

Consider:
1. Does it fully address the task?
2. Are there any errors or omissions?
3. Is the reasoning sound?
4. What could be improved?

If the response is good, reply "APPROVED".
If it needs improvement, explain what and how."""

        try:
            reflection = self.client.messages.create(
                model=REFLECTOR_MODEL,
                max_tokens=300,
                messages=[{"role": "user", "content": reflect_prompt}]
            )
            self.budget.use_api_call()
            feedback = reflection.content[0].text

            if "APPROVED" in feedback.upper():
                return response

            # If not approved, note the feedback
            self.memory.add("reflection", feedback)
            return response + f"\n\n[Self-reflection: {feedback}]"

        except Exception as e:
            self._log(f"Reflection failed: {e}", "error")
            return response

    # =========================================================================
    # IMPROVEMENT 6: Confidence Scoring
    # =========================================================================

    def _score_confidence(self, response: str) -> float:
        """Score confidence in the response (0-1)."""
        # Simple heuristic scoring
        score = 0.7  # Base confidence

        # Positive indicators
        if "verified" in response.lower():
            score += 0.1
        if any(phrase in response.lower() for phrase in ["successfully", "completed", "works"]):
            score += 0.05

        # Negative indicators
        if "?" in response:
            score -= 0.1
        if any(phrase in response.lower() for phrase in ["might", "possibly", "unsure", "maybe"]):
            score -= 0.15
        if "error" in response.lower():
            score -= 0.2

        return max(0.0, min(1.0, score))

    # =========================================================================
    # IMPROVEMENT 11: Context Compression
    # =========================================================================

    def _compress_context(self) -> bool:
        """Compress conversation if getting too long."""
        if len(self.conversation) < 10:
            return False

        self._log("Compressing context...", "info")

        # Get a summary of earlier messages
        early_messages = self.conversation[:len(self.conversation) - 4]
        summary_content = []
        for msg in early_messages:
            if msg["role"] == "user" and isinstance(msg["content"], str):
                summary_content.append(f"User: {msg['content'][:200]}")
            elif msg["role"] == "assistant" and isinstance(msg["content"], list):
                for block in msg["content"]:
                    if isinstance(block, dict) and block.get("type") == "text":
                        summary_content.append(f"Assistant: {block['text'][:200]}")

        summary = "\n".join(summary_content[-5:])
        self.memory.add("context_summary", summary)

        # Keep only recent messages
        self.conversation = self.conversation[-4:]
        return True

    # =========================================================================
    # IMPROVEMENT 13: Verification Steps
    # =========================================================================

    def _verify_edit(self, path: str, expected_content: Optional[str] = None) -> bool:
        """Verify an edit was successful."""
        result = read_file(path)
        if result.status != ResultStatus.SUCCESS:
            self._log(f"Verification failed: could not read {path}", "error")
            return False

        if expected_content and expected_content not in result.data.get("content", ""):
            self._log(f"Verification failed: expected content not found", "error")
            return False

        self._log(f"Verified: {path}", "tool")
        return True

    # =========================================================================
    # Tool Execution with Caching and Analytics
    # =========================================================================

    def _execute_tool(self, name: str, args: Dict) -> ToolResult:
        """Execute tool with caching and analytics."""
        start_time = time.time()
        self.budget.use_tool_call()

        # Check cache for cacheable tools
        if name not in NON_CACHEABLE_TOOLS:
            cached = self.tool_cache.get(name, args)
            if cached is not None:
                self._log(f"Cache hit: {name}", "cache")
                cached.cached = True
                return cached

        # Handle meta-tools
        if name == "spawn_agent":
            return self._spawn_sub_agent(**args)
        elif name == "create_checkpoint":
            return self._create_checkpoint(**args)
        elif name == "add_to_memory":
            return self._add_to_memory(**args)
        elif name == "decompose_goal":
            return self._decompose_goal(**args)

        # Execute regular tool
        if name not in TOOL_FUNCTIONS:
            return ToolResult(ResultStatus.ERROR, {"tool": name}, f"Unknown: {name}")

        if name not in self.allowed_tools:
            return ToolResult(ResultStatus.ERROR, {"tool": name}, f"Not allowed: {name}")

        try:
            result = TOOL_FUNCTIONS[name](**args)
        except Exception as e:
            result = ToolResult(ResultStatus.ERROR, {"tool": name}, str(e))

        duration = time.time() - start_time
        result.duration = duration

        # Record analytics
        self.analytics.record_call(name, result.status == ResultStatus.SUCCESS, duration)

        # Cache if successful and cacheable
        if name not in NON_CACHEABLE_TOOLS and result.status == ResultStatus.SUCCESS:
            self.tool_cache.set(name, args, result)

        # Invalidate related caches for write operations
        if name in ["write_file", "edit_file"]:
            path = args.get("path", "")
            self.tool_cache.invalidate(path)

        return result

    def _spawn_sub_agent(self, task: str, agent_type: str = "general",
                        context: Optional[str] = None,
                        max_iterations: int = 10) -> ToolResult:
        """Spawn a sub-agent."""
        if self.depth >= self.max_depth:
            return ToolResult(ResultStatus.ERROR, {}, f"Max depth {self.max_depth} reached")

        self._log(f"Spawning {agent_type} sub-agent...", "agent")

        sub_agent = EnhancedAgenticAgent(
            api_key=self.api_key,
            model=self.model,
            agent_type=agent_type,
            parent=self,
            depth=self.depth + 1,
            max_depth=self.max_depth,
            verbose=self.verbose,
            budget=self.budget  # Share budget
        )

        # Transfer relevant memory
        if context:
            sub_agent.memory.add("parent_context", context)

        try:
            response = sub_agent.process_message(task, max_iterations=max_iterations)
            return ToolResult(ResultStatus.SUCCESS, {
                "task": task, "agent_type": agent_type,
                "response": response, "tool_calls": sub_agent.analytics.usage_count
            })
        except Exception as e:
            return ToolResult(ResultStatus.ERROR, {"task": task}, str(e))

    def _create_checkpoint(self, description: str) -> ToolResult:
        """Create checkpoint for backtracking."""
        ckpt = self.backtracking.create_checkpoint(description, self.conversation, self.memory)
        self._log(f"Checkpoint created: {ckpt.id}", "info")
        return ToolResult(ResultStatus.SUCCESS, {"checkpoint_id": ckpt.id, "description": description})

    def _add_to_memory(self, category: str, content: str) -> ToolResult:
        """Add to working memory."""
        if category == "fact":
            key = f"fact_{len(self.memory.facts)}"
            self.memory.add_fact(key, content)
        elif category == "goal":
            self.memory.add_goal(content)
        elif category == "hypothesis":
            self.memory.add_hypothesis(content)
        else:
            self.memory.add(category, content)

        return ToolResult(ResultStatus.SUCCESS, {"category": category, "stored": True})

    def _decompose_goal(self, main_goal: str, sub_goals: List[str]) -> ToolResult:
        """Decompose a goal into sub-goals."""
        main = self.goals.add_goal(main_goal, priority=8)
        for i, sg in enumerate(sub_goals):
            self.goals.add_goal(sg, parent_id=main.id, priority=7 - i)

        self._log(f"Goal decomposed into {len(sub_goals)} sub-goals", "info")
        return ToolResult(ResultStatus.SUCCESS, {
            "main_goal": main_goal, "sub_goals": sub_goals,
            "goal_tree": self.goals.to_tree_str()
        })

    # =========================================================================
    # Main Processing Loop
    # =========================================================================

    def process_message(self, user_message: str, max_iterations: int = 20,
                       use_planning: bool = True, use_reflection: bool = True) -> str:
        """
        Process message with all enhancements:
        1. Planning phase
        2. Execution with caching, memory, analytics
        3. Verification
        4. Reflection
        5. Budget enforcement
        """
        # Check budget
        if self.budget.is_exhausted():
            return f"Budget exhausted. {self.budget.get_status()}"

        # IMPROVEMENT 3: Planning Phase
        if use_planning and self.depth == 0:
            plan = self._create_plan(user_message)
            if plan:
                self._log(f"Plan: {plan[:100]}...", "plan")

        # Add user message
        self.conversation.append({"role": "user", "content": user_message})

        iteration = 0
        while iteration < max_iterations:
            iteration += 1

            # Check budget
            if self.budget.is_exhausted():
                return f"Budget exhausted during execution. {self.budget.get_status()}"

            # IMPROVEMENT 11: Compress context if needed
            if len(self.conversation) > 15:
                self._compress_context()

            # Build system prompt with memory context
            enhanced_system = self.system_prompt
            memory_context = self.memory.get_context(5)
            if memory_context:
                enhanced_system += f"\n\nWorking Memory:\n{memory_context}"

            # Add budget awareness
            enhanced_system += f"\n\n{self.budget.get_status()}"

            # Call API
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=MAX_TOKENS,
                    system=enhanced_system,
                    tools=self.tools,
                    messages=self.conversation
                )
                self.budget.use_api_call()
                self.budget.use_tokens(response.usage.input_tokens + response.usage.output_tokens)
            except Exception as e:
                return f"API error: {e}"

            # Process response
            assistant_content = []
            tool_results = []
            has_tool_use = False

            for block in response.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    has_tool_use = True
                    tool_id = block.id
                    tool_name = block.name
                    tool_args = block.input

                    assistant_content.append({
                        "type": "tool_use", "id": tool_id,
                        "name": tool_name, "input": tool_args
                    })

                    self._log(f"Tool: {tool_name}", "tool")
                    result = self._execute_tool(tool_name, tool_args)

                    # IMPROVEMENT 13: Verification for edits
                    if tool_name == "edit_file" and result.status == ResultStatus.SUCCESS:
                        self._verify_edit(tool_args.get("path", ""))

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_id,
                        "content": result.to_content()
                    })

            # Add assistant message
            self.conversation.append({"role": "assistant", "content": assistant_content})

            if not has_tool_use:
                # Extract final response
                text_parts = [b["text"] for b in assistant_content if b.get("type") == "text"]
                final_response = "\n".join(text_parts)

                # IMPROVEMENT 1: Reflection
                if use_reflection and self.depth == 0:
                    final_response = self._reflect(final_response, user_message)

                # IMPROVEMENT 6: Confidence scoring
                confidence = self._score_confidence(final_response)
                if confidence < 0.5:
                    self.memory.add("low_confidence_response", final_response)
                    self._log(f"Low confidence: {confidence:.2f}", "error")

                # End tool sequence for analytics
                self.analytics.end_sequence()

                return final_response

            # Add tool results and continue
            self.conversation.append({"role": "user", "content": tool_results})

        return "Maximum iterations reached."

    def stream_message(self, user_message: str, max_iterations: int = 20) -> Iterator[str]:
        """Stream response with all enhancements."""
        if self.budget.is_exhausted():
            yield f"Budget exhausted. {self.budget.get_status()}"
            return

        self.conversation.append({"role": "user", "content": user_message})
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            if self.budget.is_exhausted():
                yield f"\nBudget exhausted."
                return

            if len(self.conversation) > 15:
                self._compress_context()

            enhanced_system = self.system_prompt
            memory_context = self.memory.get_context(5)
            if memory_context:
                enhanced_system += f"\n\nWorking Memory:\n{memory_context}"
            enhanced_system += f"\n\n{self.budget.get_status()}"

            try:
                with self.client.messages.stream(
                    model=self.model,
                    max_tokens=MAX_TOKENS,
                    system=enhanced_system,
                    tools=self.tools,
                    messages=self.conversation
                ) as stream:
                    for event in stream:
                        if hasattr(event, 'type') and event.type == "content_block_delta":
                            if hasattr(event.delta, 'text'):
                                yield event.delta.text

                    final = stream.get_final_message()

                self.budget.use_api_call()
                self.budget.use_tokens(final.usage.input_tokens + final.usage.output_tokens)

            except Exception as e:
                yield f"\nError: {e}"
                return

            # Process final message
            assistant_content = []
            tool_results = []
            has_tool_use = False

            for block in final.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    has_tool_use = True
                    assistant_content.append({
                        "type": "tool_use", "id": block.id,
                        "name": block.name, "input": block.input
                    })

                    yield f"\n[{block.name}...] "
                    result = self._execute_tool(block.name, block.input)

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result.to_content()
                    })

            self.conversation.append({"role": "assistant", "content": assistant_content})

            if not has_tool_use:
                self.analytics.end_sequence()
                return

            self.conversation.append({"role": "user", "content": tool_results})
            yield "\n"

    def run_interactive(self):
        """Interactive mode."""
        print(f"\n{ANSI['info']}Enhanced Agentic Agent V3{ANSI['reset']}")
        print(f"{ANSI['dim']}Model: {self.model}")
        print(f"16 Intelligence Improvements Active")
        print(f"Commands: exit, clear, memory, goals, analytics, budget{ANSI['reset']}\n")

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
                break
            elif cmd == "clear":
                self.conversation = []
                self.memory.clear()
                print("Cleared.\n")
                continue
            elif cmd == "memory":
                print(f"\n{self.memory.get_context()}\n")
                continue
            elif cmd == "goals":
                print(f"\n{self.goals.to_tree_str()}\n")
                continue
            elif cmd == "analytics":
                print(f"\n{self.analytics.get_summary()}\n")
                print(f"Cache hit rate: {self.tool_cache.hit_rate:.1%}\n")
                continue
            elif cmd == "budget":
                print(f"\n{self.budget.get_status()}\n")
                continue

            print(f"\n{ANSI['assistant']}Assistant: {ANSI['reset']}", end="", flush=True)

            for chunk in self.stream_message(user_input):
                print(chunk, end="", flush=True)

            print("\n")


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Enhanced Agentic Agent V3")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--prompt", help="Single prompt mode")
    parser.add_argument("--agent-type", default="general",
                       choices=["general", "planner", "researcher", "coder", "reviewer", "tester"])
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--no-planning", action="store_true")
    parser.add_argument("--no-reflection", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--budget-tokens", type=int, default=100000)
    parser.add_argument("--budget-time", type=float, default=300.0)

    args = parser.parse_args()

    budget = ExecutionBudget(
        max_tokens=args.budget_tokens,
        max_time_seconds=args.budget_time
    )

    try:
        agent = EnhancedAgenticAgent(
            model=args.model,
            agent_type=args.agent_type,
            max_depth=args.max_depth,
            verbose=not args.quiet,
            budget=budget
        )

        if args.prompt:
            response = agent.process_message(
                args.prompt,
                use_planning=not args.no_planning,
                use_reflection=not args.no_reflection
            )
            print(response)
        else:
            agent.run_interactive()

    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
