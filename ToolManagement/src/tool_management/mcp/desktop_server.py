"""
Desktop MCP Server - Local file access and project management.

Provides tools for:
- File operations (read, write, list, search)
- Project context management
- System information
- Long-running task coordination

This server runs locally and allows agents to work with
user's desktop files and manage long-running projects.
"""

from __future__ import annotations

import os
import json
import glob
import asyncio
import hashlib
import logging
import mimetypes
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ProjectContext:
    """Tracks context for a long-running project."""
    project_id: str
    name: str
    root_path: str
    description: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    files_accessed: List[str] = field(default_factory=list)
    notes: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "active"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "project_id": self.project_id,
            "name": self.name,
            "root_path": self.root_path,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "files_accessed": self.files_accessed[-50:],  # Last 50
            "notes": self.notes[-20:],  # Last 20
            "status": self.status,
            "metadata": self.metadata,
        }


class DesktopMCPServer:
    """
    MCP Server providing desktop/file system tools.

    Tools:
    - read_file: Read file contents
    - write_file: Write content to file
    - list_directory: List directory contents
    - search_files: Search for files by pattern
    - get_file_info: Get file metadata
    - run_command: Execute shell command
    - project_create: Create new project context
    - project_update: Update project notes/status
    - project_list: List active projects
    - system_info: Get system information
    """

    def __init__(
        self,
        allowed_paths: Optional[List[str]] = None,
        projects_dir: str = "~/.dsco/projects",
        max_file_size: int = 10 * 1024 * 1024,  # 10MB
    ):
        self.allowed_paths = allowed_paths or [os.path.expanduser("~")]
        self.projects_dir = Path(os.path.expanduser(projects_dir))
        self.projects_dir.mkdir(parents=True, exist_ok=True)
        self.max_file_size = max_file_size

        # In-memory project cache
        self._projects: Dict[str, ProjectContext] = {}
        self._load_projects()

    def _load_projects(self):
        """Load saved projects from disk."""
        projects_file = self.projects_dir / "projects.json"
        if projects_file.exists():
            try:
                with open(projects_file) as f:
                    data = json.load(f)
                    for p in data.get("projects", []):
                        ctx = ProjectContext(
                            project_id=p["project_id"],
                            name=p["name"],
                            root_path=p["root_path"],
                            description=p.get("description", ""),
                            created_at=datetime.fromisoformat(p["created_at"]),
                            updated_at=datetime.fromisoformat(p["updated_at"]),
                            files_accessed=p.get("files_accessed", []),
                            notes=p.get("notes", []),
                            status=p.get("status", "active"),
                            metadata=p.get("metadata", {}),
                        )
                        self._projects[ctx.project_id] = ctx
            except Exception as e:
                logger.warning(f"Failed to load projects: {e}")

    def _save_projects(self):
        """Save projects to disk."""
        projects_file = self.projects_dir / "projects.json"
        try:
            with open(projects_file, "w") as f:
                json.dump({
                    "projects": [p.to_dict() for p in self._projects.values()]
                }, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save projects: {e}")

    def _is_path_allowed(self, path: str) -> bool:
        """Check if path is within allowed directories."""
        abs_path = os.path.abspath(os.path.expanduser(path))
        for allowed in self.allowed_paths:
            allowed_abs = os.path.abspath(os.path.expanduser(allowed))
            if abs_path.startswith(allowed_abs):
                return True
        return False

    def get_tools(self) -> List[Dict[str, Any]]:
        """Return MCP tool definitions."""
        return [
            {
                "name": "read_file",
                "description": "Read contents of a file. Returns text content for text files, base64 for binary.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to file"},
                        "encoding": {"type": "string", "default": "utf-8"},
                        "max_lines": {"type": "integer", "description": "Max lines to read (0=all)"},
                    },
                    "required": ["path"],
                },
            },
            {
                "name": "write_file",
                "description": "Write content to a file. Creates parent directories if needed.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to file"},
                        "content": {"type": "string", "description": "Content to write"},
                        "append": {"type": "boolean", "default": False},
                        "create_dirs": {"type": "boolean", "default": True},
                    },
                    "required": ["path", "content"],
                },
            },
            {
                "name": "list_directory",
                "description": "List contents of a directory with file metadata.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Directory path"},
                        "pattern": {"type": "string", "description": "Glob pattern filter"},
                        "recursive": {"type": "boolean", "default": False},
                        "include_hidden": {"type": "boolean", "default": False},
                    },
                    "required": ["path"],
                },
            },
            {
                "name": "search_files",
                "description": "Search for files matching pattern. Supports glob patterns.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string", "description": "Glob pattern (e.g., **/*.py)"},
                        "root": {"type": "string", "description": "Root directory to search"},
                        "content_pattern": {"type": "string", "description": "Regex to match in file content"},
                        "max_results": {"type": "integer", "default": 100},
                    },
                    "required": ["pattern"],
                },
            },
            {
                "name": "get_file_info",
                "description": "Get detailed metadata about a file or directory.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "Path to file or directory"},
                    },
                    "required": ["path"],
                },
            },
            {
                "name": "run_command",
                "description": "Execute a shell command and return output. Use with caution.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "command": {"type": "string", "description": "Command to execute"},
                        "cwd": {"type": "string", "description": "Working directory"},
                        "timeout": {"type": "integer", "default": 30, "description": "Timeout in seconds"},
                        "capture_stderr": {"type": "boolean", "default": True},
                    },
                    "required": ["command"],
                },
            },
            {
                "name": "project_create",
                "description": "Create a new project context for tracking long-running work.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Project name"},
                        "root_path": {"type": "string", "description": "Project root directory"},
                        "description": {"type": "string", "description": "Project description"},
                        "metadata": {"type": "object", "description": "Additional metadata"},
                    },
                    "required": ["name", "root_path"],
                },
            },
            {
                "name": "project_update",
                "description": "Update project context with notes, files accessed, or status.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "project_id": {"type": "string", "description": "Project ID"},
                        "note": {"type": "string", "description": "Add a note"},
                        "files_accessed": {"type": "array", "items": {"type": "string"}},
                        "status": {"type": "string", "enum": ["active", "paused", "completed", "archived"]},
                        "metadata": {"type": "object", "description": "Update metadata"},
                    },
                    "required": ["project_id"],
                },
            },
            {
                "name": "project_list",
                "description": "List all projects or get details of a specific project.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "project_id": {"type": "string", "description": "Specific project ID (optional)"},
                        "status_filter": {"type": "string", "description": "Filter by status"},
                    },
                },
            },
            {
                "name": "system_info",
                "description": "Get system information including OS, user, environment.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "include_env": {"type": "boolean", "default": False, "description": "Include env vars"},
                    },
                },
            },
        ]

    async def call_tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a tool and return results."""
        try:
            if name == "read_file":
                return await self._read_file(**arguments)
            elif name == "write_file":
                return await self._write_file(**arguments)
            elif name == "list_directory":
                return await self._list_directory(**arguments)
            elif name == "search_files":
                return await self._search_files(**arguments)
            elif name == "get_file_info":
                return await self._get_file_info(**arguments)
            elif name == "run_command":
                return await self._run_command(**arguments)
            elif name == "project_create":
                return await self._project_create(**arguments)
            elif name == "project_update":
                return await self._project_update(**arguments)
            elif name == "project_list":
                return await self._project_list(**arguments)
            elif name == "system_info":
                return await self._system_info(**arguments)
            else:
                return {"error": f"Unknown tool: {name}"}
        except Exception as e:
            logger.error(f"Tool {name} failed: {e}")
            return {"error": str(e)}

    async def _read_file(
        self,
        path: str,
        encoding: str = "utf-8",
        max_lines: int = 0,
    ) -> Dict[str, Any]:
        """Read file contents."""
        path = os.path.expanduser(path)

        if not self._is_path_allowed(path):
            return {"error": f"Path not allowed: {path}"}

        if not os.path.exists(path):
            return {"error": f"File not found: {path}"}

        if os.path.isdir(path):
            return {"error": f"Path is a directory: {path}"}

        size = os.path.getsize(path)
        if size > self.max_file_size:
            return {"error": f"File too large: {size} bytes (max: {self.max_file_size})"}

        mime_type, _ = mimetypes.guess_type(path)
        is_binary = mime_type and not mime_type.startswith("text/")

        try:
            if is_binary:
                import base64
                with open(path, "rb") as f:
                    content = base64.b64encode(f.read()).decode("ascii")
                return {
                    "path": path,
                    "content": content,
                    "encoding": "base64",
                    "mime_type": mime_type,
                    "size": size,
                }
            else:
                with open(path, "r", encoding=encoding) as f:
                    if max_lines > 0:
                        lines = []
                        for i, line in enumerate(f):
                            if i >= max_lines:
                                break
                            lines.append(line)
                        content = "".join(lines)
                    else:
                        content = f.read()

                return {
                    "path": path,
                    "content": content,
                    "encoding": encoding,
                    "mime_type": mime_type or "text/plain",
                    "size": size,
                    "lines": content.count("\n") + 1,
                }
        except Exception as e:
            return {"error": f"Failed to read file: {e}"}

    async def _write_file(
        self,
        path: str,
        content: str,
        append: bool = False,
        create_dirs: bool = True,
    ) -> Dict[str, Any]:
        """Write content to file."""
        path = os.path.expanduser(path)

        if not self._is_path_allowed(path):
            return {"error": f"Path not allowed: {path}"}

        if create_dirs:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        mode = "a" if append else "w"
        try:
            with open(path, mode) as f:
                f.write(content)

            return {
                "path": path,
                "written": len(content),
                "append": append,
                "success": True,
            }
        except Exception as e:
            return {"error": f"Failed to write file: {e}"}

    async def _list_directory(
        self,
        path: str,
        pattern: Optional[str] = None,
        recursive: bool = False,
        include_hidden: bool = False,
    ) -> Dict[str, Any]:
        """List directory contents."""
        path = os.path.expanduser(path)

        if not self._is_path_allowed(path):
            return {"error": f"Path not allowed: {path}"}

        if not os.path.isdir(path):
            return {"error": f"Not a directory: {path}"}

        entries = []
        try:
            if recursive:
                search_pattern = os.path.join(path, "**", pattern or "*")
                matches = glob.glob(search_pattern, recursive=True)
            else:
                search_pattern = os.path.join(path, pattern or "*")
                matches = glob.glob(search_pattern)

            for match in matches[:500]:  # Limit results
                name = os.path.basename(match)
                if not include_hidden and name.startswith("."):
                    continue

                stat = os.stat(match)
                entries.append({
                    "name": name,
                    "path": match,
                    "is_dir": os.path.isdir(match),
                    "size": stat.st_size if not os.path.isdir(match) else None,
                    "modified": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
                })

            return {
                "path": path,
                "pattern": pattern,
                "entries": entries,
                "total": len(entries),
            }
        except Exception as e:
            return {"error": f"Failed to list directory: {e}"}

    async def _search_files(
        self,
        pattern: str,
        root: Optional[str] = None,
        content_pattern: Optional[str] = None,
        max_results: int = 100,
    ) -> Dict[str, Any]:
        """Search for files."""
        root = os.path.expanduser(root or "~")

        if not self._is_path_allowed(root):
            return {"error": f"Path not allowed: {root}"}

        results = []
        search_pattern = os.path.join(root, pattern)

        try:
            matches = glob.glob(search_pattern, recursive=True)

            for match in matches:
                if len(results) >= max_results:
                    break

                if os.path.isdir(match):
                    continue

                entry = {
                    "path": match,
                    "name": os.path.basename(match),
                    "size": os.path.getsize(match),
                }

                # Content search if pattern provided
                if content_pattern:
                    import re
                    try:
                        with open(match, "r", errors="ignore") as f:
                            content = f.read(100000)  # First 100KB
                            matches_content = re.findall(content_pattern, content, re.IGNORECASE)
                            if matches_content:
                                entry["content_matches"] = len(matches_content)
                                entry["preview"] = matches_content[0][:100]
                                results.append(entry)
                    except Exception:
                        pass
                else:
                    results.append(entry)

            return {
                "pattern": pattern,
                "root": root,
                "content_pattern": content_pattern,
                "results": results,
                "total": len(results),
            }
        except Exception as e:
            return {"error": f"Search failed: {e}"}

    async def _get_file_info(self, path: str) -> Dict[str, Any]:
        """Get file metadata."""
        path = os.path.expanduser(path)

        if not self._is_path_allowed(path):
            return {"error": f"Path not allowed: {path}"}

        if not os.path.exists(path):
            return {"error": f"Path not found: {path}"}

        stat = os.stat(path)
        mime_type, _ = mimetypes.guess_type(path)

        info = {
            "path": path,
            "name": os.path.basename(path),
            "is_dir": os.path.isdir(path),
            "is_file": os.path.isfile(path),
            "is_link": os.path.islink(path),
            "size": stat.st_size,
            "created": datetime.fromtimestamp(stat.st_ctime, timezone.utc).isoformat(),
            "modified": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "accessed": datetime.fromtimestamp(stat.st_atime, timezone.utc).isoformat(),
            "mime_type": mime_type,
            "permissions": oct(stat.st_mode)[-3:],
        }

        if os.path.isdir(path):
            try:
                entries = os.listdir(path)
                info["entry_count"] = len(entries)
            except PermissionError:
                info["entry_count"] = "permission denied"

        # Compute hash for files
        if os.path.isfile(path) and stat.st_size < 10 * 1024 * 1024:  # <10MB
            try:
                with open(path, "rb") as f:
                    info["sha256"] = hashlib.sha256(f.read()).hexdigest()[:16]
            except Exception:
                pass

        return info

    async def _run_command(
        self,
        command: str,
        cwd: Optional[str] = None,
        timeout: int = 30,
        capture_stderr: bool = True,
    ) -> Dict[str, Any]:
        """Execute shell command."""
        if cwd:
            cwd = os.path.expanduser(cwd)
            if not self._is_path_allowed(cwd):
                return {"error": f"Working directory not allowed: {cwd}"}

        try:
            result = subprocess.run(
                command,
                shell=True,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )

            return {
                "command": command,
                "cwd": cwd,
                "returncode": result.returncode,
                "stdout": result.stdout[:50000],  # Limit output
                "stderr": result.stderr[:10000] if capture_stderr else None,
                "success": result.returncode == 0,
            }
        except subprocess.TimeoutExpired:
            return {"error": f"Command timed out after {timeout}s", "command": command}
        except Exception as e:
            return {"error": f"Command failed: {e}"}

    async def _project_create(
        self,
        name: str,
        root_path: str,
        description: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create new project context."""
        root_path = os.path.expanduser(root_path)

        if not self._is_path_allowed(root_path):
            return {"error": f"Path not allowed: {root_path}"}

        project_id = hashlib.sha256(f"{name}:{root_path}:{datetime.now().isoformat()}".encode()).hexdigest()[:12]

        ctx = ProjectContext(
            project_id=project_id,
            name=name,
            root_path=root_path,
            description=description,
            metadata=metadata or {},
        )

        self._projects[project_id] = ctx
        self._save_projects()

        return {
            "success": True,
            "project": ctx.to_dict(),
        }

    async def _project_update(
        self,
        project_id: str,
        note: Optional[str] = None,
        files_accessed: Optional[List[str]] = None,
        status: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Update project context."""
        ctx = self._projects.get(project_id)
        if not ctx:
            return {"error": f"Project not found: {project_id}"}

        ctx.updated_at = datetime.now(timezone.utc)

        if note:
            ctx.notes.append({
                "text": note,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        if files_accessed:
            for f in files_accessed:
                if f not in ctx.files_accessed:
                    ctx.files_accessed.append(f)

        if status:
            ctx.status = status

        if metadata:
            ctx.metadata.update(metadata)

        self._save_projects()

        return {
            "success": True,
            "project": ctx.to_dict(),
        }

    async def _project_list(
        self,
        project_id: Optional[str] = None,
        status_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """List projects."""
        if project_id:
            ctx = self._projects.get(project_id)
            if not ctx:
                return {"error": f"Project not found: {project_id}"}
            return {"project": ctx.to_dict()}

        projects = list(self._projects.values())

        if status_filter:
            projects = [p for p in projects if p.status == status_filter]

        return {
            "projects": [p.to_dict() for p in projects],
            "total": len(projects),
        }

    async def _system_info(self, include_env: bool = False) -> Dict[str, Any]:
        """Get system information."""
        import platform

        info = {
            "platform": platform.system(),
            "platform_release": platform.release(),
            "platform_version": platform.version(),
            "architecture": platform.machine(),
            "hostname": platform.node(),
            "python_version": platform.python_version(),
            "user": os.getenv("USER") or os.getenv("USERNAME"),
            "home": os.path.expanduser("~"),
            "cwd": os.getcwd(),
            "pid": os.getpid(),
        }

        if include_env:
            # Filter sensitive env vars
            safe_env = {}
            sensitive = {"password", "secret", "token", "key", "credential"}
            for k, v in os.environ.items():
                if not any(s in k.lower() for s in sensitive):
                    safe_env[k] = v[:100]  # Truncate long values
            info["environment"] = safe_env

        return info


# Global instance
_desktop_server: Optional[DesktopMCPServer] = None

def get_desktop_server(
    allowed_paths: Optional[List[str]] = None,
) -> DesktopMCPServer:
    """Get or create singleton desktop server."""
    global _desktop_server
    if _desktop_server is None:
        _desktop_server = DesktopMCPServer(allowed_paths=allowed_paths)
    return _desktop_server
