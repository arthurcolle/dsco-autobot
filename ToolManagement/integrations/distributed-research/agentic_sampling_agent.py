#!/usr/bin/env python3
"""
Agentic Sampling Agent with Comprehensive Tool Categories

A production-ready coding agent implementing the core agentic sampling loop
with custom tools for: file I/O, network operations, code execution,
deployment, and search/navigation.

Inspired by "The Emperor Has No Clothes: How to Code Claude Code in 200 Lines"
but extended with full tool categories for real-world agentic workflows.
"""

import asyncio
import inspect
import json
import os
import re
import subprocess
import sys
import tempfile
import socket
import ssl
import urllib.request
import urllib.parse
import urllib.error
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, TypeVar, Generic
from datetime import datetime
import fnmatch
import hashlib
import shutil

try:
    import anthropic
except ImportError:
    print("Please install anthropic: pip install anthropic")
    sys.exit(1)

# =============================================================================
# Configuration & Constants
# =============================================================================

ANSI_COLORS = {
    "reset": "\u001b[0m",
    "user": "\u001b[94m",      # Blue
    "assistant": "\u001b[93m", # Yellow
    "tool": "\u001b[92m",      # Green
    "error": "\u001b[91m",     # Red
    "info": "\u001b[96m",      # Cyan
    "dim": "\u001b[90m",       # Gray
}

DEFAULT_MODEL = "claude-sonnet-5"
MAX_TOKENS = 8192
DEFAULT_TIMEOUT = 30

# =============================================================================
# Tool Result Types
# =============================================================================

class ToolResultStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"
    PARTIAL = "partial"
    REQUIRES_APPROVAL = "requires_approval"

@dataclass
class ToolResult:
    """Standardized result from any tool execution"""
    status: ToolResultStatus
    data: Dict[str, Any]
    message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status.value,
            "data": self.data,
            "message": self.message,
            "metadata": self.metadata
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)

# =============================================================================
# Path Utilities
# =============================================================================

def resolve_path(path_str: str, base_dir: Optional[Path] = None) -> Path:
    """
    Resolve a path string to an absolute path.
    Handles ~, relative paths, and validates the result.
    """
    path = Path(path_str).expanduser()
    if not path.is_absolute():
        base = base_dir or Path.cwd()
        path = (base / path).resolve()
    return path

def is_safe_path(path: Path, allowed_roots: Optional[List[Path]] = None) -> bool:
    """Check if a path is safe to access (no traversal attacks)"""
    resolved = path.resolve()
    if allowed_roots:
        return any(str(resolved).startswith(str(root.resolve())) for root in allowed_roots)
    return True

# =============================================================================
# TOOL CATEGORY 1: File Operations
# =============================================================================

def read_file(filename: str, encoding: str = "utf-8",
              start_line: Optional[int] = None,
              end_line: Optional[int] = None) -> ToolResult:
    """
    Read the contents of a file from the filesystem.

    :param filename: Path to the file to read (absolute or relative)
    :param encoding: Character encoding to use (default: utf-8)
    :param start_line: Optional starting line number (1-indexed)
    :param end_line: Optional ending line number (1-indexed, inclusive)
    :return: File contents with metadata
    """
    try:
        full_path = resolve_path(filename)

        if not full_path.exists():
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"path": str(full_path)},
                message=f"File not found: {full_path}"
            )

        if not full_path.is_file():
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"path": str(full_path)},
                message=f"Path is not a file: {full_path}"
            )

        content = full_path.read_text(encoding=encoding)
        lines = content.splitlines(keepends=True)
        total_lines = len(lines)

        # Handle line range if specified
        if start_line is not None or end_line is not None:
            start_idx = (start_line - 1) if start_line else 0
            end_idx = end_line if end_line else total_lines
            lines = lines[start_idx:end_idx]
            content = "".join(lines)

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "path": str(full_path),
                "content": content,
                "size_bytes": full_path.stat().st_size,
                "total_lines": total_lines,
                "lines_returned": len(lines)
            },
            metadata={
                "encoding": encoding,
                "modified": datetime.fromtimestamp(full_path.stat().st_mtime).isoformat()
            }
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"path": filename},
            message=f"Error reading file: {str(e)}"
        )

def write_file(path: str, content: str,
               encoding: str = "utf-8",
               create_dirs: bool = True,
               backup: bool = False) -> ToolResult:
    """
    Write content to a file, creating it if it doesn't exist.

    :param path: Path where the file should be written
    :param content: The content to write to the file
    :param encoding: Character encoding to use (default: utf-8)
    :param create_dirs: Create parent directories if they don't exist
    :param backup: Create a backup of existing file before overwriting
    :return: Result with path and action taken
    """
    try:
        full_path = resolve_path(path)
        action = "created" if not full_path.exists() else "overwritten"

        if create_dirs:
            full_path.parent.mkdir(parents=True, exist_ok=True)

        if backup and full_path.exists():
            backup_path = full_path.with_suffix(full_path.suffix + ".bak")
            shutil.copy2(full_path, backup_path)
            action = "overwritten_with_backup"

        full_path.write_text(content, encoding=encoding)

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "path": str(full_path),
                "action": action,
                "size_bytes": len(content.encode(encoding)),
                "lines": len(content.splitlines())
            }
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"path": path},
            message=f"Error writing file: {str(e)}"
        )

def edit_file(path: str, old_str: str, new_str: str,
              replace_all: bool = False) -> ToolResult:
    """
    Edit a file by replacing text. If old_str is empty, creates/overwrites the file.

    :param path: Path to the file to edit
    :param old_str: The text to find and replace (empty string = create new file)
    :param new_str: The text to replace with
    :param replace_all: Replace all occurrences (default: first only)
    :return: Result with edit details
    """
    try:
        full_path = resolve_path(path)

        # Empty old_str means create/overwrite file
        if old_str == "":
            return write_file(path, new_str)

        if not full_path.exists():
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"path": str(full_path)},
                message=f"File not found: {full_path}"
            )

        original = full_path.read_text(encoding="utf-8")

        if old_str not in original:
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={
                    "path": str(full_path),
                    "search_string": old_str[:100] + "..." if len(old_str) > 100 else old_str
                },
                message="Search string not found in file"
            )

        if replace_all:
            edited = original.replace(old_str, new_str)
            count = original.count(old_str)
        else:
            edited = original.replace(old_str, new_str, 1)
            count = 1

        full_path.write_text(edited, encoding="utf-8")

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "path": str(full_path),
                "action": "edited",
                "replacements": count,
                "size_delta": len(edited) - len(original)
            }
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"path": path},
            message=f"Error editing file: {str(e)}"
        )

def delete_file(path: str, force: bool = False) -> ToolResult:
    """
    Delete a file from the filesystem.

    :param path: Path to the file to delete
    :param force: If True, don't error if file doesn't exist
    :return: Result of deletion
    """
    try:
        full_path = resolve_path(path)

        if not full_path.exists():
            if force:
                return ToolResult(
                    status=ToolResultStatus.SUCCESS,
                    data={"path": str(full_path), "action": "not_found"}
                )
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"path": str(full_path)},
                message="File not found"
            )

        full_path.unlink()

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={"path": str(full_path), "action": "deleted"}
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"path": path},
            message=f"Error deleting file: {str(e)}"
        )

def list_files(path: str = ".",
               pattern: Optional[str] = None,
               recursive: bool = False,
               include_hidden: bool = False) -> ToolResult:
    """
    List files and directories at the given path.

    :param path: Directory path to list (default: current directory)
    :param pattern: Glob pattern to filter results (e.g., "*.py")
    :param recursive: Include subdirectories recursively
    :param include_hidden: Include hidden files (starting with .)
    :return: List of files with their types and sizes
    """
    try:
        full_path = resolve_path(path)

        if not full_path.exists():
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"path": str(full_path)},
                message="Directory not found"
            )

        if not full_path.is_dir():
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"path": str(full_path)},
                message="Path is not a directory"
            )

        files = []

        if recursive:
            iterator = full_path.rglob(pattern or "*")
        else:
            iterator = full_path.glob(pattern or "*")

        for item in iterator:
            if not include_hidden and item.name.startswith("."):
                continue

            try:
                stat = item.stat()
                files.append({
                    "name": item.name,
                    "path": str(item.relative_to(full_path)),
                    "type": "file" if item.is_file() else "directory",
                    "size": stat.st_size if item.is_file() else None,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
                })
            except (PermissionError, OSError):
                continue

        # Sort: directories first, then by name
        files.sort(key=lambda x: (x["type"] == "file", x["name"].lower()))

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "path": str(full_path),
                "files": files,
                "count": len(files)
            }
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"path": path},
            message=f"Error listing files: {str(e)}"
        )

def copy_file(source: str, destination: str,
              overwrite: bool = False) -> ToolResult:
    """
    Copy a file to a new location.

    :param source: Source file path
    :param destination: Destination file path
    :param overwrite: Overwrite destination if it exists
    :return: Result of copy operation
    """
    try:
        src_path = resolve_path(source)
        dst_path = resolve_path(destination)

        if not src_path.exists():
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"source": str(src_path)},
                message="Source file not found"
            )

        if dst_path.exists() and not overwrite:
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"destination": str(dst_path)},
                message="Destination already exists (use overwrite=True)"
            )

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src_path, dst_path)

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "source": str(src_path),
                "destination": str(dst_path),
                "size_bytes": dst_path.stat().st_size
            }
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"source": source, "destination": destination},
            message=f"Error copying file: {str(e)}"
        )

def move_file(source: str, destination: str,
              overwrite: bool = False) -> ToolResult:
    """
    Move a file to a new location.

    :param source: Source file path
    :param destination: Destination file path
    :param overwrite: Overwrite destination if it exists
    :return: Result of move operation
    """
    try:
        src_path = resolve_path(source)
        dst_path = resolve_path(destination)

        if not src_path.exists():
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"source": str(src_path)},
                message="Source file not found"
            )

        if dst_path.exists() and not overwrite:
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"destination": str(dst_path)},
                message="Destination already exists (use overwrite=True)"
            )

        dst_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src_path), str(dst_path))

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "source": str(src_path),
                "destination": str(dst_path)
            }
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"source": source, "destination": destination},
            message=f"Error moving file: {str(e)}"
        )

# =============================================================================
# TOOL CATEGORY 2: Search & Navigation
# =============================================================================

def glob_search(pattern: str, path: str = ".",
                max_results: int = 100) -> ToolResult:
    """
    Find files matching a glob pattern.

    :param pattern: Glob pattern (e.g., "**/*.py", "src/*.ts")
    :param path: Base directory to search from
    :param max_results: Maximum number of results to return
    :return: List of matching file paths
    """
    try:
        base_path = resolve_path(path)

        if not base_path.exists():
            return ToolResult(
                status=ToolResultStatus.ERROR,
                data={"path": str(base_path)},
                message="Base directory not found"
            )

        matches = []
        for match in base_path.glob(pattern):
            if len(matches) >= max_results:
                break
            try:
                rel_path = match.relative_to(base_path)
                matches.append({
                    "path": str(rel_path),
                    "absolute": str(match),
                    "type": "file" if match.is_file() else "directory",
                    "size": match.stat().st_size if match.is_file() else None
                })
            except (PermissionError, OSError):
                continue

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "pattern": pattern,
                "base_path": str(base_path),
                "matches": matches,
                "count": len(matches),
                "truncated": len(list(base_path.glob(pattern))) > max_results
            }
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"pattern": pattern, "path": path},
            message=f"Error in glob search: {str(e)}"
        )

def grep_search(pattern: str, path: str = ".",
                file_pattern: Optional[str] = None,
                context_lines: int = 0,
                case_sensitive: bool = True,
                max_results: int = 100) -> ToolResult:
    """
    Search file contents for a regex pattern.

    :param pattern: Regular expression pattern to search for
    :param path: Directory or file to search
    :param file_pattern: Glob pattern for files to search (e.g., "*.py")
    :param context_lines: Number of context lines before/after match
    :param case_sensitive: Whether search is case sensitive
    :param max_results: Maximum number of matches to return
    :return: List of matches with file, line number, and content
    """
    try:
        base_path = resolve_path(path)
        flags = 0 if case_sensitive else re.IGNORECASE
        regex = re.compile(pattern, flags)

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
                        context_before = lines[max(0, i-context_lines):i]
                        context_after = lines[i+1:i+1+context_lines]

                        matches.append({
                            "file": str(file_path.relative_to(base_path) if base_path.is_dir() else file_path.name),
                            "line_number": i + 1,
                            "line": line,
                            "context_before": context_before if context_lines > 0 else None,
                            "context_after": context_after if context_lines > 0 else None
                        })
            except (PermissionError, OSError, UnicodeDecodeError):
                pass

        if base_path.is_file():
            search_file(base_path)
        else:
            glob_pattern = file_pattern or "**/*"
            for file_path in base_path.glob(glob_pattern):
                if file_path.is_file() and len(matches) < max_results:
                    search_file(file_path)

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "pattern": pattern,
                "path": str(base_path),
                "matches": matches,
                "count": len(matches),
                "files_searched": files_searched
            }
        )
    except re.error as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"pattern": pattern},
            message=f"Invalid regex pattern: {str(e)}"
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"pattern": pattern, "path": path},
            message=f"Error in grep search: {str(e)}"
        )

# =============================================================================
# TOOL CATEGORY 3: Code Execution
# =============================================================================

def execute_bash(command: str,
                 working_dir: Optional[str] = None,
                 timeout: int = DEFAULT_TIMEOUT,
                 env: Optional[Dict[str, str]] = None) -> ToolResult:
    """
    Execute a bash command and return the output.

    :param command: The bash command to execute
    :param working_dir: Working directory for command execution
    :param timeout: Timeout in seconds (default: 30)
    :param env: Additional environment variables
    :return: Command output with exit code
    """
    try:
        cwd = resolve_path(working_dir) if working_dir else Path.cwd()

        # Merge environment
        cmd_env = os.environ.copy()
        if env:
            cmd_env.update(env)

        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(cwd),
            env=cmd_env
        )

        return ToolResult(
            status=ToolResultStatus.SUCCESS if result.returncode == 0 else ToolResultStatus.ERROR,
            data={
                "command": command,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "exit_code": result.returncode,
                "working_dir": str(cwd)
            }
        )
    except subprocess.TimeoutExpired:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"command": command},
            message=f"Command timed out after {timeout} seconds"
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"command": command},
            message=f"Error executing command: {str(e)}"
        )

def execute_python(code: str,
                   timeout: int = DEFAULT_TIMEOUT,
                   capture_vars: Optional[List[str]] = None) -> ToolResult:
    """
    Execute Python code in a subprocess and return the output.

    :param code: Python code to execute
    :param timeout: Timeout in seconds (default: 30)
    :param capture_vars: Variable names to capture and return
    :return: Code output and any captured variables
    """
    try:
        # Create a temporary file for the code
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            # Add capture logic if needed
            if capture_vars:
                capture_code = f"""
import json
_captured = {{}}
{code}
for _var in {capture_vars}:
    if _var in dir():
        _captured[_var] = eval(_var)
print("__CAPTURED__:" + json.dumps(_captured, default=str))
"""
                f.write(capture_code)
            else:
                f.write(code)
            temp_path = f.name

        try:
            result = subprocess.run(
                [sys.executable, temp_path],
                capture_output=True,
                text=True,
                timeout=timeout
            )

            stdout = result.stdout
            captured = {}

            # Extract captured variables
            if capture_vars and "__CAPTURED__:" in stdout:
                lines = stdout.split("\n")
                for line in lines:
                    if line.startswith("__CAPTURED__:"):
                        captured = json.loads(line[13:])
                        stdout = "\n".join(l for l in lines if not l.startswith("__CAPTURED__:"))
                        break

            return ToolResult(
                status=ToolResultStatus.SUCCESS if result.returncode == 0 else ToolResultStatus.ERROR,
                data={
                    "stdout": stdout,
                    "stderr": result.stderr,
                    "exit_code": result.returncode,
                    "captured_variables": captured if capture_vars else None
                }
            )
        finally:
            os.unlink(temp_path)

    except subprocess.TimeoutExpired:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"code": code[:200] + "..." if len(code) > 200 else code},
            message=f"Code execution timed out after {timeout} seconds"
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"code": code[:200] + "..." if len(code) > 200 else code},
            message=f"Error executing code: {str(e)}"
        )

# =============================================================================
# TOOL CATEGORY 4: Network Operations
# =============================================================================

def http_fetch(url: str,
               method: str = "GET",
               headers: Optional[Dict[str, str]] = None,
               body: Optional[str] = None,
               timeout: int = DEFAULT_TIMEOUT) -> ToolResult:
    """
    Fetch content from a URL using HTTP/HTTPS.

    :param url: The URL to fetch
    :param method: HTTP method (GET, POST, PUT, DELETE, etc.)
    :param headers: Optional headers to include
    :param body: Optional request body for POST/PUT
    :param timeout: Timeout in seconds
    :return: Response with status code, headers, and body
    """
    try:
        req = urllib.request.Request(url, method=method)

        # Add default headers
        req.add_header("User-Agent", "AgenticSamplingAgent/1.0")

        if headers:
            for key, value in headers.items():
                req.add_header(key, value)

        if body:
            req.data = body.encode("utf-8")

        context = ssl.create_default_context()

        with urllib.request.urlopen(req, timeout=timeout, context=context) as response:
            content = response.read()
            try:
                decoded_content = content.decode("utf-8")
            except UnicodeDecodeError:
                decoded_content = content.decode("latin-1")

            return ToolResult(
                status=ToolResultStatus.SUCCESS,
                data={
                    "url": url,
                    "status_code": response.status,
                    "headers": dict(response.headers),
                    "content": decoded_content[:50000],  # Limit content size
                    "content_length": len(content),
                    "truncated": len(content) > 50000
                }
            )
    except urllib.error.HTTPError as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={
                "url": url,
                "status_code": e.code,
                "reason": e.reason
            },
            message=f"HTTP Error {e.code}: {e.reason}"
        )
    except urllib.error.URLError as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"url": url},
            message=f"URL Error: {str(e.reason)}"
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"url": url},
            message=f"Error fetching URL: {str(e)}"
        )

def tcp_connect(host: str, port: int,
                send_data: Optional[str] = None,
                timeout: int = 10,
                use_ssl: bool = False) -> ToolResult:
    """
    Open a TCP connection and optionally send/receive data.

    :param host: Hostname or IP address
    :param port: Port number
    :param send_data: Optional data to send
    :param timeout: Connection timeout in seconds
    :param use_ssl: Whether to use SSL/TLS
    :return: Connection result and any received data
    """
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

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "host": host,
                "port": port,
                "connected": True,
                "ssl": use_ssl,
                "sent_bytes": len(send_data) if send_data else 0,
                "received": received
            }
        )
    except socket.timeout:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"host": host, "port": port},
            message="Connection timed out"
        )
    except ConnectionRefusedError:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"host": host, "port": port},
            message="Connection refused"
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"host": host, "port": port},
            message=f"Connection error: {str(e)}"
        )

def dns_lookup(hostname: str,
               record_type: str = "A") -> ToolResult:
    """
    Perform a DNS lookup for a hostname.

    :param hostname: The hostname to look up
    :param record_type: Type of DNS record (A, AAAA, etc.)
    :return: DNS resolution results
    """
    try:
        if record_type == "A":
            result = socket.gethostbyname(hostname)
            addresses = [result]
        else:
            result = socket.getaddrinfo(hostname, None)
            addresses = list(set(r[4][0] for r in result))

        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            data={
                "hostname": hostname,
                "record_type": record_type,
                "addresses": addresses
            }
        )
    except socket.gaierror as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"hostname": hostname},
            message=f"DNS lookup failed: {str(e)}"
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"hostname": hostname},
            message=f"Error: {str(e)}"
        )

# =============================================================================
# TOOL CATEGORY 5: Git Operations
# =============================================================================

def git_status(repo_path: str = ".") -> ToolResult:
    """
    Get the git status of a repository.

    :param repo_path: Path to the git repository
    :return: Git status information
    """
    result = execute_bash("git status --porcelain", working_dir=repo_path)
    if result.status == ToolResultStatus.SUCCESS:
        lines = result.data["stdout"].strip().split("\n") if result.data["stdout"].strip() else []
        files = []
        for line in lines:
            if len(line) >= 3:
                status = line[:2].strip()
                filename = line[3:]
                files.append({"status": status, "file": filename})

        result.data["parsed_files"] = files
        result.data["is_clean"] = len(files) == 0
    return result

def git_diff(path: str = ".", staged: bool = False) -> ToolResult:
    """
    Get the git diff of changes.

    :param path: Path to the repository or specific file
    :param staged: If True, show staged changes
    :return: Diff output
    """
    cmd = "git diff --staged" if staged else "git diff"
    return execute_bash(cmd, working_dir=path)

def git_log(repo_path: str = ".",
            max_commits: int = 10,
            oneline: bool = True) -> ToolResult:
    """
    Get the git log of recent commits.

    :param repo_path: Path to the git repository
    :param max_commits: Maximum number of commits to show
    :param oneline: Use one-line format
    :return: Git log output
    """
    format_flag = "--oneline" if oneline else "--format=medium"
    cmd = f"git log {format_flag} -n {max_commits}"
    return execute_bash(cmd, working_dir=repo_path)

def git_commit(message: str,
               repo_path: str = ".",
               add_all: bool = False) -> ToolResult:
    """
    Create a git commit.

    :param message: Commit message
    :param repo_path: Path to the git repository
    :param add_all: Stage all changes before committing
    :return: Commit result
    """
    if add_all:
        add_result = execute_bash("git add -A", working_dir=repo_path)
        if add_result.status == ToolResultStatus.ERROR:
            return add_result

    # Escape message for shell
    escaped_message = message.replace('"', '\\"').replace("$", "\\$")
    return execute_bash(f'git commit -m "{escaped_message}"', working_dir=repo_path)

# =============================================================================
# TOOL CATEGORY 6: Deployment Operations
# =============================================================================

def docker_build(dockerfile: str = "Dockerfile",
                 tag: str = "app:latest",
                 context: str = ".",
                 build_args: Optional[Dict[str, str]] = None) -> ToolResult:
    """
    Build a Docker image.

    :param dockerfile: Path to the Dockerfile
    :param tag: Image tag
    :param context: Build context directory
    :param build_args: Build arguments
    :return: Build result
    """
    cmd = f"docker build -f {dockerfile} -t {tag}"
    if build_args:
        for key, value in build_args.items():
            cmd += f" --build-arg {key}={value}"
    cmd += f" {context}"

    return execute_bash(cmd, timeout=300)

def docker_run(image: str,
               name: Optional[str] = None,
               ports: Optional[Dict[str, str]] = None,
               env: Optional[Dict[str, str]] = None,
               volumes: Optional[Dict[str, str]] = None,
               detach: bool = True,
               command: Optional[str] = None) -> ToolResult:
    """
    Run a Docker container.

    :param image: Image to run
    :param name: Container name
    :param ports: Port mappings (host: container)
    :param env: Environment variables
    :param volumes: Volume mounts (host: container)
    :param detach: Run in detached mode
    :param command: Command to run in container
    :return: Container ID or output
    """
    cmd = "docker run"

    if detach:
        cmd += " -d"

    if name:
        cmd += f" --name {name}"

    if ports:
        for host_port, container_port in ports.items():
            cmd += f" -p {host_port}:{container_port}"

    if env:
        for key, value in env.items():
            cmd += f" -e {key}={value}"

    if volumes:
        for host_path, container_path in volumes.items():
            cmd += f" -v {host_path}:{container_path}"

    cmd += f" {image}"

    if command:
        cmd += f" {command}"

    return execute_bash(cmd)

def docker_ps(all: bool = False) -> ToolResult:
    """
    List Docker containers.

    :param all: Show all containers (including stopped)
    :return: List of containers
    """
    cmd = "docker ps --format '{{json .}}'"
    if all:
        cmd = "docker ps -a --format '{{json .}}'"

    result = execute_bash(cmd)
    if result.status == ToolResultStatus.SUCCESS:
        containers = []
        for line in result.data["stdout"].strip().split("\n"):
            if line:
                try:
                    containers.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        result.data["containers"] = containers

    return result

def npm_run(script: str,
            package_dir: str = ".",
            args: Optional[List[str]] = None) -> ToolResult:
    """
    Run an npm script.

    :param script: Script name from package.json
    :param package_dir: Directory containing package.json
    :param args: Additional arguments to pass
    :return: Script output
    """
    cmd = f"npm run {script}"
    if args:
        cmd += " -- " + " ".join(args)

    return execute_bash(cmd, working_dir=package_dir, timeout=300)

def pip_install(packages: List[str],
                requirements_file: Optional[str] = None,
                upgrade: bool = False) -> ToolResult:
    """
    Install Python packages with pip.

    :param packages: List of packages to install
    :param requirements_file: Path to requirements.txt
    :param upgrade: Upgrade packages to latest version
    :return: Installation result
    """
    if requirements_file:
        cmd = f"pip install -r {requirements_file}"
    else:
        cmd = "pip install " + " ".join(packages)

    if upgrade:
        cmd += " --upgrade"

    return execute_bash(cmd, timeout=300)

# =============================================================================
# TOOL REGISTRY
# =============================================================================

TOOL_CATEGORIES = {
    "file_operations": {
        "read_file": read_file,
        "write_file": write_file,
        "edit_file": edit_file,
        "delete_file": delete_file,
        "list_files": list_files,
        "copy_file": copy_file,
        "move_file": move_file,
    },
    "search_navigation": {
        "glob_search": glob_search,
        "grep_search": grep_search,
    },
    "code_execution": {
        "execute_bash": execute_bash,
        "execute_python": execute_python,
    },
    "network_operations": {
        "http_fetch": http_fetch,
        "tcp_connect": tcp_connect,
        "dns_lookup": dns_lookup,
    },
    "git_operations": {
        "git_status": git_status,
        "git_diff": git_diff,
        "git_log": git_log,
        "git_commit": git_commit,
    },
    "deployment": {
        "docker_build": docker_build,
        "docker_run": docker_run,
        "docker_ps": docker_ps,
        "npm_run": npm_run,
        "pip_install": pip_install,
    }
}

# Flatten for direct access
TOOL_REGISTRY: Dict[str, Callable] = {}
for category, tools in TOOL_CATEGORIES.items():
    TOOL_REGISTRY.update(tools)

# =============================================================================
# TOOL DESCRIPTION GENERATION
# =============================================================================

def get_tool_description(tool_name: str, tool_func: Callable) -> str:
    """Generate a description of a tool from its docstring and signature."""
    sig = inspect.signature(tool_func)
    doc = tool_func.__doc__ or "No description available"

    # Parse parameters from signature
    params = []
    for name, param in sig.parameters.items():
        annotation = param.annotation
        if annotation != inspect.Parameter.empty:
            type_str = annotation.__name__ if hasattr(annotation, '__name__') else str(annotation)
        else:
            type_str = "any"

        default = param.default
        if default != inspect.Parameter.empty:
            params.append(f"  - {name}: {type_str} (default: {default!r})")
        else:
            params.append(f"  - {name}: {type_str} (required)")

    params_str = "\n".join(params) if params else "  (no parameters)"

    return f"""
TOOL: {tool_name}
{'-' * 40}
{doc}

Parameters:
{params_str}
"""

def generate_tool_documentation() -> str:
    """Generate full documentation for all tools."""
    docs = []

    for category, tools in TOOL_CATEGORIES.items():
        docs.append(f"\n{'=' * 50}")
        docs.append(f"CATEGORY: {category.upper().replace('_', ' ')}")
        docs.append('=' * 50)

        for tool_name, tool_func in tools.items():
            docs.append(get_tool_description(tool_name, tool_func))

    return "\n".join(docs)

# =============================================================================
# SYSTEM PROMPT
# =============================================================================

SYSTEM_PROMPT_TEMPLATE = """You are an advanced coding assistant with access to a comprehensive set of tools for software development tasks. You can read and write files, execute code, search codebases, interact with networks, manage git repositories, and handle deployments.

## Available Tools

{tool_documentation}

## Tool Call Format

When you need to use a tool, respond with EXACTLY this format on a single line:
tool: TOOL_NAME({{"param1": "value1", "param2": "value2"}})

Use compact JSON with double quotes. Only one tool call per line.
After receiving tool results, analyze them and continue the task.

## Guidelines

1. **Read before edit**: Always read a file before modifying it
2. **Verify paths**: Use list_files or glob_search to verify paths exist
3. **Handle errors gracefully**: Check tool results and handle failures
4. **Be precise with edits**: Use exact string matching for edit_file
5. **Explain your actions**: Briefly describe what you're doing and why
6. **Chain operations**: You can make multiple tool calls to complete complex tasks
7. **Security awareness**: Never execute commands that could harm the system

## Response Format

When not calling tools, respond in clear, concise prose. Explain your reasoning and next steps.
"""

def get_system_prompt() -> str:
    """Generate the full system prompt with tool documentation."""
    tool_docs = generate_tool_documentation()
    return SYSTEM_PROMPT_TEMPLATE.format(tool_documentation=tool_docs)

# =============================================================================
# TOOL INVOCATION PARSING
# =============================================================================

def extract_tool_invocations(text: str) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Extract tool invocations from LLM response text.

    Looks for lines in format: tool: TOOL_NAME({"param": "value"})
    """
    invocations = []

    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("tool:"):
            continue

        try:
            # Extract content after "tool:"
            after_prefix = line[5:].strip()

            # Find the function name and JSON args
            paren_start = after_prefix.index("(")
            tool_name = after_prefix[:paren_start].strip()

            # Extract JSON between parentheses
            paren_end = after_prefix.rindex(")")
            json_str = after_prefix[paren_start + 1:paren_end].strip()

            # Parse JSON arguments
            if json_str:
                args = json.loads(json_str)
            else:
                args = {}

            if tool_name in TOOL_REGISTRY:
                invocations.append((tool_name, args))
            else:
                print(f"{ANSI_COLORS['error']}Unknown tool: {tool_name}{ANSI_COLORS['reset']}")

        except (ValueError, json.JSONDecodeError) as e:
            print(f"{ANSI_COLORS['error']}Failed to parse tool call: {line}{ANSI_COLORS['reset']}")
            continue

    return invocations

def execute_tool(tool_name: str, args: Dict[str, Any]) -> ToolResult:
    """Execute a tool with the given arguments."""
    if tool_name not in TOOL_REGISTRY:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"tool": tool_name},
            message=f"Unknown tool: {tool_name}"
        )

    tool_func = TOOL_REGISTRY[tool_name]

    try:
        # Get the function signature to map args correctly
        sig = inspect.signature(tool_func)

        # Filter args to only include valid parameters
        valid_params = set(sig.parameters.keys())
        filtered_args = {k: v for k, v in args.items() if k in valid_params}

        result = tool_func(**filtered_args)

        # Ensure result is a ToolResult
        if not isinstance(result, ToolResult):
            result = ToolResult(
                status=ToolResultStatus.SUCCESS,
                data={"result": result}
            )

        return result

    except TypeError as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"tool": tool_name, "args": args},
            message=f"Invalid arguments: {str(e)}"
        )
    except Exception as e:
        return ToolResult(
            status=ToolResultStatus.ERROR,
            data={"tool": tool_name, "args": args},
            message=f"Tool execution error: {str(e)}"
        )

# =============================================================================
# LLM CLIENT
# =============================================================================

class AgentClient:
    """Client for interacting with the Anthropic API."""

    def __init__(self,
                 api_key: Optional[str] = None,
                 model: str = DEFAULT_MODEL,
                 max_tokens: int = MAX_TOKENS):
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        if not self.api_key:
            raise ValueError("ANTHROPIC_API_KEY environment variable or api_key parameter required")

        self.client = anthropic.Anthropic(api_key=self.api_key)
        self.model = model
        self.max_tokens = max_tokens

    def chat(self, conversation: List[Dict[str, str]],
             system: Optional[str] = None) -> str:
        """Send a conversation to the API and get a response."""

        # Separate system message from other messages
        messages = []
        system_content = system or ""

        for msg in conversation:
            if msg["role"] == "system":
                system_content = msg["content"]
            else:
                messages.append(msg)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system_content,
            messages=messages
        )

        return response.content[0].text

    def stream_chat(self, conversation: List[Dict[str, str]],
                    system: Optional[str] = None):
        """Stream a conversation response."""

        messages = []
        system_content = system or ""

        for msg in conversation:
            if msg["role"] == "system":
                system_content = msg["content"]
            else:
                messages.append(msg)

        with self.client.messages.stream(
            model=self.model,
            max_tokens=self.max_tokens,
            system=system_content,
            messages=messages
        ) as stream:
            for text in stream.text_stream:
                yield text

# =============================================================================
# AGENT LOOP
# =============================================================================

class AgenticSamplingAgent:
    """
    Main agent class that implements the agentic sampling loop.

    The loop:
    1. Get user input
    2. Send to LLM with conversation history
    3. Parse response for tool calls
    4. Execute tools and add results to conversation
    5. Continue until LLM responds without tool calls
    """

    def __init__(self,
                 api_key: Optional[str] = None,
                 model: str = DEFAULT_MODEL,
                 allowed_tools: Optional[List[str]] = None,
                 verbose: bool = True):
        self.client = AgentClient(api_key=api_key, model=model)
        self.system_prompt = get_system_prompt()
        self.conversation: List[Dict[str, str]] = []
        self.verbose = verbose

        # Filter allowed tools if specified
        if allowed_tools:
            self.allowed_tools = set(allowed_tools)
        else:
            self.allowed_tools = set(TOOL_REGISTRY.keys())

    def _print(self, message: str, color: str = "reset"):
        """Print with color if verbose mode is on."""
        if self.verbose:
            print(f"{ANSI_COLORS.get(color, '')}{message}{ANSI_COLORS['reset']}")

    def _add_user_message(self, content: str):
        """Add a user message to the conversation."""
        self.conversation.append({"role": "user", "content": content})

    def _add_assistant_message(self, content: str):
        """Add an assistant message to the conversation."""
        self.conversation.append({"role": "assistant", "content": content})

    def _execute_tools(self, invocations: List[Tuple[str, Dict[str, Any]]]) -> str:
        """Execute a list of tool invocations and return formatted results."""
        results = []

        for tool_name, args in invocations:
            if tool_name not in self.allowed_tools:
                result = ToolResult(
                    status=ToolResultStatus.ERROR,
                    data={"tool": tool_name},
                    message=f"Tool '{tool_name}' is not allowed"
                )
            else:
                self._print(f"  Executing: {tool_name}({json.dumps(args, default=str)})", "tool")
                result = execute_tool(tool_name, args)

            results.append(f"tool_result({tool_name}): {result.to_json()}")

        return "\n".join(results)

    def process_message(self, user_message: str) -> str:
        """
        Process a user message through the agent loop.

        Returns the final assistant response after all tool calls are complete.
        """
        self._add_user_message(user_message)

        max_iterations = 20  # Prevent infinite loops
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            # Get LLM response
            response = self.client.chat(
                self.conversation,
                system=self.system_prompt
            )

            # Check for tool invocations
            invocations = extract_tool_invocations(response)

            if not invocations:
                # No tool calls - this is the final response
                self._add_assistant_message(response)
                return response

            # Execute tools
            self._add_assistant_message(response)
            tool_results = self._execute_tools(invocations)
            self._add_user_message(tool_results)

        return "Maximum iterations reached. The agent loop was terminated."

    def stream_message(self, user_message: str):
        """
        Stream a response for a user message.
        Yields text chunks as they arrive.
        """
        self._add_user_message(user_message)

        max_iterations = 20
        iteration = 0

        while iteration < max_iterations:
            iteration += 1

            full_response = ""
            for chunk in self.client.stream_chat(
                self.conversation,
                system=self.system_prompt
            ):
                full_response += chunk
                yield chunk

            # Check for tool invocations
            invocations = extract_tool_invocations(full_response)

            if not invocations:
                self._add_assistant_message(full_response)
                return

            # Execute tools
            self._add_assistant_message(full_response)
            yield "\n\n"

            tool_results = self._execute_tools(invocations)
            self._add_user_message(tool_results)

            yield f"\n[Tool results received, continuing...]\n\n"

    def run_interactive(self):
        """Run the agent in interactive mode."""
        print(f"\n{ANSI_COLORS['info']}Agentic Sampling Agent{ANSI_COLORS['reset']}")
        print(f"{ANSI_COLORS['dim']}Model: {self.client.model}")
        print(f"Available tools: {len(self.allowed_tools)}")
        print(f"Type 'exit' or 'quit' to end the session")
        print(f"Type 'clear' to reset the conversation{ANSI_COLORS['reset']}\n")

        while True:
            try:
                user_input = input(f"{ANSI_COLORS['user']}You: {ANSI_COLORS['reset']}")
            except (KeyboardInterrupt, EOFError):
                print("\n\nGoodbye!")
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
                for category, tools in TOOL_CATEGORIES.items():
                    print(f"\n{category}:")
                    for tool_name in tools:
                        if tool_name in self.allowed_tools:
                            print(f"  - {tool_name}")
                print()
                continue

            print(f"\n{ANSI_COLORS['assistant']}Assistant: {ANSI_COLORS['reset']}", end="", flush=True)

            for chunk in self.stream_message(user_input):
                print(chunk, end="", flush=True)

            print("\n")

# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point for the agent."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Agentic Sampling Agent - A comprehensive coding assistant"
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=f"Model to use (default: {DEFAULT_MODEL})"
    )
    parser.add_argument(
        "--tools",
        nargs="+",
        help="Specific tools to enable (default: all)"
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="List all available tools and exit"
    )
    parser.add_argument(
        "--prompt",
        help="Single prompt to run (non-interactive mode)"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Quiet mode (less verbose output)"
    )

    args = parser.parse_args()

    if args.list_tools:
        print(generate_tool_documentation())
        return

    try:
        agent = AgenticSamplingAgent(
            model=args.model,
            allowed_tools=args.tools,
            verbose=not args.quiet
        )

        if args.prompt:
            # Single prompt mode
            response = agent.process_message(args.prompt)
            print(response)
        else:
            # Interactive mode
            agent.run_interactive()

    except ValueError as e:
        print(f"Error: {e}")
        print("Please set the ANTHROPIC_API_KEY environment variable")
        sys.exit(1)

if __name__ == "__main__":
    main()
