"""
Python Sandbox for safe code execution.

Provides isolated execution environment for ad-hoc Python tools.
"""
import sys
import io
import ast
import asyncio
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from contextlib import redirect_stdout, redirect_stderr

logger = logging.getLogger(__name__)


@dataclass
class SandboxResult:
    """Result from sandbox execution."""
    output: Any = None
    stdout: str = ""
    stderr: str = ""
    error: Optional[str] = None
    execution_time_ms: int = 0


class PythonSandbox:
    """
    Safe Python code execution sandbox.

    Features:
    - Restricted builtins
    - Timeout support
    - Stdout/stderr capture
    - Input data injection
    """

    # Allowed builtins for sandbox
    SAFE_BUILTINS = {
        'abs', 'all', 'any', 'ascii', 'bin', 'bool', 'bytearray', 'bytes',
        'callable', 'chr', 'dict', 'dir', 'divmod', 'enumerate', 'filter',
        'float', 'format', 'frozenset', 'getattr', 'hasattr', 'hash', 'hex',
        'int', 'isinstance', 'issubclass', 'iter', 'len', 'list', 'map',
        'max', 'min', 'next', 'oct', 'ord', 'pow', 'print', 'range',
        'repr', 'reversed', 'round', 'set', 'slice', 'sorted', 'str',
        'sum', 'tuple', 'type', 'zip',
        'True', 'False', 'None',
    }

    # Blocked modules
    BLOCKED_MODULES = {
        'os', 'sys', 'subprocess', 'shutil', 'socket', 'requests',
        'urllib', 'http', 'ftplib', 'telnetlib', 'smtplib', 'poplib',
        'imaplib', 'nntplib', 'multiprocessing', 'threading', 'ctypes',
        'pickle', 'marshal', 'shelve', 'dbm', 'sqlite3', 'builtins',
        '__builtins__', 'importlib', 'imp', 'code', 'codeop', 'compile',
        'exec', 'eval', 'execfile', 'input', 'raw_input', 'open', 'file',
    }

    def __init__(self, timeout: float = 30.0, max_output_size: int = 1024 * 1024):
        """
        Initialize sandbox.

        Args:
            timeout: Maximum execution time in seconds
            max_output_size: Maximum output size in bytes
        """
        self.timeout = timeout
        self.max_output_size = max_output_size

    def _create_safe_globals(self, input_data: Dict[str, Any], user_settings: Dict[str, Any]) -> Dict[str, Any]:
        """Create restricted globals for execution."""
        safe_builtins = {k: getattr(__builtins__, k) if hasattr(__builtins__, k) else None
                        for k in self.SAFE_BUILTINS if hasattr(__builtins__, k) or k in ('True', 'False', 'None')}

        # Handle True, False, None
        safe_builtins['True'] = True
        safe_builtins['False'] = False
        safe_builtins['None'] = None

        # Add safe modules
        import json
        import math
        import re
        import datetime
        import collections
        import itertools
        import functools
        import operator
        import decimal
        import fractions
        import random
        import statistics
        import string
        import base64
        import hashlib
        import hmac

        safe_modules = {
            'json': json,
            'math': math,
            're': re,
            'datetime': datetime,
            'collections': collections,
            'itertools': itertools,
            'functools': functools,
            'operator': operator,
            'decimal': decimal,
            'fractions': fractions,
            'random': random,
            'statistics': statistics,
            'string': string,
            'base64': base64,
            'hashlib': hashlib,
            'hmac': hmac,
        }

        return {
            '__builtins__': safe_builtins,
            '__name__': '__sandbox__',
            '__doc__': None,
            'input_data': input_data,
            'user_settings': user_settings,
            **safe_modules,
        }

    def _validate_code(self, code: str) -> Optional[str]:
        """
        Validate code for safety.

        Returns error message if unsafe, None if safe.
        """
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return f"Syntax error: {e}"

        for node in ast.walk(tree):
            # Check for import statements
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                module_name = node.names[0].name if isinstance(node, ast.Import) else node.module
                if module_name and any(blocked in module_name for blocked in self.BLOCKED_MODULES):
                    return f"Import of '{module_name}' is not allowed"

            # Check for exec/eval calls
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in ('exec', 'eval', 'compile', '__import__'):
                        return f"Call to '{node.func.id}' is not allowed"

            # Check for attribute access to dangerous methods
            if isinstance(node, ast.Attribute):
                if node.attr in ('__class__', '__bases__', '__subclasses__', '__mro__',
                                '__globals__', '__code__', '__closure__', '__func__'):
                    return f"Access to '{node.attr}' is not allowed"

        return None

    async def execute(
        self,
        code: str,
        input_data: Dict[str, Any],
        user_settings: Dict[str, Any]
    ) -> SandboxResult:
        """
        Execute Python code in sandbox.

        Args:
            code: Python code to execute
            input_data: Input data available as 'input_data' variable
            user_settings: User settings available as 'user_settings' variable

        Returns:
            SandboxResult with output, stdout, stderr, and any errors
        """
        import time
        start_time = time.time()

        # Validate code
        validation_error = self._validate_code(code)
        if validation_error:
            return SandboxResult(error=validation_error)

        # Create safe execution environment
        safe_globals = self._create_safe_globals(input_data, user_settings)
        local_vars: Dict[str, Any] = {}

        # Capture stdout/stderr
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        try:
            # Execute with timeout
            def run_code():
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    exec(code, safe_globals, local_vars)

            # Run in thread pool with timeout
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(None, run_code),
                timeout=self.timeout
            )

            # Get result (look for 'result' variable or last expression)
            output = local_vars.get('result', local_vars.get('output'))

            execution_time = int((time.time() - start_time) * 1000)

            return SandboxResult(
                output=output,
                stdout=stdout_capture.getvalue()[:self.max_output_size],
                stderr=stderr_capture.getvalue()[:self.max_output_size],
                execution_time_ms=execution_time
            )

        except asyncio.TimeoutError:
            return SandboxResult(error=f"Execution timed out after {self.timeout} seconds")
        except Exception as e:
            logger.exception(f"Sandbox execution error: {e}")
            return SandboxResult(
                error=str(e),
                stdout=stdout_capture.getvalue()[:self.max_output_size],
                stderr=stderr_capture.getvalue()[:self.max_output_size],
                execution_time_ms=int((time.time() - start_time) * 1000)
            )


class JavaScriptSandbox:
    """
    JavaScript code execution via external service or subprocess.

    For production, use a dedicated JS runtime service.
    """

    def __init__(self, runtime_url: Optional[str] = None, timeout: float = 30.0):
        self.runtime_url = runtime_url
        self.timeout = timeout

    async def execute(
        self,
        code: str,
        function_name: str,
        input_data: Dict[str, Any],
        user_settings: Dict[str, Any]
    ) -> SandboxResult:
        """Execute JavaScript code."""
        if self.runtime_url:
            # Use external runtime service
            import httpx
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.post(
                        f"{self.runtime_url}/execute",
                        json={
                            "code": code,
                            "function_name": function_name,
                            "input_data": input_data,
                            "user_settings": user_settings
                        }
                    )
                    response.raise_for_status()
                    result = response.json()
                    return SandboxResult(
                        output=result.get("output"),
                        stdout=result.get("stdout", ""),
                        stderr=result.get("stderr", ""),
                        error=result.get("error")
                    )
            except Exception as e:
                return SandboxResult(error=f"JavaScript execution failed: {e}")
        else:
            # Fallback: use Node.js subprocess (requires node installed)
            return await self._execute_node(code, function_name, input_data, user_settings)

    async def _execute_node(
        self,
        code: str,
        function_name: str,
        input_data: Dict[str, Any],
        user_settings: Dict[str, Any]
    ) -> SandboxResult:
        """Execute JavaScript using Node.js subprocess."""
        import json
        import tempfile
        import os

        # Create wrapper script
        wrapper = f"""
const inputData = {json.dumps(input_data)};
const userSettings = {json.dumps(user_settings)};

{code}

try {{
    const result = {function_name}(inputData, userSettings);
    if (result instanceof Promise) {{
        result.then(r => console.log(JSON.stringify({{output: r}})))
              .catch(e => console.log(JSON.stringify({{error: e.message}})));
    }} else {{
        console.log(JSON.stringify({{output: result}}));
    }}
}} catch (e) {{
    console.log(JSON.stringify({{error: e.message}}));
}}
"""

        try:
            # Write to temp file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.js', delete=False) as f:
                f.write(wrapper)
                temp_path = f.name

            # Execute
            proc = await asyncio.create_subprocess_exec(
                'node', temp_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=self.timeout
            )

            # Clean up
            os.unlink(temp_path)

            # Parse result
            try:
                result = json.loads(stdout.decode())
                return SandboxResult(
                    output=result.get("output"),
                    error=result.get("error"),
                    stderr=stderr.decode()
                )
            except json.JSONDecodeError:
                return SandboxResult(
                    stdout=stdout.decode(),
                    stderr=stderr.decode()
                )

        except asyncio.TimeoutError:
            return SandboxResult(error=f"JavaScript execution timed out after {self.timeout} seconds")
        except FileNotFoundError:
            return SandboxResult(error="Node.js not found. Install Node.js or configure runtime_url.")
        except Exception as e:
            return SandboxResult(error=f"JavaScript execution failed: {e}")
