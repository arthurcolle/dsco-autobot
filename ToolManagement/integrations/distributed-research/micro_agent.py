import json
import sys
import traceback
from io import StringIO
from contextlib import redirect_stdout, redirect_stderr
from typing import Any, Dict, Optional

class ExecutionResult:
    def __init__(self, success: bool, output: str, error: Optional[str] = None):
        self.success = success
        self.output = output
        self.error = error

class MicroAgent:
    def __init__(self):
        self.globals_dict = {"__builtins__": __builtins__}
        self.locals_dict = {}
    
    def execute(self, code: str) -> str:
        stdout = StringIO()
        try:
            with redirect_stdout(stdout):
                exec(code, self.globals_dict, self.locals_dict)
            return stdout.getvalue()
        except Exception as e:
            return f"Error: {e}"
