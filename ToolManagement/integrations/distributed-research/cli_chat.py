#!/usr/bin/env python3
"""
STREAMING CLI CHAT - Interactive Autonomous System Interface

A streaming chat interface that connects to all autonomous systems:
- Learning from knowledge graph
- Code generation with verification
- GOAP planning
- Parallel execution
- File operations
"""

import os
import sys
import json
import time
import readline
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import anthropic
except ImportError:
    print("Error: anthropic package required. Run: uv pip install anthropic")
    sys.exit(1)

# Import our systems
from meta_protocol import UnifiedRuntime, Message, MessageType


class StreamingCLI:
    """Interactive CLI with streaming responses and tool access."""

    def __init__(self):
        self.client = anthropic.Anthropic()
        self.runtime: Optional[UnifiedRuntime] = None
        self.conversation: List[Dict] = []
        self.model = "claude-sonnet-5"
        self.initialized = False
        self.tools = self._define_tools()

    def _define_tools(self) -> List[Dict]:
        """Define tools available to the assistant."""
        return [
            {
                "name": "learn_patterns",
                "description": "Learn patterns from the knowledge graph. Analyzes Python files to extract function signatures, class structures, imports, and decorators.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "max_files": {
                            "type": "integer",
                            "description": "Maximum number of files to analyze (default 500)"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "get_patterns",
                "description": "Get extracted patterns from the learned codebase. Returns imports, decorators, function signatures, and class structures.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "pattern_type": {
                            "type": "string",
                            "enum": ["all", "imports", "decorators", "functions", "classes"],
                            "description": "Type of patterns to retrieve"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "generate_code",
                "description": "Generate Python code based on a description. Uses learned patterns to match the codebase style.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "description": {
                            "type": "string",
                            "description": "Description of what code to generate"
                        },
                        "verify": {
                            "type": "boolean",
                            "description": "Whether to verify the generated code (default true)"
                        }
                    },
                    "required": ["description"]
                }
            },
            {
                "name": "generate_function",
                "description": "Generate a Python function with specific signature.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Function name"
                        },
                        "purpose": {
                            "type": "string",
                            "description": "What the function should do"
                        },
                        "args": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"},
                                    "type": {"type": "string"}
                                }
                            },
                            "description": "Function arguments"
                        },
                        "return_type": {
                            "type": "string",
                            "description": "Return type annotation"
                        }
                    },
                    "required": ["name", "purpose"]
                }
            },
            {
                "name": "generate_class",
                "description": "Generate a Python class with specific structure.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Class name"
                        },
                        "purpose": {
                            "type": "string",
                            "description": "What the class should do"
                        },
                        "bases": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Base classes"
                        },
                        "methods": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Method names to include"
                        },
                        "pattern": {
                            "type": "string",
                            "enum": ["factory", "builder", "dataclass", "pydantic", "observer"],
                            "description": "Design pattern to use"
                        }
                    },
                    "required": ["name", "purpose"]
                }
            },
            {
                "name": "verify_code",
                "description": "Verify Python code for syntax errors, import issues, and execution problems.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "code": {
                            "type": "string",
                            "description": "Python code to verify"
                        }
                    },
                    "required": ["code"]
                }
            },
            {
                "name": "get_development_plan",
                "description": "Get the GOAP development plan showing steps to achieve autonomous self-improvement.",
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "get_goals",
                "description": "Get all goals in the GOAP hierarchy.",
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "assess_state",
                "description": "Assess current system capabilities and state.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "capabilities": {
                            "type": "object",
                            "description": "Current capabilities to assess"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "parallel_execute",
                "description": "Execute multiple tasks in parallel using subagents.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "tasks": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "List of task descriptions to execute in parallel"
                        }
                    },
                    "required": ["tasks"]
                }
            },
            {
                "name": "run_workflow",
                "description": "Run a predefined workflow.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "workflow": {
                            "type": "string",
                            "enum": ["learn_and_generate", "plan_and_execute", "self_improve", "full_cycle"],
                            "description": "Workflow to run"
                        },
                        "params": {
                            "type": "object",
                            "description": "Workflow parameters"
                        }
                    },
                    "required": ["workflow"]
                }
            },
            {
                "name": "write_file",
                "description": "Write content to a file.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "File path to write to"
                        },
                        "content": {
                            "type": "string",
                            "description": "Content to write"
                        }
                    },
                    "required": ["path", "content"]
                }
            },
            {
                "name": "read_file",
                "description": "Read content from a file.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "File path to read"
                        }
                    },
                    "required": ["path"]
                }
            },
            {
                "name": "list_files",
                "description": "List files in a directory.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "Directory path"
                        },
                        "pattern": {
                            "type": "string",
                            "description": "Glob pattern (e.g., '*.py')"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "execute_python",
                "description": "Execute Python code and return the result.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "code": {
                            "type": "string",
                            "description": "Python code to execute"
                        }
                    },
                    "required": ["code"]
                }
            },
            {
                "name": "get_system_status",
                "description": "Get status of all connected systems.",
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            },
            {
                "name": "get_datetime",
                "description": "Get the current date, time, and timezone information.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "format": {
                            "type": "string",
                            "description": "Optional strftime format string (e.g., '%Y-%m-%d')"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "run_shell",
                "description": "Run a shell command and return the output. Use for system tasks, git, file operations, installing packages, etc.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "The shell command to execute"
                        },
                        "timeout": {
                            "type": "integer",
                            "description": "Timeout in seconds (default 60)"
                        }
                    },
                    "required": ["command"]
                }
            },
            {
                "name": "web_search",
                "description": "Search the web for information. Returns relevant results.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Search query"
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "http_request",
                "description": "Make an HTTP request to fetch data from URLs or APIs.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "url": {
                            "type": "string",
                            "description": "The URL to request"
                        },
                        "method": {
                            "type": "string",
                            "enum": ["GET", "POST", "PUT", "DELETE"],
                            "description": "HTTP method (default GET)"
                        },
                        "headers": {
                            "type": "object",
                            "description": "Optional headers"
                        },
                        "body": {
                            "type": "string",
                            "description": "Optional request body"
                        }
                    },
                    "required": ["url"]
                }
            }
        ]

    def initialize(self):
        """Initialize the unified runtime."""
        print("\033[90mInitializing systems...\033[0m")
        self.runtime = UnifiedRuntime()
        result = self.runtime.initialize()
        self.initialized = True
        systems = list(result.get('systems', {}).keys())
        print(f"\033[90mSystems ready: {', '.join(systems)}\033[0m")
        return result

    def execute_tool(self, name: str, params: Dict) -> str:
        """Execute a tool and return the result."""
        if not self.initialized:
            self.initialize()

        bus = self.runtime.orchestrator.bus

        try:
            if name == "learn_patterns":
                max_files = params.get("max_files", 500)
                resp = bus.request(Message(
                    source="cli",
                    target="learner",
                    action="learn",
                    payload={"max_files": max_files}
                ), timeout=120)
                return json.dumps(resp.payload, indent=2)

            elif name == "get_patterns":
                pattern_type = params.get("pattern_type", "all")
                resp = bus.request(Message(
                    source="cli",
                    target="learner",
                    action="get_patterns",
                    payload={"type": pattern_type}
                ), timeout=30)
                return json.dumps(resp.payload, indent=2)

            elif name == "generate_code":
                resp = bus.request(Message(
                    source="cli",
                    target="generator",
                    action="generate",
                    payload={
                        "description": params.get("description", ""),
                        "verify": params.get("verify", True)
                    }
                ), timeout=120)
                return json.dumps(resp.payload, indent=2)

            elif name == "generate_function":
                resp = bus.request(Message(
                    source="cli",
                    target="generator",
                    action="generate_function",
                    payload=params
                ), timeout=90)
                return json.dumps(resp.payload, indent=2)

            elif name == "generate_class":
                resp = bus.request(Message(
                    source="cli",
                    target="generator",
                    action="generate_class",
                    payload=params
                ), timeout=120)
                return json.dumps(resp.payload, indent=2)

            elif name == "verify_code":
                resp = bus.request(Message(
                    source="cli",
                    target="generator",
                    action="verify",
                    payload={"code": params.get("code", "")}
                ), timeout=30)
                return json.dumps(resp.payload, indent=2)

            elif name == "get_development_plan":
                resp = bus.request(Message(
                    source="cli",
                    target="planner",
                    action="get_development_plan",
                    payload={}
                ), timeout=30)
                return json.dumps(resp.payload, indent=2)

            elif name == "get_goals":
                resp = bus.request(Message(
                    source="cli",
                    target="planner",
                    action="get_goals",
                    payload={}
                ), timeout=30)
                return json.dumps(resp.payload, indent=2)

            elif name == "assess_state":
                caps = params.get("capabilities", {
                    "file_read": True,
                    "file_write": True,
                    "code_exec": True,
                    "llm_access": True,
                    "knowledge_graph": True
                })
                resp = bus.request(Message(
                    source="cli",
                    target="planner",
                    action="assess_state",
                    payload={"capabilities": caps}
                ), timeout=30)
                return json.dumps(resp.payload, indent=2)

            elif name == "parallel_execute":
                tasks = params.get("tasks", [])
                resp = bus.request(Message(
                    source="cli",
                    target="executor",
                    action="parallel_execute",
                    payload={"tasks": tasks}
                ), timeout=120)
                return json.dumps(resp.payload, indent=2)

            elif name == "run_workflow":
                workflow = params.get("workflow", "self_improve")
                wf_params = params.get("params", {})
                result = self.runtime.orchestrator.run_workflow(workflow, **wf_params)
                return json.dumps(result, indent=2)

            elif name == "write_file":
                path = Path(params.get("path", ""))
                content = params.get("content", "")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
                return json.dumps({"status": "ok", "path": str(path), "bytes": len(content)})

            elif name == "read_file":
                path = Path(params.get("path", ""))
                if path.exists():
                    content = path.read_text()
                    return json.dumps({"status": "ok", "content": content[:5000], "truncated": len(content) > 5000})
                return json.dumps({"status": "error", "error": "File not found"})

            elif name == "list_files":
                path = Path(params.get("path", "."))
                pattern = params.get("pattern", "*")
                if path.is_dir():
                    files = [str(f) for f in path.glob(pattern)][:50]
                    return json.dumps({"files": files, "count": len(files)})
                return json.dumps({"status": "error", "error": "Not a directory"})

            elif name == "execute_python":
                code = params.get("code", "")
                import io
                import contextlib
                stdout = io.StringIO()
                result = None
                try:
                    with contextlib.redirect_stdout(stdout):
                        exec_globals = {}
                        exec(code, exec_globals)
                        result = stdout.getvalue()
                    return json.dumps({"status": "ok", "output": result})
                except Exception as e:
                    return json.dumps({"status": "error", "error": str(e), "output": stdout.getvalue()})

            elif name == "get_system_status":
                status = self.runtime.get_status()
                return json.dumps(status, indent=2)

            elif name == "get_datetime":
                from datetime import datetime
                import platform
                fmt = params.get("format")
                now = datetime.now()
                if fmt:
                    formatted = now.strftime(fmt)
                else:
                    formatted = now.isoformat()
                return json.dumps({
                    "datetime": formatted,
                    "date": now.strftime("%Y-%m-%d"),
                    "time": now.strftime("%H:%M:%S"),
                    "timezone": time.tzname[0],
                    "timestamp": now.timestamp(),
                    "weekday": now.strftime("%A"),
                    "iso": now.isoformat()
                })

            elif name == "run_shell":
                import subprocess
                command = params.get("command", "")
                timeout = params.get("timeout", 60)
                try:
                    result = subprocess.run(
                        command,
                        shell=True,
                        capture_output=True,
                        text=True,
                        timeout=timeout
                    )
                    return json.dumps({
                        "status": "ok" if result.returncode == 0 else "error",
                        "returncode": result.returncode,
                        "stdout": result.stdout[:10000],
                        "stderr": result.stderr[:2000] if result.stderr else None
                    })
                except subprocess.TimeoutExpired:
                    return json.dumps({"status": "error", "error": f"Command timed out after {timeout}s"})
                except Exception as e:
                    return json.dumps({"status": "error", "error": str(e)})

            elif name == "web_search":
                # Use a simple approach - could integrate with actual search API
                query = params.get("query", "")
                try:
                    import urllib.request
                    import urllib.parse
                    # DuckDuckGo instant answer API
                    url = f"https://api.duckduckgo.com/?q={urllib.parse.quote(query)}&format=json&no_html=1"
                    with urllib.request.urlopen(url, timeout=10) as response:
                        data = json.loads(response.read().decode())
                        return json.dumps({
                            "status": "ok",
                            "abstract": data.get("Abstract", ""),
                            "answer": data.get("Answer", ""),
                            "related": [t.get("Text", "") for t in data.get("RelatedTopics", [])[:5] if isinstance(t, dict)]
                        })
                except Exception as e:
                    return json.dumps({"status": "error", "error": str(e), "note": "Web search failed - try run_shell with curl"})

            elif name == "http_request":
                import urllib.request
                import urllib.error
                url = params.get("url", "")
                method = params.get("method", "GET")
                headers = params.get("headers", {})
                body = params.get("body")
                try:
                    req = urllib.request.Request(url, method=method, headers=headers)
                    if body:
                        req.data = body.encode()
                    with urllib.request.urlopen(req, timeout=30) as response:
                        content = response.read().decode('utf-8', errors='replace')
                        return json.dumps({
                            "status": "ok",
                            "code": response.status,
                            "headers": dict(response.headers),
                            "body": content[:20000],
                            "truncated": len(content) > 20000
                        })
                except urllib.error.HTTPError as e:
                    return json.dumps({"status": "error", "code": e.code, "error": str(e)})
                except Exception as e:
                    return json.dumps({"status": "error", "error": str(e)})

            else:
                return json.dumps({"error": f"Unknown tool: {name}"})

        except Exception as e:
            return json.dumps({"error": str(e)})

    def _stream_response(self, system_prompt: str):
        """Stream a response and return (text, tool_uses, stop_reason)."""
        with self.client.messages.stream(
            model=self.model,
            max_tokens=4096,
            system=system_prompt,
            messages=self.conversation,
            tools=self.tools
        ) as stream:
            response_text = ""
            tool_uses = []
            current_tool = None
            stop_reason = None

            for event in stream:
                if event.type == "content_block_start":
                    if hasattr(event.content_block, 'type'):
                        if event.content_block.type == "tool_use":
                            current_tool = {
                                "id": event.content_block.id,
                                "name": event.content_block.name,
                                "input": ""
                            }

                elif event.type == "content_block_delta":
                    if hasattr(event.delta, 'text'):
                        print(event.delta.text, end="", flush=True)
                        response_text += event.delta.text
                    elif hasattr(event.delta, 'partial_json'):
                        if current_tool:
                            current_tool["input"] += event.delta.partial_json

                elif event.type == "content_block_stop":
                    if current_tool:
                        tool_uses.append(current_tool)
                        current_tool = None

                elif event.type == "message_stop":
                    pass

            # Get final message for stop reason
            final = stream.get_final_message()
            stop_reason = final.stop_reason

            if response_text:
                print()

            return response_text, tool_uses, stop_reason

    def chat(self, user_input: str, max_turns: int = 25):
        """Process user input with multi-turn tool use until completion."""
        self.conversation.append({"role": "user", "content": user_input})

        system_prompt = """You are an autonomous AI agent with full access to tools that let you do almost anything.

## Core Principle
ALWAYS use your tools to accomplish tasks. You have the power to:
- Run ANY shell command (git, curl, pip, etc.)
- Execute ANY Python code
- Make HTTP requests to any URL/API
- Search the web for information
- Read/write any files
- Get system info like current date/time

## Available Tools (USE THEM!)
- **get_datetime**: Get current date, time, timezone - USE THIS for any time-related questions
- **run_shell**: Run ANY shell command - git, curl, ls, pip install, etc.
- **execute_python**: Run arbitrary Python code for calculations, data processing, etc.
- **http_request**: Fetch URLs, call APIs, scrape pages
- **web_search**: Search the web for information
- **read_file/write_file/list_files**: File operations
- **generate_code/generate_function/generate_class**: Code generation with pattern matching
- **parallel_execute**: Run multiple tasks simultaneously
- **learn_patterns/get_patterns**: Learn from codebases

## Critical Rules
1. **When in doubt, USE A TOOL.** Don't say "I can't" - find a tool that can.
2. For date/time questions → use get_datetime
3. For system commands → use run_shell
4. For calculations → use execute_python
5. For web info → use web_search or http_request
6. **NEVER say you lack capabilities if a tool can do it**
7. You can call multiple tools in sequence to accomplish complex tasks
8. After getting tool results, analyze them and decide if more tool calls are needed

## Examples
- "What time is it?" → Use get_datetime
- "List files in /tmp" → Use run_shell with 'ls /tmp' or list_files
- "What's 2^100?" → Use execute_python with 'print(2**100)'
- "Install requests" → Use run_shell with 'pip install requests'
- "What's the weather?" → Use web_search or http_request to an API
- Complex tasks → Chain multiple tool calls until done

Be proactive. Be capable. Use your tools. Keep going until the task is complete."""

        print()
        turn = 0

        while turn < max_turns:
            turn += 1
            response_text, tool_uses, stop_reason = self._stream_response(system_prompt)

            if not tool_uses:
                # No tool calls - agent is done
                if response_text:
                    self.conversation.append({"role": "assistant", "content": response_text})
                break

            # Build assistant message with text and tool uses
            content_blocks = []
            if response_text:
                content_blocks.append({"type": "text", "text": response_text})

            for tool in tool_uses:
                try:
                    tool_input = json.loads(tool["input"]) if tool["input"] else {}
                except:
                    tool_input = {}
                content_blocks.append({
                    "type": "tool_use",
                    "id": tool["id"],
                    "name": tool["name"],
                    "input": tool_input
                })

            self.conversation.append({"role": "assistant", "content": content_blocks})

            # Execute all tools and collect results
            tool_results = []
            for tool in tool_uses:
                try:
                    tool_input = json.loads(tool["input"]) if tool["input"] else {}
                except:
                    tool_input = {}

                print(f"\033[90m[Turn {turn}: Executing {tool['name']}...]\033[0m")
                result = self.execute_tool(tool["name"], tool_input)

                # Show abbreviated result
                result_preview = result[:300] + "..." if len(result) > 300 else result
                print(f"\033[90m{result_preview}\033[0m\n")

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tool["id"],
                    "content": result
                })

            self.conversation.append({"role": "user", "content": tool_results})

            # Check stop reason - if it was 'end_turn' and we have tool results, continue
            # The loop will continue and get the next response

        if turn >= max_turns:
            print(f"\033[93m[Reached max turns ({max_turns}), stopping]\033[0m")

    def run(self):
        """Main chat loop."""
        print("\033[1m" + "="*60 + "\033[0m")
        print("\033[1mAUTONOMOUS CODING ASSISTANT\033[0m")
        print("\033[1m" + "="*60 + "\033[0m")
        print()
        print("Connected systems: learner, generator, planner, executor")
        print("Commands: 'quit' to exit, 'clear' to reset conversation")
        print()

        # Initialize on first run
        if not self.initialized:
            self.initialize()

        while True:
            try:
                user_input = input("\033[1m>\033[0m ").strip()

                if not user_input:
                    continue

                if user_input.lower() in ('quit', 'exit', 'q'):
                    print("Goodbye!")
                    break

                if user_input.lower() == 'clear':
                    self.conversation = []
                    print("\033[90mConversation cleared.\033[0m")
                    continue

                if user_input.lower() == 'status':
                    status = self.runtime.get_status()
                    print(json.dumps(status, indent=2))
                    continue

                self.chat(user_input)
                print()

            except KeyboardInterrupt:
                print("\n\033[90mInterrupted. Type 'quit' to exit.\033[0m")
            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"\033[91mError: {e}\033[0m")


def main():
    cli = StreamingCLI()
    cli.run()


if __name__ == "__main__":
    main()
