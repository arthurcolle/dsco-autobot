"""
Tool Management API Server.

Handles tool CRUD, execution, search, and validation.
Uses Graph Management API for persistence.
"""
import os
import json
import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, Depends, Body
from fastapi.middleware.cors import CORSMiddleware

from ..common.schemas import (
    ToolCreate, ToolResponse, ToolAction, ToolOutput, HTTPAction, PythonCode, JavaScriptCode,
    UserSetting, ToolExecutionRequest, ToolExecutionResponse,
    SequentialToolExecutionRequest, ParallelToolExecutionRequest,
    ToolSearchRequest, ToolValidationRequest, ToolValidationResponse,
    PendingToolsResponse, AgentToolsResponse
)
from ..common.sandbox import PythonSandbox, JavaScriptSandbox
from ..graph_api.client import GraphAPIClient
from .formatters import format_tools, format_for_anthropic, format_for_openai

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# =============================================================================
# Tool Management Service
# =============================================================================

class ToolManagementService:
    """
    Core service for tool management operations.

    Uses GraphAPIClient for all persistence, keeping Neo4j logic
    separated in the Graph Management API.
    """

    def __init__(
        self,
        graph_api_url: str = "http://localhost:2015",
        embedding_api_url: Optional[str] = None
    ):
        self.graph = GraphAPIClient(graph_api_url)
        self.embedding_api_url = embedding_api_url
        self.python_sandbox = PythonSandbox()
        self.js_sandbox = JavaScriptSandbox()
        self._http_client: Optional[httpx.AsyncClient] = None

    async def _get_http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=120.0)
        return self._http_client

    async def close(self) -> None:
        """Close all connections."""
        await self.graph.close()
        if self._http_client:
            await self._http_client.aclose()

    # =========================================================================
    # Embeddings
    # =========================================================================

    async def generate_embedding(self, text: str) -> Optional[List[float]]:
        """Generate embedding for semantic search."""
        if not self.embedding_api_url:
            return None

        try:
            client = await self._get_http_client()
            response = await client.post(
                f"{self.embedding_api_url}/generate_embeddings",
                json={"texts": [text], "batch_size": 32, "max_length": 8192}
            )
            response.raise_for_status()
            result = response.json()
            return result["embeddings"][0]["embedding"]
        except Exception as e:
            logger.warning(f"Failed to generate embedding: {e}")
            return None

    # =========================================================================
    # CRUD Operations
    # =========================================================================

    async def create_tool(self, tool: ToolCreate) -> ToolResponse:
        """Create a new tool."""
        # Generate embeddings
        tool_text = f"{tool.name}: {tool.description}"
        embedding = await self.generate_embedding(tool_text)

        # Get next version
        version = await self._get_next_version(tool.name)

        # Build action dict
        action_dict = {
            "type": tool.action.type,
            "javascript": tool.action.javascript.model_dump() if tool.action.javascript else None,
            "http": tool.action.http.model_dump() if tool.action.http else None,
            "python": tool.action.python.model_dump() if tool.action.python else None
        }

        # Build output dict
        output_dict = {
            "type": tool.output.type,
            "content": tool.output.content
        }

        # Determine validation status
        validation_status = "pending" if tool.created_by == "agent" else "approved"

        # Create via Graph API
        result = await self.graph.create_tool_with_schema(
            name=tool.name,
            description=tool.description,
            input_schema=tool.input_schema,
            output_schema=tool.output_schema,
            action=action_dict,
            output=output_dict,
            embedding=embedding,
            user_settings=[s.model_dump() for s in tool.user_settings] if tool.user_settings else None,
            version=version
        )

        # Update tool with additional metadata
        await self.graph.update_node("Tool", result["tool_id"], {
            "created_by": tool.created_by,
            "agent_id": tool.agent_id,
            "validation_status": validation_status,
            "is_adhoc": tool.is_adhoc,
            "runtime": tool.runtime
        })

        logger.info(f"Created tool: {tool.name} (ID: {result['tool_id']}, Version: {version})")

        return ToolResponse(
            id=result["tool_id"],
            name=tool.name,
            description=tool.description,
            input_schema=tool.input_schema,
            output_schema=tool.output_schema,
            action=tool.action,
            output=tool.output,
            version=version,
            user_settings=tool.user_settings,
            status="online",
            created_by=tool.created_by,
            agent_id=tool.agent_id,
            validation_status=validation_status
        )

    async def get_tool(self, tool_id: str) -> Optional[ToolResponse]:
        """Get a tool by ID."""
        result = await self.graph.get_tool_full(tool_id)
        if not result or not result.get("tool"):
            return None
        return self._parse_tool_result(result)

    async def get_tool_by_name(self, name: str) -> Optional[ToolResponse]:
        """Get latest version of a tool by name."""
        result = await self.graph.execute_query("""
            MATCH (t:Tool {name: $name})
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            RETURN t, s, a, o
            ORDER BY t.version DESC
            LIMIT 1
        """, {"name": name}, strategy="single")

        if not result or not result.get("result"):
            return None

        r = result["result"]
        return self._parse_tool_result({
            "tool": r.get("t"),
            "schema": r.get("s"),
            "action": r.get("a"),
            "output": r.get("o")
        })

    async def get_tool_versions(self, name: str) -> List[ToolResponse]:
        """Get all versions of a tool."""
        result = await self.graph.execute_query("""
            MATCH (t:Tool {name: $name})
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            RETURN t, s, a, o
            ORDER BY t.version DESC
        """, {"name": name}, strategy="all")

        tools = []
        for r in result.get("result", []):
            tools.append(self._parse_tool_result({
                "tool": r.get("t"),
                "schema": r.get("s"),
                "action": r.get("a"),
                "output": r.get("o")
            }))
        return tools

    async def list_tools(self, limit: int = 100, approved_only: bool = True) -> List[ToolResponse]:
        """List all tools."""
        where_clause = "WHERE t.validation_status = 'approved'" if approved_only else ""
        result = await self.graph.execute_query(f"""
            MATCH (t:Tool)
            {where_clause}
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            RETURN t, s, a, o
            LIMIT $limit
        """, {"limit": limit}, strategy="all")

        return [self._parse_tool_result({
            "tool": r.get("t"),
            "schema": r.get("s"),
            "action": r.get("a"),
            "output": r.get("o")
        }) for r in result.get("result", []) if r.get("t")]

    async def update_tool(self, tool_id: str, tool: ToolCreate) -> ToolResponse:
        """Update an existing tool (creates new version)."""
        await self.graph.delete_tool_full(tool_id)
        return await self.create_tool(tool)

    async def delete_tool(self, tool_id: str) -> bool:
        """Delete a tool."""
        return await self.graph.delete_tool_full(tool_id)

    async def delete_all_tools(self) -> int:
        """Delete all tools."""
        result = await self.graph.execute_query("""
            MATCH (t:Tool)
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            DETACH DELETE t, s, a, o
            RETURN count(t) as deleted
        """, {}, strategy="single")
        return result.get("result", {}).get("deleted", 0)

    # =========================================================================
    # Search
    # =========================================================================

    async def search_tools(self, prompt: str, limit: int = 5) -> List[ToolResponse]:
        """Search tools by semantic similarity."""
        embedding = await self.generate_embedding(prompt)

        if embedding:
            results = await self.graph.vector_search("Tool", embedding, limit)
            tool_ids = [r["node"]["id"] for r in results]
        else:
            results = await self.graph.full_text_search("Tool", prompt, limit)
            tool_ids = [r["node"]["id"] for r in results]

        tools = []
        for tool_id in tool_ids:
            tool = await self.get_tool(tool_id)
            if tool:
                tools.append(tool)

        return tools

    # =========================================================================
    # Execution
    # =========================================================================

    async def execute_tool(self, request: ToolExecutionRequest) -> ToolExecutionResponse:
        """Execute a single tool."""
        start_time = datetime.now(timezone.utc)

        # Get the tool
        if request.tool_id:
            tool = await self.get_tool(request.tool_id)
        elif request.tool_name:
            tool = await self.get_tool_by_name(request.tool_name)
        else:
            raise HTTPException(status_code=400, detail="Either tool_id or tool_name required")

        if not tool:
            raise HTTPException(status_code=404, detail="Tool not found")

        if tool.validation_status != "approved":
            raise HTTPException(status_code=403, detail="Tool not approved for execution")

        # Execute based on action type
        action = tool.action
        output_data: Any = None

        try:
            if action.type == "http" and action.http:
                output_data = await self._execute_http(action.http, request.input_data)
            elif action.type == "python" and action.python:
                output_data = await self._execute_python(
                    action.python, request.input_data, request.user_settings
                )
            elif action.type == "javascript" and action.javascript:
                output_data = await self._execute_javascript(
                    action.javascript, request.input_data, request.user_settings
                )
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported action type: {action.type}")
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error executing tool {tool.name}: {e}")
            raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")

        end_time = datetime.now(timezone.utc)
        execution_time_ms = int((end_time - start_time).total_seconds() * 1000)

        # Format output if requested
        if request.format_type:
            output_data = format_tools([{
                "name": tool.name,
                "description": tool.description,
                "input_schema": tool.input_schema,
                "output": output_data
            }], request.format_type)

        return ToolExecutionResponse(
            tool_id=tool.id,
            tool_name=tool.name,
            output_data=output_data,
            execution_time_ms=execution_time_ms
        )

    async def execute_tools_sequential(
        self,
        request: SequentialToolExecutionRequest
    ) -> List[ToolExecutionResponse]:
        """Execute tools sequentially, passing output to next input."""
        results = []
        current_input = request.initial_input

        for tool_id in request.tool_ids:
            exec_request = ToolExecutionRequest(
                tool_id=tool_id,
                input_data=current_input,
                format_type=request.format_type
            )
            result = await self.execute_tool(exec_request)
            results.append(result)
            current_input = result.output_data if isinstance(result.output_data, dict) else {"result": result.output_data}

        return results

    async def execute_tools_parallel(
        self,
        request: ParallelToolExecutionRequest
    ) -> List[ToolExecutionResponse]:
        """Execute tools in parallel."""
        tasks = []
        for tool_id in request.tool_ids:
            input_data = request.input_data.get(tool_id, {})
            exec_request = ToolExecutionRequest(
                tool_id=tool_id,
                input_data=input_data,
                format_type=request.format_type
            )
            tasks.append(self.execute_tool(exec_request))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to error responses
        final_results = []
        for tool_id, result in zip(request.tool_ids, results):
            if isinstance(result, Exception):
                final_results.append(ToolExecutionResponse(
                    tool_id=tool_id,
                    output_data={"error": str(result)}
                ))
            else:
                final_results.append(result)

        return final_results

    async def _execute_http(self, http: HTTPAction, input_data: Dict[str, Any]) -> Any:
        """Execute an HTTP action."""
        client = await self._get_http_client()

        # Replace template parameters in URL
        url = self._replace_template_params(http.url, input_data)
        headers = {k: self._replace_template_params(v, input_data) for k, v in http.headers.items()}

        method = http.method.upper()
        if method in ["POST", "PUT", "PATCH"]:
            body = {**(http.body or {}), **input_data}
            response = await client.request(method, url, json=body, headers=headers)
        else:
            response = await client.request(method, url, params=input_data, headers=headers)

        response.raise_for_status()
        return response.json()

    async def _execute_python(
        self,
        python: PythonCode,
        input_data: Dict[str, Any],
        user_settings: Dict[str, Any]
    ) -> Any:
        """Execute Python code in sandbox."""
        result = await self.python_sandbox.execute(
            python.code,
            input_data,
            user_settings
        )
        if result.error:
            raise HTTPException(status_code=500, detail=f"Python execution error: {result.error}")

        output = result.output
        if result.stdout:
            output = {"result": output, "stdout": result.stdout}
        return output

    async def _execute_javascript(
        self,
        js: JavaScriptCode,
        input_data: Dict[str, Any],
        user_settings: Dict[str, Any]
    ) -> Any:
        """Execute JavaScript code."""
        result = await self.js_sandbox.execute(
            js.code,
            js.function_name,
            input_data,
            user_settings
        )
        if result.error:
            raise HTTPException(status_code=500, detail=f"JavaScript execution error: {result.error}")
        return result.output

    def _replace_template_params(self, template: str, params: Dict[str, Any]) -> str:
        """Replace {param} placeholders in template."""
        for key, value in params.items():
            template = template.replace(f"{{{key}}}", str(value))
        return template

    # =========================================================================
    # Validation
    # =========================================================================

    async def get_pending_tools(self) -> List[ToolResponse]:
        """Get tools pending validation."""
        result = await self.graph.execute_query("""
            MATCH (t:Tool {validation_status: 'pending'})
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            RETURN t, s, a, o
        """, {}, strategy="all")

        return [self._parse_tool_result({
            "tool": r.get("t"),
            "schema": r.get("s"),
            "action": r.get("a"),
            "output": r.get("o")
        }) for r in result.get("result", []) if r.get("t")]

    async def get_agent_tools(self, agent_id: str) -> List[ToolResponse]:
        """Get tools created by an agent."""
        result = await self.graph.execute_query("""
            MATCH (t:Tool {agent_id: $agent_id})
            OPTIONAL MATCH (t)-[:HAS_SCHEMA]->(s:ToolSchema)
            OPTIONAL MATCH (t)-[:HAS_ACTION]->(a:ToolAction)
            OPTIONAL MATCH (t)-[:HAS_OUTPUT]->(o:ToolOutput)
            RETURN t, s, a, o
        """, {"agent_id": agent_id}, strategy="all")

        return [self._parse_tool_result({
            "tool": r.get("t"),
            "schema": r.get("s"),
            "action": r.get("a"),
            "output": r.get("o")
        }) for r in result.get("result", []) if r.get("t")]

    async def validate_tool(self, request: ToolValidationRequest) -> ToolValidationResponse:
        """Approve or reject a tool."""
        new_status = "approved" if request.action == "approve" else "rejected"

        await self.graph.update_node("Tool", request.tool_id, {
            "validation_status": new_status,
            "validated_at": datetime.now(timezone.utc).isoformat()
        })

        return ToolValidationResponse(
            tool_id=request.tool_id,
            validation_status=new_status,
            validated_at=datetime.now(timezone.utc)
        )

    # =========================================================================
    # User Settings
    # =========================================================================

    async def update_user_settings(self, tool_id: str, settings: Dict[str, Any]) -> Dict[str, Any]:
        """Update user settings for a tool."""
        await self.graph.update_node("Tool", tool_id, {
            "user_settings": settings
        })
        return {"tool_id": tool_id, "settings": settings}

    # =========================================================================
    # Helpers
    # =========================================================================

    async def _get_next_version(self, tool_name: str) -> str:
        """Get the next version number for a tool."""
        result = await self.graph.execute_query("""
            MATCH (t:Tool {name: $name})
            RETURN max(t.version) as latest
        """, {"name": tool_name}, strategy="single")

        if result and result.get("result") and result["result"].get("latest"):
            latest = result["result"]["latest"]
            major, minor, patch = map(int, latest.split('.'))
            return f"{major}.{minor}.{patch + 1}"
        return "1.0.0"

    def _parse_tool_result(self, result: Dict[str, Any]) -> ToolResponse:
        """Parse a tool result from Graph API into ToolResponse."""
        t = result.get("tool", {}) or {}
        s = result.get("schema", {}) or {}
        a = result.get("action", {}) or {}
        o = result.get("output", {}) or {}

        # Parse JSON strings
        input_schema = s.get("input_schema", "{}")
        output_schema = s.get("output_schema", "{}")
        if isinstance(input_schema, str):
            input_schema = json.loads(input_schema)
        if isinstance(output_schema, str):
            output_schema = json.loads(output_schema)

        # Parse action
        action_type = a.get("type", "http")
        action = ToolAction(
            type=action_type,
            http=HTTPAction(**json.loads(a["http"])) if a.get("http") else None,
            python=PythonCode(**json.loads(a["python"])) if a.get("python") else None,
            javascript=JavaScriptCode(**json.loads(a["javascript"])) if a.get("javascript") else None
        )

        # Parse output
        output = ToolOutput(
            type=o.get("type", "json"),
            content=o.get("content", "")
        )

        # Parse user settings
        user_settings = None
        if t.get("user_settings"):
            settings_data = t["user_settings"]
            if isinstance(settings_data, str):
                settings_data = json.loads(settings_data)
            if isinstance(settings_data, list):
                user_settings = [UserSetting(**s) for s in settings_data]

        return ToolResponse(
            id=t.get("id", ""),
            name=t.get("name", ""),
            description=t.get("description", ""),
            input_schema=input_schema,
            output_schema=output_schema,
            action=action,
            output=output,
            version=t.get("version", "1.0.0"),
            user_settings=user_settings,
            status=t.get("status", "online"),
            created_by=t.get("created_by", "user"),
            agent_id=t.get("agent_id"),
            validation_status=t.get("validation_status", "approved")
        )


# =============================================================================
# FastAPI Application
# =============================================================================

_service: Optional[ToolManagementService] = None


def get_service() -> ToolManagementService:
    if _service is None:
        raise HTTPException(status_code=503, detail="Service not initialized")
    return _service


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _service
    _service = ToolManagementService(
        graph_api_url=os.getenv("GRAPH_API_URL", "http://localhost:2015"),
        embedding_api_url=os.getenv("EMBEDDING_API_URL")
    )
    yield
    if _service:
        await _service.close()


app = FastAPI(
    title="Tool Management API",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


# Tools CRUD
@app.post("/tools", response_model=ToolResponse)
async def create_tool(tool: ToolCreate, service: ToolManagementService = Depends(get_service)):
    return await service.create_tool(tool)


@app.get("/tools", response_model=List[ToolResponse])
async def list_tools(
    limit: int = 100,
    approved_only: bool = True,
    service: ToolManagementService = Depends(get_service)
):
    return await service.list_tools(limit, approved_only)


@app.get("/tools/{tool_id}", response_model=ToolResponse)
async def get_tool(tool_id: str, service: ToolManagementService = Depends(get_service)):
    tool = await service.get_tool(tool_id)
    if not tool:
        raise HTTPException(status_code=404, detail="Tool not found")
    return tool


@app.get("/tools/name/{name}", response_model=ToolResponse)
async def get_tool_by_name(name: str, service: ToolManagementService = Depends(get_service)):
    tool = await service.get_tool_by_name(name)
    if not tool:
        raise HTTPException(status_code=404, detail="Tool not found")
    return tool


@app.get("/tools/by_name/{name}", response_model=List[ToolResponse])
async def get_tool_versions(name: str, service: ToolManagementService = Depends(get_service)):
    return await service.get_tool_versions(name)


@app.get("/tools/latest/{name}", response_model=ToolResponse)
async def get_latest_tool(name: str, service: ToolManagementService = Depends(get_service)):
    tool = await service.get_tool_by_name(name)
    if not tool:
        raise HTTPException(status_code=404, detail="Tool not found")
    return tool


@app.put("/tools/{tool_id}", response_model=ToolResponse)
async def update_tool(tool_id: str, tool: ToolCreate, service: ToolManagementService = Depends(get_service)):
    return await service.update_tool(tool_id, tool)


@app.delete("/tools/{tool_id}")
async def delete_tool(tool_id: str, service: ToolManagementService = Depends(get_service)):
    if await service.delete_tool(tool_id):
        return {"message": "Tool deleted successfully"}
    raise HTTPException(status_code=404, detail="Tool not found")


@app.delete("/tools")
async def delete_all_tools(service: ToolManagementService = Depends(get_service)):
    count = await service.delete_all_tools()
    return {"message": f"Deleted {count} tools"}


# Search
@app.post("/tools/search", response_model=List[ToolResponse])
async def search_tools(
    prompt: str = Body(...),
    limit: int = Body(5),
    for_model: str = Body("anthropic"),
    service: ToolManagementService = Depends(get_service)
):
    tools = await service.search_tools(prompt, limit)
    if for_model != "raw":
        # Return formatted tool dicts
        tool_dicts = [{"name": t.name, "description": t.description, "input_schema": t.input_schema} for t in tools]
        return format_tools(tool_dicts, for_model)
    return tools


# Execution
@app.post("/execute_tool")
async def execute_tool(request: ToolExecutionRequest, service: ToolManagementService = Depends(get_service)):
    return await service.execute_tool(request)


@app.post("/execute_tools_sequential", response_model=List[ToolExecutionResponse])
async def execute_tools_sequential(
    request: SequentialToolExecutionRequest,
    service: ToolManagementService = Depends(get_service)
):
    return await service.execute_tools_sequential(request)


@app.post("/execute_tools_parallel", response_model=List[ToolExecutionResponse])
async def execute_tools_parallel(
    request: ParallelToolExecutionRequest,
    service: ToolManagementService = Depends(get_service)
):
    return await service.execute_tools_parallel(request)


# Validation
@app.get("/tools/pending", response_model=PendingToolsResponse)
async def get_pending_tools(service: ToolManagementService = Depends(get_service)):
    tools = await service.get_pending_tools()
    return PendingToolsResponse(tools=tools, count=len(tools))


@app.get("/tools/agent/{agent_id}", response_model=AgentToolsResponse)
async def get_agent_tools(agent_id: str, service: ToolManagementService = Depends(get_service)):
    tools = await service.get_agent_tools(agent_id)
    return AgentToolsResponse(agent_id=agent_id, tools=tools, count=len(tools))


@app.post("/tools/validate", response_model=ToolValidationResponse)
async def validate_tool(request: ToolValidationRequest, service: ToolManagementService = Depends(get_service)):
    return await service.validate_tool(request)


# User Settings
@app.post("/tools/{tool_id}/user_settings")
async def update_user_settings(
    tool_id: str,
    settings: Dict[str, Any],
    service: ToolManagementService = Depends(get_service)
):
    return await service.update_user_settings(tool_id, settings)


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "2016")))
