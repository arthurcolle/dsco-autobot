+ # tool_management_modal_app.py
from modal import App, Image, Secret, asgi_app
from fastapi import FastAPI, HTTPException, Depends, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Union, Optional
import os
import json
import asyncio
import logging
import httpx
+ 
+ # Attempt to import Neo4j
+ try:
+     from neo4j import AsyncGraphDatabase
+     NEO4J_AVAILABLE = True
+ except ImportError:
+     print("Warning: Neo4j driver not installed. Some functionality may be limited.")
+     NEO4J_AVAILABLE = False
+     AsyncGraphDatabase = None
+ 
+ # ──────────────────────────────────────────────────────────────────────────
+ # Configuration
+ # ──────────────────────────────────────────────────────────────────────────
+ TOOL_MGMT_APP_LABEL = "tool_management_api"
+ NEO4J_SECRET_NAME = "NEO4J_CREDENTIALS"  # (Or any name you use in your Modal project)
+ LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
+ 
+ # ──────────────────────────────────────────────────────────────────────────
+ # Logging setup
+ # ──────────────────────────────────────────────────────────────────────────
+ logging.basicConfig(level=LOG_LEVEL)
+ logger = logging.getLogger(__name__)
+ 
+ # ──────────────────────────────────────────────────────────────────────────
+ # Modal configuration
+ # ──────────────────────────────────────────────────────────────────────────
+ # Create a Modal image with the required packages
+ image = (
+     Image.debian_slim()
+     .pip_install(
+         [
+             "fastapi",
+             "uvicorn",
+             "pydantic",
+             "neo4j",         # remove if you don't need Neo4j
+             "python-dotenv", # if you want to load .env
+         ]
+     )
+     .apt_install(["python3-dev", "gcc"])
+ )

# Create the Modal stub (App)
stub = App(TOOL_MGMT_APP_LABEL)

# ──────────────────────────────────────────────────────────────────────────
# Pydantic models
# ──────────────────────────────────────────────────────────────────────────
class Tool(BaseModel):
    name: str
    description: str
    input_schema: dict
    output_schema: dict
    code: str

class ToolResponse(BaseModel):
    id: str
    name: str
    description: str
    input_schema: dict
    output_schema: dict

class ToolExecutionRequest(BaseModel):
    tool_id: str
    input_data: Dict[str, Any]

class ToolExecutionResponse(BaseModel):
    tool_id: str
    output_data: Dict[str, Any]

class SequentialToolExecutionRequest(BaseModel):
    tool_ids: List[str]
    initial_input: Dict[str, Any]

class ParallelToolExecutionRequest(BaseModel):
    tool_ids: List[str]
    input_data: Dict[str, Dict[str, Any]]

class BatchToolExecutionRequest(BaseModel):
    """Execute multiple tools with the SAME input"""
    tool_ids: List[str]
    input_data: Dict[str, Any]

class DAGNode(BaseModel):
    """Node in execution DAG"""
    tool_id: str
    input_data: Dict[str, Any] = {}
    depends_on: List[str] = []  # List of node IDs this depends on
    merge_outputs: bool = False  # If True, merge outputs from dependencies into input

class DAGExecutionRequest(BaseModel):
    """Execute tools as a directed acyclic graph"""
    nodes: Dict[str, DAGNode]  # node_id -> DAGNode
+ 
# ──────────────────────────────────────────────────────────────────────────
# Embedding Provider
# ──────────────────────────────────────────────────────────────────────────
class EmbeddingProvider:
    """Compute embeddings for semantic search."""

    def __init__(self):
        self.api_key = os.getenv("JINA_API_KEY") or os.getenv("OPENAI_API_KEY")
        self.provider = "jina" if os.getenv("JINA_API_KEY") else "openai" if os.getenv("OPENAI_API_KEY") else None
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0)
        return self._client

    async def embed(self, text: str) -> Optional[List[float]]:
        """Generate embedding for text."""
        if not self.provider or not self.api_key:
            logger.warning("No embedding provider configured")
            return None

        try:
            client = await self._get_client()

            if self.provider == "jina":
                # jina-embeddings-v4 supports: 2048 (default), 1024, 512, 256
                dim = int(os.getenv("EMBEDDING_DIM", "1024"))
                response = await client.post(
                    "https://api.jina.ai/v1/embeddings",
                    headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                    json={"model": "jina-embeddings-v4", "input": [text], "normalized": True, "dimensions": dim}
                )
                response.raise_for_status()
                return response.json()["data"][0]["embedding"]

            elif self.provider == "openai":
                response = await client.post(
                    "https://api.openai.com/v1/embeddings",
                    headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                    json={"model": "text-embedding-3-small", "input": text}
                )
                response.raise_for_status()
                return response.json()["data"][0]["embedding"]

        except Exception as e:
            logger.error(f"Embedding failed: {e}")
            return None

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# Global embedder
_embedder: Optional[EmbeddingProvider] = None

def get_embedder() -> EmbeddingProvider:
    global _embedder
    if _embedder is None:
        _embedder = EmbeddingProvider()
    return _embedder


# ──────────────────────────────────────────────────────────────────────────
# Neo4j connection management
# ──────────────────────────────────────────────────────────────────────────
class Neo4jConnection:
+     """Manages the Neo4j database connection."""
+ 
+     def __init__(self):
+         """Initialize the Neo4j connection using environment variables from secrets."""
+         if not NEO4J_AVAILABLE:
+             logger.warning("Neo4j driver not installed. Some functionalities are disabled.")
+             return
+ 
+         uri = os.getenv("NEO4J_URI", "bolt://n4j.brainchain.cloud")
+         user = os.getenv("NEO4J_USER", "neo4j")
+         password = os.getenv("NEO4J_PASSWORD", "password")
+         self.driver = AsyncGraphDatabase.driver(uri, auth=(user, password))
+ 
+     async def close(self):
+         """Close the Neo4j connection."""
+         if NEO4J_AVAILABLE and self.driver is not None:
+             await self.driver.close()
+ 
+     async def get_session(self):
+         """Get a new Neo4j session."""
+         if NEO4J_AVAILABLE and self.driver is not None:
+             return self.driver.session()
+         return None
+ 
+ async def get_db():
+     """Dependency to get database session."""
+     db = Neo4jConnection()
+     try:
+         yield db
+     finally:
+         await db.close()
+ 
+ async def get_session(db: Neo4jConnection = Depends(get_db)):
+     """Get a new Neo4j session."""
+     session = await db.get_session()
+     if session is None:
+         # If Neo4j is not available or driver not installed
+         raise HTTPException(status_code=500, detail="Neo4j session unavailable.")
+     async with session as s:
+         yield s
+ 
+ # ──────────────────────────────────────────────────────────────────────────
+ # Creating the FastAPI application
+ # ──────────────────────────────────────────────────────────────────────────
+ @stub.function(
+     image=image,
+     secrets=[Secret.from_name(NEO4J_SECRET_NAME)],
+ )
+ @asgi_app()
+ def fastapi_app():
+     """
+     The FastAPI instance, wrapped in a Modal asgi_app.
+     """
+     app = FastAPI(title="Tool Management API", version="1.0.0")
+ 
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ──────────────────────────────────────────────────────────────────────
    # Startup: Create vector index if not exists
    # ──────────────────────────────────────────────────────────────────────
    @app.on_event("startup")
    async def create_vector_index():
        """Create vector index for tool embeddings on startup."""
        dim = int(os.getenv("EMBEDDING_DIM", "1024"))
        try:
            db = Neo4jConnection()
            session = await db.get_session()
            if session:
                async with session as s:
                    # Create vector index (idempotent - will skip if exists)
                    await s.run(f"""
                        CREATE VECTOR INDEX tool_embedding IF NOT EXISTS
                        FOR (t:Tool) ON t.embedding
                        OPTIONS {{indexConfig: {{
                            `vector.dimensions`: {dim},
                            `vector.similarity_function`: 'cosine'
                        }}}}
                    """)
                    logger.info(f"Vector index 'tool_embedding' ensured ({dim} dims)")
                await db.close()
        except Exception as e:
            logger.warning(f"Could not create vector index: {e}")

    # ──────────────────────────────────────────────────────────────────────
    # Utility / Execution function
    # ──────────────────────────────────────────────────────────────────────
+     async def execute_tool(
+         tool_id: str,
+         input_data: Dict[str, Any],
+         session
+     ) -> Dict[str, Any]:
+         """Execute a single tool by looking up the Python code in Neo4j."""
+         logger.info(f"Executing tool with id: {tool_id}")
+         result = await session.run(
+             """
+             MATCH (t:Tool)-[:HAS_CODE]->(c:ToolCode)
+             WHERE ID(t) = $tool_id
+             RETURN c.code AS code
+             """,
+             tool_id=int(tool_id)
+         )
+ 
+         record = await result.single()
+         if record:
+             code = record["code"]
+             # Execute the tool code in a controlled environment
+             try:
+                 exec_globals = {"input_data": input_data, "output_data": {}}
+                 exec(code, exec_globals)
+                 output_data = exec_globals["output_data"]
+                 logger.info(f"Tool executed successfully: {tool_id}")
+                 return output_data
+             except Exception as e:
+                 logger.error(f"Error executing tool {tool_id}: {str(e)}")
+                 raise HTTPException(status_code=500, detail=f"Error executing tool: {str(e)}")
+         else:
+             logger.warning(f"Tool not found for execution with id: {tool_id}")
+             raise HTTPException(status_code=404, detail="Tool not found")
+ 
+     # ──────────────────────────────────────────────────────────────────────
+     # Routes / Endpoints
+     # ──────────────────────────────────────────────────────────────────────
+     @app.get("/")
+     async def root():
+         """Root endpoint."""
+         return {"message": "Welcome to the Tool Management API"}
+ 
+     @app.get("/health")
+     async def health_check():
+         """Health check endpoint."""
+         return {"status": "healthy"}
+ 
    # Create a new tool
    @app.post("/tools", response_model=ToolResponse)
    async def create_tool(tool: Tool, session=Depends(get_session)):
        logger.info(f"Creating new tool: {tool.name}")

        # Compute embedding
        embedder = get_embedder()
        embed_text = f"{tool.name}: {tool.description}"
        embedding = await embedder.embed(embed_text)

        if embedding:
            logger.info(f"Computed embedding for '{tool.name}' ({len(embedding)} dims)")
            result = await session.run(
                """
                CREATE (t:Tool {name: $name, description: $description, embedding: $embedding})
                CREATE (s:ToolSchema {input_schema: $input_schema, output_schema: $output_schema})
                CREATE (c:ToolCode {code: $code})
                CREATE (t)-[:HAS_SCHEMA]->(s)
                CREATE (t)-[:HAS_CODE]->(c)
                RETURN t, s
                """,
                name=tool.name,
                description=tool.description,
                embedding=embedding,
                input_schema=json.dumps(tool.input_schema),
                output_schema=json.dumps(tool.output_schema),
                code=tool.code,
            )
        else:
            logger.warning(f"No embedding for '{tool.name}' - no provider configured")
            result = await session.run(
                """
                CREATE (t:Tool {name: $name, description: $description})
                CREATE (s:ToolSchema {input_schema: $input_schema, output_schema: $output_schema})
                CREATE (c:ToolCode {code: $code})
                CREATE (t)-[:HAS_SCHEMA]->(s)
                CREATE (t)-[:HAS_CODE]->(c)
                RETURN t, s
                """,
                name=tool.name,
                description=tool.description,
                input_schema=json.dumps(tool.input_schema),
                output_schema=json.dumps(tool.output_schema),
                code=tool.code,
            )

        record = await result.single()
        if record:
            tool_node = record["t"]
            schema_node = record["s"]
            logger.info(f"Tool created successfully: {tool.name}")
            return ToolResponse(
                id=str(tool_node.id),
                name=tool_node["name"],
                description=tool_node["description"],
                input_schema=json.loads(schema_node["input_schema"]),
                output_schema=json.loads(schema_node["output_schema"]),
            )
        else:
            logger.error(f"Failed to create tool: {tool.name}")
            raise HTTPException(status_code=500, detail="Failed to create tool")
+ 
+     # Get all tools
+     @app.get("/tools", response_model=List[ToolResponse])
+     async def get_tools(session=Depends(get_session)):
+         logger.info("Fetching all tools")
+         result = await session.run(
+             """
+             MATCH (t:Tool)-[:HAS_SCHEMA]->(s:ToolSchema)
+             RETURN t, s
+             """
+         )
+         tools = []
+         async for record in result:
+             tool = record["t"]
+             schema = record["s"]
+             tools.append(
+                 ToolResponse(
+                     id=str(tool.id),
+                     name=tool["name"],
+                     description=tool["description"],
+                     input_schema=json.loads(schema["input_schema"]),
+                     output_schema=json.loads(schema["output_schema"]),
+                 )
+             )
        logger.info(f"Fetched {len(tools)} tools")
        return tools

    # Vector similarity search for tools
    @app.post("/tools/search")
    async def search_tools_vector(prompt: str, limit: int = 10, session=Depends(get_session)):
        """
        Semantic search for tools using Neo4j vector index.

        Computes embedding for query, then uses db.index.vector.queryNodes
        to find similar tools.
        """
        logger.info(f"Vector search: '{prompt}' (limit={limit})")

        # Compute query embedding
        embedder = get_embedder()
        query_embedding = await embedder.embed(prompt)

        if not query_embedding:
            raise HTTPException(
                status_code=503,
                detail="No embedding provider configured (set JINA_API_KEY or OPENAI_API_KEY)"
            )

        # Query Neo4j vector index
        result = await session.run(
            """
            CALL db.index.vector.queryNodes('tool_embedding', $k, $queryEmbedding)
            YIELD node, score
            MATCH (node)-[:HAS_SCHEMA]->(s:ToolSchema)
            RETURN node.name AS name, node.description AS description,
                   ID(node) AS id, score,
                   s.input_schema AS input_schema, s.output_schema AS output_schema
            """,
            k=limit,
            queryEmbedding=query_embedding
        )

        results = []
        async for record in result:
            results.append({
                "tool_id": str(record["id"]),
                "name": record["name"],
                "description": record["description"],
                "score": record["score"],
                "input_schema": json.loads(record["input_schema"]) if record["input_schema"] else {},
                "output_schema": json.loads(record["output_schema"]) if record["output_schema"] else {},
            })

        logger.info(f"Found {len(results)} tools for query")
        return {"query": prompt, "results": results}

    # Get a specific tool by ID
+     @app.get("/tools/{tool_id}", response_model=ToolResponse)
+     async def get_tool(tool_id: str, session=Depends(get_session)):
+         logger.info(f"Fetching tool with id: {tool_id}")
+         result = await session.run(
+             """
+             MATCH (t:Tool)-[:HAS_SCHEMA]->(s:ToolSchema)
+             WHERE ID(t) = $tool_id
+             RETURN t, s
+             """,
+             tool_id=int(tool_id),
+         )
+         record = await result.single()
+         if record:
+             tool = record["t"]
+             schema = record["s"]
+             logger.info(f"Tool found: {tool['name']}")
+             return ToolResponse(
+                 id=str(tool.id),
+                 name=tool["name"],
+                 description=tool["description"],
+                 input_schema=json.loads(schema["input_schema"]),
+                 output_schema=json.loads(schema["output_schema"]),
+             )
+         else:
+             logger.warning(f"Tool not found with id: {tool_id}")
+             raise HTTPException(status_code=404, detail="Tool not found")
+ 
+     # Update a tool
+     @app.put("/tools/{tool_id}", response_model=ToolResponse)
+     async def update_tool(tool_id: str, tool: Tool, session=Depends(get_session)):
+         logger.info(f"Updating tool with id: {tool_id}")
+         result = await session.run(
+             """
+             MATCH (t:Tool)-[:HAS_SCHEMA]->(s:ToolSchema)
+             WHERE ID(t) = $tool_id
+             SET t.name = $name,
+                 t.description = $description,
+                 s.input_schema = $input_schema,
+                 s.output_schema = $output_schema
+             WITH t, s
+             MATCH (t)-[:HAS_CODE]->(c:ToolCode)
+             SET c.code = $code
+             RETURN t, s
+             """,
+             tool_id=int(tool_id),
+             name=tool.name,
+             description=tool.description,
+             input_schema=json.dumps(tool.input_schema),
+             output_schema=json.dumps(tool.output_schema),
+             code=tool.code,
+         )
+ 
+         record = await result.single()
+         if record:
+             tool_node = record["t"]
+             schema_node = record["s"]
+             logger.info(f"Tool updated successfully: {tool_node['name']}")
+             return ToolResponse(
+                 id=str(tool_node.id),
+                 name=tool_node["name"],
+                 description=tool_node["description"],
+                 input_schema=json.loads(schema_node["input_schema"]),
+                 output_schema=json.loads(schema_node["output_schema"]),
+             )
+         else:
+             logger.warning(f"Tool not found for update with id: {tool_id}")
+             raise HTTPException(status_code=404, detail="Tool not found")
+ 
+     # Delete a tool
+     @app.delete("/tools/{tool_id}")
+     async def delete_tool(tool_id: str, session=Depends(get_session)):
+         logger.info(f"Deleting tool with id: {tool_id}")
+         result = await session.run(
+             """
+             MATCH (t:Tool)-[:HAS_SCHEMA]->(s:ToolSchema)
+             WHERE ID(t) = $tool_id
+             OPTIONAL MATCH (t)-[:HAS_CODE]->(c:ToolCode)
+             DETACH DELETE t, s, c
+             """,
+             tool_id=int(tool_id),
+         )
+         if result.consume().counters.nodes_deleted > 0:
+             logger.info(f"Tool deleted successfully: {tool_id}")
+             return {"message": "Tool deleted successfully"}
+         else:
+             logger.warning(f"Tool not found for deletion with id: {tool_id}")
+             raise HTTPException(status_code=404, detail="Tool not found")
+ 
+     # Execute single tool
+     @app.post("/execute_tool", response_model=ToolExecutionResponse)
+     async def execute_single_tool(request: ToolExecutionRequest, session=Depends(get_session)):
+         logger.info(f"Received request to execute tool: {request.tool_id}")
+         logger.debug(f"Input data: {request.input_data}")
+         try:
+             output_data = await execute_tool(request.tool_id, request.input_data, session)
+             return ToolExecutionResponse(tool_id=request.tool_id, output_data=output_data)
+         except HTTPException as he:
+             logger.error(f"HTTP error while executing tool {request.tool_id}: {str(he)}")
+             raise
+         except Exception as e:
+             logger.error(f"Unexpected error while executing tool {request.tool_id}: {str(e)}")
+             raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
+ 
+     # Execute tools sequentially
+     @app.post("/execute_tools_sequential", response_model=List[ToolExecutionResponse])
+     async def execute_tools_sequential(request: SequentialToolExecutionRequest, session=Depends(get_session)):
+         logger.info(f"Executing tools sequentially: {request.tool_ids}")
+         current_input = request.initial_input
+         results = []
+         for idx, tool_id in enumerate(request.tool_ids, start=1):
+             logger.info(f"[{idx}/{len(request.tool_ids)}] Executing tool {tool_id}")
+             try:
+                 output_data = await execute_tool(tool_id, current_input, session)
+                 results.append(ToolExecutionResponse(tool_id=tool_id, output_data=output_data))
+                 current_input = output_data  # Pass output as input to next tool
+             except HTTPException as e:
+                 logger.error(f"Error executing tool {tool_id} in sequence: {str(e)}")
+                 raise HTTPException(status_code=e.status_code, detail=f"Error at tool {tool_id}: {e.detail}")
+             except Exception as e:
+                 logger.error(f"Unexpected error executing tool {tool_id}: {str(e)}")
+                 raise HTTPException(status_code=500, detail=f"Unexpected error at tool {tool_id}: {str(e)}")
+ 
+         return results
+ 
+     # Execute tools in parallel
+     @app.post("/execute_tools_parallel", response_model=List[ToolExecutionResponse])
+     async def execute_tools_parallel(request: ParallelToolExecutionRequest, session=Depends(get_session)):
+         logger.info(f"Executing tools in parallel: {request.tool_ids}")
+         tasks = []
+         for tool_id in request.tool_ids:
+             in_data = request.input_data.get(tool_id, {})
+             tasks.append(execute_tool(tool_id, in_data, session))
+ 
+         try:
+             results = await asyncio.gather(*tasks)
+         except Exception as e:
+             logger.error(f"Error during parallel execution: {str(e)}")
+             raise HTTPException(status_code=500, detail=f"Error during parallel execution: {str(e)}")
+ 
        # Bundle results
        responses = []
        for tid, output_data in zip(request.tool_ids, results):
            responses.append(ToolExecutionResponse(tool_id=tid, output_data=output_data))
        return responses

    # Execute tools in batch (same input to all)
    @app.post("/execute_tools_batch", response_model=List[ToolExecutionResponse])
    async def execute_tools_batch(request: BatchToolExecutionRequest, session=Depends(get_session)):
        """Execute multiple tools with the same input data (fan-out)."""
        logger.info(f"Executing {len(request.tool_ids)} tools in batch")
        tasks = [execute_tool(tid, request.input_data, session) for tid in request.tool_ids]

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error during batch execution: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Batch execution failed: {str(e)}")

        responses = []
        for tid, result in zip(request.tool_ids, results):
            if isinstance(result, Exception):
                responses.append(ToolExecutionResponse(
                    tool_id=tid,
                    output_data={"error": str(result), "success": False}
                ))
            else:
                responses.append(ToolExecutionResponse(tool_id=tid, output_data=result))

        return responses

    # Execute tools as DAG
    @app.post("/execute_tools_dag")
    async def execute_tools_dag(request: DAGExecutionRequest, session=Depends(get_session)):
        """
        Execute tools as a directed acyclic graph.

        Nodes execute when all dependencies are satisfied.
        Supports merging dependency outputs into node input.
        """
        logger.info(f"Executing DAG with {len(request.nodes)} nodes")

        # Track execution state
        completed: Dict[str, Dict[str, Any]] = {}
        pending = set(request.nodes.keys())

        async def can_execute(node_id: str) -> bool:
            node = request.nodes[node_id]
            return all(dep in completed for dep in node.depends_on)

        async def execute_node(node_id: str):
            node = request.nodes[node_id]

            # Build input - start with node's input_data
            input_data = dict(node.input_data)

            # Merge outputs from dependencies if requested
            if node.merge_outputs and node.depends_on:
                for dep_id in node.depends_on:
                    dep_output = completed.get(dep_id, {})
                    input_data.update(dep_output)

            # Execute
            output = await execute_tool(node.tool_id, input_data, session)
            return output

        results = {}
        max_iterations = len(request.nodes) + 1  # Prevent infinite loops

        for _ in range(max_iterations):
            if not pending:
                break

            # Find all nodes ready to execute
            ready = [nid for nid in pending if await can_execute(nid)]

            if not ready:
                # Circular dependency or invalid DAG
                raise HTTPException(
                    status_code=400,
                    detail=f"Circular dependency or unresolvable DAG. Stuck nodes: {list(pending)}"
                )

            # Execute all ready nodes in parallel
            tasks = [(nid, execute_node(nid)) for nid in ready]
            for nid, task in tasks:
                try:
                    output = await task
                    completed[nid] = output
                    results[nid] = {"success": True, "output": output}
                except Exception as e:
                    completed[nid] = {"error": str(e)}
                    results[nid] = {"success": False, "error": str(e)}
                pending.discard(nid)

        return {
            "dag_results": results,
            "execution_order": list(completed.keys()),
            "total_nodes": len(request.nodes)
        }

    # Handle HTTP exceptions
+     @app.exception_handler(HTTPException)
+     async def http_exception_handler(request, exc):
+         logger.error(f"HTTP exception: {exc.detail}")
+         return {"detail": exc.detail}, exc.status_code
+ 
+     return app
