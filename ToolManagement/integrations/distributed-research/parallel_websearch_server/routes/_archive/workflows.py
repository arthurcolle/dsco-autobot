"""
Workflow API Routes (DAG-based task execution)

Provides endpoints for defining and executing complex task workflows with dependencies.
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
import time
import uuid
import logging

from parallel_websearch_server.orchestration.dag import (
    TaskDAG,
    CyclicDependencyError,
    DAGExecutionError
)
from parallel_websearch_server.orchestration.dag_executor import DAGExecutor
from parallel_websearch_server.dashboard import get_progress_tracker

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1beta/workflows", tags=["workflows"])

# In-memory DAG storage (in production, use persistent storage)
_dags: Dict[str, TaskDAG] = {}
_executors: Dict[str, DAGExecutor] = {}
_execution_results: Dict[str, Dict] = {}


# Request/Response Models

class WorkflowTask(BaseModel):
    """Task definition for workflow"""
    task_id: str = Field(..., description="Unique task identifier within workflow")
    input_data: str = Field(..., description="Task input/query")
    dependencies: List[str] = Field(default_factory=list, description="List of task_ids this depends on")
    processor: Optional[str] = Field(None, description="Target processor tier")
    metadata: Dict = Field(default_factory=dict, description="Additional metadata")


class CreateWorkflowRequest(BaseModel):
    """Request to create workflow"""
    workflow_id: Optional[str] = Field(None, description="Optional workflow ID (auto-generated if not provided)")
    tasks: List[WorkflowTask] = Field(..., description="List of tasks in workflow")


class ExecuteWorkflowRequest(BaseModel):
    """Request to execute workflow"""
    max_parallel: int = Field(10, ge=1, le=100, description="Max parallel task execution")
    task_timeout: float = Field(3600.0, ge=1, description="Timeout per task (seconds)")
    execution_mode: str = Field("level", description="Execution mode: 'level' or 'adaptive'")


# Endpoints

@router.post("/create")
async def create_workflow(request: CreateWorkflowRequest):
    """
    Create a new workflow (DAG).

    Defines a set of tasks with dependencies.
    Validates DAG structure (no cycles, valid dependencies).

    Returns:
        workflow_id and validation results
    """
    workflow_id = request.workflow_id or f"wf_{uuid.uuid4().hex[:12]}"

    if workflow_id in _dags:
        raise HTTPException(status_code=400, detail=f"Workflow {workflow_id} already exists")

    # Create DAG
    dag = TaskDAG(dag_id=workflow_id)

    try:
        # Add tasks in dependency order
        for task in request.tasks:
            dag.add_task(
                task_id=task.task_id,
                input_data=task.input_data,
                dependencies=task.dependencies,
                processor=task.processor,
                metadata=task.metadata
            )

        # Validate DAG
        dag.validate()

        # Get execution plan
        levels = dag.topological_sort()

        # Store DAG
        _dags[workflow_id] = dag

        logger.info(f"Created workflow {workflow_id} with {len(request.tasks)} tasks")

        return {
            "workflow_id": workflow_id,
            "validated": True,
            "total_tasks": len(request.tasks),
            "execution_levels": len(levels),
            "max_parallelism": max(len(level) for level in levels),
            "execution_plan": levels
        }

    except CyclicDependencyError as e:
        raise HTTPException(status_code=400, detail=f"Cyclic dependency detected: {str(e)}")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{workflow_id}/execute")
async def execute_workflow(
    workflow_id: str,
    request: ExecuteWorkflowRequest,
    background_tasks: BackgroundTasks
):
    """
    Execute a workflow.

    Executes all tasks according to their dependencies.
    Tasks at the same dependency level run in parallel.

    Args:
        workflow_id: Workflow to execute
        request: Execution configuration

    Returns:
        Execution started confirmation with execution_id
    """
    if workflow_id not in _dags:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    dag = _dags[workflow_id]
    executor = DAGExecutor(
        max_parallel=request.max_parallel,
        task_timeout=request.task_timeout
    )

    execution_id = f"exec_{uuid.uuid4().hex[:12]}"
    _executors[execution_id] = executor

    # Progress tracker
    tracker = get_progress_tracker()

    async def task_executor_impl(task_id: str, input_data: str, processor: Optional[str]):
        """Actual task execution implementation"""
        # This is a placeholder - in production, integrate with actual task execution system
        # For now, simulate task execution
        logger.info(f"Executing task {task_id} with processor {processor}")

        # Register task with progress tracker
        await tracker.register_task(
            task_id=task_id,
            scope=workflow_id,
            metadata={"workflow_id": workflow_id, "execution_id": execution_id}
        )

        # Update to running
        await tracker.update_task_status(task_id, "running")

        # Simulate work (in production, call actual task API)
        await asyncio.sleep(1)

        # Mark complete
        await tracker.update_task_status(
            task_id,
            "completed",
            metadata={"result": "Task completed successfully"}
        )

        return {"status": "completed", "output": f"Result for {task_id}"}

    async def progress_callback_impl(task_id: str, status: str, data: Dict):
        """Progress callback"""
        logger.debug(f"Task {task_id} progress: {status} - {data}")

    async def execute_dag():
        """Background DAG execution"""
        try:
            if request.execution_mode == "adaptive":
                result = await executor.execute_adaptive(
                    dag=dag,
                    task_executor=task_executor_impl,
                    progress_callback=progress_callback_impl
                )
            else:
                result = await executor.execute(
                    dag=dag,
                    task_executor=task_executor_impl,
                    progress_callback=progress_callback_impl
                )

            _execution_results[execution_id] = result
            logger.info(f"Workflow {workflow_id} execution {execution_id} completed")

        except Exception as e:
            logger.error(f"Workflow execution {execution_id} failed: {e}", exc_info=True)
            _execution_results[execution_id] = {
                "error": str(e),
                "success": False
            }

    # Start execution in background
    background_tasks.add_task(execute_dag)

    return {
        "workflow_id": workflow_id,
        "execution_id": execution_id,
        "status": "started",
        "total_tasks": len(dag.nodes),
        "execution_mode": request.execution_mode
    }


@router.get("/{workflow_id}")
async def get_workflow(workflow_id: str):
    """
    Get workflow definition and current state.

    Returns:
        Workflow metadata, tasks, and execution status
    """
    if workflow_id not in _dags:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    dag = _dags[workflow_id]

    return {
        "workflow_id": workflow_id,
        "total_tasks": len(dag.nodes),
        "statistics": dag.get_statistics(),
        "is_complete": dag.is_complete(),
        "has_failures": dag.has_failures(),
        "tasks": [
            {
                "task_id": node.task_id,
                "status": node.status,
                "dependencies": list(node.dependencies),
                "dependents": list(node.dependents),
                "execution_time": node.execution_time,
                "error": node.error
            }
            for node in dag.nodes.values()
        ]
    }


@router.get("/{workflow_id}/graph")
async def get_workflow_graph(workflow_id: str):
    """
    Get workflow graph visualization data.

    Returns graph structure suitable for visualization tools (D3.js, vis.js, etc.)
    """
    if workflow_id not in _dags:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    dag = _dags[workflow_id]
    return dag.get_graph_metadata()


@router.get("/execution/{execution_id}")
async def get_execution_result(execution_id: str):
    """
    Get workflow execution result.

    Returns:
        Execution summary and statistics
    """
    if execution_id not in _execution_results:
        # Check if execution is still running
        if execution_id in _executors:
            return {
                "execution_id": execution_id,
                "status": "running",
                "completed": False
            }

        raise HTTPException(status_code=404, detail=f"Execution {execution_id} not found")

    result = _execution_results[execution_id]

    return {
        "execution_id": execution_id,
        "status": "completed",
        "completed": True,
        **result
    }


@router.delete("/{workflow_id}")
async def delete_workflow(workflow_id: str):
    """
    Delete a workflow.

    Removes workflow definition and execution history.
    """
    if workflow_id not in _dags:
        raise HTTPException(status_code=404, detail=f"Workflow {workflow_id} not found")

    del _dags[workflow_id]

    # Clean up execution results
    execution_ids_to_remove = [
        exec_id for exec_id, result in _execution_results.items()
        if result.get("dag_id") == workflow_id
    ]

    for exec_id in execution_ids_to_remove:
        del _execution_results[exec_id]
        if exec_id in _executors:
            del _executors[exec_id]

    return {
        "workflow_id": workflow_id,
        "deleted": True
    }


@router.get("/")
async def list_workflows():
    """
    List all workflows.

    Returns:
        List of workflow summaries
    """
    workflows = []

    for workflow_id, dag in _dags.items():
        workflows.append({
            "workflow_id": workflow_id,
            "total_tasks": len(dag.nodes),
            "is_complete": dag.is_complete(),
            "has_failures": dag.has_failures(),
            "statistics": dag.get_statistics()
        })

    return {
        "workflows": workflows,
        "total": len(workflows)
    }


# Import asyncio for sleep
import asyncio
