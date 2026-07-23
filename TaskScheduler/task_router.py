from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Any, Optional, Dict
from datetime import datetime
from croniter import croniter
from scheduling.flexible_scheduler import get_flexible_scheduler
from models.task_graph import TaskGraph
from ws_endpoint import notify_agent

task_scheduling_and_execution = APIRouter()

class ScheduleTask(BaseModel):
    name: str
    description: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    code: str
    func_name: str
    func_args: dict[str, Any]
    job_id: Optional[str] = None
    cron_string: str
    max_duration: float
    dependencies: List[str] = []
    priority: int = 0
    agent_id: str
    pre_conditions: Optional[List[str]] = []
    post_conditions: Optional[List[str]] = []

class ModifyTask(BaseModel):
    job_id: str
    new_cron_string: Optional[str] = None
    new_priority: Optional[int] = None

class RemoveTask(BaseModel):
    job_id: str

class ListTaskResponse(BaseModel):
    job_id: str
    func_name: str
    cron_string: str
    next_run: datetime
    priority: int

class ListTasksResponse(BaseModel):
    tasks: List[ListTaskResponse]

scheduler = get_flexible_scheduler()
task_graph = TaskGraph()

@task_scheduling_and_execution.post("/tasks/schedule")
async def schedule_task(task: ScheduleTask):
    job_id = task.job_id or f"{task.name}_{datetime.now().timestamp()}"

    async def task_func():
        try:
            # Check pre-conditions if any
            if task.pre_conditions:
                for condition in task.pre_conditions:
                    # Implement pre-condition checking logic
                    pass

            # Notify the agent to execute the task
            await notify_agent(task.agent_id, {
                "job_id": job_id,
                "func_name": task.func_name,
                "func_args": task.func_args,
                "pre_conditions": task.pre_conditions,
                "post_conditions": task.post_conditions
            })

            # Note: The actual execution happens on the agent side
            # The agent will send back results via WebSocket

            # Check post-conditions if any
            if task.post_conditions:
                for condition in task.post_conditions:
                    # Implement post-condition checking logic
                    pass

        except Exception as e:
            print(f"Error executing task {task.name}: {str(e)}")
            raise

    scheduler.schedule_task(
        job_id,
        task.cron_string,
        task_func,
        priority=task.priority,
        max_duration=task.max_duration,
        dependencies=task.dependencies
    )

    # Create task in TaskGraph
    task_data = {
        "name": task.name,
        "description": task.description,
        "status": "scheduled",
        "priority": task.priority,
        "agent_id": task.agent_id,
        "created_at": datetime.now().isoformat(),
        "metadata": {
            "cron_string": task.cron_string,
            "max_duration": task.max_duration,
            "dependencies": task.dependencies,
            "pre_conditions": task.pre_conditions,
            "post_conditions": task.post_conditions,
            "input_schema": task.input_schema,
            "output_schema": task.output_schema,
            "code": task.code,
            "func_name": task.func_name,
            "func_args": task.func_args
        }
    }
    task_graph.create_task(task_data)

    return {"job_id": job_id}

@task_scheduling_and_execution.put("/tasks/modify")
async def modify_task(task: ModifyTask):
    try:
        if task.new_cron_string:
            scheduler.modify_task(task.job_id, cron_expression=task.new_cron_string)
        if task.new_priority is not None:
            scheduler.modify_task(task.job_id, priority=task.new_priority)
        
        # Update task in TaskGraph
        task_data = {}
        if task.new_cron_string:
            task_data["metadata"] = {"cron_string": task.new_cron_string}
        if task.new_priority is not None:
            task_data["priority"] = task.new_priority
        task_graph.update_task(task.job_id, task_data)

        return {"message": "Task modified successfully"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@task_scheduling_and_execution.get("/tasks/list", response_model=ListTasksResponse)
async def list_tasks():
    tasks = scheduler.get_tasks()
    response_tasks = []
    for job_id, task_info in tasks.items():
        cron_string = task_info['cron_expression']
        next_run = croniter(cron_string, datetime.now()).get_next(datetime)
        response_tasks.append(ListTaskResponse(
            job_id=job_id,
            func_name=task_info['task_func'].__name__,
            cron_string=cron_string,
            next_run=next_run,
            priority=scheduler.task_priorities.get(job_id, 0)
        ))
    return ListTasksResponse(tasks=response_tasks)

@task_scheduling_and_execution.delete("/tasks/remove")
async def remove_task(task: RemoveTask):
    try:
        scheduler.cancel_task(task.job_id)
        # Remove task from TaskGraph
        task_graph.delete_task(task.job_id)
        return {"message": "Task removed successfully"}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@task_scheduling_and_execution.get("/tasks/{task_id}")
async def get_task(task_id: str):
    task = task_graph.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

@task_scheduling_and_execution.get("/tasks/search")
async def search_tasks(query: str):
    tasks = task_graph.find_tasks_by_text(query)
    return {"tasks": tasks}

@task_scheduling_and_execution.on_event("shutdown")
def shutdown_event():
    scheduler.stop()
