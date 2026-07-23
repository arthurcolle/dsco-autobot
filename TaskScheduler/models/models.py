from pydantic import BaseModel
from datetime import datetime
from typing import Union, List, Any, Optional

class ListTaskResponse(BaseModel):
    job_id: str
    func_name: str
    cron_string: str
    next_run: datetime


class ListTasksResponse(BaseModel):
    tasks: List[ListTaskResponse]


class ScheduleTask(BaseModel):
    func_name: str
    func_args: dict[str, Any]
    job_id: str
    cron_string: str
    max_duration: float
    dependencies: List[str] = []


class ModifyTask(BaseModel):
    job_id: str
    new_time: str
    new_interval: int


class RemoveTask(BaseModel):
    job_id: str


class Agent(BaseModel):
    agent_id: str
    name: str
    description: Optional[str] = None
    capabilities: List[str] = []
    status: str = "active"
    websocket_id: Optional[str] = None


class TaskExecution(BaseModel):
    pre_conditions: Optional[List[str]] = []
    post_conditions: Optional[List[str]] = []
    agent_id: str
    task_data: ScheduleTask
