import json
import uuid
import time
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from enum import Enum
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Callable
from typing import Union


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskResult:
    def __init__(self, task_id: str, status: TaskStatus, result: Any = None, error: Optional[str] = None):
        self.task_id = task_id
        self.status = status
        self.result = result
        self.error = error
        self.created_at = time.time()
        self.completed_at: Optional[float] = None
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at,
            "completed_at": self.completed_at
        }
        
    def to_json(self) -> str:
        return json.dumps(self.to_dict())


class SwarmTask:
    def __init__(self, max_workers: int = 4, task_timeout: int = 300):
        self.max_workers = max_workers
        self.task_timeout = task_timeout
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.results: Dict[str, TaskResult] = {}
        self._lock = threading.RLock()
        
    def submit(self, func: Callable, *args, task_id: Optional[str] = None, **kwargs) -> str:
        """Submit a task for execution."""
        if task_id is None:
            task_id = str(uuid.uuid4())
            
        with self._lock:
            if task_id in self.tasks:
                raise ValueError(f"Task with ID {task_id} already exists")
                
            future = self.executor.submit(self._execute_task, task_id, func, *args, **kwargs)
            
            self.tasks[task_id] = {
                "future": future,
                "func": func,
                "args": args,
                "kwargs": kwargs,
                "submitted_at": time.time()
            }
            
            self.results[task_id] = TaskResult(task_id, TaskStatus.PENDING)
            
        return task_id
        
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a task synchronously and return the result."""
        task_id = self.submit(func, *args, **kwargs)
        result = self.get_result(task_id, wait=True)
        
        if result.status == TaskStatus.COMPLETED:
            return result.result
        elif result.status == TaskStatus.FAILED:
            raise Exception(f"Task failed: {result.error}")
        else:
            raise Exception(f"Task ended with unexpected status: {result.status}")
            
    def get_result(self, task_id: str, wait: bool = False, timeout: Optional[int] = None) -> TaskResult:
        """Get the result of a task."""
        with self._lock:
            if task_id not in self.tasks:
                raise ValueError(f"Task with ID {task_id} not found")
                
            task = self.tasks[task_id]
            future: Future = task["future"]
            
        if wait:
            try:
                future.result(timeout=timeout or self.task_timeout)
            except Exception:
                pass  # Result will be updated by _execute_task
                
        with self._lock:
            return self.results[task_id]
            
    def _execute_task(self, task_id: str, func: Callable, *args, **kwargs) -> Any:
        """Internal method to execute a task and update its result."""
        try:
            with self._lock:
                self.results[task_id].status = TaskStatus.RUNNING
                
            result = func(*args, **kwargs)
            
            with self._lock:
                task_result = self.results[task_id]
                task_result.status = TaskStatus.COMPLETED
                task_result.result = result
                task_result.completed_at = time.time()
                
            return result
            
        except Exception as e:
            with self._lock:
                task_result = self.results[task_id]
                task_result.status = TaskStatus.FAILED
                task_result.error = str(e)
                task_result.completed_at = time.time()
                
            raise
            
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending or running task."""
        with self._lock:
            if task_id not in self.tasks:
                return False
                
            future: Future = self.tasks[task_id]["future"]
            if future.cancel():
                self.results[task_id].status = TaskStatus.CANCELLED
                self.results[task_id].completed_at = time.time()
                return True
                
        return False
        
    def get_task_status(self, task_id: str) -> TaskStatus:
        """Get the status of a task."""
        with self._lock:
            if task_id not in self.results:
                raise ValueError(f"Task with ID {task_id} not found")
            return self.results[task_id].status
            
    def list_tasks(self, status_filter: Optional[TaskStatus] = None) -> List[str]:
        """List all task IDs, optionally filtered by status."""
        with self._lock:
            if status_filter is None:
                return list(self.tasks.keys())
            return [task_id for task_id, result in self.results.items() 
                   if result.status == status_filter]
                   
    def cleanup_completed_tasks(self, max_age: int = 3600) -> int:
        """Remove completed tasks older than max_age seconds."""
        current_time = time.time()
        removed_count = 0
        
        with self._lock:
            tasks_to_remove = []
            for task_id, result in self.results.items():
                if (result.status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED] 
                    and result.completed_at is not None
                    and current_time - result.completed_at > max_age):
                    tasks_to_remove.append(task_id)
                    
            for task_id in tasks_to_remove:
                del self.tasks[task_id]
                del self.results[task_id]
                removed_count += 1
                
        return removed_count
        
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the task executor."""
        self.executor.shutdown(wait=wait)


def create_task_manager(max_workers: int = 4, task_timeout: int = 300) -> SwarmTask:
    """Create a new SwarmTask instance."""
    return SwarmTask(max_workers=max_workers, task_timeout=task_timeout)


def batch_submit_tasks(swarm: SwarmTask, tasks: List[Dict[str, Any]]) -> List[str]:
    """Submit multiple tasks at once."""
    task_ids = []
    for task in tasks:
        func = task["func"]
        args = task.get("args", [])
        kwargs = task.get("kwargs", {})
        task_id = task.get("task_id")
        
        submitted_id = swarm.submit(func, *args, task_id=task_id, **kwargs)
        task_ids.append(submitted_id)
        
    return task_ids


def wait_for_all_tasks(swarm: SwarmTask, task_ids: List[str], timeout: Optional[int] = None) -> List[TaskResult]:
    """Wait for all tasks to complete and return their results."""
    results = []
    for task_id in task_ids:
        result = swarm.get_result(task_id, wait=True, timeout=timeout)
        results.append(result)
    return results
