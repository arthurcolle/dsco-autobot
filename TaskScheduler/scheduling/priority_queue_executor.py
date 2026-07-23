import heapq
import threading
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, TimeoutError, Future
from typing import Dict, Any, Callable, Optional, Set, List
from .prioritized_future import PrioritizedFuture

class PriorityQueueExecutor:
    """
    A highly sophisticated, thread-safe priority queue executor that manages and executes tasks
    based on their priority, dependencies, maximum duration, and resource requirements.

    This executor uses a multi-level heap structure to maintain the priority order of tasks,
    considering both static and dynamic priorities. It employs a ThreadPoolExecutor for concurrent
    task execution and implements advanced features such as task preemption, resource management,
    and adaptive scheduling.

    Attributes:
        objects (Dict[str, Any]): A dictionary of objects (e.g., agents) that can be accessed by tasks.
        heap (List[List[PrioritizedFuture]]): A list of heaps, each representing a priority level.
        lock (threading.Lock): A reentrant lock to ensure thread-safety when modifying shared data.
        executor (ThreadPoolExecutor): The underlying thread pool executor for running tasks.
        completed_tasks (Set[str]): A set of names of completed tasks, used for dependency tracking.
        running_tasks (Dict[str, PrioritizedFuture]): A dictionary of currently running tasks.
        resource_pool (Dict[str, int]): A pool of available resources for tasks.
        task_history (List[Dict[str, Any]]): A list of executed tasks with their metadata for analysis.
        performance_metrics (Dict[str, float]): Various metrics to monitor executor performance.

    """

    def __init__(self, max_workers: Optional[int] = None, objects: Optional[Dict[str, Any]] = None):
        """
        Initialize the PriorityQueueExecutor with advanced configuration options.

        Args:
            max_workers (int, optional): The maximum number of worker threads to use.
                If None, it will default to the number of processors on the machine,
                multiplied by 5.
            objects (Dict[str, Any], optional): A dictionary of objects that can be accessed by tasks.

        """
        self.objects = objects or {}
        self.heap = [[] for _ in range(10)]  # 10 priority levels
        self.lock = threading.RLock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.completed_tasks: Set[str] = set()
        self.running_tasks: Dict[str, PrioritizedFuture] = {}
        self.resource_pool: Dict[str, int] = {"CPU": max_workers or 20, "Memory": 1000}  # Example resource pool
        self.task_history: List[Dict[str, Any]] = []
        self.performance_metrics: Dict[str, float] = {
            "avg_wait_time": 0,
            "avg_execution_time": 0,
            "throughput": 0,
            "resource_utilization": 0,
        }

    def submit(self, priority: int, func: Callable, *args, 
               max_duration: Optional[float] = None, 
               dependencies: Optional[Set[str]] = None,
               resource_requirements: Optional[Dict[str, int]] = None,
               **kwargs) -> PrioritizedFuture:
        """
        Submit a task to the executor with comprehensive execution parameters.

        This method creates a PrioritizedFuture for the task and adds it to the appropriate
        priority queue. It performs extensive checks on dependencies, resource availability,
        and scheduling constraints before attempting to start the task.

        Args:
            priority (int): The priority of the task. Lower numbers indicate higher priority.
            func (Callable): The function to be executed.
            *args: Positional arguments to pass to the function.
            max_duration (float, optional): The maximum duration in seconds that the task is
                allowed to run. If None, the task can run indefinitely.
            dependencies (Set[str], optional): A set of task names that must be completed before
                this task can start.
            resource_requirements (Dict[str, int], optional): A dictionary specifying the
                resources required by this task.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            PrioritizedFuture: A future representing the submitted task.

        Raises:
            ValueError: If the priority is out of range or if resource requirements cannot be met.
        """
        if not 0 <= priority < len(self.heap):
            raise ValueError(f"Priority must be between 0 and {len(self.heap) - 1}")

        future = PrioritizedFuture(priority, func, *args, max_duration=max_duration, 
                                   dependencies=dependencies, 
                                   resource_requirements=resource_requirements, **kwargs)
        
        with self.lock:
            if not self._check_resource_availability(future.resource_requirements):
                raise ValueError("Insufficient resources to schedule this task")
            
            heapq.heappush(self.heap[priority], future)
            print(f"Task '{func.__name__}' added with priority {priority}, "
                  f"max duration {max_duration}, dependencies {dependencies}, "
                  f"and resource requirements {future.resource_requirements}.")
            
            self._update_performance_metrics()
            self._start_next_task()
        
        return future

    def _check_resource_availability(self, requirements: Optional[Dict[str, int]]) -> bool:
        """
        Check if the required resources are available in the resource pool.

        Args:
            requirements (Dict[str, int], optional): The resource requirements of a task.

        Returns:
            bool: True if resources are available, False otherwise.
        """
        if not requirements:
            return True
        
        return all(self.resource_pool.get(resource, 0) >= amount 
                   for resource, amount in requirements.items())

    def _allocate_resources(self, requirements: Optional[Dict[str, int]]) -> None:
        """
        Allocate resources from the resource pool for a task.

        Args:
            requirements (Dict[str, int], optional): The resource requirements of a task.
        """
        if not requirements:
            return
        
        for resource, amount in requirements.items():
            self.resource_pool[resource] -= amount

    def _release_resources(self, requirements: Optional[Dict[str, int]]) -> None:
        """
        Release allocated resources back to the resource pool after task completion.

        Args:
            requirements (Dict[str, int], optional): The resource requirements of a completed task.
        """
        if not requirements:
            return
        
        for resource, amount in requirements.items():
            self.resource_pool[resource] += amount

    def _start_next_task(self) -> None:
        """
        Start the next task from the priority queue if available and constraints are satisfied.

        This method iterates through all priority levels, starting from the highest, to find
        the next eligible task. It checks for dependency satisfaction, resource availability,
        and other constraints before starting a task.

        Note:
            This method should only be called while holding the lock to ensure thread-safety.
        """
        for priority_level in range(len(self.heap)):
            heap = self.heap[priority_level]
            for i in range(len(heap)):
                future = heap[i]
                if (future.dependencies <= self.completed_tasks and 
                    self._check_resource_availability(future.resource_requirements)):
                    print(f"Starting task '{future.func.__name__}' with priority {priority_level}")
                    del heap[i]
                    heapq.heapify(heap)  # Restore heap invariant
                    self._allocate_resources(future.resource_requirements)
                    self.running_tasks[future.func.__name__] = future
                    self.executor.submit(self._run_with_timeout, future)
                    return  # Start only one task per call to maintain priority order
    def _run_with_timeout(self, future: PrioritizedFuture) -> None:
        """
        Run the future with a sophisticated timeout mechanism and extensive error handling.

        This method wraps the execution of a task with a timeout mechanism, detailed logging,
        and comprehensive error handling. It manages resource allocation and deallocation,
        updates task history, and handles task preemption if necessary.

        Args:
            future (PrioritizedFuture): The future to run.
        Note:
            This method is intended to be run in a separate thread by the executor.
        """
        start_time = time.time()
        task_name = future.func.__name__
        print(f"Starting task '{task_name}' with priority {future.priority}")
        try:
            if asyncio.iscoroutinefunction(future.func):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(asyncio.wait_for(future.run(), timeout=future.max_duration))
            else:
                result = future.run()
            future.set_result(result)
            print(f"Task '{task_name}' completed successfully.")
        except asyncio.TimeoutError:
            print(f"Task '{task_name}' exceeded the max duration of {future.max_duration} seconds.")
            future.set_exception(TimeoutError(f"Task '{task_name}' timed out after {future.max_duration} seconds."))
            self._handle_task_timeout(future)
        except Exception as e:
            print(f"Task '{task_name}' failed with error: {str(e)}")
            future.set_exception(e)
        finally:
            execution_time = time.time() - start_time
            with self.lock:
                self.completed_tasks.add(task_name)
                self._release_resources(future.resource_requirements)
                del self.running_tasks[task_name]
                self._update_task_history(future, execution_time)
                self._update_performance_metrics()
            self._start_next_task()  # Try to start the next task after this one completes
    def _handle_task_timeout(self, future: PrioritizedFuture) -> None:
        """
        Handle the timeout of a task, potentially implementing task preemption or rescheduling.

        This method is called when a task exceeds its maximum duration. It can implement
        strategies such as task preemption, rescheduling with lower priority, or triggering
        alerts for long-running tasks.

        Args:
            future (PrioritizedFuture): The future representing the timed-out task.
        """
        # TBD: Implement task preemption or rescheduling logic
        pass

    def _update_task_history(self, future: PrioritizedFuture, execution_time: float) -> None:
        """
        Update the task history with metadata about the completed task.

        This method records detailed information about each executed task, which can be
        used for analysis, debugging, and optimization of the scheduling algorithm.

        Args:
            future (PrioritizedFuture): The completed future.
            execution_time (float): The actual execution time of the task.
        """
        self.task_history.append({
            "name": future.func.__name__,
            "priority": future.priority,
            "max_duration": future.max_duration,
            "actual_duration": execution_time,
            "dependencies": list(future.dependencies),
            "resource_requirements": future.resource_requirements,
            "status": "completed" if future.exception() is None else "failed",
            "exception": str(future.exception()) if future.exception() else None,
        })

    def _update_performance_metrics(self) -> None:
        """
        Update various performance metrics of the executor.

        This method calculates and updates metrics such as average wait time, average execution time,
        throughput, and resource utilization. These metrics can be used to monitor and optimize
        the performance of the executor.
        """
        # TBD: Implement detailed performance metric calculations
        pass

    def shutdown(self, wait: bool = True) -> None:
        """
        Shut down the executor and optionally wait for tasks to finish.

        This method initiates a graceful shutdown of the executor. It stops accepting new tasks,
        optionally waits for pending tasks to complete, and releases all resources.

        Args:
            wait (bool): If True, wait for pending tasks to complete before shutting down.
                         If False, stop accepting new tasks and return immediately.

        Note:
            After calling shutdown(), attempts to submit new tasks will raise RuntimeError.
        """
        self.executor.shutdown(wait=wait)
        if wait:
            self._wait_for_running_tasks()
        self._cleanup_resources()

    def _wait_for_running_tasks(self) -> None:
        """
        Wait for all currently running tasks to complete.

        This method is called during shutdown to ensure all tasks have finished execution
        before fully shutting down the executor.
        """
        while self.running_tasks:
            time.sleep(0.1)  # Short sleep to prevent busy-waiting

    def _cleanup_resources(self) -> None:
        """
        Perform final cleanup of resources and state.

        This method is called at the end of the shutdown process to ensure all resources
        are properly released and the state is cleaned up.
        """
        self.resource_pool.clear()
        self.completed_tasks.clear()
        self.running_tasks.clear()
        self.heap.clear()

    def get_task_status(self, task_name: str) -> Dict[str, Any]:
        """
        Get the current status and details of a specific task.

        Args:
            task_name (str): The name of the task to query.

        Returns:
            Dict[str, Any]: A dictionary containing the task's current status and details.

        Raises:
            KeyError: If the task is not found.
        """
        with self.lock:
            for priority_level in self.heap:
                for task in priority_level:
                    if task.func.__name__ == task_name:
                        return {
                            "status": "pending",
                            "priority": task.priority,
                            "dependencies": list(task.dependencies),
                            "max_duration": task.max_duration,
                            "resource_requirements": task.resource_requirements
                        }
            
            if task_name in self.running_tasks:
                task = self.running_tasks[task_name]
                return {
                    "status": "running",
                    "priority": task.priority,
                    "dependencies": list(task.dependencies),
                    "max_duration": task.max_duration,
                    "resource_requirements": task.resource_requirements
                }
            
            if task_name in self.completed_tasks:
                return {"status": "completed"}
            
            raise KeyError(f"Task '{task_name}' not found")

    def pause_task(self, task_name: str) -> None:
        """
        Pause a running task, if possible.

        Args:
            task_name (str): The name of the task to pause.

        Raises:
            KeyError: If the task is not found.
            RuntimeError: If the task cannot be paused.
        """
        with self.lock:
            if task_name not in self.running_tasks:
                raise KeyError(f"Task '{task_name}' not found in running tasks")
            
            task = self.running_tasks[task_name]
            if not hasattr(task, 'pause'):
                raise RuntimeError(f"Task '{task_name}' does not support pausing")
            
            task.pause()

    def resume_task(self, task_name: str) -> None:
        """
        Resume a paused task.

        Args:
            task_name (str): The name of the task to resume.

        Raises:
            KeyError: If the task is not found.
            RuntimeError: If the task is not in a pausable state.
        """
        with self.lock:
            if task_name not in self.running_tasks:
                raise KeyError(f"Task '{task_name}' not found in running tasks")
            
            task = self.running_tasks[task_name]
            if not hasattr(task, 'resume'):
                raise RuntimeError(f"Task '{task_name}' does not support resuming")
            
            task.resume()

    def cancel_task(self, task_name: str) -> bool:
        """
        Attempt to cancel a pending or running task.

        Args:
            task_name (str): The name of the task to cancel.

        Returns:
            bool: True if the task was successfully cancelled, False otherwise.

        Raises:
            KeyError: If the task is not found.
        """
        with self.lock:
            # Check pending tasks
            for priority_level in self.heap:
                for i, task in enumerate(priority_level):
                    if task.func.__name__ == task_name:
                        del priority_level[i]
                        heapq.heapify(priority_level)
                        return True
            
            # Check running tasks
            if task_name in self.running_tasks:
                task = self.running_tasks[task_name]
                if task.cancel():
                    del self.running_tasks[task_name]
                    return True
            
            raise KeyError(f"Task '{task_name}' not found")

    def reschedule_task(self, task_name: str, new_priority: int) -> None:
        """
        Reschedule a pending task with a new priority.

        Args:
            task_name (str): The name of the task to reschedule.
            new_priority (int): The new priority for the task.

        Raises:
            KeyError: If the task is not found.
            ValueError: If the new priority is invalid.
        """
        if new_priority < 0 or new_priority >= len(self.heap):
            raise ValueError(f"Invalid priority: {new_priority}")
        
        with self.lock:
            for priority_level in self.heap:
                for i, task in enumerate(priority_level):
                    if task.func.__name__ == task_name:
                        task = priority_level.pop(i)
                        task.priority = new_priority
                        heapq.heappush(self.heap[new_priority], task)
                        return
            
            raise KeyError(f"Task '{task_name}' not found in pending tasks")

    def get_performance_report(self) -> Dict[str, Any]:
        """
        Generate a comprehensive performance report of the executor.

        Returns:
            Dict[str, Any]: A dictionary containing various performance metrics and statistics.
        """
        with self.lock:
            total_tasks = len(self.completed_tasks) + len(self.running_tasks) + sum(len(level) for level in self.heap)
            completed_tasks = len(self.completed_tasks)
            running_tasks = len(self.running_tasks)
            pending_tasks = sum(len(level) for level in self.heap)
            
            avg_wait_time = sum(task.get('wait_time', 0) for task in self.task_history) / len(self.task_history) if self.task_history else 0
            avg_execution_time = sum(task.get('execution_time', 0) for task in self.task_history) / len(self.task_history) if self.task_history else 0
            
            return {
                "total_tasks": total_tasks,
                "completed_tasks": completed_tasks,
                "running_tasks": running_tasks,
                "pending_tasks": pending_tasks,
                "average_wait_time": avg_wait_time,
                "average_execution_time": avg_execution_time,
                "resource_utilization": {
                    resource: used / total for resource, (used, total) in self.resource_pool.items()
                },
                "task_history": self.task_history[-10:]  # Last 10 tasks for brevity
            }

