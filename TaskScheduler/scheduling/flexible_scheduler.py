import asyncio
import logging
import threading
import time
import redis.asyncio as redis
from datetime import datetime, timedelta
from croniter import croniter
from typing import Dict, Any, Callable, Optional, List, Set
from .priority_queue_executor import PriorityQueueExecutor
from pydantic import create_model
import networkx as nx
from prometheus_client import Counter, Histogram, Gauge
import json
import hashlib

class FlexibleScheduler:
    def __init__(self, max_workers: int = 4, redis_url: str = "redis://localhost:6379"):
        self.objects: Dict[str, Any] = {}
        self.executor = PriorityQueueExecutor(max_workers=max_workers, objects=self.objects)
        self.lock = threading.Lock()
        self.scheduled_tasks: Dict[str, Dict[str, Any]] = {}
        self.stopped = False
        self.event_loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.task_priorities: Dict[str, int] = {}
        
        # Redis for distributed locking and caching
        self.redis = redis.from_url(redis_url, decode_responses=True)
        
        # Task dependency graph
        self.task_graph = nx.DiGraph()
        
        # Metrics
        self.task_executions = Counter('task_executions_total', 'Total number of task executions', ['task_name', 'status'])
        self.task_duration = Histogram('task_duration_seconds', 'Task execution duration', ['task_name'])
        self.active_tasks = Gauge('active_tasks', 'Number of currently running tasks')
        
        # Task versioning
        self.task_versions: Dict[str, int] = {}
        
        # Result cache with TTL
        self.result_cache: Dict[str, Any] = {}
        self.cache_ttl: Dict[str, datetime] = {}
        
        # Retry configuration
        self.max_retries: Dict[str, int] = {}
        self.retry_delays: Dict[str, List[float]] = {}

    def start(self):
        """Start the entire scheduler."""
        self.stopped = False
        self.thread.start()
        logging.info("FlexibleScheduler started.")

    def stop(self):
        """Stop the entire scheduler."""
        self.stopped = True
        self.event_loop.call_soon_threadsafe(self.event_loop.stop)
        self.thread.join()
        self.executor.shutdown()
        logging.info("FlexibleScheduler stopped.")

    def _run_event_loop(self):
        """Run the event loop in a separate thread."""
        asyncio.set_event_loop(self.event_loop)
        self.event_loop.run_forever()

    async def run_pending(self):
        """Check for pending tasks and run them if it's time."""
        now = datetime.now()
        tasks_to_run = []
        with self.lock:
            for name, task_info in self.scheduled_tasks.items():
                cron_expression = task_info['cron_expression']
                last_run = task_info.get('last_run', datetime.min)
                iter_cron = croniter(cron_expression, last_run)
                next_run = iter_cron.get_next(datetime)
                
                if next_run <= now:
                    tasks_to_run.append((name, task_info))
                    task_info['last_run'] = now

        for name, task_info in tasks_to_run:
            print(f"Executing task '{name}' at {datetime.now()}")
            await self._execute_task(name, task_info)

    async def _execute_task(self, name: str, task_info: Dict[str, Any], retry_count: int = 0):
        """Execute a task asynchronously with retries and metrics."""
        task_func = task_info['task_func']
        args = task_info['args']
        kwargs = task_info['kwargs']
        input_schema = task_info.get('input_schema')
        priority = self.task_priorities.get(name, 0)
        version = self.task_versions.get(name, 1)
        
        # Check dependencies
        if not await self._check_dependencies(name):
            raise ValueError(f"Dependencies not met for task {name}")
            
        # Generate cache key
        cache_key = self._generate_cache_key(name, args, kwargs, version)
        
        # Check cache
        cached_result = await self._get_cached_result(cache_key)
        if cached_result is not None:
            return cached_result
            
        # Acquire distributed lock
        lock_key = f"task_lock:{name}"
        async with self.redis.lock(lock_key, timeout=300):
            self.active_tasks.inc()
            start_time = time.time()
            
            try:
                print(f"Task function: {task_func}")
                print(f"Arguments: {args}")
                print(f"Keyword arguments: {kwargs}")
                print(f"Input schema: {input_schema}")
                print(f"Priority: {priority}")
                
                # Create a Pydantic model from the input schema
                if input_schema:
                    InputModel = create_model(f"{name}Input", **input_schema)
                    # Validate and create the input object
                    input_obj = InputModel(**args)
                    validated_args = input_obj.dict()
                else:
                    validated_args = {}  # Initialize as an empty dict if no input_schema

                # Merge validated_args with kwargs
                merged_kwargs = {**validated_args, **kwargs}

                if asyncio.iscoroutinefunction(task_func):
                    result = await task_func(**merged_kwargs)
                else:
                    result = await self.event_loop.run_in_executor(
                        None,
                        lambda: task_func(**merged_kwargs)
                    )
                
                # Cache result
                await self._cache_result(cache_key, result, task_info.get('cache_ttl', 3600))
                
                # Update metrics
                duration = time.time() - start_time
                self.task_executions.labels(task_name=name, status="success").inc()
                self.task_duration.labels(task_name=name).observe(duration)
                
                logging.info(f"Task '{name}' executed successfully at {datetime.now()}")
                return result
                
            except Exception as e:
                self.task_executions.labels(task_name=name, status="error").inc()
                logging.error(f"Error executing task '{name}': {str(e)}")
                
                # Handle retries
                max_retries = self.max_retries.get(name, 3)
                if retry_count < max_retries:
                    delay = self._get_retry_delay(name, retry_count)
                    logging.info(f"Retrying task '{name}' in {delay} seconds (attempt {retry_count + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    return await self._execute_task(name, task_info, retry_count + 1)
                raise
            finally:
                self.active_tasks.dec()

    def get_job(self, name: str) -> Optional[Dict[str, Any]]:
        """Get a scheduled task by name."""
        with self.lock:
            return self.scheduled_tasks.get(name)

    def get_tasks(self) -> Dict[str, Dict[str, Any]]:
        """Get all scheduled tasks."""
        with self.lock:
            return self.scheduled_tasks.copy()

    def schedule_task(self, name: str, cron_expression: str, task_func: Callable, *args, priority: int = 0, **kwargs):
        """Schedule a task according to the cron expression with optional priority."""
        with self.lock:
            if name in self.scheduled_tasks:
                raise ValueError(f"Task '{name}' already exists.")
            self.scheduled_tasks[name] = {
                'cron_expression': cron_expression,
                'task_func': task_func,
                'args': args,
                'kwargs': kwargs
            }
            self.task_priorities[name] = priority
        
        self.event_loop.call_soon_threadsafe(self._schedule_next_run, name)
        logging.info(f"Task '{name}' scheduled with cron expression: {cron_expression} and priority: {priority}")

    def _schedule_next_run(self, name: str):
        """Schedule the next run of a task."""
        if self.stopped or name not in self.scheduled_tasks:
            return

        task_info = self.scheduled_tasks[name]
        cron_expression = task_info['cron_expression']
        last_run = task_info.get('last_run', datetime.now())
        iter_cron = croniter(cron_expression, last_run)
        next_run = iter_cron.get_next(datetime)

        delay = (next_run - datetime.now()).total_seconds()
        self.event_loop.call_later(delay, self._run_task, name)

    def _run_task(self, name: str):
        """Run a task and schedule its next execution."""
        async def wrapper():
            await self._execute_task(name, self.scheduled_tasks[name])
            self._schedule_next_run(name)

        asyncio.run_coroutine_threadsafe(wrapper(), self.event_loop)

    def cancel_task(self, name: str):
        """Cancel a scheduled task."""
        with self.lock:
            if name in self.scheduled_tasks:
                del self.scheduled_tasks[name]
                del self.task_priorities[name]
                logging.info(f"Task '{name}' canceled.")
            else:
                logging.warning(f"Attempted to cancel non-existent task '{name}'.")

    def modify_task(self, name: str, cron_expression: Optional[str] = None, priority: Optional[int] = None):
        """Modify the schedule or priority of an existing task."""
        with self.lock:
            if name not in self.scheduled_tasks:
                raise ValueError(f"Task '{name}' does not exist.")
            
            if cron_expression is not None:
                self.scheduled_tasks[name]['cron_expression'] = cron_expression
                self._schedule_next_run(name)
            
            if priority is not None:
                self.task_priorities[name] = priority

            logging.info(f"Task '{name}' modified. New cron expression: {cron_expression}, New priority: {priority}")

    def add_one_time_task(self, name: str, task_func: Callable, run_at: datetime, *args, priority: int = 0, **kwargs):
        """Add a one-time task to be executed at a specific time."""
        delay = (run_at - datetime.now()).total_seconds()
        if delay < 0:
            raise ValueError("The specified time is in the past.")

        self.event_loop.call_later(delay, self._execute_one_time_task, name, task_func, args, kwargs, priority)
        logging.info(f"One-time task '{name}' scheduled for {run_at} with priority {priority}")

    def _execute_one_time_task(self, name: str, task_func: Callable, args: tuple, kwargs: dict, priority: int):
        """Execute a one-time task."""
        future = asyncio.run_coroutine_threadsafe(self._execute_task(name, {
            'task_func': task_func,
            'args': args,
            'kwargs': kwargs
        }), self.event_loop)
        future.add_done_callback(lambda _: logging.info(f"One-time task '{name}' completed"))

    async def get_upcoming_tasks(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get a list of upcoming tasks sorted by next execution time."""
        now = datetime.now()
        upcoming_tasks = []

        with self.lock:
            for name, task_info in self.scheduled_tasks.items():
                cron_expression = task_info['cron_expression']
                last_run = task_info.get('last_run', datetime.min)
                iter_cron = croniter(cron_expression, last_run)
                next_run = iter_cron.get_next(datetime)

                upcoming_tasks.append({
                    'name': name,
                    'next_run': next_run,
                    'priority': self.task_priorities.get(name, 0)
                })

        upcoming_tasks.sort(key=lambda x: (x['next_run'], -x['priority']))
        return upcoming_tasks[:limit]

    async def get_task_history(self, name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get the execution history of a specific task."""
        # This is a placeholder. In a real implementation, you would store and retrieve
        # task execution history, possibly from a database.
        return []

    def pause_task(self, name: str):
        """Pause a scheduled task."""
        with self.lock:
            if name in self.scheduled_tasks:
                self.scheduled_tasks[name]['paused'] = True
                logging.info(f"Task '{name}' paused.")
            else:
                logging.warning(f"Attempted to pause non-existent task '{name}'.")

    def resume_task(self, name: str):
        """Resume a paused task."""
        with self.lock:
            if name in self.scheduled_tasks:
                self.scheduled_tasks[name]['paused'] = False
                self._schedule_next_run(name)
                logging.info(f"Task '{name}' resumed.")
            else:
                logging.warning(f"Attempted to resume non-existent task '{name}'.")

    async def get_scheduler_stats(self) -> Dict[str, Any]:
        """Get statistics about the scheduler's performance."""
        return {
            'total_tasks': len(self.scheduled_tasks),
            'running_tasks': sum(1 for task in self.scheduled_tasks.values() if not task.get('paused', False)),
            'paused_tasks': sum(1 for task in self.scheduled_tasks.values() if task.get('paused', False)),
            'upcoming_tasks': await self.get_upcoming_tasks(5)
        }

def get_flexible_scheduler():
    global scheduler
    if 'scheduler' not in globals():
        scheduler = FlexibleScheduler()
        scheduler.start()
    return scheduler
