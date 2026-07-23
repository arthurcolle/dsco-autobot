"""
DAG Executor

Executes task DAGs with:
- Parallel execution of independent tasks
- Dependency resolution
- Progress tracking
- Error handling and cascading failures
"""

import asyncio
import time
from typing import Dict, Callable, Awaitable, Optional, List
import logging

from parallel_websearch_server.orchestration.dag import TaskDAG, TaskStatus, DAGExecutionError

logger = logging.getLogger(__name__)


class DAGExecutor:
    """
    Executes task DAGs with parallel execution and dependency management.

    Handles:
    - Topological execution order
    - Parallel execution of independent tasks
    - Dependency tracking
    - Error cascading
    - Progress callbacks
    """

    def __init__(
        self,
        max_parallel: int = 10,
        task_timeout: float = 3600.0
    ):
        """
        Args:
            max_parallel: Maximum number of tasks to run in parallel
            task_timeout: Timeout for individual task execution (seconds)
        """
        self.max_parallel = max_parallel
        self.task_timeout = task_timeout

    async def execute(
        self,
        dag: TaskDAG,
        task_executor: Callable[[str, str, Optional[str]], Awaitable[Dict]],
        progress_callback: Optional[Callable[[str, str, Dict], Awaitable[None]]] = None
    ) -> Dict:
        """
        Execute DAG.

        Args:
            dag: Task DAG to execute
            task_executor: Async function to execute individual tasks
                          Signature: (task_id, input_data, processor) -> result
            progress_callback: Optional callback for progress updates
                             Signature: (task_id, status, data) -> None

        Returns:
            Execution summary with statistics

        Raises:
            DAGExecutionError: If DAG execution fails
        """
        # Validate DAG
        dag.validate()

        logger.info(f"Starting DAG execution: {dag.dag_id} ({len(dag.nodes)} tasks)")
        start_time = time.time()

        # Get execution levels
        levels = dag.topological_sort()
        logger.info(
            f"Execution plan: {len(levels)} levels, "
            f"max parallelism: {max(len(level) for level in levels)}"
        )

        # Execute level by level
        for level_idx, task_ids in enumerate(levels):
            logger.info(f"Executing level {level_idx}: {len(task_ids)} tasks")

            # Execute tasks in this level in parallel
            await self._execute_level(
                dag=dag,
                task_ids=task_ids,
                task_executor=task_executor,
                progress_callback=progress_callback
            )

            # Check for failures
            if dag.has_failures():
                logger.warning(f"Failures detected after level {level_idx}")

        elapsed = time.time() - start_time
        stats = dag.get_statistics()

        logger.info(
            f"DAG execution complete: {dag.dag_id} | "
            f"Elapsed: {elapsed:.2f}s | "
            f"Completed: {stats['status_counts'][TaskStatus.COMPLETED]} | "
            f"Failed: {stats['status_counts'][TaskStatus.FAILED]} | "
            f"Skipped: {stats['status_counts'][TaskStatus.SKIPPED]}"
        )

        return {
            "dag_id": dag.dag_id,
            "elapsed_seconds": elapsed,
            "statistics": stats,
            "success": not dag.has_failures()
        }

    async def _execute_level(
        self,
        dag: TaskDAG,
        task_ids: List[str],
        task_executor: Callable,
        progress_callback: Optional[Callable]
    ):
        """Execute all tasks in a level in parallel"""
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.max_parallel)

        async def execute_task(task_id: str):
            """Execute single task with semaphore"""
            async with semaphore:
                await self._execute_task(
                    dag=dag,
                    task_id=task_id,
                    task_executor=task_executor,
                    progress_callback=progress_callback
                )

        # Execute all tasks in parallel
        await asyncio.gather(
            *[execute_task(task_id) for task_id in task_ids],
            return_exceptions=False  # Let exceptions propagate
        )

    async def _execute_task(
        self,
        dag: TaskDAG,
        task_id: str,
        task_executor: Callable,
        progress_callback: Optional[Callable]
    ):
        """Execute a single task"""
        node = dag.nodes[task_id]

        # Skip if already in terminal state (e.g., cascade-skipped)
        if node.is_terminal:
            logger.debug(f"Task {task_id} already terminal: {node.status}")
            return

        try:
            # Mark started
            started_at = time.time()
            dag.mark_started(task_id, started_at)

            if progress_callback:
                await progress_callback(task_id, "started", {
                    "started_at": started_at
                })

            logger.info(f"Executing task {task_id} (processor={node.processor})")

            # Execute task with timeout
            result = await asyncio.wait_for(
                task_executor(task_id, node.input_data, node.processor),
                timeout=self.task_timeout
            )

            # Mark completed
            completed_at = time.time()
            dag.mark_completed(task_id, completed_at, result)

            if progress_callback:
                await progress_callback(task_id, "completed", {
                    "completed_at": completed_at,
                    "execution_time": node.execution_time,
                    "result": result
                })

            logger.info(
                f"Task {task_id} completed successfully "
                f"(execution_time={node.execution_time:.2f}s)"
            )

        except asyncio.TimeoutError:
            error = f"Task timed out after {self.task_timeout}s"
            completed_at = time.time()
            dag.mark_failed(task_id, completed_at, error, cascade=True)

            if progress_callback:
                await progress_callback(task_id, "failed", {
                    "completed_at": completed_at,
                    "error": error
                })

            logger.error(f"Task {task_id} timed out")

        except Exception as e:
            error = str(e)
            completed_at = time.time()
            dag.mark_failed(task_id, completed_at, error, cascade=True)

            if progress_callback:
                await progress_callback(task_id, "failed", {
                    "completed_at": completed_at,
                    "error": error
                })

            logger.error(f"Task {task_id} failed: {error}", exc_info=True)

    async def execute_adaptive(
        self,
        dag: TaskDAG,
        task_executor: Callable,
        progress_callback: Optional[Callable] = None
    ) -> Dict:
        """
        Execute DAG with adaptive parallelism.

        Unlike level-by-level execution, this executes tasks as soon as
        their dependencies are satisfied, maximizing throughput.

        Args:
            dag: Task DAG to execute
            task_executor: Task execution function
            progress_callback: Optional progress callback

        Returns:
            Execution summary
        """
        dag.validate()

        logger.info(f"Starting adaptive DAG execution: {dag.dag_id}")
        start_time = time.time()

        # Semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.max_parallel)

        # Track running tasks
        running_tasks: Dict[str, asyncio.Task] = {}

        async def execute_when_ready():
            """Continuously execute tasks as they become ready"""
            while not dag.is_complete():
                # Get ready tasks
                ready_task_ids = dag.get_ready_tasks()

                # Filter out already running
                ready_task_ids = [
                    tid for tid in ready_task_ids
                    if tid not in running_tasks
                ]

                # Start new tasks
                for task_id in ready_task_ids:
                    async def run_task(tid=task_id):
                        async with semaphore:
                            await self._execute_task(
                                dag=dag,
                                task_id=tid,
                                task_executor=task_executor,
                                progress_callback=progress_callback
                            )

                    task = asyncio.create_task(run_task())
                    running_tasks[task_id] = task

                # Wait for at least one task to complete
                if running_tasks:
                    done, pending = await asyncio.wait(
                        running_tasks.values(),
                        return_when=asyncio.FIRST_COMPLETED
                    )

                    # Remove completed tasks from tracking
                    for task in done:
                        for tid, t in list(running_tasks.items()):
                            if t == task:
                                del running_tasks[tid]
                                break
                else:
                    # No ready tasks and nothing running - should not happen
                    # unless all remaining are skipped
                    if not dag.is_complete():
                        logger.warning("No ready tasks but DAG not complete")
                        break

                    # Small delay to avoid busy loop
                    await asyncio.sleep(0.1)

        # Execute
        await execute_when_ready()

        elapsed = time.time() - start_time
        stats = dag.get_statistics()

        logger.info(
            f"Adaptive DAG execution complete: {dag.dag_id} | "
            f"Elapsed: {elapsed:.2f}s | "
            f"Stats: {stats}"
        )

        return {
            "dag_id": dag.dag_id,
            "elapsed_seconds": elapsed,
            "statistics": stats,
            "success": not dag.has_failures()
        }
