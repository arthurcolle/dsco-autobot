"""
Directed Acyclic Graph (DAG) for Task Dependencies

Manages complex task workflows with dependencies:
- DAG validation (cycle detection)
- Topological sort for execution order
- Parallel execution of independent tasks
- Dependency resolution and tracking
- Graph visualization metadata
"""

from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Tuple
from enum import Enum
from collections import deque, defaultdict
import logging

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    """Task execution status in DAG"""
    PENDING = "pending"  # Not yet ready to execute
    READY = "ready"  # Dependencies satisfied, can execute
    RUNNING = "running"  # Currently executing
    COMPLETED = "completed"  # Successfully completed
    FAILED = "failed"  # Failed execution
    SKIPPED = "skipped"  # Skipped due to failed dependency
    CANCELLED = "cancelled"  # Manually cancelled


@dataclass
class DAGNode:
    """Node in the execution graph"""
    task_id: str
    input_data: str
    processor: Optional[str] = None
    metadata: Dict = field(default_factory=dict)

    # Dependencies
    dependencies: Set[str] = field(default_factory=set)  # task_ids this depends on
    dependents: Set[str] = field(default_factory=set)  # task_ids that depend on this

    # Execution state
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[Dict] = None
    error: Optional[str] = None

    # Timing
    submitted_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    @property
    def is_ready(self) -> bool:
        """Check if task is ready to execute (all dependencies satisfied)"""
        return self.status == TaskStatus.READY

    @property
    def is_terminal(self) -> bool:
        """Check if task is in a terminal state"""
        return self.status in (
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.SKIPPED,
            TaskStatus.CANCELLED
        )

    @property
    def execution_time(self) -> Optional[float]:
        """Get execution time in seconds"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None


class DAGExecutionError(Exception):
    """DAG execution error"""
    pass


class CyclicDependencyError(DAGExecutionError):
    """Cyclic dependency detected in DAG"""
    pass


class TaskDAG:
    """
    Directed Acyclic Graph for task dependencies.

    Manages execution order and dependencies between tasks.
    Supports parallel execution of independent tasks.
    """

    def __init__(self, dag_id: str):
        self.dag_id = dag_id
        self.nodes: Dict[str, DAGNode] = {}  # task_id -> node
        self._validated = False

    def add_task(
        self,
        task_id: str,
        input_data: str,
        dependencies: Optional[List[str]] = None,
        processor: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> DAGNode:
        """
        Add task to DAG.

        Args:
            task_id: Unique task identifier
            input_data: Task input/query
            dependencies: List of task_ids this task depends on
            processor: Target processor tier
            metadata: Additional task metadata

        Returns:
            Created DAG node

        Raises:
            ValueError: If task_id already exists or dependencies are invalid
        """
        if task_id in self.nodes:
            raise ValueError(f"Task {task_id} already exists in DAG")

        # Create node
        node = DAGNode(
            task_id=task_id,
            input_data=input_data,
            processor=processor,
            metadata=metadata or {},
            dependencies=set(dependencies or [])
        )

        # Validate dependencies exist
        for dep_id in node.dependencies:
            if dep_id not in self.nodes:
                raise ValueError(f"Dependency {dep_id} does not exist for task {task_id}")

        # Add to graph
        self.nodes[task_id] = node

        # Update dependent relationships
        for dep_id in node.dependencies:
            self.nodes[dep_id].dependents.add(task_id)

        # Invalidate validation cache
        self._validated = False

        logger.debug(f"Added task {task_id} to DAG with dependencies: {node.dependencies}")

        return node

    def validate(self) -> bool:
        """
        Validate DAG structure.

        Checks for:
        - Cyclic dependencies
        - Missing dependencies
        - Isolated nodes

        Returns:
            True if valid

        Raises:
            CyclicDependencyError: If cycles detected
            ValueError: If other validation errors
        """
        # Check for cycles using DFS
        visited = set()
        rec_stack = set()

        def has_cycle(node_id: str) -> bool:
            visited.add(node_id)
            rec_stack.add(node_id)

            node = self.nodes[node_id]
            for dependent_id in node.dependents:
                if dependent_id not in visited:
                    if has_cycle(dependent_id):
                        return True
                elif dependent_id in rec_stack:
                    return True

            rec_stack.remove(node_id)
            return False

        # Check each node
        for task_id in self.nodes:
            if task_id not in visited:
                if has_cycle(task_id):
                    raise CyclicDependencyError(
                        f"Cyclic dependency detected in DAG {self.dag_id}"
                    )

        # Check for missing dependencies (shouldn't happen with add_task validation)
        for task_id, node in self.nodes.items():
            for dep_id in node.dependencies:
                if dep_id not in self.nodes:
                    raise ValueError(
                        f"Task {task_id} depends on non-existent task {dep_id}"
                    )

        self._validated = True
        logger.info(f"DAG {self.dag_id} validated successfully ({len(self.nodes)} nodes)")
        return True

    def topological_sort(self) -> List[List[str]]:
        """
        Get topological sort of DAG.

        Returns tasks grouped by execution level.
        Tasks at the same level can be executed in parallel.

        Returns:
            List of task_id lists, where each sublist is a parallel execution level

        Raises:
            CyclicDependencyError: If DAG contains cycles

        Example:
            [[A, B], [C, D], [E]]
            - Level 0: A and B can run in parallel
            - Level 1: C and D can run in parallel (after A, B complete)
            - Level 2: E runs after C, D complete
        """
        if not self._validated:
            self.validate()

        # Calculate in-degree for each node
        in_degree = {
            task_id: len(node.dependencies)
            for task_id, node in self.nodes.items()
        }

        # Initialize queue with nodes that have no dependencies
        queue = deque([
            task_id for task_id, degree in in_degree.items()
            if degree == 0
        ])

        levels = []

        while queue:
            # Current level: all tasks ready to execute
            current_level = list(queue)
            levels.append(current_level)
            queue.clear()

            # Process current level
            for task_id in current_level:
                node = self.nodes[task_id]

                # Decrease in-degree for all dependents
                for dependent_id in node.dependents:
                    in_degree[dependent_id] -= 1

                    # If all dependencies satisfied, add to queue
                    if in_degree[dependent_id] == 0:
                        queue.append(dependent_id)

        # Check if all nodes processed (would fail if cycles exist)
        total_processed = sum(len(level) for level in levels)
        if total_processed != len(self.nodes):
            raise CyclicDependencyError(
                f"DAG contains cycles or unreachable nodes "
                f"(processed {total_processed}/{len(self.nodes)})"
            )

        logger.info(
            f"DAG topological sort: {len(levels)} levels, "
            f"max parallelism: {max(len(level) for level in levels)}"
        )

        return levels

    def get_ready_tasks(self) -> List[str]:
        """
        Get list of task_ids that are ready to execute.

        A task is ready if:
        - Status is READY or PENDING
        - All dependencies are COMPLETED
        - Not in terminal state

        Returns:
            List of task_ids ready for execution
        """
        ready = []

        for task_id, node in self.nodes.items():
            # Skip if already terminal
            if node.is_terminal:
                continue

            # Check if dependencies are satisfied
            dependencies_satisfied = all(
                self.nodes[dep_id].status == TaskStatus.COMPLETED
                for dep_id in node.dependencies
            )

            if dependencies_satisfied:
                # Mark as ready if pending
                if node.status == TaskStatus.PENDING:
                    node.status = TaskStatus.READY

                if node.status == TaskStatus.READY:
                    ready.append(task_id)

        return ready

    def mark_started(self, task_id: str, started_at: float):
        """Mark task as started"""
        node = self.nodes[task_id]
        node.status = TaskStatus.RUNNING
        node.started_at = started_at
        logger.debug(f"Task {task_id} started")

    def mark_completed(
        self,
        task_id: str,
        completed_at: float,
        result: Optional[Dict] = None
    ):
        """Mark task as successfully completed"""
        node = self.nodes[task_id]
        node.status = TaskStatus.COMPLETED
        node.completed_at = completed_at
        node.result = result
        logger.info(f"Task {task_id} completed in {node.execution_time:.2f}s")

    def mark_failed(
        self,
        task_id: str,
        completed_at: float,
        error: str,
        cascade: bool = True
    ):
        """
        Mark task as failed.

        Args:
            task_id: Task that failed
            completed_at: Failure timestamp
            error: Error message
            cascade: If True, mark all dependent tasks as SKIPPED
        """
        node = self.nodes[task_id]
        node.status = TaskStatus.FAILED
        node.completed_at = completed_at
        node.error = error
        logger.error(f"Task {task_id} failed: {error}")

        if cascade:
            self._cascade_failure(task_id)

    def _cascade_failure(self, failed_task_id: str):
        """Cascade failure to all dependent tasks"""
        visited = set()
        queue = deque([failed_task_id])

        while queue:
            current_id = queue.popleft()
            if current_id in visited:
                continue
            visited.add(current_id)

            current_node = self.nodes[current_id]

            # Skip dependent tasks
            for dependent_id in current_node.dependents:
                dependent_node = self.nodes[dependent_id]

                # Only skip if not already terminal
                if not dependent_node.is_terminal:
                    dependent_node.status = TaskStatus.SKIPPED
                    dependent_node.error = f"Dependency {current_id} failed"
                    logger.info(f"Task {dependent_id} skipped due to failed dependency")

                    # Continue cascade
                    queue.append(dependent_id)

    def get_statistics(self) -> Dict:
        """Get DAG execution statistics"""
        stats = {
            "total_tasks": len(self.nodes),
            "status_counts": defaultdict(int),
            "avg_execution_time": 0.0,
            "total_execution_time": 0.0,
            "success_rate": 0.0,
        }

        execution_times = []

        for node in self.nodes.values():
            stats["status_counts"][node.status] += 1

            if node.execution_time:
                execution_times.append(node.execution_time)
                stats["total_execution_time"] += node.execution_time

        if execution_times:
            stats["avg_execution_time"] = sum(execution_times) / len(execution_times)

        completed = stats["status_counts"][TaskStatus.COMPLETED]
        failed = stats["status_counts"][TaskStatus.FAILED]
        total_finished = completed + failed

        if total_finished > 0:
            stats["success_rate"] = completed / total_finished

        return dict(stats)

    def get_graph_metadata(self) -> Dict:
        """
        Get graph metadata for visualization.

        Returns:
            Graph structure suitable for visualization tools
        """
        nodes = []
        edges = []

        for task_id, node in self.nodes.items():
            nodes.append({
                "id": task_id,
                "label": task_id,
                "status": node.status,
                "processor": node.processor,
                "execution_time": node.execution_time,
            })

            # Add edges (dependencies)
            for dep_id in node.dependencies:
                edges.append({
                    "from": dep_id,
                    "to": task_id
                })

        return {
            "dag_id": self.dag_id,
            "nodes": nodes,
            "edges": edges,
            "levels": self.topological_sort() if self._validated else None,
            "statistics": self.get_statistics()
        }

    def is_complete(self) -> bool:
        """Check if all tasks are in terminal state"""
        return all(node.is_terminal for node in self.nodes.values())

    def has_failures(self) -> bool:
        """Check if any tasks failed"""
        return any(
            node.status == TaskStatus.FAILED
            for node in self.nodes.values()
        )
