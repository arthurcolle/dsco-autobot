from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Callable, Union
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from enum import Enum
import uuid
from threading import Lock

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

@dataclass
class TaskResult:
    task_id: str
    agent_id: str
    result: Any
    status: TaskStatus
    error: Optional[str] = None

class Agent(ABC):
    """Abstract base class for agents."""
    
    @abstractmethod
    def execute_task(self, task_data: Any) -> Any:
        """Execute a task and return the result."""
        pass
    
    @property
    @abstractmethod
    def agent_id(self) -> str:
        """Return unique agent identifier."""
        pass

class TaskRunFactory:
    """Factory for creating task execution instances."""
    
    @staticmethod
    def create_task_run(task_id: str, agent: Agent, task_data: Any) -> Callable[[], TaskResult]:
        """Create a callable task run."""
        def run_task() -> TaskResult:
            try:
                result = agent.execute_task(task_data)
                return TaskResult(
                    task_id=task_id,
                    agent_id=agent.agent_id,
                    result=result,
                    status=TaskStatus.COMPLETED
                )
            except Exception as e:
                return TaskResult(
                    task_id=task_id,
                    agent_id=agent.agent_id,
                    result=None,
                    status=TaskStatus.FAILED,
                    error=str(e)
                )
        return run_task

class SearchResponseFactory:
    """Factory for creating search responses for agent selection."""
    
    @staticmethod
    def create_search_response(agents: List[Agent], criteria: Optional[Dict[str, Any]] = None) -> List[Agent]:
        """Create a filtered list of agents based on search criteria."""
        if not criteria:
            return agents
        
        # Simple filtering example - can be extended based on criteria
        filtered_agents = []
        for agent in agents:
            if SearchResponseFactory._matches_criteria(agent, criteria):
                filtered_agents.append(agent)
        
        return filtered_agents if filtered_agents else agents
    
    @staticmethod
    def _matches_criteria(agent: Agent, criteria: Dict[str, Any]) -> bool:
        """Check if agent matches the given criteria."""
        # Placeholder implementation - extend based on agent capabilities
        return True

class AgentRegistry:
    """Registry for managing agent instances."""
    
    def __init__(self) -> None:
        self._agents: Dict[str, Agent] = {}
        self._lock = Lock()
    
    def register(self, agent: Agent) -> None:
        """Register an agent in the registry."""
        with self._lock:
            self._agents[agent.agent_id] = agent
    
    def unregister(self, agent_id: str) -> None:
        """Unregister an agent from the registry."""
        with self._lock:
            self._agents.pop(agent_id, None)
    
    def get_agent(self, agent_id: str) -> Optional[Agent]:
        """Get an agent by ID."""
        with self._lock:
            return self._agents.get(agent_id)
    
    def get_all_agents(self) -> List[Agent]:
        """Get all registered agents."""
        with self._lock:
            return list(self._agents.values())
    
    def clear(self) -> None:
        """Clear all agents from registry."""
        with self._lock:
            self._agents.clear()

class AgentCoordinator:
    """
    Coordinate multiple agents with task distribution and result aggregation.
    
    This class manages a pool of agents, distributes tasks among them using a factory pattern,
    and aggregates results from concurrent task execution.
    """
    
    def __init__(self, max_workers: int = 10) -> None:
        """
        Initialize the AgentCoordinator.
        
        Args:
            max_workers: Maximum number of concurrent worker threads
        """
        self.registry = AgentRegistry()
        self.task_run_factory = TaskRunFactory()
        self.search_response_factory = SearchResponseFactory()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._pending_tasks: Dict[str, Future[TaskResult]] = {}
        self._completed_results: Dict[str, TaskResult] = {}
        self._lock = Lock()
        self._is_shutdown = False
    
    def register_agent(self, agent: Agent) -> None:
        """
        Register an agent for task execution.
        
        Args:
            agent: The agent instance to register
        
        Raises:
            RuntimeError: If coordinator is shutdown
        """
        if self._is_shutdown:
            raise RuntimeError("Cannot register agent: coordinator is shutdown")
        
        self.registry.register(agent)
    
    def submit_task(
        self, 
        task_data: Any, 
        agent_selection_criteria: Optional[Dict[str, Any]] = None,
        specific_agent_id: Optional[str] = None
    ) -> str:
        """
        Submit a task for execution by an available agent.
        
        Args:
            task_data: Data/parameters for the task
            agent_selection_criteria: Criteria for selecting appropriate agents
            specific_agent_id: ID of specific agent to use (overrides criteria)
        
        Returns:
            Task ID for tracking the submitted task
        
        Raises:
            RuntimeError: If coordinator is shutdown or no agents available
        """
        if self._is_shutdown:
            raise RuntimeError("Cannot submit task: coordinator is shutdown")
        
        # Select agent
        if specific_agent_id:
            agent = self.registry.get_agent(specific_agent_id)
            if not agent:
                raise ValueError(f"Agent with ID {specific_agent_id} not found")
            selected_agents = [agent]
        else:
            all_agents = self.registry.get_all_agents()
            if not all_agents:
                raise RuntimeError("No agents available for task execution")
            selected_agents = self.search_response_factory.create_search_response(
                all_agents, agent_selection_criteria
            )
        
        if not selected_agents:
            raise RuntimeError("No suitable agents found for task")
        
        # Use first available agent (can be extended for load balancing)
        selected_agent = selected_agents[0]
        
        # Create task
        task_id = str(uuid.uuid4())
        task_run = self.task_run_factory.create_task_run(task_id, selected_agent, task_data)
        
        # Submit for execution
        future = self._executor.submit(task_run)
        
        with self._lock:
            self._pending_tasks[task_id] = future
        
        return task_id
    
    def collect_results(
        self, 
        task_ids: Optional[List[str]] = None, 
        timeout: Optional[float] = None,
        wait_for_completion: bool = True
    ) -> Dict[str, TaskResult]:
        """
        Collect results from submitted tasks.
        
        Args:
            task_ids: Specific task IDs to collect (None for all)
            timeout: Maximum time to wait for results
            wait_for_completion: Whether to wait for pending tasks
        
        Returns:
            Dictionary mapping task IDs to their results
        
        Raises:
            RuntimeError: If coordinator is shutdown
        """
        if self._is_shutdown:
            raise RuntimeError("Cannot collect results: coordinator is shutdown")
        
        results = {}
        
        # Get completed results
        with self._lock:
            if task_ids:
                for task_id in task_ids:
                    if task_id in self._completed_results:
                        results[task_id] = self._completed_results[task_id]
            else:
                results.update(self._completed_results)
        
        if not wait_for_completion:
            return results
        
        # Wait for pending tasks
        tasks_to_wait = []
        with self._lock:
            if task_ids:
                for task_id in task_ids:
                    if task_id in self._pending_tasks:
                        tasks_to_wait.append((task_id, self._pending_tasks[task_id]))
            else:
                tasks_to_wait = list(self._pending_tasks.items())
        
        # Collect results from futures
        for task_id, future in tasks_to_wait:
            try:
                result = future.result(timeout=timeout)
                results[task_id] = result
                
                with self._lock:
                    self._completed_results[task_id] = result
                    self._pending_tasks.pop(task_id, None)
                    
            except Exception as e:
                # Create error result
                error_result = TaskResult(
                    task_id=task_id,
                    agent_id="unknown",
                    result=None,
                    status=TaskStatus.FAILED,
                    error=str(e)
                )
                results[task_id] = error_result
                
                with self._lock:
                    self._completed_results[task_id] = error_result
                    self._pending_tasks.pop(task_id, None)
        
        return results
    
    def shutdown(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Shutdown the coordinator and cleanup resources.
        
        Args:
            wait: Whether to wait for pending tasks to complete
            timeout: Maximum time to wait for shutdown
        """
        self._is_shutdown = True
        
        if wait:
            # Collect any remaining results
            try:
                self.collect_results(timeout=timeout)
            except Exception:
                pass  # Best effort cleanup
        
        # Shutdown executor
        self._executor.shutdown(wait=wait, timeout=timeout)
        
        # Clear registries
        self.registry.clear()
        
        with self._lock:
            self._pending_tasks.clear()
            self._completed_results.clear()