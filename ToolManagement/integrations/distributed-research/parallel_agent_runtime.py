#!/usr/bin/env python3
"""
PARALLEL AGENT RUNTIME

Advanced parallelization for autonomous agents:
- Subagent orchestration
- Parallel task execution
- Context compaction
- Work stealing
- Result aggregation
"""

import asyncio
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from datetime import datetime
import threading
import queue
import heapq

try:
    import anthropic
except ImportError:
    anthropic = None


# =============================================================================
# TYPES
# =============================================================================

class TaskState(Enum):
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


class AgentRole(Enum):
    ORCHESTRATOR = auto()  # Coordinates other agents
    WORKER = auto()        # Executes tasks
    SEARCHER = auto()      # Searches for information
    VERIFIER = auto()      # Verifies results
    SYNTHESIZER = auto()   # Combines results


@dataclass
class Task:
    id: str
    description: str
    priority: int = 5
    state: TaskState = TaskState.PENDING
    parent_id: Optional[str] = None
    result: Any = None
    error: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    assigned_to: Optional[str] = None
    subtasks: List[str] = field(default_factory=list)

    def __lt__(self, other):
        return self.priority < other.priority


@dataclass
class AgentContext:
    """Context for an agent, supports compaction."""
    messages: List[Dict] = field(default_factory=list)
    facts: Dict[str, Any] = field(default_factory=dict)
    goals: List[str] = field(default_factory=list)
    token_count: int = 0
    max_tokens: int = 100000

    def add_message(self, role: str, content: str):
        # Estimate tokens (rough: 4 chars per token)
        tokens = len(content) // 4
        self.messages.append({
            "role": role,
            "content": content,
            "tokens": tokens,
            "timestamp": time.time()
        })
        self.token_count += tokens

    def needs_compaction(self) -> bool:
        return self.token_count > self.max_tokens * 0.8

    def compact(self, llm_client=None) -> str:
        """Compact context by summarizing older messages."""
        if len(self.messages) < 5:
            return ""

        # Keep recent messages
        recent = self.messages[-3:]
        older = self.messages[:-3]

        # Create summary
        if llm_client and older:
            older_text = "\n".join(m["content"][:200] for m in older)
            try:
                resp = llm_client.messages.create(
                    model="claude-sonnet-5",
                    max_tokens=500,
                    messages=[{
                        "role": "user",
                        "content": f"Summarize this conversation context in 2-3 sentences:\n{older_text}"
                    }]
                )
                summary = resp.content[0].text
            except:
                summary = f"[{len(older)} earlier messages]"
        else:
            summary = f"[{len(older)} earlier messages compacted]"

        # Replace with summary + recent
        self.messages = [{"role": "system", "content": summary, "tokens": len(summary)//4}] + recent
        self.token_count = sum(m.get("tokens", 0) for m in self.messages)

        return summary


# =============================================================================
# WORK QUEUE WITH STEALING
# =============================================================================

class WorkStealingQueue:
    """
    Distributed work queue with work stealing.

    Agents can steal work from other agents' queues
    when their own queue is empty.
    """

    def __init__(self):
        self.queues: Dict[str, queue.PriorityQueue] = {}
        self.global_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._lock = threading.RLock()

    def register_agent(self, agent_id: str):
        with self._lock:
            self.queues[agent_id] = queue.PriorityQueue()

    def submit(self, task: Task, agent_id: Optional[str] = None):
        """Submit task to specific agent or global queue."""
        with self._lock:
            if agent_id and agent_id in self.queues:
                self.queues[agent_id].put((task.priority, task))
            else:
                self.global_queue.put((task.priority, task))

    def get(self, agent_id: str, timeout: float = 1.0) -> Optional[Task]:
        """Get task, trying own queue first, then stealing."""
        # Try own queue
        try:
            _, task = self.queues[agent_id].get_nowait()
            return task
        except queue.Empty:
            pass

        # Try global queue
        try:
            _, task = self.global_queue.get(timeout=timeout/2)
            return task
        except queue.Empty:
            pass

        # Try stealing from other agents
        with self._lock:
            for other_id, other_queue in self.queues.items():
                if other_id != agent_id and not other_queue.empty():
                    try:
                        _, task = other_queue.get_nowait()
                        return task
                    except queue.Empty:
                        continue

        return None

    def size(self) -> int:
        with self._lock:
            total = self.global_queue.qsize()
            for q in self.queues.values():
                total += q.qsize()
            return total


# =============================================================================
# SUBAGENT
# =============================================================================

class SubAgent:
    """
    A subagent that executes tasks with isolated context.

    Subagents:
    - Have their own context window
    - Can run in parallel
    - Return only relevant results to parent
    """

    def __init__(self,
                 agent_id: str,
                 role: AgentRole,
                 parent_id: Optional[str] = None,
                 llm_client=None,
                 tools: Optional[Dict[str, Callable]] = None):
        self.id = agent_id
        self.role = role
        self.parent_id = parent_id
        self.llm = llm_client
        self.tools = tools or {}
        self.context = AgentContext()
        self.tasks_completed = 0
        self.running = False

    def execute(self, task: Task) -> Tuple[bool, Any]:
        """Execute a task and return (success, result)."""
        task.state = TaskState.RUNNING
        task.started_at = time.time()
        task.assigned_to = self.id

        try:
            # Add task to context
            self.context.add_message("user", f"Task: {task.description}")

            # Compact if needed
            if self.context.needs_compaction():
                self.context.compact(self.llm)

            # Execute based on role
            if self.role == AgentRole.SEARCHER:
                result = self._search(task)
            elif self.role == AgentRole.VERIFIER:
                result = self._verify(task)
            elif self.role == AgentRole.SYNTHESIZER:
                result = self._synthesize(task)
            else:
                result = self._work(task)

            task.state = TaskState.COMPLETED
            task.result = result
            task.completed_at = time.time()
            self.tasks_completed += 1

            # Add result to context
            self.context.add_message("assistant", f"Result: {str(result)[:500]}")

            return True, result

        except Exception as e:
            task.state = TaskState.FAILED
            task.error = str(e)
            task.completed_at = time.time()
            return False, str(e)

    def _search(self, task: Task) -> Dict:
        """Search for information."""
        if not self.llm:
            return {"query": task.description, "results": []}

        resp = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=1000,
            messages=[{
                "role": "user",
                "content": f"Search/analyze: {task.description}\nReturn key findings only."
            }]
        )
        return {"query": task.description, "findings": resp.content[0].text}

    def _verify(self, task: Task) -> Dict:
        """Verify a result."""
        if not self.llm:
            return {"verified": True, "confidence": 0.5}

        resp = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=500,
            messages=[{
                "role": "user",
                "content": f"Verify this is correct and complete: {task.description}\nReturn: verified (true/false), confidence (0-1), issues (if any)"
            }]
        )
        return {"verification": resp.content[0].text}

    def _synthesize(self, task: Task) -> str:
        """Synthesize multiple results."""
        if not self.llm:
            return task.description

        resp = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": f"Synthesize these results into a coherent summary:\n{task.description}"
            }]
        )
        return resp.content[0].text

    def _work(self, task: Task) -> Any:
        """General work execution."""
        if not self.llm:
            return f"Completed: {task.description}"

        # Build tool descriptions
        tool_desc = "\n".join(f"- {name}" for name in self.tools.keys()) if self.tools else "None"

        resp = self.llm.messages.create(
            model="claude-sonnet-5",
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": f"Complete this task: {task.description}\n\nAvailable tools: {tool_desc}"
            }]
        )
        return resp.content[0].text


# =============================================================================
# PARALLEL ORCHESTRATOR
# =============================================================================

class ParallelOrchestrator:
    """
    Orchestrates parallel execution of subagents.

    Features:
    - Spawn subagents dynamically
    - Distribute work across agents
    - Collect and aggregate results
    - Handle failures and retries
    """

    def __init__(self,
                 max_workers: int = 4,
                 llm_client=None):
        self.max_workers = max_workers
        self.llm = llm_client
        self.agents: Dict[str, SubAgent] = {}
        self.work_queue = WorkStealingQueue()
        self.tasks: Dict[str, Task] = {}
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self._lock = threading.RLock()
        self.running = False

    def spawn_agent(self, role: AgentRole = AgentRole.WORKER,
                    tools: Optional[Dict] = None) -> SubAgent:
        """Spawn a new subagent."""
        agent_id = f"agent_{uuid.uuid4().hex[:8]}"
        agent = SubAgent(
            agent_id=agent_id,
            role=role,
            llm_client=self.llm,
            tools=tools
        )

        with self._lock:
            self.agents[agent_id] = agent
            self.work_queue.register_agent(agent_id)

        return agent

    def submit_task(self, description: str,
                    priority: int = 5,
                    parent_id: Optional[str] = None) -> Task:
        """Submit a task for execution."""
        task = Task(
            id=f"task_{uuid.uuid4().hex[:8]}",
            description=description,
            priority=priority,
            parent_id=parent_id
        )

        with self._lock:
            self.tasks[task.id] = task
            self.work_queue.submit(task)

        return task

    def parallel_search(self, queries: List[str]) -> List[Dict]:
        """Execute multiple search queries in parallel."""
        # Spawn searchers
        searchers = [self.spawn_agent(AgentRole.SEARCHER) for _ in range(min(len(queries), self.max_workers))]

        # Submit search tasks
        tasks = []
        for query in queries:
            task = self.submit_task(query, priority=3)
            tasks.append(task)

        # Execute in parallel
        futures = []
        for i, task in enumerate(tasks):
            agent = searchers[i % len(searchers)]
            future = self.executor.submit(agent.execute, task)
            futures.append((task, future))

        # Collect results
        results = []
        for task, future in futures:
            try:
                success, result = future.result(timeout=30)
                if success:
                    results.append(result)
            except Exception as e:
                results.append({"error": str(e), "query": task.description})

        return results

    def parallel_execute(self, tasks_desc: List[str],
                         role: AgentRole = AgentRole.WORKER) -> List[Any]:
        """Execute multiple tasks in parallel."""
        agents = [self.spawn_agent(role) for _ in range(min(len(tasks_desc), self.max_workers))]

        tasks = [self.submit_task(desc) for desc in tasks_desc]

        futures = []
        for i, task in enumerate(tasks):
            agent = agents[i % len(agents)]
            future = self.executor.submit(agent.execute, task)
            futures.append(future)

        results = []
        for future in as_completed(futures):
            try:
                success, result = future.result(timeout=60)
                results.append(result if success else None)
            except:
                results.append(None)

        return results

    def map_reduce(self, items: List[Any],
                   map_prompt: str,
                   reduce_prompt: str) -> str:
        """
        Parallel map-reduce operation.

        Map: Process each item in parallel
        Reduce: Synthesize all results
        """
        # Map phase - parallel
        map_tasks = [f"{map_prompt}\n\nItem: {item}" for item in items]
        map_results = self.parallel_execute(map_tasks, AgentRole.WORKER)

        # Filter successful results
        valid_results = [r for r in map_results if r]

        # Reduce phase
        reducer = self.spawn_agent(AgentRole.SYNTHESIZER)
        reduce_task = Task(
            id=f"reduce_{uuid.uuid4().hex[:8]}",
            description=f"{reduce_prompt}\n\nResults:\n" + "\n---\n".join(str(r)[:500] for r in valid_results)
        )

        success, result = reducer.execute(reduce_task)
        return result if success else f"Reduce failed: {reduce_task.error}"

    def hierarchical_search(self, query: str, depth: int = 2) -> Dict:
        """
        Hierarchical parallel search.

        Level 0: Main query
        Level 1: Sub-queries derived from level 0
        Level 2: Deep dive on promising results
        """
        results = {"query": query, "levels": []}

        # Level 0: Initial search
        searcher = self.spawn_agent(AgentRole.SEARCHER)
        task = Task(id="l0", description=query)
        success, l0_result = searcher.execute(task)
        results["levels"].append({"level": 0, "result": l0_result})

        if not success or depth < 1:
            return results

        # Generate sub-queries
        if self.llm:
            resp = self.llm.messages.create(
                model="claude-sonnet-5",
                max_tokens=500,
                messages=[{
                    "role": "user",
                    "content": f"Based on this search result, generate 3 follow-up search queries (one per line):\n{l0_result}"
                }]
            )
            sub_queries = [q.strip() for q in resp.content[0].text.strip().split('\n') if q.strip()][:3]
        else:
            sub_queries = [f"{query} - aspect {i}" for i in range(3)]

        # Level 1: Parallel sub-searches
        l1_results = self.parallel_search(sub_queries)
        results["levels"].append({"level": 1, "results": l1_results})

        if depth < 2:
            return results

        # Level 2: Synthesize
        synthesizer = self.spawn_agent(AgentRole.SYNTHESIZER)
        all_findings = "\n".join(str(r) for r in l1_results)
        synth_task = Task(id="synth", description=f"Synthesize findings:\n{all_findings}")
        success, synthesis = synthesizer.execute(synth_task)
        results["levels"].append({"level": 2, "synthesis": synthesis})

        return results

    def get_stats(self) -> Dict:
        """Get orchestrator statistics."""
        with self._lock:
            return {
                "agents": len(self.agents),
                "tasks_total": len(self.tasks),
                "tasks_pending": sum(1 for t in self.tasks.values() if t.state == TaskState.PENDING),
                "tasks_running": sum(1 for t in self.tasks.values() if t.state == TaskState.RUNNING),
                "tasks_completed": sum(1 for t in self.tasks.values() if t.state == TaskState.COMPLETED),
                "tasks_failed": sum(1 for t in self.tasks.values() if t.state == TaskState.FAILED),
                "queue_size": self.work_queue.size(),
                "agents_by_role": {
                    role.name: sum(1 for a in self.agents.values() if a.role == role)
                    for role in AgentRole
                }
            }

    def shutdown(self):
        """Shutdown the orchestrator."""
        self.running = False
        self.executor.shutdown(wait=True)


# =============================================================================
# AGENT FACTORY
# =============================================================================

class AgentFactory:
    """
    Factory for creating specialized agents.

    Creates agents with appropriate:
    - Tools
    - Context
    - Role configuration
    """

    def __init__(self, llm_client=None, tools_registry: Optional[Dict[str, Callable]] = None):
        self.llm = llm_client
        self.tools = tools_registry or {}

    def create_searcher(self) -> SubAgent:
        """Create a search-specialized agent."""
        return SubAgent(
            agent_id=f"searcher_{uuid.uuid4().hex[:6]}",
            role=AgentRole.SEARCHER,
            llm_client=self.llm,
            tools={k: v for k, v in self.tools.items() if 'search' in k or 'read' in k}
        )

    def create_coder(self) -> SubAgent:
        """Create a code-specialized agent."""
        return SubAgent(
            agent_id=f"coder_{uuid.uuid4().hex[:6]}",
            role=AgentRole.WORKER,
            llm_client=self.llm,
            tools={k: v for k, v in self.tools.items() if 'write' in k or 'edit' in k or 'exec' in k}
        )

    def create_verifier(self) -> SubAgent:
        """Create a verification-specialized agent."""
        return SubAgent(
            agent_id=f"verifier_{uuid.uuid4().hex[:6]}",
            role=AgentRole.VERIFIER,
            llm_client=self.llm,
            tools={k: v for k, v in self.tools.items() if 'test' in k or 'check' in k}
        )

    def create_orchestrator(self, max_workers: int = 4) -> ParallelOrchestrator:
        """Create a parallel orchestrator."""
        return ParallelOrchestrator(max_workers=max_workers, llm_client=self.llm)


# =============================================================================
# DEMO
# =============================================================================

def main():
    print("PARALLEL AGENT RUNTIME")
    print("=" * 50)

    # Initialize
    llm = None
    if anthropic and os.environ.get('ANTHROPIC_API_KEY'):
        llm = anthropic.Anthropic()
        print("LLM: Connected")
    else:
        print("LLM: Not available (demo mode)")

    # Create orchestrator
    orch = ParallelOrchestrator(max_workers=4, llm_client=llm)

    # Demo parallel execution
    print("\n1. Parallel Task Execution")
    print("-" * 30)

    tasks = [
        "Analyze Python file structure",
        "Find function definitions",
        "Identify class hierarchies",
        "Extract import patterns"
    ]

    results = orch.parallel_execute(tasks)
    print(f"Executed {len(tasks)} tasks in parallel")
    for i, r in enumerate(results):
        print(f"  Task {i+1}: {str(r)[:50]}...")

    # Demo hierarchical search
    if llm:
        print("\n2. Hierarchical Search")
        print("-" * 30)

        search_result = orch.hierarchical_search(
            "How do autonomous coding agents work?",
            depth=2
        )
        print(f"Levels searched: {len(search_result['levels'])}")
        for level in search_result['levels']:
            print(f"  Level {level['level']}: {str(level)[:80]}...")

    # Stats
    print("\n3. Orchestrator Stats")
    print("-" * 30)
    stats = orch.get_stats()
    for k, v in stats.items():
        print(f"  {k}: {v}")

    orch.shutdown()
    print("\nDone.")


if __name__ == "__main__":
    main()
