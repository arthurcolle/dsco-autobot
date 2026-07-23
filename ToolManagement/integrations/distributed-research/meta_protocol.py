#!/usr/bin/env python3
"""
META-PROTOCOL LAYER - Unified Interoperability for Autonomous Systems

Defines protocols and message passing for inter-operation between:
- AutonomousCoder: Learning + Generation + Verification
- ParallelOrchestrator: Parallel subagent execution
- GOAPPlanner: Self-improvement planning
- SwarmLearner: Pattern extraction
- KnowledgeGraph: Collective memory

Architecture:
┌─────────────────────────────────────────────────────────────────────────────┐
│                           META-PROTOCOL LAYER                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐│
│  │                         MESSAGE BUS                                      ││
│  │   publish/subscribe • request/response • broadcast • streaming           ││
│  └─────────────────────────────────────────────────────────────────────────┘│
│           │              │              │              │              │      │
│  ┌────────▼───┐  ┌───────▼────┐  ┌─────▼─────┐  ┌────▼─────┐  ┌─────▼────┐ │
│  │  Learner   │  │  Generator │  │  Planner  │  │ Executor │  │ Verifier │ │
│  │  Protocol  │  │  Protocol  │  │  Protocol │  │ Protocol │  │ Protocol │ │
│  └────────────┘  └────────────┘  └───────────┘  └──────────┘  └──────────┘ │
│           │              │              │              │              │      │
│  ┌────────▼───┐  ┌───────▼────┐  ┌─────▼─────┐  ┌────▼─────┐  ┌─────▼────┐ │
│  │  Swarm     │  │ Autonomous │  │   GOAP    │  │ Parallel │  │  Code    │ │
│  │  Learner   │  │   Coder    │  │  Planner  │  │ Runtime  │  │ Verifier │ │
│  └────────────┘  └────────────┘  └───────────┘  └──────────┘  └──────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘

Protocol Categories:
1. Capability Protocols - What systems can do
2. Message Protocols - How systems communicate
3. State Protocols - How state is shared
4. Orchestration Protocols - How systems coordinate
"""

from __future__ import annotations
import asyncio
import json
import time
import uuid
import hashlib
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, Generic, List, Optional,
    Protocol, Set, Tuple, TypeVar, Union, runtime_checkable
)
from concurrent.futures import ThreadPoolExecutor, Future
import threading
import queue


# =============================================================================
# TYPE VARIABLES
# =============================================================================

T = TypeVar('T')
R = TypeVar('R')
S = TypeVar('S')


# =============================================================================
# MESSAGE TYPES
# =============================================================================

class MessageType(Enum):
    """Types of messages in the protocol."""
    # Requests
    REQUEST = auto()
    COMMAND = auto()
    QUERY = auto()

    # Responses
    RESPONSE = auto()
    ERROR = auto()
    ACK = auto()

    # Events
    EVENT = auto()
    NOTIFICATION = auto()
    BROADCAST = auto()

    # Streaming
    STREAM_START = auto()
    STREAM_DATA = auto()
    STREAM_END = auto()

    # Control
    HEARTBEAT = auto()
    SHUTDOWN = auto()
    HANDSHAKE = auto()


class MessagePriority(Enum):
    """Priority levels for messages."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3
    BACKGROUND = 4


@dataclass
class Message:
    """Universal message format for inter-system communication."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: MessageType = MessageType.REQUEST
    source: str = ""
    target: str = ""
    action: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    priority: MessagePriority = MessagePriority.NORMAL
    timestamp: float = field(default_factory=time.time)
    ttl: Optional[float] = None  # Time to live in seconds
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'type': self.type.name,
            'source': self.source,
            'target': self.target,
            'action': self.action,
            'payload': self.payload,
            'correlation_id': self.correlation_id,
            'priority': self.priority.name,
            'timestamp': self.timestamp,
            'ttl': self.ttl,
            'metadata': self.metadata
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        return cls(
            id=data.get('id', str(uuid.uuid4())),
            type=MessageType[data.get('type', 'REQUEST')],
            source=data.get('source', ''),
            target=data.get('target', ''),
            action=data.get('action', ''),
            payload=data.get('payload', {}),
            correlation_id=data.get('correlation_id'),
            priority=MessagePriority[data.get('priority', 'NORMAL')],
            timestamp=data.get('timestamp', time.time()),
            ttl=data.get('ttl'),
            metadata=data.get('metadata', {})
        )

    def reply(self, payload: Dict[str, Any],
              msg_type: MessageType = MessageType.RESPONSE) -> 'Message':
        """Create a reply message."""
        return Message(
            type=msg_type,
            source=self.target,
            target=self.source,
            action=f"{self.action}_response",
            payload=payload,
            correlation_id=self.id,
            priority=self.priority
        )

    def error(self, error: str, code: Optional[str] = None) -> 'Message':
        """Create an error response."""
        return Message(
            type=MessageType.ERROR,
            source=self.target,
            target=self.source,
            action=f"{self.action}_error",
            payload={'error': error, 'code': code},
            correlation_id=self.id,
            priority=MessagePriority.HIGH
        )


# =============================================================================
# CAPABILITY PROTOCOLS
# =============================================================================

@runtime_checkable
class Learnable(Protocol):
    """Protocol for systems that can learn from data."""

    def learn(self, source: Any, **kwargs) -> Dict[str, Any]:
        """Learn from a data source."""
        ...

    def get_knowledge(self) -> Dict[str, Any]:
        """Get current learned knowledge."""
        ...


@runtime_checkable
class Generatable(Protocol):
    """Protocol for systems that can generate artifacts."""

    def generate(self, specification: str, **kwargs) -> Any:
        """Generate an artifact from specification."""
        ...

    def validate(self, artifact: Any) -> Tuple[bool, Optional[str]]:
        """Validate a generated artifact."""
        ...


@runtime_checkable
class Plannable(Protocol):
    """Protocol for systems that can create plans."""

    def plan(self, goal: Any, current_state: Any) -> Optional[List[Any]]:
        """Create a plan to achieve goal from current state."""
        ...

    def execute_step(self, step: Any) -> Tuple[bool, Any]:
        """Execute a single plan step."""
        ...


@runtime_checkable
class Executable(Protocol):
    """Protocol for systems that can execute tasks."""

    def submit(self, task: Callable, *args, **kwargs) -> str:
        """Submit a task for execution."""
        ...

    def get_result(self, task_id: str, wait: bool = False) -> Any:
        """Get result of a submitted task."""
        ...


@runtime_checkable
class Observable(Protocol):
    """Protocol for systems that emit events."""

    def subscribe(self, event_type: str, handler: Callable) -> str:
        """Subscribe to events."""
        ...

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        ...


# =============================================================================
# UNIFIED MESSAGE BUS
# =============================================================================

class MessageBus:
    """
    Central message bus for inter-system communication.

    Supports:
    - Publish/Subscribe for events
    - Request/Response for queries
    - Broadcast for notifications
    - Streaming for continuous data
    """

    def __init__(self):
        self._subscribers: Dict[str, Dict[str, Callable]] = defaultdict(dict)
        self._request_handlers: Dict[str, Callable] = {}
        self._pending_requests: Dict[str, asyncio.Future] = {}
        self._message_queue: queue.PriorityQueue = queue.PriorityQueue()
        self._lock = threading.RLock()
        self._running = False
        self._executor = ThreadPoolExecutor(max_workers=8)
        self._message_history: List[Message] = []
        self._max_history = 1000

    def subscribe(self, topic: str, handler: Callable[[Message], None],
                  subscriber_id: Optional[str] = None) -> str:
        """Subscribe to a topic."""
        sub_id = subscriber_id or str(uuid.uuid4())
        with self._lock:
            self._subscribers[topic][sub_id] = handler
        return sub_id

    def unsubscribe(self, topic: str, subscriber_id: str) -> bool:
        """Unsubscribe from a topic."""
        with self._lock:
            if topic in self._subscribers and subscriber_id in self._subscribers[topic]:
                del self._subscribers[topic][subscriber_id]
                return True
        return False

    def publish(self, topic: str, message: Message) -> None:
        """Publish a message to a topic."""
        message.metadata['topic'] = topic
        self._record_message(message)

        with self._lock:
            handlers = list(self._subscribers.get(topic, {}).values())
            # Also notify wildcard subscribers
            handlers.extend(self._subscribers.get('*', {}).values())

        for handler in handlers:
            self._executor.submit(self._safe_call, handler, message)

    def _safe_call(self, handler: Callable, message: Message) -> None:
        """Safely call a handler."""
        try:
            handler(message)
        except Exception as e:
            error_msg = Message(
                type=MessageType.ERROR,
                source='message_bus',
                action='handler_error',
                payload={'error': str(e), 'original_message': message.id}
            )
            self._record_message(error_msg)

    def register_handler(self, action: str, handler: Callable[[Message], Message]) -> None:
        """Register a request handler for an action."""
        with self._lock:
            self._request_handlers[action] = handler

    def request(self, message: Message, timeout: float = 30.0) -> Message:
        """Send a request and wait for response."""
        future: Future = Future()

        with self._lock:
            self._pending_requests[message.id] = future

        self._record_message(message)

        # Find and call handler
        handler = self._request_handlers.get(message.action)
        if handler:
            def process():
                try:
                    response = handler(message)
                    response.correlation_id = message.id
                    future.set_result(response)
                except Exception as e:
                    future.set_result(message.error(str(e)))

            self._executor.submit(process)
        else:
            future.set_result(message.error(f"No handler for action: {message.action}"))

        try:
            return future.result(timeout=timeout)
        except TimeoutError:
            return message.error("Request timeout")
        finally:
            with self._lock:
                self._pending_requests.pop(message.id, None)

    async def request_async(self, message: Message, timeout: float = 30.0) -> Message:
        """Async version of request."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            lambda: self.request(message, timeout)
        )

    def broadcast(self, message: Message) -> None:
        """Broadcast a message to all subscribers."""
        message.type = MessageType.BROADCAST
        self._record_message(message)

        with self._lock:
            all_handlers = []
            for handlers in self._subscribers.values():
                all_handlers.extend(handlers.values())

        for handler in all_handlers:
            self._executor.submit(self._safe_call, handler, message)

    def _record_message(self, message: Message) -> None:
        """Record message in history."""
        with self._lock:
            self._message_history.append(message)
            if len(self._message_history) > self._max_history:
                self._message_history = self._message_history[-self._max_history:]

    def get_history(self, limit: int = 100) -> List[Message]:
        """Get recent message history."""
        with self._lock:
            return self._message_history[-limit:]

    def shutdown(self) -> None:
        """Shutdown the message bus."""
        self._running = False
        self._executor.shutdown(wait=True)


# =============================================================================
# SYSTEM ADAPTERS
# =============================================================================

class SystemAdapter(ABC):
    """Base adapter for integrating systems with the protocol layer."""

    def __init__(self, system_id: str, bus: MessageBus):
        self.system_id = system_id
        self.bus = bus
        self._subscriptions: List[str] = []
        self._register_handlers()

    @abstractmethod
    def _register_handlers(self) -> None:
        """Register message handlers for this system."""
        pass

    def send(self, target: str, action: str, payload: Dict[str, Any],
             msg_type: MessageType = MessageType.REQUEST) -> Message:
        """Send a message to another system."""
        message = Message(
            type=msg_type,
            source=self.system_id,
            target=target,
            action=action,
            payload=payload
        )
        return self.bus.request(message)

    def emit(self, event: str, data: Dict[str, Any]) -> None:
        """Emit an event."""
        message = Message(
            type=MessageType.EVENT,
            source=self.system_id,
            action=event,
            payload=data
        )
        self.bus.publish(f"{self.system_id}.{event}", message)

    def listen(self, topic: str, handler: Callable[[Message], None]) -> str:
        """Listen to a topic."""
        sub_id = self.bus.subscribe(topic, handler, f"{self.system_id}_{topic}")
        self._subscriptions.append((topic, sub_id))
        return sub_id


class LearnerAdapter(SystemAdapter):
    """Adapter for SwarmLearner / AutonomousCoder learning capabilities."""

    def __init__(self, bus: MessageBus, learner: Any):
        self.learner = learner
        super().__init__('learner', bus)

    def _register_handlers(self) -> None:
        self.bus.register_handler('learn', self._handle_learn)
        self.bus.register_handler('get_knowledge', self._handle_get_knowledge)
        self.bus.register_handler('get_patterns', self._handle_get_patterns)

    def _handle_learn(self, msg: Message) -> Message:
        max_files = msg.payload.get('max_files', 1000)
        try:
            if hasattr(self.learner, 'learn'):
                result = self.learner.learn(max_files=max_files)
            else:
                result = {'error': 'Learner does not support learn()'}
            return msg.reply({'status': 'ok', 'result': result})
        except Exception as e:
            return msg.error(str(e))

    def _handle_get_knowledge(self, msg: Message) -> Message:
        try:
            if hasattr(self.learner, 'get_stats'):
                knowledge = self.learner.get_stats()
            elif hasattr(self.learner, 'knowledge'):
                knowledge = {
                    'functions': len(getattr(self.learner.knowledge, 'function_signatures', [])),
                    'classes': len(getattr(self.learner.knowledge, 'class_structures', []))
                }
            else:
                knowledge = {}
            return msg.reply({'knowledge': knowledge})
        except Exception as e:
            return msg.error(str(e))

    def _handle_get_patterns(self, msg: Message) -> Message:
        pattern_type = msg.payload.get('type', 'all')
        try:
            patterns = {}
            if hasattr(self.learner, 'knowledge') and self.learner.knowledge:
                k = self.learner.knowledge
                if pattern_type in ('all', 'imports'):
                    patterns['imports'] = dict(k.import_patterns.most_common(20))
                if pattern_type in ('all', 'decorators'):
                    patterns['decorators'] = dict(k.decorator_patterns.most_common(20))
                if pattern_type in ('all', 'functions'):
                    patterns['functions'] = k.function_signatures[:10]
                if pattern_type in ('all', 'classes'):
                    patterns['classes'] = k.class_structures[:10]
            return msg.reply({'patterns': patterns})
        except Exception as e:
            return msg.error(str(e))


class GeneratorAdapter(SystemAdapter):
    """Adapter for code generation capabilities."""

    def __init__(self, bus: MessageBus, generator: Any):
        self.generator = generator
        super().__init__('generator', bus)

    def _register_handlers(self) -> None:
        self.bus.register_handler('generate', self._handle_generate)
        self.bus.register_handler('generate_function', self._handle_generate_function)
        self.bus.register_handler('generate_class', self._handle_generate_class)
        self.bus.register_handler('verify', self._handle_verify)

    def _handle_generate(self, msg: Message) -> Message:
        description = msg.payload.get('description', '')
        verify = msg.payload.get('verify', True)
        try:
            result = self.generator.generate(description, verify=verify)
            self.emit('code_generated', {'description': description, 'valid': result.get('valid')})
            return msg.reply(result)
        except Exception as e:
            return msg.error(str(e))

    def _handle_generate_function(self, msg: Message) -> Message:
        name = msg.payload.get('name', 'generated_fn')
        purpose = msg.payload.get('purpose', '')
        args = msg.payload.get('args')
        return_type = msg.payload.get('return_type')
        try:
            result = self.generator.generate_function(
                name, purpose, args=args, return_type=return_type
            )
            return msg.reply(result)
        except Exception as e:
            return msg.error(str(e))

    def _handle_generate_class(self, msg: Message) -> Message:
        name = msg.payload.get('name', 'GeneratedClass')
        purpose = msg.payload.get('purpose', '')
        bases = msg.payload.get('bases')
        methods = msg.payload.get('methods')
        pattern = msg.payload.get('pattern')
        try:
            result = self.generator.generate_class(
                name, purpose, bases=bases, methods=methods, pattern=pattern
            )
            return msg.reply(result)
        except Exception as e:
            return msg.error(str(e))

    def _handle_verify(self, msg: Message) -> Message:
        code = msg.payload.get('code', '')
        try:
            if hasattr(self.generator, 'verifier'):
                result = self.generator.verifier.full_verify(code)
            else:
                result = {'error': 'No verifier available'}
            return msg.reply({'verification': result})
        except Exception as e:
            return msg.error(str(e))


class PlannerAdapter(SystemAdapter):
    """Adapter for GOAP planning capabilities."""

    def __init__(self, bus: MessageBus, planner: Any):
        self.planner = planner
        super().__init__('planner', bus)

    def _register_handlers(self) -> None:
        self.bus.register_handler('plan', self._handle_plan)
        self.bus.register_handler('get_goals', self._handle_get_goals)
        self.bus.register_handler('assess_state', self._handle_assess_state)
        self.bus.register_handler('get_development_plan', self._handle_get_dev_plan)

    def _handle_plan(self, msg: Message) -> Message:
        goal_id = msg.payload.get('goal_id', 'ultimate')
        try:
            if hasattr(self.planner, 'goal_hierarchy'):
                goal = self.planner.goal_hierarchy.goals.get(goal_id)
                if goal:
                    plan = self.planner.plan_for_goal(goal)
                    if plan:
                        return msg.reply({
                            'plan': [{'action': a.name, 'cost': a.cost} for a in plan],
                            'total_cost': sum(a.cost for a in plan)
                        })
                    return msg.error(f"No plan found for goal: {goal_id}")
                return msg.error(f"Goal not found: {goal_id}")
            return msg.error("Planner does not support goal hierarchy")
        except Exception as e:
            return msg.error(str(e))

    def _handle_get_goals(self, msg: Message) -> Message:
        try:
            if hasattr(self.planner, 'goal_hierarchy'):
                goals = {
                    gid: {
                        'name': g.name,
                        'priority': g.priority.name,
                        'progress': g.progress
                    }
                    for gid, g in self.planner.goal_hierarchy.goals.items()
                }
                return msg.reply({'goals': goals})
            return msg.error("No goal hierarchy")
        except Exception as e:
            return msg.error(str(e))

    def _handle_assess_state(self, msg: Message) -> Message:
        capabilities = msg.payload.get('capabilities', {})
        try:
            state = self.planner.assess_current_state(capabilities)
            return msg.reply({
                'state': {k.name: v for k, v in state.to_dict().items()}
            })
        except Exception as e:
            return msg.error(str(e))

    def _handle_get_dev_plan(self, msg: Message) -> Message:
        try:
            plan = self.planner.get_development_plan()
            return msg.reply(plan)
        except Exception as e:
            return msg.error(str(e))


class ExecutorAdapter(SystemAdapter):
    """Adapter for parallel execution capabilities."""

    def __init__(self, bus: MessageBus, executor: Any):
        self.executor = executor
        super().__init__('executor', bus)

    def _register_handlers(self) -> None:
        self.bus.register_handler('execute', self._handle_execute)
        self.bus.register_handler('parallel_execute', self._handle_parallel_execute)
        self.bus.register_handler('get_executor_stats', self._handle_get_stats)

    def _handle_execute(self, msg: Message) -> Message:
        task_desc = msg.payload.get('task', '')
        try:
            if hasattr(self.executor, 'execute_task'):
                result = self.executor.execute_task(task_desc)
            elif hasattr(self.executor, 'parallel_execute'):
                results = self.executor.parallel_execute([task_desc])
                result = results[0] if results else None
            else:
                result = {'error': 'No execute capability'}
            return msg.reply({'result': result})
        except Exception as e:
            return msg.error(str(e))

    def _handle_parallel_execute(self, msg: Message) -> Message:
        tasks = msg.payload.get('tasks', [])
        try:
            if hasattr(self.executor, 'parallel_execute'):
                results = self.executor.parallel_execute(tasks)
                return msg.reply({'results': results})
            return msg.error("No parallel execute capability")
        except Exception as e:
            return msg.error(str(e))

    def _handle_get_stats(self, msg: Message) -> Message:
        try:
            if hasattr(self.executor, 'get_stats'):
                stats = self.executor.get_stats()
            else:
                stats = {}
            return msg.reply({'stats': stats})
        except Exception as e:
            return msg.error(str(e))


# =============================================================================
# ORCHESTRATION LAYER
# =============================================================================

class MetaOrchestrator:
    """
    High-level orchestrator that coordinates all systems through the protocol layer.

    Implements complex workflows like:
    - Learn → Plan → Generate → Verify → Execute
    - Continuous self-improvement loops
    - Parallel task distribution
    """

    def __init__(self):
        self.bus = MessageBus()
        self.adapters: Dict[str, SystemAdapter] = {}
        self.workflows: Dict[str, Callable] = {}
        self._setup_workflows()

    def register_system(self, system_id: str, system: Any, adapter_class: type) -> None:
        """Register a system with its adapter."""
        adapter = adapter_class(self.bus, system)
        self.adapters[system_id] = adapter

    def _setup_workflows(self) -> None:
        """Setup standard workflows."""
        self.workflows['learn_and_generate'] = self._workflow_learn_and_generate
        self.workflows['plan_and_execute'] = self._workflow_plan_and_execute
        self.workflows['full_cycle'] = self._workflow_full_cycle
        self.workflows['self_improve'] = self._workflow_self_improve

    def send(self, target: str, action: str, payload: Dict[str, Any]) -> Message:
        """Send a message to a system."""
        message = Message(
            type=MessageType.REQUEST,
            source='orchestrator',
            target=target,
            action=action,
            payload=payload
        )
        return self.bus.request(message)

    def run_workflow(self, workflow_name: str, **kwargs) -> Dict[str, Any]:
        """Run a named workflow."""
        if workflow_name not in self.workflows:
            return {'error': f'Unknown workflow: {workflow_name}'}
        return self.workflows[workflow_name](**kwargs)

    def _workflow_learn_and_generate(self, description: str,
                                      max_files: int = 1000) -> Dict[str, Any]:
        """Workflow: Learn from codebase, then generate code."""
        results = {'steps': []}

        # Step 1: Learn
        learn_response = self.send('learner', 'learn', {'max_files': max_files})
        results['steps'].append({
            'action': 'learn',
            'success': learn_response.type != MessageType.ERROR,
            'result': learn_response.payload
        })

        if learn_response.type == MessageType.ERROR:
            results['error'] = learn_response.payload.get('error')
            return results

        # Step 2: Generate
        gen_response = self.send('generator', 'generate', {
            'description': description,
            'verify': True
        })
        results['steps'].append({
            'action': 'generate',
            'success': gen_response.type != MessageType.ERROR,
            'result': gen_response.payload
        })

        results['success'] = all(s['success'] for s in results['steps'])
        return results

    def _workflow_plan_and_execute(self, goal_id: str = 'ultimate') -> Dict[str, Any]:
        """Workflow: Create a plan and execute it step by step."""
        results = {'steps': [], 'plan': []}

        # Step 1: Get plan
        plan_response = self.send('planner', 'plan', {'goal_id': goal_id})
        if plan_response.type == MessageType.ERROR:
            return {'error': plan_response.payload.get('error')}

        plan = plan_response.payload.get('plan', [])
        results['plan'] = plan

        # Step 2: Execute each step (if executor available)
        if 'executor' in self.adapters:
            for step in plan:
                exec_response = self.send('executor', 'execute', {
                    'task': f"Execute: {step['action']}"
                })
                results['steps'].append({
                    'action': step['action'],
                    'success': exec_response.type != MessageType.ERROR,
                    'result': exec_response.payload
                })

        results['success'] = True
        return results

    def _workflow_full_cycle(self, task: str, max_files: int = 500) -> Dict[str, Any]:
        """Workflow: Full cycle - Learn → Plan → Generate → Verify → Report."""
        results = {'phases': {}}

        # Phase 1: Learn
        learn_resp = self.send('learner', 'learn', {'max_files': max_files})
        results['phases']['learn'] = {
            'success': learn_resp.type != MessageType.ERROR,
            'data': learn_resp.payload
        }

        # Phase 2: Get patterns for context
        patterns_resp = self.send('learner', 'get_patterns', {'type': 'all'})
        results['phases']['patterns'] = {
            'success': patterns_resp.type != MessageType.ERROR,
            'data': patterns_resp.payload
        }

        # Phase 3: Generate code
        gen_resp = self.send('generator', 'generate', {
            'description': task,
            'verify': True
        })
        results['phases']['generate'] = {
            'success': gen_resp.type != MessageType.ERROR,
            'data': gen_resp.payload
        }

        # Phase 4: Get development plan status
        if 'planner' in self.adapters:
            plan_resp = self.send('planner', 'get_development_plan', {})
            results['phases']['plan'] = {
                'success': plan_resp.type != MessageType.ERROR,
                'data': plan_resp.payload
            }

        results['success'] = all(
            p.get('success', False) for p in results['phases'].values()
        )
        return results

    def _workflow_self_improve(self) -> Dict[str, Any]:
        """Workflow: Self-improvement cycle based on GOAP planning."""
        results = {'iterations': [], 'improvements': []}

        # Get current capabilities assessment
        capabilities = {
            'file_read': True,
            'file_write': True,
            'code_exec': True,
            'llm_access': True,
            'knowledge_graph': True,
            'patterns_learned': 0,
            'functions_known': 0,
            'classes_known': 0
        }

        # Update from learner if available
        if 'learner' in self.adapters:
            knowledge_resp = self.send('learner', 'get_knowledge', {})
            if knowledge_resp.type != MessageType.ERROR:
                k = knowledge_resp.payload.get('knowledge', {})
                capabilities['patterns_learned'] = k.get('patterns', 0)
                capabilities['functions_known'] = k.get('functions', 0)
                capabilities['classes_known'] = k.get('classes', 0)

        # Assess state
        if 'planner' in self.adapters:
            state_resp = self.send('planner', 'assess_state', {
                'capabilities': capabilities
            })
            results['current_state'] = state_resp.payload

            # Get improvement plan
            plan_resp = self.send('planner', 'get_development_plan', {})
            if plan_resp.type != MessageType.ERROR:
                results['improvement_plan'] = plan_resp.payload

        results['success'] = True
        return results

    def get_system_status(self) -> Dict[str, Any]:
        """Get status of all registered systems."""
        status = {}
        for system_id in self.adapters:
            status[system_id] = {
                'registered': True,
                'adapter_type': type(self.adapters[system_id]).__name__
            }
        return status


# =============================================================================
# UNIFIED RUNTIME
# =============================================================================

class UnifiedRuntime:
    """
    Complete runtime that initializes and connects all systems.

    Usage:
        runtime = UnifiedRuntime()
        runtime.initialize()
        result = runtime.execute("Generate a task scheduler class")
    """

    def __init__(self, knowledge_db: Optional[Path] = None):
        self.db_path = knowledge_db or Path('.swarm_test/knowledge/knowledge.db')
        self.orchestrator = MetaOrchestrator()
        self.initialized = False
        self._systems = {}

    def initialize(self) -> Dict[str, Any]:
        """Initialize all systems and connect them."""
        results = {'systems': {}}

        # Initialize AutonomousCoder
        try:
            from autonomous_coder import AutonomousCoder
            coder = AutonomousCoder(self.db_path)
            self._systems['coder'] = coder
            self.orchestrator.register_system('learner', coder, LearnerAdapter)
            self.orchestrator.register_system('generator', coder, GeneratorAdapter)
            results['systems']['coder'] = 'initialized'
        except Exception as e:
            results['systems']['coder'] = f'error: {e}'

        # Initialize GOAP Planner
        try:
            from goap_agent_planner import AgentDevelopmentPlanner
            planner = AgentDevelopmentPlanner()
            self._systems['planner'] = planner
            self.orchestrator.register_system('planner', planner, PlannerAdapter)
            results['systems']['planner'] = 'initialized'
        except Exception as e:
            results['systems']['planner'] = f'error: {e}'

        # Initialize Parallel Runtime
        try:
            from parallel_agent_runtime import ParallelOrchestrator
            executor = ParallelOrchestrator(max_workers=8)
            self._systems['executor'] = executor
            self.orchestrator.register_system('executor', executor, ExecutorAdapter)
            results['systems']['executor'] = 'initialized'
        except Exception as e:
            results['systems']['executor'] = f'error: {e}'

        self.initialized = True
        results['status'] = 'ready'
        return results

    def learn(self, max_files: int = 1000) -> Dict[str, Any]:
        """Trigger learning across systems."""
        if not self.initialized:
            self.initialize()
        return self.orchestrator.send('learner', 'learn', {'max_files': max_files}).payload

    def generate(self, description: str) -> Dict[str, Any]:
        """Generate code based on description."""
        if not self.initialized:
            self.initialize()
        return self.orchestrator.send('generator', 'generate', {
            'description': description,
            'verify': True
        }).payload

    def plan(self, goal: str = 'ultimate') -> Dict[str, Any]:
        """Get development plan for a goal."""
        if not self.initialized:
            self.initialize()
        return self.orchestrator.send('planner', 'get_development_plan', {}).payload

    def execute(self, task: str) -> Dict[str, Any]:
        """Execute a task through the unified system."""
        if not self.initialized:
            self.initialize()
        return self.orchestrator.run_workflow('full_cycle', task=task)

    def self_improve(self) -> Dict[str, Any]:
        """Run self-improvement cycle."""
        if not self.initialized:
            self.initialize()
        return self.orchestrator.run_workflow('self_improve')

    def get_status(self) -> Dict[str, Any]:
        """Get status of all systems."""
        return {
            'initialized': self.initialized,
            'systems': self.orchestrator.get_system_status(),
            'message_history': len(self.orchestrator.bus.get_history())
        }


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("META-PROTOCOL LAYER")
    print("=" * 70)
    print()

    # Initialize unified runtime
    runtime = UnifiedRuntime()

    print("1. INITIALIZING SYSTEMS")
    print("-" * 50)
    init_result = runtime.initialize()
    for system, status in init_result['systems'].items():
        print(f"  {system}: {status}")
    print()

    print("2. SYSTEM STATUS")
    print("-" * 50)
    status = runtime.get_status()
    print(f"  Initialized: {status['initialized']}")
    print(f"  Systems: {list(status['systems'].keys())}")
    print()

    print("3. LEARNING PHASE")
    print("-" * 50)
    learn_result = runtime.learn(max_files=500)
    if 'result' in learn_result:
        r = learn_result['result']
        print(f"  Files: {r.get('files_analyzed', 'N/A')}")
        print(f"  Functions: {r.get('functions_learned', 'N/A')}")
        print(f"  Classes: {r.get('classes_learned', 'N/A')}")
    print()

    print("4. DEVELOPMENT PLAN")
    print("-" * 50)
    plan_result = runtime.plan()
    if 'plan' in plan_result:
        print(f"  Steps to self-improvement: {len(plan_result['plan'])}")
        print(f"  Total cost: {plan_result.get('total_cost', 'N/A')}")
        for i, step in enumerate(plan_result['plan'][:5], 1):
            print(f"    {i}. {step['action']}")
        if len(plan_result['plan']) > 5:
            print(f"    ... and {len(plan_result['plan']) - 5} more steps")
    print()

    print("5. CODE GENERATION")
    print("-" * 50)
    gen_result = runtime.generate(
        "Create a message queue class with publish, subscribe, and ack methods"
    )
    if gen_result and gen_result.get('code'):
        print(f"  Generated: {len(gen_result['code'])} chars")
        print(f"  Valid: {gen_result.get('valid', 'N/A')}")
        if gen_result.get('verification'):
            for check, result in gen_result['verification'].items():
                if isinstance(result, dict):
                    status = 'PASS' if result.get('passed') else 'FAIL'
                    print(f"    {check}: {status}")
    elif gen_result and gen_result.get('error'):
        print(f"  Error: {gen_result.get('error')}")
    else:
        print(f"  Result: {gen_result}")
    print()

    print("6. SELF-IMPROVEMENT ASSESSMENT")
    print("-" * 50)
    improve_result = runtime.self_improve()
    if 'current_state' in improve_result:
        state = improve_result['current_state'].get('state', {})
        print(f"  Current capabilities: {len(state)} states tracked")
        for key, value in list(state.items())[:5]:
            print(f"    {key}: {value}")
    if 'improvement_plan' in improve_result:
        plan = improve_result['improvement_plan']
        print(f"  Improvement steps available: {plan.get('steps', 'N/A')}")
    print()

    print("7. MESSAGE BUS STATISTICS")
    print("-" * 50)
    history = runtime.orchestrator.bus.get_history()
    print(f"  Total messages: {len(history)}")
    msg_types = {}
    for msg in history:
        msg_types[msg.type.name] = msg_types.get(msg.type.name, 0) + 1
    for mtype, count in msg_types.items():
        print(f"    {mtype}: {count}")
    print()

    print("INTER-OPERATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
