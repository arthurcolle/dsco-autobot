"""
Intent → DAG Compiler

Compiles natural language intent into executable tool DAGs.
Uses semantic search to find tools, type algebra for validation,
and execution memory for optimization.
"""

from __future__ import annotations
import asyncio
import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any, Dict, List, Optional, Set, Tuple, Callable, Union,
    TYPE_CHECKING
)
from collections import defaultdict
import hashlib
import json

if TYPE_CHECKING:
    from .algebra import ToolAlgebra, ToolSignature, ToolType
    from .embeddings import ToolIndex
    from .memory import ExecutionMemory


class NodeType(Enum):
    """Types of nodes in the execution DAG."""
    TOOL = auto()       # Execute a tool
    PARALLEL = auto()   # Execute children in parallel
    SEQUENCE = auto()   # Execute children in sequence
    CHOICE = auto()     # Execute one of children based on condition
    MAP = auto()        # Apply tool to each element
    FILTER = auto()     # Filter elements by predicate
    REDUCE = auto()     # Reduce elements to single value
    TRANSFORM = auto()  # Apply type coercion
    INPUT = auto()      # DAG input node
    OUTPUT = auto()     # DAG output node


@dataclass
class DAGNode:
    """Node in the execution DAG."""
    id: str
    node_type: NodeType
    tool_id: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    children: List[str] = field(default_factory=list)
    parents: List[str] = field(default_factory=list)
    condition: Optional[str] = None
    transform: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Computed during optimization
    estimated_latency_ms: float = 0.0
    estimated_cost: float = 0.0
    parallelizable: bool = True

    def __hash__(self):
        return hash(self.id)


@dataclass
class ExecutionDAG:
    """Compiled execution DAG."""
    id: str
    intent: str
    nodes: Dict[str, DAGNode] = field(default_factory=dict)
    edges: List[Tuple[str, str]] = field(default_factory=list)
    input_node: Optional[str] = None
    output_node: Optional[str] = None

    # Analysis results
    critical_path: List[str] = field(default_factory=list)
    total_estimated_latency_ms: float = 0.0
    total_estimated_cost: float = 0.0
    parallelism_factor: float = 1.0

    def add_node(self, node: DAGNode) -> None:
        """Add node to DAG."""
        self.nodes[node.id] = node

    def add_edge(self, from_id: str, to_id: str) -> None:
        """Add edge between nodes."""
        self.edges.append((from_id, to_id))
        if to_id not in self.nodes[from_id].children:
            self.nodes[from_id].children.append(to_id)
        if from_id not in self.nodes[to_id].parents:
            self.nodes[to_id].parents.append(from_id)

    def topological_sort(self) -> List[str]:
        """Return nodes in topological order."""
        in_degree = {n: 0 for n in self.nodes}
        for _, to_id in self.edges:
            in_degree[to_id] += 1

        queue = [n for n, d in in_degree.items() if d == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for child in self.nodes[node].children:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        return result

    def get_parallel_groups(self) -> List[List[str]]:
        """Group nodes that can execute in parallel."""
        levels: Dict[str, int] = {}

        # Compute level for each node
        for node_id in self.topological_sort():
            if not self.nodes[node_id].parents:
                levels[node_id] = 0
            else:
                levels[node_id] = max(
                    levels[p] for p in self.nodes[node_id].parents
                ) + 1

        # Group by level
        groups: Dict[int, List[str]] = defaultdict(list)
        for node_id, level in levels.items():
            if self.nodes[node_id].parallelizable:
                groups[level].append(node_id)

        return [groups[i] for i in sorted(groups.keys())]

    def to_dict(self) -> Dict[str, Any]:
        """Serialize DAG to dict."""
        return {
            "id": self.id,
            "intent": self.intent,
            "nodes": {
                nid: {
                    "id": n.id,
                    "type": n.node_type.name,
                    "tool_id": n.tool_id,
                    "parameters": n.parameters,
                    "children": n.children,
                    "parents": n.parents,
                    "estimated_latency_ms": n.estimated_latency_ms,
                    "estimated_cost": n.estimated_cost,
                }
                for nid, n in self.nodes.items()
            },
            "edges": self.edges,
            "critical_path": self.critical_path,
            "total_estimated_latency_ms": self.total_estimated_latency_ms,
            "total_estimated_cost": self.total_estimated_cost,
            "parallelism_factor": self.parallelism_factor,
        }


@dataclass
class IntentSlot:
    """A slot in parsed intent."""
    name: str
    value: Optional[str] = None
    slot_type: str = "any"
    required: bool = True
    alternatives: List[str] = field(default_factory=list)


@dataclass
class ParsedIntent:
    """Parsed natural language intent."""
    raw: str
    action: str
    objects: List[str] = field(default_factory=list)
    modifiers: List[str] = field(default_factory=list)
    slots: Dict[str, IntentSlot] = field(default_factory=dict)
    sub_intents: List['ParsedIntent'] = field(default_factory=list)
    confidence: float = 1.0


class IntentParser:
    """Parse natural language into structured intent."""

    # Action patterns
    ACTION_PATTERNS = [
        (r"\b(get|fetch|retrieve|load|read)\b", "retrieve"),
        (r"\b(create|make|generate|build|add)\b", "create"),
        (r"\b(update|modify|change|edit|set)\b", "update"),
        (r"\b(delete|remove|drop|clear)\b", "delete"),
        (r"\b(search|find|look\s+for|query)\b", "search"),
        (r"\b(transform|convert|map|process)\b", "transform"),
        (r"\b(combine|merge|join|aggregate)\b", "combine"),
        (r"\b(filter|select|pick|choose)\b", "filter"),
        (r"\b(sort|order|rank|arrange)\b", "sort"),
        (r"\b(validate|check|verify|test)\b", "validate"),
        (r"\b(send|post|push|publish)\b", "send"),
        (r"\b(analyze|examine|inspect)\b", "analyze"),
    ]

    # Modifier patterns
    MODIFIER_PATTERNS = [
        (r"\b(all|every|each)\b", "all"),
        (r"\b(first|top|latest)\b", "first"),
        (r"\b(last|bottom|oldest)\b", "last"),
        (r"\b(unique|distinct)\b", "unique"),
        (r"\b(random|any)\b", "random"),
        (r"\b(async|parallel|concurrent)\b", "parallel"),
        (r"\b(sync|sequential|ordered)\b", "sequential"),
        (r"\b(cached|memoized)\b", "cached"),
        (r"\b(retry|resilient)\b", "retry"),
    ]

    # Connector patterns for sub-intents
    CONNECTOR_PATTERNS = [
        (r"\bthen\b", "sequence"),
        (r"\band\s+then\b", "sequence"),
        (r"\bafter\s+that\b", "sequence"),
        (r"\band\b", "parallel"),
        (r"\balso\b", "parallel"),
        (r"\bwhile\b", "parallel"),
        (r"\bor\b", "choice"),
        (r"\botherwise\b", "choice"),
        (r"\bfor\s+each\b", "map"),
        (r"\bwhere\b", "filter"),
    ]

    def parse(self, intent: str) -> ParsedIntent:
        """Parse natural language intent."""
        intent = intent.strip().lower()

        # Split on connectors
        sub_intents = self._split_on_connectors(intent)

        if len(sub_intents) > 1:
            # Compound intent
            parsed_subs = [self._parse_simple(s) for s in sub_intents]
            return ParsedIntent(
                raw=intent,
                action="compound",
                sub_intents=parsed_subs,
                confidence=min(p.confidence for p in parsed_subs),
            )

        return self._parse_simple(intent)

    def _split_on_connectors(self, intent: str) -> List[str]:
        """Split intent on connector words."""
        # Find all connector positions
        splits = []
        for pattern, _ in self.CONNECTOR_PATTERNS:
            for match in re.finditer(pattern, intent):
                splits.append((match.start(), match.end(), match.group()))

        if not splits:
            return [intent]

        # Sort by position
        splits.sort()

        # Extract segments
        segments = []
        last_end = 0
        for start, end, _ in splits:
            if start > last_end:
                segment = intent[last_end:start].strip()
                if segment:
                    segments.append(segment)
            last_end = end

        # Add final segment
        if last_end < len(intent):
            segment = intent[last_end:].strip()
            if segment:
                segments.append(segment)

        return segments if segments else [intent]

    def _parse_simple(self, intent: str) -> ParsedIntent:
        """Parse a simple (non-compound) intent."""
        action = "unknown"
        confidence = 0.5

        # Extract action
        for pattern, action_type in self.ACTION_PATTERNS:
            if re.search(pattern, intent):
                action = action_type
                confidence = 0.8
                break

        # Extract modifiers
        modifiers = []
        for pattern, modifier in self.MODIFIER_PATTERNS:
            if re.search(pattern, intent):
                modifiers.append(modifier)

        # Extract objects (nouns) - simple heuristic
        objects = self._extract_objects(intent)

        # Extract slots (key=value patterns)
        slots = self._extract_slots(intent)

        return ParsedIntent(
            raw=intent,
            action=action,
            objects=objects,
            modifiers=modifiers,
            slots=slots,
            confidence=confidence,
        )

    def _extract_objects(self, intent: str) -> List[str]:
        """Extract object nouns from intent."""
        # Remove action words
        cleaned = intent
        for pattern, _ in self.ACTION_PATTERNS:
            cleaned = re.sub(pattern, "", cleaned)
        for pattern, _ in self.MODIFIER_PATTERNS:
            cleaned = re.sub(pattern, "", cleaned)

        # Extract remaining words as objects
        words = re.findall(r'\b[a-z_]+\b', cleaned)
        # Filter common words
        stopwords = {'the', 'a', 'an', 'to', 'from', 'in', 'on', 'at', 'by', 'with', 'for', 'of', 'it', 'them'}
        return [w for w in words if w not in stopwords and len(w) > 2]

    def _extract_slots(self, intent: str) -> Dict[str, IntentSlot]:
        """Extract slot=value patterns."""
        slots = {}

        # Pattern: key=value or key:value
        for match in re.finditer(r'(\w+)\s*[=:]\s*["\']?([^"\'\s,]+)["\']?', intent):
            key, value = match.groups()
            slots[key] = IntentSlot(name=key, value=value)

        # Pattern: key "value" or key 'value'
        for match in re.finditer(r'(\w+)\s+["\']([^"\']+)["\']', intent):
            key, value = match.groups()
            slots[key] = IntentSlot(name=key, value=value)

        return slots


class DAGCompiler:
    """Compile parsed intent into executable DAG."""

    def __init__(
        self,
        tool_index: Optional['ToolIndex'] = None,
        tool_algebra: Optional['ToolAlgebra'] = None,
        execution_memory: Optional['ExecutionMemory'] = None,
    ):
        self.tool_index = tool_index
        self.tool_algebra = tool_algebra
        self.memory = execution_memory
        self.parser = IntentParser()
        self._node_counter = 0

    def _new_node_id(self) -> str:
        """Generate unique node ID."""
        self._node_counter += 1
        return f"node_{self._node_counter}"

    async def compile(
        self,
        intent: str,
        context: Optional[Dict[str, Any]] = None,
        constraints: Optional[Dict[str, Any]] = None,
    ) -> ExecutionDAG:
        """Compile intent into execution DAG."""
        # Parse intent
        parsed = self.parser.parse(intent)

        # Create DAG
        dag_id = hashlib.md5(intent.encode()).hexdigest()[:12]
        dag = ExecutionDAG(id=dag_id, intent=intent)

        # Create input node
        input_node = DAGNode(
            id=self._new_node_id(),
            node_type=NodeType.INPUT,
            metadata={"context": context or {}},
        )
        dag.add_node(input_node)
        dag.input_node = input_node.id

        # Compile intent tree
        last_node_id = await self._compile_intent(dag, parsed, input_node.id)

        # Create output node
        output_node = DAGNode(
            id=self._new_node_id(),
            node_type=NodeType.OUTPUT,
        )
        dag.add_node(output_node)
        dag.output_node = output_node.id
        dag.add_edge(last_node_id, output_node.id)

        # Optimize DAG
        await self._optimize_dag(dag, constraints)

        # Analyze DAG
        self._analyze_dag(dag)

        return dag

    async def _compile_intent(
        self,
        dag: ExecutionDAG,
        parsed: ParsedIntent,
        parent_id: str,
    ) -> str:
        """Compile parsed intent into DAG nodes. Returns last node ID."""
        if parsed.action == "compound":
            return await self._compile_compound(dag, parsed, parent_id)

        return await self._compile_simple(dag, parsed, parent_id)

    async def _compile_compound(
        self,
        dag: ExecutionDAG,
        parsed: ParsedIntent,
        parent_id: str,
    ) -> str:
        """Compile compound intent with sub-intents."""
        # Determine relationship between sub-intents
        if "parallel" in parsed.modifiers:
            # Create parallel node
            parallel_node = DAGNode(
                id=self._new_node_id(),
                node_type=NodeType.PARALLEL,
            )
            dag.add_node(parallel_node)
            dag.add_edge(parent_id, parallel_node.id)

            # Compile each sub-intent in parallel
            child_ids = []
            for sub in parsed.sub_intents:
                child_id = await self._compile_intent(dag, sub, parallel_node.id)
                child_ids.append(child_id)

            # Create join node
            join_node = DAGNode(
                id=self._new_node_id(),
                node_type=NodeType.SEQUENCE,
                metadata={"join": True},
            )
            dag.add_node(join_node)
            for child_id in child_ids:
                dag.add_edge(child_id, join_node.id)

            return join_node.id
        else:
            # Sequential by default
            current_id = parent_id
            for sub in parsed.sub_intents:
                current_id = await self._compile_intent(dag, sub, current_id)
            return current_id

    async def _compile_simple(
        self,
        dag: ExecutionDAG,
        parsed: ParsedIntent,
        parent_id: str,
    ) -> str:
        """Compile simple intent into tool nodes."""
        # Find relevant tools
        tools = await self._find_tools(parsed)

        if not tools:
            # No tools found - create placeholder
            node = DAGNode(
                id=self._new_node_id(),
                node_type=NodeType.TOOL,
                tool_id="__unknown__",
                metadata={"parsed": parsed.raw, "error": "no_tools_found"},
            )
            dag.add_node(node)
            dag.add_edge(parent_id, node.id)
            return node.id

        # Handle modifiers
        if "map" in parsed.modifiers or "all" in parsed.modifiers:
            return await self._compile_map(dag, tools, parsed, parent_id)

        if "filter" in parsed.modifiers:
            return await self._compile_filter(dag, tools, parsed, parent_id)

        # Simple tool execution
        tool_id, score = tools[0]
        node = DAGNode(
            id=self._new_node_id(),
            node_type=NodeType.TOOL,
            tool_id=tool_id,
            parameters=self._extract_parameters(parsed),
            metadata={"match_score": score},
        )
        dag.add_node(node)
        dag.add_edge(parent_id, node.id)

        # Check if we need type transformations
        if self.tool_algebra and len(tools) > 1:
            # Try to compose with next tool
            next_tool_id, _ = tools[1]
            transform = await self._maybe_add_transform(
                dag, node.id, tool_id, next_tool_id
            )
            if transform:
                return transform

        return node.id

    async def _compile_map(
        self,
        dag: ExecutionDAG,
        tools: List[Tuple[str, float]],
        parsed: ParsedIntent,
        parent_id: str,
    ) -> str:
        """Compile map operation."""
        map_node = DAGNode(
            id=self._new_node_id(),
            node_type=NodeType.MAP,
            tool_id=tools[0][0] if tools else None,
            parameters=self._extract_parameters(parsed),
        )
        dag.add_node(map_node)
        dag.add_edge(parent_id, map_node.id)
        return map_node.id

    async def _compile_filter(
        self,
        dag: ExecutionDAG,
        tools: List[Tuple[str, float]],
        parsed: ParsedIntent,
        parent_id: str,
    ) -> str:
        """Compile filter operation."""
        filter_node = DAGNode(
            id=self._new_node_id(),
            node_type=NodeType.FILTER,
            tool_id=tools[0][0] if tools else None,
            condition=parsed.slots.get("where", IntentSlot("where")).value,
            parameters=self._extract_parameters(parsed),
        )
        dag.add_node(filter_node)
        dag.add_edge(parent_id, filter_node.id)
        return filter_node.id

    async def _find_tools(
        self,
        parsed: ParsedIntent,
    ) -> List[Tuple[str, float]]:
        """Find tools matching parsed intent."""
        if not self.tool_index:
            return []

        # Build search query from intent
        query = f"{parsed.action} {' '.join(parsed.objects)}"

        # Search for tools
        results = await self.tool_index.search(query, k=5, threshold=0.3)

        # Convert SearchResult objects to (tool_id, score) tuples
        result_tuples = [(r.tool_id, r.score) for r in results]

        # Apply memory-based reranking if available
        if self.memory and result_tuples:
            reranked = []
            for tool_id, score in result_tuples:
                # Boost/penalize based on past performance
                failure_prob = self.memory.predict_failure_probability(tool_id)
                adjusted_score = score * (1.0 - failure_prob * 0.5)
                reranked.append((tool_id, adjusted_score))
            result_tuples = sorted(reranked, key=lambda x: -x[1])

        return result_tuples

    async def _maybe_add_transform(
        self,
        dag: ExecutionDAG,
        after_node_id: str,
        from_tool_id: str,
        to_tool_id: str,
    ) -> Optional[str]:
        """Add transform node if type coercion needed."""
        if not self.tool_algebra:
            return None

        from_sig = self.tool_algebra.get_signature(from_tool_id)
        to_sig = self.tool_algebra.get_signature(to_tool_id)

        if not from_sig or not to_sig:
            return None

        # Check compatibility
        from .algebra import TypeChecker
        checker = TypeChecker()
        compat = checker.check_compatibility(from_sig.output_type, to_sig.input_type)

        from .algebra import Compatibility
        if compat.level == Compatibility.COERCIBLE:
            # Add transform node
            transform_node = DAGNode(
                id=self._new_node_id(),
                node_type=NodeType.TRANSFORM,
                metadata={"coercion": compat.coercion},
            )
            dag.add_node(transform_node)
            dag.add_edge(after_node_id, transform_node.id)
            return transform_node.id

        return None

    def _extract_parameters(self, parsed: ParsedIntent) -> Dict[str, Any]:
        """Extract tool parameters from parsed intent."""
        params = {}
        for name, slot in parsed.slots.items():
            if slot.value is not None:
                params[name] = slot.value
        return params

    async def _optimize_dag(
        self,
        dag: ExecutionDAG,
        constraints: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Optimize the DAG for execution."""
        constraints = constraints or {}

        # Mark parallelizable nodes
        for node_id in dag.nodes:
            node = dag.nodes[node_id]
            if node.node_type == NodeType.TOOL and node.tool_id:
                # Check if tool supports parallel execution
                if self.memory:
                    stats = self.memory.get_tool_stats(node.tool_id)
                    if stats and stats.total_executions > 10:
                        # Use stats to estimate
                        node.estimated_latency_ms = stats.ema_latency_ms
                        node.estimated_cost = stats.total_cost / stats.total_executions

        # Apply constraints
        max_parallelism = constraints.get("max_parallelism", float("inf"))
        if max_parallelism < float("inf"):
            groups = dag.get_parallel_groups()
            for group in groups:
                if len(group) > max_parallelism:
                    # Mark excess nodes as non-parallelizable
                    for node_id in group[int(max_parallelism):]:
                        dag.nodes[node_id].parallelizable = False

        # Find cheaper alternatives if cost-constrained
        max_cost = constraints.get("max_cost")
        if max_cost and self.memory:
            total_cost = sum(n.estimated_cost for n in dag.nodes.values())
            if total_cost > max_cost:
                # Try to find cheaper tools
                for node_id, node in dag.nodes.items():
                    if node.tool_id and node.estimated_cost > 0:
                        alt = self.memory.find_cheaper_alternative(node.tool_id)
                        if alt:
                            alt_id, savings = alt
                            node.metadata["alternative"] = alt_id
                            node.metadata["potential_savings"] = savings

    def _analyze_dag(self, dag: ExecutionDAG) -> None:
        """Analyze DAG metrics."""
        # Find critical path (longest path)
        distances: Dict[str, float] = {}
        predecessors: Dict[str, str] = {}

        for node_id in dag.topological_sort():
            node = dag.nodes[node_id]
            if not node.parents:
                distances[node_id] = node.estimated_latency_ms
            else:
                max_parent = max(node.parents, key=lambda p: distances.get(p, 0))
                distances[node_id] = distances[max_parent] + node.estimated_latency_ms
                predecessors[node_id] = max_parent

        # Reconstruct critical path
        if dag.output_node:
            path = []
            current = dag.output_node
            while current:
                path.append(current)
                current = predecessors.get(current)
            dag.critical_path = list(reversed(path))

        # Calculate totals
        dag.total_estimated_latency_ms = max(distances.values()) if distances else 0
        dag.total_estimated_cost = sum(n.estimated_cost for n in dag.nodes.values())

        # Calculate parallelism factor
        groups = dag.get_parallel_groups()
        if groups:
            max_parallel = max(len(g) for g in groups)
            dag.parallelism_factor = max_parallel


class IntentDAGCompiler:
    """
    High-level compiler that integrates intent parsing, tool search,
    type algebra, and execution memory into a unified compilation pipeline.
    """

    def __init__(
        self,
        tool_index: Optional['ToolIndex'] = None,
        tool_algebra: Optional['ToolAlgebra'] = None,
        execution_memory: Optional['ExecutionMemory'] = None,
    ):
        self.compiler = DAGCompiler(tool_index, tool_algebra, execution_memory)
        self.tool_index = tool_index
        self.tool_algebra = tool_algebra
        self.memory = execution_memory

    async def compile(
        self,
        intent: str,
        context: Optional[Dict[str, Any]] = None,
        constraints: Optional[Dict[str, Any]] = None,
    ) -> ExecutionDAG:
        """
        Compile natural language intent into executable DAG.

        Args:
            intent: Natural language description of what to do
            context: Optional context (current values, environment, etc.)
            constraints: Optional constraints (max_cost, max_latency, etc.)

        Returns:
            ExecutionDAG ready for execution
        """
        return await self.compiler.compile(intent, context, constraints)

    async def compile_with_validation(
        self,
        intent: str,
        input_type: Optional['ToolType'] = None,
        output_type: Optional['ToolType'] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Tuple[ExecutionDAG, List[str]]:
        """
        Compile with type validation.

        Returns:
            Tuple of (DAG, validation_errors)
        """
        dag = await self.compile(intent, context)
        errors = []

        if not self.tool_algebra:
            return dag, errors

        # Validate type flow through DAG
        for node_id in dag.topological_sort():
            node = dag.nodes[node_id]
            if node.node_type != NodeType.TOOL or not node.tool_id:
                continue

            sig = self.tool_algebra.get_signature(node.tool_id)
            if not sig:
                errors.append(f"Unknown tool: {node.tool_id}")
                continue

            # Check input compatibility from parents
            for parent_id in node.parents:
                parent = dag.nodes[parent_id]
                if parent.tool_id:
                    parent_sig = self.tool_algebra.get_signature(parent.tool_id)
                    if parent_sig:
                        from .algebra import TypeChecker, Compatibility
                        checker = TypeChecker()
                        compat = checker.check_compatibility(
                            parent_sig.output_type, sig.input_type
                        )
                        if compat.level == Compatibility.INCOMPATIBLE:
                            errors.append(
                                f"Type mismatch: {parent.tool_id} output → {node.tool_id} input"
                            )

        return dag, errors

    async def explain(self, dag: ExecutionDAG) -> str:
        """Generate human-readable explanation of the DAG."""
        lines = [
            f"Execution Plan for: {dag.intent}",
            f"  Total estimated latency: {dag.total_estimated_latency_ms:.0f}ms",
            f"  Total estimated cost: ${dag.total_estimated_cost:.4f}",
            f"  Parallelism factor: {dag.parallelism_factor:.1f}x",
            "",
            "Steps:",
        ]

        groups = dag.get_parallel_groups()
        for i, group in enumerate(groups, 1):
            if len(group) == 1:
                node = dag.nodes[group[0]]
                lines.append(f"  {i}. {self._describe_node(node)}")
            else:
                lines.append(f"  {i}. In parallel:")
                for node_id in group:
                    node = dag.nodes[node_id]
                    lines.append(f"     - {self._describe_node(node)}")

        if dag.critical_path:
            lines.extend([
                "",
                "Critical path: " + " → ".join(
                    self._short_name(dag.nodes[n]) for n in dag.critical_path
                ),
            ])

        return "\n".join(lines)

    def _describe_node(self, node: DAGNode) -> str:
        """Describe a single node."""
        if node.node_type == NodeType.INPUT:
            return "Receive input"
        elif node.node_type == NodeType.OUTPUT:
            return "Return output"
        elif node.node_type == NodeType.TOOL:
            params = ", ".join(f"{k}={v}" for k, v in node.parameters.items())
            return f"Execute {node.tool_id}({params})"
        elif node.node_type == NodeType.MAP:
            return f"Map {node.tool_id} over each element"
        elif node.node_type == NodeType.FILTER:
            return f"Filter where {node.condition}"
        elif node.node_type == NodeType.TRANSFORM:
            return f"Transform: {node.metadata.get('coercion', 'convert')}"
        else:
            return f"{node.node_type.name}"

    def _short_name(self, node: DAGNode) -> str:
        """Short name for node."""
        if node.tool_id:
            return node.tool_id
        return node.node_type.name.lower()
