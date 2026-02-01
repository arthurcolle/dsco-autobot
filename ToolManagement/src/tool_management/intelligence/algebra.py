"""
Tool Algebra - Formal type system for tool composition.

Provides:
- Type signatures for tools (input/output schemas)
- Compatibility checking between tools
- Automatic type coercion rules
- Composition operators (sequence, parallel, choice)
- Constraint propagation for valid DAG construction
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable
from pydantic import BaseModel
import hashlib
import json


class TypeKind(str, Enum):
    """Primitive type kinds."""
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    NULL = "null"
    ANY = "any"
    UNION = "union"


@dataclass
class ToolType:
    """
    Type representation for tool inputs/outputs.

    Supports:
    - Primitives: string, number, integer, boolean
    - Compounds: array<T>, object{fields}
    - Special: any, null, union<T1, T2, ...>
    """
    kind: TypeKind
    element_type: Optional[ToolType] = None  # For arrays
    fields: Dict[str, ToolType] = field(default_factory=dict)  # For objects
    required_fields: Set[str] = field(default_factory=set)
    union_types: List[ToolType] = field(default_factory=list)  # For unions
    constraints: Dict[str, Any] = field(default_factory=dict)  # min, max, pattern, etc.

    def __hash__(self):
        return hash(self.signature())

    def signature(self) -> str:
        """Generate unique type signature."""
        if self.kind == TypeKind.ARRAY:
            return f"array<{self.element_type.signature() if self.element_type else 'any'}>"
        elif self.kind == TypeKind.OBJECT:
            fields_sig = ",".join(
                f"{k}:{v.signature()}" + ("!" if k in self.required_fields else "?")
                for k, v in sorted(self.fields.items())
            )
            return f"object{{{fields_sig}}}"
        elif self.kind == TypeKind.UNION:
            return f"union<{','.join(t.signature() for t in self.union_types)}>"
        return self.kind.value

    @classmethod
    def from_json_schema(cls, schema: Dict[str, Any]) -> ToolType:
        """Parse JSON Schema into ToolType."""
        if not schema:
            return cls(kind=TypeKind.ANY)

        schema_type = schema.get("type", "any")

        if schema_type == "string":
            return cls(kind=TypeKind.STRING, constraints={
                k: v for k, v in schema.items()
                if k in ("minLength", "maxLength", "pattern", "enum")
            })
        elif schema_type == "number":
            return cls(kind=TypeKind.NUMBER, constraints={
                k: v for k, v in schema.items()
                if k in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum")
            })
        elif schema_type == "integer":
            return cls(kind=TypeKind.INTEGER, constraints={
                k: v for k, v in schema.items()
                if k in ("minimum", "maximum")
            })
        elif schema_type == "boolean":
            return cls(kind=TypeKind.BOOLEAN)
        elif schema_type == "null":
            return cls(kind=TypeKind.NULL)
        elif schema_type == "array":
            items = schema.get("items", {})
            return cls(
                kind=TypeKind.ARRAY,
                element_type=cls.from_json_schema(items),
                constraints={
                    k: v for k, v in schema.items()
                    if k in ("minItems", "maxItems", "uniqueItems")
                }
            )
        elif schema_type == "object":
            properties = schema.get("properties", {})
            required = set(schema.get("required", []))
            return cls(
                kind=TypeKind.OBJECT,
                fields={k: cls.from_json_schema(v) for k, v in properties.items()},
                required_fields=required
            )
        elif isinstance(schema_type, list):
            # Union type
            return cls(
                kind=TypeKind.UNION,
                union_types=[cls.from_json_schema({"type": t}) for t in schema_type]
            )
        else:
            return cls(kind=TypeKind.ANY)

    def to_json_schema(self) -> Dict[str, Any]:
        """Convert back to JSON Schema."""
        if self.kind == TypeKind.ANY:
            return {}
        elif self.kind == TypeKind.ARRAY:
            schema = {"type": "array"}
            if self.element_type:
                schema["items"] = self.element_type.to_json_schema()
            schema.update(self.constraints)
            return schema
        elif self.kind == TypeKind.OBJECT:
            schema = {
                "type": "object",
                "properties": {k: v.to_json_schema() for k, v in self.fields.items()}
            }
            if self.required_fields:
                schema["required"] = list(self.required_fields)
            return schema
        elif self.kind == TypeKind.UNION:
            return {"type": [t.kind.value for t in self.union_types]}
        else:
            schema = {"type": self.kind.value}
            schema.update(self.constraints)
            return schema


class Compatibility(str, Enum):
    """Type compatibility levels."""
    EXACT = "exact"           # Types match exactly
    COERCIBLE = "coercible"   # Can convert with transformation
    PARTIAL = "partial"       # Some fields match
    INCOMPATIBLE = "incompatible"


@dataclass
class CompatibilityResult:
    """Result of type compatibility check."""
    level: Compatibility
    confidence: float  # 0.0 to 1.0
    coercion_needed: bool
    coercion_path: Optional[str] = None  # e.g., "extract:.summary" or "wrap:array"
    missing_fields: Set[str] = field(default_factory=set)
    extra_fields: Set[str] = field(default_factory=set)


class TypeChecker:
    """
    Check type compatibility and compute coercions.

    Coercion rules:
    - string -> number (if parseable)
    - number -> string (always)
    - T -> array<T> (wrap in array)
    - array<T> -> T (extract first)
    - object{a,b,c} -> object{a,b} (project fields)
    - object.field -> T (extract field)
    """

    # Implicit coercion rules: (from, to) -> coercion_function_name
    COERCIONS: Dict[Tuple[TypeKind, TypeKind], str] = {
        (TypeKind.NUMBER, TypeKind.STRING): "to_string",
        (TypeKind.INTEGER, TypeKind.STRING): "to_string",
        (TypeKind.INTEGER, TypeKind.NUMBER): "to_number",
        (TypeKind.BOOLEAN, TypeKind.STRING): "to_string",
        (TypeKind.STRING, TypeKind.NUMBER): "parse_number",
        (TypeKind.STRING, TypeKind.INTEGER): "parse_integer",
        (TypeKind.STRING, TypeKind.BOOLEAN): "parse_boolean",
    }

    @classmethod
    def check(cls, source: ToolType, target: ToolType) -> CompatibilityResult:
        """Check if source type can satisfy target type."""

        # Any matches anything
        if target.kind == TypeKind.ANY:
            return CompatibilityResult(
                level=Compatibility.EXACT,
                confidence=1.0,
                coercion_needed=False
            )

        if source.kind == TypeKind.ANY:
            return CompatibilityResult(
                level=Compatibility.COERCIBLE,
                confidence=0.5,
                coercion_needed=True,
                coercion_path="cast"
            )

        # Exact match
        if source.kind == target.kind:
            if source.kind == TypeKind.OBJECT:
                return cls._check_object_compat(source, target)
            elif source.kind == TypeKind.ARRAY:
                return cls._check_array_compat(source, target)
            else:
                return CompatibilityResult(
                    level=Compatibility.EXACT,
                    confidence=1.0,
                    coercion_needed=False
                )

        # Check coercion rules
        coercion_key = (source.kind, target.kind)
        if coercion_key in cls.COERCIONS:
            return CompatibilityResult(
                level=Compatibility.COERCIBLE,
                confidence=0.8,
                coercion_needed=True,
                coercion_path=cls.COERCIONS[coercion_key]
            )

        # T -> array<T>
        if target.kind == TypeKind.ARRAY and target.element_type:
            inner_result = cls.check(source, target.element_type)
            if inner_result.level in (Compatibility.EXACT, Compatibility.COERCIBLE):
                return CompatibilityResult(
                    level=Compatibility.COERCIBLE,
                    confidence=inner_result.confidence * 0.9,
                    coercion_needed=True,
                    coercion_path="wrap:array"
                )

        # array<T> -> T (extract first)
        if source.kind == TypeKind.ARRAY and source.element_type:
            inner_result = cls.check(source.element_type, target)
            if inner_result.level in (Compatibility.EXACT, Compatibility.COERCIBLE):
                return CompatibilityResult(
                    level=Compatibility.COERCIBLE,
                    confidence=inner_result.confidence * 0.7,
                    coercion_needed=True,
                    coercion_path="extract:first"
                )

        # object.field extraction
        if source.kind == TypeKind.OBJECT and source.fields:
            for field_name, field_type in source.fields.items():
                inner_result = cls.check(field_type, target)
                if inner_result.level == Compatibility.EXACT:
                    return CompatibilityResult(
                        level=Compatibility.COERCIBLE,
                        confidence=0.9,
                        coercion_needed=True,
                        coercion_path=f"extract:.{field_name}"
                    )

        # Union source - check if any member matches
        if source.kind == TypeKind.UNION:
            best_result = None
            for member in source.union_types:
                result = cls.check(member, target)
                if result.level == Compatibility.EXACT:
                    return result
                if best_result is None or result.confidence > best_result.confidence:
                    best_result = result
            if best_result and best_result.level != Compatibility.INCOMPATIBLE:
                return best_result

        return CompatibilityResult(
            level=Compatibility.INCOMPATIBLE,
            confidence=0.0,
            coercion_needed=False
        )

    @classmethod
    def _check_object_compat(cls, source: ToolType, target: ToolType) -> CompatibilityResult:
        """Check object type compatibility."""
        missing = target.required_fields - set(source.fields.keys())
        extra = set(source.fields.keys()) - set(target.fields.keys())

        if missing:
            return CompatibilityResult(
                level=Compatibility.INCOMPATIBLE,
                confidence=0.0,
                coercion_needed=False,
                missing_fields=missing
            )

        # Check field type compatibility
        for field_name, target_type in target.fields.items():
            if field_name in source.fields:
                field_result = cls.check(source.fields[field_name], target_type)
                if field_result.level == Compatibility.INCOMPATIBLE:
                    return CompatibilityResult(
                        level=Compatibility.INCOMPATIBLE,
                        confidence=0.0,
                        coercion_needed=False
                    )

        if extra:
            return CompatibilityResult(
                level=Compatibility.PARTIAL,
                confidence=0.9,
                coercion_needed=True,
                coercion_path="project",
                extra_fields=extra
            )

        return CompatibilityResult(
            level=Compatibility.EXACT,
            confidence=1.0,
            coercion_needed=False
        )

    @classmethod
    def _check_array_compat(cls, source: ToolType, target: ToolType) -> CompatibilityResult:
        """Check array type compatibility."""
        if not target.element_type:
            return CompatibilityResult(
                level=Compatibility.EXACT,
                confidence=1.0,
                coercion_needed=False
            )

        if not source.element_type:
            return CompatibilityResult(
                level=Compatibility.COERCIBLE,
                confidence=0.5,
                coercion_needed=True,
                coercion_path="cast_elements"
            )

        return cls.check(source.element_type, target.element_type)


@dataclass
class ToolSignature:
    """
    Complete type signature for a tool.

    Represents: inputs -> outputs with metadata
    """
    tool_id: str
    tool_name: str
    input_type: ToolType
    output_type: ToolType
    is_async: bool = False
    is_deterministic: bool = False
    estimated_latency_ms: Optional[float] = None
    estimated_cost: Optional[float] = None
    tags: Set[str] = field(default_factory=set)

    def __hash__(self):
        return hash(self.tool_id)

    def signature_hash(self) -> str:
        """Unique hash of the type signature."""
        sig = f"{self.input_type.signature()}->{self.output_type.signature()}"
        return hashlib.md5(sig.encode()).hexdigest()[:8]

    def can_chain_to(self, other: ToolSignature) -> CompatibilityResult:
        """Check if this tool's output can feed into another tool's input."""
        return TypeChecker.check(self.output_type, other.input_type)


class CompositionOp(str, Enum):
    """Tool composition operators."""
    SEQUENCE = ">>"    # A >> B: output of A feeds into B
    PARALLEL = "||"    # A || B: run in parallel, merge outputs
    CHOICE = "??"      # A ?? B: run A, if fails try B
    MAP = ">>*"        # A >>* B: apply B to each element of A's array output
    FILTER = ">>?"     # A >>? B: filter A's output through B predicate


@dataclass
class ComposedTool:
    """
    A tool created by composing other tools.

    Tracks the composition graph and computes combined type signature.
    """
    op: CompositionOp
    left: Union[ToolSignature, ComposedTool]
    right: Union[ToolSignature, ComposedTool]

    @property
    def input_type(self) -> ToolType:
        """Input type is the left operand's input."""
        if isinstance(self.left, ComposedTool):
            return self.left.input_type
        return self.left.input_type

    @property
    def output_type(self) -> ToolType:
        """Output type depends on composition operator."""
        right_output = self.right.output_type if isinstance(self.right, ComposedTool) else self.right.output_type
        left_output = self.left.output_type if isinstance(self.left, ComposedTool) else self.left.output_type

        if self.op == CompositionOp.SEQUENCE:
            return right_output
        elif self.op == CompositionOp.PARALLEL:
            # Merge outputs into object
            return ToolType(
                kind=TypeKind.OBJECT,
                fields={
                    "left": left_output,
                    "right": right_output
                }
            )
        elif self.op == CompositionOp.CHOICE:
            # Union of possible outputs
            return ToolType(
                kind=TypeKind.UNION,
                union_types=[left_output, right_output]
            )
        elif self.op == CompositionOp.MAP:
            return ToolType(kind=TypeKind.ARRAY, element_type=right_output)
        elif self.op == CompositionOp.FILTER:
            return left_output
        return right_output

    def validate(self) -> List[str]:
        """Validate composition is type-safe. Returns list of errors."""
        errors = []

        left_output = self.left.output_type if isinstance(self.left, ComposedTool) else self.left.output_type
        right_input = self.right.input_type if isinstance(self.right, ComposedTool) else self.right.input_type

        if self.op == CompositionOp.SEQUENCE:
            result = TypeChecker.check(left_output, right_input)
            if result.level == Compatibility.INCOMPATIBLE:
                errors.append(
                    f"Sequence composition invalid: {left_output.signature()} "
                    f"cannot flow to {right_input.signature()}"
                )

        elif self.op == CompositionOp.MAP:
            # Left must produce array
            if left_output.kind != TypeKind.ARRAY:
                errors.append(f"Map requires array input, got {left_output.signature()}")
            elif left_output.element_type:
                result = TypeChecker.check(left_output.element_type, right_input)
                if result.level == Compatibility.INCOMPATIBLE:
                    errors.append(
                        f"Map element type mismatch: {left_output.element_type.signature()} "
                        f"cannot flow to {right_input.signature()}"
                    )

        elif self.op == CompositionOp.FILTER:
            # Right must return boolean
            right_output = self.right.output_type if isinstance(self.right, ComposedTool) else self.right.output_type
            if right_output.kind != TypeKind.BOOLEAN:
                errors.append(f"Filter predicate must return boolean, got {right_output.signature()}")

        # Recursively validate children
        if isinstance(self.left, ComposedTool):
            errors.extend(self.left.validate())
        if isinstance(self.right, ComposedTool):
            errors.extend(self.right.validate())

        return errors


class ToolAlgebra:
    """
    Registry of tool signatures with composition operations.

    Provides:
    - Tool registration with type inference
    - Compatibility queries
    - Composition operators with validation
    - Path finding between types
    """

    def __init__(self):
        self.signatures: Dict[str, ToolSignature] = {}
        self._output_index: Dict[str, Set[str]] = {}  # type_sig -> tool_ids
        self._input_index: Dict[str, Set[str]] = {}   # type_sig -> tool_ids

    def register(self, signature: ToolSignature):
        """Register a tool signature."""
        self.signatures[signature.tool_id] = signature

        # Index by output type
        out_sig = signature.output_type.signature()
        if out_sig not in self._output_index:
            self._output_index[out_sig] = set()
        self._output_index[out_sig].add(signature.tool_id)

        # Index by input type
        in_sig = signature.input_type.signature()
        if in_sig not in self._input_index:
            self._input_index[in_sig] = set()
        self._input_index[in_sig].add(signature.tool_id)

    def register_from_schema(
        self,
        tool_id: str,
        tool_name: str,
        input_schema: Dict[str, Any],
        output_schema: Dict[str, Any],
        **kwargs
    ) -> ToolSignature:
        """Register tool from JSON schemas."""
        sig = ToolSignature(
            tool_id=tool_id,
            tool_name=tool_name,
            input_type=ToolType.from_json_schema(input_schema),
            output_type=ToolType.from_json_schema(output_schema),
            **kwargs
        )
        self.register(sig)
        return sig

    def find_compatible_inputs(self, output_type: ToolType) -> List[Tuple[ToolSignature, CompatibilityResult]]:
        """Find tools that can consume the given output type."""
        results = []
        for sig in self.signatures.values():
            result = TypeChecker.check(output_type, sig.input_type)
            if result.level != Compatibility.INCOMPATIBLE:
                results.append((sig, result))

        # Sort by compatibility confidence
        results.sort(key=lambda x: x[1].confidence, reverse=True)
        return results

    def find_compatible_outputs(self, input_type: ToolType) -> List[Tuple[ToolSignature, CompatibilityResult]]:
        """Find tools that can produce output compatible with the given input type."""
        results = []
        for sig in self.signatures.values():
            result = TypeChecker.check(sig.output_type, input_type)
            if result.level != Compatibility.INCOMPATIBLE:
                results.append((sig, result))

        results.sort(key=lambda x: x[1].confidence, reverse=True)
        return results

    def find_path(
        self,
        source_type: ToolType,
        target_type: ToolType,
        max_depth: int = 5
    ) -> Optional[List[ToolSignature]]:
        """
        Find a sequence of tools that transforms source_type into target_type.

        Uses BFS to find shortest path.
        """
        from collections import deque

        # Direct compatibility?
        direct = TypeChecker.check(source_type, target_type)
        if direct.level in (Compatibility.EXACT, Compatibility.COERCIBLE):
            return []

        # BFS
        queue = deque([(source_type, [])])
        visited = {source_type.signature()}

        while queue:
            current_type, path = queue.popleft()

            if len(path) >= max_depth:
                continue

            # Find tools that can accept current_type
            for sig, compat in self.find_compatible_inputs(current_type):
                if compat.level == Compatibility.INCOMPATIBLE:
                    continue

                new_path = path + [sig]

                # Check if output matches target
                result = TypeChecker.check(sig.output_type, target_type)
                if result.level in (Compatibility.EXACT, Compatibility.COERCIBLE):
                    return new_path

                # Continue search
                out_sig = sig.output_type.signature()
                if out_sig not in visited:
                    visited.add(out_sig)
                    queue.append((sig.output_type, new_path))

        return None

    def compose(
        self,
        left: Union[str, ToolSignature, ComposedTool],
        op: CompositionOp,
        right: Union[str, ToolSignature, ComposedTool]
    ) -> ComposedTool:
        """
        Compose two tools with the given operator.

        Validates type compatibility and returns composed tool.
        """
        # Resolve tool IDs to signatures
        if isinstance(left, str):
            left = self.signatures[left]
        if isinstance(right, str):
            right = self.signatures[right]

        composed = ComposedTool(op=op, left=left, right=right)
        errors = composed.validate()

        if errors:
            raise TypeError(f"Invalid composition: {'; '.join(errors)}")

        return composed

    def sequence(self, *tools: Union[str, ToolSignature]) -> ComposedTool:
        """Compose tools in sequence: a >> b >> c >> ..."""
        if len(tools) < 2:
            raise ValueError("Sequence requires at least 2 tools")

        result = self.compose(tools[0], CompositionOp.SEQUENCE, tools[1])
        for tool in tools[2:]:
            result = self.compose(result, CompositionOp.SEQUENCE, tool)

        return result

    def parallel(self, *tools: Union[str, ToolSignature]) -> ComposedTool:
        """Compose tools in parallel: a || b || c || ..."""
        if len(tools) < 2:
            raise ValueError("Parallel requires at least 2 tools")

        result = self.compose(tools[0], CompositionOp.PARALLEL, tools[1])
        for tool in tools[2:]:
            result = self.compose(result, CompositionOp.PARALLEL, tool)

        return result
