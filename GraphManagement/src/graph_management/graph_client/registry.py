from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time as time_, timedelta
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Literal, Sequence, Tuple, Type, Union, get_args, get_origin
from uuid import UUID
from collections.abc import Sequence as SequenceABC

from pydantic.fields import FieldInfo, PydanticUndefined

from .models import GraphNodeModel, GraphRelationshipModel
from ..schemas import (
    NodeSchemaSpec,
    PropertyDef,
    RelationshipSchemaSpec,
)

_SIMPLE_TYPE_MAP: Dict[type, str] = {
    str: "string",
    int: "integer",
    float: "float",
    bool: "boolean",
    datetime: "datetime",
    date: "date",
    time_: "time",
    timedelta: "duration",
    UUID: "uuid",
    Decimal: "decimal",
}


@dataclass
class NodeRegistration:
    cls: Type[GraphNodeModel]
    label: str
    description: str | None
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    constraints: List[Dict[str, Any]] = field(default_factory=list)
    embedding: List[float] | None = None


@dataclass
class RelationshipRegistration:
    cls: Type[GraphRelationshipModel]
    type: str
    description: str | None
    direction: str
    start_labels: List[str]
    end_labels: List[str]
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    constraints: List[Dict[str, Any]] = field(default_factory=list)


class GraphRegistry:
    """Keeps track of node/relationship Pydantic models and builds schema specs."""

    def __init__(self) -> None:
        self._nodes: Dict[str, NodeRegistration] = {}
        self._nodes_by_cls: Dict[Type[GraphNodeModel], NodeRegistration] = {}
        self._relationships: Dict[str, RelationshipRegistration] = {}
        self._relationships_by_cls: Dict[Type[GraphRelationshipModel], RelationshipRegistration] = {}

    # -----------------------
    # Registration helpers
    # -----------------------

    def register_node(
        self,
        cls: Type[GraphNodeModel],
        *,
        label: str | None = None,
        description: str | None = None,
        indexes: Iterable[Dict[str, Any]] | None = None,
        constraints: Iterable[Dict[str, Any]] | None = None,
        embedding: Sequence[float] | None = None,
    ) -> NodeRegistration:
        if not issubclass(cls, GraphNodeModel):
            raise TypeError("Graph node classes must inherit from GraphNodeModel")
        final_label = label or getattr(cls, "__graph_label__", cls.__name__)
        existing = self._nodes.get(final_label)
        if existing and existing.cls is not cls:
            raise ValueError(f"Node label '{final_label}' already registered")
        reg = NodeRegistration(
            cls=cls,
            label=final_label,
            description=description or getattr(cls, "__graph_description__", None),
            indexes=list(indexes or []),
            constraints=list(constraints or []),
            embedding=list(embedding) if embedding is not None else None,
        )
        self._nodes[final_label] = reg
        self._nodes_by_cls[cls] = reg
        cls.__graph_label__ = final_label
        cls.__graph_description__ = reg.description
        return reg

    def register_relationship(
        self,
        cls: Type[GraphRelationshipModel],
        *,
        type_: str | None = None,
        description: str | None = None,
        direction: str = "outgoing",
        start: Sequence[str] | Type[GraphNodeModel] | str,
        end: Sequence[str] | Type[GraphNodeModel] | str,
        indexes: Iterable[Dict[str, Any]] | None = None,
        constraints: Iterable[Dict[str, Any]] | None = None,
    ) -> RelationshipRegistration:
        if not issubclass(cls, GraphRelationshipModel):
            raise TypeError("Relationship classes must inherit from GraphRelationshipModel")
        rel_type = type_ or getattr(cls, "__relationship_type__", cls.__name__)
        existing = self._relationships.get(rel_type)
        if existing and existing.cls is not cls:
            raise ValueError(f"Relationship type '{rel_type}' already registered")
        start_labels = self._resolve_labels(start)
        end_labels = self._resolve_labels(end)
        reg = RelationshipRegistration(
            cls=cls,
            type=rel_type,
            description=description or getattr(cls, "__relationship_description__", None),
            direction=direction,
            start_labels=start_labels,
            end_labels=end_labels,
            indexes=list(indexes or []),
            constraints=list(constraints or []),
        )
        self._relationships[rel_type] = reg
        self._relationships_by_cls[cls] = reg
        cls.__relationship_type__ = rel_type
        cls.__relationship_description__ = reg.description
        return reg

    # -----------------------
    # Schema spec builders
    # -----------------------

    def node_specs(self) -> List[NodeSchemaSpec]:
        return [self._build_node_spec(reg) for reg in self._nodes.values()]

    def relationship_specs(self) -> List[RelationshipSchemaSpec]:
        return [self._build_relationship_spec(reg) for reg in self._relationships.values()]

    # -----------------------
    # Utilities
    # -----------------------

    def _resolve_labels(self, value: Sequence[str] | Type[GraphNodeModel] | str) -> List[str]:
        if isinstance(value, str):
            return [value]
        if isinstance(value, SequenceABC) and not isinstance(value, type):
            return [str(v) for v in value]
        if isinstance(value, type) and issubclass(value, GraphNodeModel):
            if value in self._nodes_by_cls:
                return [self._nodes_by_cls[value].label]
            return [value.graph_label()]
        raise TypeError(f"Unsupported label reference: {value!r}")

    def _build_node_spec(self, reg: NodeRegistration) -> NodeSchemaSpec:
        properties = self._properties_from_model(reg.cls)
        return NodeSchemaSpec(
            label=reg.label,
            description=reg.description,
            properties=properties,
            indexes=reg.indexes,
            constraints=reg.constraints,
            embedding=reg.embedding,
        )

    def _build_relationship_spec(self, reg: RelationshipRegistration) -> RelationshipSchemaSpec:
        properties = self._properties_from_model(reg.cls)
        return RelationshipSchemaSpec(
            type=reg.type,
            description=reg.description,
            direction=reg.direction,
            start_labels=reg.start_labels,
            end_labels=reg.end_labels,
            properties=properties,
            indexes=reg.indexes,
            constraints=reg.constraints,
        )

    def _properties_from_model(self, cls: type[GraphNodeModel | GraphRelationshipModel]) -> List[PropertyDef]:
        props: List[PropertyDef] = []
        for name, field in cls.model_fields.items():
            prop_type, enum_values = self._infer_property_type(field)
            metadata = self._extract_metadata(field)
            prop = PropertyDef(
                name=name,
                type=prop_type,
                required=field.is_required(),
                unique=metadata.get("unique", False),
                indexed=metadata.get("indexed", False),
                default=metadata.get("default"),
                enum=enum_values,
                minimum=metadata.get("minimum"),
                maximum=metadata.get("maximum"),
                notes=metadata.get("notes"),
            )
            props.append(prop)
        return props

    def _extract_metadata(self, field: FieldInfo) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        if field.default is not PydanticUndefined:
            data["default"] = field.default
        extra = field.json_schema_extra or {}
        data["unique"] = bool(extra.get("unique", False))
        data["indexed"] = bool(extra.get("indexed", False))
        if field.description:
            data["notes"] = field.description
        if "notes" in extra:
            data["notes"] = extra["notes"]
        if "minimum" in extra:
            data["minimum"] = extra["minimum"]
        if "maximum" in extra:
            data["maximum"] = extra["maximum"]
        for item in field.metadata:
            ge = getattr(item, "ge", None)
            le = getattr(item, "le", None)
            if ge is not None:
                data["minimum"] = ge
            if le is not None:
                data["maximum"] = le
        return data

    def _infer_property_type(self, field: FieldInfo) -> Tuple[str, List[Any] | None]:
        annotation = field.annotation
        annotation = self._strip_optional(annotation)
        enum_values: List[Any] | None = None
        origin = get_origin(annotation)
        if origin is None and isinstance(annotation, type):
            prop_type = _SIMPLE_TYPE_MAP.get(annotation, "any")
        elif origin in (list, List, SequenceABC, tuple, set):
            inner = get_args(annotation)[:1] or (Any,)
            inner_type, _ = self._infer_type_from_annotation(inner[0])
            prop_type = f"list[{inner_type}]"
        elif origin in (dict, Dict):
            prop_type = "map"
        else:
            prop_type, enum_values = self._infer_type_from_annotation(annotation)
        return prop_type, enum_values

    def _infer_type_from_annotation(self, annotation: Any) -> Tuple[str, List[Any] | None]:
        annotation = self._strip_optional(annotation)
        origin = get_origin(annotation)
        if origin is None:
            if isinstance(annotation, type):
                return _SIMPLE_TYPE_MAP.get(annotation, "any"), None
            return "any", None
        if origin is Literal:
            values = list(get_args(annotation))
            base_type = type(values[0]) if values else str
            return _SIMPLE_TYPE_MAP.get(base_type, "any"), values
        if origin in (list, List, SequenceABC, tuple, set):
            inner = get_args(annotation)[:1] or (Any,)
            inner_type, _ = self._infer_type_from_annotation(inner[0])
            return f"list[{inner_type}]", None
        if origin in (dict, Dict):
            return "map", None
        return "any", None

    def _strip_optional(self, annotation: Any) -> Any:
        origin = get_origin(annotation)
        if origin is Union:
            args = [arg for arg in get_args(annotation) if arg is not type(None)]  # noqa: E721
            if len(args) == 1:
                return args[0]
        return annotation
