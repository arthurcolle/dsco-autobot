from __future__ import annotations

from typing import Any, ClassVar, Dict, Optional

from pydantic import BaseModel, ConfigDict


class GraphNodeModel(BaseModel):
    """Base class for graph node definitions backed by Pydantic."""

    __graph_label__: ClassVar[str]
    __graph_description__: ClassVar[Optional[str]] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def graph_label(cls) -> str:
        return getattr(cls, "__graph_label__", cls.__name__)

    @classmethod
    def from_serialized(cls, payload: Dict[str, Any]) -> GraphNodeModel:
        """Instantiate from the serialized form produced by the Neo4j API."""
        props = payload.get("properties", payload)
        return cls.model_validate(props)

    def to_properties(self) -> Dict[str, Any]:
        """Return a dictionary suitable for Cypher parameterization."""
        return self.model_dump(exclude_none=True)


class GraphRelationshipModel(BaseModel):
    """Base class for relationship definitions."""

    __relationship_type__: ClassVar[str]
    __relationship_description__: ClassVar[Optional[str]] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def relationship_type(cls) -> str:
        return getattr(cls, "__relationship_type__", cls.__name__)

    @classmethod
    def from_serialized(cls, payload: Dict[str, Any]) -> GraphRelationshipModel:
        props = payload.get("properties", payload)
        return cls.model_validate(props)

    def to_properties(self) -> Dict[str, Any]:
        return self.model_dump(exclude_none=True)
