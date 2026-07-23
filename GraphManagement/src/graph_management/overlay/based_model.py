"""
BasedModel: ORM-like convenience layer over GraphObject.

Provides friendly methods for persistence, relationship management,
graph traversal, and serialization.

Combines graph-native functionality with Pydantic serialization helpers.
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Type, TypeVar
from datetime import datetime
from enum import Enum
import json

from .decorator import GraphObject

T = TypeVar('T', bound='BasedModel')


class BasedEncoder(json.JSONEncoder):
    """JSON encoder that handles BasedModel instances and common types."""

    def default(self, obj):
        if isinstance(obj, BasedModel):
            return obj.to_dict()
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Enum):
            return obj.value
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        return super().default(obj)


class BasedModel(GraphObject):
    """
    Graph-ORM facade with ergonomic methods.

    Usage:
        @graph.object(label="Person", key="name", service_id="graph-svc")
        class Person(BasedModel):
            name: str
            born: int | None = None

        # Create and save
        p = Person(name="Alice", born=1990)
        p.save()

        # Link to another object
        p2 = Person(name="Bob")
        p.link("KNOWS", p2, props={"since": 2020})

        # Traverse
        friends = p.neighbors(rel_type="KNOWS", direction="OUT")
    """

    def save(self, *, update: bool = False):
        """
        Persist this object to the graph.

        Args:
            update: True if updating existing node

        Returns:
            Result from merge operation
        """
        return self.merge(is_update=update)

    def link(
        self,
        rel_type: str,
        other: "BasedModel",
        props: Optional[Dict[str, Any]] = None
    ):
        """
        Create a relationship to another BasedModel instance.

        Args:
            rel_type: Relationship type (e.g., "KNOWS", "AUTHORED")
            other: Target BasedModel instance
            props: Optional relationship properties

        Returns:
            Result from merge_rel operation
        """
        return self.relate_to(other, rel_type, props or {})

    def relate_to(
        self,
        other: "BasedModel",
        rel_type: str,
        props: Optional[Dict[str, Any]] = None
    ):
        """
        Create a relationship (alias for link).

        Args:
            other: Target BasedModel instance
            rel_type: Relationship type
            props: Optional relationship properties
        """
        from .runtime import get_graph_client

        gc = get_graph_client(self.__overlay__.service_id)
        return gc.merge_rel(
            self.__overlay__.label,
            self._identity(),
            rel_type,
            other.__overlay__.label,
            other._identity(),
            props or {}
        )

    def unlink(self, rel_type: str, other: "BasedModel"):
        """
        Remove a relationship to another BasedModel instance.

        Note: This requires backend support for relationship deletion.
        Currently implemented as a special merge with __UNLINK__ prefix.

        Args:
            rel_type: Relationship type to remove
            other: Target BasedModel instance
        """
        from .runtime import get_graph_client

        gc = get_graph_client(self.__overlay__.service_id)
        # Backend should handle __UNLINK__:TYPE as DELETE operation
        return gc.merge_rel(
            self.__overlay__.label,
            self._identity(),
            f"__UNLINK__:{rel_type}",
            other.__overlay__.label,
            other._identity(),
            {}
        )

    def neighbors(
        self,
        rel_type: Optional[str] = None,
        direction: str = "OUT",
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Traverse to neighboring nodes.

        Args:
            rel_type: Relationship type filter (None = all types)
            direction: "OUT", "IN", or "BOTH"
            limit: Maximum results

        Returns:
            List of neighbor nodes as dicts

        Note: Requires backend support for traverse endpoint.
        """
        from .runtime import get_graph_client

        gc = get_graph_client(self.__overlay__.service_id)

        # Check if client has traverse method
        if not hasattr(gc, 'traverse'):
            return []

        try:
            return gc.traverse(
                start={"label": self.__overlay__.label, "key": self._identity()},
                rel_type=rel_type,
                direction=direction,
                limit=limit
            )
        except (AttributeError, NotImplementedError):
            return []

    def refresh(self):
        """
        Reload this object's properties from the graph.

        Note: Requires backend support for node fetch by key.
        """
        from .runtime import get_graph_client

        gc = get_graph_client(self.__overlay__.service_id)

        if not hasattr(gc, 'get_node'):
            raise NotImplementedError("Backend does not support node refresh")

        node = gc.get_node(self.__overlay__.label, self._identity())
        if node and "properties" in node:
            for k, v in node["properties"].items():
                if hasattr(self, k):
                    setattr(self, k, v)

    def delete(self):
        """
        Delete this node from the graph.

        Note: Requires backend support for node deletion.
        """
        from .runtime import get_graph_client

        gc = get_graph_client(self.__overlay__.service_id)

        if not hasattr(gc, 'delete_node'):
            raise NotImplementedError("Backend does not support node deletion")

        return gc.delete_node(self.__overlay__.label, self._identity())

    # ========= Serialization Helpers (from original BasedModel) =========

    def to_json(self, **kwargs) -> str:
        """
        Convert to JSON string.

        Args:
            **kwargs: Additional arguments for model_dump_json

        Returns:
            JSON string representation
        """
        return self.model_dump_json(**kwargs)

    def to_dict(self, **kwargs) -> Dict[str, Any]:
        """
        Convert to dictionary.

        Args:
            **kwargs: Additional arguments for model_dump

        Returns:
            Dictionary representation
        """
        return self.model_dump(**kwargs)

    @classmethod
    def from_json(cls: Type[T], json_str: str) -> T:
        """
        Create instance from JSON string.

        Args:
            json_str: JSON string to parse

        Returns:
            New instance of this model
        """
        return cls.model_validate_json(json_str)

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Create instance from dictionary.

        Args:
            data: Dictionary to validate

        Returns:
            New instance of this model
        """
        return cls.model_validate(data)

    def __json__(self) -> Dict[str, Any]:
        """Support for json.dumps() with custom encoder."""
        return self.to_dict()

    def __str__(self) -> str:
        """String representation as JSON."""
        return self.to_json()

    def __repr__(self) -> str:
        """Representation including class name and JSON."""
        return f"{self.__class__.__name__}({self.to_json()})"
