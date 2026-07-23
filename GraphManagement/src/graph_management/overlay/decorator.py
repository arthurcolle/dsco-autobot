"""
@graph.object decorator for creating graph-native Pydantic models.

Transforms regular Pydantic models into graph objects with:
- Multi-space embeddings
- Declarative relationships
- Calculated fields
- Memory policies
"""

from __future__ import annotations
from typing import Any, Dict, List, Optional, Sequence, Type, Union
from pydantic import BaseModel
from dataclasses import dataclass

from .specs import (
    EmbeddingSpec,
    RelationSpec,
    FieldSpec,
    CalculatedFieldSpec,
    MemoryPolicies,
    EmbedWhen,
    CalcWhen,
    Direction,
)


@dataclass
class OverlayMeta:
    """Metadata attached to graph objects."""
    label: str
    key_fields: List[str]
    service_id: str
    persist_on_init: bool
    embeddings: List[EmbeddingSpec]
    relations: List[RelationSpec]
    fields: List[FieldSpec]
    calc_fields: List[CalculatedFieldSpec]
    policies: MemoryPolicies


class GraphObject(BaseModel):
    """
    Base class for graph-backed objects.

    Extended by the @graph.object decorator to add graph persistence,
    embeddings, relationships, and calculated fields.
    """
    __overlay__: OverlayMeta

    def _identity(self) -> Dict[str, Any]:
        """Get the identity key dict for this object."""
        return {k: getattr(self, k) for k in self.__overlay__.key_fields}

    def _all_props(self) -> Dict[str, Any]:
        """Get all model properties."""
        return {k: getattr(self, k) for k in self.model_fields}

    def _apply_calculated(self, when: CalcWhen):
        """Compute and set calculated fields."""
        for cf in self.__overlay__.calc_fields:
            if when in cf.when:
                val = cf.fn(self)
                if cf.store:
                    setattr(self, cf.name, val)

    def _compute_embeddings(self, when: EmbedWhen) -> Dict[str, List[float]]:
        """
        Compute embeddings for configured spaces.

        Returns:
            Dict mapping space name -> vector
        """
        from .runtime import get_embedder

        vectors: Dict[str, List[float]] = {}
        for es in self.__overlay__.embeddings:
            if when not in es.when:
                continue

            text = es.source.extract(self) or ""
            if not text.strip():
                continue

            embedder = get_embedder(es.provider)
            vec = embedder(text, model=es.model, dim=es.dim)

            if not isinstance(vec, list) or len(vec) != es.dim:
                raise ValueError(f"Embedder returned invalid vector for space '{es.name}'")

            vectors[es.name] = vec

        return vectors

    def _materialize_relations(self, gc, vectors: Optional[Dict[str, List[float]]] = None):
        """
        Create declared relationships and auto-link SIMILAR edges.
        """
        # 1) Declarative relations
        for rel in self.__overlay__.relations:
            # Lifecycle hooks
            if hasattr(rel, "on_create") and callable(rel.on_create):
                try:
                    rel.on_create(self, gc)
                except Exception:
                    pass

            # Dynamic from_field
            if rel.from_field:
                src_val = getattr(self, rel.from_field, None)
                if src_val is None:
                    continue

                items = src_val if isinstance(src_val, list) else [src_val]
                for item in items:
                    # Case A: GraphObject -> relate directly
                    if hasattr(item, "__overlay__"):
                        out_label, out_key = self.__overlay__.label, self._identity()
                        in_label, in_key = item.__overlay__.label, item._identity()
                        if rel.direction == Direction.OUT:
                            gc.merge_rel(out_label, out_key, rel.name, in_label, in_key, rel.props)
                        elif rel.direction == Direction.IN:
                            gc.merge_rel(in_label, in_key, rel.name, out_label, out_key, rel.props)
                        else:  # BOTH
                            gc.merge_rel(out_label, out_key, rel.name, in_label, in_key, rel.props)
                            gc.merge_rel(in_label, in_key, rel.name, out_label, out_key, rel.props)
                        continue

                    # Case B: scalar -> resolve to node
                    if rel.resolve_to_label and rel.resolve_key:
                        in_label = rel.resolve_to_label
                        in_key = {rel.resolve_key[0]: item}
                        out_label, out_key = self.__overlay__.label, self._identity()
                        if rel.direction == Direction.OUT:
                            gc.merge_rel(out_label, out_key, rel.name, in_label, in_key, rel.props)
                        elif rel.direction == Direction.IN:
                            gc.merge_rel(in_label, in_key, rel.name, out_label, out_key, rel.props)
                        else:
                            gc.merge_rel(out_label, out_key, rel.name, in_label, in_key, rel.props)
                            gc.merge_rel(in_label, in_key, rel.name, out_label, out_key, rel.props)

            # Static endpoint
            elif rel.to_label and rel.to_key:
                out_label, out_key = self.__overlay__.label, self._identity()
                in_label = rel.to_label
                in_key = {k: getattr(self, k) for k in rel.to_key}
                gc.merge_rel(out_label, out_key, rel.name, in_label, in_key, rel.props)

        # 2) Auto-link SIMILAR
        for es in self.__overlay__.embeddings:
            if not es.autolink:
                continue
            if not vectors or es.name not in vectors:
                continue

            al = es.autolink
            search_label = al.label_scope or self.__overlay__.label

            hits = gc.vector_search(
                label=search_label,
                space=es.name,
                vector=vectors[es.name],
                k=al.k,
                threshold=al.threshold,
                similarity=es.similarity.value,
            )

            me_label, me_key = self.__overlay__.label, self._identity()
            for h in hits:
                if h.get("key") == me_key:  # skip self
                    continue

                other_label = h.get("label", search_label)
                other_key = h.get("key", {})

                gc.merge_rel(me_label, me_key, al.rel_type, other_label, other_key, {"weight": al.weight})
                if al.bidirectional:
                    gc.merge_rel(other_label, other_key, al.rel_type, me_label, me_key, {"weight": al.weight})

    def merge(self, *, is_update: bool = False):
        """
        Persist this object to the graph.

        Performs:
        1. Calculated field computation
        2. Embedding generation
        3. Node merge with props + vectors
        4. Relationship materialization
        5. SIMILAR auto-linking

        Args:
            is_update: True if updating existing node
        """
        from .runtime import get_graph_client

        gc = get_graph_client(self.__overlay__.service_id)

        # 1) Calculated fields
        self._apply_calculated(CalcWhen.update if is_update else CalcWhen.create)

        # 2) Embeddings
        vectors = self._compute_embeddings(EmbedWhen.update if is_update else EmbedWhen.create)

        # 3) Filter props (respect FieldSpec.store=False)
        props = self._all_props()
        if self.__overlay__.fields:
            keep = {f.name for f in self.__overlay__.fields if f.store}
            props = {
                k: v for k, v in props.items()
                if not self.__overlay__.fields or k in keep or k in self.__overlay__.key_fields
            }

        # 4) Merge node + vectors
        gc.merge_node(self.__overlay__.label, self._identity(), props, vectors=vectors)

        # 5) Relations + autolinks
        self._materialize_relations(gc, vectors=vectors)


def object(
    *,
    label: str,
    key: Union[str, List[str]],
    service_id: str,
    persist_on_init: bool = False,
    embeddings: Optional[Sequence[EmbeddingSpec]] = None,
    relations: Optional[Sequence[RelationSpec]] = None,
    fields: Optional[Sequence[FieldSpec]] = None,
    calc_fields: Optional[Sequence[CalculatedFieldSpec]] = None,
    policies: Optional[MemoryPolicies] = None,
):
    """
    Decorator to transform a Pydantic model into a graph-backed memory object.

    Example:
        @graph.object(
            label="MemoryItem",
            key=["id"],
            service_id="graph-svc",
            embeddings=[
                EmbeddingSpec(
                    name="content",
                    provider="openai",
                    model="text-embedding-3-large",
                    dim=1536,
                    source=EmbeddingSource(fields=["title", "text"]),
                    autolink=AutoLinkSimilar(k=25, threshold=0.80)
                )
            ],
            relations=[
                RelationSpec(
                    name="BELONGS_TO",
                    to_label="Conversation",
                    to_key=["conversation_id"]
                )
            ],
            calc_fields=[
                CalculatedFieldSpec(name="tokens", fn=calc_tokens("text"))
            ]
        )
        class MemoryItem(BaseModel):
            id: str
            text: str
            conversation_id: str | None = None

    Args:
        label: Graph node label
        key: Field(s) forming the unique key
        service_id: Graph service identifier
        persist_on_init: Auto-merge on construction
        embeddings: Embedding space definitions
        relations: Relationship specifications
        fields: Structured field metadata
        calc_fields: Calculated field specifications
        policies: Memory overlay policies
    """
    def _decorator(cls: Type[BaseModel]):
        key_fields = [key] if isinstance(key, str) else list(key)

        name = f"{cls.__name__}Overlay"
        NewCls = type(name, (GraphObject, cls), {})
        NewCls.__overlay__ = OverlayMeta(
            label=label,
            key_fields=key_fields,
            service_id=service_id,
            persist_on_init=persist_on_init,
            embeddings=list(embeddings or []),
            relations=list(relations or []),
            fields=list(fields or []),
            calc_fields=list(calc_fields or []),
            policies=policies or MemoryPolicies(),
        )

        # Auto-persist hook
        orig_post = getattr(NewCls, "model_post_init", None)

        def model_post_init(self, __ctx):
            if orig_post:
                orig_post(self, __ctx)
            if NewCls.__overlay__.persist_on_init:
                self.merge(is_update=False)

        NewCls.model_post_init = model_post_init
        return NewCls

    return _decorator
