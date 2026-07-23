"""
Basic tests for graph overlay system.
"""

import pytest
from pydantic import BaseModel
from graph_management.overlay import (
    graph_object, BasedModel, EmbeddingSpec, EmbeddingSource,
    RelationSpec, FieldSpec, CalculatedFieldSpec,
    Direction, FieldIndexKind
)


def test_graph_object_decorator():
    """Test basic @graph.object decoration."""

    @graph_object(
        label="TestNode",
        key=["id"],
        service_id="test-svc",
        fields=[
            FieldSpec(name="id", index=FieldIndexKind.btree, unique=True),
        ]
    )
    class TestNode(BaseModel):
        id: str
        name: str

    # Check overlay metadata attached
    assert hasattr(TestNode, "__overlay__")
    assert TestNode.__overlay__.label == "TestNode"
    assert TestNode.__overlay__.key_fields == ["id"]
    assert TestNode.__overlay__.service_id == "test-svc"

    # Create instance
    node = TestNode(id="test-1", name="Test")
    assert node.id == "test-1"
    assert node.name == "Test"


def test_based_model_serialization():
    """Test BasedModel serialization helpers."""

    @graph_object(
        label="TestModel",
        key=["id"],
        service_id="test-svc"
    )
    class TestModel(BasedModel):
        id: str
        value: int

    # Create instance
    obj = TestModel(id="test-1", value=42)

    # Test to_dict
    data = obj.to_dict()
    assert data["id"] == "test-1"
    assert data["value"] == 42

    # Test to_json
    json_str = obj.to_json()
    assert "test-1" in json_str
    assert "42" in json_str

    # Test from_dict
    obj2 = TestModel.from_dict(data)
    assert obj2.id == obj.id
    assert obj2.value == obj.value

    # Test from_json
    obj3 = TestModel.from_json(json_str)
    assert obj3.id == obj.id


def test_calculated_fields():
    """Test calculated field specification."""

    def calc_double(obj):
        return obj.value * 2

    @graph_object(
        label="CalcTest",
        key=["id"],
        service_id="test-svc",
        calc_fields=[
            CalculatedFieldSpec(name="doubled", fn=calc_double)
        ]
    )
    class CalcTest(BasedModel):
        id: str
        value: int
        doubled: int | None = None

    obj = CalcTest(id="test-1", value=5)

    # Calculated field should be computed (would happen in merge)
    # For now just verify spec is attached
    assert len(obj.__overlay__.calc_fields) == 1
    assert obj.__overlay__.calc_fields[0].name == "doubled"


def test_relationship_spec():
    """Test relationship specification."""

    @graph_object(
        label="Parent",
        key=["id"],
        service_id="test-svc",
        relations=[
            RelationSpec(
                name="HAS_CHILD",
                direction=Direction.OUT,
                to_label="Child",
                to_key=["child_id"]
            )
        ]
    )
    class Parent(BasedModel):
        id: str
        child_id: str

    obj = Parent(id="parent-1", child_id="child-1")

    # Verify relation spec attached
    assert len(obj.__overlay__.relations) == 1
    rel = obj.__overlay__.relations[0]
    assert rel.name == "HAS_CHILD"
    assert rel.direction == Direction.OUT
    assert rel.to_label == "Child"


def test_embedding_spec():
    """Test embedding specification."""

    @graph_object(
        label="EmbedTest",
        key=["id"],
        service_id="test-svc",
        embeddings=[
            EmbeddingSpec(
                name="content",
                provider="test-provider",
                model="test-model",
                dim=128,
                source=EmbeddingSource(field="text")
            )
        ]
    )
    class EmbedTest(BasedModel):
        id: str
        text: str

    obj = EmbedTest(id="test-1", text="Hello world")

    # Verify embedding spec attached
    assert len(obj.__overlay__.embeddings) == 1
    emb = obj.__overlay__.embeddings[0]
    assert emb.name == "content"
    assert emb.provider == "test-provider"
    assert emb.dim == 128


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
