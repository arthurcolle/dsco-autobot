import pytest

from native.distributed_publish_mcp.server import DistributedPublishMCP


class FakeSubstackClient:
    publication = "example.substack.com"

    def __init__(self):
        self.published = []
        self.notes = []

    def publish_draft(self, draft_id, send_email=False):
        self.published.append((draft_id, send_email))
        return {"canonical_url": f"https://example.substack.com/p/{draft_id}"}

    def post_note(self, text):
        self.notes.append(text)
        return {"id": "note-1"}


def test_substack_is_a_discoverable_core_feature_without_worker_credentials(monkeypatch):
    monkeypatch.delenv("SUBSTACK_SID", raising=False)
    monkeypatch.delenv("SUBSTACK_PUBLICATION", raising=False)
    server = DistributedPublishMCP()
    names = {tool["name"] for tool in server.tools}

    assert "substack_connection_status" in names
    assert "substack_create_draft" in names
    assert "substack_publish" in names
    assert "substack_post_note" in names
    assert "substack_generate_image" in names


@pytest.mark.asyncio
async def test_publication_and_subscriber_email_have_separate_confirmation_gates():
    server = DistributedPublishMCP()
    fake = FakeSubstackClient()
    server._substack_client = fake

    with pytest.raises(ValueError, match="confirm_publish=true"):
        await server._execute_substack_publish({"draft_id": 42})
    with pytest.raises(ValueError, match="confirm_email=true"):
        await server._execute_substack_publish({
            "draft_id": 42,
            "confirm_publish": True,
            "send_email": True,
        })
    assert fake.published == []

    result = await server._execute_substack_publish({
        "draft_id": 42,
        "confirm_publish": True,
        "send_email": True,
        "confirm_email": True,
    })
    assert result["success"] is True
    assert fake.published == [(42, True)]


@pytest.mark.asyncio
async def test_public_note_requires_explicit_confirmation():
    server = DistributedPublishMCP()
    fake = FakeSubstackClient()
    server._substack_client = fake

    with pytest.raises(ValueError, match="confirm_post=true"):
        await server._execute_substack_post_note({"text": "review me"})
    assert fake.notes == []
