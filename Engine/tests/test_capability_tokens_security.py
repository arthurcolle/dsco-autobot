"""Security invariants for the Autobot capability-token boundary."""

from dataclasses import replace

import pytest

from substrate.capability_tokens import (
    CapabilityScope,
    CapabilityTokenManager,
    Permissions,
)


@pytest.fixture
def manager():
    private_key, public_key = CapabilityTokenManager.generate_keypair()
    return CapabilityTokenManager("issuer-a", private_key, public_key)


def test_signed_token_verifies_and_tampering_fails(manager):
    token = manager.create_token(
        "agent-a",
        Permissions.READ_NODE,
        CapabilityScope.nodes_scope(["node-a"]),
    )

    assert manager.verify_token(token)
    assert not manager.verify_token(replace(token, holder_id="agent-b"))


def test_unsigned_token_is_rejected(manager):
    token = manager.create_token(
        "agent-a",
        Permissions.READ_NODE,
        CapabilityScope.global_scope(),
    )

    assert not manager.verify_token(replace(token, signature="unsigned"))
    with pytest.raises(ValueError, match="unsigned capability tokens are forbidden"):
        CapabilityTokenManager("issuer-a").create_token(
            "agent-a", Permissions.READ_NODE, CapabilityScope.global_scope()
        )


def test_delegation_requires_permission_and_scope_attenuation(manager):
    parent = manager.create_token(
        "agent-a",
        Permissions.READ_NODE | Permissions.WRITE_NODE,
        CapabilityScope.nodes_scope(["node-a", "node-b"]),
    )
    child = manager.delegate_token(
        parent,
        "agent-b",
        Permissions.READ_NODE,
        CapabilityScope.nodes_scope(["node-a"]),
    )

    assert manager.verify_token(child)
    assert child.parent_token_id == parent.token_id
    with pytest.raises(ValueError, match="Cannot expand or change scope"):
        manager.delegate_token(
            parent,
            "agent-c",
            Permissions.READ_NODE,
            CapabilityScope.nodes_scope(["node-c"]),
        )
    with pytest.raises(ValueError, match="Cannot expand permissions"):
        manager.delegate_token(
            parent,
            "agent-c",
            Permissions.DELETE_NODE,
            CapabilityScope.nodes_scope(["node-a"]),
        )


def test_unresolved_scope_fails_closed(manager):
    token = manager.create_token(
        "agent-a",
        Permissions.READ_NODE,
        CapabilityScope.shard_scope(7),
    )

    assert not manager.check_authorization(token, Permissions.READ_NODE, "node-a")
