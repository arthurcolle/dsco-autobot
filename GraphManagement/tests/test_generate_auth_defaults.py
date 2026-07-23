from __future__ import annotations

from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from scripts.generate_auth_defaults import (  # noqa: E402
    parse_plan_arg,
    generate_keyset,
    plans_for_presets,
)


def test_parse_plan_with_subject_org_and_scopes():
    plan = parse_plan_arg("writer:svc-writer:factory#writer:company+writer:data=service")

    assert plan.label == "writer"
    assert plan.subject_suffix == "svc-writer"
    assert plan.org_override == "factory"
    assert list(plan.scopes) == ["writer:company", "writer:data"]
    assert plan.roles == ("service",)


def test_parse_plan_absolute_subject():
    plan = parse_plan_arg("admin:@root=org_admin")

    assert plan.absolute_subject is True
    assert plan.subject_suffix == "root"
    assert plan.roles == ("org_admin",)


def test_generate_keyset_with_custom_plan():
    plan = parse_plan_arg("dash:dashboard:acme=readonly,developer")
    payload, inventory = generate_keyset(
        org="default-org",
        prefix="pref",
        include_service=False,
        extra_plans=[plan],
        include_default_plans=False,
    )

    assert len(payload) == 1
    secret = inventory[0][1]
    entry = payload[secret]
    assert entry["sub"] == "pref-dashboard"
    assert entry["org"] == "acme"
    assert entry["roles"] == ["readonly", "developer"]


def test_preset_solo():
    plans = plans_for_presets(["solo"], "acme")
    # Solo preset yields owner + observer
    assert len(plans) == 2
    owner = next(p for p in plans if p.label == "solo-owner")
    assert owner.roles == ("org_admin", "schema_admin", "developer")
