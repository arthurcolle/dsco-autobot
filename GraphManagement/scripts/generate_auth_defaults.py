#!/usr/bin/env python3
"""
Generate a sensible AUTH_API_KEYS payload for the Graph Management overlay.

The script works whether the FastAPI app is currently running or not:
- If the overlay is running, it reminds you to restart after exporting the
  generated value so the new keys take effect.
- If it is not running, it simply prints the exports you can use before launch.
"""

from __future__ import annotations

import argparse
import json
import secrets
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Sequence, Tuple

import httpx


@dataclass(frozen=True)
class KeyPlan:
    """Definition for an API key entry to generate."""

    label: str
    subject_suffix: str
    roles: Sequence[str]
    org_override: str | None = None
    scopes: Sequence[str] = field(default_factory=tuple)
    absolute_subject: bool = False


DEFAULT_PLANS: Tuple[KeyPlan, ...] = (
    KeyPlan(label="org_admin", subject_suffix="admin", roles=("org_admin",)),
    KeyPlan(label="developer", subject_suffix="developer", roles=("developer",)),
    KeyPlan(label="readonly", subject_suffix="readonly", roles=("readonly",)),
)

SERVICE_PLAN = KeyPlan(label="service", subject_suffix="service", roles=("service",))

PresetBuilder = Callable[[str], List[KeyPlan]]


def _preset_solo(org: str) -> List[KeyPlan]:
    """Single-owner shop: single key with broad rights plus optional observer."""
    return [
        KeyPlan(label="solo-owner", subject_suffix="owner", roles=("org_admin", "schema_admin", "developer")),
        KeyPlan(label="solo-observer", subject_suffix="observer", roles=("readonly",)),
    ]


def _preset_small_team(org: str) -> List[KeyPlan]:
    """Small/seed team: founder admin + builder + observer + ops bot."""
    return [
        KeyPlan(label="founder-admin", subject_suffix="founder", roles=("org_admin", "schema_admin")),
        KeyPlan(label="builder", subject_suffix="builder", roles=("developer", "service")),
        KeyPlan(label="observer", subject_suffix="observer", roles=("readonly",)),
        KeyPlan(label="ops-service", subject_suffix="ops", roles=("service",)),
    ]


def _preset_enterprise(org: str) -> List[KeyPlan]:
    """Larger org: distinct duties for platform, schema, app dev, read-only, runtime."""
    return [
        KeyPlan(label="platform-admin", subject_suffix="platform-admin", roles=("org_admin",)),
        KeyPlan(label="schema-admin", subject_suffix="schema-admin", roles=("schema_admin",)),
        KeyPlan(label="app-dev", subject_suffix="app-dev", roles=("developer",)),
        KeyPlan(label="observability", subject_suffix="observability", roles=("readonly",)),
        KeyPlan(label="runtime-service", subject_suffix="runtime", roles=("service",), scopes=("writer:company",)),
    ]


PRESET_BUILDERS: Dict[str, PresetBuilder] = {
    "solo": _preset_solo,
    "small-team": _preset_small_team,
    "enterprise": _preset_enterprise,
}


def _compact_json(data: Dict[str, Dict[str, object]]) -> str:
    """Return a single-line JSON string suitable for env exports."""
    return json.dumps(data, separators=(",", ":"))


def _pretty_json(data: Dict[str, Dict[str, object]]) -> str:
    """Return a readable JSON string for humans."""
    return json.dumps(data, indent=2, sort_keys=True)


def _subject_from_plan(plan: KeyPlan, prefix: str) -> str:
    """Build final subject id."""
    if plan.absolute_subject:
        return plan.subject_suffix
    return f"{prefix}-{plan.subject_suffix}" if prefix else plan.subject_suffix


def generate_keyset(
    org: str,
    prefix: str,
    include_service: bool,
    extra_plans: Iterable[KeyPlan],
    include_default_plans: bool,
) -> Tuple[Dict[str, Dict[str, object]], List[Tuple[str, str]]]:
    """Generate API key secrets and metadata."""
    plans: List[KeyPlan] = []
    if include_default_plans:
        plans.extend(DEFAULT_PLANS)
    if include_service:
        plans.append(SERVICE_PLAN)
    plans.extend(extra_plans)
    if not plans:
        raise SystemExit("No plans provided. Use defaults or pass --plan.")

    mapping: Dict[str, Dict[str, object]] = {}
    inventory: List[Tuple[str, str]] = []
    for plan in plans:
        secret = secrets.token_urlsafe(24)
        entry: Dict[str, object] = {
            "sub": _subject_from_plan(plan, prefix),
            "roles": list(plan.roles),
        }
        org_value = plan.org_override or org
        if org_value:
            entry["org"] = org_value
        if plan.scopes:
            entry["scopes"] = list(plan.scopes)
        mapping[secret] = entry
        inventory.append((plan.label, secret))
    return mapping, inventory


def parse_plan_arg(spec: str) -> KeyPlan:
    """
    Parse strings like:
    - "admin=org_admin"
    - "dashboard:dash=readonly"
    - "writer:@svc-writer:factory#writer:company=service"
    """
    if "=" not in spec:
        raise SystemExit(f"Invalid plan '{spec}'. Expected LABEL[:SUB][:ORG][#scope+a]=role1,role2")
    left, right = spec.split("=", 1)
    left = left.strip()
    right = right.strip()
    if not left or not right:
        raise SystemExit(f"Invalid plan '{spec}'. Label and roles cannot be empty.")

    roles = [r.strip() for r in right.split(",") if r.strip()]
    if not roles:
        raise SystemExit(f"Invalid plan '{spec}'. At least one role required.")

    scopes: List[str] = []
    if "#" in left:
        left, scope_part = left.split("#", 1)
        scopes = [s.strip() for s in scope_part.split("+") if s.strip()]

    label_parts = [part.strip() for part in left.split(":") if part.strip()]
    if not label_parts:
        raise SystemExit(f"Invalid plan '{spec}'. Missing label.")
    if len(label_parts) > 3:
        raise SystemExit(f"Invalid plan '{spec}'. Too many ':' sections.")

    label = label_parts[0]
    subject_suffix = label_parts[1] if len(label_parts) >= 2 else label
    org_override = label_parts[2] if len(label_parts) == 3 else None

    absolute_subject = False
    if subject_suffix.startswith("@"):
        absolute_subject = True
        subject_suffix = subject_suffix[1:]
    if not subject_suffix:
        raise SystemExit(f"Invalid plan '{spec}'. Subject cannot be empty.")

    return KeyPlan(
        label=label,
        subject_suffix=subject_suffix,
        roles=tuple(roles),
        org_override=org_override,
        scopes=tuple(scopes),
        absolute_subject=absolute_subject,
    )


def load_plan_file(path: Path) -> List[KeyPlan]:
    """Read plan definitions from a JSON file."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Plan file must be a JSON list of objects.")
    plans: List[KeyPlan] = []
    for entry in data:
        if not isinstance(entry, dict):
            raise SystemExit("Each plan entry must be a JSON object.")
        try:
            plan = KeyPlan(
                label=entry["label"],
                subject_suffix=entry.get("subject", entry["label"]),
                roles=tuple(entry["roles"]),
                org_override=entry.get("org"),
                scopes=tuple(entry.get("scopes", [])),
                absolute_subject=bool(entry.get("absolute_subject", False)),
            )
        except KeyError as exc:
            raise SystemExit(f"Plan entry missing field: {exc}") from exc
        plans.append(plan)
    return plans


def plans_for_presets(names: Sequence[str] | None, org: str) -> List[KeyPlan]:
    """Expand preset names into concrete plans."""
    plans: List[KeyPlan] = []
    if not names:
        return plans
    for name in names:
        builder = PRESET_BUILDERS.get(name)
        if not builder:
            raise SystemExit(f"Unknown preset '{name}'. Valid options: {', '.join(PRESET_BUILDERS)}")
        plans.extend(builder(org))
    return plans


def check_overlay(base_url: str) -> bool:
    """Return True if the overlay responds at /health."""
    url = f"{base_url.rstrip('/')}/health"
    try:
        resp = httpx.get(url, timeout=3.0)
    except httpx.RequestError:
        return False
    return resp.status_code == 200


def write_env_file(path: Path, payload: Dict[str, Dict[str, object]], force: bool, append: bool) -> None:
    """Write or append a file containing AUTH_API_KEYS=..."""
    if path.exists() and not (force or append):
        raise SystemExit(f"{path} already exists; pass --force to overwrite or --append-dotenv to append.")
    line = f"AUTH_API_KEYS='{_compact_json(payload)}'\n"
    if append and path.exists():
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line)
    else:
        path.write_text(line, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate AUTH_API_KEYS values for local or remote deployments.")
    parser.add_argument("--org", default="denmark", help="Organization id baked into generated entries (default: %(default)s).")
    parser.add_argument("--prefix", default="graph", help="Prefix for generated subject ids (default: %(default)s).")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000", help="Overlay URL to probe for status (default: %(default)s).")
    parser.add_argument("--write-dotenv", type=Path, help="Optional path to write AUTH_API_KEYS='...' so you can source it later.")
    parser.add_argument("--append-dotenv", action="store_true", help="Append to existing --write-dotenv instead of overwriting.")
    parser.add_argument("--force", action="store_true", help="Overwrite --write-dotenv destination if it exists.")
    parser.add_argument("--no-service-key", dest="include_service", action="store_false", help="Skip generating the service workload key.")
    parser.add_argument(
        "--preset",
        action="append",
        choices=sorted(PRESET_BUILDERS.keys()),
        help="Add a built-in plan set tuned for solo builders, small teams, or enterprise orgs. Repeatable.",
    )
    parser.add_argument("--no-default-plans", action="store_true", help="Only use user-provided --plan/--preset definitions.")
    parser.add_argument("--plan", action="append", help="Custom plan spec: LABEL[:SUB][:ORG][#scope+scope]=role1,role2. Repeatable.")
    parser.add_argument("--plan-file", type=Path, help="JSON file with plan entries [{label, roles, subject?, org?, scopes?, absolute_subject?}].")
    parser.set_defaults(include_service=True)
    return parser.parse_args()


def build_additional_plans(args: argparse.Namespace) -> List[KeyPlan]:
    """Collect plans defined via CLI args."""
    plans: List[KeyPlan] = []
    plans.extend(plans_for_presets(args.preset, args.org))
    if args.plan:
        for spec in args.plan:
            plans.append(parse_plan_arg(spec))
    if args.plan_file:
        plans.extend(load_plan_file(args.plan_file))
    return plans


def main() -> None:
    args = parse_args()
    extra_plans = build_additional_plans(args)
    payload, inventory = generate_keyset(
        org=args.org,
        prefix=args.prefix,
        include_service=args.include_service,
        extra_plans=extra_plans,
        include_default_plans=not args.no_default_plans,
    )

    print("Generated AUTH_API_KEYS payload:\n")
    print(_pretty_json(payload))
    print("\nShell export command:\n")
    print(f"export AUTH_API_KEYS='{_compact_json(payload)}'")

    print("\nKey inventory (use these secrets in Authorization headers):")
    for label, secret in inventory:
        display_label = label or "<unnamed>"
        print(f"  - {display_label:12s} -> {secret}")

    if args.write_dotenv:
        write_env_file(args.write_dotenv, payload, args.force, args.append_dotenv)
        action = "Appended" if args.append_dotenv else "Wrote"
        print(f"\n{action} AUTH_API_KEYS entry to {args.write_dotenv}. Source it via `set -a && source {args.write_dotenv} && set +a` or export manually.")

    overlay_up = check_overlay(args.base_url)
    if overlay_up:
        print(f"\nOverlay detected at {args.base_url}. Restart that process after exporting the new AUTH_API_KEYS value so these keys take effect.")
    else:
        print(f"\nNo overlay detected at {args.base_url}. Start it with `uvicorn graph_management_service:api --reload --port 8000` after exporting AUTH_API_KEYS.")

    print("\nExample curl usage:")
    example = inventory[0][1]
    print(f"  curl -H \"Authorization: Bearer {example}\" {args.base_url.rstrip('/')}/v1/schema")
    if len(inventory) > 1:
        print(f"  curl -H \"Authorization: Bearer {inventory[-1][1]}\" {args.base_url.rstrip('/')}/health")


if __name__ == "__main__":
    main()
