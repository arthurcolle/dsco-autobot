"""Codex Apps connector catalog adapter.

Turns the local Codex app-directory cache into small wrapper descriptors that
ToolManagement can reason about without claiming the catalog entries are live
callable integrations. The adapter is intentionally read-only: it normalizes
metadata, derives policy/action flags, and builds vertical bundles. Execution
must still go through an installed/live MCP connector.
"""

from __future__ import annotations

import glob
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

DEFAULT_DIRECTORY_GLOB = "~/.codex/cache/codex_app_directory/*.json"
DEFAULT_PLUGIN_MANIFEST_GLOB = "~/.codex/.tmp/plugins/plugins/*/.app.json"

COMMERCIAL_BUNDLES: Dict[str, Dict[str, Any]] = {
    "engineering_product": {
        "name": "Engineering/Product",
        "description": "Code, product planning, observability, deployment, and developer data systems.",
        "connectors": [
            "GitHub", "Linear", "Atlassian Rovo", "Azure Boards", "Slack", "Notion",
            "Datadog", "PostHog", "Statsig", "Neon Postgres", "Supabase", "Vercel",
            "Netlify", "Stripe", "Hugging Face", "Context7", "Tavily AI", "Jam",
            "Replay.io",
        ],
    },
    "gtm_support": {
        "name": "GTM/Support",
        "description": "Sales, support, customer context, and revenue workflow systems.",
        "connectors": [
            "HubSpot", "Pipedrive", "Close", "Intercom", "Help Scout", "Zoho",
            "Zoho Desk", "Apollo.io", "Clay", "Demandbase", "Common Room", "Attio",
            "ZoomInfo", "Outreach", "Pylon",
        ],
    },
    "finance_data": {
        "name": "Finance/Data",
        "description": "Financial data, BI, analytics, accounting, and market intelligence.",
        "connectors": [
            "BigQuery", "MotherDuck", "Hex", "ThoughtSpot Spotter", "Omni Analytics",
            "Intuit QuickBooks", "Brex", "Alpaca", "FactSet AI-Ready Data", "PitchBook",
            "Morningstar", "LSEG", "Moody's", "Moody's Credit MCP", "Daloopa", "Aiera",
            "Cube",
        ],
    },
    "governance_meta": {
        "name": "Governance/Meta",
        "description": "Safety, registry, readiness, enterprise knowledge, and cost-control surfaces.",
        "connectors": [
            "ToolCheck by M8ven", "Agent Ready", "HAPI MCP Registry", "Alation", "Atlan",
            "Coveo", "Glean", "Box", "SharePoint", "Vantage", "AccessOwl", "Radar Lite",
            "Bastion", "Neura Relay MCP",
        ],
    },
    "enterprise_knowledge": {
        "name": "Enterprise Knowledge",
        "description": "Documents, files, email, calendars, chats, and task context for retrieval-first agents.",
        "connectors": [
            "Gmail", "Google Calendar", "Google Drive", "Outlook Email", "Outlook Calendar",
            "SharePoint", "Teams", "Slack", "Box", "Dropbox", "Egnyte", "Notion",
            "Aha!", "Asana (synced)", "ClickUp (synced)", "Basecamp", "Teamwork.com",
        ],
    },
}

READ_LABELS = {"retrievable", "sync"}
WRITE_LABELS = {"consequential"}
FLOW_LABELS = {"interactive"}


@dataclass(frozen=True)
class CodexAppConnector:
    """Normalized connector entry from the Codex app-directory cache."""

    id: str
    name: str
    description: str = ""
    distribution_channel: str = ""
    install_url: str = ""
    is_enabled: bool = False
    is_accessible: bool = False
    labels: Dict[str, str] = field(default_factory=dict)
    categories: List[str] = field(default_factory=list)
    review_status: str = "UNSET"
    plugin_display_names: List[str] = field(default_factory=list)
    curated_slugs: List[str] = field(default_factory=list)

    @property
    def slug(self) -> str:
        if self.curated_slugs:
            return self.curated_slugs[0]
        return slugify(self.name or self.id)

    @property
    def catalog_status(self) -> str:
        if self.is_accessible:
            return "live"
        if self.is_enabled:
            return "catalog_only"
        return "disabled"

    @property
    def action_flags(self) -> List[str]:
        flags = {"catalog"}
        if any(self.labels.get(label) == "true" for label in READ_LABELS):
            flags.add("read")
        if self.labels.get("sync") == "true":
            flags.add("sync")
            flags.add("untrusted_content")
        if self.labels.get("retrievable") == "true":
            flags.add("untrusted_content")
        if any(self.labels.get(label) == "true" for label in WRITE_LABELS):
            flags.add("write")
            flags.add("requires_confirmation")
        if any(self.labels.get(label) == "true" for label in FLOW_LABELS):
            flags.add("interactive")
            flags.add("requires_user_flow")
        if not self.is_accessible:
            flags.add("not_live")
        return sorted(flags)

    @property
    def requires_confirmation(self) -> bool:
        return "requires_confirmation" in self.action_flags

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "slug": self.slug,
            "name": self.name,
            "description": self.description,
            "distribution_channel": self.distribution_channel,
            "install_url": self.install_url,
            "is_enabled": self.is_enabled,
            "is_accessible": self.is_accessible,
            "catalog_status": self.catalog_status,
            "labels": dict(self.labels),
            "categories": list(self.categories),
            "review_status": self.review_status,
            "plugin_display_names": list(self.plugin_display_names),
            "curated_slugs": list(self.curated_slugs),
            "action_flags": self.action_flags,
            "requires_confirmation": self.requires_confirmation,
        }


@dataclass(frozen=True)
class ConnectorWrapper:
    """Small wrapper descriptor for a connector catalog entry."""

    name: str
    connector_id: str
    display_name: str
    description: str
    category: str
    action_flags: List[str]
    catalog_status: str
    live: bool
    requires_confirmation: bool
    metadata: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "connector_id": self.connector_id,
            "display_name": self.display_name,
            "description": self.description,
            "category": self.category,
            "action_flags": list(self.action_flags),
            "catalog_status": self.catalog_status,
            "live": self.live,
            "requires_confirmation": self.requires_confirmation,
            "input_schema": {
                "type": "object",
                "properties": {
                    "operation": {
                        "type": "string",
                        "enum": ["describe", "authorize", "install", "proxy_call"],
                        "description": "Wrapper operation. proxy_call is only valid after the connector is live.",
                    },
                    "arguments": {
                        "type": "object",
                        "description": "Connector-specific arguments for a live proxy call.",
                    },
                },
                "required": ["operation"],
            },
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class CuratedPointer:
    slug: str
    connector_id: str
    manifest_path: str
    matched: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "slug": self.slug,
            "connector_id": self.connector_id,
            "manifest_path": self.manifest_path,
            "matched": self.matched,
        }


def slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "connector"


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def _expand_glob(pattern: str) -> List[Path]:
    return sorted(Path(p).expanduser() for p in glob.glob(os.path.expanduser(pattern)) if Path(p).expanduser().exists())


def _paths_from_env(env_name: str, default_glob: str) -> List[Path]:
    configured = os.getenv(env_name, "").strip()
    if configured:
        paths: List[Path] = []
        for part in configured.split(os.pathsep):
            if not part:
                continue
            p = Path(part).expanduser()
            if any(ch in part for ch in "*?[]"):
                paths.extend(_expand_glob(part))
            elif p.is_dir():
                paths.extend(sorted(p.glob("*.json")))
            else:
                paths.append(p)
        return [p for p in paths if p.exists()]
    return _expand_glob(default_glob)


class CodexAppsCatalogAdapter:
    """Loader and wrapper factory for the local Codex Apps connector cache."""

    def __init__(
        self,
        directory_paths: Optional[Sequence[Path | str]] = None,
        plugin_manifest_paths: Optional[Sequence[Path | str]] = None,
    ):
        self.directory_paths = [Path(p).expanduser() for p in directory_paths] if directory_paths else _paths_from_env(
            "CODEX_APP_DIRECTORY_CACHE", DEFAULT_DIRECTORY_GLOB
        )
        self.plugin_manifest_paths = [Path(p).expanduser() for p in plugin_manifest_paths] if plugin_manifest_paths else _paths_from_env(
            "CODEX_APP_PLUGIN_MANIFESTS", DEFAULT_PLUGIN_MANIFEST_GLOB
        )
        self._connectors: Optional[List[CodexAppConnector]] = None
        self._curated_pointers: Optional[List[CuratedPointer]] = None

    def load(self, force: bool = False) -> List[CodexAppConnector]:
        if self._connectors is not None and not force:
            return self._connectors

        raw_connectors: List[Dict[str, Any]] = []
        for path in self.directory_paths:
            data = _read_json(path)
            connectors = data.get("connectors", [])
            if not isinstance(connectors, list):
                raise ValueError(f"Invalid connector cache schema in {path}: connectors must be an array")
            raw_connectors.extend(c for c in connectors if isinstance(c, dict))

        slug_by_id = defaultdict(list)
        pointer_paths: Dict[Tuple[str, str], str] = {}
        for path in self.plugin_manifest_paths:
            try:
                apps = _read_json(path).get("apps", {})
            except Exception:
                continue
            if not isinstance(apps, dict):
                continue
            for slug, info in apps.items():
                if isinstance(info, dict) and info.get("id"):
                    cid = str(info["id"])
                    slug_by_id[cid].append(str(slug))
                    pointer_paths[(str(slug), cid)] = str(path)

        connectors = [self._normalize(raw, slug_by_id.get(str(raw.get("id", "")), [])) for raw in raw_connectors]
        self._connectors = connectors

        connector_ids = {c.id for c in connectors}
        self._curated_pointers = [
            CuratedPointer(slug=slug, connector_id=cid, manifest_path=manifest_path, matched=cid in connector_ids)
            for (slug, cid), manifest_path in sorted(pointer_paths.items())
        ]
        return connectors

    def _normalize(self, raw: Dict[str, Any], curated_slugs: List[str]) -> CodexAppConnector:
        app_metadata = raw.get("appMetadata") if isinstance(raw.get("appMetadata"), dict) else {}
        labels = raw.get("labels") if isinstance(raw.get("labels"), dict) else {}
        categories = app_metadata.get("categories") if isinstance(app_metadata.get("categories"), list) else []
        review = app_metadata.get("review") if isinstance(app_metadata.get("review"), dict) else {}
        plugin_display_names = raw.get("pluginDisplayNames") if isinstance(raw.get("pluginDisplayNames"), list) else []
        return CodexAppConnector(
            id=str(raw.get("id", "")),
            name=str(raw.get("name", "")),
            description=str(raw.get("description", "")),
            distribution_channel=str(raw.get("distributionChannel", "")),
            install_url=str(raw.get("installUrl", "")),
            is_enabled=_as_bool(raw.get("isEnabled")),
            is_accessible=_as_bool(raw.get("isAccessible")),
            labels={str(k): str(v).lower() for k, v in labels.items()},
            categories=[str(c) for c in categories],
            review_status=str(review.get("status", "UNSET")),
            plugin_display_names=[str(v) for v in plugin_display_names],
            curated_slugs=sorted(set(curated_slugs)),
        )

    def connectors(self) -> List[CodexAppConnector]:
        return self.load()

    def curated_pointers(self) -> List[CuratedPointer]:
        self.load()
        return list(self._curated_pointers or [])

    def stale_curated_pointers(self) -> List[CuratedPointer]:
        return [p for p in self.curated_pointers() if not p.matched]

    def summary(self) -> Dict[str, Any]:
        connectors = self.connectors()
        category_counts = Counter(c for conn in connectors for c in conn.categories)
        label_counts = Counter(
            f"{k}=true"
            for conn in connectors
            for k, v in conn.labels.items()
            if str(v).lower() == "true"
        )
        distribution_counts = Counter(conn.distribution_channel for conn in connectors)
        accessibility_counts = Counter(f"enabled={conn.is_enabled},accessible={conn.is_accessible}" for conn in connectors)
        curated = self.curated_pointers()
        return {
            "total_connectors": len(connectors),
            "distribution_channels": dict(distribution_counts),
            "categories": dict(category_counts),
            "labels": dict(label_counts),
            "accessibility": dict(accessibility_counts),
            "curated_pointers": len(curated),
            "curated_matched": sum(1 for p in curated if p.matched),
            "curated_stale": sum(1 for p in curated if not p.matched),
            "directory_paths": [str(p) for p in self.directory_paths],
            "plugin_manifest_count": len(self.plugin_manifest_paths),
        }

    def query(
        self,
        *,
        q: str = "",
        category: str = "",
        label: str = "",
        distribution_channel: str = "",
        accessible: Optional[bool] = None,
        curated_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        q_norm = q.strip().lower()
        label_norm = label.strip().lower()
        category_norm = category.strip().upper()
        channel_norm = distribution_channel.strip().upper()

        rows = self.connectors()
        if q_norm:
            rows = [c for c in rows if q_norm in c.name.lower() or q_norm in c.description.lower() or q_norm in c.id.lower()]
        if category_norm:
            rows = [c for c in rows if any(cat.upper() == category_norm for cat in c.categories)]
        if label_norm:
            rows = [c for c in rows if c.labels.get(label_norm) == "true"]
        if channel_norm:
            rows = [c for c in rows if c.distribution_channel.upper() == channel_norm]
        if accessible is not None:
            rows = [c for c in rows if c.is_accessible is accessible]
        if curated_only:
            rows = [c for c in rows if c.curated_slugs]

        total = len(rows)
        offset = max(offset, 0)
        limit = min(max(limit, 1), 1000)
        page = rows[offset : offset + limit]
        return {
            "total": total,
            "offset": offset,
            "limit": limit,
            "has_more": total > offset + len(page),
            "connectors": [c.to_dict() for c in page],
        }

    def get(self, key: str) -> Optional[CodexAppConnector]:
        key_norm = key.strip().lower()
        # Exact curated slug first; then exact id/name/slug.
        for conn in self.connectors():
            if key_norm in {slug.lower() for slug in conn.curated_slugs}:
                return conn
        for conn in self.connectors():
            if conn.id.lower() == key_norm or conn.slug.lower() == key_norm or conn.name.lower() == key_norm:
                return conn
        return None

    def build_wrapper(self, connector: CodexAppConnector | str) -> ConnectorWrapper:
        conn = self.get(connector) if isinstance(connector, str) else connector
        if conn is None:
            raise KeyError(f"Connector not found: {connector}")
        category = conn.categories[0].lower() if conn.categories else "uncategorized"
        wrapper_name = f"codex_app__{slugify(conn.slug)}"
        return ConnectorWrapper(
            name=wrapper_name,
            connector_id=conn.id,
            display_name=conn.name,
            description=conn.description,
            category=category,
            action_flags=conn.action_flags,
            catalog_status=conn.catalog_status,
            live=conn.is_accessible,
            requires_confirmation=conn.requires_confirmation,
            metadata={
                "source": "codex_app_directory",
                "distribution_channel": conn.distribution_channel,
                "install_url": conn.install_url,
                "labels": conn.labels,
                "categories": conn.categories,
                "curated_slugs": conn.curated_slugs,
                "not_executable_reason": None if conn.is_accessible else "connector is present in the catalog cache but not accessible/live in this session",
            },
        )

    def wrappers(self, **query_kwargs: Any) -> Dict[str, Any]:
        result = self.query(**query_kwargs)
        wrappers = [self.build_wrapper(c["id"]).to_dict() for c in result["connectors"]]
        return {**{k: v for k, v in result.items() if k != "connectors"}, "wrappers": wrappers}

    def bundles(self) -> List[Dict[str, Any]]:
        return [self.bundle(bundle_id) for bundle_id in COMMERCIAL_BUNDLES]

    def bundle(self, bundle_id: str) -> Dict[str, Any]:
        if bundle_id not in COMMERCIAL_BUNDLES:
            raise KeyError(f"Unknown bundle: {bundle_id}")
        spec = COMMERCIAL_BUNDLES[bundle_id].copy()
        connectors = []
        missing = []
        for name in spec["connectors"]:
            conn = self.get(name)
            if conn:
                connectors.append(self.build_wrapper(conn).to_dict())
            else:
                missing.append(name)
        return {
            "id": bundle_id,
            "name": spec["name"],
            "description": spec["description"],
            "connectors": connectors,
            "missing": missing,
            "total": len(connectors),
            "live": sum(1 for c in connectors if c["live"]),
            "catalog_only": sum(1 for c in connectors if not c["live"]),
            "requires_confirmation": sum(1 for c in connectors if c["requires_confirmation"]),
        }


def get_codex_apps_adapter() -> CodexAppsCatalogAdapter:
    """Factory kept small so tests can instantiate isolated adapters directly."""
    return CodexAppsCatalogAdapter()
