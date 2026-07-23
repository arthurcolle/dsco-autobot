"""OpenAPI schema diffing engine."""

import logging
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ChangeType(str, Enum):
    """Types of schema changes."""

    # Endpoints
    ENDPOINT_ADDED = "endpoint_added"
    ENDPOINT_REMOVED = "endpoint_removed"
    ENDPOINT_MODIFIED = "endpoint_modified"
    ENDPOINT_DEPRECATED = "endpoint_deprecated"

    # Parameters
    PARAMETER_ADDED = "parameter_added"
    PARAMETER_REMOVED = "parameter_removed"
    PARAMETER_REQUIRED_CHANGED = "parameter_required_changed"
    PARAMETER_TYPE_CHANGED = "parameter_type_changed"

    # Request/Response
    REQUEST_BODY_CHANGED = "request_body_changed"
    RESPONSE_SCHEMA_CHANGED = "response_schema_changed"

    # Models
    MODEL_ADDED = "model_added"
    MODEL_REMOVED = "model_removed"
    MODEL_FIELD_ADDED = "model_field_added"
    MODEL_FIELD_REMOVED = "model_field_removed"
    MODEL_FIELD_TYPE_CHANGED = "model_field_type_changed"

    # Security
    SECURITY_CHANGED = "security_changed"

    # Versioning
    VERSION_CHANGED = "version_changed"


class ChangeSeverity(str, Enum):
    """Severity of a schema change."""

    BREAKING = "breaking"  # Requires code changes
    MAJOR = "major"  # Significant change, should be reviewed
    MINOR = "minor"  # Minor change, low impact
    PATCH = "patch"  # Trivial change, no impact


class SchemaChange(BaseModel):
    """Represents a single schema change."""

    type: ChangeType
    severity: ChangeSeverity
    path: str  # JSON path to the changed element
    description: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    affected_endpoints: List[str] = Field(default_factory=list)


class SchemaDiffReport(BaseModel):
    """Complete diff report between two schemas."""

    old_version: str
    new_version: str
    total_changes: int
    breaking_changes: int
    major_changes: int
    minor_changes: int
    patch_changes: int
    changes: List[SchemaChange]

    def has_breaking_changes(self) -> bool:
        """Check if there are any breaking changes."""
        return self.breaking_changes > 0

    def get_changes_by_severity(self, severity: ChangeSeverity) -> List[SchemaChange]:
        """Get changes by severity level."""
        return [c for c in self.changes if c.severity == severity]


class SchemaDiffer:
    """
    Intelligent OpenAPI schema diffing engine.

    Detects:
    - New/removed endpoints
    - Parameter changes
    - Model schema changes
    - Breaking vs non-breaking changes
    - Security changes
    """

    def diff_schemas(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any]
    ) -> SchemaDiffReport:
        """
        Diff two OpenAPI schemas.

        Args:
            old_schema: Previous schema version
            new_schema: New schema version

        Returns:
            Comprehensive diff report
        """
        changes: List[SchemaChange] = []

        # Version change
        old_version = old_schema.get("info", {}).get("version", "unknown")
        new_version = new_schema.get("info", {}).get("version", "unknown")

        if old_version != new_version:
            changes.append(SchemaChange(
                type=ChangeType.VERSION_CHANGED,
                severity=ChangeSeverity.PATCH,
                path="/info/version",
                description=f"Version updated from {old_version} to {new_version}",
                old_value=old_version,
                new_value=new_version
            ))

        # Diff endpoints
        changes.extend(self._diff_paths(
            old_schema.get("paths", {}),
            new_schema.get("paths", {})
        ))

        # Diff models
        changes.extend(self._diff_models(
            old_schema.get("components", {}).get("schemas", {}),
            new_schema.get("components", {}).get("schemas", {})
        ))

        # Diff security schemes
        changes.extend(self._diff_security(
            old_schema.get("components", {}).get("securitySchemes", {}),
            new_schema.get("components", {}).get("securitySchemes", {})
        ))

        # Build report
        report = SchemaDiffReport(
            old_version=old_version,
            new_version=new_version,
            total_changes=len(changes),
            breaking_changes=sum(1 for c in changes if c.severity == ChangeSeverity.BREAKING),
            major_changes=sum(1 for c in changes if c.severity == ChangeSeverity.MAJOR),
            minor_changes=sum(1 for c in changes if c.severity == ChangeSeverity.MINOR),
            patch_changes=sum(1 for c in changes if c.severity == ChangeSeverity.PATCH),
            changes=changes
        ))

        logger.info(
            f"Schema diff complete: {report.total_changes} changes "
            f"({report.breaking_changes} breaking, {report.major_changes} major, "
            f"{report.minor_changes} minor, {report.patch_changes} patch)"
        )

        return report

    def _diff_paths(
        self,
        old_paths: Dict[str, Any],
        new_paths: Dict[str, Any]
    ) -> List[SchemaChange]:
        """Diff API endpoints."""
        changes: List[SchemaChange] = []

        old_endpoints = self._extract_endpoints(old_paths)
        new_endpoints = self._extract_endpoints(new_paths)

        # Find added endpoints
        added = new_endpoints - old_endpoints
        for endpoint in added:
            method, path = endpoint.split(" ", 1)
            changes.append(SchemaChange(
                type=ChangeType.ENDPOINT_ADDED,
                severity=ChangeSeverity.MINOR,
                path=f"/paths{path}/{method}",
                description=f"New endpoint: {method.upper()} {path}",
                new_value=endpoint
            ))

        # Find removed endpoints
        removed = old_endpoints - new_endpoints
        for endpoint in removed:
            method, path = endpoint.split(" ", 1)
            changes.append(SchemaChange(
                type=ChangeType.ENDPOINT_REMOVED,
                severity=ChangeSeverity.BREAKING,
                path=f"/paths{path}/{method}",
                description=f"Removed endpoint: {method.upper()} {path}",
                old_value=endpoint
            ))

        # Find modified endpoints
        common = old_endpoints & new_endpoints
        for endpoint in common:
            method, path = endpoint.split(" ", 1)
            old_spec = old_paths.get(path, {}).get(method, {})
            new_spec = new_paths.get(path, {}).get(method, {})

            # Check if deprecated
            if not old_spec.get("deprecated") and new_spec.get("deprecated"):
                changes.append(SchemaChange(
                    type=ChangeType.ENDPOINT_DEPRECATED,
                    severity=ChangeSeverity.MAJOR,
                    path=f"/paths{path}/{method}/deprecated",
                    description=f"Endpoint deprecated: {method.upper()} {path}",
                    old_value=False,
                    new_value=True,
                    affected_endpoints=[endpoint]
                ))

            # Diff parameters
            changes.extend(self._diff_parameters(
                old_spec.get("parameters", []),
                new_spec.get("parameters", []),
                f"{method.upper()} {path}"
            ))

            # Diff request body
            if old_spec.get("requestBody") != new_spec.get("requestBody"):
                changes.append(SchemaChange(
                    type=ChangeType.REQUEST_BODY_CHANGED,
                    severity=ChangeSeverity.MAJOR,
                    path=f"/paths{path}/{method}/requestBody",
                    description=f"Request body changed: {method.upper()} {path}",
                    affected_endpoints=[endpoint]
                ))

            # Diff responses
            if old_spec.get("responses") != new_spec.get("responses"):
                changes.append(SchemaChange(
                    type=ChangeType.RESPONSE_SCHEMA_CHANGED,
                    severity=ChangeSeverity.MAJOR,
                    path=f"/paths{path}/{method}/responses",
                    description=f"Response schema changed: {method.upper()} {path}",
                    affected_endpoints=[endpoint]
                ))

        return changes

    def _diff_parameters(
        self,
        old_params: List[Dict[str, Any]],
        new_params: List[Dict[str, Any]],
        endpoint: str
    ) -> List[SchemaChange]:
        """Diff endpoint parameters."""
        changes: List[SchemaChange] = []

        old_param_names = {p["name"] for p in old_params}
        new_param_names = {p["name"] for p in new_params}

        # Added parameters
        for param_name in new_param_names - old_param_names:
            param = next(p for p in new_params if p["name"] == param_name)
            is_required = param.get("required", False)

            changes.append(SchemaChange(
                type=ChangeType.PARAMETER_ADDED,
                severity=ChangeSeverity.BREAKING if is_required else ChangeSeverity.MINOR,
                path=f"/parameters/{param_name}",
                description=f"Parameter added to {endpoint}: {param_name} ({'required' if is_required else 'optional'})",
                new_value=param,
                affected_endpoints=[endpoint]
            ))

        # Removed parameters
        for param_name in old_param_names - new_param_names:
            param = next(p for p in old_params if p["name"] == param_name)
            changes.append(SchemaChange(
                type=ChangeType.PARAMETER_REMOVED,
                severity=ChangeSeverity.BREAKING,
                path=f"/parameters/{param_name}",
                description=f"Parameter removed from {endpoint}: {param_name}",
                old_value=param,
                affected_endpoints=[endpoint]
            ))

        # Modified parameters
        for param_name in old_param_names & new_param_names:
            old_param = next(p for p in old_params if p["name"] == param_name)
            new_param = next(p for p in new_params if p["name"] == param_name)

            # Check required status
            old_required = old_param.get("required", False)
            new_required = new_param.get("required", False)

            if old_required != new_required:
                changes.append(SchemaChange(
                    type=ChangeType.PARAMETER_REQUIRED_CHANGED,
                    severity=ChangeSeverity.BREAKING if new_required else ChangeSeverity.MAJOR,
                    path=f"/parameters/{param_name}/required",
                    description=f"Parameter '{param_name}' in {endpoint} is now {'required' if new_required else 'optional'}",
                    old_value=old_required,
                    new_value=new_required,
                    affected_endpoints=[endpoint]
                ))

            # Check type changes
            old_type = old_param.get("schema", {}).get("type")
            new_type = new_param.get("schema", {}).get("type")

            if old_type != new_type:
                changes.append(SchemaChange(
                    type=ChangeType.PARAMETER_TYPE_CHANGED,
                    severity=ChangeSeverity.BREAKING,
                    path=f"/parameters/{param_name}/schema/type",
                    description=f"Parameter '{param_name}' type changed in {endpoint}: {old_type} -> {new_type}",
                    old_value=old_type,
                    new_value=new_type,
                    affected_endpoints=[endpoint]
                ))

        return changes

    def _diff_models(
        self,
        old_models: Dict[str, Any],
        new_models: Dict[str, Any]
    ) -> List[SchemaChange]:
        """Diff data models."""
        changes: List[SchemaChange] = []

        old_model_names = set(old_models.keys())
        new_model_names = set(new_models.keys())

        # Added models
        for model_name in new_model_names - old_model_names:
            changes.append(SchemaChange(
                type=ChangeType.MODEL_ADDED,
                severity=ChangeSeverity.MINOR,
                path=f"/components/schemas/{model_name}",
                description=f"New model: {model_name}",
                new_value=new_models[model_name]
            ))

        # Removed models
        for model_name in old_model_names - new_model_names:
            changes.append(SchemaChange(
                type=ChangeType.MODEL_REMOVED,
                severity=ChangeSeverity.BREAKING,
                path=f"/components/schemas/{model_name}",
                description=f"Model removed: {model_name}",
                old_value=old_models[model_name]
            ))

        # Modified models
        for model_name in old_model_names & new_model_names:
            old_model = old_models[model_name]
            new_model = new_models[model_name]

            # Diff properties
            old_props = old_model.get("properties", {})
            new_props = new_model.get("properties", {})

            old_prop_names = set(old_props.keys())
            new_prop_names = set(new_props.keys())

            # Added fields
            for prop_name in new_prop_names - old_prop_names:
                # Check if required
                is_required = prop_name in new_model.get("required", [])

                changes.append(SchemaChange(
                    type=ChangeType.MODEL_FIELD_ADDED,
                    severity=ChangeSeverity.BREAKING if is_required else ChangeSeverity.MINOR,
                    path=f"/components/schemas/{model_name}/properties/{prop_name}",
                    description=f"Field added to {model_name}: {prop_name} ({'required' if is_required else 'optional'})",
                    new_value=new_props[prop_name]
                ))

            # Removed fields
            for prop_name in old_prop_names - new_prop_names:
                changes.append(SchemaChange(
                    type=ChangeType.MODEL_FIELD_REMOVED,
                    severity=ChangeSeverity.BREAKING,
                    path=f"/components/schemas/{model_name}/properties/{prop_name}",
                    description=f"Field removed from {model_name}: {prop_name}",
                    old_value=old_props[prop_name]
                ))

            # Modified fields (type changes)
            for prop_name in old_prop_names & new_prop_names:
                old_prop_type = old_props[prop_name].get("type")
                new_prop_type = new_props[prop_name].get("type")

                if old_prop_type != new_prop_type:
                    changes.append(SchemaChange(
                        type=ChangeType.MODEL_FIELD_TYPE_CHANGED,
                        severity=ChangeSeverity.BREAKING,
                        path=f"/components/schemas/{model_name}/properties/{prop_name}/type",
                        description=f"Field type changed in {model_name}.{prop_name}: {old_prop_type} -> {new_prop_type}",
                        old_value=old_prop_type,
                        new_value=new_prop_type
                    ))

        return changes

    def _diff_security(
        self,
        old_security: Dict[str, Any],
        new_security: Dict[str, Any]
    ) -> List[SchemaChange]:
        """Diff security schemes."""
        changes: List[SchemaChange] = []

        if old_security != new_security:
            changes.append(SchemaChange(
                type=ChangeType.SECURITY_CHANGED,
                severity=ChangeSeverity.BREAKING,
                path="/components/securitySchemes",
                description="Security schemes changed",
                old_value=list(old_security.keys()),
                new_value=list(new_security.keys())
            ))

        return changes

    def _extract_endpoints(self, paths: Dict[str, Any]) -> Set[str]:
        """Extract set of endpoint identifiers (method + path)."""
        endpoints = set()
        for path, methods in paths.items():
            for method in ["get", "post", "put", "patch", "delete", "head", "options"]:
                if method in methods:
                    endpoints.add(f"{method} {path}")
        return endpoints
