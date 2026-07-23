"""Per-service runtime settings.

Reserved for Phase 2, where each service gets a ``pydantic-settings``
``BaseSettings`` subclass (provider config, timeouts, feature flags) sharing a
common base. For now, services read addressing via
:mod:`dsco_shared.config.service_registry`.
"""
from __future__ import annotations
