"""
Testing Module

Chaos testing framework and Jepsen-style resilience testing.

Components:
- chaos_framework.py: Core chaos testing infrastructure
- jepsen_scenarios.py: Specific test scenarios based on Jepsen findings
"""

from .chaos_framework import (
    ChaosTestFramework,
    ChaosScenario,
    ChaosResult,
    FailureInjection,
    FailureType,
    FailureTarget,
    ScenarioBuilder,
)

from .jepsen_scenarios import (
    JepsenScenarios,
    run_jepsen_test_suite,
)

__all__ = [
    "ChaosTestFramework",
    "ChaosScenario",
    "ChaosResult",
    "FailureInjection",
    "FailureType",
    "FailureTarget",
    "ScenarioBuilder",
    "JepsenScenarios",
    "run_jepsen_test_suite",
]
