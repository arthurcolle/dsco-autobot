#!/usr/bin/env python3
"""
INTEGRATION TEST - Autonomous System Inter-Operation

Tests the complete pipeline:
1. Learning from knowledge graph
2. Pattern extraction and analysis
3. GOAP planning for self-improvement
4. Code generation with verification
5. Parallel task execution
6. Cross-system message routing
"""

import sys
import time
from pathlib import Path
from dataclasses import dataclass
from typing import List, Tuple

# Test results tracking
@dataclass
class TestResult:
    name: str
    passed: bool
    duration: float
    details: str = ""

class IntegrationTester:
    def __init__(self):
        self.results: List[TestResult] = []
        self.runtime = None
        self.bus = None

    def setup(self):
        """Initialize the unified runtime."""
        from meta_protocol import UnifiedRuntime
        self.runtime = UnifiedRuntime()
        init_result = self.runtime.initialize()
        self.bus = self.runtime.orchestrator.bus
        return len(init_result.get('systems', {})) >= 3

    def run_test(self, name: str, test_fn) -> TestResult:
        """Run a single test and record results."""
        start = time.time()
        try:
            passed, details = test_fn()
            duration = time.time() - start
            result = TestResult(name, passed, duration, details)
        except Exception as e:
            duration = time.time() - start
            result = TestResult(name, False, duration, f"Exception: {e}")
        self.results.append(result)
        return result

    # =========================================================================
    # TESTS
    # =========================================================================

    def test_learning(self) -> Tuple[bool, str]:
        """Test pattern learning from knowledge graph."""
        from meta_protocol import Message, MessageType

        resp = self.bus.request(Message(
            source="test",
            target="learner",
            action="learn",
            payload={"max_files": 500}
        ), timeout=60)

        if resp.type == MessageType.ERROR:
            return False, f"Error: {resp.payload.get('error')}"

        result = resp.payload.get("result", {})
        functions = result.get("functions_learned", 0)
        classes = result.get("classes_learned", 0)

        passed = functions > 1000 and classes > 100
        return passed, f"Functions: {functions}, Classes: {classes}"

    def test_pattern_extraction(self) -> Tuple[bool, str]:
        """Test pattern extraction."""
        from meta_protocol import Message, MessageType

        resp = self.bus.request(Message(
            source="test",
            target="learner",
            action="get_patterns",
            payload={"type": "all"}
        ), timeout=30)

        if resp.type == MessageType.ERROR:
            return False, f"Error: {resp.payload.get('error')}"

        patterns = resp.payload.get("patterns", {})
        has_imports = len(patterns.get("imports", {})) > 0
        has_decorators = len(patterns.get("decorators", {})) > 0
        has_functions = len(patterns.get("functions", [])) > 0

        passed = has_imports and has_decorators and has_functions
        return passed, f"Imports: {has_imports}, Decorators: {has_decorators}, Functions: {has_functions}"

    def test_goap_planning(self) -> Tuple[bool, str]:
        """Test GOAP development planning."""
        from meta_protocol import Message, MessageType

        resp = self.bus.request(Message(
            source="test",
            target="planner",
            action="get_goals",
            payload={}
        ), timeout=30)

        if resp.type == MessageType.ERROR:
            return False, f"Error: {resp.payload.get('error')}"

        goals = resp.payload.get("goals", {})
        has_ultimate = "ultimate" in goals
        has_learn = "learn_codebase" in goals

        passed = has_ultimate and has_learn and len(goals) >= 5
        return passed, f"Goals: {list(goals.keys())[:5]}"

    def test_state_assessment(self) -> Tuple[bool, str]:
        """Test state assessment."""
        from meta_protocol import Message, MessageType

        resp = self.bus.request(Message(
            source="test",
            target="planner",
            action="assess_state",
            payload={
                "capabilities": {
                    "file_read": True,
                    "file_write": True,
                    "code_exec": True,
                    "llm_access": True,
                    "knowledge_graph": True,
                    "patterns_learned": 100,
                    "functions_known": 1000,
                    "classes_known": 100
                }
            }
        ), timeout=30)

        if resp.type == MessageType.ERROR:
            return False, f"Error: {resp.payload.get('error')}"

        state = resp.payload.get("state", {})
        passed = len(state) >= 5
        return passed, f"State keys: {len(state)}"

    def test_code_generation(self) -> Tuple[bool, str]:
        """Test code generation."""
        from meta_protocol import Message, MessageType

        resp = self.bus.request(Message(
            source="test",
            target="generator",
            action="generate_function",
            payload={
                "name": "test_hash",
                "purpose": "Compute MD5 hash of input string"
            }
        ), timeout=90)

        if resp.type == MessageType.ERROR:
            return False, f"Error: {resp.payload.get('error')}"

        code = resp.payload.get("code", "")
        valid = resp.payload.get("valid", False)

        passed = len(code) > 50 and valid
        return passed, f"Generated: {len(code)} chars, Valid: {valid}"

    def test_code_verification(self) -> Tuple[bool, str]:
        """Test code verification."""
        from meta_protocol import Message, MessageType

        test_code = '''
def example_function(x: int, y: int) -> int:
    """Add two numbers."""
    return x + y
'''

        resp = self.bus.request(Message(
            source="test",
            target="generator",
            action="verify",
            payload={"code": test_code}
        ), timeout=30)

        if resp.type == MessageType.ERROR:
            return False, f"Error: {resp.payload.get('error')}"

        verification = resp.payload.get("verification", {})
        syntax_ok = verification.get("syntax", {}).get("passed", False)

        passed = syntax_ok
        return passed, f"Syntax: {syntax_ok}"

    def test_parallel_execution(self) -> Tuple[bool, str]:
        """Test parallel task execution."""
        from meta_protocol import Message, MessageType

        resp = self.bus.request(Message(
            source="test",
            target="executor",
            action="parallel_execute",
            payload={
                "tasks": [
                    "Task A: analyze",
                    "Task B: process",
                    "Task C: verify"
                ]
            }
        ), timeout=60)

        if resp.type == MessageType.ERROR:
            return False, f"Error: {resp.payload.get('error')}"

        results = resp.payload.get("results", [])
        passed = len(results) == 3
        return passed, f"Tasks completed: {len(results)}"

    def test_workflow_execution(self) -> Tuple[bool, str]:
        """Test workflow execution."""
        result = self.runtime.orchestrator.run_workflow("self_improve")
        passed = result.get("success", False)
        return passed, f"Workflow success: {passed}"

    def test_message_bus(self) -> Tuple[bool, str]:
        """Test message bus functionality."""
        history = self.bus.get_history()
        passed = len(history) > 0
        return passed, f"Messages in history: {len(history)}"

    def test_cross_system_routing(self) -> Tuple[bool, str]:
        """Test routing between all systems."""
        from meta_protocol import Message, MessageType

        targets = ["learner", "generator", "planner", "executor"]
        actions = ["get_knowledge", "verify", "get_goals", "get_executor_stats"]
        payloads = [{}, {"code": "pass"}, {}, {}]

        successes = 0
        for target, action, payload in zip(targets, actions, payloads):
            resp = self.bus.request(Message(
                source="test",
                target=target,
                action=action,
                payload=payload
            ), timeout=30)
            if resp.type != MessageType.ERROR:
                successes += 1

        passed = successes == 4
        return passed, f"Routes working: {successes}/4"

    # =========================================================================
    # RUNNER
    # =========================================================================

    def run_all(self):
        """Run all integration tests."""
        print("INTEGRATION TEST SUITE")
        print("=" * 70)
        print()

        # Setup
        print("Setting up unified runtime...")
        if not self.setup():
            print("SETUP FAILED")
            return False
        print("Setup complete.\n")

        # Define tests
        tests = [
            ("Learning", self.test_learning),
            ("Pattern Extraction", self.test_pattern_extraction),
            ("GOAP Planning", self.test_goap_planning),
            ("State Assessment", self.test_state_assessment),
            ("Code Generation", self.test_code_generation),
            ("Code Verification", self.test_code_verification),
            ("Parallel Execution", self.test_parallel_execution),
            ("Workflow Execution", self.test_workflow_execution),
            ("Message Bus", self.test_message_bus),
            ("Cross-System Routing", self.test_cross_system_routing),
        ]

        # Run tests
        print("Running tests...\n")
        for name, test_fn in tests:
            result = self.run_test(name, test_fn)
            status = "PASS" if result.passed else "FAIL"
            print(f"  [{status}] {result.name} ({result.duration:.2f}s)")
            print(f"        {result.details}")

        # Summary
        print()
        print("-" * 70)
        passed = sum(1 for r in self.results if r.passed)
        total = len(self.results)
        total_time = sum(r.duration for r in self.results)

        print(f"Results: {passed}/{total} passed")
        print(f"Total time: {total_time:.2f}s")
        print()

        if passed == total:
            print("ALL TESTS PASSED")
            return True
        else:
            print("SOME TESTS FAILED")
            for r in self.results:
                if not r.passed:
                    print(f"  - {r.name}: {r.details}")
            return False


def main():
    tester = IntegrationTester()
    success = tester.run_all()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
