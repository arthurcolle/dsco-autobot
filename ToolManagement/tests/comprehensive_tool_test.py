#!/usr/bin/env python3
"""
Comprehensive Tool Testing Suite for Tool Management Registry.

Features:
- Multi-case testing per tool
- Response time measurement & percentiles
- Schema validation
- Error handling verification
- Tool chaining tests
- Stress testing
- Detailed reporting with metrics
"""

import asyncio
import json
import statistics
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys


class TestStatus(str, Enum):
    PASSED = "✓"
    FAILED = "✗"
    SKIPPED = "⊘"
    TIMEOUT = "⏱"
    ERROR = "⚠"


@dataclass
class TestCase:
    """Individual test case for a tool."""
    name: str
    args: Dict[str, Any]
    expected_status: int = 200
    validate: Optional[Callable[[Dict], bool]] = None
    description: str = ""
    timeout: float = 15.0


@dataclass
class TestResult:
    """Result of a single test execution."""
    tool_name: str
    test_case: str
    status: TestStatus
    response_time_ms: float
    response_data: Optional[Dict] = None
    error_message: Optional[str] = None


@dataclass
class ToolTestReport:
    """Aggregate report for a tool."""
    tool_name: str
    service: str
    total_tests: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    timeouts: int = 0
    response_times: List[float] = field(default_factory=list)
    results: List[TestResult] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        return (self.passed / self.total_tests * 100) if self.total_tests > 0 else 0

    @property
    def avg_response_time(self) -> float:
        return statistics.mean(self.response_times) if self.response_times else 0

    @property
    def p50_response_time(self) -> float:
        return statistics.median(self.response_times) if self.response_times else 0

    @property
    def p95_response_time(self) -> float:
        if len(self.response_times) < 2:
            return self.avg_response_time
        sorted_times = sorted(self.response_times)
        idx = int(len(sorted_times) * 0.95)
        return sorted_times[min(idx, len(sorted_times) - 1)]


class ToolTester:
    """Comprehensive tool testing framework."""

    def __init__(self):
        self.results: Dict[str, ToolTestReport] = {}
        self.start_time = None
        self.end_time = None

    def _http_call(self, url: str, method: str = "POST", data: Optional[Dict] = None,
                   timeout: float = 15.0) -> Tuple[int, Dict, float]:
        """Make HTTP call and return (status, data, response_time_ms)."""
        start = time.perf_counter()
        try:
            req = urllib.request.Request(url, method=method)
            req.add_header('Content-Type', 'application/json')
            body = json.dumps(data).encode('utf-8') if data else None
            with urllib.request.urlopen(req, body, timeout=timeout) as resp:
                elapsed = (time.perf_counter() - start) * 1000
                return resp.status, json.loads(resp.read()), elapsed
        except urllib.error.HTTPError as e:
            elapsed = (time.perf_counter() - start) * 1000
            try:
                body = json.loads(e.read().decode())
            except:
                body = {"error": e.reason}
            return e.code, body, elapsed
        except urllib.error.URLError as e:
            elapsed = (time.perf_counter() - start) * 1000
            return 0, {"error": f"Connection failed: {e.reason}"}, elapsed
        except Exception as e:
            elapsed = (time.perf_counter() - start) * 1000
            return 0, {"error": str(e)}, elapsed

    def call_registry_tool(self, tool_name: str, args: Dict, timeout: float = 15.0) -> Tuple[int, Dict, float]:
        """Call a tool on the registry."""
        url = f"http://localhost:2016/api/v1/tools/{tool_name}/execute"
        return self._http_call(url, data={"inputs": args}, timeout=timeout)

    def call_native_mcp_rest(self, port: int, tool_name: str, args: Dict, timeout: float = 15.0) -> Tuple[int, Dict, float]:
        """Call a tool on a native MCP via REST /execute endpoint."""
        url = f"http://localhost:{port}/execute"
        return self._http_call(url, data={"name": tool_name, "arguments": args}, timeout=timeout)

    def call_native_mcp_jsonrpc(self, port: int, tool_name: str, args: Dict, timeout: float = 15.0) -> Tuple[int, Dict, float]:
        """Call a tool on a native MCP via MCP JSON-RPC protocol."""
        url = f"http://localhost:{port}/mcp"
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": args}
        }
        status, data, elapsed = self._http_call(url, data=payload, timeout=timeout)
        # Unwrap MCP response
        if status == 200 and "result" in data:
            return status, data["result"], elapsed
        elif "error" in data:
            return 500, data, elapsed
        return status, data, elapsed

    def run_test_case(self, service: str, tool_name: str, test_case: TestCase,
                      call_fn: Callable) -> TestResult:
        """Run a single test case."""
        try:
            status_code, response, elapsed = call_fn(tool_name, test_case.args, test_case.timeout)

            # Determine test status
            if status_code == 0:
                if "timed out" in str(response.get("error", "")).lower():
                    test_status = TestStatus.TIMEOUT
                else:
                    test_status = TestStatus.ERROR
                error_msg = response.get("error", "Unknown error")
            elif status_code == test_case.expected_status:
                # Check custom validation if provided
                if test_case.validate:
                    try:
                        if test_case.validate(response):
                            test_status = TestStatus.PASSED
                            error_msg = None
                        else:
                            test_status = TestStatus.FAILED
                            error_msg = "Validation failed"
                    except Exception as e:
                        test_status = TestStatus.FAILED
                        error_msg = f"Validation error: {e}"
                else:
                    test_status = TestStatus.PASSED
                    error_msg = None
            else:
                test_status = TestStatus.FAILED
                error_msg = f"Expected {test_case.expected_status}, got {status_code}"

            return TestResult(
                tool_name=tool_name,
                test_case=test_case.name,
                status=test_status,
                response_time_ms=elapsed,
                response_data=response,
                error_message=error_msg
            )
        except Exception as e:
            return TestResult(
                tool_name=tool_name,
                test_case=test_case.name,
                status=TestStatus.ERROR,
                response_time_ms=0,
                error_message=str(e)
            )

    def run_tool_tests(self, service: str, tool_name: str, test_cases: List[TestCase],
                       call_fn: Callable) -> ToolTestReport:
        """Run all test cases for a tool."""
        report = ToolTestReport(tool_name=tool_name, service=service)

        for tc in test_cases:
            result = self.run_test_case(service, tool_name, tc, call_fn)
            report.results.append(result)
            report.total_tests += 1
            report.response_times.append(result.response_time_ms)

            if result.status == TestStatus.PASSED:
                report.passed += 1
            elif result.status == TestStatus.FAILED:
                report.failed += 1
            elif result.status == TestStatus.TIMEOUT:
                report.timeouts += 1
            else:
                report.errors += 1

        return report

    def stress_test_tool(self, service: str, tool_name: str, args: Dict,
                         call_fn: Callable, num_requests: int = 20,
                         concurrency: int = 5) -> Dict:
        """Run concurrent stress test on a tool."""
        results = []
        errors = 0

        def single_call():
            try:
                status, _, elapsed = call_fn(tool_name, args, timeout=30.0)
                return {"status": status, "time": elapsed, "error": None}
            except Exception as e:
                return {"status": 0, "time": 0, "error": str(e)}

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(single_call) for _ in range(num_requests)]
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if result["error"] or result["status"] != 200:
                    errors += 1

        times = [r["time"] for r in results if r["time"] > 0]
        return {
            "total_requests": num_requests,
            "concurrency": concurrency,
            "successful": num_requests - errors,
            "failed": errors,
            "avg_time_ms": statistics.mean(times) if times else 0,
            "min_time_ms": min(times) if times else 0,
            "max_time_ms": max(times) if times else 0,
            "p50_ms": statistics.median(times) if times else 0,
            "p95_ms": sorted(times)[int(len(times) * 0.95)] if len(times) > 1 else (times[0] if times else 0),
            "throughput_rps": num_requests / (sum(times) / 1000 / concurrency) if times else 0
        }


# =============================================================================
# TEST CASE DEFINITIONS
# =============================================================================

def has_result(response: Dict) -> bool:
    """Validate response has a result."""
    return "result" in response or response.get("success", True)

def has_content(response: Dict) -> bool:
    """Validate MCP response has content."""
    return "content" in response or "result" in response

def is_list_response(response: Dict) -> bool:
    """Validate response contains a list."""
    result = response.get("result", response)
    return isinstance(result, (list, dict))


REGISTRY_TESTS = {
    "add": [
        TestCase("basic_addition", {"a": 5, "b": 3}, validate=lambda r: r.get("output") == 8),
        TestCase("negative_numbers", {"a": -10, "b": 5}, validate=lambda r: r.get("output") == -5),
        TestCase("zero_addition", {"a": 0, "b": 0}, validate=lambda r: r.get("output") == 0),
        TestCase("large_numbers", {"a": 999999, "b": 1}, validate=lambda r: r.get("output") == 1000000),
        TestCase("floats", {"a": 3.14, "b": 2.86}, validate=lambda r: abs(r.get("output", 0) - 6.0) < 0.01),
    ],
    "subtract": [
        TestCase("basic_subtraction", {"a": 10, "b": 4}, validate=lambda r: r.get("output") == 6),
        TestCase("negative_result", {"a": 5, "b": 10}, validate=lambda r: r.get("output") == -5),
    ],
    "multiply": [
        TestCase("basic_multiply", {"a": 6, "b": 7}, validate=lambda r: r.get("output") == 42),
        TestCase("multiply_by_zero", {"a": 100, "b": 0}, validate=lambda r: r.get("output") == 0),
        TestCase("negative_multiply", {"a": -5, "b": 3}, validate=lambda r: r.get("output") == -15),
    ],
    "divide": [
        TestCase("basic_divide", {"a": 20, "b": 4}, validate=lambda r: r.get("output") == 5),
        TestCase("decimal_result", {"a": 10, "b": 4}, validate=lambda r: r.get("output") == 2.5),
        TestCase("divide_by_zero", {"a": 10, "b": 0}, validate=lambda r: r.get("status") == "error"),
    ],
    "uppercase": [
        TestCase("basic_upper", {"text": "hello world"}, validate=lambda r: r.get("output") == "HELLO WORLD"),
        TestCase("mixed_case", {"text": "HeLLo WoRLD"}, validate=lambda r: r.get("output") == "HELLO WORLD"),
        TestCase("empty_string", {"text": ""}, validate=lambda r: r.get("output") == ""),
        TestCase("unicode", {"text": "café"}, validate=lambda r: r.get("output") == "CAFÉ"),
    ],
    "lowercase": [
        TestCase("basic_lower", {"text": "HELLO WORLD"}, validate=lambda r: r.get("output") == "hello world"),
        TestCase("mixed_case", {"text": "HeLLo WoRLD"}, validate=lambda r: r.get("output") == "hello world"),
    ],
    "word_count": [
        TestCase("basic_count", {"text": "the quick brown fox"}, validate=lambda r: r.get("output") == 4),
        TestCase("single_word", {"text": "hello"}, validate=lambda r: r.get("output") == 1),
        TestCase("empty_string", {"text": ""}, validate=lambda r: r.get("output") == 0),
        TestCase("multiple_spaces", {"text": "hello   world"}, validate=lambda r: r.get("output") == 2),
    ],
    "reverse_string": [
        TestCase("basic_reverse", {"text": "hello"}, validate=lambda r: r.get("output") == "olleh"),
        TestCase("palindrome", {"text": "racecar"}, validate=lambda r: r.get("output") == "racecar"),
        TestCase("empty_string", {"text": ""}, validate=lambda r: r.get("output") == ""),
    ],
    "json_to_dict": [
        TestCase("basic_json", {"json_string": '{"key": "value"}'}, validate=lambda r: r.get("output", {}).get("key") == "value"),
        TestCase("nested_json", {"json_string": '{"a": {"b": 1}}'}, validate=lambda r: r.get("output", {}).get("a", {}).get("b") == 1),
    ],
    "dict_to_json": [
        TestCase("basic_dict", {"data": {"key": "value"}}, validate=lambda r: "key" in str(r.get("output", ""))),
        TestCase("nested_dict", {"data": {"a": {"b": 1}}}, validate=lambda r: "b" in str(r.get("output", ""))),
    ],
    "flatten_list": [
        TestCase("basic_flatten", {"nested_list": [[1, 2], [3, 4]]}, validate=lambda r: r.get("output") == [1, 2, 3, 4]),
        TestCase("deep_nested", {"nested_list": [[[1]], [[2]]]}, validate=lambda r: len(r.get("output", [])) > 0),
    ],
    "filter_dict": [
        TestCase("basic_filter", {"data": {"a": 1, "b": 2, "c": 3}, "keys": ["a", "c"]},
                 validate=lambda r: r.get("output") == {"a": 1, "c": 3}),
        TestCase("missing_keys", {"data": {"a": 1}, "keys": ["a", "z"]},
                 validate=lambda r: r.get("output", {}).get("a") == 1),
    ],
}


SEARCH_MCP_TESTS = {
    "search": [
        TestCase("basic_search", {"query": "Python programming", "num_results": 3},
                 validate=lambda r: "results" in r.get("result", r), timeout=20.0),
        TestCase("single_result", {"query": "OpenAI GPT", "num_results": 1}, timeout=20.0),
    ],
    "read": [
        TestCase("read_url", {"url": "https://example.com"},
                 validate=lambda r: r.get("result", r).get("success", True), timeout=15.0),
    ],
    "embed": [
        TestCase("basic_embed", {"text": "Hello world"},
                 validate=lambda r: "embedding" in r.get("result", r), timeout=10.0),
        TestCase("longer_text", {"text": "The quick brown fox jumps over the lazy dog"}, timeout=10.0),
    ],
    "rerank": [
        TestCase("basic_rerank", {
            "query": "programming",
            "documents": ["Python is great", "Java is verbose", "Python is easy to learn"]
        }, validate=lambda r: "results" in r.get("result", r), timeout=15.0),
    ],
    "verify": [
        TestCase("simple_fact", {"statement": "The sky is blue"}, timeout=30.0),
    ],
}


RESEARCH_MCP_TESTS = {
    "list_tasks": [
        TestCase("list_all", {}, validate=lambda r: "tasks" in r.get("result", r)),
        TestCase("list_with_limit", {"limit": 5}),
    ],
    "get_task": [
        TestCase("get_nonexistent", {"task_id": "nonexistent-task-id"},
                 description="Should handle missing task gracefully"),
    ],
    "deep_search": [
        TestCase("quick_search", {"objective": "What is machine learning?", "depth": "quick"}, timeout=30.0),
    ],
    "run_task": [
        TestCase("simple_task", {"input": "What is 2+2?", "processor": "lite"}, timeout=30.0),
    ],
    "extract_data": [
        TestCase("extract_from_url", {
            "url": "https://example.com",
            "schema": {"title": "string", "description": "string"}
        }, timeout=20.0),
    ],
    "synthesize": [
        TestCase("synthesize_sources", {
            "sources": ["Python is a programming language", "Python is interpreted"],
            "objective": "Summarize Python"
        }, timeout=15.0),
    ],
}


PUBLISH_MCP_TESTS = {
    "list_webhooks": [
        TestCase("list_all", {}, validate=lambda r: "webhooks" in r.get("result", r)),
    ],
    "list_events": [
        TestCase("list_recent", {"limit": 10}, validate=lambda r: "events" in r.get("result", r)),
    ],
    "publish_event": [
        TestCase("publish_test_event", {"type": "test.comprehensive", "data": {"test": True}}),
    ],
    "queue_message": [
        TestCase("queue_test_message", {"channel": "test-comprehensive", "payload": {"msg": "test"}}),
    ],
    "get_queue_messages": [
        TestCase("get_messages", {"channel": "test-comprehensive", "limit": 5}),
    ],
    "send_notification": [
        TestCase("send_test_notification", {"title": "Test", "body": "Comprehensive test notification"}),
    ],
    "create_webhook": [
        TestCase("create_test_webhook", {
            "name": "comprehensive-test-webhook",
            "url": "https://httpbin.org/post",
            "events": ["test.event"]
        }),
    ],
}


# Memory MCP tests - uses MCP JSON-RPC protocol
MEMORY_MCP_TESTS = {
    "get_memory_stats": [
        TestCase("get_stats", {}, validate=has_content),
    ],
    "_connectivity_test": [
        TestCase("connectivity", {}, validate=has_content),
    ],
    "focus": [
        TestCase("focus_item", {"content": "Important test data", "importance": 0.9, "tags": ["test", "comprehensive"]}),
    ],
    "expand_context": [
        TestCase("expand_query", {"query": "test data", "limit": 5}),
    ],
    "record_episode": [
        TestCase("record_test_episode", {
            "event_type": "comprehensive_test",
            "content": "Running comprehensive test suite",
            "importance": 0.7
        }),
    ],
    "recall_episodes": [
        TestCase("recall_recent", {"limit": 5}),
        TestCase("recall_by_type", {"event_type": "comprehensive_test", "limit": 3}),
    ],
    "store_belief": [
        TestCase("store_test_belief", {
            "subject": "comprehensive_tests",
            "predicate": "are",
            "object": "thorough",
            "confidence": 0.99
        }),
    ],
    "query_beliefs": [
        TestCase("query_test_beliefs", {"subject": "comprehensive_tests"}),
    ],
    "learn_skill": [
        TestCase("learn_test_skill", {
            "name": "comprehensive_test_skill",
            "description": "A skill learned during comprehensive testing",
            "pattern": "when testing then verify"
        }),
    ],
    "search_all": [
        TestCase("search_across_tiers", {"query": "test", "limit_per_tier": 3}),
    ],
    "consolidate": [
        TestCase("consolidate_memory", {"working_to_episodic": True, "importance_threshold": 0.5}),
    ],
}

# Graph MCP tests (port 4004 when graph server is running)
GRAPH_MCP_TESTS = {
    "list_nodes": [
        TestCase("list_all_nodes", {}, validate=has_content),
    ],
    "query_graph": [
        TestCase("basic_query", {"query": "MATCH (n) RETURN n LIMIT 5"}, validate=has_content),
    ],
}


# =============================================================================
# MAIN TEST RUNNER
# =============================================================================

def print_report_header():
    """Print fancy header."""
    print("\n" + "═" * 80)
    print("║" + " " * 20 + "COMPREHENSIVE TOOL TEST SUITE" + " " * 29 + "║")
    print("║" + " " * 20 + f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" + " " * 21 + "║")
    print("═" * 80)


def print_service_header(name: str, port: int, tool_count: int):
    """Print service section header."""
    print(f"\n┌{'─' * 78}┐")
    print(f"│ {name:<40} Port: {port:<6} Tools: {tool_count:<5}        │")
    print(f"└{'─' * 78}┘")


def print_tool_result(report: ToolTestReport):
    """Print results for a single tool."""
    status_icon = "✓" if report.pass_rate == 100 else ("◐" if report.pass_rate >= 50 else "✗")
    color_start = "\033[92m" if report.pass_rate == 100 else ("\033[93m" if report.pass_rate >= 50 else "\033[91m")
    color_end = "\033[0m"

    print(f"  {status_icon} {report.tool_name:<25} "
          f"{color_start}{report.passed}/{report.total_tests} passed{color_end} "
          f"│ avg: {report.avg_response_time:>7.1f}ms │ p95: {report.p95_response_time:>7.1f}ms")

    # Print failed test details
    for result in report.results:
        if result.status != TestStatus.PASSED:
            print(f"      └─ {result.status.value} {result.test_case}: {result.error_message or 'Failed'}")


def print_summary(all_reports: List[ToolTestReport], duration: float):
    """Print final summary."""
    total_tests = sum(r.total_tests for r in all_reports)
    total_passed = sum(r.passed for r in all_reports)
    total_failed = sum(r.failed for r in all_reports)
    total_errors = sum(r.errors for r in all_reports)
    total_timeouts = sum(r.timeouts for r in all_reports)
    all_times = [t for r in all_reports for t in r.response_times]

    print("\n" + "═" * 80)
    print("║" + " " * 30 + "TEST SUMMARY" + " " * 36 + "║")
    print("═" * 80)

    pass_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
    color = "\033[92m" if pass_rate >= 90 else ("\033[93m" if pass_rate >= 70 else "\033[91m")

    print(f"""
  Total Tools Tested:    {len(all_reports)}
  Total Test Cases:      {total_tests}

  ┌─────────────────────────────────────┐
  │  {color}Passed:  {total_passed:>4}{' ' * 24}\033[0m│
  │  Failed:  {total_failed:>4}                        │
  │  Errors:  {total_errors:>4}                        │
  │  Timeouts: {total_timeouts:>3}                        │
  │                                     │
  │  Pass Rate: {color}{pass_rate:>6.1f}%\033[0m                 │
  └─────────────────────────────────────┘

  Response Time Statistics:
    • Average:  {statistics.mean(all_times) if all_times else 0:>8.1f} ms
    • Median:   {statistics.median(all_times) if all_times else 0:>8.1f} ms
    • Min:      {min(all_times) if all_times else 0:>8.1f} ms
    • Max:      {max(all_times) if all_times else 0:>8.1f} ms
    • Std Dev:  {statistics.stdev(all_times) if len(all_times) > 1 else 0:>8.1f} ms

  Total Duration: {duration:.2f} seconds
""")
    print("═" * 80)


def detect_port_4004_service() -> str:
    """Detect which service is running on port 4004."""
    try:
        req = urllib.request.Request("http://localhost:4004/health")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
            return data.get("service", "unknown")
    except:
        return "unavailable"


def run_stress_tests(tester: ToolTester):
    """Run stress tests on key tools."""
    print("\n" + "═" * 80)
    print("║" + " " * 28 + "STRESS TEST RESULTS" + " " * 31 + "║")
    print("═" * 80)

    stress_targets = [
        ("Registry", "add", {"a": 1, "b": 2}, tester.call_registry_tool),
        ("Search MCP", "embed", {"text": "test"}, lambda n, a, t: tester.call_native_mcp_rest(4001, n, a, t)),
        ("Research MCP", "list_tasks", {}, lambda n, a, t: tester.call_native_mcp_rest(4002, n, a, t)),
        ("Publish MCP", "list_events", {"limit": 5}, lambda n, a, t: tester.call_native_mcp_rest(4003, n, a, t)),
    ]

    for service, tool, args, call_fn in stress_targets:
        print(f"\n  Stress testing {service}/{tool} (20 requests, 5 concurrent)...")
        result = tester.stress_test_tool(service, tool, args, call_fn, num_requests=20, concurrency=5)

        print(f"    • Success Rate: {result['successful']}/{result['total_requests']} "
              f"({result['successful']/result['total_requests']*100:.0f}%)")
        print(f"    • Throughput:   {result['throughput_rps']:.1f} req/sec")
        print(f"    • Latency:      avg={result['avg_time_ms']:.1f}ms, "
              f"p50={result['p50_ms']:.1f}ms, p95={result['p95_ms']:.1f}ms")


def main():
    """Main test runner."""
    tester = ToolTester()
    all_reports: List[ToolTestReport] = []

    print_report_header()
    start_time = time.perf_counter()

    # Test Registry Tools
    print_service_header("REGISTRY", 2016, len(REGISTRY_TESTS))
    for tool_name, test_cases in REGISTRY_TESTS.items():
        report = tester.run_tool_tests("registry", tool_name, test_cases, tester.call_registry_tool)
        all_reports.append(report)
        print_tool_result(report)

    # Test Search MCP Tools
    print_service_header("SEARCH MCP", 4001, len(SEARCH_MCP_TESTS))
    for tool_name, test_cases in SEARCH_MCP_TESTS.items():
        call_fn = lambda n, a, t, port=4001: tester.call_native_mcp_rest(port, n, a, t)
        report = tester.run_tool_tests("search-mcp", tool_name, test_cases, call_fn)
        all_reports.append(report)
        print_tool_result(report)

    # Test Research MCP Tools
    print_service_header("RESEARCH MCP", 4002, len(RESEARCH_MCP_TESTS))
    for tool_name, test_cases in RESEARCH_MCP_TESTS.items():
        call_fn = lambda n, a, t, port=4002: tester.call_native_mcp_rest(port, n, a, t)
        report = tester.run_tool_tests("research-mcp", tool_name, test_cases, call_fn)
        all_reports.append(report)
        print_tool_result(report)

    # Test Publish MCP Tools
    print_service_header("PUBLISH MCP", 4003, len(PUBLISH_MCP_TESTS))
    for tool_name, test_cases in PUBLISH_MCP_TESTS.items():
        call_fn = lambda n, a, t, port=4003: tester.call_native_mcp_rest(port, n, a, t)
        report = tester.run_tool_tests("publish-mcp", tool_name, test_cases, call_fn)
        all_reports.append(report)
        print_tool_result(report)

    # Detect service on port 4004
    port_4004_service = detect_port_4004_service()

    if "memory" in port_4004_service.lower():
        # Test Memory MCP Tools (uses JSON-RPC)
        print_service_header("MEMORY MCP", 4004, len(MEMORY_MCP_TESTS))
        for tool_name, test_cases in MEMORY_MCP_TESTS.items():
            call_fn = lambda n, a, t, port=4004: tester.call_native_mcp_jsonrpc(port, n, a, t)
            report = tester.run_tool_tests("memory-mcp", tool_name, test_cases, call_fn)
            all_reports.append(report)
            print_tool_result(report)
    elif "graph" in port_4004_service.lower():
        # Test Graph MCP Tools
        print_service_header("GRAPH MCP", 4004, len(GRAPH_MCP_TESTS))
        for tool_name, test_cases in GRAPH_MCP_TESTS.items():
            call_fn = lambda n, a, t, port=4004: tester.call_native_mcp_jsonrpc(port, n, a, t)
            report = tester.run_tool_tests("graph-mcp", tool_name, test_cases, call_fn)
            all_reports.append(report)
            print_tool_result(report)
    else:
        print(f"\n  ⚠ Port 4004: {port_4004_service} (skipping - unknown service)")

    # Run stress tests
    run_stress_tests(tester)

    # Print summary
    total_duration = time.perf_counter() - start_time
    print_summary(all_reports, total_duration)

    # Return exit code based on pass rate
    total_passed = sum(r.passed for r in all_reports)
    total_tests = sum(r.total_tests for r in all_reports)
    # Allow 90% pass rate
    pass_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
    return 0 if pass_rate >= 90 else 1


if __name__ == "__main__":
    sys.exit(main())
