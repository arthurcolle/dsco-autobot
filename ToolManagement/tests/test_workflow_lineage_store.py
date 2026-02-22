import time

from tool_management.persistence.execution_store import ExecutionStore


def test_workflow_lineage_persistence(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "lineage.db"))
    execution_id = "wf_exec_1"
    started_at = time.time()

    store.upsert_workflow_execution(
        execution_id=execution_id,
        workflow_id="wf_abc",
        workflow_name="research_and_store",
        workflow_kind="composition",
        workflow_type="pipe",
        status="running",
        inputs={"query": "dc weather"},
        started_at=started_at,
        metadata={"step_count": 2},
    )

    store.upsert_workflow_step(
        execution_id=execution_id,
        step_key="search",
        step_name="search",
        step_index=0,
        tool_name="search",
        mode="tool",
        status="running",
        input_data={"query": "dc weather"},
        started_at=started_at,
    )
    store.append_workflow_event(
        execution_id=execution_id,
        step_key="search",
        event_type="step_started",
        message="search started",
        data={"step_index": 0},
    )

    store.upsert_workflow_step(
        execution_id=execution_id,
        step_key="search",
        step_name="search",
        step_index=0,
        tool_name="search",
        mode="tool",
        status="completed",
        output_data={"results": [{"title": "Forecast"}]},
        completed_at=started_at + 1,
        duration_ms=1000,
    )
    store.append_workflow_event(
        execution_id=execution_id,
        step_key="search",
        event_type="step_completed",
        message="search complete",
        data={"duration_ms": 1000},
    )

    store.upsert_workflow_execution(
        execution_id=execution_id,
        workflow_id="wf_abc",
        workflow_name="research_and_store",
        workflow_kind="composition",
        workflow_type="pipe",
        status="completed",
        output={"ok": True},
        started_at=started_at,
        completed_at=started_at + 1.2,
        duration_ms=1200,
        metadata={"stages_executed": ["search"]},
    )

    run = store.get_workflow_execution(execution_id, include_steps=True, include_events=True, event_limit=20)
    assert run is not None
    assert run["status"] == "completed"
    assert run["workflow_name"] == "research_and_store"
    assert run["inputs"]["query"] == "dc weather"
    assert run["output"]["ok"] is True

    assert len(run["steps"]) == 1
    step = run["steps"][0]
    assert step["step_key"] == "search"
    assert step["status"] == "completed"
    assert step["output"]["results"][0]["title"] == "Forecast"

    event_types = [e["event_type"] for e in run["events"]]
    assert "step_started" in event_types
    assert "step_completed" in event_types


def test_workflow_analytics_and_filters(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "lineage_analytics.db"))
    now = time.time()

    store.upsert_workflow_execution(
        execution_id="exec_completed",
        workflow_id="wf_1",
        workflow_name="wf_one",
        workflow_kind="simple_composition",
        workflow_type="parallel",
        status="completed",
        inputs={"a": 1},
        output={"ok": True},
        started_at=now - 10,
        completed_at=now - 5,
        duration_ms=5000,
        metadata={},
    )
    store.upsert_workflow_execution(
        execution_id="exec_failed",
        workflow_id="wf_2",
        workflow_name="wf_two",
        workflow_kind="named_workflow",
        workflow_type="research",
        status="failed",
        inputs={"a": 2},
        error="timeout",
        started_at=now - 20,
        completed_at=now - 18,
        duration_ms=2000,
        metadata={},
    )

    completed = store.list_workflow_executions(status="completed", limit=10)
    assert len(completed) == 1
    assert completed[0]["execution_id"] == "exec_completed"

    named = store.list_workflow_executions(workflow_kind="named_workflow", limit=10)
    assert len(named) == 1
    assert named[0]["execution_id"] == "exec_failed"

    analytics = store.get_workflow_analytics(hours=24)
    assert analytics["totals"]["executions"] == 2
    assert analytics["totals"]["completed"] == 1
    assert analytics["totals"]["failed"] == 1
    assert analytics["kind_mix"]["simple_composition"] == 1
    assert analytics["kind_mix"]["named_workflow"] == 1
