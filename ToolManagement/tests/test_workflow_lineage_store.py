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


def test_workflow_definition_persistence(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "workflow_defs.db"))
    definition = {
        "id": "wf_durable",
        "name": "durable_workflow",
        "description": "Persisted workflow definition",
        "steps": [],
        "version": "1.0.0",
        "tags": ["durable", "test"],
    }

    store.upsert_workflow_definition(
        workflow_id="wf_durable",
        workflow_name="durable_workflow",
        definition=definition,
        tags=["durable", "test"],
    )

    stored = store.get_workflow_definition("wf_durable")
    assert stored is not None
    assert stored["workflow_name"] == "durable_workflow"
    assert stored["definition"]["id"] == "wf_durable"
    assert "durable" in stored["tags"]

    listed = store.list_workflow_definitions(limit=10)
    assert any(item["workflow_id"] == "wf_durable" for item in listed)


def test_trace_and_artifact_persistence(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "trace_artifacts.db"))
    execution_id = "exec_trace_1"

    store.upsert_workflow_execution(
        execution_id=execution_id,
        workflow_id="wf_trace",
        workflow_name="traceable_workflow",
        workflow_kind="composition",
        workflow_type="sequential",
        trace_id="tr_abc123",
        session_id="sess_001",
        parent_execution_id="parent_001",
        status="running",
        inputs={"x": 1},
        metadata={},
    )

    store.upsert_workflow_step(
        execution_id=execution_id,
        step_key="step_a",
        step_name="step_a",
        step_index=0,
        tool_name="search",
        mode="tool",
        trace_id="tr_abc123",
        session_id="sess_001",
        status="completed",
        input_data={"query": "dc weather"},
        output_data={"top": "forecast"},
        duration_ms=100,
        metadata={},
    )

    artifact = store.append_workflow_artifact(
        execution_id=execution_id,
        step_key="step_a",
        trace_id="tr_abc123",
        session_id="sess_001",
        artifact_type="output",
        payload={"top": "forecast"},
        source="test",
        metadata={"kind": "unit"},
    )
    assert artifact is not None
    assert artifact["content_hash"]

    run = store.get_workflow_execution(
        execution_id,
        include_steps=True,
        include_events=False,
        include_artifacts=True,
    )
    assert run is not None
    assert run["trace_id"] == "tr_abc123"
    assert run["session_id"] == "sess_001"
    assert run["parent_execution_id"] == "parent_001"
    assert len(run.get("artifacts", [])) == 1
    assert run["artifacts"][0]["payload"]["top"] == "forecast"


def test_workflow_idempotency_and_chain_analytics(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "chain_analytics.db"))
    now = time.time()

    store.upsert_workflow_execution(
        execution_id="exec_chain_1",
        workflow_id="wf_chain",
        workflow_name="chainable",
        workflow_kind="composition",
        workflow_type="pipe",
        idempotency_key="idem-1",
        request_hash="hash-1",
        status="completed",
        started_at=now - 5,
        completed_at=now - 1,
        duration_ms=4000,
        metadata={},
    )
    store.upsert_workflow_step(
        execution_id="exec_chain_1",
        step_key="s1",
        step_name="search",
        step_index=0,
        tool_name="search",
        mode="tool",
        status="completed",
        metadata={},
    )
    store.upsert_workflow_step(
        execution_id="exec_chain_1",
        step_key="s2",
        step_name="read",
        step_index=1,
        tool_name="read",
        mode="tool",
        status="completed",
        metadata={},
    )

    idem = store.find_recent_workflow_by_idempotency(
        workflow_kind="composition",
        workflow_id="wf_chain",
        idempotency_key="idem-1",
        since_ts=now - 60,
    )
    assert idem is not None
    assert idem["execution_id"] == "exec_chain_1"
    assert idem["request_hash"] == "hash-1"

    chains = store.get_workflow_chain_analytics(hours=24, limit=10, min_chain_length=2)
    assert len(chains["chains"]) == 1
    assert chains["chains"][0]["chain"] == "search -> read"
    assert chains["chains"][0]["runs"] == 1


def test_side_effect_journal_lineage_and_storage_stats(tmp_path):
    store = ExecutionStore(db_path=str(tmp_path / "side_effects.db"))
    now = time.time()

    store.upsert_workflow_execution(
        execution_id="exec_root",
        workflow_id="wf_sidefx",
        workflow_name="sidefx_workflow",
        workflow_kind="composition",
        workflow_type="sequential",
        trace_id="tr_sidefx",
        session_id="sess_sidefx",
        status="completed",
        started_at=now - 20,
        completed_at=now - 19,
        duration_ms=1000,
        metadata={},
    )
    store.upsert_workflow_execution(
        execution_id="exec_replay",
        workflow_id="wf_sidefx",
        workflow_name="sidefx_workflow",
        workflow_kind="composition",
        workflow_type="sequential",
        trace_id="tr_sidefx",
        session_id="sess_sidefx",
        parent_execution_id="exec_root",
        replay_of_execution_id="exec_root",
        status="completed",
        started_at=now - 10,
        completed_at=now - 9,
        duration_ms=1000,
        metadata={},
    )

    store.upsert_workflow_step(
        execution_id="exec_replay",
        step_key="store",
        step_name="store",
        step_index=1,
        tool_name="store_belief",
        mode="tool",
        is_side_effect=True,
        side_effect_key="fx_1",
        status="completed",
        input_data={"belief": "storm incoming"},
        output_data={"stored": True},
        duration_ms=120,
        metadata={},
    )

    record = store.append_workflow_side_effect(
        execution_id="exec_replay",
        step_key="store",
        tool_name="store_belief",
        side_effect_key="fx_1",
        status="completed",
        request_data={"belief": "storm incoming"},
        response_data={"stored": True},
        trace_id="tr_sidefx",
        session_id="sess_sidefx",
        idempotency_key="idem-sidefx-1",
        request_hash="req_hash_1",
        replay_policy="allow",
        replay_of_execution_id="exec_root",
    )
    assert record["status"] == "completed"

    execution = store.get_workflow_execution(
        "exec_replay",
        include_steps=True,
        include_side_effects=True,
    )
    assert execution is not None
    assert execution["steps"][0]["is_side_effect"] is True
    assert len(execution["side_effects"]) == 1
    assert execution["side_effects"][0]["request"]["belief"] == "storm incoming"

    lineage = store.get_workflow_lineage("exec_replay", depth=3)
    assert lineage["root_execution_id"] == "exec_replay"
    assert len(lineage["nodes"]) >= 2
    assert any(edge["type"] == "replay_of" for edge in lineage["edges"])

    trace_lineage = store.get_trace_workflow_lineage("tr_sidefx", limit=50)
    assert trace_lineage["count"] == 2
    assert len(trace_lineage["edges"]) >= 1

    stats = store.get_storage_stats()
    assert stats["table_counts"]["workflow_side_effects"] == 1
    assert stats["db_size_bytes"] > 0

    with store._conn() as conn:
        conn.execute(
            "UPDATE workflow_side_effects SET created_at = ? WHERE execution_id = ?",
            (now - 10_000, "exec_replay"),
        )
    removed = store.cleanup_old_workflow_side_effects(max_age_seconds=120)
    assert removed == 1
