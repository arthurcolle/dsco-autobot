"""T20/T21-partial tests: worker pool concurrency, no double-claiming,
pooled HTTP client usage, and circuit-breaker behavior.

The worker talks to backends over httpx; we monkeypatch AsyncClient.post so
the "backend" is in-process and slow — proving parallel workers interleave.
"""
import os
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
os.environ.setdefault("TOKEN_SECRET", "testsecret123")
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="dsco-queue-test-"))

import asyncio

import pytest

from tool_management.queue.task_queue import DurableTaskQueue, MCPBackend, TaskStatus

pytest_plugins = []


def make_queue(worker_count: int = 3) -> DurableTaskQueue:
    db = Path(tempfile.mkdtemp(prefix="dsco-pool-")) / "q.db"
    q = DurableTaskQueue(db_path=str(db), worker_count=worker_count)
    q.register_backend(MCPBackend(id="fake", name="Fake MCP", url="http://fake.invalid"))
    return q


@pytest.fixture(autouse=True)
def _healthy_fake_backend(monkeypatch):
    """The fake backend URL is unreachable by design; the real health loop
    would mark it unhealthy ~30s in and stall the queue. Pin health to True."""
    async def healthy(self, backend_id):
        b = self._backends.get(backend_id)
        if b:
            b.is_healthy = True
        return True
    monkeypatch.setattr(DurableTaskQueue, "check_backend_health", healthy)


class FakeSlowResponse:
    status_code = 200
    def json(self):
        return {"result": {"ok": True}}


@pytest.mark.asyncio
async def test_worker_pool_parallel_no_double_claim(monkeypatch):
    q = make_queue(worker_count=3)
    # each fake execution takes 0.2s; 6 tasks should finish in ~0.4-0.6s
    # with 3 parallel workers (vs ~1.2s sequential)
    inflight = set()
    double_claims = []

    async def fake_post(self, url, **kwargs):
        task_id = kwargs["json"]["id"]
        if task_id in inflight:
            double_claims.append(task_id)
        inflight.add(task_id)
        await asyncio.sleep(0.2)
        inflight.discard(task_id)
        return FakeSlowResponse()

    monkeypatch.setattr("httpx.AsyncClient.post", fake_post)

    ids = [q.enqueue(f"tool{i}", {"n": i}).id for i in range(6)]
    await q.start()
    try:
        deadline = time.time() + 8
        while time.time() < deadline:
            done = [q.get_task(i) for i in ids]
            if all(t and t.status == TaskStatus.COMPLETED for t in done):
                break
            await asyncio.sleep(0.1)
        states = [q.get_task(i).status for i in ids]
        assert all(s == TaskStatus.COMPLETED for s in states), states
        assert not double_claims, f"double-claimed: {double_claims}"
    finally:
        await q.stop()


@pytest.mark.asyncio
async def test_circuit_breaker_opens_and_recovers(monkeypatch):
    q = make_queue(worker_count=2)
    calls = {"n": 0}

    async def failing_post(self, url, **kwargs):
        calls["n"] += 1
        raise ConnectionError("backend down")

    monkeypatch.setattr("httpx.AsyncClient.post", failing_post)

    ids = [q.enqueue("boom", {"i": i}, max_retries=0).id for i in range(3)]
    await q.start()
    try:
        deadline = time.time() + 8
        while time.time() < deadline and calls["n"] < 3:
            await asyncio.sleep(0.1)
        assert calls["n"] >= 3
        assert q._breaker_is_open("fake"), "breaker should be OPEN after 3 consecutive failures"
        # a new task must NOT dispatch to the open backend
        t4 = q.enqueue("boom", {"after": 1})
        before = calls["n"]
        await asyncio.sleep(0.5)
        assert calls["n"] == before, "task dispatched to an open breaker"
        # force cooldown elapsed -> half-open probe allowed
        q._breaker_open_until["fake"] = 0.0
        assert q._breaker_is_open("fake") is False
    finally:
        await q.stop()


@pytest.mark.asyncio
async def test_pooled_client_reused(monkeypatch):
    q = make_queue(worker_count=1)
    created = []

    real_fn = DurableTaskQueue._shared_http_client

    async def counting_client(self):
        client = await real_fn(self)
        if not created:
            created.append(id(client))
        return client

    monkeypatch.setattr(DurableTaskQueue, "_shared_http_client", counting_client)

    async def ok_post(self, url, **kwargs):
        return FakeSlowResponse()

    monkeypatch.setattr("httpx.AsyncClient.post", ok_post)

    ids = [q.enqueue(f"t{i}", {}).id for i in range(3)]
    await q.start()
    try:
        deadline = time.time() + 6
        while time.time() < deadline and not all(
            q.get_task(i).status == TaskStatus.COMPLETED for i in ids
        ):
            await asyncio.sleep(0.1)
        assert all(q.get_task(i).status == TaskStatus.COMPLETED for i in ids)
        assert len(created) <= 1, f"expected a single pooled client, created {len(created)}"
    finally:
        await q.stop()
