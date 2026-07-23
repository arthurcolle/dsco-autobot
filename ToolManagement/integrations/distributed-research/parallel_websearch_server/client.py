"""Parallel.ai API client wrapper."""

import json
import logging
from typing import Any, AsyncGenerator, Dict, List, Optional

import httpx

from parallel_websearch_server.auth import api_key_cache
from parallel_websearch_server.config import settings
from parallel_websearch_server.models import (
    BetaTaskRunInput,
    TaskRun,
    TaskRunResult,
    BetaTaskRunResult,
    SearchRequest,
    SearchResponse,
    ExtractRequest,
    ExtractResponse,
    CreateTaskGroupRequest,
    TaskGroupResponse,
    TaskGroupRunRequest,
    TaskGroupRunResponse,
    IngestInput,
    FindAllRunInput,
    FindAllRun,
    FindAllRunResult,
    FindAllExtendInput,
    FindAllEnrichInput,
    CreateMonitorRequest,
    UpdateMonitorRequest,
    MonitorResponse,
    MonitorExecutionHistory,
    MonitorEventGroupResponse,
    MonitorEventsResponse,
    ChatCompletionRequest,
    ChatCompletionResponse,
)

logger = logging.getLogger(__name__)


class ParallelAPIClient:
    """Typed wrapper around the Parallel.ai REST surface."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: Optional[int] = None,
    ):
        self.api_key = api_key
        self.base_url = (base_url or settings.api_base_url).rstrip("/")
        self.timeout = timeout or settings.request_timeout
        self.beta_header_value = settings.parallel_beta_header
        self._client: Optional[httpx.AsyncClient] = None

    async def connect(self) -> None:
        """Initialize the HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                headers={"User-Agent": "parallel-websearch-server/0.1.0"},
            )
            logger.info("Parallel API client connected to %s", self.base_url)

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
            logger.info("Parallel API client closed")

    async def __aenter__(self) -> "ParallelAPIClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:  # pragma: no cover - context manager
        await self.close()

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("ParallelAPIClient not connected. Call connect() first.")
        return self._client

    def _auth_headers(self, *, beta: bool = False, beta_type: str = "search_extract") -> Dict[str, str]:
        key = self.api_key or api_key_cache.get() or settings.api_key
        if not key:
            raise RuntimeError("Parallel API key is not configured.")

        headers = {
            "x-api-key": key,
            "Content-Type": "application/json",
        }
        if beta:
            if beta_type == "findall":
                headers["parallel-beta"] = settings.parallel_beta_findall
            else:
                headers["parallel-beta"] = settings.parallel_beta_search_extract
        return headers

    def _bearer_headers(self) -> Dict[str, str]:
        """Get auth headers using Bearer token format (for Chat API)."""
        key = self.api_key or api_key_cache.get() or settings.api_key
        if not key:
            raise RuntimeError("Parallel API key is not configured.")
        return {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, str]] = None,
        beta: bool = False,
        beta_type: str = "search_extract",
    ) -> httpx.Response:
        client = self._get_client()
        response = await client.request(
            method,
            path,
            json=json,
            params=params,
            headers=self._auth_headers(beta=beta, beta_type=beta_type),
        )
        response.raise_for_status()
        return response

    async def _stream(
        self,
        path: str,
        *,
        params: Optional[Dict[str, str]] = None,
        beta: bool = False,
        beta_type: str = "search_extract",
    ) -> AsyncGenerator[str, None]:
        client = self._get_client()
        try:
            async with client.stream(
                "GET",
                path,
                params=params,
                headers=self._auth_headers(beta=beta, beta_type=beta_type),
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.strip():
                        yield line + "\n"
        except httpx.HTTPStatusError as exc:
            error_data = {"error": f"Stream failed: {exc.response.status_code}"}
            yield f"event: error\ndata: {json.dumps(error_data)}\n\n"

    async def search(self, request: SearchRequest) -> SearchResponse:
        """Run a search request."""
        response = await self._request(
            "POST",
            "/v1beta/search",
            json=request.model_dump(exclude_none=True),
            beta=True,
        )
        return SearchResponse(**response.json())

    async def extract(self, request: ExtractRequest) -> ExtractResponse:
        """Run an extract request."""
        response = await self._request(
            "POST",
            "/v1beta/extract",
            json=request.model_dump(exclude_none=True),
            beta=True,
        )
        return ExtractResponse(**response.json())

    async def chat_completion(self, request: ChatCompletionRequest) -> ChatCompletionResponse:
        """Run a chat completion request (OpenAI-compatible)."""
        client = self._get_client()
        response = await client.post(
            "/chat/completions",
            json=request.model_dump(exclude_none=True),
            headers=self._bearer_headers(),
        )
        response.raise_for_status()
        return ChatCompletionResponse(**response.json())

    async def stream_chat_completion(self, request: ChatCompletionRequest) -> AsyncGenerator[str, None]:
        """Stream chat completion response."""
        client = self._get_client()
        stream_request = request.model_copy(update={"stream": True})
        try:
            async with client.stream(
                "POST",
                "/chat/completions",
                json=stream_request.model_dump(exclude_none=True),
                headers=self._bearer_headers(),
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.strip():
                        yield line + "\n"
        except httpx.HTTPStatusError as exc:
            error_data = {"error": f"Stream failed: {exc.response.status_code}"}
            yield f"data: {json.dumps(error_data)}\n\n"

    async def create_task_run(self, request: BetaTaskRunInput) -> TaskRun:
        """Create a task run."""
        response = await self._request(
            "POST",
            "/v1/tasks/runs",
            json=request.model_dump(exclude_none=True),
            beta=True,
        )
        return TaskRun(**response.json())

    async def get_task_run(self, run_id: str) -> TaskRun:
        """Retrieve task run metadata."""
        response = await self._request("GET", f"/v1/tasks/runs/{run_id}", beta=True)
        return TaskRun(**response.json())

    async def get_task_run_input(self, run_id: str) -> Dict[str, Any]:
        """Retrieve task run input payload."""
        response = await self._request("GET", f"/v1/tasks/runs/{run_id}/input", beta=True)
        return response.json()

    async def get_task_run_result(self, run_id: str) -> TaskRunResult:
        """Retrieve the final task run result."""
        response = await self._request("GET", f"/v1/tasks/runs/{run_id}/result", beta=True)
        data = response.json()
        if data.get("output", {}).get("mcp_tool_calls") or data.get("output", {}).get("beta_fields"):
            return BetaTaskRunResult(**data)
        return TaskRunResult(**data)

    async def stream_task_run_events(self, run_id: str) -> AsyncGenerator[str, None]:
        """Stream SSE events for a single task run."""
        async for line in self._stream(f"/v1beta/tasks/runs/{run_id}/events", beta=True):
            yield line

    async def create_task_group(self, request: CreateTaskGroupRequest) -> TaskGroupResponse:
        response = await self._request(
            "POST",
            "/v1beta/tasks/groups",
            json=request.model_dump(exclude_none=True),
            beta=True,
        )
        return TaskGroupResponse(**response.json())

    async def get_task_group(self, taskgroup_id: str) -> TaskGroupResponse:
        response = await self._request("GET", f"/v1beta/tasks/groups/{taskgroup_id}", beta=True)
        return TaskGroupResponse(**response.json())

    async def add_runs_to_group(
        self,
        taskgroup_id: str,
        request: TaskGroupRunRequest,
    ) -> TaskGroupRunResponse:
        response = await self._request(
            "POST",
            f"/v1beta/tasks/groups/{taskgroup_id}/runs",
            json=request.model_dump(exclude_none=True),
            beta=True,
        )
        return TaskGroupRunResponse(**response.json())

    async def get_task_group_run(self, taskgroup_id: str, run_id: str) -> TaskRun:
        response = await self._request("GET", f"/v1beta/tasks/groups/{taskgroup_id}/runs/{run_id}", beta=True)
        return TaskRun(**response.json())

    async def stream_task_group_runs(
        self,
        taskgroup_id: str,
        include_input: bool = False,
        include_output: bool = False,
        last_event_id: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        params: Dict[str, str] = {}
        if include_input:
            params["include_input"] = "true"
        if include_output:
            params["include_output"] = "true"
        if last_event_id:
            params["last_event_id"] = last_event_id
        async for line in self._stream(
            f"/v1beta/tasks/groups/{taskgroup_id}/runs",
            params=params or None,
            beta=True,
        ):
            yield line

    async def stream_task_group_events(self, taskgroup_id: str) -> AsyncGenerator[str, None]:
        async for line in self._stream(f"/v1beta/tasks/groups/{taskgroup_id}/events", beta=True):
            yield line

    async def ingest_findall(self, request: IngestInput) -> Dict[str, Any]:
        response = await self._request(
            "POST",
            "/v1beta/findall/ingest",
            json=request.model_dump(exclude_none=True),
            beta=True,
            beta_type="findall",
        )
        return response.json()

    async def create_findall_run(self, request: FindAllRunInput) -> FindAllRun:
        response = await self._request(
            "POST",
            "/v1beta/findall/runs",
            json=request.model_dump(exclude_none=True),
            beta=True,
            beta_type="findall",
        )
        return FindAllRun(**response.json())

    async def get_findall_run(self, findall_id: str) -> FindAllRun:
        response = await self._request("GET", f"/v1beta/findall/runs/{findall_id}", beta=True, beta_type="findall")
        return FindAllRun(**response.json())

    async def get_findall_result(self, findall_id: str) -> FindAllRunResult:
        response = await self._request("GET", f"/v1beta/findall/runs/{findall_id}/result", beta=True, beta_type="findall")
        return FindAllRunResult(**response.json())

    async def get_findall_schema(self, findall_id: str) -> Dict[str, Any]:
        """Retrieve the inferred schema for a FindAll run."""
        response = await self._request("GET", f"/v1beta/findall/runs/{findall_id}/schema", beta=True, beta_type="findall")
        return response.json()

    async def extend_findall_run(self, findall_id: str, request: FindAllExtendInput) -> FindAllRun:
        response = await self._request(
            "POST",
            f"/v1beta/findall/runs/{findall_id}/extend",
            json=request.model_dump(exclude_none=True),
            beta=True,
            beta_type="findall",
        )
        return FindAllRun(**response.json())

    async def enrich_findall_run(self, findall_id: str, request: FindAllEnrichInput) -> FindAllRun:
        response = await self._request(
            "POST",
            f"/v1beta/findall/runs/{findall_id}/enrich",
            json=request.model_dump(exclude_none=True),
            beta=True,
            beta_type="findall",
        )
        return FindAllRun(**response.json())

    async def cancel_findall_run(self, findall_id: str) -> None:
        await self._request("POST", f"/v1beta/findall/runs/{findall_id}/cancel", beta=True, beta_type="findall")

    async def stream_findall_events(
        self,
        findall_id: str,
        *,
        last_event_id: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> AsyncGenerator[str, None]:
        params: Dict[str, str] = {}
        if last_event_id:
            params["last_event_id"] = last_event_id
        if timeout:
            params["timeout"] = str(timeout)
        async for line in self._stream(
            f"/v1beta/findall/runs/{findall_id}/events",
            params=params or None,
            beta=True,
            beta_type="findall",
        ):
            yield line

    async def create_monitor(self, request: CreateMonitorRequest) -> MonitorResponse:
        response = await self._request(
            "POST",
            "/v1alpha/monitors",
            json=request.model_dump(exclude_none=True),
        )
        return MonitorResponse(**response.json())

    async def list_monitors(self) -> List[MonitorResponse]:
        response = await self._request("GET", "/v1alpha/monitors")
        return [MonitorResponse(**item) for item in response.json()]

    async def get_monitor(self, monitor_id: str) -> MonitorResponse:
        response = await self._request("GET", f"/v1alpha/monitors/{monitor_id}")
        return MonitorResponse(**response.json())

    async def update_monitor(self, monitor_id: str, request: UpdateMonitorRequest) -> MonitorResponse:
        response = await self._request(
            "PATCH",
            f"/v1alpha/monitors/{monitor_id}",
            json=request.model_dump(exclude_none=True),
        )
        return MonitorResponse(**response.json())

    async def delete_monitor(self, monitor_id: str) -> None:
        await self._request("DELETE", f"/v1alpha/monitors/{monitor_id}")

    async def get_monitor_executions(self, monitor_id: str, lookback_period: str) -> MonitorExecutionHistory:
        response = await self._request(
            "GET",
            f"/v1alpha/monitors/{monitor_id}/executions",
            params={"lookback_period": lookback_period},
        )
        return MonitorExecutionHistory(**response.json())

    async def get_monitor_event_group(
        self,
        monitor_id: str,
        event_group_id: str,
    ) -> MonitorEventGroupResponse:
        response = await self._request(
            "GET",
            f"/v1alpha/monitors/{monitor_id}/event_groups/{event_group_id}",
        )
        return MonitorEventGroupResponse(**response.json())

    async def list_monitor_events(
        self,
        monitor_id: str,
        lookback_period: Optional[str] = None,
    ) -> MonitorEventsResponse:
        params = {"lookback_period": lookback_period} if lookback_period else None
        response = await self._request(
            "GET",
            f"/v1alpha/monitors/{monitor_id}/events",
            params=params,
        )
        return MonitorEventsResponse(**response.json())


# Backwards-compatible alias used by other modules.
ParallelClient = ParallelAPIClient
