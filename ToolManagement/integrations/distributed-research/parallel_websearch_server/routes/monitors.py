"""Monitor API routes (Alpha)."""

import logging
from typing import List

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from parallel_websearch_server.auth import get_auth_headers
from parallel_websearch_server.dependencies import get_http_client
from parallel_websearch_server.models import (
    CreateMonitorRequest,
    UpdateMonitorRequest,
    MonitorResponse,
    MonitorExecutionHistory,
    MonitorEventGroupResponse,
    MonitorEventsResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/v1alpha/monitors", tags=["Monitors"])


@router.post("", response_model=MonitorResponse, status_code=201)
async def create_monitor(
    request: CreateMonitorRequest,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> MonitorResponse:
    """Create a web monitor.

    Args:
        request: Monitor configuration

    Returns:
        Created monitor
    """
    try:
        response = await client.post(
            "/v1alpha/monitors",
            json=request.model_dump(exclude_none=True),
            headers=get_auth_headers(include_beta=False),
        )
        response.raise_for_status()
        return MonitorResponse(**response.json())
    except httpx.HTTPStatusError as e:
        logger.error(f"Monitor creation failed: {e.response.status_code}")
        raise HTTPException(
            status_code=e.response.status_code,
            detail=e.response.json() if e.response.text else {"error": "Monitor creation failed"},
        )


@router.get("", response_model=List[MonitorResponse])
async def list_monitors(
    client: httpx.AsyncClient = Depends(get_http_client),
) -> List[MonitorResponse]:
    """List all monitors.

    Returns:
        List of monitors
    """
    try:
        response = await client.get(
            "/v1alpha/monitors",
            headers=get_auth_headers(include_beta=False),
        )
        response.raise_for_status()
        data = response.json()
        return [MonitorResponse(**m) for m in data]
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.get("/{monitor_id}", response_model=MonitorResponse)
async def get_monitor(
    monitor_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> MonitorResponse:
    """Get monitor details.

    Args:
        monitor_id: Monitor ID

    Returns:
        Monitor details
    """
    try:
        response = await client.get(
            f"/v1alpha/monitors/{monitor_id}",
            headers=get_auth_headers(include_beta=False),
        )
        response.raise_for_status()
        return MonitorResponse(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.patch("/{monitor_id}", response_model=MonitorResponse)
async def update_monitor(
    monitor_id: str,
    request: UpdateMonitorRequest,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> MonitorResponse:
    """Update monitor configuration.

    Args:
        monitor_id: Monitor ID
        request: Updated configuration

    Returns:
        Updated monitor
    """
    try:
        response = await client.patch(
            f"/v1alpha/monitors/{monitor_id}",
            json=request.model_dump(exclude_none=True),
            headers=get_auth_headers(include_beta=False),
        )
        response.raise_for_status()
        return MonitorResponse(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.delete("/{monitor_id}", status_code=204)
async def delete_monitor(
    monitor_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
):
    """Delete a monitor.

    Args:
        monitor_id: Monitor ID
    """
    try:
        response = await client.delete(
            f"/v1alpha/monitors/{monitor_id}",
            headers=get_auth_headers(include_beta=False),
        )
        response.raise_for_status()
        return None
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.get("/{monitor_id}/executions", response_model=MonitorExecutionHistory, operation_id="get_monitor_executions")
async def get_monitor_executions(
    monitor_id: str,
    lookback_period: str = Query(..., description="Lookback period (e.g., '10d', '1w')"),
    client: httpx.AsyncClient = Depends(get_http_client),
) -> MonitorExecutionHistory:
    """Get monitor execution history.

    Args:
        monitor_id: Monitor ID
        lookback_period: Period to fetch (e.g., '10d', '1w')

    Returns:
        Execution history
    """
    try:
        response = await client.get(
            f"/v1alpha/monitors/{monitor_id}/executions",
            params={"lookback_period": lookback_period},
            headers=get_auth_headers(include_beta=False),
        )
        response.raise_for_status()
        return MonitorExecutionHistory(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.get(
    "/{monitor_id}/event_groups/{event_group_id}",
    response_model=MonitorEventGroupResponse,
    operation_id="get_monitor_event_group",
)
async def get_monitor_event_group(
    monitor_id: str,
    event_group_id: str,
    client: httpx.AsyncClient = Depends(get_http_client),
) -> MonitorEventGroupResponse:
    """Retrieve material change events for a given event group."""
    try:
        response = await client.get(
            f"/v1alpha/monitors/{monitor_id}/event_groups/{event_group_id}",
            headers=get_auth_headers(include_beta=False),
        )
        response.raise_for_status()
        return MonitorEventGroupResponse(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())


@router.get(
    "/{monitor_id}/events",
    response_model=MonitorEventsResponse,
    operation_id="list_monitor_events",
)
async def list_monitor_events(
    monitor_id: str,
    lookback_period: str = Query(
        "10d",
        description="Lookback window (e.g., '10d', '1w'). Minimum 1 day.",
    ),
    client: httpx.AsyncClient = Depends(get_http_client),
) -> MonitorEventsResponse:
    """List recent monitor events (events, completions, errors)."""
    try:
        params = {"lookback_period": lookback_period} if lookback_period else None
        response = await client.get(
            f"/v1alpha/monitors/{monitor_id}/events",
            params=params,
            headers=get_auth_headers(include_beta=False),
        )
        response.raise_for_status()
        return MonitorEventsResponse(**response.json())
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=e.response.json())
