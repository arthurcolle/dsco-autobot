from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from tool_management.api import butler_routes


class _FakeResolver:
    def __init__(self, candidates):
        self.candidates = candidates

    async def resolve(self, desire, intent, context, limit):
        return self.candidates.get(desire.id, [])[:limit]


def _candidate(name, *, backend="test", confidence=0.9, arguments=None):
    return butler_routes.ToolCandidate(
        tool_name=name,
        backend=backend,
        confidence=confidence,
        reasoning=f"fixture candidate for {name}",
        inputs_template=arguments or {},
    )


def _request(**overrides):
    payload = {
        "name": "Household desk",
        "objective": "Keep the week coordinated without losing user control",
        "initiative": "attentive",
        "follow_through": "steady",
        "constraints": {"max_parallelism": 2},
        "desires": [
            {
                "id": "calendar_scan",
                "area": "work",
                "outcome": "Review the week's calendar conflicts",
                "priority": 1,
                "authority": "see",
            },
            {
                "id": "health_plan",
                "area": "health",
                "outcome": "Prepare a training plan",
                "priority": 2,
                "authority": "prepare",
            },
            {
                "id": "trip_message",
                "area": "travel",
                "outcome": "Send the revised itinerary",
                "priority": 1,
                "authority": "ask",
                "depends_on": ["calendar_scan"],
            },
        ],
    }
    payload.update(overrides)
    return butler_routes.ButlerBlueprintRequest.model_validate(payload)


@pytest.mark.asyncio
async def test_planner_builds_stable_components_stages_and_gates():
    planner = butler_routes.ButlerBlueprintPlanner(
        _FakeResolver(
            {
                "calendar_scan": [_candidate("get_events", confidence=0.92)],
                "health_plan": [_candidate("prepare_plan", confidence=0.88)],
                "trip_message": [_candidate("send_itinerary", confidence=0.94)],
            }
        )
    )
    request = _request()

    first = await planner.plan(request)
    second = await planner.plan(request)

    assert first.blueprint_id == second.blueprint_id
    assert [component.id for component in first.components] == [
        "work-desk",
        "travel-desk",
        "health-desk",
    ]
    assert first.stages[0].execution == "parallel"
    assert set(first.stages[0].step_ids) == {"calendar_scan", "health_plan"}
    assert first.stages[1].step_ids == ["trip_message"]
    assert first.steps[-1].approval_gate is not None
    assert first.steps[-1].approval_gate.default_action == "abort"
    assert first.unresolved == []
    assert first.execution.mode == "plan_only"
    assert first.execution.direct_execution_supported is False


@pytest.mark.asyncio
async def test_planner_selects_lower_risk_feasible_candidate():
    planner = butler_routes.ButlerBlueprintPlanner(
        _FakeResolver(
            {
                "reply": [
                    _candidate("send_message", confidence=0.98),
                    _candidate("draft_message", confidence=0.81),
                ]
            }
        )
    )
    request = butler_routes.ButlerBlueprintRequest.model_validate(
        {
            "objective": "Prepare a reply without sending it",
            "desires": [
                {
                    "id": "reply",
                    "area": "people",
                    "outcome": "Prepare a thoughtful reply",
                    "authority": "prepare",
                }
            ],
        }
    )

    blueprint = await planner.plan(request)

    assert [step.tool for step in blueprint.steps] == ["test:draft_message"]
    assert blueprint.steps[0].effect == butler_routes.EffectClass.PREPARE
    assert blueprint.steps[0].approval_gate is None


@pytest.mark.asyncio
async def test_planner_gates_spend_without_a_known_in_limit_amount():
    planner = butler_routes.ButlerBlueprintPlanner(
        _FakeResolver({"hotel": [_candidate("book_hotel")]})
    )
    request = butler_routes.ButlerBlueprintRequest.model_validate(
        {
            "objective": "Coordinate lodging",
            "constraints": {"max_spend_cents": 50000},
            "desires": [
                {
                    "id": "hotel",
                    "area": "travel",
                    "outcome": "Book the hotel",
                    "authority": "spend_to_limit",
                }
            ],
        }
    )

    blueprint = await planner.plan(request)

    assert blueprint.steps[0].effect == butler_routes.EffectClass.SPEND
    assert blueprint.steps[0].approval_gate is not None
    assert "spending" in blueprint.steps[0].approval_gate.reason


@pytest.mark.asyncio
async def test_irreversible_flag_never_elevates_read_only_authority():
    planner = butler_routes.ButlerBlueprintPlanner(
        _FakeResolver({"message": [_candidate("send_message")]})
    )
    request = butler_routes.ButlerBlueprintRequest.model_validate(
        {
            "objective": "Keep message access read-only",
            "desires": [
                {
                    "id": "message",
                    "area": "people",
                    "outcome": "Send a message",
                    "authority": "see",
                    "irreversible": True,
                }
            ],
        }
    )

    blueprint = await planner.plan(request)

    assert blueprint.steps == []
    assert blueprint.unresolved[0].reason == "authority_insufficient"


def test_route_rejects_cyclic_desires_before_discovery():
    app = FastAPI()
    app.include_router(butler_routes.router)
    client = TestClient(app)

    response = client.post(
        "/api/v1/butler/blueprints/plan",
        json={
            "objective": "Coordinate two linked outcomes",
            "desires": [
                {
                    "id": "first",
                    "area": "work",
                    "outcome": "Review the first item",
                    "depends_on": ["second"],
                },
                {
                    "id": "second",
                    "area": "work",
                    "outcome": "Review the second item",
                    "depends_on": ["first"],
                },
            ],
        },
    )

    assert response.status_code == 422
    assert "cycle" in response.text


def test_subcomponent_api_resolves_structure_without_tool_discovery():
    app = FastAPI()
    app.include_router(butler_routes.router)
    client = TestClient(app)

    catalog = client.get("/api/v1/butler/subcomponents/catalog")
    assert catalog.status_code == 200
    assert [item["area"] for item in catalog.json()] == [
        "work", "home", "travel", "money", "health", "people"
    ]
    assert all("widget" in item["realtime_surfaces"] for item in catalog.json())

    resolved = client.post(
        "/api/v1/butler/subcomponents/resolve",
        json={
            "objective": "Protect training while coordinating the trip",
            "desires": [
                {"id": "training", "area": "health", "outcome": "Protect training"},
                {"id": "trip", "area": "travel", "outcome": "Coordinate the trip"},
            ],
        },
    )
    assert resolved.status_code == 200
    assert [item["id"] for item in resolved.json()["components"]] == [
        "travel-desk", "health-desk"
    ]


@pytest.mark.asyncio
async def test_intent_compiler_infers_multiple_life_areas(monkeypatch):
    captured = {}

    class _Planner:
        async def plan(self, request):
            captured["request"] = request
            return butler_routes.ButlerBlueprintResponse(
                blueprint_id="butler_bp_intent",
                generated_at=butler_routes.datetime.now(butler_routes.timezone.utc),
                name=request.name,
                objective=request.objective,
                initiative=request.initiative,
                follow_through=request.follow_through,
                components=butler_routes._resolve_components(request),
                steps=[],
                stages=[],
                unresolved=[],
                optimization=butler_routes.OptimizationSummary(
                    method="fixture",
                    objective_score=0,
                    candidates_considered=0,
                    selected_steps=0,
                    unresolved_desires=0,
                    stage_count=0,
                    selection_basis=[],
                ),
            )

    monkeypatch.setattr(butler_routes, "_planner", _Planner())
    response = await butler_routes.compile_butler_intent(
        butler_routes.IntentCompileRequest(
            intent="Move the Aspen flight and protect my training block"
        )
    )

    assert response.inferred_areas == [
        butler_routes.LifeArea.TRAVEL,
        butler_routes.LifeArea.HEALTH,
    ]
    assert [desire.id for desire in captured["request"].desires] == [
        "travel_outcome", "health_outcome"
    ]
