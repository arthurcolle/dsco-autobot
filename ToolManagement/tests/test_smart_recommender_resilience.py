import asyncio
import importlib.util
from pathlib import Path
import sys
import types

import pytest


@pytest.fixture
def integration(monkeypatch):
    root = Path(__file__).parents[1] / "src/tool_management"
    for name, directory in (("recommender_test", root), ("recommender_test.ml", root / "ml"),
                            ("recommender_test.integrations", root / "integrations")):
        package = types.ModuleType(name)
        package.__path__ = [str(directory)]
        monkeypatch.setitem(sys.modules, name, package)
    name = "recommender_test.ml.integration"
    spec = importlib.util.spec_from_file_location(name, root / "ml/integration.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    yield module
    sys.modules.pop("recommender_test.ml.intent_labels", None)


def test_jina_outage_reaches_heuristic_fallback(integration, monkeypatch):
    recommender = integration.SmartRecommender()
    async def unavailable():
        recommender._model_available = False
        return False
    async def failing_jina(*args, **kwargs):
        raise RuntimeError("reranker 503")
    monkeypatch.setattr(recommender, "_check_model_health", unavailable)
    monkeypatch.setattr(recommender, "_score_with_jina", failing_jina)
    results = asyncio.run(recommender.recommend("weather", [{"name": "weather_lookup"}]))
    assert results[0].source == "heuristic"


def test_local_model_recovers_after_temporary_outage(integration, monkeypatch):
    recommender = integration.SmartRecommender(jina_fallback=False)
    clock = [0.0]
    health_checks = []
    monkeypatch.setattr(integration.time, "monotonic", lambda: clock[0])
    class Response:
        def __init__(self, code):
            self.status_code = code
    class Client:
        async def get(self, url):
            health_checks.append(url)
            return Response(503 if len(health_checks) == 1 else 200)
    async def client():
        return Client()
    async def score(*args, **kwargs):
        return [integration.ScoredTool("weather_lookup", "weather", .9, .8)]
    monkeypatch.setattr(recommender, "_get_client", client)
    monkeypatch.setattr(recommender, "_score_with_model", score)
    assert asyncio.run(recommender.recommend("weather", [{"name": "weather_lookup"}]))[0].source == "heuristic"
    clock[0] = 3600.0
    assert asyncio.run(recommender.recommend("weather", [{"name": "weather_lookup"}]))[0].source == "model"
    assert len(health_checks) == 2


def test_first_classification_call_checks_model_health(integration, monkeypatch):
    recommender = integration.SmartRecommender()
    checked = []
    async def health():
        checked.append(True)
        recommender._model_available = True
        return True
    class Response:
        def raise_for_status(self):
            pass
        def json(self):
            return {"intent": "research", "confidence": .8}
    class Client:
        async def post(self, *args, **kwargs):
            return Response()
    async def client():
        return Client()
    monkeypatch.setattr(recommender, "_check_model_health", health)
    monkeypatch.setattr(recommender, "_get_client", client)
    assert asyncio.run(recommender.classify_intent("research papers")) == ("research", .8)
    assert checked


def test_classification_fallback_does_not_import_gpu_model(integration, monkeypatch):
    recommender = integration.SmartRecommender()
    async def health():
        recommender._model_available = False
        return False
    class Jina:
        async def classify(self, task, labels):
            assert "research" in labels
            return [{"predictions": [{"label": "research", "score": .7}]}]
    provider = types.ModuleType("recommender_test.integrations.jina_provider")
    provider.get_jina = Jina
    monkeypatch.setitem(sys.modules, provider.__name__, provider)
    monkeypatch.setitem(sys.modules, "recommender_test.ml.model", None)
    monkeypatch.setattr(recommender, "_check_model_health", health)
    assert asyncio.run(recommender.classify_intent("research papers")) == ("research", .7)


def test_empty_candidate_set_never_calls_providers(integration, monkeypatch):
    recommender = integration.SmartRecommender()
    async def fail():
        pytest.fail("empty candidates should return before probing providers")
    monkeypatch.setattr(recommender, "_check_model_health", fail)
    assert asyncio.run(recommender.recommend("weather", [])) == []


def model_client(monkeypatch, recommender, rankings):
    class Response:
        def raise_for_status(self):
            pass
        def json(self):
            return {"rankings": rankings, "confidence": .8}
    class Client:
        async def post(self, *args, **kwargs):
            return Response()
    async def client():
        return Client()
    monkeypatch.setattr(recommender, "_get_client", client)


def test_model_indices_preserve_same_named_tools_across_backends(integration, monkeypatch):
    recommender = integration.SmartRecommender()
    model_client(monkeypatch, recommender, [
        {"tool": "lookup", "candidate_index": 1, "score": .9},
        {"tool": "lookup", "candidate_index": 0, "score": .2},
    ])
    candidates = [
        {"name": "lookup", "tool_id": "weather:lookup", "backend": "weather"},
        {"name": "lookup", "tool_id": "finance:lookup", "backend": "finance"},
    ]
    results = asyncio.run(recommender._score_with_model("lookup", candidates))
    assert [r.tool_id for r in results] == ["finance:lookup", "weather:lookup"]
    assert [r.backend for r in results] == ["finance", "weather"]


def test_legacy_name_only_model_cannot_guess_between_backends(integration, monkeypatch):
    recommender = integration.SmartRecommender()
    model_client(monkeypatch, recommender, [{"tool": "lookup", "score": .9}])
    candidates = [{"name": "lookup", "tool_id": "weather:lookup"},
                  {"name": "lookup", "tool_id": "finance:lookup"}]
    assert asyncio.run(recommender._score_with_model("lookup", candidates)) is None


@pytest.mark.parametrize("index", [-1, 99, True, "0", None])
def test_invalid_model_indices_cannot_be_reinterpreted_as_names(integration, monkeypatch, index):
    recommender = integration.SmartRecommender()
    model_client(monkeypatch, recommender, [{"tool": "lookup", "candidate_index": index, "score": .9}])
    assert asyncio.run(recommender._score_with_model("lookup", [{"name": "lookup"}])) is None


@pytest.mark.parametrize("score", [None, "invalid", float("nan"), float("inf"), True])
def test_invalid_model_scores_trigger_fallback(integration, monkeypatch, score):
    recommender = integration.SmartRecommender()
    model_client(monkeypatch, recommender, [{"tool": "lookup", "score": score}])
    assert asyncio.run(recommender._score_with_model("lookup", [{"name": "lookup"}])) is None


def test_jina_discards_invalid_scores_and_duplicate_indices(integration, monkeypatch):
    class Jina:
        async def rerank(self, **kwargs):
            return [
                {"index": 0, "relevance_score": None},
                {"index": 1, "relevance_score": .9},
                {"index": 1, "relevance_score": .8},
                {"index": -1, "relevance_score": .7},
                {"index": 0, "relevance_score": .2},
            ]
    provider = types.ModuleType("recommender_test.integrations.jina_provider")
    provider.get_jina = Jina
    monkeypatch.setitem(sys.modules, provider.__name__, provider)
    recommender = integration.SmartRecommender()
    results = asyncio.run(recommender._score_with_jina("lookup", [
        {"name": "lookup", "tool_id": "weather:lookup"},
        {"name": "lookup", "tool_id": "finance:lookup"},
    ]))
    assert [r.tool_id for r in results] == ["finance:lookup", "weather:lookup"]


INVALID_CLASSIFICATIONS = [
    ("unknown-intent", .8), (None, .8), (123, .8),
    ("research", None), ("research", "0.8"), ("research", True),
    ("research", float("nan")), ("research", float("inf")),
    ("research", -.1), ("research", 1.1),
]


@pytest.mark.parametrize("label,confidence", INVALID_CLASSIFICATIONS)
def test_invalid_model_classification_reaches_jina_fallback(integration, monkeypatch, label, confidence):
    recommender = integration.SmartRecommender()
    async def health():
        recommender._model_available = True
        return True
    class Response:
        def raise_for_status(self):
            pass
        def json(self):
            return {"intent": label, "confidence": confidence}
    class Client:
        async def post(self, *args, **kwargs):
            return Response()
    async def client():
        return Client()
    class Jina:
        async def classify(self, task, labels):
            return [{"predictions": [{"label": "research", "score": .7}]}]
    provider = types.ModuleType("recommender_test.integrations.jina_provider")
    provider.get_jina = Jina
    monkeypatch.setitem(sys.modules, provider.__name__, provider)
    monkeypatch.setattr(recommender, "_check_model_health", health)
    monkeypatch.setattr(recommender, "_get_client", client)
    assert asyncio.run(recommender.classify_intent("research papers")) == ("research", .7)


@pytest.mark.parametrize("label,confidence", INVALID_CLASSIFICATIONS)
def test_invalid_jina_classification_reaches_default(integration, monkeypatch, label, confidence):
    recommender = integration.SmartRecommender()
    async def health():
        recommender._model_available = False
        return False
    class Jina:
        async def classify(self, task, labels):
            return [{"predictions": [{"label": label, "score": confidence}]}]
    provider = types.ModuleType("recommender_test.integrations.jina_provider")
    provider.get_jina = Jina
    monkeypatch.setitem(sys.modules, provider.__name__, provider)
    monkeypatch.setattr(recommender, "_check_model_health", health)
    assert asyncio.run(recommender.classify_intent("research papers")) == ("query", .5)
