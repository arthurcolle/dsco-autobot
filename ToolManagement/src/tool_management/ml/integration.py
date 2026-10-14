"""
Stage 7: Wire the trained model into the existing recommend_tools endpoint.

Provides a drop-in replacement scorer that:
1. Uses the local model for tool scoring (policy head)
2. Falls back to Jina rerank if model unavailable
3. Blends model scores with historical feedback (Bayesian update)
4. Auto-records feedback for self-improvement

Usage in tools_api.py:
    from tool_management.ml.integration import get_smart_recommender
    recommender = get_smart_recommender()
    results = await recommender.recommend(query, candidates, intent)
"""

import logging
import math
import os
import time
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def _validated_intent(label, confidence) -> Optional[Tuple[str, float]]:
    """Accept only known intents with finite probability confidences."""
    from .intent_labels import INTENT_LABELS

    if not isinstance(label, str) or label.strip().lower() not in INTENT_LABELS:
        return None
    if (type(confidence) not in (int, float) or not math.isfinite(confidence)
            or not 0.0 <= confidence <= 1.0):
        return None
    return label.strip().lower(), float(confidence)


@dataclass
class ScoredTool:
    name: str
    description: str
    score: float
    confidence: float
    backend: Optional[str] = None
    source: str = "model"  # "model" | "jina" | "heuristic"
    intent_match: bool = False
    historical_success_rate: Optional[float] = None
    observation_count: int = 0
    tool_id: Optional[str] = None


class SmartRecommender:
    """
    Intelligent tool recommender that blends:
    1. Trained local model (policy head) — primary scorer
    2. Jina reranker — fallback / validation
    3. Historical feedback — Bayesian posterior update
    4. SemanticToolStore — ANN retrieval
    """

    def __init__(
        self,
        model_endpoint: Optional[str] = None,
        jina_fallback: bool = True,
        feedback_blend_threshold: int = 20,
        model_timeout_ms: int = 2000,
        model_health_ttl_seconds: float = 30.0,
    ):
        self.model_endpoint = model_endpoint or os.getenv(
            "TOOL_POLICY_ENDPOINT", "http://localhost:8100"
        )
        self.jina_fallback = jina_fallback
        self.feedback_blend_threshold = max(1, feedback_blend_threshold)
        self.model_timeout_ms = model_timeout_ms
        self.model_health_ttl_seconds = max(1.0, model_health_ttl_seconds)
        self._model_health_checked_at = 0.0

        self._model_available = None  # None = unknown, True/False = tested
        self._http_client = None

    async def _get_client(self):
        if self._http_client is None or self._http_client.is_closed:
            import httpx
            self._http_client = httpx.AsyncClient(timeout=self.model_timeout_ms / 1000)
        return self._http_client

    async def _check_model_health(self) -> bool:
        """Check if the local model server is available."""
        try:
            client = await self._get_client()
            resp = await client.get(f"{self.model_endpoint}/health")
            self._model_available = resp.status_code == 200
        except Exception:
            self._model_available = False
        self._model_health_checked_at = time.monotonic()
        return self._model_available

    async def _ensure_model_health(self) -> bool:
        """Refresh failed probes so a restarted model can rejoin service."""
        if (self._model_available is None or
                time.monotonic() - self._model_health_checked_at >= self.model_health_ttl_seconds):
            await self._check_model_health()
        return bool(self._model_available)

    async def _score_with_model(
        self,
        task: str,
        candidates: List[Dict[str, str]],
        top_k: int = 10,
    ) -> Optional[List[ScoredTool]]:
        """Score tools using the local model server."""
        try:
            client = await self._get_client()
            resp = await client.post(
                f"{self.model_endpoint}/score_tools",
                json={
                    "task": task,
                    "candidates": candidates,
                    "top_k": top_k,
                },
            )
            resp.raise_for_status()
            data = resp.json()

            results = []
            by_name = {}
            for candidate in candidates:
                by_name.setdefault(candidate["name"], []).append(candidate)
            seen = set()
            for r in data.get("rankings", []):
                if not isinstance(r, dict):
                    continue
                idx = r.get("candidate_index")
                if type(idx) is int and 0 <= idx < len(candidates):
                    candidate = candidates[idx]
                elif "candidate_index" in r:
                    continue
                else:
                    matches = by_name.get(r.get("tool"), [])
                    # Legacy servers return names only. Duplicate names across
                    # providers cannot be assigned safely; use indexed rerank.
                    if len(matches) != 1:
                        continue
                    candidate = matches[0]
                    idx = candidates.index(candidate)
                score = r.get("score")
                if (idx in seen or type(score) not in (int, float)
                        or not math.isfinite(score)):
                    continue
                seen.add(idx)
                confidence = data.get("confidence", 0.5)
                if type(confidence) not in (int, float) or not math.isfinite(confidence):
                    confidence = 0.5
                results.append(ScoredTool(
                    name=candidate["name"],
                    description=candidate.get("description", ""),
                    backend=candidate.get("backend"),
                    tool_id=candidate.get("tool_id"),
                    score=float(score),
                    confidence=confidence,
                    source="model",
                ))

            return results or None
        except Exception as e:
            logger.warning(f"Model scoring failed: {e}")
            self._model_available = False
            self._model_health_checked_at = time.monotonic()
            return None

    async def _score_with_jina(
        self,
        task: str,
        candidates: List[Dict[str, str]],
        top_k: int = 10,
    ) -> List[ScoredTool]:
        """Fallback: score tools with Jina reranker."""
        from ..integrations.jina_provider import get_jina

        jina = get_jina()
        documents = [
            f"{c['name']}: {c.get('description', '')[:200]}"
            for c in candidates
        ]

        reranked = await jina.rerank(
            query=task,
            documents=documents,
            top_n=top_k,
        )

        results = []
        seen = set()
        for r in reranked:
            if not isinstance(r, dict):
                continue
            idx = r.get("index")
            score = r.get("relevance_score")
            if (type(idx) is int and 0 <= idx < len(candidates) and idx not in seen
                    and type(score) in (int, float) and math.isfinite(score)):
                seen.add(idx)
                results.append(ScoredTool(
                    name=candidates[idx]["name"],
                    description=candidates[idx].get("description", ""),
                    backend=candidates[idx].get("backend"),
                    tool_id=candidates[idx].get("tool_id"),
                    score=float(score),
                    confidence=float(score),
                    source="jina",
                ))

        return results

    def _get_feedback_stats(
        self,
        tool_name: str,
        execution_store=None,
    ) -> Tuple[float, int]:
        """
        Get historical success rate and observation count for a tool.
        Returns (success_rate, count).
        """
        if execution_store is None:
            return 0.5, 0

        try:
            # Query the recommendation_feedback table
            conn = execution_store._get_conn()
            cursor = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN outcome_status = 'completed' THEN 1 ELSE 0 END) as successes
                FROM recommendation_feedback
                WHERE chosen_tool = ?
                  AND outcome_status IS NOT NULL
            """, (tool_name,))
            row = cursor.fetchone()
            if row and row[0] > 0:
                return row[1] / row[0], row[0]
        except Exception:
            pass

        return 0.5, 0

    def _bayesian_blend(
        self,
        model_score: float,
        historical_success: float,
        observation_count: int,
    ) -> float:
        """
        Bayesian blend of model prior with historical posterior.

        β = min(1.0, observation_count / threshold)
        blended = (1 - β) * model_score + β * historical_success
        """
        beta = min(1.0, observation_count / self.feedback_blend_threshold)
        return (1 - beta) * model_score + beta * historical_success

    def _exploration_bonus(self, observation_count: int) -> float:
        """
        Thompson Sampling-inspired exploration bonus.
        Tools with few observations get a bonus to encourage exploration.
        """
        if observation_count < 5:
            return 0.1 * (1.0 / (observation_count + 1))
        return 0.0

    async def recommend(
        self,
        task: str,
        candidates: List[Dict[str, str]],
        intent: Optional[str] = None,
        top_k: int = 10,
        execution_store=None,
    ) -> List[ScoredTool]:
        """
        Main recommendation method.

        Pipeline:
        1. Try local model (fast, free)
        2. Fall back to Jina rerank if model unavailable
        3. Blend with historical feedback
        4. Add exploration bonus
        5. Return top-K sorted by blended score
        """
        if not candidates or top_k <= 0:
            return []
        await self._ensure_model_health()

        # Score tools
        scored: Optional[List[ScoredTool]] = None

        if self._model_available:
            scored = await self._score_with_model(task, candidates, top_k=len(candidates))

        if scored is None and self.jina_fallback:
            logger.info("Using Jina fallback for tool scoring")
            try:
                scored = await self._score_with_jina(task, candidates, top_k=len(candidates))
            except Exception as exc:
                logger.warning("Jina scoring failed, using heuristic fallback: %s", exc)

        if not scored:
            # Last resort: return candidates in order with uniform scores
            scored = [
                ScoredTool(
                    name=c["name"],
                    description=c.get("description", ""),
                    score=1.0 / (i + 1),
                    confidence=0.1,
                    source="heuristic",
                    backend=c.get("backend"),
                    tool_id=c.get("tool_id"),
                )
                for i, c in enumerate(candidates)
            ]

        # Tag intent matches
        if intent:
            intent = intent.lower()
            for tool in scored:
                tool.intent_match = intent in tool.name.lower() or intent in tool.description.lower()

        # Bayesian blend with historical feedback
        for tool in scored:
            success_rate, count = self._get_feedback_stats(tool.name, execution_store)
            tool.historical_success_rate = success_rate
            tool.observation_count = count

            blended = self._bayesian_blend(tool.score, success_rate, count)
            bonus = self._exploration_bonus(count)
            tool.score = blended + bonus

        # Sort by blended score
        scored.sort(key=lambda t: t.score, reverse=True)

        return scored[:top_k]

    async def classify_intent(self, task: str) -> Tuple[str, float]:
        """Classify task intent via model or Jina fallback."""
        await self._ensure_model_health()
        if self._model_available:
            try:
                client = await self._get_client()
                resp = await client.post(
                    f"{self.model_endpoint}/classify_intent",
                    json={"task": task},
                )
                resp.raise_for_status()
                data = resp.json()
                prediction = _validated_intent(data.get("intent"), data.get("confidence"))
                if prediction is not None:
                    return prediction
                logger.warning("Model intent classification returned an invalid prediction")
            except Exception as exc:
                logger.warning("Model intent classification failed: %s", exc)
                self._model_available = False
                self._model_health_checked_at = time.monotonic()

        # Jina fallback
        try:
            from ..integrations.jina_provider import get_jina
            from .intent_labels import INTENT_LABELS

            jina = get_jina()
            results = await jina.classify(task, INTENT_LABELS)
            if results and results[0].get("predictions"):
                top = results[0]["predictions"][0]
                prediction = _validated_intent(top.get("label"), top.get("score"))
                if prediction is not None:
                    return prediction
        except Exception:
            pass

        return "query", 0.5  # default

    async def evaluate_output(self, task: str, output: str) -> float:
        """Evaluate output quality via model."""
        await self._ensure_model_health()
        if self._model_available:
            try:
                client = await self._get_client()
                resp = await client.post(
                    f"{self.model_endpoint}/evaluate_output",
                    json={"task": task, "output": output[:2000]},
                )
                resp.raise_for_status()
                data = resp.json()
                return data["quality_score"]
            except Exception:
                pass

        return 0.5  # unknown quality


# ---------- Singleton ----------

_recommender: Optional[SmartRecommender] = None


def get_smart_recommender(**kwargs) -> SmartRecommender:
    """Get or create the singleton SmartRecommender."""
    global _recommender
    if _recommender is None:
        _recommender = SmartRecommender(**kwargs)
    return _recommender
