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
import os
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


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
    ):
        self.model_endpoint = model_endpoint or os.getenv(
            "TOOL_POLICY_ENDPOINT", "http://localhost:8100"
        )
        self.jina_fallback = jina_fallback
        self.feedback_blend_threshold = feedback_blend_threshold
        self.model_timeout_ms = model_timeout_ms

        self._model_available = None  # None = unknown, True/False = tested
        self._http_client = None

    async def _get_client(self):
        if self._http_client is None:
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
        return self._model_available

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
            for r in data.get("rankings", []):
                results.append(ScoredTool(
                    name=r["tool"],
                    description="",  # not returned by model
                    score=r["score"],
                    confidence=data.get("confidence", 0.5),
                    source="model",
                ))

            return results
        except Exception as e:
            logger.warning(f"Model scoring failed: {e}")
            self._model_available = False
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
        for r in reranked:
            idx = r.get("index", 0)
            if idx < len(candidates):
                results.append(ScoredTool(
                    name=candidates[idx]["name"],
                    description=candidates[idx].get("description", ""),
                    score=r.get("relevance_score", 0.0),
                    confidence=r.get("relevance_score", 0.0),
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
        # Check model availability (cached after first check)
        if self._model_available is None:
            await self._check_model_health()

        # Score tools
        scored: Optional[List[ScoredTool]] = None

        if self._model_available:
            scored = await self._score_with_model(task, candidates, top_k=len(candidates))

        if scored is None and self.jina_fallback:
            logger.info("Using Jina fallback for tool scoring")
            scored = await self._score_with_jina(task, candidates, top_k=len(candidates))

        if scored is None:
            # Last resort: return candidates in order with uniform scores
            scored = [
                ScoredTool(
                    name=c["name"],
                    description=c.get("description", ""),
                    score=1.0 / (i + 1),
                    confidence=0.1,
                    source="heuristic",
                )
                for i, c in enumerate(candidates)
            ]

        # Tag intent matches
        if intent:
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
        if self._model_available:
            try:
                client = await self._get_client()
                resp = await client.post(
                    f"{self.model_endpoint}/classify_intent",
                    json={"task": task},
                )
                resp.raise_for_status()
                data = resp.json()
                return data["intent"], data["confidence"]
            except Exception:
                pass

        # Jina fallback
        try:
            from ..integrations.jina_provider import get_jina
            from .model import INTENT_LABELS

            jina = get_jina()
            results = await jina.classify(task, INTENT_LABELS)
            if results and results[0].get("predictions"):
                top = results[0]["predictions"][0]
                return top["label"], top["score"]
        except Exception:
            pass

        return "query", 0.5  # default

    async def evaluate_output(self, task: str, output: str) -> float:
        """Evaluate output quality via model."""
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
