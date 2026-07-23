"""Model auto-evaluation for optimal extraction performance.

Benchmarks OpenRouter models to find best latency/price/intelligence ratio.
Excludes top 7.5% expensive and bottom 7.5% cheap models.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# Test prompts with known expected entities for evaluation
EVAL_PROMPTS = [
    {
        "text": "Apple CEO Tim Cook announced the iPhone 16 at their Cupertino headquarters on September 9, 2024.",
        "expected_entities": ["Apple", "Tim Cook", "iPhone 16", "Cupertino", "September 9, 2024"],
        "expected_types": {"Apple": "organization", "Tim Cook": "person", "Cupertino": "location"},
    },
    {
        "text": "Microsoft acquired Activision Blizzard for $69 billion, making it the largest gaming deal in history.",
        "expected_entities": ["Microsoft", "Activision Blizzard", "$69 billion"],
        "expected_types": {"Microsoft": "organization", "Activision Blizzard": "organization"},
    },
    {
        "text": "Elon Musk's SpaceX launched Starship from Boca Chica, Texas in March 2024.",
        "expected_entities": ["Elon Musk", "SpaceX", "Starship", "Boca Chica", "Texas", "March 2024"],
        "expected_types": {"Elon Musk": "person", "SpaceX": "organization", "Texas": "location"},
    },
]


@dataclass
class ModelScore:
    """Evaluation score for a model."""
    model_id: str
    latency_ms: float
    price_per_1k: float  # prompt + completion combined
    precision: float  # correct entities / extracted entities
    recall: float  # correct entities / expected entities
    f1: float
    intelligence_score: float  # weighted combination
    value_score: float  # intelligence / (price * latency)
    errors: int = 0
    eval_time: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict:
        return {
            "model_id": self.model_id,
            "latency_ms": round(self.latency_ms, 2),
            "price_per_1k": self.price_per_1k,
            "precision": round(self.precision, 4),
            "recall": round(self.recall, 4),
            "f1": round(self.f1, 4),
            "intelligence_score": round(self.intelligence_score, 4),
            "value_score": round(self.value_score, 4),
            "errors": self.errors,
            "eval_time": self.eval_time.isoformat(),
        }


class ModelEvaluator:
    """Evaluates and ranks OpenRouter models for entity extraction."""

    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=15.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
        self._headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://parallel-ai.local",
        }
        self._model_cache: Dict[str, List[Dict]] = {}
        self._eval_cache: Dict[str, ModelScore] = {}
        self._best_models: List[str] = []

    async def fetch_models(self, force_refresh: bool = False) -> List[Dict]:
        """Fetch and filter OpenRouter models (middle 85%)."""
        cache_key = "filtered_models"
        if not force_refresh and cache_key in self._model_cache:
            return self._model_cache[cache_key]

        try:
            resp = await self.client.get(f"{OPENROUTER_BASE_URL}/models")
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch models: {e}")
            return []

        models = []
        for m in data.get("data", []):
            pricing = m.get("pricing", {})
            prompt = pricing.get("prompt")
            completion = pricing.get("completion")
            if prompt and prompt != "-1" and completion:
                try:
                    p = float(prompt)
                    c = float(completion)
                    if p > 0:  # exclude free
                        models.append({
                            "id": m["id"],
                            "prompt_price": p,
                            "completion_price": c,
                            "context_length": m.get("context_length", 4096),
                            "name": m.get("name", m["id"]),
                        })
                except (ValueError, TypeError):
                    pass

        # Sort by price and trim top/bottom 7.5%
        models.sort(key=lambda x: x["prompt_price"])
        n = len(models)
        low_cut = int(n * 0.075)
        high_cut = int(n * 0.925)
        filtered = models[low_cut:high_cut]

        logger.info(f"Filtered to {len(filtered)} models (from {n})")
        self._model_cache[cache_key] = filtered
        return filtered

    async def _call_model(
        self,
        model_id: str,
        prompt: str,
        max_tokens: int = 500,
    ) -> Tuple[Optional[str], float, Optional[str]]:
        """Call a model and return (response, latency_ms, error)."""
        payload = {
            "model": model_id,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.0,
        }

        async with self._semaphore:
            start = time.perf_counter()
            try:
                resp = await self.client.post(
                    f"{OPENROUTER_BASE_URL}/chat/completions",
                    headers=self._headers,
                    json=payload,
                )
                latency = (time.perf_counter() - start) * 1000

                if resp.status_code != 200:
                    return None, latency, f"HTTP {resp.status_code}: {resp.text[:200]}"

                data = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                return content, latency, None

            except httpx.TimeoutException:
                return None, (time.perf_counter() - start) * 1000, "timeout"
            except Exception as e:
                return None, (time.perf_counter() - start) * 1000, str(e)

    def _parse_entities(self, response: str) -> List[Dict]:
        """Parse entity JSON from model response."""
        if not response:
            return []

        response = response.strip()

        # Try direct parse
        try:
            parsed = json.loads(response)
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict) and "entities" in parsed:
                return parsed["entities"]
        except json.JSONDecodeError:
            pass

        # Extract from markdown code block
        if "```" in response:
            try:
                start = response.find("```")
                end = response.rfind("```")
                if start != end:
                    inner = response[start+3:end].strip()
                    if inner.startswith("json"):
                        inner = inner[4:].strip()
                    parsed = json.loads(inner)
                    if isinstance(parsed, list):
                        return parsed
            except:
                pass

        # Find JSON array boundaries
        start = response.find("[")
        end = response.rfind("]")
        if start != -1 and end > start:
            try:
                return json.loads(response[start:end+1])
            except:
                pass

        return []

    def _score_extraction(
        self,
        extracted: List[Dict],
        expected_entities: List[str],
        expected_types: Dict[str, str],
    ) -> Tuple[float, float, float]:
        """Score extraction quality. Returns (precision, recall, f1)."""
        if not extracted:
            return 0.0, 0.0, 0.0

        extracted_names = set()
        type_matches = 0
        for e in extracted:
            name = e.get("name", e.get("entity", "")).lower().strip()
            extracted_names.add(name)

            # Check type correctness
            etype = e.get("type", e.get("entity_type", "")).lower()
            for exp_name, exp_type in expected_types.items():
                if exp_name.lower() in name or name in exp_name.lower():
                    if exp_type.lower() in etype or etype in exp_type.lower():
                        type_matches += 1

        expected_lower = {e.lower() for e in expected_entities}

        # Fuzzy matching
        correct = 0
        for exp in expected_lower:
            for ext in extracted_names:
                if exp in ext or ext in exp:
                    correct += 1
                    break

        precision = correct / len(extracted_names) if extracted_names else 0
        recall = correct / len(expected_lower) if expected_lower else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        # Boost for correct types
        type_bonus = type_matches / len(expected_types) if expected_types else 0
        f1 = min(1.0, f1 + type_bonus * 0.1)

        return precision, recall, f1

    async def evaluate_model(self, model: Dict) -> ModelScore:
        """Evaluate a single model on the test prompts."""
        model_id = model["id"]

        # Check cache (valid for 1 hour)
        if model_id in self._eval_cache:
            cached = self._eval_cache[model_id]
            if datetime.utcnow() - cached.eval_time < timedelta(hours=1):
                return cached

        prompt_template = """Extract named entities from this text. Return ONLY a JSON array.

Text: {text}

Return format: [{{"name": "entity name", "type": "person|organization|location|product|event|date|money|quantity"}}]
JSON only, no explanation:"""

        total_latency = 0
        total_precision = 0
        total_recall = 0
        total_f1 = 0
        errors = 0

        for test in EVAL_PROMPTS:
            prompt = prompt_template.format(text=test["text"])
            response, latency, error = await self._call_model(model_id, prompt)
            total_latency += latency

            if error:
                errors += 1
                continue

            entities = self._parse_entities(response)
            p, r, f = self._score_extraction(
                entities,
                test["expected_entities"],
                test["expected_types"]
            )
            total_precision += p
            total_recall += r
            total_f1 += f

        n_tests = len(EVAL_PROMPTS)
        avg_latency = total_latency / n_tests
        avg_precision = total_precision / n_tests if n_tests > errors else 0
        avg_recall = total_recall / n_tests if n_tests > errors else 0
        avg_f1 = total_f1 / n_tests if n_tests > errors else 0

        # Calculate prices (per 1K tokens, ~500 tokens per request)
        price_per_1k = (model["prompt_price"] + model["completion_price"]) * 1000

        # Intelligence score (weighted F1 with error penalty)
        error_penalty = 1 - (errors / n_tests) if n_tests > 0 else 0
        intelligence = avg_f1 * error_penalty

        # Value score: intelligence / (normalized_price * normalized_latency)
        # Lower price and latency = higher value
        norm_price = max(price_per_1k, 0.0001)  # avoid division by zero
        norm_latency = max(avg_latency / 1000, 0.1)  # convert to seconds, min 0.1
        value = intelligence / (norm_price * norm_latency) if intelligence > 0 else 0

        score = ModelScore(
            model_id=model_id,
            latency_ms=avg_latency,
            price_per_1k=price_per_1k,
            precision=avg_precision,
            recall=avg_recall,
            f1=avg_f1,
            intelligence_score=intelligence,
            value_score=value,
            errors=errors,
        )

        self._eval_cache[model_id] = score
        return score

    async def evaluate_candidates(
        self,
        n_candidates: int = 20,
        price_tier: str = "low",  # low, mid, high
    ) -> List[ModelScore]:
        """Evaluate a subset of candidate models from specified price tier."""
        models = await self.fetch_models()
        if not models:
            return []

        # Select candidates from price tier
        n = len(models)
        if price_tier == "low":
            candidates = models[:n//3]
        elif price_tier == "high":
            candidates = models[2*n//3:]
        else:  # mid
            candidates = models[n//3:2*n//3]

        # Sample if too many
        if len(candidates) > n_candidates:
            import random
            candidates = random.sample(candidates, n_candidates)

        logger.info(f"Evaluating {len(candidates)} models from {price_tier} tier")

        # Evaluate in parallel
        tasks = [self.evaluate_model(m) for m in candidates]
        scores = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        for s in scores:
            if isinstance(s, ModelScore):
                results.append(s)
            else:
                logger.warning(f"Evaluation error: {s}")

        # Sort by value score
        results.sort(key=lambda x: -x.value_score)
        return results

    async def find_best_models(
        self,
        n_per_tier: int = 10,
        top_n: int = 5,
    ) -> List[ModelScore]:
        """Find best models across all price tiers."""
        all_scores = []

        # Evaluate from each tier in parallel
        tier_tasks = [
            self.evaluate_candidates(n_per_tier, "low"),
            self.evaluate_candidates(n_per_tier, "mid"),
            self.evaluate_candidates(n_per_tier, "high"),
        ]
        tier_results = await asyncio.gather(*tier_tasks)

        for results in tier_results:
            all_scores.extend(results)

        # Sort by value and get top N
        all_scores.sort(key=lambda x: -x.value_score)
        best = all_scores[:top_n]

        # Cache best model IDs
        self._best_models = [s.model_id for s in best]

        return best

    def get_best_models(self) -> List[str]:
        """Get cached best model IDs."""
        return self._best_models if self._best_models else [
            "google/gemini-2.0-flash-lite-001",
            "meta-llama/llama-3.1-8b-instruct",
            "qwen/qwen-2.5-7b-instruct",
        ]

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()


# Global instance
_evaluator: Optional[ModelEvaluator] = None


def get_evaluator() -> ModelEvaluator:
    """Get or create global evaluator instance."""
    global _evaluator
    if _evaluator is None:
        _evaluator = ModelEvaluator()
    return _evaluator


async def quick_benchmark(n_models: int = 15) -> List[Dict]:
    """Run a quick benchmark and return top models."""
    evaluator = get_evaluator()
    scores = await evaluator.find_best_models(n_per_tier=n_models // 3, top_n=5)
    return [s.to_dict() for s in scores]
