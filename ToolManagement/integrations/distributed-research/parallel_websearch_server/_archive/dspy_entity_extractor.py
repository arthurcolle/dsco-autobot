"""Parallel entity extraction with auto-evaluated models.

Uses OpenRouter models selected for best latency/price/intelligence ratio.
Runs extraction in parallel across multiple models with consensus voting.
Validates entities against source text (grounding).
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

logger = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# Default models if no evaluation has been run
DEFAULT_MODELS = [
    "google/gemini-2.0-flash-lite-001",
    "meta-llama/llama-3.1-8b-instruct",
    "qwen/qwen-2.5-7b-instruct",
    "deepseek/deepseek-chat",
]

# Entity types
ENTITY_TYPES = {
    "person": "Named individual humans",
    "organization": "Companies, institutions, agencies, groups",
    "location": "Cities, countries, geographic places, addresses",
    "product": "Named products, services, software",
    "event": "Named events, conferences, incidents",
    "date": "Specific dates, time periods",
    "money": "Monetary amounts, prices",
    "quantity": "Numbers with units, percentages",
    "technology": "Technologies, frameworks, protocols",
}

# In-memory cache
_extraction_cache: Dict[str, Dict[str, Any]] = {}
_cache_max_size = 200


def _cache_key(text: str, models: List[str]) -> str:
    """Generate cache key from text and models."""
    h = hashlib.md5((text + "|".join(sorted(models))).encode()).hexdigest()[:16]
    return h


def _normalize(text: str) -> str:
    """Normalize text for matching."""
    return re.sub(r'\s+', ' ', text.lower().strip())


def _entity_in_text(entity_name: str, text: str) -> Tuple[bool, float]:
    """Check if entity appears in text. Returns (found, confidence)."""
    text_lower = text.lower()
    entity_lower = entity_name.lower().strip()

    if not entity_lower or len(entity_lower) < 2:
        return False, 0.0

    # Exact match
    if entity_lower in text_lower:
        return True, 1.0

    # Word boundary match
    pattern = r'\b' + re.escape(entity_lower) + r'\b'
    if re.search(pattern, text_lower):
        return True, 1.0

    # Multi-word partial match
    words = entity_lower.split()
    if len(words) > 1:
        found = sum(1 for w in words if len(w) > 2 and w in text_lower)
        if found >= len(words) * 0.7:
            return True, found / len(words)

    return False, 0.0


@dataclass
class ExtractedEntity:
    """Entity extracted by models."""
    name: str
    entity_type: str
    confidence: float
    grounded: bool
    models: List[str]
    context: str = ""

    def to_dict(self) -> Dict:
        return {
            "name": self.name,
            "type": self.entity_type,
            "confidence": round(self.confidence, 4),
            "grounded": self.grounded,
            "models": self.models,
            "consensus_votes": len(self.models),
            "context": self.context,
        }


class ParallelEntityExtractor:
    """Extract entities using multiple OpenRouter models in parallel.

    Features:
    - Runs 3-5 models concurrently for speed
    - Consensus voting: entities found by 2+ models are kept
    - Grounding: validates entities against source text
    - Auto-eval: uses best models based on latency/price/intelligence
    """

    def __init__(
        self,
        models: Optional[List[str]] = None,
        max_concurrent: int = 10,
        min_consensus: int = 2,
    ):
        self.models = models or DEFAULT_MODELS
        self.max_concurrent = max_concurrent
        self.min_consensus = min_consensus
        self._semaphore = asyncio.Semaphore(max_concurrent)

        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(45.0, connect=10.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
        self._headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://parallel-ai.local",
        }

    async def _call_model(
        self,
        model_id: str,
        prompt: str,
        max_tokens: int = 1000,
    ) -> Tuple[str, Optional[str], float]:
        """Call model, return (model_id, response, latency_ms)."""
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
                    logger.warning(f"{model_id} returned {resp.status_code}")
                    return model_id, None, latency

                data = resp.json()
                content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                return model_id, content, latency

            except Exception as e:
                latency = (time.perf_counter() - start) * 1000
                logger.warning(f"{model_id} error: {e}")
                return model_id, None, latency

    def _parse_entities(self, response: str) -> List[Dict]:
        """Parse entity JSON from model response."""
        if not response:
            return []

        response = response.strip()

        # Direct parse
        try:
            parsed = json.loads(response)
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                return parsed.get("entities", [])
        except json.JSONDecodeError:
            pass

        # Extract from markdown
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

        # Find JSON boundaries
        for start_char, end_char in [("[", "]"), ("{", "}")]:
            start = response.find(start_char)
            end = response.rfind(end_char)
            if start != -1 and end > start:
                try:
                    result = json.loads(response[start:end+1])
                    if isinstance(result, list):
                        return result
                    if isinstance(result, dict):
                        return result.get("entities", [])
                except:
                    pass

        return []

    def _normalize_entity_key(self, name: str) -> str:
        """Normalize entity name for matching across models."""
        return _normalize(name)

    async def extract_entities(
        self,
        text: str,
        entity_types: Optional[List[str]] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        """Extract entities from text using parallel models with consensus."""
        # Check cache
        cache_key = _cache_key(text, self.models)
        if use_cache and cache_key in _extraction_cache:
            cached = _extraction_cache[cache_key].copy()
            cached["from_cache"] = True
            return cached

        start_time = datetime.utcnow()
        types_list = entity_types or list(ENTITY_TYPES.keys())

        prompt = f"""Extract named entities from this text. Return ONLY a JSON array.

RULES:
1. Extract ONLY entities explicitly mentioned in the text
2. Each entity must be an exact or near-exact quote from the text
3. Do not infer or guess entities

Entity types: {', '.join(types_list)}

Text:
---
{text[:4000]}
---

Return JSON array: [{{"name": "exact entity name", "type": "type", "confidence": 0.0-1.0}}]
JSON only:"""

        # Call all models in parallel
        tasks = [self._call_model(m, prompt) for m in self.models]
        results = await asyncio.gather(*tasks)

        # Aggregate entities by normalized name
        entity_votes: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "names": [],
            "types": defaultdict(int),
            "confidences": [],
            "models": [],
        })

        model_latencies = {}
        for model_id, response, latency in results:
            model_latencies[model_id] = latency
            if not response:
                continue

            entities = self._parse_entities(response)
            short_name = model_id.split("/")[-1]

            for e in entities:
                if not isinstance(e, dict):
                    continue

                name = str(e.get("name", e.get("entity", ""))).strip()
                if not name or len(name) < 2:
                    continue

                key = self._normalize_entity_key(name)
                entity_votes[key]["names"].append(name)
                etype = e.get("type", e.get("entity_type", "other")).lower()
                entity_votes[key]["types"][etype] += 1
                entity_votes[key]["confidences"].append(
                    float(e.get("confidence", 0.5))
                )
                entity_votes[key]["models"].append(short_name)

        # Build consensus entities (2+ model votes)
        validated_entities: List[ExtractedEntity] = []
        for key, data in entity_votes.items():
            vote_count = len(set(data["models"]))
            if vote_count < self.min_consensus:
                continue

            # Pick most common name form
            name = max(set(data["names"]), key=data["names"].count)

            # Pick most voted type
            etype = max(data["types"], key=data["types"].get)

            # Average confidence
            avg_conf = sum(data["confidences"]) / len(data["confidences"])

            # Check grounding
            found, match_conf = _entity_in_text(name, text)
            if not found:
                logger.debug(f"Entity '{name}' not grounded - skipping")
                continue

            final_conf = (avg_conf + match_conf) / 2

            validated_entities.append(ExtractedEntity(
                name=name,
                entity_type=etype,
                confidence=final_conf,
                grounded=True,
                models=list(set(data["models"])),
            ))

        # Sort by confidence
        validated_entities.sort(key=lambda x: -x.confidence)

        # Compute stats
        total_raw = sum(len(self._parse_entities(r[1])) for r in results if r[1])
        grounded_count = len(validated_entities)

        processing_time = round(
            (datetime.utcnow() - start_time).total_seconds() * 1000, 2
        )

        result = {
            "entities": [e.to_dict() for e in validated_entities],
            "entity_count": grounded_count,
            "stats": {
                "total_raw_extractions": total_raw,
                "consensus_entities": grounded_count,
                "grounding_rate": round(grounded_count / total_raw, 4) if total_raw else 0,
                "models_responded": sum(1 for r in results if r[1]),
                "avg_latency_ms": round(sum(model_latencies.values()) / len(model_latencies), 2),
            },
            "model_latencies": model_latencies,
            "models_used": self.models,
            "processing_time_ms": processing_time,
            "from_cache": False,
        }

        # Update cache
        if use_cache and validated_entities:
            if len(_extraction_cache) >= _cache_max_size:
                oldest = next(iter(_extraction_cache))
                del _extraction_cache[oldest]
            _extraction_cache[cache_key] = result

        return result

    async def extract_relationships(
        self,
        entities: List[Dict],
        text: str,
        max_pairs: int = 15,
    ) -> List[Dict]:
        """Extract relationships between co-occurring entities."""
        if len(entities) < 2:
            return []

        relationships = []
        text_lower = text.lower()

        for i, e1 in enumerate(entities[:10]):
            for e2 in entities[i+1:10]:
                if len(relationships) >= max_pairs:
                    break

                name1 = e1["name"].lower()
                name2 = e2["name"].lower()
                pos1 = text_lower.find(name1)
                pos2 = text_lower.find(name2)

                if pos1 == -1 or pos2 == -1:
                    continue

                distance = abs(pos1 - pos2)
                if distance < 500:
                    start = max(0, min(pos1, pos2) - 40)
                    end = min(len(text), max(pos1, pos2) + 80)
                    context = text[start:end]

                    conf = max(0.3, 1.0 - (distance / 500))

                    relationships.append({
                        "source": e1["name"],
                        "target": e2["name"],
                        "relation_type": "co_occurs",
                        "confidence": round(conf, 4),
                        "distance_chars": distance,
                        "context": context[:150],
                    })

        # Optionally classify relationships using a model
        if relationships and len(relationships) <= 12:
            relationships = await self._classify_relationships(relationships)

        return relationships

    async def _classify_relationships(
        self,
        relationships: List[Dict]
    ) -> List[Dict]:
        """Use a fast model to classify relationship types."""
        pairs_desc = "\n".join([
            f"{i+1}. {r['source']} <-> {r['target']}: \"{r['context'][:60]}...\""
            for i, r in enumerate(relationships)
        ])

        prompt = f"""Classify these entity pair relationships.

Pairs:
{pairs_desc}

Types: works_for, founded, leads, member_of, located_in, owns, acquired, competes_with, partnered_with, created, uses, part_of, co_occurs

Return JSON: [{{"pair": 1, "relation": "type", "confidence": 0.0-1.0}}]
JSON only:"""

        # Use fastest model
        model_id, response, _ = await self._call_model(
            self.models[0], prompt, max_tokens=400
        )

        if response:
            try:
                parsed = self._parse_entities(response)
                for item in parsed:
                    if not isinstance(item, dict):
                        continue
                    idx = item.get("pair", 0) - 1
                    if 0 <= idx < len(relationships):
                        relationships[idx]["relation_type"] = item.get("relation", "co_occurs")
                        model_conf = item.get("confidence", 0.5)
                        relationships[idx]["confidence"] = max(
                            relationships[idx]["confidence"],
                            model_conf
                        )
            except:
                pass

        return relationships

    async def process_document(
        self,
        text: str,
        doc_id: str = "",
        extract_relationships: bool = True,
    ) -> Dict[str, Any]:
        """Full extraction pipeline."""
        start_time = datetime.utcnow()

        extraction = await self.extract_entities(text)
        entities = extraction.get("entities", [])

        result = {
            "doc_id": doc_id or f"doc_{start_time.strftime('%Y%m%d_%H%M%S')}",
            "entities": entities,
            "entity_count": len(entities),
            "relationships": [],
            "stats": extraction.get("stats", {}),
            "models_used": extraction.get("models_used", []),
            "model_latencies": extraction.get("model_latencies", {}),
            "processing_time_ms": 0,
            "from_cache": extraction.get("from_cache", False),
        }

        if extract_relationships and len(entities) >= 2:
            result["relationships"] = await self.extract_relationships(entities, text)

        result["processing_time_ms"] = round(
            (datetime.utcnow() - start_time).total_seconds() * 1000, 2
        )

        return result

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()


# Global instance management
_extractor: Optional[ParallelEntityExtractor] = None
_extractor_models: Optional[List[str]] = None


def get_extractor(models: Optional[List[str]] = None) -> ParallelEntityExtractor:
    """Get or create extractor with specified models."""
    global _extractor, _extractor_models

    models = models or DEFAULT_MODELS

    # Recreate if models changed
    if _extractor is None or _extractor_models != models:
        _extractor = ParallelEntityExtractor(models=models)
        _extractor_models = models

    return _extractor


def clear_cache():
    """Clear extraction cache."""
    global _extraction_cache
    _extraction_cache.clear()


async def extract_with_consensus(text: str, models: Optional[List[str]] = None) -> Dict[str, Any]:
    """High-level extraction function."""
    extractor = get_extractor(models)
    return await extractor.process_document(text)


# Legacy alias
GroundedEntityExtractor = ParallelEntityExtractor
extract_with_grounding = extract_with_consensus
