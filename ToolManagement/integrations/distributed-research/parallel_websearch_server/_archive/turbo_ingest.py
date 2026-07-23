"""Turbo batch ingestion with extreme parallelism for instant processing.

Key optimizations:
- 100 concurrent chunks (vs 8)
- 50 chunk batch sizes (vs 10)
- Multi-provider load balancing (OpenRouter + Together + Groq)
- Adaptive rate limiting with backpressure
- Optional local model fallback
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import httpx

logger = logging.getLogger(__name__)

# Provider configurations
PROVIDERS = {
    "openrouter": {
        "base_url": "https://openrouter.ai/api/v1",
        "api_key_env": "OPENROUTER_API_KEY",
        "models": [
            "google/gemini-2.0-flash-lite-001",
            "meta-llama/llama-3.1-8b-instruct",
            "qwen/qwen-2.5-7b-instruct",
            "deepseek/deepseek-chat",
        ],
        "rate_limit": 100,  # requests per second
    },
    "together": {
        "base_url": "https://api.together.xyz/v1",
        "api_key_env": "TOGETHER_API_KEY",
        "models": [
            "meta-llama/Llama-3.2-3B-Instruct-Turbo",
            "meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo",
            "Qwen/Qwen2.5-7B-Instruct-Turbo",
        ],
        "rate_limit": 60,
    },
    "groq": {
        "base_url": "https://api.groq.com/openai/v1",
        "api_key_env": "GROQ_API_KEY",
        "models": [
            "llama-3.1-8b-instant",
            "llama-3.2-3b-preview",
            "gemma2-9b-it",
        ],
        "rate_limit": 30,  # Groq has lower limits
    },
}

# High parallelism settings
MAX_CONCURRENT_CHUNKS = 100
BATCH_SIZE = 50
MAX_CONCURRENT_PER_PROVIDER = 50
TIMEOUT_SECONDS = 30


@dataclass
class ProviderClient:
    """Client for a single LLM provider."""
    name: str
    base_url: str
    api_key: str
    models: List[str]
    rate_limit: int
    client: httpx.AsyncClient = field(default=None, repr=False)
    semaphore: asyncio.Semaphore = field(default=None, repr=False)
    request_count: int = 0
    error_count: int = 0
    avg_latency_ms: float = 0.0
    _rate_limiter: asyncio.Semaphore = field(default=None, repr=False)

    def __post_init__(self):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(TIMEOUT_SECONDS, connect=10.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
        )
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_PER_PROVIDER)
        self._rate_limiter = asyncio.Semaphore(self.rate_limit)

    async def call(self, model: str, prompt: str, max_tokens: int = 800, retries: int = 2) -> Tuple[Optional[str], float, Optional[str]]:
        """Call model with retry logic, return (response, latency_ms, error)."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if self.name == "openrouter":
            headers["HTTP-Referer"] = "https://parallel-ai.local"

        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.0,
        }

        last_error = None
        total_latency = 0

        for attempt in range(retries + 1):
            async with self.semaphore:
                async with self._rate_limiter:
                    start = time.perf_counter()
                    try:
                        resp = await self.client.post(
                            f"{self.base_url}/chat/completions",
                            headers=headers,
                            json=payload,
                        )
                        latency = (time.perf_counter() - start) * 1000
                        total_latency += latency
                        self.request_count += 1

                        # Update rolling average
                        self.avg_latency_ms = (self.avg_latency_ms * 0.9 + latency * 0.1)

                        if resp.status_code == 429:  # Rate limited
                            last_error = "rate_limited"
                            if attempt < retries:
                                await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff
                                continue
                            self.error_count += 1
                            return None, total_latency, last_error

                        if resp.status_code != 200:
                            self.error_count += 1
                            return None, latency, f"HTTP {resp.status_code}"

                        data = resp.json()
                        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                        return content, latency, None

                    except httpx.TimeoutException:
                        latency = (time.perf_counter() - start) * 1000
                        total_latency += latency
                        last_error = "timeout"
                        if attempt < retries:
                            continue
                        self.error_count += 1
                        return None, total_latency, last_error
                    except Exception as e:
                        latency = (time.perf_counter() - start) * 1000
                        total_latency += latency
                        last_error = str(e)
                        if attempt < retries:
                            continue
                        self.error_count += 1
                        return None, total_latency, last_error

        return None, total_latency, last_error

    async def close(self):
        if self.client:
            await self.client.aclose()


class TurboExtractor:
    """Multi-provider parallel entity extractor for extreme throughput."""

    def __init__(
        self,
        providers: Optional[List[str]] = None,
        min_consensus: int = 2,
    ):
        self.min_consensus = min_consensus
        self.providers: Dict[str, ProviderClient] = {}

        # Initialize available providers
        provider_names = providers or ["openrouter", "together", "groq"]
        for name in provider_names:
            if name not in PROVIDERS:
                continue
            config = PROVIDERS[name]
            api_key = os.getenv(config["api_key_env"])
            if api_key:
                self.providers[name] = ProviderClient(
                    name=name,
                    base_url=config["base_url"],
                    api_key=api_key,
                    models=config["models"],
                    rate_limit=config["rate_limit"],
                )
                logger.info(f"Initialized provider: {name} with {len(config['models'])} models")
            else:
                logger.debug(f"Skipping provider {name} - no API key")

        if not self.providers:
            raise RuntimeError("No LLM providers available - check API keys")

        # Build model pool
        self.model_pool: List[Tuple[str, str]] = []  # (provider, model)
        for pname, pclient in self.providers.items():
            for model in pclient.models:
                self.model_pool.append((pname, model))

        logger.info(f"Total model pool: {len(self.model_pool)} models across {len(self.providers)} providers")

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

    async def extract_entities(
        self,
        text: str,
        n_models: int = 4,
    ) -> Dict[str, Any]:
        """Extract entities using multiple models in parallel."""
        import random

        # Select models (round-robin with some randomization)
        selected = random.sample(self.model_pool, min(n_models, len(self.model_pool)))

        prompt = f"""Extract named entities from this text. Return ONLY a JSON array.

RULES:
1. Extract ONLY entities explicitly mentioned in the text
2. Each entity must be an exact or near-exact quote from the text
3. Do not infer or guess entities

Entity types: person, organization, location, product, event, date, money, quantity, technology

Text:
---
{text[:4000]}
---

Return JSON array: [{{"name": "exact entity name", "type": "type", "confidence": 0.0-1.0}}]
JSON only:"""

        # Call all selected models in parallel
        tasks = []
        for provider_name, model_id in selected:
            provider = self.providers[provider_name]
            tasks.append(provider.call(model_id, prompt))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate entities by normalized name
        entity_votes: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
            "names": [],
            "types": defaultdict(int),
            "confidences": [],
            "models": [],
        })

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                continue
            response, latency, error = result
            if error or not response:
                continue

            entities = self._parse_entities(response)
            provider_name, model_id = selected[i]
            short_name = f"{provider_name}:{model_id.split('/')[-1]}"

            for e in entities:
                if not isinstance(e, dict):
                    continue

                name = str(e.get("name", e.get("entity", ""))).strip()
                if not name or len(name) < 2:
                    continue

                key = name.lower()
                entity_votes[key]["names"].append(name)
                etype = e.get("type", e.get("entity_type", "other")).lower()
                entity_votes[key]["types"][etype] += 1
                entity_votes[key]["confidences"].append(float(e.get("confidence", 0.5)))
                entity_votes[key]["models"].append(short_name)

        # Build consensus entities
        validated = []
        for key, data in entity_votes.items():
            vote_count = len(set(data["models"]))
            if vote_count < self.min_consensus:
                continue

            # Check grounding (entity appears in text)
            name = max(set(data["names"]), key=data["names"].count)
            if name.lower() not in text.lower():
                continue

            etype = max(data["types"], key=data["types"].get)
            avg_conf = sum(data["confidences"]) / len(data["confidences"])

            validated.append({
                "name": name,
                "type": etype,
                "confidence": round(avg_conf, 4),
                "models": list(set(data["models"])),
            })

        validated.sort(key=lambda x: -x["confidence"])

        return {
            "entities": validated,
            "entity_count": len(validated),
        }

    async def close(self):
        for provider in self.providers.values():
            await provider.close()

    def get_stats(self) -> Dict[str, Any]:
        """Get provider statistics."""
        stats = {}
        for name, provider in self.providers.items():
            stats[name] = {
                "requests": provider.request_count,
                "errors": provider.error_count,
                "error_rate": provider.error_count / max(1, provider.request_count),
                "avg_latency_ms": round(provider.avg_latency_ms, 2),
            }
        return stats


class TurboIngester:
    """Extreme parallelism corpus ingester."""

    def __init__(
        self,
        corpus_path: Path,
        providers: Optional[List[str]] = None,
        max_concurrent_chunks: int = MAX_CONCURRENT_CHUNKS,
        batch_size: int = BATCH_SIZE,
    ):
        self.corpus_path = corpus_path
        self.providers = providers
        self.max_concurrent = max_concurrent_chunks
        self.batch_size = batch_size
        self._semaphore = asyncio.Semaphore(max_concurrent_chunks)

        # Aggregated results
        self.entity_counts: Dict[str, Dict] = defaultdict(lambda: {
            "count": 0,
            "type": None,
            "sources": set(),
            "confidence_sum": 0,
        })
        self.processed_chunks = 0
        self.total_chunks = 0
        self.errors: List[str] = []

    async def _process_chunk(
        self,
        chunk: "Chunk",
        extractor: TurboExtractor,
    ) -> Dict[str, Any]:
        """Process a single chunk."""
        async with self._semaphore:
            try:
                result = await extractor.extract_entities(chunk.text)
                return {
                    "chunk": chunk,
                    "entities": result.get("entities", []),
                }
            except Exception as e:
                self.errors.append(f"{chunk.doc_id}:{chunk.chunk_idx} - {e}")
                return {"chunk": chunk, "entities": [], "error": str(e)}

    def _aggregate_entity(self, entity: Dict, source_doc: str):
        """Aggregate entity into global counts."""
        name = entity.get("name", "").strip()
        if not name or len(name) < 2:
            return

        key = name.lower()
        self.entity_counts[key]["count"] += 1
        self.entity_counts[key]["sources"].add(source_doc)
        self.entity_counts[key]["confidence_sum"] += entity.get("confidence", 0.5)

        etype = entity.get("type", "other")
        if self.entity_counts[key]["type"] is None:
            self.entity_counts[key]["type"] = etype
            self.entity_counts[key]["canonical_name"] = name

    async def ingest_all(self, progress_callback=None) -> Dict[str, Any]:
        """Ingest entire corpus with extreme parallelism."""
        from parallel_websearch_server.batch_ingest import prepare_corpus
        from parallel_websearch_server.knowledge_graph import Entity, get_knowledge_graph, save_knowledge_graph

        start_time = datetime.utcnow()

        # Prepare corpus
        documents = prepare_corpus(self.corpus_path)
        all_chunks = []
        for doc in documents:
            all_chunks.extend(doc.chunks)

        self.total_chunks = len(all_chunks)
        logger.info(f"TURBO: Starting ingestion of {self.total_chunks} chunks from {len(documents)} documents")
        logger.info(f"TURBO: max_concurrent={self.max_concurrent}, batch_size={self.batch_size}")

        # Get extractor
        extractor = TurboExtractor(providers=self.providers)

        try:
            # Process ALL chunks in large parallel batches
            for i in range(0, len(all_chunks), self.batch_size):
                batch = all_chunks[i:i+self.batch_size]

                # Process entire batch in parallel
                tasks = [self._process_chunk(c, extractor) for c in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Aggregate results
                for result in results:
                    if isinstance(result, Exception):
                        self.errors.append(str(result))
                        continue

                    if isinstance(result, dict):
                        chunk = result.get("chunk")
                        entities = result.get("entities", [])

                        for entity in entities:
                            self._aggregate_entity(entity, chunk.doc_id if chunk else "unknown")

                        self.processed_chunks += 1

                # Progress callback
                if progress_callback:
                    progress_callback(self.processed_chunks, self.total_chunks, len(self.entity_counts))

                logger.info(f"TURBO: {self.processed_chunks}/{self.total_chunks} chunks, {len(self.entity_counts)} entities")

            # Build final entities and add to graph
            graph = get_knowledge_graph()

            final_entities = []
            for key, data in self.entity_counts.items():
                if data["count"] >= 1:
                    avg_conf = data["confidence_sum"] / data["count"] if data["count"] > 0 else 0.5

                    entity_data = {
                        "name": data.get("canonical_name", key),
                        "type": data["type"] or "other",
                        "confidence": round(avg_conf, 4),
                        "mention_count": data["count"],
                        "source_docs": list(data["sources"]),
                    }
                    final_entities.append(entity_data)

                    # Add to graph
                    entity = Entity(
                        text=entity_data["name"],
                        entity_type=entity_data["type"],
                        properties={
                            "confidence": entity_data["confidence"],
                            "mention_count": entity_data["mention_count"],
                            "source_docs": entity_data["source_docs"],
                            "extraction_method": "turbo_multi_provider",
                        }
                    )
                    graph.add_entity(entity)

            # Save graph
            save_knowledge_graph()

        finally:
            await extractor.close()

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        return {
            "documents_processed": len(documents),
            "chunks_processed": self.processed_chunks,
            "total_chunks": self.total_chunks,
            "unique_entities": len(final_entities),
            "total_mentions": sum(e["mention_count"] for e in final_entities),
            "processing_time_seconds": round(processing_time, 2),
            "chunks_per_second": round(self.processed_chunks / max(1, processing_time), 2),
            "errors": len(self.errors),
            "provider_stats": extractor.get_stats() if 'extractor' in dir() else {},
            "graph_stats": graph.get_stats(),
            "top_entities": sorted(final_entities, key=lambda x: -x["mention_count"])[:20],
        }


async def run_turbo_ingest(
    corpus_path: str = "/Users/arthurcolle/parallel_research_v2",
    providers: Optional[List[str]] = None,
    max_concurrent: int = MAX_CONCURRENT_CHUNKS,
    batch_size: int = BATCH_SIZE,
) -> Dict[str, Any]:
    """Run turbo batch ingestion on corpus."""
    ingester = TurboIngester(
        corpus_path=Path(corpus_path),
        providers=providers,
        max_concurrent_chunks=max_concurrent,
        batch_size=batch_size,
    )

    def progress(done, total, entities):
        pct = done * 100 // total if total > 0 else 0
        print(f"\rTURBO: {done}/{total} chunks ({pct}%), {entities} entities", end="", flush=True)

    result = await ingester.ingest_all(progress_callback=progress)
    print()  # newline after progress
    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

    result = asyncio.run(run_turbo_ingest())
    print(json.dumps(result, indent=2, default=str))
