"""AI-powered entity analysis using OpenRouter/Claude Haiku.

Analyzes entities and relationships to improve extraction quality.
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

# Model configs - using fast, cheap models for analysis
MODELS = {
    "haiku": "anthropic/claude-3.5-haiku",  # Fast and cheap
    "flash": "google/gemini-2.0-flash-001",  # Very fast
    "deepseek": "deepseek/deepseek-chat",  # Ultra cheap
}


class EntityAnalyzer:
    """Analyze and improve entity extraction using AI."""

    def __init__(self, model: str = "haiku"):
        self.model = MODELS.get(model, MODELS["haiku"])
        self.client = httpx.AsyncClient(timeout=30.0)

    async def analyze_entity(self, entity: Dict[str, Any], context: str = "") -> Dict[str, Any]:
        """Analyze a single entity and suggest improvements."""
        prompt = f"""Analyze this extracted entity and suggest improvements:

Entity: {entity['text']}
Type: {entity['type']}
Mentions: {entity.get('mentions', 1)}
Aliases: {entity.get('aliases', [])}

Context (if available): {context[:500] if context else 'N/A'}

Respond in JSON format:
{{
    "is_valid": true/false,
    "suggested_type": "person|organization|location|product|event|concept|other",
    "normalized_name": "cleaned up name",
    "aliases": ["other names"],
    "confidence": 0.0-1.0,
    "reasoning": "brief explanation"
}}"""

        return await self._call_llm(prompt)

    async def find_relationships(
        self,
        entity: Dict[str, Any],
        other_entities: List[Dict[str, Any]],
        document_text: str = ""
    ) -> List[Dict[str, Any]]:
        """Find meaningful relationships between entities."""
        other_names = [e['text'] for e in other_entities[:20]]  # Limit for context

        prompt = f"""Analyze relationships between these entities from a document:

Main Entity: {entity['text']} (type: {entity['type']})

Other Entities: {', '.join(other_names)}

Document excerpt: {document_text[:1000] if document_text else 'N/A'}

Find meaningful relationships (not just co-occurrence).
Respond in JSON format - list of relationships:
[
    {{
        "source": "entity1",
        "target": "entity2",
        "relation": "works_for|founded|located_in|partnered_with|created|manages|part_of|etc",
        "confidence": 0.0-1.0,
        "evidence": "brief quote or reasoning"
    }}
]

Only include high-confidence relationships. Return empty list [] if none found."""

        result = await self._call_llm(prompt)
        if isinstance(result, list):
            return result
        return result.get("relationships", []) if isinstance(result, dict) else []

    async def classify_document(self, text: str) -> Dict[str, Any]:
        """Classify document and extract key themes."""
        prompt = f"""Analyze this document and extract key information:

Document (first 2000 chars):
{text[:2000]}

Respond in JSON:
{{
    "topics": ["list of main topics"],
    "domain": "technology|science|business|politics|other",
    "key_entities": ["most important entities mentioned"],
    "summary": "1-2 sentence summary",
    "sentiment": "positive|negative|neutral"
}}"""

        return await self._call_llm(prompt)

    async def suggest_entity_merges(
        self,
        entities: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Suggest entities that should be merged (same entity, different names)."""
        entity_list = "\n".join([f"- {e['text']} (type: {e['type']}, id: {e['id']})"
                                  for e in entities[:50]])

        prompt = f"""These entities were extracted from documents.
Find any that refer to the SAME real-world entity and should be merged:

{entity_list}

Respond in JSON - list of merge suggestions:
[
    {{
        "primary_id": "id to keep",
        "merge_ids": ["ids to merge into primary"],
        "canonical_name": "best name to use",
        "confidence": 0.0-1.0,
        "reason": "why these are the same"
    }}
]

Only suggest merges you're confident about. Return [] if none."""

        result = await self._call_llm(prompt)
        return result if isinstance(result, list) else []

    async def _call_llm(self, prompt: str) -> Any:
        """Call OpenRouter API."""
        if not OPENROUTER_API_KEY:
            logger.warning("OPENROUTER_API_KEY not set, returning empty result")
            return {}

        headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://parallel.ai",
            "X-Title": "Knowledge Graph Analyzer"
        }

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "You are an expert at analyzing entities and relationships from text. Always respond with valid JSON only, no markdown."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.3,
            "max_tokens": 2000,
        }

        try:
            response = await self.client.post(
                f"{OPENROUTER_BASE_URL}/chat/completions",
                headers=headers,
                json=payload
            )
            response.raise_for_status()
            data = response.json()

            content = data["choices"][0]["message"]["content"]
            # Parse JSON from response
            content = content.strip()
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            return json.loads(content)

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse LLM response as JSON: {e}")
            return {}
        except Exception as e:
            logger.error(f"OpenRouter API error: {e}")
            return {}

    async def close(self):
        await self.client.aclose()


# Singleton instance
_analyzer: Optional[EntityAnalyzer] = None


def get_analyzer(model: str = "haiku") -> EntityAnalyzer:
    """Get or create the global analyzer."""
    global _analyzer
    if _analyzer is None:
        _analyzer = EntityAnalyzer(model)
    return _analyzer


async def analyze_and_improve_entities(
    entities: List[Dict[str, Any]],
    document_text: str = "",
    model: str = "haiku"
) -> Dict[str, Any]:
    """High-level function to analyze and improve a batch of entities."""
    analyzer = get_analyzer(model)

    results = {
        "analyzed": 0,
        "improvements": [],
        "relationships": [],
        "merge_suggestions": [],
    }

    # Analyze entities in parallel (limit concurrency)
    semaphore = asyncio.Semaphore(5)

    async def analyze_one(entity):
        async with semaphore:
            return await analyzer.analyze_entity(entity, document_text)

    analyses = await asyncio.gather(*[analyze_one(e) for e in entities[:20]])

    for entity, analysis in zip(entities[:20], analyses):
        if analysis:
            results["analyzed"] += 1
            if analysis.get("suggested_type") != entity.get("type"):
                results["improvements"].append({
                    "entity_id": entity["id"],
                    "current_type": entity["type"],
                    "suggested_type": analysis.get("suggested_type"),
                    "confidence": analysis.get("confidence", 0),
                })

    # Find relationships
    if len(entities) >= 2:
        main_entity = entities[0]
        rels = await analyzer.find_relationships(main_entity, entities[1:], document_text)
        results["relationships"] = rels

    # Suggest merges
    if len(entities) >= 3:
        merges = await analyzer.suggest_entity_merges(entities)
        results["merge_suggestions"] = merges

    return results
