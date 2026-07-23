"""Modal vLLM server for distributed knowledge graph extraction.

Uses vLLM OpenAI-compatible server for Qwen3-VL model inference.
"""

import os
import subprocess
import modal
from modal import Image, App, Secret, web_server

# ============================================================================
# Configuration
# ============================================================================

MODEL_DIR = "/model"
MODEL_ID = "Qwen/Qwen3-VL-30B-A3B-Instruct"  # 30B MoE VLM (3B active params)
GPU_CONFIG = "A100-80GB"
SCALEDOWN_WINDOW = 300

# ============================================================================
# Download model at image build time
# ============================================================================

def download_model_to_folder():
    from huggingface_hub import snapshot_download

    os.makedirs(MODEL_DIR, exist_ok=True)
    snapshot_download(
        MODEL_ID,
        local_dir=MODEL_DIR,
        ignore_patterns=["*.pt", "*.bin"],  # Using safetensors
    )


# ============================================================================
# Modal Image with vLLM
# ============================================================================

image = (
    Image.debian_slim(python_version="3.11")
    .pip_install(
        "vllm>=0.7.0",
        "fastapi[standard]>=0.115.0",
        "huggingface_hub[hf_transfer]>=0.26.0",
        "httpx>=0.25.0",
    )
    .env({"HF_HUB_ENABLE_HF_TRANSFER": "1"})
    .run_function(
        download_model_to_folder,
        timeout=60 * 30,  # 30 min for model download
    )
)

app = App("kg-extraction-server", image=image)

# ============================================================================
# vLLM OpenAI-compatible Server
# ============================================================================

@app.function(
    gpu=GPU_CONFIG,
    scaledown_window=SCALEDOWN_WINDOW,
    timeout=600,
)
@modal.concurrent(max_inputs=100)
@web_server(8000, startup_timeout=300)
def serve():
    """Start vLLM OpenAI-compatible server."""
    cmd = (
        f"python -m vllm.entrypoints.openai.api_server "
        f"--model {MODEL_DIR} "
        f"--served-model-name kg-extractor "
        f"--host 0.0.0.0 "
        f"--port 8000 "
        f"--max-model-len 8192 "
        f"--trust-remote-code "
        f"--enforce-eager"
    )
    subprocess.Popen(cmd, shell=True)


# ============================================================================
# Local Development / CLI
# ============================================================================

@app.local_entrypoint()
def main():
    """Local entrypoint for testing."""
    print(f"Deploying vLLM server for {MODEL_ID}")
    print("Use the OpenAI-compatible API at the deployed URL")


# ============================================================================
# Client for calling the deployed API
# ============================================================================

class KGExtractionClient:
    """Client for calling the Modal vLLM KG extraction API."""

    def __init__(self, base_url: str):
        """Initialize client with the Modal server URL."""
        self.base_url = base_url.rstrip("/")
        self.extraction_prompt = """You are a knowledge graph extractor. Extract entities and relationships from text.

ENTITY TYPES: person, organization, concept, product, process, protocol, language, event, location, metric, pattern, category, property

RELATION TYPES: enables, uses, implements, causes, part_of, contains, subtype_of, instance_of, supports, contradicts, authored, owns, controls, requires, ensures

Return ONLY valid JSON with this exact format:
{"entities": [{"id": "e1", "name": "exact name", "entity_type": "type"}], "relations": [{"source": "e1", "target": "e2", "relation_type": "type"}]}"""

    async def extract(self, text: str, chunk_id: str = None) -> dict:
        """Extract entities and relations from text using vLLM."""
        import httpx
        import json
        import time

        start = time.perf_counter()

        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{self.base_url}/v1/chat/completions",
                json={
                    "model": "kg-extractor",
                    "messages": [
                        {"role": "system", "content": self.extraction_prompt},
                        {"role": "user", "content": f"Extract knowledge graph from:\n\n{text[:5000]}"},
                    ],
                    "temperature": 0.1,
                    "max_tokens": 2000,
                },
            )
            response.raise_for_status()
            result = response.json()

        latency_ms = (time.perf_counter() - start) * 1000

        # Parse the response
        content = result["choices"][0]["message"]["content"]
        entities, relations = self._parse_response(content)

        return {
            "entities": entities,
            "relations": relations,
            "chunk_id": chunk_id,
            "latency_ms": latency_ms,
        }

    def _parse_response(self, response: str) -> tuple:
        """Parse JSON response into entities and relations."""
        import json
        import re

        entities = []
        relations = []

        try:
            # Handle markdown code blocks
            if "```" in response:
                match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', response)
                if match:
                    response = match.group(1)

            # Find JSON boundaries
            start = response.find('{')
            end = response.rfind('}') + 1
            if start >= 0 and end > start:
                response = response[start:end]

            data = json.loads(response)

            for e in data.get("entities", []):
                if isinstance(e, dict) and "name" in e:
                    entities.append({
                        "id": e.get("id", f"e{len(entities)}"),
                        "name": e["name"],
                        "entity_type": e.get("entity_type", e.get("type", "concept")),
                    })

            for r in data.get("relations", []):
                if isinstance(r, dict) and "source" in r and "target" in r:
                    relations.append({
                        "source": r["source"],
                        "target": r["target"],
                        "relation_type": r.get("relation_type", r.get("type", "related_to")),
                    })

        except json.JSONDecodeError as e:
            print(f"JSON parse error: {e}")

        return entities, relations

    async def extract_batch(self, chunks: list) -> list:
        """Extract from multiple chunks concurrently."""
        import asyncio
        tasks = [self.extract(text, chunk_id) for text, chunk_id in chunks]
        return await asyncio.gather(*tasks)

    async def health(self) -> dict:
        """Check server health."""
        import httpx
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"{self.base_url}/health")
            return {"status": "ok", "model": MODEL_ID}
