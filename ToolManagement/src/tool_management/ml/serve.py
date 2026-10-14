"""
Stage 6: Modal inference server for the trained model.

Deploys the LoRA-adapted model on a T4 GPU ($0.10/hr).
Provides HTTP endpoints for tool scoring, output quality, and intent classification.

Deploy:
    modal deploy tool_management/ml/serve.py
"""

import json
import os
from pathlib import Path
from typing import List, Dict, Any, Optional

# FastAPI app for local testing
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field


# ---------- Request / Response schemas ----------

class ScoreToolsRequest(BaseModel):
    task: str
    candidates: List[Dict[str, str]] = Field(..., min_length=1, max_length=200)
    top_k: int = Field(default=5, ge=1, le=200)


class ScoreToolsResponse(BaseModel):
    rankings: List[Dict[str, Any]]  # [{tool, score, rank}]
    selected: str
    confidence: float
    intent: str


class EvaluateOutputRequest(BaseModel):
    task: str
    output: str  # stringified tool output


class EvaluateOutputResponse(BaseModel):
    quality_score: float
    above_threshold: bool


class ClassifyIntentRequest(BaseModel):
    task: str


class ClassifyIntentResponse(BaseModel):
    intent: str
    confidence: float
    all_scores: Dict[str, float]


# ---------- App ----------

app = FastAPI(title="DSCO Tool Policy Model", version="1.0.0")

# Global model (loaded on startup)
_model = None
_device = "cpu"


def get_model():
    global _model, _device
    if _model is None:
        import torch
        from .model import ToolPolicyRewardModel

        checkpoint_path = os.getenv("MODEL_CHECKPOINT", "./checkpoints/dpo/final")
        _device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"

        _model = ToolPolicyRewardModel.load_full(checkpoint_path, device=_device)
        _model.eval()
        print(f"Model loaded from {checkpoint_path} on {_device}")

    return _model


@app.on_event("startup")
async def startup():
    get_model()


@app.get("/health")
async def health():
    return {"status": "ok", "model_loaded": _model is not None, "device": _device}


@app.post("/score_tools", response_model=ScoreToolsResponse)
async def score_tools(request: ScoreToolsRequest):
    model = get_model()

    tool_schemas = [
        f"{c['name']}: {c.get('description', '')[:200]}"
        for c in request.candidates
    ]

    # Score tools
    import torch
    with torch.no_grad():
        result = model.score_tools(
            tasks=[request.task],
            tool_schemas=[tool_schemas],
        )

    scores = result.tool_scores[0].cpu().tolist()
    selected_idx = result.selected_index[0].item()
    confidence = result.confidence[0].item()

    # Classify intent
    with torch.no_grad():
        intent_result = model.classify_intent(tasks=[request.task])

    intent = intent_result.label[0]

    # Build rankings
    tool_scores = [
        {"tool": request.candidates[i]["name"], "score": scores[i], "rank": 0,
         "candidate_index": i, "tool_id": request.candidates[i].get("tool_id")}
        for i in range(len(request.candidates))
    ]
    tool_scores.sort(key=lambda x: x["score"], reverse=True)
    for i, ts in enumerate(tool_scores):
        ts["rank"] = i + 1

    return ScoreToolsResponse(
        rankings=tool_scores[:request.top_k],
        selected=request.candidates[selected_idx]["name"],
        confidence=confidence,
        intent=intent,
    )


@app.post("/evaluate_output", response_model=EvaluateOutputResponse)
async def evaluate_output(request: EvaluateOutputRequest):
    model = get_model()

    import torch
    with torch.no_grad():
        result = model.score_output(
            tasks=[request.task],
            outputs=[request.output],
        )

    quality = result.quality_score[0].item()

    return EvaluateOutputResponse(
        quality_score=quality,
        above_threshold=quality >= 0.5,
    )


@app.post("/classify_intent", response_model=ClassifyIntentResponse)
async def classify_intent(request: ClassifyIntentRequest):
    model = get_model()

    import torch
    from .model import INTENT_LABELS

    with torch.no_grad():
        result = model.classify_intent(tasks=[request.task])

    logits = result.logits[0].cpu()
    probs = torch.softmax(logits, dim=0)
    all_scores = {label: round(prob.item(), 4) for label, prob in zip(INTENT_LABELS, probs)}

    return ClassifyIntentResponse(
        intent=result.label[0],
        confidence=result.confidence[0].item(),
        all_scores=all_scores,
    )


# ---------- Modal deployment ----------

def create_modal_app():
    """Create Modal app for GPU deployment."""
    import modal

    modal_app = modal.App("dsco-tool-policy")

    image = (
        modal.Image.debian_slim(python_version="3.11")
        .pip_install(
            "torch>=2.1.0",
            "transformers>=4.36.0",
            "peft>=0.7.0",
            "accelerate>=0.25.0",
            "fastapi[standard]>=0.115.0",
            "uvicorn>=0.30.0",
            "numpy>=1.24.0",
        )
    )

    model_volume = modal.Volume.from_name("dsco-tool-policy-model", create_if_missing=True)

    @modal_app.function(
        image=image,
        gpu="T4",
        volumes={"/model": model_volume},
        timeout=300,
        container_idle_timeout=300,
        min_containers=0,
        max_containers=1,
    )
    @modal.asgi_app()
    def serve():
        import os
        os.environ["MODEL_CHECKPOINT"] = "/model/latest"
        # Import and return the FastAPI app from this module
        from tool_management.ml.serve import app as fastapi_app
        return fastapi_app

    return modal_app


# For direct local testing
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8100)
