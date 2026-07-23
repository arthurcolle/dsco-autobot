#!/usr/bin/env bash
#
# ML Pipeline: Bootstrap → SFT → DPO → Serve
#
# Full pipeline to train the tool policy model from scratch.
# Requires: JINA_API_KEY env var, ~$200 Jina budget, GPU recommended.
#
# Usage:
#   ./scripts/ml_pipeline.sh bootstrap   # Stage 1-2: Generate training data from Jina
#   ./scripts/ml_pipeline.sh train       # Stage 3-4: SFT + DPO training
#   ./scripts/ml_pipeline.sh serve       # Stage 6: Start inference server
#   ./scripts/ml_pipeline.sh improve     # Stage 5: Self-improvement from feedback
#   ./scripts/ml_pipeline.sh all         # Full pipeline
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

DATA_DIR="${DATA_DIR:-./training_data}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-./checkpoints}"
MODEL="${MODEL:-qwen2.5-1.5b}"
DEVICE="${DEVICE:-auto}"
EXECUTION_DB="${EXECUTION_DB:-./data/executions.db}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[ML]${NC} $*"; }
warn() { echo -e "${YELLOW}[ML]${NC} $*"; }
err() { echo -e "${RED}[ML]${NC} $*" >&2; }

check_deps() {
    python3 -c "import torch; print(f'PyTorch {torch.__version__} on {torch.device(\"cuda\" if torch.cuda.is_available() else \"mps\" if torch.backends.mps.is_available() else \"cpu\")}')" 2>/dev/null || {
        err "PyTorch not found. Install: pip install torch transformers peft accelerate"
        exit 1
    }
}

stage_bootstrap() {
    log "Stage 1-2: Bootstrapping training data from Jina..."

    if [ -z "${JINA_API_KEY:-}" ]; then
        err "JINA_API_KEY not set. Export it first."
        exit 1
    fi

    mkdir -p "$DATA_DIR"

    python3 -m tool_management.ml.bootstrap_data \
        --output-dir "$DATA_DIR" \
        --num-tasks 5000 \
        --batch-size 50 \
        --device "$DEVICE"

    log "Training data saved to $DATA_DIR"
    log "Files:"
    ls -lh "$DATA_DIR"/*.jsonl 2>/dev/null || warn "No JSONL files found"
}

stage_sft() {
    log "Stage 3: Supervised Fine-Tuning on Jina labels..."
    check_deps

    if [ ! -f "$DATA_DIR/scored_pairs.jsonl" ]; then
        err "No training data found at $DATA_DIR/scored_pairs.jsonl"
        err "Run: $0 bootstrap"
        exit 1
    fi

    python3 -m tool_management.ml.train_sft \
        --data-dir "$DATA_DIR" \
        --output-dir "$CHECKPOINT_DIR/sft" \
        --model "$MODEL" \
        --epochs 3 \
        --batch-size 16 \
        --lr 2e-4 \
        --device "$DEVICE"

    log "SFT checkpoint saved to $CHECKPOINT_DIR/sft/final"
}

stage_dpo() {
    log "Stage 4: DPO training on preference pairs..."
    check_deps

    if [ ! -d "$CHECKPOINT_DIR/sft/final" ]; then
        err "No SFT checkpoint at $CHECKPOINT_DIR/sft/final"
        err "Run: $0 train-sft"
        exit 1
    fi

    if [ ! -f "$DATA_DIR/preferences.jsonl" ]; then
        err "No preference data at $DATA_DIR/preferences.jsonl"
        exit 1
    fi

    python3 -m tool_management.ml.train_dpo \
        --data-dir "$DATA_DIR" \
        --sft-checkpoint "$CHECKPOINT_DIR/sft/final" \
        --output-dir "$CHECKPOINT_DIR/dpo" \
        --epochs 2 \
        --batch-size 8 \
        --lr 5e-5 \
        --beta 0.1 \
        --device "$DEVICE"

    log "DPO checkpoint saved to $CHECKPOINT_DIR/dpo/final"
}

stage_serve() {
    log "Stage 6: Starting inference server..."

    export MODEL_CHECKPOINT="${CHECKPOINT_DIR}/dpo/final"

    if [ ! -d "$MODEL_CHECKPOINT" ]; then
        warn "No DPO checkpoint found. Trying SFT checkpoint..."
        export MODEL_CHECKPOINT="${CHECKPOINT_DIR}/sft/final"
    fi

    if [ ! -d "$MODEL_CHECKPOINT" ]; then
        err "No model checkpoint found. Run training first."
        exit 1
    fi

    log "Serving from $MODEL_CHECKPOINT"
    python3 -m uvicorn tool_management.ml.serve:app \
        --host 0.0.0.0 \
        --port 8100 \
        --workers 1
}

stage_improve() {
    log "Stage 5: Self-improvement from production feedback..."
    check_deps

    CURRENT_CHECKPOINT="${CHECKPOINT_DIR}/dpo/final"
    if [ ! -d "$CURRENT_CHECKPOINT" ]; then
        CURRENT_CHECKPOINT="${CHECKPOINT_DIR}/sft/final"
    fi

    if [ ! -d "$CURRENT_CHECKPOINT" ]; then
        err "No model checkpoint found."
        exit 1
    fi

    if [ ! -f "$EXECUTION_DB" ]; then
        err "No execution database at $EXECUTION_DB"
        exit 1
    fi

    python3 -m tool_management.ml.self_improve \
        --checkpoint "$CURRENT_CHECKPOINT" \
        --execution-db "$EXECUTION_DB" \
        --output-dir "$CHECKPOINT_DIR/self_improve" \
        --hours 168 \
        --min-preferences 50 \
        --device "$DEVICE"

    # If improvement was accepted, update the latest symlink
    if [ -d "$CHECKPOINT_DIR/self_improve/latest" ]; then
        log "Self-improvement accepted. Updating active checkpoint."
        ln -sfn "$CHECKPOINT_DIR/self_improve/latest" "$CHECKPOINT_DIR/active"
    fi
}

stage_deploy_modal() {
    log "Deploying to Modal (T4 GPU)..."

    if ! command -v modal &>/dev/null; then
        err "Modal CLI not found. Install: pip install modal"
        exit 1
    fi

    # Upload checkpoint to Modal volume
    CHECKPOINT="${CHECKPOINT_DIR}/dpo/final"
    if [ ! -d "$CHECKPOINT" ]; then
        CHECKPOINT="${CHECKPOINT_DIR}/sft/final"
    fi

    log "Uploading checkpoint from $CHECKPOINT"
    modal volume put dsco-tool-policy-model "$CHECKPOINT" /latest/

    log "Deploying serve.py to Modal..."
    modal deploy src/tool_management/ml/serve.py

    log "Deployment complete. Model running on Modal T4."
}

usage() {
    echo "Usage: $0 <command>"
    echo ""
    echo "Commands:"
    echo "  bootstrap     Generate training data from Jina (~\$200)"
    echo "  train-sft     SFT on Jina labels (Stage 3)"
    echo "  train-dpo     DPO on preferences (Stage 4)"
    echo "  train         SFT + DPO (Stage 3+4)"
    echo "  serve         Start local inference server (Stage 6)"
    echo "  improve       Self-improvement from feedback (Stage 5)"
    echo "  deploy        Deploy to Modal T4 GPU"
    echo "  all           Full pipeline: bootstrap → train → serve"
    echo ""
    echo "Environment:"
    echo "  JINA_API_KEY    Required for bootstrap"
    echo "  DATA_DIR        Training data dir (default: ./training_data)"
    echo "  CHECKPOINT_DIR  Checkpoint dir (default: ./checkpoints)"
    echo "  MODEL           Base model (default: qwen2.5-1.5b)"
    echo "  DEVICE          auto|cuda|mps|cpu (default: auto)"
    echo "  EXECUTION_DB    Execution database (default: ./data/executions.db)"
}

case "${1:-}" in
    bootstrap)
        stage_bootstrap
        ;;
    train-sft)
        stage_sft
        ;;
    train-dpo)
        stage_dpo
        ;;
    train)
        stage_sft
        stage_dpo
        ;;
    serve)
        stage_serve
        ;;
    improve)
        stage_improve
        ;;
    deploy)
        stage_deploy_modal
        ;;
    all)
        stage_bootstrap
        stage_sft
        stage_dpo
        log "Pipeline complete. Start serving with: $0 serve"
        ;;
    *)
        usage
        exit 1
        ;;
esac
