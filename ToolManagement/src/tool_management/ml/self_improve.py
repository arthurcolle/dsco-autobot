"""
Stage 5: Self-improvement from production execution feedback.

Runs periodically (weekly) to:
1. Extract preference pairs from execution feedback
2. Run DPO update on the policy head
3. Validate against held-out Jina labels
4. Deploy if improved, rollback if degraded

Usage:
    python -m tool_management.ml.self_improve \
        --checkpoint ./checkpoints/dpo/final \
        --execution-db ./data/executions.db \
        --output-dir ./checkpoints/self_improve \
        --hours 168
"""

import argparse
import json
import logging
import sqlite3
from pathlib import Path
from typing import List, Dict, Tuple

import torch
from torch.utils.data import DataLoader

logger = logging.getLogger(__name__)


def extract_preferences_from_feedback(
    db_path: str,
    hours: int = 168,
) -> List[Dict]:
    """
    Extract preference pairs from execution feedback.

    Sources of signal:
    1. Tool switches within a session (A failed → B succeeded)
    2. Accepted vs rejected recommendations
    3. Latency preferences (faster tool preferred if both succeed)
    """
    conn = sqlite3.connect(db_path)
    since_ts = f"datetime('now', '-{hours} hours')"

    preferences = []

    # Signal 1: Failed → succeeded tool switches in same trace
    cursor = conn.execute(f"""
        SELECT
            a.chosen_tool AS failed_tool,
            b.chosen_tool AS succeeded_tool,
            a.context_json AS context,
            a.latency_ms AS failed_latency,
            b.latency_ms AS succeeded_latency
        FROM recommendation_feedback a
        JOIN recommendation_feedback b
            ON a.trace_id = b.trace_id
            AND a.id < b.id
        WHERE a.outcome_status = 'failed'
          AND b.outcome_status = 'completed'
          AND a.created_at >= {since_ts}
    """)

    for row in cursor:
        failed_tool, succeeded_tool, context_json, _, _ = row
        try:
            context = json.loads(context_json) if context_json else {}
        except (json.JSONDecodeError, TypeError):
            context = {}

        task = context.get("query", context.get("task", ""))
        if not task or failed_tool == succeeded_tool:
            continue

        preferences.append({
            "task": task,
            "chosen": succeeded_tool,
            "rejected": failed_tool,
            "margin": 1.0,  # strong: failure vs success
            "source": "failure_switch",
        })

    # Signal 2: Accepted recommendations with outcomes
    cursor = conn.execute(f"""
        SELECT
            context_json,
            recommendation_json,
            chosen_tool,
            accepted,
            outcome_status
        FROM recommendation_feedback
        WHERE created_at >= {since_ts}
          AND accepted IS NOT NULL
    """)

    # Group by context to find accept/reject within same query
    context_groups: Dict[str, List[Dict]] = {}
    for row in cursor:
        context_json, rec_json, chosen_tool, accepted, outcome = row
        try:
            context = json.loads(context_json) if context_json else {}
        except (json.JSONDecodeError, TypeError):
            context = {}

        task = context.get("query", "")
        if not task:
            continue

        if task not in context_groups:
            context_groups[task] = []
        context_groups[task].append({
            "tool": chosen_tool,
            "accepted": bool(accepted),
            "outcome": outcome,
        })

    for task, group in context_groups.items():
        accepted = [g for g in group if g["accepted"] and g["outcome"] == "completed"]
        rejected = [g for g in group if not g["accepted"] or g["outcome"] == "failed"]

        for a in accepted:
            for r in rejected:
                if a["tool"] != r["tool"]:
                    preferences.append({
                        "task": task,
                        "chosen": a["tool"],
                        "rejected": r["tool"],
                        "margin": 0.5,
                        "source": "accept_reject",
                    })

    # Signal 3: Latency preferences (both succeeded, one 2x faster)
    cursor = conn.execute(f"""
        SELECT
            a.chosen_tool,
            b.chosen_tool,
            a.context_json,
            a.latency_ms,
            b.latency_ms
        FROM recommendation_feedback a
        JOIN recommendation_feedback b
            ON a.session_id = b.session_id
            AND a.id < b.id
        WHERE a.outcome_status = 'completed'
          AND b.outcome_status = 'completed'
          AND a.latency_ms < b.latency_ms * 0.5
          AND a.created_at >= {since_ts}
    """)

    for row in cursor:
        fast_tool, slow_tool, context_json, fast_lat, slow_lat = row
        try:
            context = json.loads(context_json) if context_json else {}
        except (json.JSONDecodeError, TypeError):
            context = {}

        task = context.get("query", "")
        if not task or fast_tool == slow_tool:
            continue

        preferences.append({
            "task": task,
            "chosen": fast_tool,
            "rejected": slow_tool,
            "margin": 0.3,  # weak: speed preference
            "source": "latency",
        })

    conn.close()
    logger.info(
        f"Extracted {len(preferences)} preference pairs from last {hours}h of feedback "
        f"(failure_switch: {sum(1 for p in preferences if p['source'] == 'failure_switch')}, "
        f"accept_reject: {sum(1 for p in preferences if p['source'] == 'accept_reject')}, "
        f"latency: {sum(1 for p in preferences if p['source'] == 'latency')})"
    )

    return preferences


def validate_model(model, validation_path: str, device: str) -> float:
    """
    Validate model against held-out Jina labels.
    Returns accuracy (fraction of pairs where chosen > rejected).
    """
    if not Path(validation_path).exists():
        logger.warning(f"No validation file at {validation_path}, skipping validation")
        return 1.0

    pairs = []
    with open(validation_path) as f:
        for line in f:
            if line.strip():
                pairs.append(json.loads(line))

    if not pairs:
        return 1.0

    correct = 0
    total = 0

    model.eval()
    with torch.no_grad():
        for p in pairs[:500]:  # cap at 500 for speed
            chosen_text = f"Task: {p['task']}\nTool: {p['chosen_schema']}"
            rejected_text = f"Task: {p['task']}\nTool: {p['rejected_schema']}"

            inputs = model._tokenize([chosen_text, rejected_text], max_length=384)
            inputs = {k: v.to(device) for k, v in inputs.items()}

            hidden = model._get_last_hidden(inputs["input_ids"], inputs["attention_mask"])
            scores = model.policy_head(hidden).squeeze(-1)

            if scores[0] > scores[1]:
                correct += 1
            total += 1

    acc = correct / max(total, 1)
    logger.info(f"Validation accuracy: {acc:.3f} ({correct}/{total})")
    return acc


class InMemoryPreferenceDataset:
    """Simple dataset from extracted preferences."""

    def __init__(self, preferences: List[Dict]):
        self.data = preferences

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        p = self.data[idx]
        return {
            "task": p["task"],
            "chosen": p["chosen"],
            "rejected": p["rejected"],
            "margin": p["margin"],
        }


def self_improve(
    checkpoint: str,
    execution_db: str,
    output_dir: str,
    validation_path: str = "",
    hours: int = 168,
    min_preferences: int = 50,
    epochs: int = 1,
    batch_size: int = 8,
    lr: float = 1e-6,
    beta: float = 0.1,
    device: str = "auto",
    min_accuracy_threshold: float = 0.95,
):
    """
    Self-improvement loop:
    1. Extract preferences from execution feedback
    2. DPO update
    3. Validate
    4. Save if improved, rollback if degraded
    """
    from .model import ToolPolicyRewardModel

    if device == "auto":
        device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"

    # Extract preferences
    preferences = extract_preferences_from_feedback(execution_db, hours=hours)

    if len(preferences) < min_preferences:
        logger.info(
            f"Only {len(preferences)} preferences (need {min_preferences}). "
            f"Skipping self-improvement."
        )
        return None

    # Load model
    logger.info(f"Loading model from {checkpoint}")
    model = ToolPolicyRewardModel.load_full(checkpoint, device=device)

    # Pre-update validation
    pre_acc = 1.0
    if validation_path:
        pre_acc = validate_model(model, validation_path, device)
        logger.info(f"Pre-update validation accuracy: {pre_acc:.3f}")

    # DPO update
    model.train()
    dataset = InMemoryPreferenceDataset(preferences)
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    trainable = [p for p in model.parameters() if p.requires_grad]
    optimizer = torch.optim.AdamW(trainable, lr=lr, weight_decay=0.01)

    for epoch in range(epochs):
        total_loss = 0
        n = 0
        for batch in loader:
            optimizer.zero_grad()
            loss = model.forward_dpo_loss(
                tasks=batch["task"],
                chosen_tools=batch["chosen"],
                rejected_tools=batch["rejected"],
                beta=beta,
            )
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 0.5)
            optimizer.step()
            total_loss += loss.item()
            n += 1

        logger.info(f"Self-improve epoch {epoch+1}/{epochs} | Loss: {total_loss/max(n,1):.4f}")

    # Post-update validation
    post_acc = 1.0
    if validation_path:
        post_acc = validate_model(model, validation_path, device)
        logger.info(f"Post-update validation accuracy: {post_acc:.3f}")

    # Accept or reject update
    relative_threshold = pre_acc * min_accuracy_threshold
    if post_acc >= relative_threshold:
        out_path = Path(output_dir) / "latest"
        model.save_full(str(out_path))
        logger.info(
            f"Self-improvement ACCEPTED ({pre_acc:.3f} → {post_acc:.3f}). "
            f"Saved to {out_path}"
        )
        return str(out_path)
    else:
        logger.warning(
            f"Self-improvement REJECTED ({pre_acc:.3f} → {post_acc:.3f}, "
            f"threshold={relative_threshold:.3f}). Keeping old model."
        )
        return None


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Self-improvement from feedback")
    parser.add_argument("--checkpoint", required=True, help="Current model checkpoint")
    parser.add_argument("--execution-db", required=True, help="Path to executions.db")
    parser.add_argument("--output-dir", default="./checkpoints/self_improve")
    parser.add_argument("--validation-path", default="", help="Held-out validation JSONL")
    parser.add_argument("--hours", type=int, default=168, help="Lookback window")
    parser.add_argument("--min-preferences", type=int, default=50)
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--lr", type=float, default=1e-6)
    parser.add_argument("--beta", type=float, default=0.1)
    parser.add_argument("--device", default="auto")
    args = parser.parse_args()

    self_improve(
        checkpoint=args.checkpoint,
        execution_db=args.execution_db,
        output_dir=args.output_dir,
        validation_path=args.validation_path,
        hours=args.hours,
        min_preferences=args.min_preferences,
        epochs=args.epochs,
        batch_size=args.batch_size,
        lr=args.lr,
        beta=args.beta,
        device=args.device,
    )


if __name__ == "__main__":
    main()
