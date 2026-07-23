"""
Stage 3: Supervised Fine-Tuning on Jina reranker scores.

Trains the policy head to replicate Jina's relevance scoring,
and the intent head on Jina's classification labels.

Usage:
    python -m tool_management.ml.train_sft \
        --data-dir ./training_data \
        --output-dir ./checkpoints/sft \
        --model qwen2.5-1.5b \
        --epochs 3 \
        --batch-size 16 \
        --lr 2e-4
"""

import argparse
import json
import logging
import random
from pathlib import Path
from typing import List, Dict

import torch
from torch.utils.data import Dataset, DataLoader

logger = logging.getLogger(__name__)


class ScoredPairDataset(Dataset):
    """Dataset of (task, tool, score) triples for policy head SFT."""

    def __init__(self, jsonl_path: str, max_samples: int = 0):
        self.samples = []
        with open(jsonl_path) as f:
            for line in f:
                if line.strip():
                    self.samples.append(json.loads(line))
        if max_samples > 0:
            self.samples = self.samples[:max_samples]
        logger.info(f"Loaded {len(self.samples)} scored pairs")

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        s = self.samples[idx]
        return {
            "text": f"Task: {s['task']}\nTool: {s['tool_schema']}",
            "score": s["jina_relevance_score"],
        }


class IntentDataset(Dataset):
    """Dataset of (task, intent) pairs for intent head SFT."""

    def __init__(self, jsonl_path: str, max_samples: int = 0):
        from .model import INTENT_LABELS
        self.intent_labels = INTENT_LABELS
        self.samples = []
        with open(jsonl_path) as f:
            for line in f:
                if line.strip():
                    d = json.loads(line)
                    if d["intent"] in self.intent_labels:
                        self.samples.append(d)
        if max_samples > 0:
            self.samples = self.samples[:max_samples]
        logger.info(f"Loaded {len(self.samples)} intent labels")

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        s = self.samples[idx]
        return {
            "task": s["task"],
            "intent": s["intent"],
        }


def train_sft(
    data_dir: str,
    output_dir: str,
    model_name: str = "qwen2.5-1.5b",
    epochs: int = 3,
    batch_size: int = 16,
    lr: float = 2e-4,
    max_samples: int = 0,
    device: str = "auto",
    policy_weight: float = 1.0,
    intent_weight: float = 0.5,
):
    """
    SFT training loop: train policy + intent heads on Jina labels.
    """
    from .model import ToolPolicyRewardModel

    if device == "auto":
        device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"

    logger.info(f"Training on device: {device}")
    logger.info(f"Model: {model_name}, epochs: {epochs}, batch_size: {batch_size}, lr: {lr}")

    # Load data
    data_path = Path(data_dir)
    scored_dataset = ScoredPairDataset(str(data_path / "scored_pairs.jsonl"), max_samples)
    intent_dataset = IntentDataset(str(data_path / "intent_labels.jsonl"), max_samples)

    scored_loader = DataLoader(scored_dataset, batch_size=batch_size, shuffle=True)
    intent_loader = DataLoader(intent_dataset, batch_size=batch_size, shuffle=True)

    # Initialize model
    model = ToolPolicyRewardModel(
        model_name=model_name,
        use_lora=True,
        freeze_backbone=False,
    )
    model.to(device)
    model.train()

    # Optimizer: different LR for backbone (LoRA) vs heads
    lora_params = [p for n, p in model.backbone.named_parameters() if p.requires_grad]
    head_params = (
        list(model.policy_head.parameters()) +
        list(model.reward_head.parameters()) +
        list(model.intent_head.parameters())
    )

    optimizer = torch.optim.AdamW([
        {"params": lora_params, "lr": lr * 0.1},  # LoRA: lower LR
        {"params": head_params, "lr": lr},          # Heads: full LR
    ], weight_decay=0.01)

    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=epochs * len(scored_loader)
    )

    # Training loop
    for epoch in range(epochs):
        total_policy_loss = 0.0
        total_intent_loss = 0.0
        num_policy_batches = 0
        num_intent_batches = 0

        # Alternate between policy and intent batches
        intent_iter = iter(intent_loader)

        for batch_idx, policy_batch in enumerate(scored_loader):
            optimizer.zero_grad()

            # Policy head loss
            p_loss = model.forward_policy_loss(
                task_tool_texts=policy_batch["text"],
                target_scores=policy_batch["score"].float(),
            )
            loss = policy_weight * p_loss
            total_policy_loss += p_loss.item()
            num_policy_batches += 1

            # Intent head loss (interleaved)
            try:
                intent_batch = next(intent_iter)
            except StopIteration:
                intent_iter = iter(intent_loader)
                intent_batch = next(intent_iter)

            i_loss = model.forward_intent_loss(
                tasks=intent_batch["task"],
                target_intents=intent_batch["intent"],
            )
            loss += intent_weight * i_loss
            total_intent_loss += i_loss.item()
            num_intent_batches += 1

            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            scheduler.step()

            if (batch_idx + 1) % 100 == 0:
                avg_p = total_policy_loss / num_policy_batches
                avg_i = total_intent_loss / num_intent_batches
                current_lr = scheduler.get_last_lr()[0]
                logger.info(
                    f"Epoch {epoch+1}/{epochs} | Batch {batch_idx+1}/{len(scored_loader)} | "
                    f"Policy Loss: {avg_p:.4f} | Intent Loss: {avg_i:.4f} | LR: {current_lr:.6f}"
                )

        avg_p = total_policy_loss / max(num_policy_batches, 1)
        avg_i = total_intent_loss / max(num_intent_batches, 1)
        logger.info(f"Epoch {epoch+1}/{epochs} complete | Policy: {avg_p:.4f} | Intent: {avg_i:.4f}")

        # Save checkpoint
        ckpt_dir = Path(output_dir) / f"epoch_{epoch+1}"
        model.save_full(str(ckpt_dir))
        logger.info(f"Saved checkpoint to {ckpt_dir}")

    # Save final model
    final_dir = Path(output_dir) / "final"
    model.save_full(str(final_dir))
    logger.info(f"Training complete. Final model saved to {final_dir}")

    return model


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="SFT training on Jina labels")
    parser.add_argument("--data-dir", required=True, help="Training data directory")
    parser.add_argument("--output-dir", default="./checkpoints/sft", help="Checkpoint output")
    parser.add_argument("--model", default="qwen2.5-1.5b", help="Base model name")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--lr", type=float, default=2e-4)
    parser.add_argument("--max-samples", type=int, default=0)
    parser.add_argument("--device", default="auto")
    args = parser.parse_args()

    train_sft(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        model_name=args.model,
        epochs=args.epochs,
        batch_size=args.batch_size,
        lr=args.lr,
        max_samples=args.max_samples,
        device=args.device,
    )


if __name__ == "__main__":
    main()
