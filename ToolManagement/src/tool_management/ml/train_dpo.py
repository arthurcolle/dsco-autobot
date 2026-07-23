"""
Stage 4: Direct Preference Optimization on preference pairs.

Takes the SFT model and fine-tunes it with DPO to learn relative
tool preferences (chosen > rejected for a given task).

Usage:
    python -m tool_management.ml.train_dpo \
        --data-dir ./training_data \
        --sft-checkpoint ./checkpoints/sft/final \
        --output-dir ./checkpoints/dpo \
        --epochs 2 \
        --batch-size 8 \
        --lr 5e-5 \
        --beta 0.1
"""

import argparse
import json
import logging
from pathlib import Path

import torch
from torch.utils.data import Dataset, DataLoader

logger = logging.getLogger(__name__)


class PreferenceDataset(Dataset):
    """Dataset of (task, chosen_tool, rejected_tool) triples."""

    def __init__(self, jsonl_path: str, max_samples: int = 0, min_margin: float = 0.1):
        self.samples = []
        with open(jsonl_path) as f:
            for line in f:
                if line.strip():
                    d = json.loads(line)
                    if d.get("margin", 0) >= min_margin:
                        self.samples.append(d)

        if max_samples > 0:
            self.samples = self.samples[:max_samples]

        logger.info(f"Loaded {len(self.samples)} preference pairs (min_margin={min_margin})")

    def __len__(self):
        return len(self.samples)

    def __getitem__(self, idx):
        s = self.samples[idx]
        return {
            "task": s["task"],
            "chosen": s["chosen_schema"],
            "rejected": s["rejected_schema"],
            "margin": s["margin"],
        }


def train_dpo(
    data_dir: str,
    sft_checkpoint: str,
    output_dir: str,
    epochs: int = 2,
    batch_size: int = 8,
    lr: float = 5e-5,
    beta: float = 0.1,
    min_margin: float = 0.1,
    max_samples: int = 0,
    device: str = "auto",
):
    """
    DPO training: optimize policy head on preference pairs.

    The DPO loss is: -log σ(β * (policy(chosen) - policy(rejected)))
    This directly optimizes the policy without needing a reward model.
    """
    from .model import ToolPolicyRewardModel

    if device == "auto":
        device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"

    logger.info(f"DPO training on {device} | beta={beta}, lr={lr}")

    # Load SFT checkpoint
    logger.info(f"Loading SFT checkpoint from {sft_checkpoint}")
    model = ToolPolicyRewardModel.load_full(sft_checkpoint, device=device)
    model.train()

    # Load preference data
    data_path = Path(data_dir)
    dataset = PreferenceDataset(
        str(data_path / "preferences.jsonl"),
        max_samples=max_samples,
        min_margin=min_margin,
    )
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    # Only optimize LoRA + policy head
    trainable_params = [p for p in model.parameters() if p.requires_grad]
    optimizer = torch.optim.AdamW(trainable_params, lr=lr, weight_decay=0.01)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=epochs * len(loader)
    )

    for epoch in range(epochs):
        total_loss = 0.0
        total_acc = 0.0
        num_batches = 0

        for batch_idx, batch in enumerate(loader):
            optimizer.zero_grad()

            loss = model.forward_dpo_loss(
                tasks=batch["task"],
                chosen_tools=batch["chosen"],
                rejected_tools=batch["rejected"],
                beta=beta,
            )

            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            scheduler.step()

            total_loss += loss.item()
            num_batches += 1

            # Compute accuracy: does model rank chosen > rejected?
            with torch.no_grad():
                chosen_texts = [f"Task: {t}\nTool: {c}" for t, c in zip(batch["task"], batch["chosen"])]
                rejected_texts = [f"Task: {t}\nTool: {r}" for t, r in zip(batch["task"], batch["rejected"])]

                all_texts = chosen_texts + rejected_texts
                inputs = model._tokenize(all_texts, max_length=384)
                inputs = {k: v.to(device) for k, v in inputs.items()}

                hidden = model._get_last_hidden(inputs["input_ids"], inputs["attention_mask"])
                scores = model.policy_head(hidden).squeeze(-1)

                chosen_scores = scores[:len(batch["task"])]
                rejected_scores = scores[len(batch["task"]):]
                acc = (chosen_scores > rejected_scores).float().mean().item()
                total_acc += acc

            if (batch_idx + 1) % 50 == 0:
                avg_loss = total_loss / num_batches
                avg_acc = total_acc / num_batches
                logger.info(
                    f"Epoch {epoch+1}/{epochs} | Batch {batch_idx+1}/{len(loader)} | "
                    f"DPO Loss: {avg_loss:.4f} | Preference Acc: {avg_acc:.3f}"
                )

        avg_loss = total_loss / max(num_batches, 1)
        avg_acc = total_acc / max(num_batches, 1)
        logger.info(
            f"Epoch {epoch+1}/{epochs} complete | "
            f"Loss: {avg_loss:.4f} | Acc: {avg_acc:.3f}"
        )

        # Save checkpoint
        ckpt_dir = Path(output_dir) / f"epoch_{epoch+1}"
        model.save_full(str(ckpt_dir))

    # Save final
    final_dir = Path(output_dir) / "final"
    model.save_full(str(final_dir))
    logger.info(f"DPO training complete. Model saved to {final_dir}")

    return model


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="DPO training on preference pairs")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--sft-checkpoint", required=True, help="Path to SFT final checkpoint")
    parser.add_argument("--output-dir", default="./checkpoints/dpo")
    parser.add_argument("--epochs", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--lr", type=float, default=5e-5)
    parser.add_argument("--beta", type=float, default=0.1, help="DPO temperature")
    parser.add_argument("--min-margin", type=float, default=0.1, help="Min score margin for pairs")
    parser.add_argument("--max-samples", type=int, default=0)
    parser.add_argument("--device", default="auto")
    args = parser.parse_args()

    train_dpo(
        data_dir=args.data_dir,
        sft_checkpoint=args.sft_checkpoint,
        output_dir=args.output_dir,
        epochs=args.epochs,
        batch_size=args.batch_size,
        lr=args.lr,
        beta=args.beta,
        min_margin=args.min_margin,
        max_samples=args.max_samples,
        device=args.device,
    )


if __name__ == "__main__":
    main()
