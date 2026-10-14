"""
Stage 2: Unified Policy + Reward model architecture.

Single LLM backbone (Qwen 2.5) with three heads:
  - Policy head:  scores (task, tool) pairs → tool selection
  - Reward head:  scores (task, output) pairs → quality evaluation
  - Intent head:  classifies task intent → 8 classes

The same forward pass through the transformer backbone is reused
across all heads. LoRA keeps fine-tuning efficient (~6hrs on A10G).
"""

import json
import torch
import torch.nn as nn
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass


# Default model configs
SUPPORTED_MODELS = {
    "qwen2.5-0.5b": "Qwen/Qwen2.5-0.5B",
    "qwen2.5-1.5b": "Qwen/Qwen2.5-1.5B",
    "qwen2.5-3b": "Qwen/Qwen2.5-3B",
    "qwen2.5-7b": "Qwen/Qwen2.5-7B",
    "smollm2-1.7b": "HuggingFaceTB/SmolLM2-1.7B",
    "phi-3.5-mini": "microsoft/Phi-3.5-mini-instruct",
}

from .intent_labels import INTENT_LABELS

DEFAULT_MODEL = "qwen2.5-1.5b"


@dataclass
class PolicyOutput:
    """Output from the policy head."""
    tool_scores: torch.Tensor       # (batch, num_candidates) relevance scores
    selected_index: torch.Tensor    # (batch,) index of selected tool
    confidence: torch.Tensor        # (batch,) confidence of selection


@dataclass
class RewardOutput:
    """Output from the reward head."""
    quality_score: torch.Tensor     # (batch,) quality score 0-1
    calibrated: bool = False        # whether Platt scaling has been applied


@dataclass
class IntentOutput:
    """Output from the intent head."""
    logits: torch.Tensor            # (batch, 8) raw logits
    label: List[str]                # predicted label per batch item
    confidence: torch.Tensor        # (batch,) softmax confidence


class ToolPolicyRewardModel(nn.Module):
    """
    Unified model for tool selection (policy), quality evaluation (reward),
    and intent classification.

    Architecture:
        [Backbone: Qwen 2.5 / SmolLM2 / Phi-3.5]
            ├── Policy Head  (Linear → GELU → Linear → scalar)
            ├── Reward Head  (Linear → GELU → Linear → scalar)
            └── Intent Head  (Linear → GELU → Linear → 8 classes)

    The backbone is shared. Only the heads + LoRA adapters are trained.
    """

    def __init__(
        self,
        model_name: str = DEFAULT_MODEL,
        num_intents: int = 8,
        freeze_backbone: bool = True,
        use_lora: bool = True,
        lora_r: int = 16,
        lora_alpha: int = 32,
        lora_dropout: float = 0.05,
    ):
        super().__init__()

        self.model_name = model_name
        hf_name = SUPPORTED_MODELS.get(model_name, model_name)

        # Lazy imports to avoid dependency issues
        from transformers import AutoModelForCausalLM, AutoTokenizer, AutoConfig

        self.tokenizer = AutoTokenizer.from_pretrained(hf_name, trust_remote_code=True)
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token

        config = AutoConfig.from_pretrained(hf_name, trust_remote_code=True)
        self.hidden_size = config.hidden_size

        self.backbone = AutoModelForCausalLM.from_pretrained(
            hf_name,
            torch_dtype=torch.bfloat16,
            trust_remote_code=True,
            output_hidden_states=True,
        )

        # Apply LoRA
        if use_lora:
            self._apply_lora(lora_r, lora_alpha, lora_dropout)

        if freeze_backbone and not use_lora:
            for param in self.backbone.parameters():
                param.requires_grad = False

        # --- Scoring Heads ---

        # Policy head: (task, tool) → relevance score
        self.policy_head = nn.Sequential(
            nn.Linear(self.hidden_size, 512),
            nn.GELU(),
            nn.Dropout(0.1),
            nn.Linear(512, 1),
        )

        # Reward head: (task, output) → quality score
        self.reward_head = nn.Sequential(
            nn.Linear(self.hidden_size, 512),
            nn.GELU(),
            nn.Dropout(0.1),
            nn.Linear(512, 1),
            nn.Sigmoid(),  # bound to [0, 1]
        )

        # Intent head: task → intent class
        self.intent_head = nn.Sequential(
            nn.Linear(self.hidden_size, 256),
            nn.GELU(),
            nn.Dropout(0.1),
            nn.Linear(256, num_intents),
        )

        # Platt scaling parameters (calibrated after training)
        self.platt_a = nn.Parameter(torch.tensor(1.0), requires_grad=False)
        self.platt_b = nn.Parameter(torch.tensor(0.0), requires_grad=False)

    def _apply_lora(self, r: int, alpha: int, dropout: float):
        """Apply LoRA adapters to the backbone."""
        from peft import LoraConfig, get_peft_model, TaskType

        lora_config = LoraConfig(
            task_type=TaskType.CAUSAL_LM,
            r=r,
            lora_alpha=alpha,
            lora_dropout=dropout,
            target_modules=["q_proj", "v_proj", "k_proj", "o_proj",
                            "gate_proj", "up_proj", "down_proj"],
            bias="none",
        )
        self.backbone = get_peft_model(self.backbone, lora_config)
        trainable = sum(p.numel() for p in self.backbone.parameters() if p.requires_grad)
        total = sum(p.numel() for p in self.backbone.parameters())
        print(f"LoRA: {trainable:,} trainable / {total:,} total ({100*trainable/total:.2f}%)")

    def _get_last_hidden(self, input_ids: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
        """Get the last hidden state from the backbone (last non-pad token)."""
        outputs = self.backbone(
            input_ids=input_ids,
            attention_mask=attention_mask,
            output_hidden_states=True,
        )
        hidden_states = outputs.hidden_states[-1]  # (batch, seq_len, hidden)

        # Get last non-padding token position
        seq_lengths = attention_mask.sum(dim=1) - 1  # (batch,)
        batch_idx = torch.arange(hidden_states.size(0), device=hidden_states.device)
        last_hidden = hidden_states[batch_idx, seq_lengths]  # (batch, hidden)

        return last_hidden.float()  # cast from bf16 for head computation

    def _tokenize(self, texts: List[str], max_length: int = 512) -> Dict[str, torch.Tensor]:
        """Tokenize texts with padding."""
        return self.tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=max_length,
            return_tensors="pt",
        )

    def score_tools(
        self,
        tasks: List[str],
        tool_schemas: List[List[str]],
        max_length: int = 384,
    ) -> PolicyOutput:
        """
        Score (task, tool) pairs for tool selection.

        Args:
            tasks: List of task descriptions (batch_size,)
            tool_schemas: List of candidate tool schemas per task (batch_size, num_candidates)

        Returns:
            PolicyOutput with scores, selected index, confidence
        """
        device = next(self.parameters()).device
        batch_size = len(tasks)
        num_candidates = max(len(tools) for tools in tool_schemas)

        all_scores = torch.zeros(batch_size, num_candidates, device=device)

        for i, (task, tools) in enumerate(zip(tasks, tool_schemas)):
            if not tools:
                continue

            # Construct (task, tool) pairs
            pair_texts = [f"Task: {task}\nTool: {tool}" for tool in tools]
            inputs = self._tokenize(pair_texts, max_length=max_length)
            inputs = {k: v.to(device) for k, v in inputs.items()}

            with torch.no_grad() if not self.training else torch.enable_grad():
                hidden = self._get_last_hidden(inputs["input_ids"], inputs["attention_mask"])
                scores = self.policy_head(hidden).squeeze(-1)  # (num_tools,)
                all_scores[i, :len(tools)] = scores

        # Selection
        selected = all_scores.argmax(dim=1)
        # Confidence: softmax then take max prob
        probs = torch.softmax(all_scores, dim=1)
        confidence = probs.max(dim=1).values

        return PolicyOutput(
            tool_scores=all_scores,
            selected_index=selected,
            confidence=confidence,
        )

    def score_output(
        self,
        tasks: List[str],
        outputs: List[str],
        max_length: int = 512,
    ) -> RewardOutput:
        """
        Score (task, output) pairs for quality evaluation.

        Args:
            tasks: Task descriptions
            outputs: Tool execution outputs (stringified)

        Returns:
            RewardOutput with quality scores
        """
        device = next(self.parameters()).device

        pair_texts = [
            f"Task: {task}\nOutput: {output[:1500]}"
            for task, output in zip(tasks, outputs)
        ]
        inputs = self._tokenize(pair_texts, max_length=max_length)
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad() if not self.training else torch.enable_grad():
            hidden = self._get_last_hidden(inputs["input_ids"], inputs["attention_mask"])
            quality = self.reward_head(hidden).squeeze(-1)  # (batch,)

        return RewardOutput(quality_score=quality)

    def classify_intent(
        self,
        tasks: List[str],
        max_length: int = 256,
    ) -> IntentOutput:
        """
        Classify task intent.

        Args:
            tasks: Task descriptions

        Returns:
            IntentOutput with logits, labels, confidence
        """
        device = next(self.parameters()).device

        inputs = self._tokenize(tasks, max_length=max_length)
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad() if not self.training else torch.enable_grad():
            hidden = self._get_last_hidden(inputs["input_ids"], inputs["attention_mask"])
            logits = self.intent_head(hidden)  # (batch, 8)

        probs = torch.softmax(logits, dim=1)
        confidence, indices = probs.max(dim=1)
        labels = [INTENT_LABELS[idx.item()] for idx in indices]

        return IntentOutput(logits=logits, label=labels, confidence=confidence)

    def forward_policy_loss(
        self,
        task_tool_texts: List[str],
        target_scores: torch.Tensor,
        max_length: int = 384,
    ) -> torch.Tensor:
        """
        Compute MSE loss for policy head training (SFT stage).

        Args:
            task_tool_texts: ["Task: ...\nTool: ..."] formatted strings
            target_scores: Jina relevance scores (0-1)
        """
        device = next(self.parameters()).device

        inputs = self._tokenize(task_tool_texts, max_length=max_length)
        inputs = {k: v.to(device) for k, v in inputs.items()}

        hidden = self._get_last_hidden(inputs["input_ids"], inputs["attention_mask"])
        predicted = self.policy_head(hidden).squeeze(-1)

        target_scores = target_scores.to(device)
        return nn.functional.mse_loss(predicted, target_scores)

    def forward_intent_loss(
        self,
        tasks: List[str],
        target_intents: List[str],
        max_length: int = 256,
    ) -> torch.Tensor:
        """
        Compute cross-entropy loss for intent classification.
        """
        device = next(self.parameters()).device

        inputs = self._tokenize(tasks, max_length=max_length)
        inputs = {k: v.to(device) for k, v in inputs.items()}

        hidden = self._get_last_hidden(inputs["input_ids"], inputs["attention_mask"])
        logits = self.intent_head(hidden)

        targets = torch.tensor(
            [INTENT_LABELS.index(intent) for intent in target_intents],
            device=device,
            dtype=torch.long,
        )

        return nn.functional.cross_entropy(logits, targets)

    def forward_dpo_loss(
        self,
        tasks: List[str],
        chosen_tools: List[str],
        rejected_tools: List[str],
        beta: float = 0.1,
        max_length: int = 384,
    ) -> torch.Tensor:
        """
        Direct Preference Optimization loss on the policy head.

        DPO loss = -log σ(β * (score(chosen) - score(rejected)))
        """
        device = next(self.parameters()).device

        chosen_texts = [f"Task: {t}\nTool: {c}" for t, c in zip(tasks, chosen_tools)]
        rejected_texts = [f"Task: {t}\nTool: {r}" for t, r in zip(tasks, rejected_tools)]

        all_texts = chosen_texts + rejected_texts
        inputs = self._tokenize(all_texts, max_length=max_length)
        inputs = {k: v.to(device) for k, v in inputs.items()}

        hidden = self._get_last_hidden(inputs["input_ids"], inputs["attention_mask"])
        scores = self.policy_head(hidden).squeeze(-1)  # (2*batch,)

        chosen_scores = scores[:len(tasks)]
        rejected_scores = scores[len(tasks):]

        # DPO: -log σ(β * (r_w - r_l))
        logits = beta * (chosen_scores - rejected_scores)
        loss = -nn.functional.logsigmoid(logits).mean()

        return loss

    def save_heads(self, path: str):
        """Save only the scoring heads (not the backbone)."""
        torch.save({
            "policy_head": self.policy_head.state_dict(),
            "reward_head": self.reward_head.state_dict(),
            "intent_head": self.intent_head.state_dict(),
            "platt_a": self.platt_a.data,
            "platt_b": self.platt_b.data,
        }, path)

    def load_heads(self, path: str):
        """Load scoring heads."""
        checkpoint = torch.load(path, map_location="cpu", weights_only=True)
        self.policy_head.load_state_dict(checkpoint["policy_head"])
        self.reward_head.load_state_dict(checkpoint["reward_head"])
        self.intent_head.load_state_dict(checkpoint["intent_head"])
        if "platt_a" in checkpoint:
            self.platt_a.data = checkpoint["platt_a"]
            self.platt_b.data = checkpoint["platt_b"]

    def save_full(self, path: str):
        """Save backbone (LoRA) + heads."""
        from pathlib import Path
        save_path = Path(path)
        save_path.mkdir(parents=True, exist_ok=True)

        # Save LoRA adapter
        self.backbone.save_pretrained(str(save_path / "lora_adapter"))

        # Save heads
        self.save_heads(str(save_path / "heads.pt"))

        # Save config
        with open(save_path / "config.json", "w") as f:
            json.dump({
                "model_name": self.model_name,
                "hidden_size": self.hidden_size,
                "num_intents": len(INTENT_LABELS),
                "intent_labels": INTENT_LABELS,
            }, f)

    @classmethod
    def load_full(cls, path: str, device: str = "cpu") -> "ToolPolicyRewardModel":
        """Load full model from checkpoint."""
        from pathlib import Path
        load_path = Path(path)

        with open(load_path / "config.json") as f:
            config = json.load(f)

        model = cls(
            model_name=config["model_name"],
            use_lora=True,
            freeze_backbone=False,
        )

        # Load LoRA weights
        from peft import PeftModel
        model.backbone = PeftModel.from_pretrained(
            model.backbone,
            str(load_path / "lora_adapter"),
        )

        # Load heads
        model.load_heads(str(load_path / "heads.pt"))
        model.to(device)
        model.eval()

        return model
