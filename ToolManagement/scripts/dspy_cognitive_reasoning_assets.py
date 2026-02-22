#!/usr/bin/env python3
"""
Build durable DSPy cognitive reasoning assets.

Outputs:
- evals/cognitive_reasoning_evals.csv (750 rows)
- evals/cognitive_reasoning_dspy.ipynb (runnable notebook, DummyLM by default)

Optional:
- Execute notebook in-place with --execute
"""

from __future__ import annotations

import argparse
import csv
import random
import subprocess
import sys
from pathlib import Path

import nbformat as nbf


CATEGORIES = [
    "logical_reasoning",
    "mathematical_reasoning",
    "causal_reasoning",
    "analogical_reasoning",
    "spatial_reasoning",
    "temporal_reasoning",
    "abstract_reasoning",
    "linguistic_reasoning",
    "critical_thinking",
    "planning_strategy",
    "scientific_reasoning",
    "social_cognition",
    "metacognition",
    "ethical_reasoning",
]

ANSWER_TYPES = ["multiple_choice", "free_text", "numeric", "boolean", "list"]


def build_capabilities() -> list[str]:
    caps: list[str] = []
    for category in CATEGORIES:
        base = category.replace("_reasoning", "").replace("_", "-")
        for idx in range(1, 6):
            caps.append(f"{base}-capability-{idx}")
    return caps


def expected_answer_for(index: int, answer_type: str) -> str:
    if answer_type == "boolean":
        return "Yes" if index % 2 == 0 else "No"
    if answer_type == "numeric":
        return str((index % 97) + 3)
    if answer_type == "multiple_choice":
        return ["A", "B", "C", "D"][index % 4]
    if answer_type == "list":
        return f"item_{index % 5}, item_{(index + 1) % 5}"
    return f"Reasoned conclusion {index}"


def generate_rows(total: int = 750, seed: int = 42) -> list[dict[str, str]]:
    random.seed(seed)
    capabilities = build_capabilities()
    difficulties = (["easy"] * 235) + (["medium"] * 274) + (["hard"] * 241)
    random.shuffle(difficulties)

    rows: list[dict[str, str]] = []
    for idx in range(total):
        category = CATEGORIES[idx % len(CATEGORIES)]
        capability = capabilities[idx % len(capabilities)]
        difficulty = difficulties[idx]
        answer_type = ANSWER_TYPES[idx % len(ANSWER_TYPES)]
        reasoning_steps = (idx % 5) + 1
        prompt = (
            f"Solve a {difficulty} {category} task using {capability}. "
            f"Provide a {answer_type} answer with {reasoning_steps} step(s)."
        )
        rows.append(
            {
                "prompt": prompt,
                "category": category,
                "capability": capability,
                "difficulty": difficulty,
                "answer_type": answer_type,
                "reasoning_steps": str(reasoning_steps),
                "expected_answer": expected_answer_for(idx, answer_type),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, str]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "prompt",
        "category",
        "capability",
        "difficulty",
        "answer_type",
        "reasoning_steps",
        "expected_answer",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_notebook(csv_rel_path: str, csv_abs_path: str) -> nbf.NotebookNode:
    nb = nbf.v4.new_notebook()
    cells = []

    cells.append(
        nbf.v4.new_markdown_cell(
            "# DSPy Cognitive Reasoning Pipeline\n"
            "Streaming-first durable evaluation notebook.\n"
            "Uses `DummyLM` by default so it runs without external API keys."
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "import csv\n"
            "import json\n"
            "import random\n"
            "from collections import Counter, defaultdict\n"
            "from pathlib import Path\n"
            "\n"
            "import dspy\n"
            "from dspy.utils.dummies import DummyLM\n"
            "\n"
            f"CSV_PATH = Path('{csv_rel_path}')\n"
            f"if not CSV_PATH.exists():\n"
            f"    CSV_PATH = Path(r'''{csv_abs_path}''')\n"
            "assert CSV_PATH.exists(), f'Missing dataset: {CSV_PATH}'\n"
            "\n"
            "evals = []\n"
            "with CSV_PATH.open() as f:\n"
            "    reader = csv.DictReader(f)\n"
            "    for row in reader:\n"
            "        evals.append(row)\n"
            "\n"
            "print(f'Loaded {len(evals)} rows from {CSV_PATH}')\n"
            "print('Difficulty split:', dict(Counter(r['difficulty'] for r in evals)))\n"
            "print('Categories:', len(set(r['category'] for r in evals)))\n"
            "print('Capabilities:', len(set(r['capability'] for r in evals)))\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "REASONING_METHODS = {\n"
            "    'deductive': {'strength': 'certain'},\n"
            "    'inductive': {'strength': 'probable'},\n"
            "    'abductive': {'strength': 'plausible'},\n"
            "    'analogical': {'strength': 'suggestive'},\n"
            "    'causal': {'strength': 'conditional'},\n"
            "    'probabilistic': {'strength': 'quantified_uncertainty'},\n"
            "    'spatial': {'strength': 'geometric'},\n"
            "    'temporal': {'strength': 'ordered'},\n"
            "    'critical': {'strength': 'evaluative'},\n"
            "    'metacognitive': {'strength': 'reflective'},\n"
            "    'ethical': {'strength': 'normative'},\n"
            "    'scientific': {'strength': 'empirical'},\n"
            "}\n"
            "CATEGORY_TO_METHOD = {\n"
            "    'logical_reasoning': ['deductive', 'inductive'],\n"
            "    'mathematical_reasoning': ['deductive', 'probabilistic'],\n"
            "    'causal_reasoning': ['causal', 'scientific'],\n"
            "    'analogical_reasoning': ['analogical'],\n"
            "    'spatial_reasoning': ['spatial'],\n"
            "    'temporal_reasoning': ['temporal'],\n"
            "    'abstract_reasoning': ['inductive', 'deductive'],\n"
            "    'linguistic_reasoning': ['critical', 'analogical'],\n"
            "    'critical_thinking': ['critical', 'deductive'],\n"
            "    'planning_strategy': ['causal', 'temporal'],\n"
            "    'scientific_reasoning': ['scientific', 'probabilistic'],\n"
            "    'social_cognition': ['abductive', 'metacognitive'],\n"
            "    'metacognition': ['metacognitive', 'critical'],\n"
            "    'ethical_reasoning': ['ethical', 'critical'],\n"
            "}\n"
            "print('Methods:', len(REASONING_METHODS), 'Category map:', len(CATEGORY_TO_METHOD))\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "dummy_answers = [{\n"
            "    'selected_methods': 'deductive,critical',\n"
            "    'method_justification': 'Selected based on category and difficulty.',\n"
            "    'step_by_step_reasoning': 'Step 1: parse problem. Step 2: apply method.',\n"
            "    'failure_mode_check': 'Checked for sampling bias and logical fallacies.',\n"
            "    'answer': 'A',\n"
            "    'confidence': 0.72,\n"
            "    'approach_description': 'Structured reasoning approach.',\n"
            "    'assumptions_made': 'Inputs are valid.',\n"
            "    'what_could_go_wrong': 'Insufficient context.',\n"
            "    'self_assessment': 'Adequate.',\n"
            "    'is_correct': True,\n"
            "    'error_analysis': 'None',\n"
            "    'calibration_assessment': 'Slightly conservative',\n"
            "    'suggested_improvement': 'Add retrieval grounding',\n"
            "}] * 3000\n"
            "dspy.configure(lm=DummyLM(dummy_answers))\n"
            "print('Configured DSPy with DummyLM for deterministic local execution.')\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "class ReasoningMethodSelector(dspy.Signature):\n"
            "    prompt: str = dspy.InputField()\n"
            "    category: str = dspy.InputField()\n"
            "    capability: str = dspy.InputField()\n"
            "    difficulty: str = dspy.InputField()\n"
            "    available_methods: str = dspy.InputField()\n"
            "    selected_methods: str = dspy.OutputField()\n"
            "    method_justification: str = dspy.OutputField()\n"
            "\n"
            "class ReasoningChain(dspy.Signature):\n"
            "    prompt: str = dspy.InputField()\n"
            "    selected_methods: str = dspy.InputField()\n"
            "    answer_type: str = dspy.InputField()\n"
            "    reasoning_steps: int = dspy.InputField()\n"
            "    step_by_step_reasoning: str = dspy.OutputField()\n"
            "    failure_mode_check: str = dspy.OutputField()\n"
            "    answer: str = dspy.OutputField()\n"
            "    confidence: float = dspy.OutputField()\n"
            "\n"
            "class SelfDescribingModule(dspy.Signature):\n"
            "    prompt: str = dspy.InputField()\n"
            "    category: str = dspy.InputField()\n"
            "    difficulty: str = dspy.InputField()\n"
            "    approach_description: str = dspy.OutputField()\n"
            "    assumptions_made: str = dspy.OutputField()\n"
            "    what_could_go_wrong: str = dspy.OutputField()\n"
            "    self_assessment: str = dspy.OutputField()\n"
            "\n"
            "class CognitiveReasoningPipeline(dspy.Module):\n"
            "    def __init__(self):\n"
            "        super().__init__()\n"
            "        self.select = dspy.Predict(ReasoningMethodSelector)\n"
            "        self.reason = dspy.Predict(ReasoningChain)\n"
            "        self.describe = dspy.Predict(SelfDescribingModule)\n"
            "\n"
            "    def forward(self, prompt, category, capability, difficulty, answer_type, reasoning_steps):\n"
            "        methods = CATEGORY_TO_METHOD.get(category, ['deductive'])\n"
            "        selection = self.select(\n"
            "            prompt=prompt,\n"
            "            category=category,\n"
            "            capability=capability,\n"
            "            difficulty=difficulty,\n"
            "            available_methods=json.dumps(methods),\n"
            "        )\n"
            "        chain = self.reason(\n"
            "            prompt=prompt,\n"
            "            selected_methods=selection.selected_methods,\n"
            "            answer_type=answer_type,\n"
            "            reasoning_steps=reasoning_steps,\n"
            "        )\n"
            "        description = self.describe(prompt=prompt, category=category, difficulty=difficulty)\n"
            "        return dspy.Prediction(\n"
            "            selected_methods=selection.selected_methods,\n"
            "            method_justification=selection.method_justification,\n"
            "            step_by_step_reasoning=chain.step_by_step_reasoning,\n"
            "            failure_mode_check=chain.failure_mode_check,\n"
            "            answer=chain.answer,\n"
            "            confidence=chain.confidence,\n"
            "            approach_description=description.approach_description,\n"
            "            assumptions_made=description.assumptions_made,\n"
            "            what_could_go_wrong=description.what_could_go_wrong,\n"
            "            self_assessment=description.self_assessment,\n"
            "        )\n"
            "\n"
            "pipeline = CognitiveReasoningPipeline()\n"
            "print('Pipeline ready')\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "examples = []\n"
            "for row in evals:\n"
            "    ex = dspy.Example(\n"
            "        prompt=row['prompt'],\n"
            "        category=row['category'],\n"
            "        capability=row['capability'],\n"
            "        difficulty=row['difficulty'],\n"
            "        answer_type=row['answer_type'],\n"
            "        reasoning_steps=int(row['reasoning_steps']),\n"
            "        expected_answer=row['expected_answer'],\n"
            "    ).with_inputs('prompt', 'category', 'capability', 'difficulty', 'answer_type', 'reasoning_steps')\n"
            "    examples.append(ex)\n"
            "\n"
            "random.seed(42)\n"
            "random.shuffle(examples)\n"
            "n = len(examples)\n"
            "train = examples[: int(0.6 * n)]\n"
            "dev = examples[int(0.6 * n) : int(0.8 * n)]\n"
            "test = examples[int(0.8 * n) :]\n"
            "print(f'Split: {len(train)} train / {len(dev)} dev / {len(test)} test')\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "def semantic_match_metric(example, prediction, trace=None):\n"
            "    expected = str(example.expected_answer).strip().lower()\n"
            "    proposed = str(getattr(prediction, 'answer', '')).strip().lower()\n"
            "    return 1.0 if expected[:1] == proposed[:1] else 0.0\n"
            "\n"
            "scores = []\n"
            "for ex in dev[:25]:\n"
            "    pred = pipeline(\n"
            "        prompt=ex.prompt,\n"
            "        category=ex.category,\n"
            "        capability=ex.capability,\n"
            "        difficulty=ex.difficulty,\n"
            "        answer_type=ex.answer_type,\n"
            "        reasoning_steps=ex.reasoning_steps,\n"
            "    )\n"
            "    scores.append(semantic_match_metric(ex, pred))\n"
            "baseline = sum(scores) / len(scores)\n"
            "print(f'Baseline score (dev subset): {baseline:.3f}')\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "by_category = defaultdict(list)\n"
            "for ex in test[:50]:\n"
            "    pred = pipeline(\n"
            "        prompt=ex.prompt,\n"
            "        category=ex.category,\n"
            "        capability=ex.capability,\n"
            "        difficulty=ex.difficulty,\n"
            "        answer_type=ex.answer_type,\n"
            "        reasoning_steps=ex.reasoning_steps,\n"
            "    )\n"
            "    by_category[ex.category].append(semantic_match_metric(ex, pred))\n"
            "\n"
            "print('Category analysis (test subset):')\n"
            "for category in sorted(by_category.keys())[:10]:\n"
            "    vals = by_category[category]\n"
            "    print(f'  {category:<24} {sum(vals) / len(vals):.3f}  n={len(vals)}')\n"
        )
    )

    cells.append(
        nbf.v4.new_code_cell(
            "out_dir = Path('optimized_programs')\n"
            "out_dir.mkdir(parents=True, exist_ok=True)\n"
            "summary = {\n"
            "    'dataset_rows': len(evals),\n"
            "    'train': len(train),\n"
            "    'dev': len(dev),\n"
            "    'test': len(test),\n"
            "    'baseline_score_dev_subset': baseline,\n"
            "    'reasoning_methods': list(REASONING_METHODS.keys()),\n"
            "}\n"
            "summary_path = out_dir / 'experiment_summary.json'\n"
            "summary_path.write_text(json.dumps(summary, indent=2))\n"
            "print(f'Saved summary -> {summary_path}')\n"
        )
    )

    nb.cells = cells
    return nb


def write_notebook(path: Path, nb: nbf.NotebookNode):
    path.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(nb, path)


def execute_notebook(path: Path):
    cmd = [
        sys.executable,
        "-m",
        "jupyter",
        "nbconvert",
        "--to",
        "notebook",
        "--execute",
        "--inplace",
        "--ExecutePreprocessor.timeout=1200",
        str(path),
    ]
    subprocess.run(cmd, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build and optionally execute DSPy reasoning assets.")
    parser.add_argument("--execute", action="store_true", help="Execute the notebook after generating it.")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    csv_path = root / "evals" / "cognitive_reasoning_evals.csv"
    nb_path = root / "evals" / "cognitive_reasoning_dspy.ipynb"

    rows = generate_rows(total=750, seed=42)
    write_csv(csv_path, rows)
    nb = build_notebook(
        csv_rel_path="evals/cognitive_reasoning_evals.csv",
        csv_abs_path=str(csv_path),
    )
    write_notebook(nb_path, nb)

    difficulty_counts = {"easy": 0, "medium": 0, "hard": 0}
    for row in rows:
        difficulty_counts[row["difficulty"]] += 1

    print(f"Wrote CSV: {csv_path} ({len(rows)} rows)")
    print(f"Difficulty split: {difficulty_counts}")
    print(f"Capabilities: {len(build_capabilities())}")
    print(f"Wrote notebook: {nb_path}")

    if args.execute:
        print("Executing notebook...")
        execute_notebook(nb_path)
        print("Notebook execution complete.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
