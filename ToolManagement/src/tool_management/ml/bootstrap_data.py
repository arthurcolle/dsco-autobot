"""
Stage 1: Bootstrap training data from Jina APIs.

Generates:
  - scored_pairs.jsonl     : (task, tool_schema, jina_relevance_score, rank)
  - preferences.jsonl      : (task, chosen_tool, rejected_tool, margin)
  - intent_labels.jsonl    : (task, intent, confidence)
  - sft_examples.jsonl     : formatted SFT training examples

Budget: ~$200 for 20K tasks × 50 candidates each.
Run once, never again.

Usage:
    python -m tool_management.ml.bootstrap_data --output-dir ./training_data --num-tasks 20000
"""

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass, asdict
from itertools import combinations
from pathlib import Path
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

INTENT_LABELS = [
    "research", "remember", "notify", "analyze",
    "publish", "transform", "query", "manage"
]

# Synthetic task templates for diversity
TASK_TEMPLATES = [
    # Financial
    "Get the current stock price for {symbol}",
    "Find options chains for {symbol} expiring this month",
    "Analyze the {indicator} for {symbol} over the last {period}",
    "Compare {symbol1} and {symbol2} performance this quarter",
    "What are the latest earnings for {symbol}?",
    "Pull the balance sheet for {symbol}",
    "Get insider transactions for {symbol}",
    "Find the top gainers and losers today",
    "Calculate the {indicator} for {symbol} with period {period}",
    "Get the latest news sentiment for {symbol}",
    "What is the current {commodity} price?",
    "Show treasury yield curve for {maturity} maturity",
    "Get the latest CPI and inflation data",
    "Find all IPOs scheduled for the next 3 months",
    "What is the {from_currency}/{to_currency} exchange rate?",
    "Get {crypto} intraday price data",

    # Research
    "Research {topic} and summarize findings",
    "Find all {entity_type} in {location}",
    "Deep search for information about {topic}",
    "Synthesize these sources about {topic}",
    "Extract structured data from {url}",
    "Verify the claim: {claim}",

    # Memory
    "Remember that {fact}",
    "What do I know about {entity}?",
    "Store the fact that {subject} {predicate} {object}",
    "Find all beliefs related to {entity}",
    "Recall recent episodes about {topic}",
    "Learn a new skill for {task_description}",

    # Publishing
    "Create a draft post about {topic}",
    "Publish the draft with id {draft_id}",
    "Post a note saying: {message}",
    "Send a notification about {event}",
    "Create a webhook for {event_type} events",
    "Start a live blog about {topic}",

    # Search
    "Search the web for {query}",
    "Read the content at {url}",
    "Embed this text for similarity search: {text}",
    "Rerank these documents by relevance to {query}",

    # Cross-domain
    "Find {symbol} stock data, analyze it, and write a report",
    "Search for news about {topic}, store key facts, and notify me",
    "Get earnings data for {symbol} and create a draft post",
    "Research {topic}, verify key claims, and summarize",
]

SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "BAC", "GS",
           "BRK.B", "V", "MA", "UNH", "JNJ", "PG", "XOM", "CVX", "HD", "DIS",
           "NFLX", "PYPL", "CRM", "INTC", "AMD", "COIN", "SQ", "SOFI", "PLTR", "SNOW"]
INDICATORS = ["SMA", "EMA", "RSI", "MACD", "BBANDS", "STOCH", "ADX", "ATR", "OBV", "VWAP"]
COMMODITIES = ["WTI crude oil", "Brent crude", "natural gas", "gold", "silver", "copper", "wheat", "corn"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "CNY"]
CRYPTOS = ["BTC", "ETH", "SOL", "DOGE", "XRP", "ADA", "DOT", "AVAX"]
TOPICS = ["AI regulation", "quantum computing", "climate policy", "semiconductor supply chain",
          "central bank digital currencies", "renewable energy", "space technology", "gene therapy"]


def _fill_template(template: str) -> str:
    """Fill a task template with random values."""
    replacements = {
        "{symbol}": random.choice(SYMBOLS),
        "{symbol1}": random.choice(SYMBOLS),
        "{symbol2}": random.choice(SYMBOLS),
        "{indicator}": random.choice(INDICATORS),
        "{period}": random.choice(["14", "20", "50", "100", "200"]),
        "{commodity}": random.choice(COMMODITIES),
        "{maturity}": random.choice(["2year", "5year", "10year", "30year"]),
        "{from_currency}": random.choice(CURRENCIES),
        "{to_currency}": random.choice(CURRENCIES),
        "{crypto}": random.choice(CRYPTOS),
        "{topic}": random.choice(TOPICS),
        "{entity}": random.choice(SYMBOLS + TOPICS),
        "{entity_type}": random.choice(["AI startups", "hedge funds", "biotech companies", "fintech firms"]),
        "{location}": random.choice(["San Francisco", "New York", "London", "Singapore"]),
        "{url}": "https://example.com/article",
        "{claim}": random.choice(["Gold is a safe haven during recessions",
                                   "NVDA will beat earnings estimates",
                                   "The Fed will cut rates in Q2"]),
        "{fact}": f"{random.choice(SYMBOLS)} reported strong earnings",
        "{subject}": random.choice(SYMBOLS),
        "{predicate}": random.choice(["is_competitor_of", "acquired", "partners_with", "is_sector"]),
        "{object}": random.choice(SYMBOLS + ["technology", "finance", "healthcare"]),
        "{task_description}": "analyzing earnings reports",
        "{draft_id}": str(random.randint(1, 100)),
        "{message}": "Market update: significant price movement detected",
        "{event}": "earnings release",
        "{event_type}": random.choice(["content.published", "price.alert", "earnings.released"]),
        "{query}": random.choice(TOPICS),
        "{text}": "The Federal Reserve announced a rate decision today.",
    }
    result = template
    for key, value in replacements.items():
        result = result.replace(key, value)
    return result


def generate_synthetic_tasks(count: int) -> List[str]:
    """Generate diverse synthetic task descriptions."""
    tasks = []
    for _ in range(count):
        template = random.choice(TASK_TEMPLATES)
        tasks.append(_fill_template(template))
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for t in tasks:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    return unique


@dataclass
class ScoredPair:
    task: str
    tool_name: str
    tool_schema: str
    jina_relevance_score: float
    jina_rank: int
    intent: str
    intent_confidence: float


@dataclass
class PreferencePair:
    task: str
    chosen_tool: str
    chosen_schema: str
    rejected_tool: str
    rejected_schema: str
    chosen_score: float
    rejected_score: float
    margin: float


@dataclass
class SFTExample:
    """Formatted example for supervised fine-tuning."""
    task: str
    intent: str
    candidates: List[Dict[str, Any]]  # [{name, description, score, rank}]
    selected_tool: str
    selected_params_hint: str
    confidence: float


def format_sft_example(task: str, intent: str, ranked_tools: List[Dict]) -> str:
    """Format a single SFT training example as chat-style text."""
    candidates_text = "\n".join(
        f"{i+1}. {t['name']}: {t['description'][:120]}"
        for i, t in enumerate(ranked_tools[:8])
    )

    output = {
        "rankings": [
            {"tool": t["name"], "score": round(t["score"], 3),
             "reason": t.get("reason", "semantic match")}
            for t in ranked_tools[:5]
        ],
        "selected": ranked_tools[0]["name"],
        "intent": intent,
        "confidence": round(ranked_tools[0]["score"], 3),
    }

    return (
        f"<|system|>You are a tool selection model. Given a task and candidate tools, "
        f"score each tool's relevance (0-1) and select the best one.<|end|>\n"
        f"<|user|>Task: {task}\n\nCandidate tools:\n{candidates_text}<|end|>\n"
        f"<|assistant|>{json.dumps(output)}<|end|>"
    )


async def generate_training_data(
    output_dir: str,
    num_tasks: int = 20_000,
    candidates_per_task: int = 50,
    jina_api_key: Optional[str] = None,
    semantic_store_path: Optional[str] = None,
    batch_size: int = 50,
    resume_from: int = 0,
):
    """
    Main entry point: generate all training data from Jina APIs.

    Cost breakdown (approximate):
        - Embed queries: num_tasks × $0.00001 = $0.20
        - Rerank: num_tasks × $0.0001 = $2.00
        - Classify: num_tasks × $0.00002 = $0.40
        Total for 20K tasks: ~$2.60
        (Most budget goes to generating diverse tasks and larger runs)
    """
    from ..integrations.jina_provider import JinaProvider
    from ..embeddings.semantic_tool_store import SemanticToolStore

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Initialize Jina
    api_key = jina_api_key or os.getenv("JINA_API_KEY")
    jina = JinaProvider(api_key=api_key)

    # Initialize semantic store (uses existing tool embeddings)
    store = SemanticToolStore(db_path=semantic_store_path)
    stats = await store.get_stats()
    total_tools = stats["total_tools"]
    logger.info(f"SemanticToolStore has {total_tools} tools indexed")

    if total_tools == 0:
        raise RuntimeError(
            "No tools in SemanticToolStore. Run the main API first to index tools, "
            "or provide a pre-populated tool_embeddings.db"
        )

    # Generate tasks
    logger.info(f"Generating {num_tasks} synthetic tasks...")
    tasks = generate_synthetic_tasks(num_tasks)
    logger.info(f"Generated {len(tasks)} unique tasks")

    # Open output files (append mode for resume support)
    mode = "a" if resume_from > 0 else "w"
    f_scored = open(output_path / "scored_pairs.jsonl", mode)
    f_prefs = open(output_path / "preferences.jsonl", mode)
    f_intents = open(output_path / "intent_labels.jsonl", mode)
    f_sft = open(output_path / "sft_examples.jsonl", mode)

    total_scored = 0
    total_prefs = 0
    total_intents = 0
    total_sft = 0
    cost_estimate = 0.0

    try:
        for batch_start in range(resume_from, len(tasks), batch_size):
            batch_end = min(batch_start + batch_size, len(tasks))
            batch = tasks[batch_start:batch_end]

            logger.info(
                f"Processing batch {batch_start//batch_size + 1}/{(len(tasks) + batch_size - 1)//batch_size} "
                f"(tasks {batch_start}-{batch_end})"
            )

            for task in batch:
                try:
                    # 1. Semantic search for candidates (free - local numpy)
                    candidates = await store.search_tools(
                        query=task,
                        limit=candidates_per_task
                    )

                    if len(candidates) < 3:
                        continue

                    # 2. Rerank with Jina (~$0.0001 per call)
                    tool_descriptions = [
                        f"{c['tool'].get('name', 'unknown')}: {c['tool'].get('description', '')[:200]}"
                        for c in candidates
                    ]

                    reranked = await jina.rerank(
                        query=task,
                        documents=tool_descriptions,
                        top_n=min(candidates_per_task, len(candidates))
                    )
                    cost_estimate += 0.0001

                    # 3. Classify intent (~$0.00002 per call)
                    intent_results = await jina.classify(
                        input_data=task,
                        labels=INTENT_LABELS
                    )
                    cost_estimate += 0.00002

                    if not intent_results or not intent_results[0].get("predictions"):
                        continue

                    intent = intent_results[0]["predictions"][0]["label"]
                    intent_conf = intent_results[0]["predictions"][0]["score"]

                    # Write intent label
                    f_intents.write(json.dumps({
                        "task": task,
                        "intent": intent,
                        "confidence": intent_conf,
                        "all_scores": {p["label"]: round(p["score"], 4)
                                       for p in intent_results[0]["predictions"]}
                    }) + "\n")
                    total_intents += 1

                    # 4. Build scored pairs
                    ranked_tools = []
                    for rank, result in enumerate(reranked):
                        idx = result.get("index", rank)
                        if idx >= len(candidates):
                            continue

                        tool = candidates[idx]["tool"]
                        score = result.get("relevance_score", 0.0)
                        tool_name = tool.get("name", "unknown")
                        tool_desc = tool.get("description", "")
                        tool_schema_text = f"{tool_name}: {tool_desc}"

                        # Write scored pair
                        pair = ScoredPair(
                            task=task,
                            tool_name=tool_name,
                            tool_schema=tool_schema_text,
                            jina_relevance_score=score,
                            jina_rank=rank,
                            intent=intent,
                            intent_confidence=intent_conf,
                        )
                        f_scored.write(json.dumps(asdict(pair)) + "\n")
                        total_scored += 1

                        ranked_tools.append({
                            "name": tool_name,
                            "description": tool_desc[:200],
                            "score": score,
                            "rank": rank,
                            "reason": "jina_reranker",
                        })

                    # 5. Generate preference pairs
                    # Top-3 vs bottom-3 (strong signal)
                    for i in range(min(3, len(ranked_tools))):
                        for j in range(max(len(ranked_tools) - 3, i + 1), len(ranked_tools)):
                            chosen = ranked_tools[i]
                            rejected = ranked_tools[j]
                            margin = chosen["score"] - rejected["score"]

                            if margin < 0.05:  # skip near-ties
                                continue

                            pref = PreferencePair(
                                task=task,
                                chosen_tool=chosen["name"],
                                chosen_schema=f"{chosen['name']}: {chosen['description']}",
                                rejected_tool=rejected["name"],
                                rejected_schema=f"{rejected['name']}: {rejected['description']}",
                                chosen_score=chosen["score"],
                                rejected_score=rejected["score"],
                                margin=margin,
                            )
                            f_prefs.write(json.dumps(asdict(pref)) + "\n")
                            total_prefs += 1

                    # 6. Format SFT example
                    if ranked_tools:
                        sft_text = format_sft_example(task, intent, ranked_tools)
                        f_sft.write(json.dumps({"text": sft_text}) + "\n")
                        total_sft += 1

                except Exception as e:
                    logger.warning(f"Failed on task '{task[:50]}...': {e}")
                    continue

            # Flush after each batch
            f_scored.flush()
            f_prefs.flush()
            f_intents.flush()
            f_sft.flush()

            logger.info(
                f"Progress: {batch_end}/{len(tasks)} tasks | "
                f"scored={total_scored} prefs={total_prefs} sft={total_sft} | "
                f"est_cost=${cost_estimate:.4f}"
            )

            # Rate limiting: ~10 requests/second to Jina
            await asyncio.sleep(0.1)

    finally:
        f_scored.close()
        f_prefs.close()
        f_intents.close()
        f_sft.close()
        await jina.close()

    logger.info(f"""
Bootstrap complete!
  Scored pairs:     {total_scored:,}
  Preference pairs: {total_prefs:,}
  Intent labels:    {total_intents:,}
  SFT examples:     {total_sft:,}
  Estimated cost:   ${cost_estimate:.4f}
  Output dir:       {output_path}
""")

    return {
        "scored_pairs": total_scored,
        "preference_pairs": total_prefs,
        "intent_labels": total_intents,
        "sft_examples": total_sft,
        "estimated_cost": cost_estimate,
    }


def main():
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Generate training data from Jina APIs")
    parser.add_argument("--output-dir", default="./training_data", help="Output directory")
    parser.add_argument("--num-tasks", type=int, default=20_000, help="Number of synthetic tasks")
    parser.add_argument("--candidates", type=int, default=50, help="Candidates per task")
    parser.add_argument("--batch-size", type=int, default=50, help="Batch size")
    parser.add_argument("--resume-from", type=int, default=0, help="Resume from task index")
    parser.add_argument("--store-path", default=None, help="Path to tool_embeddings.db")
    args = parser.parse_args()

    asyncio.run(generate_training_data(
        output_dir=args.output_dir,
        num_tasks=args.num_tasks,
        candidates_per_task=args.candidates,
        batch_size=args.batch_size,
        resume_from=args.resume_from,
        semantic_store_path=args.store_path,
    ))


if __name__ == "__main__":
    main()
