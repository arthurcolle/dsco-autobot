#!/usr/bin/env python3
"""
Baseline trial: Run tasks through the full Jina scoring pipeline.

Tasks are derived from the user's actual domains:
- Structured credit / fixed income / MBS
- AI/ML infrastructure & model training
- Crypto & blockchain
- Cybersecurity & anomaly detection
- Distributed systems & cloud economics
- Computational neuroscience
- Financial technology & trading
- Mortgage trading & securitization
- Web development (Elixir, Rails, Go)
- Research & academic papers

Usage:
    cd ~/dsco/dsco-autobot/ToolManagement
    JINA_API_KEY=... python scripts/ml_baseline_trial.py
"""

import asyncio
import json
import logging
import os
import sys
import time
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Tasks derived from user's actual domains ──────────────────────────────

TRIAL_TASKS = [
    # ── Structured Credit & Fixed Income ──
    "Get the current yield on 10-year Treasury bonds",
    "Pull the latest CPI and inflation data from the Fed",
    "Compare credit spreads on IG vs HY corporate bonds this quarter",
    "Find the latest CMBS issuance statistics for 2026",
    "Analyze MBS prepayment speeds for FNMA 30-year pools",
    "What is the current OAS on agency MBS versus Treasuries?",
    "Research structured credit risk transfer mechanisms",
    "Get the latest CLO default rates and recovery data",
    "Pull the Fed funds effective rate and SOFR spread",
    "Analyze the treasury yield curve for 2yr vs 10yr inversion signals",

    # ── AI/ML Infrastructure & Model Training ──
    "Search for the latest papers on direct preference optimization",
    "Research LoRA fine-tuning techniques for small language models",
    "Find benchmarks comparing Qwen 2.5 vs Llama 3 on tool-use tasks",
    "Get the latest Modal GPU pricing for T4 vs A10G instances",
    "Research RLHF vs DPO training methods for reward models",
    "Find information about PEFT parameter-efficient fine-tuning",
    "What are the best practices for deploying LLMs on Apple Silicon with MLX?",
    "Search for recent advances in neuromorphic computing architectures",
    "Research photonic neural network implementations for inference",
    "Find papers on curiosity-driven reinforcement learning",

    # ── Crypto & Digital Assets ──
    "Get BTC/USD current price and 24h volume",
    "What is the ETH/BTC exchange rate right now?",
    "Find the latest SOL staking yield and validator count",
    "Research central bank digital currency implementations worldwide",
    "Get the latest DeFi total value locked across major protocols",
    "Compare AVAX and DOT performance over the last 90 days",

    # ── Cybersecurity & Privacy ──
    "Research trajectory-based anomaly detection for network security",
    "Find NIST AI governance framework documents and guidelines",
    "Search for recent zero-day vulnerabilities in cloud infrastructure",
    "What are the latest developments in homomorphic encryption?",

    # ── Distributed Systems & Infrastructure ──
    "Research hyperscaler capital expenditure trends for 2026",
    "Compare data center power usage effectiveness across providers",
    "Find information about edge computing latency benchmarks",
    "Search for distributed consensus algorithms beyond Raft and Paxos",
    "Get the latest cloud GPU availability and pricing across providers",

    # ── Financial Technology & Trading ──
    "Get AAPL stock price and options chain for next month expiry",
    "Analyze the RSI and MACD for NVDA over the last 30 days",
    "Pull the balance sheet for JPM and compare to BAC",
    "Find insider transactions for TSLA in the last 90 days",
    "Get the top gainers and losers in the S&P 500 today",
    "What is the current USD/JPY exchange rate and trend?",
    "Compare GOOGL and MSFT earnings growth this quarter",
    "Get the latest gold and silver spot prices",
    "Find the IPO calendar for next quarter",
    "Analyze the BBANDS for AMD with 20-period lookback",

    # ── Mortgage Trading & Securitization ──
    "Research the latest Fannie Mae CAS deal structures",
    "Find historical prepayment data for GNMA II pools",
    "What are current conforming mortgage rates for 30yr fixed?",
    "Analyze the basis between TBA MBS and specified pools",

    # ── Knowledge Management & Memory ──
    "Remember that the Fed held rates steady at the March meeting",
    "Store the fact that NVDA beat earnings estimates by 15%",
    "What do I know about structured credit risk transfer?",
    "Recall recent episodes about AI regulation developments",
    "Find all beliefs related to central bank policy",
    "Remember that CLO default rates are at 2.1% as of Q1 2026",
    "What have I stored about mortgage prepayment speeds?",

    # ── Publishing & Communication ──
    "Create a draft post analyzing hyperscaler capex trends",
    "Publish a note about today's treasury yield curve inversion",
    "Start a live blog covering the FOMC meeting",
    "Send a notification that AAPL crossed above its 200-day SMA",
    "Create a webhook for price alerts on BTC above 100K",
    "Draft a research report on AI infrastructure economics",

    # ── Cross-Domain Compound Tasks ──
    "Get NVDA earnings data, analyze the trend, and write a draft report",
    "Research DPO training methods, store key findings, and notify me",
    "Pull treasury yields, compare to MBS spreads, and create a chart post",
    "Search for NIST AI framework, summarize findings, and store as belief",
    "Get BTC price data, analyze RSI, and send alert if overbought",
    "Research CLO market trends, find related papers, and draft a summary",

    # ── Web Scale Engineering ──
    "Find best practices for Elixir/Phoenix WebSocket scaling",
    "Research Go concurrency patterns for high-throughput trading systems",
    "Search for Ruby on Rails performance optimization techniques",
    "Compare message queue systems: RabbitMQ vs NATS vs Redpanda",

    # ── Computational Neuroscience ──
    "Research spike-triggered average methods for neural decoding",
    "Find papers on EEG-based brain-computer interface architectures",
    "Search for latest advances in spiking neural networks for edge AI",

    # ── Funding & Business Intelligence ──
    "Research AI startup funding rounds in Q1 2026",
    "Find the latest venture capital trends in fintech infrastructure",
    "Search for Series A valuations in the AI agent space",
    "Get information about government grants for AI safety research",
]

INTENT_LABELS = [
    "research", "remember", "notify", "analyze",
    "publish", "transform", "query", "manage"
]


async def run_trial():
    from tool_management.integrations.jina_provider import JinaProvider
    from tool_management.embeddings.semantic_tool_store import SemanticToolStore

    api_key = os.getenv("JINA_API_KEY")
    if not api_key:
        logger.error("JINA_API_KEY not set")
        sys.exit(1)

    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "tool_embeddings.db")
    store = SemanticToolStore(db_path=db_path)
    stats = await store.get_stats()
    logger.info(f"SemanticToolStore: {stats['total_tools']} tools indexed")

    if stats["total_tools"] == 0:
        logger.error("No tools indexed in SemanticToolStore")
        sys.exit(1)

    jina = JinaProvider(api_key=api_key)

    # Shuffle to avoid systematic bias, but use fixed seed for reproducibility
    tasks = list(TRIAL_TASKS)
    random.seed(42)
    random.shuffle(tasks)

    # Limit to N tasks (adjust as needed)
    max_tasks = int(os.getenv("MAX_TASKS", "20"))
    tasks = tasks[:max_tasks]

    results = []
    scored_pairs = []
    preference_pairs = []
    intent_labels = []
    total_cost = 0.0
    t0 = time.time()

    for i, task in enumerate(tasks):
        logger.info(f"\n[{i+1}/{len(tasks)}] {task}")

        try:
            # 1. Semantic search (local)
            t1 = time.time()
            candidates = await store.search_tools(query=task, limit=20)
            search_ms = (time.time() - t1) * 1000

            if len(candidates) < 3:
                logger.warning(f"  Only {len(candidates)} candidates, skipping")
                continue

            # 2. Jina rerank
            t1 = time.time()
            tool_descriptions = [
                f"{c['tool'].get('name', '?')}: {c['tool'].get('description', '')[:200]}"
                for c in candidates
            ]
            reranked = await jina.rerank(
                query=task,
                documents=tool_descriptions,
                top_n=10,
            )
            rerank_ms = (time.time() - t1) * 1000
            total_cost += 0.0001

            # 3. Jina classify intent
            t1 = time.time()
            intent_results = await jina.classify(
                input_data=task,
                labels=INTENT_LABELS,
            )
            classify_ms = (time.time() - t1) * 1000
            total_cost += 0.00002

            if intent_results and intent_results[0].get("predictions"):
                preds = intent_results[0]["predictions"]
                top_intent = preds[0]["label"]
                top_conf = preds[0]["score"]
                all_intents = {p["label"]: round(p["score"], 3) for p in preds}
            else:
                top_intent = "query"
                top_conf = 0.5
                all_intents = {}

            # Build ranked tools
            ranked_tools = []
            for rank, result in enumerate(reranked):
                idx = result.get("index", rank)
                if idx >= len(candidates):
                    continue
                tool = candidates[idx]["tool"]
                score = result.get("relevance_score", 0.0)
                tool_name = tool.get("name", "?")
                tool_desc = tool.get("description", "")

                ranked_tools.append({
                    "name": tool_name,
                    "description": tool_desc[:200],
                    "score": score,
                    "rank": rank,
                    "semantic_sim": candidates[idx]["similarity"],
                })

                # Scored pair for training
                scored_pairs.append({
                    "task": task,
                    "tool_name": tool_name,
                    "tool_schema": f"{tool_name}: {tool_desc[:200]}",
                    "jina_relevance_score": score,
                    "jina_rank": rank,
                    "intent": top_intent,
                    "intent_confidence": top_conf,
                })

            # Intent label
            intent_labels.append({
                "task": task,
                "intent": top_intent,
                "confidence": top_conf,
                "all_scores": all_intents,
            })

            # Preference pairs (top vs bottom)
            for ti in range(min(3, len(ranked_tools))):
                for bj in range(max(len(ranked_tools) - 3, ti + 1), len(ranked_tools)):
                    chosen = ranked_tools[ti]
                    rejected = ranked_tools[bj]
                    margin = chosen["score"] - rejected["score"]
                    if margin < 0.01:
                        continue
                    preference_pairs.append({
                        "task": task,
                        "chosen_tool": chosen["name"],
                        "chosen_schema": f"{chosen['name']}: {chosen['description']}",
                        "rejected_tool": rejected["name"],
                        "rejected_schema": f"{rejected['name']}: {rejected['description']}",
                        "chosen_score": chosen["score"],
                        "rejected_score": rejected["score"],
                        "margin": margin,
                    })

            # Print top-3
            top3 = ", ".join(f"{t['name']}({t['score']:.3f})" for t in ranked_tools[:3])
            logger.info(f"  intent={top_intent}({top_conf:.2f}) | top3: {top3}")
            logger.info(f"  timing: search={search_ms:.0f}ms rerank={rerank_ms:.0f}ms classify={classify_ms:.0f}ms")

            results.append({
                "task": task,
                "intent": top_intent,
                "intent_confidence": top_conf,
                "all_intents": all_intents,
                "num_candidates": len(candidates),
                "top_tool": ranked_tools[0]["name"] if ranked_tools else "?",
                "top_relevance": ranked_tools[0]["score"] if ranked_tools else 0.0,
                "top3": [{"name": t["name"], "score": t["score"]} for t in ranked_tools[:3]],
                "search_ms": search_ms,
                "rerank_ms": rerank_ms,
                "classify_ms": classify_ms,
            })

        except Exception as e:
            logger.error(f"  Failed: {e}")
            import traceback
            traceback.print_exc()
            continue

        await asyncio.sleep(0.15)

    elapsed = time.time() - t0
    await jina.close()

    # ── Summary ──
    print(f"\n{'='*70}")
    print(f"BASELINE TRIAL SUMMARY")
    print(f"{'='*70}")
    print(f"Tasks processed:     {len(results)}/{len(tasks)}")
    print(f"Total time:          {elapsed:.1f}s ({elapsed/max(len(results),1):.1f}s/task)")
    print(f"Estimated cost:      ${total_cost:.5f}")
    print(f"Tools in store:      {stats['total_tools']}")
    print(f"Scored pairs:        {len(scored_pairs)}")
    print(f"Preference pairs:    {len(preference_pairs)}")
    print(f"Intent labels:       {len(intent_labels)}")
    print()

    if results:
        avg_search = sum(r["search_ms"] for r in results) / len(results)
        avg_rerank = sum(r["rerank_ms"] for r in results) / len(results)
        avg_classify = sum(r["classify_ms"] for r in results) / len(results)
        avg_relevance = sum(r["top_relevance"] for r in results) / len(results)

        print(f"Avg latency:")
        print(f"  Semantic search: {avg_search:.0f}ms")
        print(f"  Jina rerank:     {avg_rerank:.0f}ms")
        print(f"  Jina classify:   {avg_classify:.0f}ms")
        print(f"  Total per task:  {avg_search + avg_rerank + avg_classify:.0f}ms")
        print(f"\nAvg top relevance:   {avg_relevance:.4f}")

        # Intent distribution
        intent_dist = {}
        for r in results:
            intent_dist[r["intent"]] = intent_dist.get(r["intent"], 0) + 1
        print(f"\nIntent distribution:")
        for intent, count in sorted(intent_dist.items(), key=lambda x: -x[1]):
            print(f"  {intent:>12}: {count} ({count/len(results)*100:.0f}%)")

        # Relevance score distribution
        scores = [r["top_relevance"] for r in results]
        scores.sort()
        print(f"\nRelevance score distribution:")
        print(f"  Min:    {scores[0]:.4f}")
        print(f"  P25:    {scores[len(scores)//4]:.4f}")
        print(f"  Median: {scores[len(scores)//2]:.4f}")
        print(f"  P75:    {scores[3*len(scores)//4]:.4f}")
        print(f"  Max:    {scores[-1]:.4f}")

        # Top tool frequency
        tool_freq = {}
        for r in results:
            tool_freq[r["top_tool"]] = tool_freq.get(r["top_tool"], 0) + 1
        print(f"\nMost recommended tools:")
        for tool, count in sorted(tool_freq.items(), key=lambda x: -x[1])[:10]:
            print(f"  {tool:<30} {count}x")

        print(f"\nAll results:")
        for r in results:
            print(f"  [{r['intent']:>10}] {r['task'][:55]:<55} → {r['top_tool']:<25} (rel={r['top_relevance']:.3f})")

    # ── Save outputs ──
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")

    with open(os.path.join(data_dir, "baseline_trial.json"), "w") as f:
        json.dump(results, f, indent=2)

    with open(os.path.join(data_dir, "baseline_scored_pairs.jsonl"), "w") as f:
        for sp in scored_pairs:
            f.write(json.dumps(sp) + "\n")

    with open(os.path.join(data_dir, "baseline_preferences.jsonl"), "w") as f:
        for pp in preference_pairs:
            f.write(json.dumps(pp) + "\n")

    with open(os.path.join(data_dir, "baseline_intents.jsonl"), "w") as f:
        for il in intent_labels:
            f.write(json.dumps(il) + "\n")

    print(f"\nSaved to {data_dir}/baseline_*.jsonl")
    print(f"  scored_pairs:    {len(scored_pairs)} rows")
    print(f"  preferences:     {len(preference_pairs)} rows")
    print(f"  intents:         {len(intent_labels)} rows")


if __name__ == "__main__":
    asyncio.run(run_trial())
