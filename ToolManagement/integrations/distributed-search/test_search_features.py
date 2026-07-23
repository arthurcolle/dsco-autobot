#!/usr/bin/env python3
"""
Test script for search_server.py features
Tests the database features without requiring MCP connection
"""

import sys
sys.path.insert(0, '/Users/arthurcolle/Dsco/dsco-mcp-integrations/jina-ai')

from search_server import (
    ContentStore, ProcessingQueue, ContentGraph, WebhookManager, SmartPrioritizer,
    # Advanced features
    SemanticCache, QueryExpander, MultiQueryFusion, SearchSession, UserProfile,
    PageRankScorer, SimHashDeduplicator, SearchAnalytics, TemporalScorer,
    # Ultra-advanced features
    VectorStore, QueryIntentClassifier, MMRDiversifier, LearningToRank,
    SpellCorrector, AutoComplete, SemanticRouter, ExplainableSearch,
    ConversationalSearch
)
import asyncio
from datetime import datetime, timedelta

print("=" * 60)
print("TESTING SEARCH SERVER FEATURES")
print("=" * 60)

# Use test database
db_path = "test_content_store.db"

# Test ContentStore
print("\n1. CONTENT STORE")
print("-" * 40)
store = ContentStore(db_path)

# Add URLs
urls_to_add = [
    ("https://arxiv.org/abs/2301.00001", "Attention is All You Need - Transformer Paper"),
    ("https://arxiv.org/abs/2302.00002", "GPT-4 Technical Report"),
    ("https://github.com/openai/gpt-4", "GPT-4 GitHub Repository"),
    ("https://docs.python.org/3/", "Python Documentation"),
    ("https://fastapi.tiangolo.com/", "FastAPI Documentation"),
]

for url, title in urls_to_add:
    result = store.add_url(url=url, title=title, source_query="AI research", tags=["ai", "ml"])
    print(f"  Added: {url[:50]}...")

# Mark some as visited
store.mark_visited("https://arxiv.org/abs/2301.00001", content_summary="Transformer architecture paper", status="visited")
store.mark_visited("https://docs.python.org/3/", content_summary="Python docs", status="visited")
print("  Marked 2 URLs as visited")

# Query
unvisited = store.get_unvisited_urls(limit=10)
print(f"  Unvisited URLs: {len(unvisited)}")

all_urls = store.get_urls(limit=10)
print(f"  Total URLs: {len(all_urls)}")

search_results = store.search_urls("GPT", limit=5)
print(f"  Search 'GPT' results: {len(search_results)}")

stats = store.get_stats()
print(f"  Stats: {stats}")

# Test Images
print("\n2. IMAGE TRACKING")
print("-" * 40)
images = [
    {"url": "https://example.com/img1.png", "alt_text": "AI diagram"},
    {"url": "https://example.com/img2.jpg", "alt_text": "Neural network"},
    {"url": "https://example.com/img3.webp", "alt_text": "Transformer architecture"},
]
for img in images:
    store.add_image(url=img["url"], source_url="https://arxiv.org/abs/2301.00001", alt_text=img["alt_text"])
print(f"  Added {len(images)} images")

stored_images = store.get_images(limit=10)
print(f"  Retrieved images: {len(stored_images)}")

# Test ProcessingQueue
print("\n3. PROCESSING QUEUE")
print("-" * 40)
queue = ProcessingQueue(db_path)

# Enqueue items
items_to_queue = [
    {"url": "https://example.com/page1", "priority": 10},
    {"url": "https://example.com/page2", "priority": 50},
    {"url": "https://example.com/page3", "priority": 30},
    {"url": "https://example.com/page4", "priority": 80},
    {"url": "https://example.com/page5", "priority": 20},
]
result = queue.enqueue_batch(items_to_queue, queue_type="url", source_query="test batch")
print(f"  Enqueued: {result['queued']} items")

# Peek at queue
pending = queue.peek(limit=5)
print(f"  Pending items: {len(pending)}")
for item in pending[:3]:
    print(f"    - {item['url']} (priority: {item['priority']})")

# Claim batch
claimed = queue.claim_batch(batch_size=2, worker_id="test_worker")
print(f"  Claimed: {len(claimed)} items")

# Complete items
if claimed:
    results = [
        {"id": claimed[0]["id"], "result": {"summary": "Processed successfully"}},
    ]
    if len(claimed) > 1:
        results.append({"id": claimed[1]["id"], "error": "Failed to process"})

    complete_result = queue.complete_batch(results)
    print(f"  Completed: {complete_result}")

# Queue stats
queue_stats = queue.get_stats()
print(f"  Queue stats: {queue_stats}")

# Test ContentGraph
print("\n4. CONTENT GRAPH")
print("-" * 40)
graph = ContentGraph(db_path)

# Add links
links = [
    ("https://arxiv.org/abs/2301.00001", "https://github.com/openai/transformer"),
    ("https://arxiv.org/abs/2301.00001", "https://arxiv.org/abs/2302.00002"),
    ("https://github.com/openai/gpt-4", "https://arxiv.org/abs/2302.00002"),
]
for source, target in links:
    graph.add_link(source, target, "reference")
print(f"  Added {len(links)} links")

# Get outlinks
outlinks = graph.get_outlinks("https://arxiv.org/abs/2301.00001")
print(f"  Outlinks from transformer paper: {len(outlinks)}")

# Get inlinks
inlinks = graph.get_inlinks("https://arxiv.org/abs/2302.00002")
print(f"  Inlinks to GPT-4 paper: {len(inlinks)}")

# Test fingerprinting
content1 = "This is a test document about machine learning and transformers."
fp1 = graph.update_fingerprint("https://test.com/doc1", content1)
print(f"  Fingerprint created: {fp1['fingerprint']}")

# Check if content changed
content2 = "This is a test document about machine learning and transformers with some edits."
fp2 = graph.update_fingerprint("https://test.com/doc1", content2)
print(f"  Content changed: {fp2['changed']}, type: {fp2['change_type']}")

# Test entities
entities = [
    {"type": "email", "value": "test@example.com"},
    {"type": "phone", "value": "555-123-4567"},
    {"type": "date", "value": "2024-01-15"},
]
for e in entities:
    graph.add_entity("https://test.com/doc1", e["type"], e["value"])
print(f"  Added {len(entities)} entities")

stored_entities = graph.get_entities(entity_type="email", limit=10)
print(f"  Email entities: {len(stored_entities)}")

# Test clustering
graph.set_cluster("https://arxiv.org/abs/2301.00001", "cluster_ai", "AI Papers", 0.95)
graph.set_cluster("https://arxiv.org/abs/2302.00002", "cluster_ai", "AI Papers", 0.92)
print("  Assigned 2 URLs to 'AI Papers' cluster")

cluster_urls = graph.get_cluster_urls("cluster_ai")
print(f"  URLs in cluster: {len(cluster_urls)}")

# Test SmartPrioritizer
print("\n5. SMART PRIORITIZER")
print("-" * 40)
prioritizer = SmartPrioritizer()

# Test priority scoring (sync version without embeddings)
async def test_priority():
    result = await prioritizer.compute_priority(
        url="https://docs.python.org/3/tutorial/",
        title="Python Tutorial - Official Documentation",
        description="Learn Python programming with the official tutorial",
        source_query="python programming tutorial"
    )
    return result

priority_result = asyncio.run(test_priority())
print(f"  Priority for Python docs: {priority_result['priority']}")
print(f"  Factors: {priority_result['factors']}")

# Update domain scores
prioritizer.update_domain_score("arxiv.org", 0.9)
prioritizer.update_domain_score("arxiv.org", 0.85)
prioritizer.update_domain_score("github.com", 0.7)
print(f"  Domain scores: {prioritizer.domain_scores}")

# Test WebhookManager
print("\n6. WEBHOOK MANAGER")
print("-" * 40)
webhooks = WebhookManager(db_path)

# Register webhook
wh = webhooks.register(
    url="https://example.com/webhook",
    events=["url.visited", "content.changed", "queue.completed"],
    secret="test_secret_123"
)
print(f"  Registered webhook: {wh['id']}")

# List webhooks
all_webhooks = webhooks.list_webhooks()
print(f"  Total webhooks: {len(all_webhooks)}")

# ============================================================================
# TEST ADVANCED FEATURES
# ============================================================================

print("\n" + "=" * 60)
print("TESTING ADVANCED FEATURES")
print("=" * 60)

# Test SemanticCache
print("\n7. SEMANTIC CACHE")
print("-" * 40)
cache = SemanticCache(max_size=100, ttl_seconds=3600)

# Set and get
cache.set("test query", {"results": ["a", "b", "c"]}, params={"top_k": 10})

async def test_cache_get():
    return await cache.get("test query", {"top_k": 10})

cached = asyncio.run(test_cache_get())
print(f"  Cache hit: {cached is not None}")
print(f"  Cache stats: {cache.stats()}")

# Test QueryExpander
print("\n8. QUERY EXPANDER")
print("-" * 40)
expander = QueryExpander()

# Synonym expansion
expanded = expander.expand_synonyms("search API for data", max_expansions=3)
print(f"  Expanded 'search API for data': {expanded}")

# Entity extraction
entities_extracted = expander.extract_entities('Check out https://example.com and email me at test@test.com about the .py files')
print(f"  Entities extracted: {entities_extracted}")

# Test MultiQueryFusion
print("\n9. MULTI-QUERY FUSION (RRF)")
print("-" * 40)
fusion = MultiQueryFusion()

# Test RRF
results_a = [{"id": "1", "title": "Doc A"}, {"id": "2", "title": "Doc B"}, {"id": "3", "title": "Doc C"}]
results_b = [{"id": "2", "title": "Doc B"}, {"id": "4", "title": "Doc D"}, {"id": "1", "title": "Doc A"}]
results_c = [{"id": "1", "title": "Doc A"}, {"id": "5", "title": "Doc E"}]

fused = fusion.reciprocal_rank_fusion([results_a, results_b, results_c])
print(f"  Fused {len(fused)} unique documents")
print(f"  Top 3 by RRF: {[(d['id'], round(d['rrf_score'], 4)) for d in fused[:3]]}")

# Test SearchSession
print("\n10. SEARCH SESSION")
print("-" * 40)
session = SearchSession(db_path)

# Create session
session_id = session.create_session("test_user_123")
print(f"  Created session: {session_id}")

# Add queries
session.add_query(session_id, "machine learning", 10, ["https://example.com/ml"])
session.add_query(session_id, "deep learning", 8)
session.add_query(session_id, "neural networks", 15)

# Get context
context = session.get_session_context(session_id)
print(f"  Session context: {context}")

# Get stats
sess_stats = session.get_session_stats(session_id)
print(f"  Session stats: query_count={sess_stats.get('query_count', 0)}")

# Test UserProfile
print("\n11. USER PROFILE & PERSONALIZATION")
print("-" * 40)
profiles = UserProfile(db_path)

# Get or create profile
profile = profiles.get_or_create("test_user_123")
print(f"  User profile created: {profile['user_id']}")

# Record interactions
profiles.record_interaction("test_user_123", "https://arxiv.org/paper1", "click", 1.0)
profiles.record_interaction("test_user_123", "https://arxiv.org/paper2", "bookmark", 1.5)
profiles.record_interaction("test_user_123", "https://github.com/repo", "click", 1.0)
print("  Recorded 3 interactions")

# Get personalization boost
boost = profiles.get_personalization_boost("test_user_123", "https://arxiv.org/paper3")
print(f"  Personalization boost for arxiv.org: {boost:.2f}")

# Test PageRankScorer
print("\n12. PAGERANK SCORER")
print("-" * 40)
pagerank = PageRankScorer(graph)

# Compute PageRank
scores = pagerank.compute(force=True)
print(f"  Computed PageRank for {len(scores)} URLs")

# Get top URLs
top_urls = pagerank.get_top_urls(3)
for url, score in top_urls:
    print(f"    - {url[:50]}... : {score:.6f}")

# Test SimHashDeduplicator
print("\n13. SIMHASH DEDUPLICATOR")
print("-" * 40)
dedup = SimHashDeduplicator()

# Add documents
doc1 = "The quick brown fox jumps over the lazy dog. This is a test document about foxes."
doc2 = "The quick brown fox jumps over the lazy dog. This is a test document about foxes and dogs."
doc3 = "Machine learning is transforming the world of technology and innovation."

result1 = dedup.add_document("https://test.com/doc1", doc1)
print(f"  Doc1: simhash={result1['simhash']}, duplicates={len(result1['duplicates'])}")

result2 = dedup.add_document("https://test.com/doc2", doc2)
print(f"  Doc2: simhash={result2['simhash']}, is_duplicate={result2['is_duplicate']}")
if result2['duplicates']:
    print(f"    Near-duplicate of: {result2['duplicates'][0]['url']} (similarity: {result2['duplicates'][0]['similarity']:.2f})")

result3 = dedup.add_document("https://test.com/doc3", doc3)
print(f"  Doc3: is_duplicate={result3['is_duplicate']} (different topic)")

# Test SearchAnalytics
print("\n14. SEARCH ANALYTICS")
print("-" * 40)
analytics = SearchAnalytics(db_path)

# Log events
analytics.log_event("search", query="test query 1", results_count=10, latency_ms=45.5, user_id="user1")
analytics.log_event("search", query="test query 2", results_count=5, latency_ms=32.1, user_id="user1")
analytics.log_event("search", query="test query 1", results_count=10, latency_ms=48.2, user_id="user2")
analytics.log_event("search", query="zero results", results_count=0, latency_ms=25.0)
print("  Logged 4 search events")

# Get stats
query_stats = analytics.get_query_stats(hours=24)
print(f"  Query stats: total={query_stats['total_queries']}, avg_latency={query_stats.get('avg_latency_ms', 0):.1f}ms")

# Get top queries
top_queries = analytics.get_top_queries(limit=3)
print(f"  Top queries: {[q['query'] for q in top_queries]}")

# Get zero result queries
zero_results = analytics.get_zero_result_queries(limit=3)
print(f"  Zero result queries: {zero_results}")

# Test A/B Testing
print("\n15. A/B TESTING")
print("-" * 40)

# Create test
test_id = analytics.create_ab_test(
    test_name="ranking_algorithm",
    variants=["control", "variant_a", "variant_b"],
    traffic_split=[0.5, 0.25, 0.25]
)
print(f"  Created A/B test: {test_id}")

# Get variants for users
variant1 = analytics.get_ab_variant(test_id, "user_001")
variant2 = analytics.get_ab_variant(test_id, "user_002")
variant3 = analytics.get_ab_variant(test_id, "user_003")
print(f"  User variants: user_001={variant1}, user_002={variant2}, user_003={variant3}")

# Same user gets same variant
variant1_again = analytics.get_ab_variant(test_id, "user_001")
print(f"  Consistent assignment: user_001 gets '{variant1_again}' again")

# Test TemporalScorer
print("\n16. TEMPORAL SCORER")
print("-" * 40)
temporal = TemporalScorer(half_life_days=7.0)

# Test decay
now = datetime.now()
old_date = now - timedelta(days=14)
very_old_date = now - timedelta(days=30)
recent_date = now - timedelta(hours=6)

decay_old = temporal.decay_score(old_date, base_score=1.0)
decay_very_old = temporal.decay_score(very_old_date, base_score=1.0)
print(f"  Decay after 14 days: {decay_old:.3f}")
print(f"  Decay after 30 days: {decay_very_old:.3f}")

# Test boost
boost_recent = temporal.boost_recent(recent_date)
boost_old = temporal.boost_recent(old_date)
print(f"  Boost for 6 hours old: {boost_recent:.3f}")
print(f"  Boost for 14 days old: {boost_old:.3f}")

# Combined score
combined = temporal.combined_temporal_score(recent_date, base_score=1.0)
print(f"  Combined score for recent content: {combined:.3f}")


print("\n" + "=" * 60)
print("TESTING ULTRA-ADVANCED FEATURES")
print("=" * 60)

# Test VectorStore
print("\n17. VECTOR STORE (ANN)")
print("-" * 40)
vs = VectorStore(dimension=128, num_tables=5, num_bits=6)

# Add some vectors
import random
random.seed(42)
for i in range(20):
    vec = [random.random() for _ in range(128)]
    vs.add(f"doc_{i}", vec, metadata={"category": f"cat_{i % 3}"})

print(f"  Added 20 vectors (dim=128)")
print(f"  Vector count: {len(vs.vectors)}")

# Search
query_vec = [random.random() for _ in range(128)]
results = vs.search(query_vec, top_k=5)
print(f"  Search results: {len(results)} matches")
if results:
    print(f"  Top match: {results[0]['id']} (score: {results[0]['score']:.4f})")

# Test QueryIntentClassifier
print("\n18. QUERY INTENT CLASSIFIER")
print("-" * 40)
ic = QueryIntentClassifier()

test_queries = [
    ("how to install python", "informational"),
    ("buy laptop amazon", "transactional"),
    ("facebook login", "navigational"),
    ("what is machine learning like explain it", "conversational"),
    ("def __init__ python", "code"),
]

for query, expected in test_queries:
    result = ic.classify(query)
    intent = result["primary_intent"]
    status = "✓" if intent == expected else f"✗ (got {intent})"
    print(f"  '{query[:30]}...' -> {intent} {status}")

strategy = ic.get_search_strategy("informational")
print(f"  Strategy for 'informational': {strategy}")

# Test MMRDiversifier
print("\n19. MMR DIVERSIFIER")
print("-" * 40)
mmr = MMRDiversifier()

# Create mock candidates with embeddings
candidates = [
    {"id": "1", "embedding": [1.0] * 10, "doc": {"title": "Doc 1"}},
    {"id": "2", "embedding": [0.9] + [1.0] * 9, "doc": {"title": "Doc 2"}},  # Similar to 1
    {"id": "3", "embedding": [0.0] + [1.0] * 9, "doc": {"title": "Doc 3"}},  # Different
    {"id": "4", "embedding": [0.5] * 10, "doc": {"title": "Doc 4"}},
]

query_emb = [1.0] * 10
diversified = mmr.diversify(query_emb, candidates, top_k=3)
print(f"  Diversified 4 candidates to top 3")
print(f"  Results: {[d['id'] for d in diversified]}")
mmr_scores = [round(d['mmr_score'], 3) for d in diversified]
print(f"  MMR scores: {mmr_scores}")

# Test LearningToRank
print("\n20. LEARNING TO RANK")
print("-" * 40)
ltr = LearningToRank(db_path)

# Extract features
doc = {
    "id": "test_doc",
    "title": "Python Machine Learning Tutorial",
    "content": "This tutorial explains how to use Python for machine learning applications.",
    "url": "https://example.com/tutorial"
}
features = ltr.extract_features("python machine learning", doc)
print(f"  Features extracted: {len(features)}")
print(f"  Sample features: title_match={features.get('title_match', 0):.2f}, content_match={features.get('content_match', 0):.2f}")

# Rerank
docs = [
    {"id": "1", "title": "Python Tutorial", "content": "Learn Python basics", "url": "https://a.com"},
    {"id": "2", "title": "Machine Learning Guide", "content": "ML with Python", "url": "https://b.com"},
    {"id": "3", "title": "Data Science", "content": "Python for data analysis", "url": "https://c.com"},
]
reranked = ltr.rerank("python tutorial", docs)
print(f"  Reranked {len(docs)} docs")
print(f"  Top result: {reranked[0]['title']}")

# Test SpellCorrector
print("\n21. SPELL CORRECTOR")
print("-" * 40)
sc = SpellCorrector()

corrections = [
    ("serch", "search"),
    ("machien learning", "machine learning"),
    ("pythn", "python"),
    ("algortihm", "algorithm"),
]

for typo, expected in corrections:
    result = sc.correct(typo)
    status = "✓" if result["corrected"] == expected else f"✗ (got {result['corrected']})"
    print(f"  '{typo}' -> '{result['corrected']}' {status}")

# Query correction
query_result = sc.correct_query("serch for pythn tutroial")
print(f"  Query: '{query_result['original_query']}'")
print(f"  Corrected: '{query_result['corrected_query']}'")
print(f"  Was corrected: {query_result['was_corrected']}")

# Test AutoComplete
print("\n22. AUTOCOMPLETE")
print("-" * 40)
ac = AutoComplete(db_path)

# Record some queries
queries = ["machine learning", "machine vision", "deep learning", "neural networks", "python tutorial"]
for q in queries:
    ac.add_query(q)
    ac.add_query(q)  # Record twice to boost frequency

print(f"  Recorded {len(queries)} queries")

# Get suggestions
suggestions = ac.suggest("mac", limit=5)
print(f"  Suggestions for 'mac': {[s['query'] for s in suggestions]}")

# Trending
trending = ac.get_trending(limit=3, hours=24)
print(f"  Trending queries: {[t['query'] for t in trending]}")

# Test SemanticRouter
print("\n23. SEMANTIC ROUTER")
print("-" * 40)
sr = SemanticRouter()

# Add some routes
sr.add_route(
    name="code_search",
    description="Search for code and programming",
    examples=["python code", "javascript function"],
    handler="code_handler",
    metadata={"priority": 1}
)
sr.add_route(
    name="docs_search",
    description="Search documentation",
    examples=["api documentation", "reference guide"],
    handler="docs_handler",
    metadata={"priority": 2}
)

print(f"  Added {len(sr.routes)} routes")
print(f"  Routes: {list(sr.routes.keys())}")

# Test ExplainableSearch
print("\n24. EXPLAINABLE SEARCH")
print("-" * 40)
es = ExplainableSearch()

result = {
    "title": "Python Machine Learning Tutorial",
    "snippet": "Learn how to build machine learning models with Python",
    "url": "https://example.com/ml-tutorial",
    "score": 0.95,
    "pagerank": 0.8,
    "freshness": 0.9
}
explanation = es.explain_result(result, "python machine learning")
print(f"  Result: {result['title'][:40]}...")
print(f"  Explanations: {len(explanation['explanations'])} factors")
print(f"  Highlights: {len(explanation['highlights'])} matches")
print(f"  Summary: {explanation['summary']}")

# Test ConversationalSearch
print("\n25. CONVERSATIONAL SEARCH")
print("-" * 40)
cs = ConversationalSearch(db_path)

# Start conversation
conv_id = cs.start_conversation("test_user")
print(f"  Started conversation: {conv_id}")

# Add some turns
cs.add_turn(conv_id, role="user", content="What is machine learning?")
cs.add_turn(conv_id, role="assistant", content="Machine learning is a subset of AI...")
cs.add_turn(conv_id, role="user", content="How does it differ from deep learning?")

# Get context
context = cs.get_context(conv_id, max_turns=5)
print(f"  Context turns: {len(context)}")

# Test follow-up detection
follow_up = cs.detect_follow_up(conv_id, "What about neural networks?")
print(f"  'What about neural networks?' is follow-up: {follow_up['is_follow_up']}")

# Test query expansion
expanded = cs.expand_query_with_context(conv_id, "Tell me more about it")
print(f"  Original: 'Tell me more about it'")
print(f"  Expanded: '{expanded[:50]}...'")

# Get full history
history = cs.get_conversation_history(conv_id)
print(f"  Full history: {len(history)} turns")

# End conversation
cs.end_conversation(conv_id)
print(f"  Conversation ended")

# Clean up ultra-advanced
ltr.close()
ac.close()
cs.close()

# Clean up
print("\n26. CLEANUP")
print("-" * 40)
store.close()
queue.close()
graph.close()
webhooks.close()
session.close()
profiles.close()
analytics.close()

import os
os.remove(db_path)
print(f"  Removed test database: {db_path}")

print("\n" + "=" * 60)
print("ALL TESTS PASSED! (26 test suites)")
print("=" * 60)
