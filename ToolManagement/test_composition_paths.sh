#!/bin/bash
#
# Compositional Workflow Test Suite
# Tests various path sequences through connected MCP tools
#

set -e

BASE_URL="http://localhost:2016"
COMPOSE_URL="$BASE_URL/api/v1/compose"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║           COMPOSITIONAL WORKFLOW TEST SUITE                      ║"
echo "║         Testing Different Tool Path Sequences                    ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Test counter
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

run_test() {
    local test_name="$1"
    local test_cmd="$2"
    local expected_status="${3:-completed}"

    TESTS_RUN=$((TESTS_RUN + 1))

    echo -e "${BLUE}[TEST $TESTS_RUN]${NC} $test_name"
    echo "Command: $test_cmd"

    result=$(eval "$test_cmd" 2>&1)
    status=$(echo "$result" | jq -r '.status // .workflow_type // "unknown"' 2>/dev/null || echo "error")

    if [[ "$status" == "$expected_status" ]] || [[ "$status" == "completed" ]]; then
        echo -e "${GREEN}✓ PASS${NC}"
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo "$result" | jq '.' 2>/dev/null || echo "$result"
    else
        echo -e "${RED}✗ FAIL${NC} (status: $status)"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo "$result"
    fi

    echo ""
    echo "---"
    echo ""
}

# =============================================================================
# TEST 1: SEQUENTIAL - Search → Read → Verify
# =============================================================================

run_test "Sequential: Search → Read → Verify" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"search\", \"read\", \"verify\"],
        \"arguments\": [
            {\"query\": \"AI trends 2024\", \"max_results\": 1},
            {\"url\": \"https://example.com\"},
            {\"statement\": \"AI is growing\"}
        ]
    }'"

# =============================================================================
# TEST 2: MAP - Apply Search to Multiple Queries
# =============================================================================

run_test "Map: Search Multiple Queries" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"map\",
        \"tools\": [\"search\"],
        \"arguments\": [{\"max_results\": 2}],
        \"inputs\": {
            \"items\": [
                \"AI trends\",
                \"Machine learning\",
                \"LLM applications\"
            ]
        }
    }'"

# =============================================================================
# TEST 3: PARALLEL - Run Multiple Searches Concurrently
# =============================================================================

run_test "Parallel: Search + Deep Research + List MCPs" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"parallel\",
        \"tools\": [\"search\", \"deep_search\", \"list_native_mcps\"],
        \"arguments\": [
            {\"query\": \"AI\"},
            {\"query\": \"Machine learning\"},
            {\"include_tools\": false}
        ]
    }'"

# =============================================================================
# TEST 4: MAP_REDUCE - Search Multiple → Aggregate Results
# =============================================================================

run_test "Map-Reduce: Search Multiple → Aggregate" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"map_reduce\",
        \"tools\": [\"search\", \"list_tools\"],
        \"arguments\": [
            {\"max_results\": 2},
            {\"limit\": 5}
        ],
        \"inputs\": {
            \"items\": [\"AI\", \"ML\", \"NLP\"]
        }
    }'"

# =============================================================================
# TEST 5: SEGMENT_PROCESS - Segment → Classify → Aggregate
# =============================================================================

run_test "Segment-Process: Text → Segments → Process" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"segment_process\",
        \"tools\": [\"search\", \"read\"],
        \"arguments\": [
            {\"query\": \"test\", \"max_results\": 1},
            {}
        ],
        \"inputs\": {
            \"text\": \"This is a test document for segmentation.\"
        }
    }'"

# =============================================================================
# TEST 6: PIPE - Functional Composition
# =============================================================================

run_test "Pipe: f(g(h(x))) - Nested Composition" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"pipe\",
        \"tools\": [\"list_tools\", \"search_tools\", \"get_registry_stats\"],
        \"arguments\": [
            {\"limit\": 3},
            {\"query\": \"search\"},
            {}
        ]
    }'"

# =============================================================================
# TEST 7: Registry Wrapper - Discover and Execute
# =============================================================================

run_test "Registry Discovery: List → Search → Get Stats" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"list_tools\", \"search_tools\", \"get_registry_stats\"],
        \"arguments\": [
            {\"limit\": 5},
            {\"query\": \"search\", \"limit\": 3},
            {}
        ]
    }'"

# =============================================================================
# TEST 8: Memory Operations - Store and Recall
# =============================================================================

run_test "Memory: Focus → Recall → Consolidate" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"focus\", \"recall\", \"consolidate\"],
        \"arguments\": [
            {\"item\": \"test data\", \"context\": \"testing\"},
            {\"query\": \"test\"},
            {}
        ]
    }'"

# =============================================================================
# TEST 9: Publish Workflow - Create Webhook → Trigger → List Events
# =============================================================================

run_test "Publish: Create Webhook → Trigger → List Events" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"create_webhook\", \"list_webhooks\", \"list_events\"],
        \"arguments\": [
            {\"name\": \"test-webhook\", \"url\": \"http://example.com/hook\", \"events\": [\"test\"]},
            {\"status\": \"all\"},
            {\"limit\": 10}
        ]
    }'"

# =============================================================================
# TEST 10: Research Pipeline - Deep Search → Extract → Synthesize
# =============================================================================

run_test "Research: Deep Search → Extract Data" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"deep_search\", \"extract_data\"],
        \"arguments\": [
            {\"query\": \"AI applications\", \"max_results\": 2},
            {\"url\": \"https://example.com\", \"schema\": {\"title\": \"string\"}}
        ]
    }'"

# =============================================================================
# TEST 11: Multi-Search Pattern - Search + Read + Verify
# =============================================================================

run_test "Multi-Search: Search → Read URLs → Verify Facts" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"search\", \"read\", \"verify\"],
        \"arguments\": [
            {\"query\": \"Claude AI\", \"max_results\": 1},
            {\"url\": \"https://www.anthropic.com\"},
            {\"statement\": \"Claude is an AI assistant\"}
        ]
    }'"

# =============================================================================
# TEST 12: Embedding Pipeline - Search → Embed → Rerank
# =============================================================================

run_test "Embedding: Search → Embed → Rerank" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"search\", \"embed\", \"rerank\"],
        \"arguments\": [
            {\"query\": \"AI\", \"max_results\": 3},
            {\"text\": \"AI is transforming technology\"},
            {\"query\": \"AI\", \"documents\": [\"doc1\", \"doc2\"]}
        ]
    }'"

# =============================================================================
# TEST 13: Notification Workflow - Queue → Send → List
# =============================================================================

run_test "Notifications: Queue → Send → List Events" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"queue_message\", \"send_notification\", \"list_events\"],
        \"arguments\": [
            {\"channel\": \"test-channel\", \"payload\": {\"msg\": \"test\"}},
            {\"title\": \"Test Notification\", \"body\": \"Testing\", \"priority\": \"normal\"},
            {\"type\": \"notification.sent\", \"limit\": 5}
        ]
    }'"

# =============================================================================
# TEST 14: Content Syndication - Create → Syndicate → Broadcast
# =============================================================================

run_test "Syndication: Syndicate → Broadcast" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"syndicate_content\", \"broadcast\"],
        \"arguments\": [
            {
                \"content\": {\"title\": \"Test\", \"body\": \"Content\"},
                \"channels\": [\"channel1\", \"channel2\"]
            },
            {\"message\": {\"type\": \"broadcast\", \"data\": \"test\"}}
        ]
    }'"

# =============================================================================
# TEST 15: Named Workflow - Entity Extraction
# =============================================================================

run_test "Named Workflow: NER Extraction" \
    "curl -s -X POST $COMPOSE_URL/execute-named -H 'Content-Type: application/json' -d '{
        \"workflow_id\": \"named_entity_extraction\",
        \"inputs\": {
            \"text\": \"Apple Inc. was founded by Steve Jobs in Cupertino, California.\"
        }
    }'"

# =============================================================================
# TEST 16: Named Workflow - Text Segmentation
# =============================================================================

run_test "Named Workflow: Text Segmentation" \
    "curl -s -X POST $COMPOSE_URL/execute-named -H 'Content-Type: application/json' -d '{
        \"workflow_id\": \"text_segmentation\",
        \"inputs\": {
            \"text\": \"This is paragraph one. It has multiple sentences. This is paragraph two. It also has content.\"
        }
    }'"

# =============================================================================
# TEST 17: Named Workflow - Semantic Search
# =============================================================================

run_test "Named Workflow: Semantic Search" \
    "curl -s -X POST $COMPOSE_URL/execute-named -H 'Content-Type: application/json' -d '{
        \"workflow_id\": \"semantic_search\",
        \"inputs\": {
            \"query\": \"machine learning applications\",
            \"index\": \"documents\"
        }
    }'"

# =============================================================================
# TEST 18: Complex Chain - Registry Discovery → Tool Execution
# =============================================================================

run_test "Complex: Discover Tools → Execute via Registry" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"search_tools\", \"list_native_mcps\", \"get_registry_stats\"],
        \"arguments\": [
            {\"query\": \"search\", \"limit\": 5},
            {\"include_tools\": true},
            {}
        ]
    }'"

# =============================================================================
# TEST 19: Parallel Research - Multi-Source Search
# =============================================================================

run_test "Parallel Research: Search + Deep Search + Read" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"parallel\",
        \"tools\": [\"search\", \"deep_search\"],
        \"arguments\": [
            {\"query\": \"AI trends\", \"max_results\": 2},
            {\"query\": \"AI applications\", \"max_results\": 2}
        ]
    }'"

# =============================================================================
# TEST 20: Full Stack - Search → Extract → Embed → Store → Notify
# =============================================================================

run_test "Full Stack: Search → Extract → Embed → Notify" \
    "curl -s -X POST $COMPOSE_URL/compose-simple -H 'Content-Type: application/json' -d '{
        \"workflow_type\": \"sequential\",
        \"tools\": [\"search\", \"read\", \"embed\", \"send_notification\"],
        \"arguments\": [
            {\"query\": \"AI\", \"max_results\": 1},
            {\"url\": \"https://example.com\"},
            {\"text\": \"embedding text\"},
            {\"title\": \"Process Complete\", \"body\": \"Search and embed finished\"}
        ]
    }'"

# =============================================================================
# SUMMARY
# =============================================================================

echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║                     TEST SUMMARY                                 ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo -e "║  Total Tests:   ${TESTS_RUN}                                           ║"
echo -e "║  ${GREEN}Passed:        ${TESTS_PASSED}${NC}                                           ║"
echo -e "║  ${RED}Failed:        ${TESTS_FAILED}${NC}                                            ║"
echo "╚══════════════════════════════════════════════════════════════════╝"

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC} ✓"
    exit 0
else
    echo -e "${RED}Some tests failed.${NC} ✗"
    exit 1
fi
