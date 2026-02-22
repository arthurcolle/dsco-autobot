"""
MCP Task Types and Routing for Swarm Economy.

Maps swarm task types to MCP backend tools for autonomous agent execution.
"""

from enum import Enum
from typing import Dict, Tuple


class MCPTaskType(str, Enum):
    """Task types that map to MCP backend capabilities."""

    # Search MCP - Web search and content retrieval
    SEARCH_WEB = "search_web"
    SEARCH_READ = "search_read"
    SEARCH_VERIFY = "search_verify"

    # Research MCP - Deep research and analysis
    RESEARCH_DEEP = "research_deep"
    RESEARCH_EXTRACT = "research_extract"
    RESEARCH_SYNTHESIZE = "research_synthesize"

    # Memory MCP - Knowledge storage and recall
    MEMORY_STORE = "memory_store"
    MEMORY_RECALL = "memory_recall"
    MEMORY_LEARN_SKILL = "memory_learn_skill"
    MEMORY_STORE_BELIEF = "memory_store_belief"
    MEMORY_QUERY_BELIEFS = "memory_query_beliefs"

    # Publish MCP - Event publishing and notifications
    PUBLISH_EVENT = "publish_event"
    PUBLISH_NOTIFY = "publish_notify"
    PUBLISH_BROADCAST = "publish_broadcast"


# Maps task types to (backend_id, tool_name)
MCP_TASK_ROUTING: Dict[MCPTaskType, Tuple[str, str]] = {
    # Search MCP routing
    MCPTaskType.SEARCH_WEB: ("distributed-search-mcp", "search"),
    MCPTaskType.SEARCH_READ: ("distributed-search-mcp", "read"),
    MCPTaskType.SEARCH_VERIFY: ("distributed-search-mcp", "verify"),

    # Research MCP routing
    MCPTaskType.RESEARCH_DEEP: ("distributed-research-mcp", "deep_search"),
    MCPTaskType.RESEARCH_EXTRACT: ("distributed-research-mcp", "extract"),
    MCPTaskType.RESEARCH_SYNTHESIZE: ("distributed-research-mcp", "synthesize"),

    # Memory MCP routing
    MCPTaskType.MEMORY_STORE: ("distributed-memory-mcp", "store"),
    MCPTaskType.MEMORY_RECALL: ("distributed-memory-mcp", "recall"),
    MCPTaskType.MEMORY_LEARN_SKILL: ("distributed-memory-mcp", "learn_skill"),
    MCPTaskType.MEMORY_STORE_BELIEF: ("distributed-memory-mcp", "store_belief"),
    MCPTaskType.MEMORY_QUERY_BELIEFS: ("distributed-memory-mcp", "query_beliefs"),

    # Publish MCP routing
    MCPTaskType.PUBLISH_EVENT: ("distributed-publish-mcp", "publish_event"),
    MCPTaskType.PUBLISH_NOTIFY: ("distributed-publish-mcp", "notify"),
    MCPTaskType.PUBLISH_BROADCAST: ("distributed-publish-mcp", "broadcast"),
}


# Backend capability groups
MCP_BACKEND_CAPABILITIES: Dict[str, Dict] = {
    "distributed-search-mcp": {
        "name": "Search MCP",
        "description": "Web search and content retrieval",
        "tools": ["search", "read", "verify"],
        "task_types": [MCPTaskType.SEARCH_WEB, MCPTaskType.SEARCH_READ, MCPTaskType.SEARCH_VERIFY],
    },
    "distributed-research-mcp": {
        "name": "Research MCP",
        "description": "Deep research, extraction, and synthesis",
        "tools": ["deep_search", "extract", "synthesize"],
        "task_types": [MCPTaskType.RESEARCH_DEEP, MCPTaskType.RESEARCH_EXTRACT, MCPTaskType.RESEARCH_SYNTHESIZE],
    },
    "distributed-memory-mcp": {
        "name": "Memory MCP",
        "description": "Knowledge storage, recall, and skill learning",
        "tools": ["store", "recall", "learn_skill", "store_belief", "query_beliefs"],
        "task_types": [
            MCPTaskType.MEMORY_STORE,
            MCPTaskType.MEMORY_RECALL,
            MCPTaskType.MEMORY_LEARN_SKILL,
            MCPTaskType.MEMORY_STORE_BELIEF,
            MCPTaskType.MEMORY_QUERY_BELIEFS,
        ],
    },
    "distributed-publish-mcp": {
        "name": "Publish MCP",
        "description": "Event publishing and notifications",
        "tools": ["publish_event", "notify", "broadcast"],
        "task_types": [MCPTaskType.PUBLISH_EVENT, MCPTaskType.PUBLISH_NOTIFY, MCPTaskType.PUBLISH_BROADCAST],
    },
}


def get_backend_for_task(task_type: str) -> Tuple[str, str]:
    """Get the backend_id and tool for a task type."""
    try:
        mcp_type = MCPTaskType(task_type)
        return MCP_TASK_ROUTING[mcp_type]
    except (ValueError, KeyError):
        raise ValueError(f"Unknown MCP task type: {task_type}")


def get_task_types_for_backend(backend_id: str) -> list:
    """Get all task types supported by a backend."""
    if backend_id in MCP_BACKEND_CAPABILITIES:
        return MCP_BACKEND_CAPABILITIES[backend_id]["task_types"]
    return []


def is_mcp_task_type(task_type: str) -> bool:
    """Check if a task type is an MCP task type."""
    try:
        MCPTaskType(task_type)
        return True
    except ValueError:
        return False
