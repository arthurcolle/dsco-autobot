import pytest

from tool_management.integrations.mcp_server import MCPServer


@pytest.mark.asyncio
async def test_meta_tools_include_policy_recommendation_and_gate_controls():
    server = MCPServer(tool_storage={})
    result = await server._handle_tools_list({}, session_id="session_meta_tools")
    names = {tool["name"] for tool in result["tools"]}

    assert "get_execution_policy_decisions" in names
    assert "get_execution_policy_analytics" in names
    assert "recommend_tools" in names
    assert "record_recommendation_feedback" in names
    assert "get_recommendation_feedback_analytics" in names
    assert "record_workflow_gate_decision" in names
    assert "list_workflow_gate_decisions" in names
    assert "generate_execution_follow_ups" in names
    assert "list_execution_follow_ups" in names
    assert "get_execution_follow_up_analytics" in names
    assert "update_execution_follow_up_status" in names
