"""
Tool formatters for different LLM providers.
"""
from typing import List, Dict, Any, Literal


def format_for_openai(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Format tools for OpenAI function calling.

    Args:
        tools: List of tool dictionaries with name, description, input_schema

    Returns:
        List of OpenAI function definitions
    """
    openai_functions = []
    for tool in tools:
        openai_function = {
            "type": "function",
            "function": {
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema", {})
            }
        }
        openai_functions.append(openai_function)
    return openai_functions


def format_for_anthropic(tools: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Format tools for Anthropic Claude function calling.

    Args:
        tools: List of tool dictionaries with name, description, input_schema

    Returns:
        List of Anthropic tool definitions
    """
    anthropic_tools = []
    for tool in tools:
        anthropic_tool = {
            "name": tool["name"],
            "description": tool.get("description", ""),
            "input_schema": tool.get("input_schema", {})
        }
        anthropic_tools.append(anthropic_tool)
    return anthropic_tools


def format_tools(
    tools: List[Dict[str, Any]],
    for_model: Literal["anthropic", "openai"] = "anthropic"
) -> List[Dict[str, Any]]:
    """
    Format tools for the specified model.

    Args:
        tools: List of tool dictionaries
        for_model: Target model format ("anthropic" or "openai")

    Returns:
        Formatted tool definitions
    """
    if for_model == "openai":
        return format_for_openai(tools)
    elif for_model == "anthropic":
        return format_for_anthropic(tools)
    else:
        return tools
