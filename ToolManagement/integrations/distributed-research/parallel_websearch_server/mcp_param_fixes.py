"""
MCP Parameter Fixes - Handle JSON serialization issues in MCP tool calls.
"""

import json
from typing import Any, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


def fix_json_parameter(param: Any) -> Any:
    """
    Fix JSON parameters that are incorrectly passed as strings.
    MCP tools sometimes stringify JSON objects/arrays instead of passing them directly.
    """
    if not isinstance(param, str):
        return param

    param = param.strip()

    # Check if it looks like JSON
    if (param.startswith('{') and param.endswith('}')) or \
       (param.startswith('[') and param.endswith(']')):
        try:
            return json.loads(param)
        except json.JSONDecodeError as e:
            logger.debug(f"Failed to parse JSON parameter: {e}")
            return param

    return param


def fix_task_input(input_data: Any, task_spec: Optional[Dict] = None) -> Union[str, Dict]:
    """
    Fix task input format based on Parallel API requirements.

    Rules:
    - Without task_spec: input should be a string
    - With task_spec: input should be a dict/object
    """
    # First fix any JSON serialization issues
    input_data = fix_json_parameter(input_data)

    if task_spec:
        # With task_spec, input must be an object
        if isinstance(input_data, str):
            # Wrap string in a query object
            return {"query": input_data}
        elif isinstance(input_data, dict):
            return input_data
        else:
            return {"query": str(input_data)}
    else:
        # Without task_spec, input must be a string
        if isinstance(input_data, dict):
            # Extract query field or convert to string
            if "query" in input_data:
                return input_data["query"]
            else:
                # For simple dicts, try to extract a meaningful string
                if len(input_data) == 1:
                    return str(list(input_data.values())[0])
                return json.dumps(input_data)
        elif isinstance(input_data, str):
            return input_data
        else:
            return str(input_data)


def fix_metadata_parameter(metadata: Any) -> Optional[Dict[str, Any]]:
    """Fix metadata parameter that might be JSON string."""
    if metadata is None:
        return None

    metadata = fix_json_parameter(metadata)

    if isinstance(metadata, dict):
        return metadata
    elif isinstance(metadata, str):
        # Try to parse as JSON one more time
        try:
            return json.loads(metadata)
        except:
            # If it's just a string, wrap it
            return {"value": metadata}
    else:
        return {"value": str(metadata)}


def fix_runs_parameter(runs: Any) -> List[Dict]:
    """Fix runs parameter for task groups."""
    runs = fix_json_parameter(runs)

    if not isinstance(runs, list):
        if isinstance(runs, dict):
            runs = [runs]
        else:
            raise ValueError(f"Invalid runs parameter: expected list, got {type(runs)}")

    fixed_runs = []
    for run in runs:
        if isinstance(run, str):
            # Try to parse as JSON
            run = fix_json_parameter(run)

        if isinstance(run, dict):
            # Fix the input field within each run
            if "input" in run:
                run["input"] = fix_task_input(
                    run["input"],
                    run.get("task_spec")
                )
            fixed_runs.append(run)
        elif hasattr(run, 'model_dump'):
            # Pydantic model - convert to dict
            run_dict = run.model_dump(exclude_none=True)
            if "input" in run_dict:
                run_dict["input"] = fix_task_input(
                    run_dict["input"],
                    run_dict.get("task_spec")
                )
            fixed_runs.append(run_dict)
        else:
            # Create a minimal run object
            fixed_runs.append({
                "processor": "lite",
                "input": str(run)
            })

    return fixed_runs


def fix_task_inputs_list(task_inputs: Any) -> List[str]:
    """Fix task_inputs parameter for optimizer endpoint."""
    task_inputs = fix_json_parameter(task_inputs)

    if isinstance(task_inputs, str):
        # Split by common delimiters if it's a string
        if ',' in task_inputs:
            task_inputs = [t.strip() for t in task_inputs.split(',')]
        else:
            task_inputs = [task_inputs]
    elif not isinstance(task_inputs, list):
        task_inputs = [str(task_inputs)]

    # Ensure all items are strings
    return [str(item) for item in task_inputs]