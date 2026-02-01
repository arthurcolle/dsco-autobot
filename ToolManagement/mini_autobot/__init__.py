"""
Mini Autobot - Tool Management System with Neo4j Graph Backend.

Components:
- graph_api: Neo4j graph operations API (port 2015)
- tool_api: Tool management API (port 2016)
- client: Client library with decorators for registering tools
- common: Shared schemas and utilities

Quick Start:
    # Start Graph API (Terminal 1)
    NEO4J_PASSWORD=your_password python -m mini_autobot.graph_api.server

    # Start Tool API (Terminal 2)
    GRAPH_API_URL=http://localhost:2015 python -m mini_autobot.tool_api.server

    # Use client in your service
    from mini_autobot.client import ToolManagementClient

    tools = ToolManagementClient(
        tool_api_url="http://localhost:2016",
        service_url="http://localhost:8000"
    )

    @tools.endpoint(description="My endpoint")
    async def my_endpoint(input: MyInput) -> MyOutput:
        ...

    await tools.register_tools()
"""

__version__ = "1.0.0"
