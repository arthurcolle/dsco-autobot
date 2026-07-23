# Tool Management Service with Advanced Interconnectivity

A comprehensive, production-ready FastAPI service for managing and executing tools with advanced interconnectivity features including MCP (Model Context Protocol) support, tool composition, webhooks, and federation capabilities.

## 🚀 Key Features

### Core Capabilities
- **Tool Registration**: Register, update, and delete tools dynamically
- **Advanced Filtering**: Filter by user, organization, toolkit, schemas, UI effects
- **Tool Execution**: Execute tools individually or in complex compositions
- **RESTful API**: Clean API with versioned endpoints
- **Modal Deployment**: Easy deployment to Modal's serverless platform

### 🌐 Interconnectivity Features
- **HATEOAS Support**: True REST with hypermedia-driven navigation (HAL, Siren formats)
- **MCP Server**: Full Model Context Protocol (DRAFT-2025-v3) implementation
- **Tool Composition**: Sequential, parallel, conditional, loop, map-reduce, and pipeline patterns
- **Event System**: Webhook support with retry logic and event bus
- **Protocol Adapters**: Convert between OpenAI, Anthropic, LangChain, and MCP formats
- **Federation**: Connect and sync with other tool registries
- **WebSocket Support**: Real-time MCP communication
- **Content Negotiation**: Support for multiple hypermedia formats

### 🔐 Security & Operations
- **Authentication**: Optional Bearer token authentication
- **CORS Support**: Configurable CORS for cross-origin requests
- **Health Checks**: Built-in health and readiness endpoints
- **Event History**: Track all tool operations and webhook deliveries

## Project Structure

```
ToolManagement/
├── fastapi_app.py           # Main FastAPI application with all endpoints
├── mcp_server.py           # MCP (Model Context Protocol) server implementation
├── interconnectivity.py    # Tool composition, webhooks, federation
├── modal_wrapper.py         # Modal deployment wrapper
├── modal_config.yaml        # Modal configuration
├── tool_mgmt_api_client.py  # Client library for tool management
├── requirements.txt         # Python dependencies
├── example_usage.py         # Usage examples
└── README.md               # This file
```

## Quick Start

### Local Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the FastAPI application locally:
```bash
python fastapi_app.py
```

The API will be available at `http://localhost:8000`

### One-Command CLI

Use `./do` for fast interaction with the deployed Tool Management API:

```bash
./do health
./do list
./do run "daily strategic market brief" --use-case-first
./do recommend "evaluate autonomous agent market shifts"
./do use-cases "market intelligence" --domain finance --limit 10
./do plan "evaluate autonomous agent market shifts"
```

Defaults:
- API URL: `https://tools.distributed.systems`

Advanced CLI options:
- `--timeout <seconds>` default request timeout
- `--retry <count>` retries transient failures (429/5xx/network)
- `--verbose` request/retry diagnostics
- `--raw` print unformatted response body

### API Documentation

When running locally, you can access:
- Interactive API docs: `http://localhost:8000/docs`
- ReDoc documentation: `http://localhost:8000/redoc`

## Modal Deployment

### Prerequisites

1. Install Modal:
```bash
pip install modal
```

2. Authenticate with Modal:
```bash
modal setup
```

### Configuration

Edit `modal_config.yaml` to customize:
- Resource allocation (CPU, memory, GPU)
- Environment variables
- Secrets management
- Custom domains
- Authentication settings

### Deploy

#### Development (with live reload):
```bash
modal serve modal_wrapper.py
```

#### Production deployment:
```bash
modal deploy modal_wrapper.py
```

After deployment, Modal will provide you with a URL for your service.

## API Endpoints

### System Endpoints

- `GET /health` - Health check endpoint
- `GET /ready` - Readiness check endpoint

### Tool Management - Basic Operations

- `POST /api/v1/tools` - Create a new tool
- `GET /api/v1/tools/{tool_id}` - Get a specific tool
- `PUT /api/v1/tools/{tool_id}` - Update a tool
- `DELETE /api/v1/tools/{tool_id}` - Delete a tool
- `GET /api/v1/tools/name/{tool_name}` - Get tool by name

### Tool Management - Advanced Filtering

- `GET /api/v1/tools` - List tools with advanced filters:
  - `user_id` - Filter by user who created the tool
  - `organization_id` - Filter by organization
  - `toolkit_id` - Filter by toolkit/collection
  - `category` - Filter by category
  - `has_ui_effects` - Filter by UI rendering capability (true/false)
  - `visibility` - Filter by visibility (public, private, organization)
  - `tags` - Comma-separated list of tags
  - `min_rating` - Minimum rating threshold
  - `limit` - Maximum results (default: 100)
  - `offset` - Pagination offset

### Tool Management - Specialized Queries

- `GET /api/v1/tools/by-user/{user_id}` - Get all tools by a specific user
- `GET /api/v1/tools/by-organization/{organization_id}` - Get tools by organization
- `GET /api/v1/tools/by-toolkit/{toolkit_id}` - Get tools in a toolkit/collection
- `POST /api/v1/tools/by-input-schema` - Find tools by input schema compatibility
  - Body: `{"schema": {...}, "match_type": "exact|compatible|partial"}`
- `POST /api/v1/tools/by-output-schema` - Find tools by output schema compatibility
  - Body: `{"schema": {...}, "match_type": "exact|compatible|partial"}`
- `GET /api/v1/tools/with-ui-effects` - Get tools that produce UI effects
  - `ui_renderer` - Optional: filter by renderer type (react, vue, html, etc.)

### Tool Discovery & Analytics

- `GET /api/v1/tools/categories` - Get all categories with tool counts
- `GET /api/v1/tools/tags` - Get all tags with usage counts
- `GET /api/v1/tools/statistics` - Get aggregated statistics about tools

### Tool Execution

- `POST /api/v1/tools/execute` - Execute a single tool
- `POST /api/v1/tools/search` - Search for tools using semantic search

### Real-World Use-Case Catalog

- `GET /api/v1/use-cases` - Discover production use-case templates by domain/objective
- `GET /api/v1/use-cases/{use_case_id}` - Inspect full template details and defaults
- `POST /api/v1/use-cases/{use_case_id}/plan` - Build a guarded execution plan with cost/reintegration checks
- `POST /api/v1/use-cases/{use_case_id}/execute` - Execute a rendered use-case as a batch workflow

See `REAL_WORLD_USE_CASES.md` for scenario details and example payloads.

### MCP (Model Context Protocol) Endpoints

- `WS /mcp` - WebSocket endpoint for MCP protocol communication
- `GET /api/v1/mcp/info` - Get MCP server information and capabilities

### Tool Composition Endpoints

- `POST /api/v1/compositions` - Create a tool composition
- `GET /api/v1/compositions` - List all compositions
- `POST /api/v1/compositions/{id}/execute` - Execute a composition
- `GET /api/v1/compositions/history` - Get execution history

### Webhook & Event Endpoints

- `POST /api/v1/webhooks` - Register a webhook for events
- `DELETE /api/v1/webhooks/{id}` - Unregister a webhook
- `GET /api/v1/webhooks` - List all webhooks
- `GET /api/v1/webhooks/deliveries` - Get webhook delivery history
- `GET /api/v1/events` - Get event history

### Protocol Adapter Endpoints

- `POST /api/v1/adapters/convert` - Convert tools between protocols
- `GET /api/v1/adapters` - List available protocol adapters

### Federation Endpoints

- `POST /api/v1/federation/nodes` - Add a federated registry
- `DELETE /api/v1/federation/nodes/{id}` - Remove a federated registry
- `GET /api/v1/federation/nodes` - List federated registries
- `POST /api/v1/federation/sync/{id}` - Manually sync a registry
- `POST /api/v1/federation/search` - Search across federated registries
- `POST /api/v1/federation/execute` - Execute tool on federated node

## Using the Client Library

The `tool_mgmt_api_client.py` provides a Python client for interacting with the service:

```python
from tool_mgmt_api_client import ToolManagementAPIClient
from pydantic import BaseModel

# Initialize client
client = ToolManagementAPIClient(
    tool_api_url="https://your-modal-url.modal.run",
    service_name="My Service",
    service_url="https://my-service.com"
)

# Define input/output models
class MyInput(BaseModel):
    text: str

class MyOutput(BaseModel):
    result: str

# Create a tool using decorator
@client.tool_wrapper(description="Process text data")
async def process_text(input: MyInput) -> MyOutput:
    """Process text and return result."""
    return MyOutput(result=f"Processed: {input.text}")

# Register tools
await client.register_tools()

# Execute a tool
result = await client.execute_tool(
    tool_name="process_text",
    input_data={"text": "Hello World"}
)
```

## Environment Variables

Configure the service using environment variables:

- `APP_NAME` - Service title (default: "Tool Management API")
- `APP_VERSION` - Version string (default: "0.1.0")
- `ENV` - Environment name (default: "development")
- `DEBUG` - Enable debug logs ("1" to enable)
- `CORS_ORIGINS` - Comma-separated list or "*" (default: "*")
- `HOST` - Bind host for uvicorn (default: "0.0.0.0")
- `PORT` - Bind port for uvicorn (default: "8000")

## Authentication

Enable Bearer token authentication by setting in `modal_config.yaml`:

```yaml
auth:
  enabled: true
  bearer_env_var: AUTH_TOKEN
```

Then set the `AUTH_TOKEN` secret in Modal:

```bash
modal secret create tool-management-secrets AUTH_TOKEN=your-secret-token
```

## Tool Schema

Tools support a comprehensive schema with organization, UI, and categorization features:

```python
{
    # Basic Information
    "name": "tool_name",                    # Unique tool identifier
    "description": "Tool description",       # What the tool does
    "input_schema": {},                      # JSON Schema for inputs
    "output_schema": {},                     # JSON Schema for outputs
    "service_url": "https://service.url",    # Where tool is hosted
    "endpoint": "/api/endpoint",             # API endpoint path

    # Organization & Ownership
    "user_id": "user123",                    # User who created the tool
    "organization_id": "org456",             # Organization owner
    "toolkit_id": "toolkit789",              # Toolkit/collection ID

    # UI & Rendering
    "has_ui_effects": true,                  # Produces UI components
    "ui_renderer": "react",                  # Type: react, vue, html, etc.
    "ui_schema": {},                         # Schema for UI components

    # Categorization & Discovery
    "tags": ["ai", "nlp", "text"],          # Tags for filtering
    "category": "text-processing",           # Primary category
    "visibility": "public",                  # public, private, organization

    # Advanced Metadata
    "version": "1.2.0",                      # Tool version
    "dependencies": ["tool1", "tool2"],      # Required tools
    "pricing_tier": "premium",               # Pricing tier if applicable
    "usage_count": 1523,                     # Usage statistics
    "rating": 4.5,                           # Average user rating

    # Additional custom metadata
    "metadata": {
        "author": "John Doe",
        "license": "MIT",
        "documentation_url": "https://docs.example.com"
    }
}
```

### Schema Matching Types

When searching by input/output schemas, you can use three matching modes:

1. **exact** - Schema must match exactly
2. **compatible** - Tool can work with the provided schema (checks required fields)
3. **partial** - Schemas share at least some common properties

## Development Tips

1. **Local Testing**: Use `modal serve` for development with hot reload
2. **Logs**: View logs with `modal logs tool-management-modal`
3. **Secrets**: Manage secrets via Modal dashboard or CLI
4. **Scaling**: Adjust `concurrency` and `keep_warm` in config

## Troubleshooting

### Common Issues

1. **Port conflicts**: If port 8000 is in use, set `PORT` environment variable
2. **CORS errors**: Check `CORS_ORIGINS` configuration
3. **Authentication failures**: Verify `AUTH_TOKEN` is set correctly
4. **Modal deployment fails**: Ensure all dependencies are in `requirements.txt`

### Debug Mode

Enable debug mode for verbose logging:

```python
client = ToolManagementAPIClient(
    tool_api_url="...",
    debug=True
)
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is provided as-is for educational and development purposes.

## Support

For issues or questions:
- Check the API documentation at `/docs`
- Review Modal documentation at https://modal.com/docs
- Check the logs for error messages

## Next Steps

- Add database persistence (PostgreSQL, MongoDB)
- Implement rate limiting
- Add webhook support
- Create admin dashboard
- Add metrics and monitoring
- Implement tool versioning
- Add batch processing capabilities
