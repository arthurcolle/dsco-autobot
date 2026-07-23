# Testing MCP Server Locally

Quick guide to run and test the parallel_websearch_server with the MCP client.

## Step 1: Start the Server

In one terminal, run:

```bash
./start_server.sh
```

This will:
- Kill any existing process on port 2040
- Start the server at http://localhost:2040
- Expose both MCP transports:
  - Streamable HTTP: http://localhost:2040/mcp (send `Accept: application/mcp+json`)
  - SSE stream: http://localhost:2040/mcp/sse (POST replies to `/mcp/sse/messages/`)
- Run in reload mode (single worker, auto-reload on code changes)

The server will show logs. Keep this terminal open.

## Step 2: Test the Client

In another terminal, run:

```bash
./test_mcp_client.sh
```

This will:
- Check if the server is running
- Connect the MCP client to localhost:2040/mcp/sse
- Send a test query: "What tools are available?"
- Display the response

### Custom Queries

You can pass any query as an argument:

```bash
./test_mcp_client.sh "Search for latest AI news"
```

### Interactive Mode

To chat interactively with the MCP server:

```bash
python chat-openrouter-mcp.py --server-url http://localhost:2040/mcp/sse
```

Then type your queries. Use `/exit` to quit.

## Environment Setup

Make sure you have:
1. A `.env` file with necessary API keys (copy from `.env.example`)
2. All dependencies installed: `pip install -r requirements.txt` or `uv sync`
3. `OPENROUTER_API_KEY` set if using OpenRouter models

## Troubleshooting

**Port 2040 already in use:**
```bash
lsof -i:2040
kill -9 <PID>
```

**Server not responding:**
- Check logs in the server terminal
- Verify .env file has correct API keys
- Try: `curl http://localhost:2040/health`

**Client connection fails:**
- Ensure server is running first
- Check the SSE endpoint: `http://localhost:2040/mcp/sse`
- Verify OPENROUTER_API_KEY is set

## Available Endpoints

- `http://localhost:2040/` - Root endpoint
- `http://localhost:2040/health` - Health check
- `http://localhost:2040/mcp/sse` - MCP SSE endpoint (for client connection)

## Notes

- The server runs with a single worker when using `./start_server.sh` (development mode)
- For production, set `RELOAD=false` and adjust `WORKERS` in .env
- The test script uses `anthropic/claude-haiku-4.5` by default (fast and cheap)
- Change model with: `python chat-openrouter-mcp.py --server-url http://localhost:2040/mcp/sse --model <model-name>`
