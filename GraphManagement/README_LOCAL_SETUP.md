# Local Development Setup Guide

This guide will help you spin up the Graph Management Layer locally using your Neo4j Desktop instance.

## Prerequisites

1. **Neo4j Desktop** running with:
   - URI: `neo4j://localhost:7687`
   - User: `neo4j`
   - Password: `GraphInstance2025!`
   - Database: `neo4j`

2. **Python 3.11+** installed

3. **Dependencies** installed:
   ```bash
   pip install -r requirements.txt
   ```

## Architecture

```
┌─────────────────────────────────────────┐
│  Your Autonomous Agent Microservices    │  (Future - Layer 3)
└──────────────────┬──────────────────────┘
                   │
┌──────────────────▼──────────────────────┐
│  Graph Management API                   │  Layer 2 (Port 8000)
│  - Multi-tenant org/project/env         │
│  - RBAC + ABAC security                 │
│  - Schema registry & governance         │
└──────────────────┬──────────────────────┘
                   │ BACKEND_URL
┌──────────────────▼──────────────────────┐
│  Neo4j Wrapper Service                  │  Layer 1 (Port 8001)
│  - HTTP wrapper for Neo4j Desktop       │
└──────────────────┬──────────────────────┘
                   │ neo4j://
┌──────────────────▼──────────────────────┐
│  Neo4j Desktop                          │  Database (Port 7687)
│  - Your local graph database            │
└─────────────────────────────────────────┘
```

## Quick Start

### Step 1: Start Services

```bash
./start_local.sh
```

This will:
- Start Layer 1 (Neo4j Wrapper) on port 8001
- Start Layer 2 (Graph Management API) on port 8000
- Verify connectivity to Neo4j Desktop
- Show logs in real-time

### Step 2: Bootstrap the Database

In a new terminal:

```bash
./bootstrap.sh
```

This will create:
- ✅ Graph constraints (unique IDs for Org, User, Role, etc.)
- ✅ RBAC roles (developer, readonly, service, org_admin, etc.)
- ✅ Permissions (schemas:read, query:write, etc.)
- ✅ Environments (dev, stage, prod)
- ✅ Your first organization (acme)

### Step 3: Verify Everything Works

```bash
# Check health
curl http://localhost:8000/health

# Get schema info
curl http://localhost:8000/v1/schema

# Check model status
curl http://localhost:8000/v1/model/status

# View API docs
open http://localhost:8000/docs
```

### Step 4: Stop Services

```bash
./stop_local.sh
```

Or press `Ctrl+C` in the terminal running `start_local.sh`.

## Directory Structure

```
GraphManagement/
├── local_neo4j_wrapper.py    # Layer 1: Neo4j wrapper service
├── graph_management_service.py # Layer 2: Main API entry point
├── src/graph_management/      # Core graph management code
│   ├── api.py                 # FastAPI app definition
│   ├── security.py            # Auth (RBAC/ABAC)
│   ├── schemas.py             # Pydantic models
│   └── routers/               # API endpoint routers
├── scripts/                   # Utility scripts
│   └── generate_auth_defaults.py
├── .env                       # Local configuration
├── start_local.sh             # Start all services
├── stop_local.sh              # Stop all services
├── bootstrap.sh               # Initialize database
└── logs/                      # Service logs (created on start)
```

## Configuration

### Environment Variables (`.env`)

```bash
# Neo4j Desktop Connection
NEO4J_URI=neo4j://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=GraphInstance2025!
NEO4J_DATABASE=neo4j

# Backend URL (Layer 1 → Layer 2)
BACKEND_URL=http://127.0.0.1:8001

# Authentication (disabled for local dev)
AUTH_MODE=disabled

# Logging
LOG_LEVEL=INFO
DEBUG_ERRORS=1
```

## Common Tasks

### Create a Team

```bash
curl -X POST http://localhost:8000/v1/orgs/acme/teams \
  -H "Content-Type: application/json" \
  -d '{
    "id": "engineering",
    "name": "Engineering",
    "description": "Product engineering team"
  }'
```

### Create a Project

```bash
curl -X POST http://localhost:8000/v1/orgs/acme/projects \
  -H "Content-Type: application/json" \
  -d '{
    "id": "graph-platform",
    "name": "Graph Platform",
    "description": "Core graph management platform",
    "env_ids": ["dev", "stage", "prod"]
  }'
```

### Run a Cypher Query

```bash
curl -X POST http://localhost:8000/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (o:Org) RETURN o",
    "parameters": {},
    "strategy": "all"
  }'
```

### Register a Node Schema

```bash
curl -X POST http://localhost:8000/v1/registry/nodes \
  -H "Content-Type: application/json" \
  -d '{
    "spec": {
      "label": "Agent",
      "description": "An autonomous agent",
      "properties": [
        {"name": "id", "type": "string", "required": true, "unique": true},
        {"name": "name", "type": "string", "required": true},
        {"name": "capabilities", "type": "array"}
      ]
    },
    "version": "1"
  }'
```

## Authentication Setup (Optional)

For production-like local development with API keys:

### 1. Generate Auth Keys

```bash
python scripts/generate_auth_defaults.py \
  --preset solo \
  --write-dotenv auth.env
```

### 2. Load Auth Keys

```bash
# Add to .env
source auth.env
```

### 3. Update AUTH_MODE

```bash
# In .env
AUTH_MODE=api_key
```

### 4. Restart Services

```bash
./stop_local.sh && ./start_local.sh
```

### 5. Use API Keys

```bash
# Get your key from auth.env
export API_KEY="sk-..."

# Use in requests
curl http://localhost:8000/v1/schema \
  -H "Authorization: Bearer $API_KEY"
```

## Troubleshooting

### Services won't start

1. **Check Neo4j Desktop is running**
   ```bash
   # Test connection
   cypher-shell -a neo4j://localhost:7687 -u neo4j -p GraphInstance2025! "RETURN 1;"
   ```

2. **Check ports are free**
   ```bash
   lsof -i :8000  # Graph Management API
   lsof -i :8001  # Neo4j Wrapper
   ```

3. **Check logs**
   ```bash
   tail -f logs/graph_management.log
   tail -f logs/neo4j_wrapper.log
   ```

### Bootstrap fails

1. **Verify services are running**
   ```bash
   curl http://localhost:8000/health
   curl http://localhost:8001/health
   ```

2. **Check existing constraints** (may need to clear database first)
   ```cypher
   // In Neo4j Browser
   SHOW CONSTRAINTS;
   ```

### Connection refused errors

Make sure:
- Neo4j Desktop is running
- Bolt connector is enabled (default: port 7687)
- Username/password in `.env` matches your Neo4j config

## Next Steps

1. **Explore the API**: http://localhost:8000/docs
2. **Review the ROADMAP**: See `ROADMAP.md` for planned features
3. **Run examples**: Check the `examples/` directory
4. **Read the architecture doc**: See the main README

## API Documentation

Once running, visit:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Support

- Check `ROADMAP.md` for implementation status
- Review `important_convo.txt` for design decisions
- Examine `src/graph_management/` for implementation details
