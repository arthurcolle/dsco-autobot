# Quick Start Guide

Get your Graph Management Layer running in under 5 minutes.

## Prerequisites Check

```bash
# 1. Verify Neo4j Desktop is running
cypher-shell -a neo4j://localhost:7687 -u neo4j -p GraphInstance2025! "RETURN 1;"

# 2. Verify Python 3.11+
python3 --version

# 3. Install dependencies
pip install -r requirements.txt
```

## 🚀 Three Commands to Get Running

### 1. Start Services (Terminal 1)

```bash
./start_local.sh
```

**What this does:**
- ✅ Starts Neo4j Wrapper (Layer 1) on port 8001
- ✅ Starts Graph Management API (Layer 2) on port 8000
- ✅ Verifies Neo4j connectivity
- ✅ Shows live logs

**Expected output:**
```
========================================
  Services Running
========================================
✓ Neo4j Wrapper:       http://localhost:8001
✓ Graph Management:    http://localhost:8000
✓ API Docs (Swagger):  http://localhost:8000/docs
```

### 2. Bootstrap Database (Terminal 2)

```bash
./bootstrap.sh
```

**What this does:**
- ✅ Creates constraints (Org, User, Role, etc.)
- ✅ Creates RBAC roles (developer, readonly, org_admin, etc.)
- ✅ Creates permissions (schemas:read, query:write, etc.)
- ✅ Creates environments (dev, stage, prod)
- ✅ Creates initial org (acme)

**Expected output:**
```
Bootstrap complete!
constraints_applied: 20
permissions_created: 25
roles_created: 7
environments_created: 3
org_id: "acme"
```

### 3. Test the API

```bash
# Check health
curl http://localhost:8000/health

# View schema
curl http://localhost:8000/v1/schema | jq

# Check model status
curl http://localhost:8000/v1/model/status | jq
```

**Or visit:** http://localhost:8000/docs

## 🎯 Common Operations

### Create a Team

```bash
curl -X POST http://localhost:8000/v1/orgs/acme/teams \
  -H "Content-Type: application/json" \
  -d '{"id": "eng", "name": "Engineering"}' | jq
```

### Create a Project

```bash
curl -X POST http://localhost:8000/v1/orgs/acme/projects \
  -H "Content-Type: application/json" \
  -d '{
    "id": "platform",
    "name": "Platform",
    "env_ids": ["dev", "prod"]
  }' | jq
```

### Register an Agent

```bash
curl -X POST http://localhost:8000/v1/agents \
  -H "Content-Type: application/json" \
  -d '{
    "id": "agent-001",
    "name": "Task Agent",
    "capabilities": ["query", "write"],
    "org_id": "acme"
  }' | jq
```

### Run a Query

```bash
curl -X POST http://localhost:8000/v1/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (n) RETURN labels(n) as label, count(n) as count",
    "strategy": "all"
  }' | jq
```

## 🛑 Stop Services

```bash
./stop_local.sh
```

Or press `Ctrl+C` in the terminal running `start_local.sh`.

## 📚 What's Next?

1. **Explore the API**: http://localhost:8000/docs
2. **Read the full setup guide**: `README_LOCAL_SETUP.md`
3. **Review the architecture**: Main README.md
4. **Check the roadmap**: `ROADMAP.md`
5. **Run examples**: `examples/` directory

## 🔧 Troubleshooting

### Services won't start

```bash
# Check if ports are in use
lsof -i :8000
lsof -i :8001

# Kill any stuck processes
./stop_local.sh

# Check Neo4j is running
cypher-shell -a neo4j://localhost:7687 -u neo4j -p GraphInstance2025! "RETURN 1;"
```

### Check logs

```bash
tail -f logs/graph_management.log
tail -f logs/neo4j_wrapper.log
```

### Reset everything

```bash
# Stop services
./stop_local.sh

# Clear logs
rm -rf logs/

# (Optional) Clear Neo4j database
cypher-shell -a neo4j://localhost:7687 -u neo4j -p GraphInstance2025! \
  "MATCH (n) DETACH DELETE n;"

# Start fresh
./start_local.sh
./bootstrap.sh
```

## 🎨 Service Architecture

```
Port 8000 → Graph Management API (Layer 2)
    ↓ HTTP (BACKEND_URL=http://localhost:8001)
Port 8001 → Neo4j Wrapper (Layer 1)
    ↓ Bolt (neo4j://localhost:7687)
Port 7687 → Neo4j Desktop (Database)
```

## 📖 Key Files

| File | Purpose |
|------|---------|
| `start_local.sh` | Start all services |
| `stop_local.sh` | Stop all services |
| `bootstrap.sh` | Initialize database |
| `local_neo4j_wrapper.py` | Layer 1 wrapper service |
| `graph_management_service.py` | Layer 2 main service |
| `.env` | Configuration |
| `logs/` | Service logs |

## 🚀 Ready to Build

Your Graph Management Layer is now running! You can:

- ✅ Create organizations, teams, and projects
- ✅ Register agents with capabilities
- ✅ Define node and relationship schemas
- ✅ Run Cypher queries with RBAC/ABAC
- ✅ Track agent runs and events
- ✅ Use vector embeddings and hybrid retrieval

**Next:** Start building your 14 autonomous agent microservices that connect to this layer!
