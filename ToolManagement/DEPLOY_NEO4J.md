# Self-Hosted Neo4j on Modal

This deployment runs a complete Neo4j database on Modal infrastructure with persistent storage.

## Features

- **Persistent Storage**: Uses Modal Volumes to persist Neo4j data across deployments
- **Self-Contained**: No external Neo4j instance needed
- **RESTful API**: FastAPI endpoints for querying Neo4j
- **Health Checks**: Built-in health and probe endpoints
- **Scalable**: Runs on Modal's infrastructure with 2 CPU cores and 4GB RAM

## Quick Start

### 1. Set the Neo4j Password (Optional)

By default, the password is `neo4j_password`. To use a custom password:

```bash
export NEO4J_PASSWORD="your-secure-password"
```

### 2. Deploy to Modal

For development (with live reloading):
```bash
modal serve neo4j_self_hosted.py
```

For production:
```bash
modal deploy neo4j_self_hosted.py
```

### 3. Get Your Deployment URL

After deployment, Modal will provide a URL like:
```
https://yourname--neo4j-self-hosted-neo4jservice-serve.modal.run
```

## API Endpoints

### Health Check
```bash
curl https://your-url.modal.run/health
```

Response:
```json
{"status": "ok"}
```

### Probe Neo4j Connection
```bash
curl https://your-url.modal.run/probe
```

Response:
```json
{"ok": 1}
```

### Execute Cypher Queries
```bash
curl -X POST https://your-url.modal.run/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "CREATE (n:Person {name: $name}) RETURN n",
    "parameters": {"name": "Alice"}
  }'
```

Response:
```json
{
  "results": [
    {"n": {"name": "Alice"}}
  ]
}
```

### Query Example
```bash
curl -X POST https://your-url.modal.run/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "MATCH (n:Person) RETURN n.name as name LIMIT 10"
  }'
```

## Configuration

The deployment uses the following configuration:

- **CPU**: 2.0 cores
- **Memory**: 4GB
- **Bolt Port**: 7687 (internal)
- **HTTP Port**: 7474 (internal)
- **Container Idle Timeout**: 10 minutes
- **Volume**: `neo4j-data` (auto-created)
- **Data Path**: `/data/neo4j`

## Data Persistence

All Neo4j data is stored in a Modal Volume named `neo4j-data`. This ensures:

- Data persists across deployments
- Database state is maintained
- No data loss when containers restart

To manage the volume:

```bash
# List volumes
modal volume list

# View volume contents
modal volume ls neo4j-data

# Delete volume (⚠️ destroys all data)
modal volume delete neo4j-data
```

## Connecting from External Applications

To connect to this Neo4j instance from your local machine or other services:

1. The database is only accessible via the FastAPI endpoints (not direct Bolt connection)
2. Use the `/query` endpoint to execute Cypher queries
3. For Python applications, create a wrapper client:

```python
import requests

class ModalNeo4jClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')

    def query(self, cypher: str, parameters: dict = None):
        response = requests.post(
            f"{self.base_url}/query",
            json={"query": cypher, "parameters": parameters or {}}
        )
        response.raise_for_status()
        return response.json()["results"]

# Usage
client = ModalNeo4jClient("https://your-url.modal.run")
results = client.query("MATCH (n) RETURN n LIMIT 10")
```

## Monitoring

Check logs with:
```bash
modal app logs neo4j-self-hosted
```

## Troubleshooting

### Neo4j fails to start

- Check logs: `modal app logs neo4j-self-hosted`
- Ensure sufficient memory (4GB minimum)
- Verify volume is accessible

### Queries timeout

- Increase container idle timeout in code
- Check query complexity
- Monitor CPU/memory usage

### Volume issues

```bash
# Check volume exists
modal volume list

# Create volume manually if needed
modal volume create neo4j-data
```

## Cost Optimization

- The container idles after 10 minutes of inactivity
- Data persists in the volume (small storage cost)
- First query after idle period will have cold start delay (~30-60s)
- For production workloads, consider adjusting `container_idle_timeout`

## Security Notes

1. **Password**: Change the default password in production
2. **API Access**: The endpoints are public by default. Consider adding authentication:
   - Add API key validation to FastAPI endpoints
   - Use Modal secrets for credentials
3. **Network**: The Neo4j Bolt port is not exposed externally (only via API)

## Next Steps

- Add authentication to API endpoints
- Implement connection pooling
- Add monitoring and metrics
- Set up backup strategy for the volume
- Configure Neo4j plugins if needed
