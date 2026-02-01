"""
Deployment scripts for white-label tool services.

Core services:
- jina_service.py: Jina AI wrapped as "Search API"
- parallel_service.py: Parallel AI wrapped as "Tasks API"
- registry_server.py: Central tool registry with orchestration

Subdirectories:
- modal/: Modal.com deployment wrappers
- neo4j/: Neo4j graph database deployment
- standalone/: Standalone FastAPI app with embedded features
"""
