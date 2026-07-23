"""
GraphManagement Service Entry Point

Re-exports the FastAPI app from the src module for Docker deployment.
"""

from src.graph_management.api import api as app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
