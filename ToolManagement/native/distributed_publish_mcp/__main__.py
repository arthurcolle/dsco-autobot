"""Allow running as: python -m native.distributed_publish_mcp"""
from .server import app
import uvicorn
import os

if __name__ == "__main__":
    port = int(os.getenv("PORT", "4003"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
