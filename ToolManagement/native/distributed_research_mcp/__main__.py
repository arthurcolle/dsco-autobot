"""Allow running as: python -m native.distributed_research_mcp"""
import uvicorn
import os

if __name__ == "__main__":
    port = int(os.getenv("PORT", "4002"))
    # Use import string to ensure proper module loading and route registration
    uvicorn.run(
        "native.distributed_research_mcp.server:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
