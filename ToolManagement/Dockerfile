# ToolManagement — single image for the registry and all native MCP workers.
# Every container serves HTTP on port 80; docker-compose.yml overrides `command`
# per worker (python -m native.<module>.server) against this same image.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependency layer — cached until requirements.txt changes
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Application code
COPY src/ /app/src/
COPY native/ /app/native/
COPY deploy/ /app/deploy/
COPY data/codex_apps_snapshot/ /app/data/codex_apps_snapshot/

ENV PYTHONPATH=/app/src:/app \
    PORT=80 \
    DATA_DIR=/app/data

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=300s --retries=3 \
    CMD curl -f http://localhost:80/health || exit 1

# Default command: the registry. Workers override this in compose.
CMD ["uvicorn", "tool_management.app:app", "--host", "0.0.0.0", "--port", "80"]
