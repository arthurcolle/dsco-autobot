# Local Development

These steps let you run both layers of the Graph Management stack against a Neo4j
Desktop instance on your machine.

## 1. Prerequisites

1. Install Python 3.11 and create a virtualenv in this repo (e.g. `python3.11 -m venv .venv && source .venv/bin/activate`).
2. Install the dependencies used by the local FastAPI shims:  
   `pip install fastapi[standard] uvicorn httpx aiohttp modal neo4j`.
3. Start the Neo4j Desktop database shown in the screenshot (`neo4j://127.0.0.1:7687`) and keep the password as `GraphInstance2025!`. The repo-level `.env` already stores this value as `NEO4J_PASSWORD`, so the shims pick it up automatically.

## 2. Launch the Neo4j FastAPI shim

This process exposes the same `/probe`, `/query`, `/v1/query`, etc. endpoints that the
Modal image provides, but it connects to your Desktop database instead of starting its own.

```bash
uvicorn neo4j_modal_image:fastapi_app --reload --port 8001
```

If everything is wired correctly you can hit `curl http://127.0.0.1:8001/probe` and receive `{"ok": 1}`.

## 3. Launch the overlay Graph Management API

Open a second terminal (virtualenv activated) and run:

```bash
uvicorn graph_management_service:api --reload --port 8000
```

The shim automatically reads `.env`, so `BACKEND_URL` resolves to `http://127.0.0.1:8001`.
You can now query the overlay at `http://127.0.0.1:8000`, e.g.:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/v1/schema
```

### Optional: require API keys locally

If you want every request to go through the FastAPI auth dependencies, export a JSON map of API keys before launching the overlay:

```bash
# Fast helper: generate secrets + env file (defaults: org_admin/developer/readonly/service)
python scripts/generate_auth_defaults.py --write-dotenv .auth.env
set -a && source .auth.env && set +a  # exposes AUTH_API_KEYS for this shell

# Custom plans (LABEL[:SUB][:ORG][#scope+scope]=roles, repeat flag)
python scripts/generate_auth_defaults.py \
  --plan admin=org_admin \
  --plan dashboard:dash:acme=readonly \
  --plan writer:@svc-writer:factory#writer:company=service \
  --no-default-plans --write-dotenv .auth.env
# (The @ prefix makes SUBJECT absolute so the global --prefix is skipped.)

# Manual example, if you prefer to hand-edit
export AUTH_API_KEYS='{"local-key":{"sub":"dev-user","roles":["org_admin"]}}'
uvicorn graph_management_service:api --reload --port 8000
```

#### Scenario templates

- **Solo builder** – one credential with broad rights plus an observer key:
  ```bash
  python scripts/generate_auth_defaults.py --preset solo --no-default-plans --no-service-key --write-dotenv .auth.env
  set -a && source .auth.env && set +a
  ```
- **Small team / seed startup** – founder admin, builder, observer, and ops bot:
  ```bash
  python scripts/generate_auth_defaults.py --preset small-team --no-default-plans --write-dotenv .auth.env
  ```
- **Enterprise / regulated org** – platform admin, schema admin, app dev, observability, runtime service:
  ```bash
  python scripts/generate_auth_defaults.py --preset enterprise --no-default-plans --write-dotenv .auth.env
  ```
- **Multi-org or tenant-specific** – mix in custom plans with explicit `:org` targets:
  ```bash
  python scripts/generate_auth_defaults.py \
    --plan admin-denmark:admin:denmark=org_admin \
    --plan admin-norway:admin:norway=org_admin \
    --plan builder-norway:builder:norway=developer \
    --no-default-plans --write-dotenv .auth.env
  ```

Clients (including the docs UI) must now send `Authorization: Bearer <printed-key>` or an `X-API-Key: <printed-key>` header with every call. When an overlay is running, the helper script notices, prints curl smoke tests, and reminds you to restart so the new tokens take effect.

## 4. Tearing down / tweaks

- If you change the Neo4j password, edit `.env` (or set `NEO4J_PASSWORD` before starting the servers).
- To point the overlay at a different backend, change `BACKEND_URL` in `.env` or export it before launching.
- `LOG_LEVEL` and `DEBUG_ERRORS` in `.env` make both layers verbose during troubleshooting.
