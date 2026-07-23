# Standardized Service Project Structure

This document defines the target layout for every service directory in `autonomous.agents` (e.g., `Completions`, `Conversations`, `GraphManagement`, `Realtime`, `ToolManagement`). The structure keeps language-specific needs in separate layers while giving ops, docs, and tooling predictable homes.

## Goals

1. **Discoverability** – every config, script, and asset lives in a consistent place.  
2. **Isolation** – service code, infrastructure, docs, and tooling are scoped to that service.  
3. **Reusability** – shared code moves to `libs/` (or a future package) instead of being copy/pasted between services.  
4. **Deployability** – infra definitions and Modal/Terraform/Docker assets align with code and stay versioned together.  
5. **Language Agnostic** – Python and Node services share the same high-level directories, even if their tooling differs.

---

## Monorepo Layout (target)

```
autonomous.agents/
├── README.md
├── docs/
│   ├── project-structure.md (this file, or a symlink)
│   └── <feature-specs>.md
├── infra/
│   ├── terraform/
│   ├── kubernetes/
│   └── modal/
├── libs/                  # shared Python/TS packages, typed clients, schemas
├── scripts/               # repo-wide automation (linting, release, bootstrap)
├── services/
│   ├── completions/
│   ├── conversations/
│   ├── graph-management/
│   ├── realtime/
│   └── tool-management/
└── tests/                 # contract/e2e suites that span services
```

Notes:

- Existing top-level service folders can remain where they are until moved into `services/`.  
- Shared `.env` templates live under `configs/` inside the repo root; each service keeps its own `.env.example`.  
- Repo-level tooling (linters, formatters, CI) reference `services/*`.

---

## Service Layout

Use kebab-case for service folders (e.g., `graph-management`). Inside each service:

```
service-name/
├── README.md
├── pyproject.toml | package.json
├── requirements/           # Python: base.txt, dev.txt, prod.txt (generated via pip-tools)
├── src/
│   └── service_name/       # application code (api/, domain/, adapters/, clients/, schemas/)
├── tests/
│   ├── unit/
│   ├── integration/
│   └── data/               # fixtures, mocked responses
├── docs/                   # design notes, API docs, onboarding for this service
├── configs/
│   ├── env/.env.example
│   ├── modal/*.yaml
│   ├── settings/*.yaml     # runtime config (FastAPI, logging, providers)
│   └── providers/          # provider JSON/YAML (OpenAI, Neo4j, etc.)
├── infra/
│   ├── docker/             # Dockerfile, compose overrides
│   ├── modal/              # Modal entrypoints, images, schedules
│   └── terraform/          # service-level IaC (if applicable)
├── scripts/
│   ├── bootstrap.py        # create secrets, seed data
│   ├── lint.sh / format.sh
│   └── data_migrations/
├── assets/ | public/       # static assets (React bundles, diagrams, SVGs)
├── Makefile                # `make dev/test/lint/package/deploy`
└── .env.example
```

### Directory Responsibilities

- `src/` – only importable application/runtime code. Never mix notebooks, tests, or scripts here.  
- `tests/` – mirror `src/` package structure; use `conftest.py`/fixtures under `tests/support`.  
- `docs/` – ADRs, architecture diagrams, API reference, onboarding checklists.  
- `configs/` – runtime configuration tracked in VCS (sample `.env`, provider configs, Modal YAML).  
- `infra/` – Docker, Modal, Terraform, GitHub Actions workflows specific to the service.  
- `scripts/` – automation and developer tooling that should not ship with the app binary.  
- `assets/` or `public/` – UI bundles, diagrams, exported JSON, etc.

### Language-Specific Defaults

**Python services**

- `pyproject.toml` with Poetry or Hatch preferred; fallback to `requirements/` + `pip-tools`.  
- Entry points live under `src/service_name/app.py` (FastAPI) or `main.py`.  
- Keep Pydantic/FastAPI schemas in `src/service_name/schemas/`.  
- Modal/Modal image builders go under `infra/modal/` with wrappers inside `src/.../infra/`.  
- Example `Makefile` targets: `install`, `format`, `lint`, `test`, `typecheck`, `serve`, `deploy`.

**Node/React services (Realtime console, UI clients)**

- Keep `package.json`, lockfile, `tsconfig.json`, `vite.config.ts`, etc. in root.  
- Application code resides in `src/` and tests in `tests/` (or `src/__tests__`).  
- Build config (`postcss.config.cjs`, `tailwind.config.js`) should stay in `configs/` if not required by tooling root.  
- Use `scripts/` for custom CLIs (seeding, data export).  
- Keep Dockerfile(s) in `infra/docker/`.

---

## Mapping for Existing Services

The following shows what needs to move (or be symlinked) once the structure is adopted. File moves can happen incrementally; update imports/tests as you migrate.

### GraphManagement

- Move runtime modules (`graph_management_service.py`, `graph_client`, `modal_wrapper.py`, etc.) into `src/graph_management/`.  
- `schemas.py`, `security.py`, and `Neo4j` utilities become `src/graph_management/schemas.py`, `src/graph_management/security.py`, etc.  
- Keep `tests/` as-is but relocate to `tests/` root with packages mirroring `src/graph_management`.  
- Create `infra/modal/graph_management_image.py` and keep `neo4j_modal_image.py` there.  
- Place policy/reference docs under `docs/`.

### Completions

- Treat UI assets (`chat-ui`, `chatbot-ui`, `particles-landing`, `public/`, `static/`) as `assets/` or `web/`.  
- Backend FastAPI logic should live under `src/completions/`.  
- Move provider/config JSONs (`provider_configs.py`, `models/`, `clients/`) into `src/completions/providers/`.  
- Store animations/interface docs in `docs/` with ADR numbering.  
- Consolidate Modal/Tool integration scripts under `infra/` and `scripts/`.

### Conversations

- `fastapi_app.py`, `modal_wrapper.py`, config files → `src/conversations/`.  
- Keep `mise.toml` and `.python-version` at root; mirror runtime settings in `configs/env/`.  
- Track Modal YAML under `infra/modal/`.

### ToolManagement

- `fastapi_app.py`, `tool_mgmt_api_client.py`, `mcp_server.py`, etc., relocate to `src/tool_management/`.  
- `neo4j` deployment assets go under `infra/` (`infra/docker/neo4j`, `infra/modal/`).  
- Docs like `COMPREHENSIVE_MODEL_BEHAVIOR_SPEC.md`, `DEPLOY_NEO4J.md` move to `docs/`.

### Realtime

- Keep `client/` under `src/` (rename to `src/client/`), `server.js` under `src/server/`.  
- Move build configs (`postcss.config.cjs`, `tailwind.config.js`, `vite.config.js`) into `configs/` if tooling allows; otherwise, add thin wrappers that re-export from `configs/`.  
- Add `tests/` for integration/unit tests (currently missing).  
- Put deployment scripts (if any) into `infra/`.

---

## Adoption Checklist (per service)

1. Create the directories listed above (even if temporarily empty) to make the layout visible.  
2. Move runtime code into `src/<service>` and update imports/tests.  
3. Relocate docs and specs into `docs/` with clear naming (`ADR-001`, `API.md`, etc.).  
4. Split environment/config files into `configs/` and provide `.env.example`.  
5. Move Modal/Docker/Terraform definitions under `infra/` and wire CI or Make targets to reference them.  
6. Update root `README.md` to link each service README and describe how to bootstrap the repo using `scripts/bootstrap_service.py` (to be written).  
7. Once every service conforms, move all service folders under `services/` and update tooling paths.

Adopting this structure gives every engineer the same expectations no matter which service they open, and it keeps deployment-related artifacts close to the code they manage.

