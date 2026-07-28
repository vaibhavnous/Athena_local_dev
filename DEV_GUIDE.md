# Athena Developer Guide

Athena turns a business requirement into a reviewed, executable Bronze -> Silver -> Gold data pipeline. This is a short technical map for developers making their first change.

## Repository map

```text
apps/
├── backend/                 FastAPI API, orchestration, execution and tests
│   ├── api/                 HTTP entry point, auth, routers and UI projections
│   ├── nodes/               Database-source pipeline stages
│   ├── sftp_nodes/          SFTP/ADLS file-source stages
│   ├── services/            Runtime, checkpoints and warehouse integrations
│   ├── utilis/              Database, environment, logging and shared helpers
│   ├── scripts/             Operational and diagnostic scripts
│   └── tests/               Backend tests
└── frontend/                React/TypeScript control plane
    └── src/
        ├── api/             Backend HTTP client and base URL
        ├── pages/           Route-level screens
        ├── components/      Reusable UI and pipeline monitor
        ├── store/           Shared Zustand run/UI state
        ├── auth/ + context/ Authentication/session handling
        └── hooks/ + utils/  Polling and presentation logic

docs/                        Architecture images and supporting documentation
.github/                     GitHub build/deployment workflows
devops/azure/                Azure environment, infrastructure and pipelines
azure-pipelines-*.yml        Compatibility entry points for Azure DevOps
```

`apps/backend` and `apps/frontend` are the canonical source directories. Old local `Athena_backend/` or `frontend/` directories may remain after the migration because `.env` files are ignored; do not develop against them.

## Architecture at a glance

```text
React UI
   -> authenticated FastAPI routes
   -> pipeline runtime and checkpoints
   -> database or file-source nodes
   -> human review gates
   -> Bronze -> Silver -> Gold generation
   -> Snowflake / Databricks native or dbt execution
```

- `apps/backend/api/main.py` creates the API and registers routers.
- `apps/backend/api/routers/` owns HTTP validation and access control.
- `apps/backend/services/pipeline_runtime.py` coordinates run state, background work, reviews, retries and completion.
- `apps/backend/graph.py` and `nodes/` define the database-source workflow.
- `sftp_nodes/` and SFTP runtime services implement the file/ADLS path.
- Warehouse-specific services execute generated assets. `dbt_snowflake_runtime.py` builds and deploys a single Snowflake dbt project.
- Pipeline checkpoints are the backend source of truth. The frontend polls run/status APIs and projects them into the monitor.
- HITL gates deliberately pause execution. Approval resumes the same run; UI state must never independently invent backend completion.

## Run locally

Backend (Python 3.10):

```powershell
cd apps/backend
Copy-Item .env.example .env
python -m pip install -r requirements.txt
python -m uvicorn api.main:app --reload --port 8000
```

Frontend (Node 18):

```powershell
cd apps/frontend
Copy-Item .env.example .env.local
npm ci
npm start
```

The frontend opens on `http://localhost:3000` and defaults to the backend on `http://127.0.0.1:8000`. If the backend uses another port, set `REACT_APP_API_BASE_URL` in `apps/frontend/.env.local`. Never commit credentials; use `devops/azure/ENVIRONMENT.md` and the example files as the configuration contract.

## Where to make a change

| Change | Start here |
|---|---|
| API request/response | `apps/backend/api/routers/` |
| Run lifecycle, resume or failure handling | `apps/backend/services/pipeline_runtime.py` |
| Bronze/Silver/Gold generation | `apps/backend/nodes/` |
| Snowflake/dbt execution | `apps/backend/services/dbt_snowflake_runtime.py` |
| Databricks execution | `apps/backend/services/databricks_runtime.py` |
| SFTP/ADLS flow | `apps/backend/sftp_nodes/` and `services/sftp_*` |
| Page or route | `apps/frontend/src/pages/` and `App.tsx` |
| API polling | `apps/frontend/src/api/` and `hooks/` |
| Run status shown in UI | `apps/frontend/src/store/useAthenaStore.ts` and `utils/pipelinePhases.ts` |

## Safe change rules

1. Trace UI -> router -> service -> checkpoint before changing a run-state bug.
2. Keep source-specific behavior separated: database and SFTP/ADLS flows do not share every stage.
3. Treat terminal backend states and warehouse execution results as authoritative.
4. Add the smallest focused regression test beside the code changed.
5. Run backend tests from `apps/backend` and frontend tests/builds from `apps/frontend`.
6. Do not rename schemas, review keys, pipeline step keys or environment variables without tracing all callers.

For full product context read `README.md`; for configuration and deployment use `devops/azure/ENVIRONMENT.md` and `devops/azure/README.md`.
