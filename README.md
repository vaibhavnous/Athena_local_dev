# Athena (Monorepo)

Single repo containing:

- `frontend/` — React UI (Create React App)
- `Athena_backend/` — FastAPI backend + pipeline runtime

## Prerequisites

- Node.js **20.x** (recommended) for `react-scripts@5`
- Python **3.11.x** for backend
- (Backend) Access to:
  - Azure SQL (ODBC)
  - Pinecone
  - Azure OpenAI (or OpenAI-compatible provider)

## Quick start (local)

### 1) Frontend

```bash
cd frontend
npm ci
npm start
```

Frontend: `http://localhost:3000`

### 2) Backend

Create a local env file:

```powershell
cd Athena_backend
copy .env.example .env
```

Fill in values in `Athena_backend/.env`, then:

```powershell
cd Athena_backend
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python -m uvicorn api.main:app --reload --port 8000
```

Backend: `http://localhost:8000`

## Configuration

Backend reads environment variables (commonly via `Athena_backend/.env`).

**Pinecone**
- `PINECONE_API_KEY`
- `PINECONE_INDEX_NAME`
- `PINECONE_ENVIRONMENT`
- `PINECONE_KNOWLEDGE_BASE_INDEX_NAME` (defaults to `knowledgebase`)

**Optional Domain Knowledge Base**
- `ATHENA_USE_DOMAIN_KB` (default `false`; set `true` for runtime retrieval)
- `ATHENA_KB_ID` (defaults to `PC_Insurance_V1`)
- `ATHENA_DOMAIN_PROFILE` (defaults to `Insurance`)
- `ATHENA_KB_TOP_K_ENRICHMENT`, `ATHENA_KB_TOP_K_GOLD`
- `ATHENA_KB_MAX_CHARS_ENRICHMENT`, `ATHENA_KB_MAX_CHARS_GOLD`
- One-time build: `cd Athena_backend && python scripts/build_domain_kb.py`

**Azure OpenAI**
- `AZURE_OPENAI_API_KEY`
- `AZURE_OPENAI_ENDPOINT`
- `AZURE_OPENAI_API_VERSION`
- `AZURE_OPENAI_DEPLOYMENT`

**Azure SQL (pipeline + source DB)**
- `AZURE_SQL_HOST`, `AZURE_SQL_PORT`, `AZURE_SQL_DRIVER`
- `AZURE_SQL_PIPELINE_DATABASE`, `AZURE_SQL_PIPELINE_SCHEMA`
- `AZURE_SQL_USERNAME`, `AZURE_SQL_PASSWORD`
- `AZURE_SQL_SOURCE_DATABASE`, `AZURE_SQL_SOURCE_SCHEMA`
- `AZURE_SQL_SOURCE_HOST`, `AZURE_SQL_SOURCE_USERNAME`, `AZURE_SQL_SOURCE_PASSWORD`

**CORS**
- `ATHENA_CORS_ORIGINS` (comma-separated; defaults include `http://localhost:3000`)

## Production build

```bash
cd frontend
npm ci
npm run build
```

## CI

GitHub Actions:
- Builds `frontend/`
- Runs `python -m compileall` for `Athena_backend/`

## Notes

- Do **not** commit secrets. Keep `Athena_backend/.env` local; use `Athena_backend/.env.example` as a template.
