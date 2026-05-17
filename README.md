# Athena (Monorepo)

This repo contains:

- `frontend/` – React UI (Create React App)
- `Athena_backend/` – FastAPI backend + pipeline runtime

## Local run

### Frontend

```bash
cd frontend
npm install
npm start
```

### Backend

```bash
cd Athena_backend
.venv\\Scripts\\python.exe -m uvicorn api.main:app --reload --port 8000
```

## CI

GitHub Actions builds the frontend and runs a Python `compileall` check for the backend.
