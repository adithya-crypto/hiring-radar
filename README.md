# Hiring Radar (Starter)

Track which companies are actively hiring SDEs and forecast who might ramp up hiring soon.

## Quickstart

### 1) Backend deps & DB
```bash
cd backend
cp .env.example .env
# Fill DATABASE_URL (e.g., postgresql+psycopg2://postgres:postgres@localhost:5432/hiring)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
alembic revision -m "init tables" --autogenerate
alembic upgrade head
```

Seed a few companies (psql or admin tool):
```sql
insert into companies(name, ticker, careers_url, ats_kind)
values
('Airbnb', 'ABNB', 'airbnb', 'greenhouse'),
('Databricks', NULL, 'databricks', 'lever');
```

### 2) Run backend
```bash
uvicorn app.main:app --reload --port 8000
```
Open http://localhost:8000/docs

### 3) Frontend
```bash
cd ../web
npm install
export NEXT_PUBLIC_API_BASE=http://localhost:8000
npm run dev
```
Open http://localhost:3000

### 4) Pull data
Use the buttons on the homepage or curl:
```bash
curl -X POST http://localhost:8000/tasks/ingest
curl -X POST http://localhost:8000/tasks/forecast
```

## Deploy on Railway (suggested)
- Create **Postgres** → copy `DATABASE_URL`.
- Deploy **backend** from `/backend` (Start cmd: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`).
- Env vars: `DATABASE_URL`, `ALLOWED_ORIGINS` → your web URL.
- Deploy **web** from `/web` (Env: `NEXT_PUBLIC_API_BASE` → backend URL).
- Add Railway **Cron** jobs:
  - Hourly: POST `/tasks/ingest`
  - Daily 03:00 UTC: POST `/tasks/forecast`

## Notes
- MVP supports **Greenhouse** and **Lever**. Add Ashby/SmartRecruiters connectors similarly.
- Hiring Score is simple; extend with external signals and better forecasting later.
- Avoid scraping restricted sources; rely on official ATS endpoints.
