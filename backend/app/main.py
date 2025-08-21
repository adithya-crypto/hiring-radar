from datetime import datetime
import traceback
from typing import Optional, Dict, Any

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import get_db  # shared DB dependency
from .config import settings
from . import crud
from .forecast import forecast_month
from .models import Signal
from .jobs.run_discovery import run_discovery_now

# Optional scheduler (hourly ingest / daily forecast)
try:
    from .jobs.scheduler import attach_scheduler
except Exception:
    attach_scheduler = None  # fine for local/dev

# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="Hiring Radar API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Request models
# -----------------------------
class CompanyIn(BaseModel):
    name: str
    careers_url: str
    ats_kind: str  # "greenhouse" | "lever" | "ashby" | "smartrecruiters"

class SignalIn(BaseModel):
    company_id: int
    kind: str  # "hn_whos_hiring" | "layoff" | "funding" | "earnings"
    happened_at: datetime
    payload_json: Optional[Dict[str, Any]] = None

# -----------------------------
# Health
# -----------------------------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/health/db")
def health_db(db: Session = Depends(get_db)):
    db.execute(text("select 1"))
    return {"ok": True}

# -----------------------------
# Companies
# -----------------------------
@app.get("/companies")
def list_companies(db: Session = Depends(get_db)):
    return crud.list_companies(db)

@app.get("/companies/{company_id}")
def company_detail(company_id: int, db: Session = Depends(get_db)):
    obj = crud.company_detail(db, company_id)
    if not obj:
        raise HTTPException(status_code=404, detail="company_not_found")
    return obj

@app.post("/companies")
def add_company(payload: CompanyIn, db: Session = Depends(get_db)):
    from .models import Company
    exists = db.query(Company).filter(Company.name == payload.name).first()
    if exists:
        return {"ok": True, "id": exists.id, "note": "already_exists"}
    c = Company(
        name=payload.name.strip(),
        careers_url=payload.careers_url.strip(),
        ats_kind=payload.ats_kind.strip().lower(),
    )
    db.add(c)
    db.commit()
    db.refresh(c)
    return {"ok": True, "id": c.id}

@app.get("/companies/{company_id}/postings")
def company_postings(
    company_id: int,
    role_family: str = "SDE",
    since_hours: Optional[int] = None,
    since_days: Optional[int] = None,
    db: Session = Depends(get_db),
):
    obj = crud.company_detail(db, company_id)
    if not obj:
        raise HTTPException(status_code=404, detail="company_not_found")

    return crud.list_company_postings(
        db,
        company_id=company_id,
        role_family=role_family,
        since_hours=since_hours,
        since_days=since_days,
    )

# -----------------------------
# Signals
# -----------------------------
@app.post("/signals")
def add_signal(payload: SignalIn, db: Session = Depends(get_db)):
    s = Signal(
        company_id=payload.company_id,
        kind=payload.kind,
        happened_at=payload.happened_at,
        payload_json=payload.payload_json,
    )
    db.add(s)
    db.commit()
    db.refresh(s)
    return {"ok": True, "id": s.id}

# -----------------------------
# Scores / Active (from job_metrics)
# Robust handling of role_family and week
# -----------------------------
FAMILY_FILTER = "lower(jm.role_family) IN ('swe','software','sde')"
LATEST_WEEK_SQL = "(SELECT date_trunc('week', MAX(week_start)) FROM job_metrics)"

@app.get("/scores")
def scores(db: Session = Depends(get_db), limit: int = 50):
    """
    Latest-week metrics from job_metrics. Robust to 'swe'/'software'/'sde' labels.
    Always returns JSON (even on error).
    """
    try:
        sql = text(f"""
            SELECT
              c.id   AS company_id,
              c.name AS company_name,
              jm.sde_openings,
              jm.sde_new,
              jm.sde_closed
            FROM job_metrics jm
            JOIN companies  c ON c.id = jm.company_id
            WHERE jm.week_start = {LATEST_WEEK_SQL}
              AND {FAMILY_FILTER}
            ORDER BY jm.sde_openings DESC, jm.sde_new DESC, c.name
            LIMIT :limit
        """)
        rows = db.execute(sql, {"limit": limit}).mappings().all()
        return JSONResponse(content=[dict(r) for r in rows])
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "scores_failed", "detail": str(e)})

@app.get("/active_top")
def active_top(db: Session = Depends(get_db), limit: int = Query(50, ge=1, le=200)):
    """
    Top companies by new SWE postings for the latest week with data.
    Robust to role_family label differences and always returns JSON.
    """
    try:
        sql = text(f"""
            SELECT
              c.id   AS company_id,
              c.name AS company_name,
              jm.sde_new,
              jm.sde_openings,
              jm.sde_closed
            FROM job_metrics jm
            JOIN companies c ON c.id = jm.company_id
            WHERE jm.week_start = {LATEST_WEEK_SQL}
              AND {FAMILY_FILTER}
              AND (jm.sde_openings > 0 OR jm.sde_new > 0)
            ORDER BY jm.sde_new DESC, jm.sde_openings DESC, c.name
            LIMIT :limit
        """)
        rows = db.execute(sql, {"limit": limit}).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "active_top_failed", "detail": str(e)})

# Legacy/aux flows (left as-is, still useful)
@app.get("/active")
def active(role_family: str = "SDE", min_score: int = 20, db: Session = Depends(get_db)):
    rows = crud.list_scores(db, role_family)
    return [r for r in rows if r["score"] >= min_score]

@app.get("/new_companies")
def new_companies(days: int = 7, db: Session = Depends(get_db)):
    try:
        return crud.list_new_companies(db, days=days)
    except Exception as e:
        return {"error": "new_companies_failed", "detail": str(e)}

@app.get("/active_top_new")
def active_top_new(
    role_family: str = "SDE",
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    try:
        return crud.list_active_top_new(db, role_family=role_family, days=days, limit=limit)
    except Exception as e:
        return {"error": "active_top_new_failed", "detail": str(e)}

# -----------------------------
# Tasks: discover / ingest / forecast
# -----------------------------
@app.post("/tasks/discover")
def run_discover(db: Session = Depends(get_db)):
    try:
        disc = run_discovery_now(db)
        # optionally chain ingest + forecast
        try:
            from .jobs.run_ingest import run_ingest_now
            ing = run_ingest_now(db)
        except Exception as e:
            ing = {"error": f"ingest_failed: {e}"}
        try:
            from .jobs.run_forecast import run_forecast_now
            fc = {"forecasted": run_forecast_now(db)}
        except Exception as e:
            fc = {"error": f"forecast_failed: {e}"}
        return {"discover": disc, "ingest": ing, "forecast": fc}
    except Exception as e:
        return {"error": "discover_failed", "detail": str(e)}

@app.post("/tasks/ingest")
def run_ingest(db: Session = Depends(get_db)):
    try:
        from .jobs.run_ingest import run_ingest_now
        result = run_ingest_now(db)
        return result
    except Exception as e:
        return {"error": "ingest_failed", "detail": str(e)}

@app.post("/tasks/forecast")
def run_forecast(db: Session = Depends(get_db)):
    try:
        from .jobs.run_forecast import run_forecast_now
        n = run_forecast_now(db)
        return {"forecasted": n}
    except Exception as e:
        tb = traceback.format_exc()
        return {"error": "forecast_failed", "detail": str(e), "trace": tb}

# -----------------------------
# Forecast (per company) & debug
# -----------------------------
@app.get("/forecast/{company_id}")
def forecast_company(company_id: int, db: Session = Depends(get_db)):
    return forecast_month(db, company_id)

@app.get("/debug/raw/{company_id}")
def latest_raw(company_id: int, db: Session = Depends(get_db)):
    from .models import JobRaw
    row = (
        db.query(JobRaw)
        .filter_by(company_id=company_id)
        .order_by(JobRaw.id.desc())
        .first()
    )
    return {"ok": True, "payload": row.payload_json if row else []}

# -----------------------------
# Attach scheduler (optional)
# -----------------------------
if attach_scheduler is not None:
    try:
        attach_scheduler(app)
    except Exception as e:
        print("[scheduler] attach failed:", repr(e))
