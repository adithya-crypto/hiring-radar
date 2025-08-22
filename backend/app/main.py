from datetime import datetime
import traceback
from typing import Optional, Dict, Any

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

# from app.db import get_db  # <-- REMOVE this
from .config import settings
from . import crud
from .forecast import forecast_month
from .models import Signal
from .jobs.run_discovery import run_discovery_now
from .db import SessionLocal  # <-- use this to define get_db locally

# ---------- DB session dependency ----------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Optional scheduler (hourly ingest / daily forecast)
try:
    from .jobs.scheduler import attach_scheduler
except Exception:
    attach_scheduler = None

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
# -----------------------------
FAMILY_FILTER = "lower(jm.role_family) IN ('swe','software','sde')"
LATEST_WEEK_SQL = "(SELECT date_trunc('week', MAX(week_start)) FROM job_metrics)"

# ----- Scores / Active – original "hiring_score" materialized view -----

@app.get("/scores")
def scores(db: Session = Depends(get_db), role_family: str = "software", limit: int = 50):
    """
    Return top companies by 'hiring_score' (materialized from job_postings).
    Defaults to role_family='software' which covers SWE/SDE roles.
    """
    sql = text("""
        SELECT
          c.id   AS company_id,
          c.name AS company_name,
          hs.score,
          hs.details_json
        FROM hiring_score hs
        JOIN companies c ON c.id = hs.company_id
        WHERE lower(hs.role_family) IN (:rf1, :rf2, :rf3)
        ORDER BY hs.score DESC, c.name
        LIMIT :limit
    """)
    rows = db.execute(sql, {
        "rf1": role_family.lower(),
        "rf2": "swe",
        "rf3": "sde",
        "limit": limit
    }).mappings().all()
    # Shape similar to your original UI expectations
    out = []
    for r in rows:
        dj = r["details_json"] or {}
        out.append({
            "company_id":   r["company_id"],
            "company_name": r["company_name"],
            "role_family":  role_family,
            "score":        r["score"],
            "details_json": dj,
            "evidence_urls": (dj.get("evidence_urls") or []),
            "open_count":    dj.get("open_now"),
        })
    return out


@app.get("/active_top")
def active_top(
    role_family: str = "software",
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Top companies by hiring_score (original logic).
    """
    try:
        sql = text("""
            SELECT
              c.id   AS company_id,
              c.name AS company_name,
              hs.score,
              (hs.details_json->>'open_now')::int   AS sde_openings,
              (hs.details_json->>'new_last_4w')::int AS sde_new,
              0::int AS sde_closed
            FROM hiring_score hs
            JOIN companies c ON c.id = hs.company_id
            WHERE lower(hs.role_family) IN (:rf1, :rf2, :rf3)
            ORDER BY hs.score DESC, c.name
            LIMIT :limit
        """)
        rows = db.execute(sql, {
            "rf1": role_family.lower(),
            "rf2": "swe",
            "rf3": "sde",
            "limit": limit
        }).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        from fastapi.responses import JSONResponse
        import traceback
        return JSONResponse(status_code=500, content={"error": "active_top_failed", "detail": str(e), "tb": traceback.format_exc()})


# Legacy
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
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Top 'new' companies (created recently) ordered by latest SWE/SDE postings.
    If companies.created_at doesn't exist, we fall back to companies that had
    any SWE/SDE posting updated/created in the last {days}.
    """
    try:
        # First try: use companies.created_at if present
        sql1 = text(f"""
            SELECT
              c.id   AS company_id,
              c.name AS company_name,
              COALESCE(SUM(CASE WHEN jp.closed_at IS NULL THEN 1 ELSE 0 END), 0)::int AS sde_openings,
              COALESCE(SUM(CASE WHEN COALESCE(jp.updated_at, jp.created_at) > now() - INTERVAL '{days} days' THEN 1 ELSE 0 END), 0)::int AS sde_new,
              0::int AS sde_closed
            FROM companies c
            LEFT JOIN job_postings jp
              ON jp.company_id = c.id
             AND lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde')
            WHERE c.created_at > now() - INTERVAL '{days} days'
            GROUP BY c.id, c.name
            HAVING COALESCE(SUM(CASE WHEN COALESCE(jp.updated_at, jp.created_at) > now() - INTERVAL '{days} days' THEN 1 ELSE 0 END), 0) > 0
               OR COALESCE(SUM(CASE WHEN jp.closed_at IS NULL THEN 1 ELSE 0 END), 0) > 0
            ORDER BY sde_new DESC, sde_openings DESC, c.name
            LIMIT :limit
        """)
        rows = db.execute(sql1, {"limit": limit}).mappings().all()
        if rows:
            return [dict(r) for r in rows]

        # Fallback: if companies.created_at is missing/unused, derive "new" by first postings
        sql2 = text(f"""
            WITH recent_companies AS (
              SELECT DISTINCT jp.company_id
              FROM job_postings jp
              WHERE lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde')
                AND COALESCE(jp.updated_at, jp.created_at) > now() - INTERVAL '{days} days'
            )
            SELECT
              c.id   AS company_id,
              c.name AS company_name,
              COALESCE(SUM(CASE WHEN jp.closed_at IS NULL THEN 1 ELSE 0 END), 0)::int AS sde_openings,
              COALESCE(SUM(CASE WHEN COALESCE(jp.updated_at, jp.created_at) > now() - INTERVAL '{days} days' THEN 1 ELSE 0 END), 0)::int AS sde_new,
              0::int AS sde_closed
            FROM recent_companies rc
            JOIN companies c ON c.id = rc.company_id
            LEFT JOIN job_postings jp
              ON jp.company_id = c.id
             AND lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde')
            GROUP BY c.id, c.name
            ORDER BY sde_new DESC, sde_openings DESC, c.name
            LIMIT :limit
        """)
        rows = db.execute(sql2, {"limit": limit}).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": "active_top_new_failed", "detail": str(e)})


# -----------------------------
# Tasks: discover / ingest / forecast
# -----------------------------
@app.post("/tasks/discover")
def run_discover(db: Session = Depends(get_db)):
    try:
        disc = run_discovery_now(db)
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
