from datetime import datetime
import traceback
from typing import Optional, Dict, Any
from math import exp
from collections import defaultdict

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

from .config import settings
from . import crud
from .forecast import forecast_month
from .models import Signal
from .db import SessionLocal  # used to define local get_db

# Routers
from .routes.tasks import router as tasks_router
from .routes.companies_live import router as companies_live_router
from .routes.sources_admin import router as sources_admin_router
from .routes.sources_discovery import router as sources_discovery_router

# ---------- FastAPI app ----------
app = FastAPI(title="Hiring Radar API", version="0.1.0")

# ---------- CORS ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=getattr(settings, "ALLOWED_ORIGINS", ["*"]),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Register routers (after app is created) ----------
app.include_router(tasks_router)              # /tasks/ingest, /tasks/forecast
app.include_router(companies_live_router)     # /live/companies
app.include_router(sources_admin_router)      # /admin/sources/bulk
app.include_router(sources_discovery_router)  # /admin/sources/discover/*

# ---------- DB session dependency ----------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Optional scheduler (hourly ingest / daily forecast)
try:
    from .jobs.scheduler import attach_scheduler  # if you have one
except Exception:
    attach_scheduler = None


def _safe_norm(values):
    # returns (min, max, lambda->norm0..1)
    if not values:
        return 0.0, 0.0, (lambda x: 0.0)
    mn, mx = min(values), max(values)
    if mx <= mn:
        return mn, mx, (lambda x: 0.0)
    rng = (mx - mn) * 1.0
    return mn, mx, (lambda x: max(0.0, min(1.0, (x - mn) / rng)))


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
class CompanyIn(BaseModel):
    name: str
    careers_url: str
    ats_kind: str  # "greenhouse" | "lever" | "ashby" | "smartrecruiters"

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
class SignalIn(BaseModel):
    company_id: int
    kind: str  # "hn_whos_hiring" | "layoff" | "funding" | "earnings"
    happened_at: datetime
    payload_json: Optional[Dict[str, Any]] = None

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

from sqlalchemy import text
from fastapi import Query
from sqlalchemy.orm import Session

@app.get("/live/companies")
def live_companies(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = 0,
    db: Session = Depends(get_db)
):
    rows = db.execute(text("""
      SELECT
        c.id,
        c.name,
        c.ats_kind,
        c.ats_handle,
        COUNT(*) FILTER (WHERE jp.status='OPEN') AS open_roles,
        MAX(jp.updated_at) AS last_update
      FROM job_postings jp
      JOIN companies c ON c.id = jp.company_id
      GROUP BY c.id, c.name, c.ats_kind, c.ats_handle
      ORDER BY last_update DESC
      LIMIT :limit OFFSET :offset
    """), {"limit": limit, "offset": offset}).mappings().all()
    return [dict(r) for r in rows]


# -----------------------------
# Scores / Active
# -----------------------------
def compute_activity_score(sde_new: int, sde_openings: int) -> int:
    # Emphasize recent activity, cap openings influence so whales don't dominate
    return 3 * int(sde_new or 0) + min(int(sde_openings or 0), 50)

@app.get("/scores")
def scores(
    db: Session = Depends(get_db),
    role_family: str = "software",
    limit: int = 50
):
    """
    Original-style materialized rows from hiring_score.
    Returns array shaped for the frontend table.
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

    out = []
    for r in rows:
        dj = r["details_json"] or {}
        out.append({
            "company_id":   r["company_id"],
            "company_name": r["company_name"],
            "role_family":  role_family,
            "score":        int(r["score"] or 0),
            "details_json": dj,
            "evidence_urls": (dj.get("evidence_urls") or []),
            "open_count":    dj.get("open_now"),
        })
    return out

@app.get("/active_top")
def active_top(
    family: str = "swe",
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Top companies for the current week by SWE activity (job_metrics).
    Returns UI-friendly fields: score (weighted), open_count, details_json.new_last_4w.
    """
    try:
        sql = text("""
            SELECT
              c.id   AS company_id,
              c.name AS company_name,
              jm.sde_new,
              jm.sde_openings,
              jm.sde_closed
            FROM job_metrics jm
            JOIN companies c ON c.id = jm.company_id
            WHERE jm.week_start = date_trunc('week', now())
              AND jm.role_family IN (:family, 'software')
              AND (jm.sde_openings > 0 OR jm.sde_new > 0)
            ORDER BY jm.sde_new DESC, jm.sde_openings DESC, c.name
            LIMIT :limit
        """)
        rows = db.execute(sql, {"family": family, "limit": limit}).mappings().all()

        out = []
        for r in rows:
            sde_new = int(r["sde_new"] or 0)
            sde_open = int(r["sde_openings"] or 0)
            score = compute_activity_score(sde_new, sde_open)
            out.append({
                "company_id": r["company_id"],
                "company_name": r["company_name"],
                "sde_openings": sde_open,
                "sde_new": sde_new,
                "sde_closed": int(r["sde_closed"] or 0),
                # UI fields:
                "score": score,
                "open_count": sde_open,
                "details_json": {"new_last_4w": sde_new, "score_formula": "3*new + min(open,50)"},
                "evidence_urls": [],
            })
        return out
    except Exception as e:
        tb = traceback.format_exc()
        print("[/active_top] error:", repr(e), "\n", tb)
        return JSONResponse(
            status_code=500, content={"error": "active_top_failed", "detail": str(e)}
        )
    
@app.get("/scores_live")
def scores_live(
    db: Session = Depends(get_db),
    role_family: str = "SDE",
    window_days: int = Query(28, ge=7, le=90)
):
    """
    Live score = weighted normalized blend of:
      - Freshness: postings updated in last 7d
      - Velocity: (updates 0-14d) - (updates 15-28d), clipped at 0
      - Volume: total open postings tagged as SWE/SDE
      - HN presence (binary from signals.kind='hn_whos_hiring' in last 35d)
      - Layoff penalty: recent layoff decay (90d half-life)
    """
    # 1) Feature pulls
    # Open SWE/SDE
    rows = db.execute(text("""
      SELECT c.id AS company_id,
             SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde') THEN 1 ELSE 0 END)::int AS open_count,
             SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde')
                       AND COALESCE(jp.updated_at, jp.created_at) > (now() - interval '7 days')
                      THEN 1 ELSE 0 END)::int AS fresh_7d,
             -- 0-14d vs 15-28d buckets
             SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde')
                       AND COALESCE(jp.updated_at, jp.created_at) > (now() - interval '14 days')
                      THEN 1 ELSE 0 END)::int AS upd_0_14,
             SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde')
                       AND COALESCE(jp.updated_at, jp.created_at) <= (now() - interval '14 days')
                       AND COALESCE(jp.updated_at, jp.created_at) > (now() - interval '28 days')
                      THEN 1 ELSE 0 END)::int AS upd_15_28
      FROM companies c
      LEFT JOIN job_postings jp ON jp.company_id = c.id
      GROUP BY c.id
    """)).mappings().all()

    feat = {r["company_id"]: {
        "open_count": int(r["open_count"] or 0),
        "fresh_7d": int(r["fresh_7d"] or 0),
        "vel_pos": max(0, int(r["upd_0_14"] or 0) - int(r["upd_15_28"] or 0)),
    } for r in rows}

    # HN presence (last 35d)
    rows = db.execute(text("""
      SELECT company_id, COUNT(*)::int AS n
      FROM signals
      WHERE kind='hn_whos_hiring'
        AND happened_at > (now() - interval '35 days')
      GROUP BY company_id
    """)).mappings().all()
    for r in rows:
        cid = r["company_id"]
        if cid in feat:
            feat[cid]["hn"] = 1
    for cid in feat:
        feat[cid].setdefault("hn", 0)

    # Layoff decay (penalty): 1.0 at event, exponential decay ~90d half-life
    rows = db.execute(text("""
      SELECT company_id, MAX(happened_at) AS last_layoff
      FROM signals
      WHERE kind='layoff'
      GROUP BY company_id
    """)).mappings().all()
    import datetime as dt
    now = dt.datetime.utcnow().replace(tzinfo=None)
    for r in rows:
        cid = r["company_id"]
        t = r["last_layoff"]
        if t and cid in feat:
            # half-life ~90d => decay factor
            days = max(0.0, (now - t.replace(tzinfo=None)).total_seconds()/86400.0)
            decay = 0.5 ** (days / 90.0)  # 1 -> ~0 over ~months
            feat[cid]["layoff_penalty"] = decay
    for cid in feat:
        feat[cid].setdefault("layoff_penalty", 0.0)

    # 2) Normalize features across companies
    opens = [v["open_count"] for v in feat.values()]
    fresh = [v["fresh_7d"] for v in feat.values()]
    vpos  = [v["vel_pos"] for v in feat.values()]
    _, _, norm_open = _safe_norm(opens)
    _, _, norm_fresh = _safe_norm(fresh)
    _, _, norm_vpos = _safe_norm(vpos)

    # 3) Weighted blend (0..100)
    out = []
    for cid, v in feat.items():
        s = (
            0.35 * norm_fresh(v["fresh_7d"]) +     # emphasize very recent activity
            0.30 * norm_vpos(v["vel_pos"]) +      # momentum
            0.20 * norm_open(v["open_count"]) +   # size, but capped by norm
            0.10 * (1.0 if v.get("hn") else 0.0)  # community signal
            - 0.15 * v.get("layoff_penalty", 0.0) # recent layoffs penalty
        )
        score = int(round(max(0.0, min(1.0, s)) * 100))
        out.append({
            "company_id": cid,
            "score": score,
            "features": v,
        })

    # Attach company names for UI
    if out:
        ids = tuple([o["company_id"] for o in out])
        q = text("SELECT id, name FROM companies WHERE id = ANY(:ids)")
        names = {r[0]: r[1] for r in db.execute(q, {"ids": list(ids)}).fetchall()}
        for o in out:
            o["company_name"] = names.get(o["company_id"], f"id:{o['company_id']}")

    # Highest-first
    out.sort(key=lambda x: (-x["score"], x["company_name"]))
    return out    

# Legacy (kept)
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
        sql = text("""
            SELECT
              c.id   AS company_id,
              c.name AS company_name,
              SUM(CASE WHEN lower(COALESCE(jp.role_family, '')) IN ('swe','software','sde')
                       THEN 1 ELSE 0 END)::int AS sde_openings,
              SUM(CASE WHEN lower(COALESCE(jp.role_family, '')) IN ('swe','software','sde')
                        AND COALESCE(jp.updated_at, jp.created_at) > (now() - make_interval(days => :days))
                       THEN 1 ELSE 0 END)::int AS sde_new
            FROM companies c
            LEFT JOIN job_postings jp ON jp.company_id = c.id
            WHERE c.created_at > (now() - make_interval(days => :days))
            GROUP BY c.id, c.name
            HAVING
              SUM(CASE WHEN lower(COALESCE(jp.role_family, '')) IN ('swe','software','sde')
                        AND COALESCE(jp.updated_at, jp.created_at) > (now() - make_interval(days => :days))
                       THEN 1 ELSE 0 END) > 0
              OR
              SUM(CASE WHEN lower(COALESCE(jp.role_family, '')) IN ('swe','software','sde')
                       THEN 1 ELSE 0 END) > 0
            ORDER BY sde_new DESC, sde_openings DESC, c.name
            LIMIT :limit
        """)
        rows = db.execute(sql, {"days": days, "limit": limit}).mappings().all()

        out = []
        for r in rows:
            sde_new = int(r["sde_new"] or 0)
            sde_open = int(r["sde_openings"] or 0)
            score = compute_activity_score(sde_new, sde_open)
            out.append({
                "company_id": r["company_id"],
                "company_name": r["company_name"],
                "sde_openings": sde_open,
                "sde_new": sde_new,
                "sde_closed": 0,
                "score": score,
                "open_count": sde_open,
                "details_json": {
                    "window_days": days,
                    "new_last_4w": sde_new,
                    "score_formula": "3*new + min(open,50)"
                },
                "evidence_urls": [],
            })
        return out
    except Exception as e:
        return JSONResponse(status_code=500, content={"error":"active_top_new_failed","detail":str(e)})


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
