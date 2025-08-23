from datetime import datetime
import traceback
from math import exp
from typing import Optional, Dict, Any, List

from fastapi import FastAPI, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

from .config import settings
from . import crud
from .models import Signal
from .db import SessionLocal  # DB session factory

# Routers
from .routes.tasks import router as tasks_router
from .routes.companies_live import router as companies_live_router
from .routes.sources_admin import router as sources_admin_router
from .routes.sources_discovery import router as sources_discovery_router


# ---------- DB session dependency ----------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# -----------------------------
# FastAPI app
# -----------------------------
app = FastAPI(title="Hiring Radar API", version="0.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=getattr(settings, "ALLOWED_ORIGINS", ["*"]),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(tasks_router)
app.include_router(companies_live_router)
app.include_router(sources_admin_router)
app.include_router(sources_discovery_router)


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
# Companies (simple helpers)
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
# Legacy/Original-style scores
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
    Original-style materialized rows from hiring_score (if you populate it).
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

@app.get("/active_top_new")
def active_top_new(
    role_family: str = "SDE",
    days: int = Query(7, ge=1, le=90),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Top 'new' companies created in the last {days} days.
    Uses SUM(CASE ...) instead of FILTER for maximum compatibility.
    """
    try:
        sql = text("""
            SELECT
              c.id   AS company_id,
              c.name AS company_name,
              -- total SWE/SDE postings for the company
              SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde') THEN 1 ELSE 0 END)::int AS sde_openings,
              -- postings considered "new" in the last :days
              SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde')
                        AND COALESCE(jp.updated_at, jp.created_at) > (now() - make_interval(days => :days))
                       THEN 1 ELSE 0 END)::int AS sde_new
            FROM companies c
            LEFT JOIN job_postings jp
              ON jp.company_id = c.id
            WHERE c.created_at > (now() - make_interval(days => :days))
            GROUP BY c.id, c.name
            HAVING
              SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde')
                         AND COALESCE(jp.updated_at, jp.created_at) > (now() - make_interval(days => :days))
                       THEN 1 ELSE 0 END) > 0
              OR
              SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde') THEN 1 ELSE 0 END) > 0
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
                "details_json": {"window_days": days, "new_last_4w": sde_new, "score_formula": "3*new + min(open,50)"},
                "evidence_urls": [],
            })
        return out
    except Exception as e:
        print("[/active_top_new] error:", repr(e))
        return {"error": "active_top_new_failed", "detail": str(e)}


# -----------------------------
# New: Live score (normalized & momentum-aware)
# -----------------------------
def _safe_norm(values: List[int]):
    if not values:
        return 0.0, 0.0, (lambda x: 0.0)
    mn, mx = min(values), max(values)
    if mx <= mn:
        return mn, mx, (lambda x: 0.0)
    rng = float(mx - mn)
    return mn, mx, (lambda x: max(0.0, min(1.0, (float(x) - mn) / rng)))

@app.get("/scores_live")
def scores_live(
    db: Session = Depends(get_db),
    role_family: str = "SDE",
    window_days: int = Query(28, ge=7, le=90)
):
    """
    Live score = weighted normalized blend of:
      - Freshness: postings updated in last 7d
      - Momentum: (updates 0–14d) - (15–28d), clipped at 0
      - Volume: total open postings tagged as SWE/SDE
      - HN presence (binary from signals.kind='hn_whos_hiring' in last 35d)
      - Layoff penalty: recent layoff decay (90d half-life)
    """
    # 1) Feature pulls (no FILTER clause)
    rows = db.execute(text("""
      SELECT c.id AS company_id,
             SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde') THEN 1 ELSE 0 END)::int AS open_count,
             SUM(CASE WHEN lower(COALESCE(jp.role_family,'')) IN ('swe','software','sde')
                        AND COALESCE(jp.updated_at, jp.created_at) > (now() - interval '7 days')
                      THEN 1 ELSE 0 END)::int AS fresh_7d,
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

    feat = {}
    for r in rows:
        feat[r["company_id"]] = {
            "open_count": int(r["open_count"] or 0),
            "fresh_7d": int(r["fresh_7d"] or 0),
            "vel_pos": max(0, int(r["upd_0_14"] or 0) - int(r["upd_15_28"] or 0)),
            "hn": 0,
            "layoff_penalty": 0.0,
        }

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

    # Layoff decay (penalty)
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
            days = max(0.0, (now - t.replace(tzinfo=None)).total_seconds()/86400.0)
            decay = 0.5 ** (days / 90.0)
            feat[cid]["layoff_penalty"] = decay

    # Normalize features
    opens = [v["open_count"] for v in feat.values()]
    fresh = [v["fresh_7d"] for v in feat.values()]
    vpos  = [v["vel_pos"] for v in feat.values()]
    _, _, norm_open  = _safe_norm(opens)
    _, _, norm_fresh = _safe_norm(fresh)
    _, _, norm_vpos  = _safe_norm(vpos)

    # Weighted blend → 0..100
    out = []
    for cid, v in feat.items():
        s = (
            0.35 * norm_fresh(v["fresh_7d"]) +
            0.30 * norm_vpos(v["vel_pos"])  +
            0.20 * norm_open(v["open_count"]) +
            0.10 * (1.0 if v.get("hn") else 0.0) -
            0.15 * v.get("layoff_penalty", 0.0)
        )
        score = int(round(max(0.0, min(1.0, s)) * 100))
        out.append({
            "company_id": cid,
            "score": score,
            "features": v,
        })

    # Names
    if out:
        ids = [o["company_id"] for o in out]
        q = text("SELECT id, name FROM companies WHERE id = ANY(:ids)")
        name_rows = db.execute(q, {"ids": ids}).fetchall()
        names = {r[0]: r[1] for r in name_rows}
        for o in out:
            o["company_name"] = names.get(o["company_id"], "Unknown")

    out.sort(key=lambda x: (-x["score"], x["company_name"]))
    return out


# -----------------------------
# Forecast (stub kept for compatibility)
# -----------------------------
@app.get("/forecast/{company_id}")
def forecast_company(company_id: int, db: Session = Depends(get_db)):
    # Plug your model here as needed
    return {"company_id": company_id, "ok": True, "note": "forecast stub"}
