# backend/app/routes/tasks.py

from fastapi import APIRouter
from sqlalchemy import text
from ..db import SessionLocal

# Connectors for all four ATS
from ..connectors.greenhouse import fetch_greenhouse
from ..connectors.lever import fetch_lever
from ..connectors.ashby import fetch_ashby
from ..connectors.smartrecruiters import fetch_smartrecruiters

router = APIRouter(prefix="/tasks", tags=["tasks"])

def upsert_job_posting(db, it: dict):
    """
    Idempotent upsert into job_postings keyed by (company_id, source_job_id).
    """
    db.execute(text("""
        INSERT INTO job_postings
            (company_id, source_job_id, title, department, location,
             apply_url, created_at, updated_at, status)
        VALUES
            (:company_id, :source_job_id, :title, :department, :location,
             :apply_url, :created_at, :updated_at, :status)
        ON CONFLICT (company_id, source_job_id) DO UPDATE SET
            title        = EXCLUDED.title,
            department   = EXCLUDED.department,
            location     = EXCLUDED.location,
            apply_url    = EXCLUDED.apply_url,
            updated_at   = EXCLUDED.updated_at,
            status       = EXCLUDED.status
    """), it)

def get_or_create_company_id(db, kind: str, handle: str, display_name: str | None):
    """
    Use (ats_kind, ats_handle) as the stable key. Create company row lazily if needed.
    """
    row = db.execute(text(
        "SELECT id FROM companies WHERE ats_kind=:k AND ats_handle=:h"
    ), {"k": kind, "h": handle}).first()
    if row:
        return row[0]

    name = display_name or handle
    row = db.execute(text("""
        INSERT INTO companies (name, ats_kind, ats_handle)
        VALUES (:name, :k, :h)
        ON CONFLICT (name) DO NOTHING
        RETURNING id
    """), {"name": name, "k": kind, "h": handle}).first()
    if row:
        return row[0]

    # Fallback if name conflicted with an existing row
    return db.execute(text(
        "SELECT id FROM companies WHERE ats_kind=:k AND ats_handle=:h"
    ), {"k": kind, "h": handle}).first()[0]

@router.post("/ingest")
def run_ingest():
    """
    Iterate all enabled sources (Greenhouse/Lever/Ashby/SmartRecruiters),
    upsert postings, and stamp last_ok_at on success.
    """
    db = SessionLocal()
    sources = db.execute(text("""
        SELECT id, kind, handle, display_name
        FROM sources
        WHERE enabled = true
          AND kind IN ('greenhouse','lever','ashby','smartrecruiters')
    """)).mappings().all()

    totals = {"greenhouse": 0, "lever": 0, "ashby": 0, "smartrecruiters": 0}
    total_upserts = 0

    for s in sources:
        kind = s["kind"]
        handle = s["handle"]

        # Dispatch per ATS
        if kind == "greenhouse":
            items = fetch_greenhouse(handle)
        elif kind == "lever":
            items = fetch_lever(handle)
        elif kind == "ashby":
            items = fetch_ashby(handle, include_comp=True)
        elif kind == "smartrecruiters":
            items = fetch_smartrecruiters(handle)
        else:
            continue  # should be filtered out by SQL

        company_id = get_or_create_company_id(db, kind, handle, s["display_name"])

        local = 0
        try:
            for it in items:
                it["company_id"] = company_id
                upsert_job_posting(db, it)
                local += 1
        except Exception as e:
            # Log & continue to next source; swap print for structured logging if you have it
            print("ingest error", {"kind": kind, "handle": handle}, e)

        if local:
            db.execute(text("UPDATE sources SET last_ok_at=now() WHERE id=:id"), {"id": s["id"]})
            totals[kind] += local
            total_upserts += local

    db.commit()
    return {"ok": True, "sources": len(sources), "upserts": total_upserts, "by_kind": totals}

@router.post("/forecast")
def run_forecast():
    # Wire your forecasting job here (or call an existing service)
    return {"ok": True, "note": "forecast job stub"}
