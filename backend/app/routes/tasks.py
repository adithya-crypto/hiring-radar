from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text
from typing import Optional
from ..db import SessionLocal

from ..connectors.greenhouse import fetch_greenhouse
from ..connectors.lever import fetch_lever
from ..connectors.ashby import fetch_ashby
from ..connectors.smartrecruiters import fetch_smartrecruiters

router = APIRouter(prefix="/tasks", tags=["tasks"])

def upsert_job_posting(db, it: dict):
    db.execute(text("""
        INSERT INTO job_postings
            (company_id, source_job_id, title, department, location,
             apply_url, created_at, updated_at, status, role_family)
        VALUES
            (:company_id, :source_job_id, :title, :department, :location,
             :apply_url, :created_at, :updated_at, :status, COALESCE(:role_family, 'SDE'))
        ON CONFLICT (company_id, source_job_id) DO UPDATE SET
            title        = EXCLUDED.title,
            department   = EXCLUDED.department,
            location     = EXCLUDED.location,
            apply_url    = EXCLUDED.apply_url,
            updated_at   = EXCLUDED.updated_at,
            status       = EXCLUDED.status,
            role_family  = COALESCE(EXCLUDED.role_family, job_postings.role_family)
    """), it)

def get_or_create_company_id(db, kind: str, handle: str, display_name: Optional[str]):
    """
    Robust company upsert keyed by (ats_kind, ats_handle). Also repairs legacy rows
    that exist by name but have NULL ats_handle.
    """
    if not handle or not handle.strip():
        raise ValueError("empty_handle")

    # 1) If exists by (ats_kind, ats_handle), use it
    row = db.execute(text(
        "SELECT id FROM companies WHERE ats_kind=:k AND ats_handle=:h"
    ), {"k": kind, "h": handle}).first()
    if row:
        return row[0]

    # 2) If there's a name-only legacy row, claim it by setting ats_* fields
    name_guess = (display_name or handle).strip()
    legacy = db.execute(text("""
        UPDATE companies
        SET ats_kind=:k, ats_handle=:h
        WHERE name=:n AND (ats_handle IS NULL OR ats_handle = '')
        RETURNING id
    """), {"k": kind, "h": handle, "n": name_guess}).first()
    if legacy:
        return legacy[0]

    # 3) Create or get via the unique (ats_kind, ats_handle)
    #    Keep existing name if present; otherwise set it from name_guess
    row = db.execute(text("""
        INSERT INTO companies(name, ats_kind, ats_handle)
        VALUES (:n, :k, :h)
        ON CONFLICT (ats_kind, ats_handle)
        DO UPDATE SET name = COALESCE(companies.name, EXCLUDED.name)
        RETURNING id
    """), {"n": name_guess, "k": kind, "h": handle}).first()
    return row[0]

@router.post("/ingest")
def run_ingest():
    db = SessionLocal()
    try:
        sources = db.execute(text("""
            SELECT id, kind, handle, display_name
            FROM sources
            WHERE enabled = true
              AND kind IN ('greenhouse','lever','ashby','smartrecruiters')
              AND handle IS NOT NULL
              AND trim(handle) <> ''
        """)).mappings().all()

        totals = {"greenhouse": 0, "lever": 0, "ashby": 0, "smartrecruiters": 0, "skipped": 0}
        total_upserts = 0

        for s in sources:
            kind = s["kind"]
            handle = (s["handle"] or "").strip()
            if not handle:
                totals["skipped"] += 1
                continue

            if kind == "greenhouse":
                items = fetch_greenhouse(handle)
            elif kind == "lever":
                items = fetch_lever(handle)
            elif kind == "ashby":
                items = fetch_ashby(handle, include_comp=True)
            elif kind == "smartrecruiters":
                items = fetch_smartrecruiters(handle)
            else:
                totals["skipped"] += 1
                continue

            # Resolve company id
            try:
                company_id = get_or_create_company_id(db, kind, handle, s.get("display_name"))
            except Exception as e:
                print("company_id error", {"kind": kind, "handle": handle}, e)
                totals["skipped"] += 1
                continue

            local = 0
            for it in items:
                it["company_id"] = company_id

                # Heuristic role family
                title = (it.get("title") or "").lower()
                it["role_family"] = "SDE" if any(k in title for k in ["software", "engineer", "developer", "swe", "sde"]) else None

                try:
                    upsert_job_posting(db, it)
                    local += 1
                except Exception as e:
                    # keep going; one bad row shouldn't kill the whole source
                    print("upsert error", {"kind": kind, "handle": handle, "job": it.get("source_job_id")}, e)

            if local:
                db.execute(text("UPDATE sources SET last_ok_at=now() WHERE id=:id"), {"id": s["id"]})
                totals[kind] += local
                total_upserts += local

        db.commit()
        return {"ok": True, "sources": len(sources), "upserts": total_upserts, "by_kind": totals}
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"ok": False, "error": "ingest_failed", "detail": str(e)})

@router.post("/forecast")
def run_forecast():
    return {"ok": True, "note": "forecast job stub"}