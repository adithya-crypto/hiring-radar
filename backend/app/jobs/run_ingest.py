# backend/app/jobs/run_ingest.py
from typing import Dict, Any, List, Tuple
import datetime as dt
from sqlalchemy import text
from ..db import SessionLocal

# connectors
from ..connectors.greenhouse import fetch_greenhouse
try:
    from ..connectors.lever import fetch_lever
except Exception:
    fetch_lever = None
try:
    from ..connectors.ashby import fetch_ashby
except Exception:
    fetch_ashby = None
try:
    from ..connectors.smartrecruiters import fetch_smartrecruiters
except Exception:
    fetch_smartrecruiters = None


UPSERT_SQL = text("""
    INSERT INTO job_postings
        (company_id, source_job_id, title, department, location,
         apply_url, created_at, updated_at, status, role_family)
    VALUES
        (:company_id, :source_job_id, :title, :department, :location,
         :apply_url, :created_at, :updated_at, :status, :role_family)
    ON CONFLICT (company_id, source_job_id) DO UPDATE SET
        title       = EXCLUDED.title,
        department  = EXCLUDED.department,
        location    = EXCLUDED.location,
        apply_url   = EXCLUDED.apply_url,
        updated_at  = EXCLUDED.updated_at,
        status      = EXCLUDED.status,
        role_family = EXCLUDED.role_family
""")

def _classify_role_family(title: str, department: str | None) -> str | None:
    t = (title or "").lower()
    d = (department or "").lower()
    keywords = [
        "software", "swe", "sde", "backend", "front end", "frontend",
        "fullstack", "full stack", "mobile", "ios", "android",
        "platform", "infrastructure", "distributed", "api",
        "machine learning", "ml engineer", "data engineer"
    ]
    for kw in keywords:
        if kw in t or kw in d:
            return "SDE"
    return None

def _normalize_item(company_id: int, it: Dict[str, Any]) -> Dict[str, Any]:
    # required
    out = {
        "company_id": company_id,
        "source_job_id": str(it.get("source_job_id") or it.get("id")),
        "title": it.get("title") or "",
        "department": it.get("department"),
        "location": it.get("location"),
        "apply_url": it.get("apply_url"),
        "created_at": it.get("created_at") or it.get("updated_at") or dt.datetime.utcnow(),
        "updated_at": it.get("updated_at") or it.get("created_at") or dt.datetime.utcnow(),
        "status": it.get("status") or "OPEN",
        "role_family": it.get("role_family"),
    }
    if not out["role_family"]:
        out["role_family"] = _classify_role_family(out["title"], out["department"])
    return out

def _dispatch(kind: str, handle: str) -> List[Dict[str, Any]]:
    if kind == "greenhouse":
        return list(fetch_greenhouse(handle))
    if kind == "lever" and fetch_lever:
        return list(fetch_lever(handle))
    if kind == "ashby" and fetch_ashby:
        return list(fetch_ashby(handle, include_comp=True))
    if kind == "smartrecruiters" and fetch_smartrecruiters:
        return list(fetch_smartrecruiters(handle))
    return []

def run_ingest_now(db: SessionLocal) -> Dict[str, Any]:
    sources = db.execute(text("""
        SELECT id, company_id, kind, handle
        FROM sources
        WHERE enabled = true
        ORDER BY id
    """)).mappings().all()

    summary = {
        "ok": True,
        "sources": len(sources),
        "touched": 0,           # inserts + updates
        "by_kind": {"greenhouse": 0, "lever": 0, "ashby": 0, "smartrecruiters": 0},
        "errors": []
    }

    for s in sources:
        sid, cid, kind, handle = s["id"], s["company_id"], (s["kind"] or "").lower(), s["handle"]
        if not cid or not kind or not handle:
            summary["errors"].append({"source_id": sid, "error": "missing_company_or_handle"})
            continue

        try:
            items = _dispatch(kind, handle)
        except Exception as e:
            summary["errors"].append({"source_id": sid, "error": f"fetch_failed: {e}"})
            continue

        local_touch = 0
        for it in items:
            row = _normalize_item(cid, it)
            db.execute(UPSERT_SQL, row)
            local_touch += 1

        if local_touch:
            db.execute(text("UPDATE sources SET last_ok_at = now() WHERE id = :id"), {"id": sid})
            summary["by_kind"][kind] = summary["by_kind"].get(kind, 0) + local_touch
            summary["touched"] += local_touch

    db.commit()
    return summary
