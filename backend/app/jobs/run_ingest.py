from sqlalchemy import text
from ..db import SessionLocal
from ..routes.tasks import get_or_create_company_id, upsert_job_posting
from ..connectors.greenhouse import fetch_greenhouse
from ..connectors.lever import fetch_lever
from ..connectors.ashby import fetch_ashby
from ..connectors.smartrecruiters import fetch_smartrecruiters


def run_ingest_now(db=None):
    own = False
    if db is None:
        db = SessionLocal()
        own = True
    totals = {"greenhouse": 0, "lever": 0, "ashby": 0, "smartrecruiters": 0, "skipped": 0}
    total_upserts = 0
    try:
        sources = db.execute(text("""
            SELECT id, kind, handle, display_name
            FROM sources
            WHERE enabled = true
              AND kind IN ('greenhouse','lever','ashby','smartrecruiters')
              AND handle IS NOT NULL
              AND trim(handle) <> ''
        """)).mappings().all()

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

            try:
                company_id = get_or_create_company_id(db, kind, handle, s.get("display_name"))
            except Exception:
                totals["skipped"] += 1
                continue

            local = 0
            for it in items:
                it["company_id"] = company_id
                title = (it.get("title") or "").lower()
                it["role_family"] = "SDE" if any(k in title for k in ["software","engineer","developer","swe","sde"]) else None
                try:
                    upsert_job_posting(db, it)
                    local += 1
                except Exception:
                    pass

            if local:
                db.execute(text("UPDATE sources SET last_ok_at=now() WHERE id=:id"), {"id": s["id"]})
                totals[kind] += local
                total_upserts += local

        db.commit()
        return {"ok": True, "sources": len(sources), "upserts": total_upserts, "by_kind": totals}
    except Exception as e:
        db.rollback()
        return {"ok": False, "error": "ingest_failed", "detail": str(e)}
    finally:
        if own:
            db.close()
