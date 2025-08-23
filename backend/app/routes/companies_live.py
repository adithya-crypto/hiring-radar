from fastapi import APIRouter, Query
from sqlalchemy import text
from ..db import SessionLocal

router = APIRouter(prefix="/live", tags=["live"])

@router.get("/companies")
def live_companies(limit: int = Query(50, ge=1, le=1000), offset: int = 0):
    db = SessionLocal()
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
