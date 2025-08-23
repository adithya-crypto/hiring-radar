# backend/app/routes/companies_live.py
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
        SUM(CASE WHEN jp.status='OPEN' THEN 1 ELSE 0 END)::int AS open_roles,
        GREATEST(
          COALESCE(MAX(jp.updated_at), to_timestamp(0)),
          COALESCE(MAX(s.last_ok_at), to_timestamp(0))
        ) AS last_update
      FROM companies c
      LEFT JOIN job_postings jp ON jp.company_id = c.id
      LEFT JOIN sources s ON s.company_id = c.id AND s.enabled = true
      GROUP BY c.id, c.name, c.ats_kind, c.ats_handle
      ORDER BY last_update DESC NULLS LAST
      LIMIT :limit OFFSET :offset
    """), {"limit": limit, "offset": offset}).mappings().all()
    return [dict(r) for r in rows]
