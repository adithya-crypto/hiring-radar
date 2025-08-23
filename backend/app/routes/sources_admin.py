from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from typing import Optional, List
from ..db import SessionLocal

router = APIRouter(prefix="/admin/sources", tags=["sources"])

ALLOWED_KINDS = {"greenhouse","lever","ashby","smartrecruiters","workable","recruitee"}

class SourceIn(BaseModel):
    kind: str
    handle: str
    display_name: Optional[str] = None
    enabled: bool = True

@router.post("/bulk")
def bulk_upsert(sources: List[SourceIn]):
    db = SessionLocal()
    for s in sources:
        if s.kind not in ALLOWED_KINDS:
            raise HTTPException(status_code=400, detail=f"invalid kind: {s.kind}")
        db.execute(text("""
          INSERT INTO sources(kind, handle, display_name, enabled)
          VALUES (:k, :h, :n, :e)
          ON CONFLICT (kind, handle)
          DO UPDATE SET display_name=COALESCE(EXCLUDED.display_name, sources.display_name),
                        enabled=EXCLUDED.enabled
        """), {"k": s.kind, "h": s.handle, "n": s.display_name, "e": s.enabled})
    db.commit()
    return {"ok": True}
