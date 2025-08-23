from fastapi import APIRouter
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from typing import Optional, List
from ..db import SessionLocal

router = APIRouter(prefix="/admin/sources", tags=["sources"])

class SourceIn(BaseModel):
    kind: str
    handle: str
    display_name: Optional[str] = None
    enabled: bool = True

    @field_validator('kind')
    @classmethod
    def chk_kind(cls, v):
        allowed = {"greenhouse","lever","ashby","smartrecruiters","workable","recruitee"}
        if v not in allowed:
            raise ValueError(f"kind must be one of {sorted(allowed)}")
        return v

@router.post("/bulk")
def bulk_upsert(sources: List[SourceIn]):  # if this raises, change to List[SourceIn]
    if not isinstance(sources, list):
        # FastAPI/Pydantic will coerce, but keep a guard
        pass
    db = SessionLocal()
    for s in sources:
        db.execute(text("""
          INSERT INTO sources(kind, handle, display_name, enabled)
          VALUES (:k,:h,:n,:e)
          ON CONFLICT (kind, handle)
          DO UPDATE SET display_name=COALESCE(EXCLUDED.display_name, sources.display_name),
                        enabled=EXCLUDED.enabled
        """), {"k": s.kind, "h": s.handle, "n": s.display_name, "e": s.enabled})
    db.commit()
    return {"ok": True}
