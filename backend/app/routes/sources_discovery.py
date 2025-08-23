from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from ..db import SessionLocal
from ..services.ats_detector import fetch, detect_from_html, detect_from_domain

router = APIRouter(prefix="/admin/sources", tags=["sources-discovery"])

class DiscoverByUrlIn(BaseModel):
    url: str
    display_name: str | None = None

class DiscoverByDomainIn(BaseModel):
    domain: str   # 'example.com' or 'https://example.com'
    display_name: str | None = None

def upsert_source(db, kind: str, handle: str, display_name: str | None):
    db.execute(text("""
      INSERT INTO sources(kind, handle, display_name, enabled)
      VALUES (:k, :h, :n, true)
      ON CONFLICT (kind, handle)
      DO UPDATE SET display_name=COALESCE(EXCLUDED.display_name, sources.display_name),
                    enabled=true
    """), {"k": kind, "h": handle, "n": display_name})
    db.commit()

@router.post("/discover/url")
def discover_from_url(body: DiscoverByUrlIn):
    html = fetch(body.url)
    if not html:
        raise HTTPException(400, "could not fetch url or non-2xx")
    hit = detect_from_html(html)
    if not hit:
        raise HTTPException(404, "no ATS link found in page")
    kind, handle = hit
    db = SessionLocal()
    upsert_source(db, kind, handle, body.display_name)
    return {"ok": True, "kind": kind, "handle": handle}

@router.post("/discover/domain")
def discover_from_domain_route(body: DiscoverByDomainIn):
    hit = detect_from_domain(body.domain)
    if not hit:
        raise HTTPException(404, "no ATS detected from common paths")
    kind, handle, name = hit
    db = SessionLocal()
    upsert_source(db, kind, handle, body.display_name or name)
    return {"ok": True, "kind": kind, "handle": handle}
