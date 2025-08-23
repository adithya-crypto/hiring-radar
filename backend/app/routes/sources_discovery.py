from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from typing import Optional
from urllib.parse import urlparse, parse_qs
from ..db import SessionLocal
from ..services.ats_detector import fetch, detect_from_html, detect_from_domain

router = APIRouter(prefix="/admin/sources", tags=["sources-discovery"])

class DiscoverByUrlIn(BaseModel):
    url: str
    display_name: Optional[str] = None

class DiscoverByDomainIn(BaseModel):
    domain: str
    display_name: Optional[str] = None

def upsert_source(db, kind: str, handle: str, display_name: Optional[str]):
    db.execute(text("""
      INSERT INTO sources(kind, handle, display_name, enabled)
      VALUES (:k, :h, :n, true)
      ON CONFLICT (kind, handle)
      DO UPDATE SET display_name=COALESCE(EXCLUDED.display_name, sources.display_name),
                    enabled=true
    """), {"k": kind, "h": handle, "n": display_name})
    db.commit()

def parse_ats_from_url(u: str):
    try:
        p = urlparse(u)
        host = (p.netloc or "").lower()
        path = (p.path or "/").strip("/")

        if host.endswith("boards.greenhouse.io"):
            handle = path.split("/")[0] if path else None
            if handle: return ("greenhouse", handle)

        if host.endswith("jobs.lever.co"):
            handle = path.split("/")[0] if path else None
            if handle: return ("lever", handle)

        if host.endswith("jobs.ashbyhq.com"):
            handle = path.split("/")[0] if path else None
            if handle: return ("ashby", handle)

        if host.endswith("api.ashbyhq.com"):
            qs = parse_qs(p.query or "")
            org = qs.get("organizationSlug", [None])[0]
            if org: return ("ashby", org)

        if host.endswith("careers.smartrecruiters.com"):
            handle = path.split("/")[0] if path else None
            if handle: return ("smartrecruiters", handle)
    except Exception:
        pass
    return None

@router.post("/discover/url")
def discover_from_url(body: DiscoverByUrlIn):
    direct = parse_ats_from_url(body.url)
    if direct:
        kind, handle = direct
        db = SessionLocal()
        upsert_source(db, kind, handle, body.display_name)
        return {"ok": True, "kind": kind, "handle": handle, "via": "url_parse"}

    html = fetch(body.url)
    if not html:
        raise HTTPException(400, "could_not_fetch_url_or_non_2xx")
    hit = detect_from_html(html)
    if not hit:
        raise HTTPException(404, "no_ats_link_found_in_page")
    kind, handle = hit
    db = SessionLocal()
    upsert_source(db, kind, handle, body.display_name)
    return {"ok": True, "kind": kind, "handle": handle, "via": "html_scan"}

@router.post("/discover/domain")
def discover_from_domain_route(body: DiscoverByDomainIn):
    hit = detect_from_domain(body.domain)
    if not hit:
        raise HTTPException(404, "no_ats_detected_from_common_paths")
    kind, handle, name = hit
    db = SessionLocal()
    upsert_source(db, kind, handle, body.display_name or name)
    return {"ok": True, "kind": kind, "handle": handle}
