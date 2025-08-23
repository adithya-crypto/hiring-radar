import re
from urllib.parse import urlparse

from fastapi import APIRouter, Body
from sqlalchemy import text

from ..db import SessionLocal
from ..services.ats_detector import fetch_url, parse_ats_from_url, detect_from_html, detect_from_domain

router = APIRouter(prefix="/admin/sources", tags=["admin"])


def upsert_source(db, kind: str, handle: str, display_name: str = None, enabled: bool = True):
    if not kind or not handle:
        return None
    row = db.execute(text("""
        INSERT INTO sources(kind, handle, display_name, enabled)
        VALUES (:k, :h, :d, :e)
        ON CONFLICT (kind, handle) DO UPDATE SET
           display_name = COALESCE(EXCLUDED.display_name, sources.display_name),
           enabled = EXCLUDED.enabled
        RETURNING id, kind, handle, display_name, enabled
    """), {"k": kind, "h": handle, "d": display_name, "e": enabled}).mappings().first()
    return dict(row) if row else None


@router.post("/discover/url")
def discover_from_url(payload: dict = Body(...)):
    """
    Body: { "url": "...", "display_name": "Stripe" }
    """
    url = (payload.get("url") or "").strip()
    display_name = payload.get("display_name")
    if not url:
        return {"error": "missing_url"}

    # 1) direct parse: ATS board URL
    kind, handle = parse_ats_from_url(url)
    if kind and handle:
        db = SessionLocal()
        try:
            row = upsert_source(db, kind, handle, display_name, True)
            db.commit()
            return {"ok": True, "source": row, "mode": "direct"}
        finally:
            db.close()

    # 2) fetch and detect within HTML
    html = fetch_url(url)
    if not html:
        return {"error": "fetch_failed"}

    detections = detect_from_html(html)
    if not detections:
        return {"detail": "no_ats_link_found_in_page"}

    db = SessionLocal()
    created = []
    try:
        for det in detections:
            row = upsert_source(db, det["kind"], det["handle"], display_name, True)
            if row:
                created.append(row)
        db.commit()
        return {"ok": True, "created": created, "mode": "html_detect"}
    finally:
        db.close()


@router.post("/discover/domain")
def discover_from_domain(payload: dict = Body(...)):
    """
    Body: { "domain": "stripe.com", "display_name": "Stripe" }
    Tries /careers, /jobs, /company/careers, etc.
    """
    domain = (payload.get("domain") or "").strip().lower()
    display_name = payload.get("display_name")
    if not domain:
        return {"error": "missing_domain"}

    detections = detect_from_domain(domain)
    if not detections:
        return {"detail": "no_ats_detected_for_domain"}

    db = SessionLocal()
    created = []
    try:
        for det in detections:
            row = upsert_source(db, det["kind"], det["handle"], display_name, True)
            if row:
                created.append(row)
        db.commit()
        return {"ok": True, "created": created, "mode": "domain_probe"}
    finally:
        db.close()


@router.post("/bulk")
def bulk_add(payload: dict = Body(...)):
    """
    Body: { "items": [ {"kind":"greenhouse","handle":"stripe","display_name":"Stripe"} ] }
    """
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        return {"error": "no_items"}
    db = SessionLocal()
    created = []
    try:
        for it in items:
            row = upsert_source(
                db,
                (it.get("kind") or "").strip().lower(),
                (it.get("handle") or "").strip().lower(),
                it.get("display_name"),
                bool(it.get("enabled", True)),
            )
            if row:
                created.append(row)
        db.commit()
        return {"ok": True, "created": created, "count": len(created)}
    finally:
        db.close()
