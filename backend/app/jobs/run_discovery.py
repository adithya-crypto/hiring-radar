# backend/app/jobs/run_discovery.py
from typing import Dict
from sqlalchemy.orm import Session
from sqlalchemy import and_
from ..models import Company, Source

def _gh_endpoint(token: str) -> str:
    # Public Greenhouse job board API (no auth)
    return f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"

def _lever_endpoint(handle: str) -> str:
    # Public Lever postings API (v0; no auth)
    return f"https://api.lever.co/v0/postings/{handle}?mode=json"

def run_discovery_now(db: Session) -> Dict:
    """
    Populate `sources` for each Company based on {ats_kind, careers_url} if missing.
    No scraping and no hard-coded company seeds — purely derived from your DB.
    """
    created = 0
    skipped = 0
    updated = 0

    # Normalize and ensure 1 source row per (company, kind)
    companies = (
        db.query(Company)
          .filter(Company.ats_kind.isnot(None))
          .all()
    )

    for c in companies:
        kind = (c.ats_kind or "").strip().lower()
        token = (c.careers_url or "").strip()
        if not token:
            skipped += 1
            continue

        # Decide endpoint based on ATS kind
        endpoint_url = None
        if kind == "greenhouse":
            endpoint_url = _gh_endpoint(token)
        elif kind == "lever":
            endpoint_url = _lever_endpoint(token)
        elif kind in ("ashby", "smartrecruiters"):
            # Add later when you wire those in
            skipped += 1
            continue
        else:
            skipped += 1
            continue

        # Find existing source for this company/kind
        existing = (
            db.query(Source)
              .filter(and_(Source.company_id == c.id, Source.kind == kind))
              .first()
        )

        if existing:
            # If endpoint changed, update it
            if existing.endpoint_url != endpoint_url:
                existing.endpoint_url = endpoint_url
                updated += 1
        else:
            # Create a fresh source row
            s = Source(
                company_id=c.id,
                kind=kind,
                endpoint_url=endpoint_url,
                auth_kind=None,
                last_ok_at=None,
            )
            db.add(s)
            created += 1

    db.commit()
    return {
        "ok": True,
        "companies_seen": len(companies),
        "sources_created": created,
        "sources_updated": updated,
        "skipped": skipped,
    }
