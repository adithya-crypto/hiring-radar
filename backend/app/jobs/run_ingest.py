# app/jobs/run_ingest.py
from __future__ import annotations

import httpx
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func

from ..models import Company, JobPosting, JobRaw, Source


# ---------- Helpers ----------
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _role_family_from_title(title: Optional[str]) -> Optional[str]:
    if not title:
        return None
    t = title.lower()

    # Excludes first (avoid non-SWE tracks)
    excludes = [
        "sales engineer", "solutions engineer", "support engineer",
        "implementation", "customer success", "professional services",
        "field engineer"
    ]
    if any(x in t for x in excludes):
        return None

    # Broader SDE includes
    includes = [
        "software engineer", "software developer", "swe", "sde",
        "backend", "back-end", "frontend", "front-end", "full stack", "full-stack",
        "platform engineer", "infrastructure engineer", "distributed systems",
        "site reliability engineer", "sre",
        "devops", "developer productivity", "build & release", "build and release",
        "android", "ios", "mobile engineer",
        "data platform engineer", "ml platform engineer",
        "compiler engineer", "systems engineer", "kernel engineer"
    ]
    if any(k in t for k in includes):
        return "SDE"

    # Catch generic titles like “Engineer, Software …”
    if ("engineer" in t and "software" in t):
        return "SDE"

    return None


# ---------- Fetchers ----------
async def fetch_greenhouse(board: str) -> List[Dict]:
    """
    Returns a list of normalized postings:
      {source_job_id, title, department, location, apply_url, created_at, updated_at, role_family, status}
    """
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json().get("jobs", [])
    out: List[Dict] = []
    for j in data:
        title = j.get("title")
        dept = None
        depts = j.get("departments") or []
        if depts:
            d0 = depts[0] or {}
            dept = d0.get("name")
        loc = (j.get("location") or {}).get("name")
        apply_url = j.get("absolute_url")
        created = j.get("created_at") or j.get("updated_on") or j.get("updated_at")
        updated = j.get("updated_at") or j.get("updated_on") or created
        out.append(
            {
                "source_job_id": str(j.get("id")),
                "title": title,
                "department": dept,
                "location": loc,
                "apply_url": apply_url,
                "created_at": created,
                "updated_at": updated,
                "role_family": _role_family_from_title(title),
                "status": "OPEN",
            }
        )
    return out


async def fetch_lever(handle: str) -> List[Dict]:
    url = f"https://api.lever.co/v0/postings/{handle}?mode=json"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
    out: List[Dict] = []
    for j in data:
        cats = j.get("categories") or {}
        title = j.get("text")
        out.append(
            {
                "source_job_id": str(j.get("id")),
                "title": title,
                "department": cats.get("team"),
                "location": cats.get("location"),
                "apply_url": j.get("hostedUrl"),
                "created_at": j.get("createdAt"),
                "updated_at": j.get("updatedAt") or j.get("createdAt"),
                "role_family": _role_family_from_title(title),
                "status": "OPEN",
            }
        )
    return out


# ---------- Upsert ----------
def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    # Try to handle both ms epoch and ISO strings
    try:
        # Lever often uses ms epoch ints/strings
        iv = int(value)
        # value might be ms since epoch
        if iv > 10_000_000_000:
            iv = iv // 1000
        return datetime.fromtimestamp(iv, tz=timezone.utc)
    except Exception:
        pass
    # Fallback: ISO
    try:
        # httpx json gives strings like "2024-10-05T12:34:56Z" or "...+00:00"
        # Let fromisoformat handle +00:00; replace Z with +00:00
        v = value.replace("Z", "+00:00")
        return datetime.fromisoformat(v)
    except Exception:
        return None


def _upsert_postings(
    db: Session, company_id: int, source_id: Optional[int], normalized: List[Dict]
) -> int:
    """
    Upsert job_postings by (company_id, source_job_id). Marks them OPEN.
    Soft-close is handled elsewhere (e.g., by a cleanup job).
    """
    count = 0
    for p in normalized:
        sjid = p.get("source_job_id")
        if not sjid:
            continue
        row = (
            db.query(JobPosting)
            .filter(JobPosting.company_id == company_id, JobPosting.source_job_id == sjid)
            .first()
        )
        created_at = _parse_dt(p.get("created_at"))
        updated_at = _parse_dt(p.get("updated_at")) or created_at or _now_utc()
        if row:
            # Update
            row.title = p.get("title") or row.title
            row.department = p.get("department")
            row.location = p.get("location")
            row.apply_url = p.get("apply_url")
            row.role_family = p.get("role_family") or row.role_family
            row.status = "OPEN"
            row.updated_at = updated_at
        else:
            # Insert
            row = JobPosting(
                company_id=company_id,
                source_job_id=sjid,
                title=p.get("title") or "Untitled",
                department=p.get("department"),
                location=p.get("location"),
                apply_url=p.get("apply_url"),
                role_family=p.get("role_family"),
                status="OPEN",
                created_at=created_at or _now_utc(),
                updated_at=updated_at or _now_utc(),
            )
            db.add(row)
        count += 1

    # store a raw payload snapshot for debugging
    try:
        sample_payload = normalized[:50]  # keep small
        jr = JobRaw(
            company_id=company_id,
            source_id=source_id,
            payload_json={"sample": sample_payload, "fetched_at": _now_utc().isoformat()},
        )
        db.add(jr)
    except Exception:
        # non-fatal
        pass

    return count


# ---------- Orchestrator ----------
async def _ingest_company(db: Session, company: Company) -> Tuple[str, int]:
    """
    Ingest a single company based on ats_kind and careers_url (handle/board).
    Returns (company_name, n_upserted).
    """
    handle = (company.careers_url or "").strip()
    if not handle:
        return (company.name, 0)

    # try to find a Source row (optional; we don’t require it)
    source_row = (
        db.query(Source)
        .filter(Source.company_id == company.id)
        .order_by(Source.id.asc())
        .first()
    )
    source_id = source_row.id if source_row else None

    # fetch & normalize
    posts: List[Dict] = []
    if company.ats_kind == "greenhouse":
        posts = await fetch_greenhouse(handle)
    elif company.ats_kind == "lever":
        posts = await fetch_lever(handle)
    else:
        # Future: ashby/smartrecruiters/workday
        return (company.name, 0)

    # keep only postings we can confidently tag as SDE
    posts = [p for p in posts if p.get("role_family") == "SDE"]

    n = _upsert_postings(db, company.id, source_id, posts)
    # update source last_ok_at
    if source_row and n >= 0:
        source_row.last_ok_at = func.now()

    return (company.name, n)


def run_ingest_now(db: Session) -> Dict[str, int]:
    """
    Pulls postings for all known companies hourly-friendly.
    Returns {company_name: n_upserted, "_total": total}
    """
    import asyncio

    companies = db.query(Company).order_by(Company.id.asc()).all()
    results: Dict[str, int] = {}
    total = 0

    async def runner() -> None:
        nonlocal total
        for c in companies:
            try:
                name, n = await _ingest_company(db, c)
                results[name] = n
                total += n
                db.commit()
            except Exception as e:
                # don’t abort the whole ingest; record error count as 0
                results[c.name] = 0
                db.rollback()

    asyncio.run(runner())
    results["_total"] = total
    return results
