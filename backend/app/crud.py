from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, timezone


from sqlalchemy.orm import Session
from sqlalchemy import desc, or_, func, and_

from .models import Company, HiringScore, JobPosting


# ----- Companies -----
def list_companies(db: Session) -> List[Company]:
    return db.query(Company).order_by(Company.name).all()


def company_detail(db: Session, company_id: int) -> Optional[Company]:
    # SQLAlchemy 2.x: Session.get
    return db.get(Company, company_id)


# ----- Helpers -----
def _recent_apply_urls(
    db: Session,
    company_id: int,
    role_family: str = "SDE",
    limit: int = 3,
) -> List[str]:
    rows = (
        db.query(JobPosting.apply_url)
        .filter(
            JobPosting.company_id == company_id,
            JobPosting.role_family == role_family,
            JobPosting.status == "OPEN",
            JobPosting.apply_url.isnot(None),
        )
        .order_by(desc(JobPosting.created_at))
        .limit(limit)
        .all()
    )
    return [r[0] for r in rows if r and r[0]]


def list_company_postings(
    db: Session,
    company_id: int,
    role_family: str = "SDE",
    since_hours: Optional[int] = None,
    since_days: Optional[int] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    q = db.query(JobPosting).filter(
        JobPosting.company_id == company_id,
        JobPosting.status == "OPEN",
    )
    if role_family:
        q = q.filter(JobPosting.role_family == role_family)

    # cutoff logic
    cutoff: Optional[datetime] = None
    if since_hours is not None and since_hours > 0:
        cutoff = datetime.utcnow() - timedelta(hours=since_hours)
    elif since_days is not None and since_days > 0:
        cutoff = datetime.utcnow() - timedelta(days=since_days)

    if cutoff:
        q = q.filter(
            or_(
                JobPosting.updated_at >= cutoff,
                JobPosting.created_at >= cutoff,
            )
        )

    rows = (
        q.order_by(desc(JobPosting.updated_at), desc(JobPosting.created_at))
        .limit(limit)
        .all()
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r.id),
                "title": r.title,
                "location": r.location,
                "department": r.department,
                "apply_url": r.apply_url,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
                "role_family": r.role_family,
            }
        )
    return out


# ----- Scores -----
def list_scores(db: Session, role_family: str) -> List[Dict[str, Any]]:
    """
    Return the most recent HiringScore per company for the given role_family,
    enriched with evidence URLs (latest OPEN postings' apply links).
    """
    rows = (
        db.query(HiringScore)
        .filter(HiringScore.role_family == role_family)
        .order_by(desc(HiringScore.computed_at), desc(HiringScore.id))
        .all()
    )

    latest: Dict[int, HiringScore] = {}
    for s in rows:
        if s.company_id not in latest:
            latest[s.company_id] = s

    out: List[Dict[str, Any]] = []
    for s in latest.values():
        out.append(
            {
                "id": int(s.id),
                "company_id": int(s.company_id),
                "role_family": s.role_family,
                "computed_at": s.computed_at,  # FastAPI serializes datetime
                "score": int(s.score),
                "details_json": s.details_json or {},
                "evidence_urls": _recent_apply_urls(db, s.company_id, role_family),
            }
        )

    out.sort(key=lambda r: r["score"], reverse=True)
    return out


def list_active_top(
    db: Session,
    role_family: str = "SDE",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """
    Top N companies with at least 1 OPEN posting for the role family,
    ordered by latest score desc (ties by open_count desc, then name).
    Returns pure dicts (JSON-friendly).
    """
    # latest score timestamp per company
    latest_ts = (
        db.query(
            HiringScore.company_id,
            func.max(HiringScore.computed_at).label("max_ts"),
        )
        .filter(HiringScore.role_family == role_family)
        .group_by(HiringScore.company_id)
        .subquery()
    )

    # latest score rows
    hs = (
        db.query(
            HiringScore.company_id.label("company_id"),
            HiringScore.role_family.label("role_family"),
            HiringScore.score.label("score"),
            HiringScore.details_json.label("details_json"),
        )
        .join(
            latest_ts,
            and_(
                HiringScore.company_id == latest_ts.c.company_id,
                HiringScore.computed_at == latest_ts.c.max_ts,
            ),
        )
        .subquery()
    )

    # live open counts for the role family
    open_counts = (
        db.query(
            JobPosting.company_id.label("cid"),
            func.count().label("open_count"),
        )
        .filter(
            JobPosting.status == "OPEN",
            JobPosting.role_family == role_family,
        )
        .group_by(JobPosting.company_id)
        .subquery()
    )

    # final projection
    rows = (
        db.query(
            Company.id.label("company_id"),
            Company.name.label("company_name"),
            hs.c.role_family,
            hs.c.score,
            hs.c.details_json,
            func.coalesce(open_counts.c.open_count, 0).label("open_count"),
        )
        .join(hs, hs.c.company_id == Company.id)
        .join(open_counts, open_counts.c.cid == Company.id)
        .filter(open_counts.c.open_count > 0)
        .order_by(
            hs.c.score.desc(),
            func.coalesce(open_counts.c.open_count, 0).desc(),
            Company.name.asc(),
        )
        .limit(limit)
        .all()
    )

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "company_id": int(r.company_id),
                "company_name": r.company_name,
                "role_family": r.role_family,
                "score": int(r.score),
                "details_json": r.details_json,
                "open_count": int(r.open_count or 0),
            }
        )
    return out
def list_new_companies(db: Session, days: int = 7) -> List[Dict]:
    """
    Companies whose earliest job_postings.created_at is within the last `days`.
    Uses a timezone-aware UTC cutoff to avoid DB-specific interval quirks.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # earliest posting per company
    sub = (
        db.query(
            JobPosting.company_id,
            func.min(JobPosting.created_at).label("first_seen")
        )
        .group_by(JobPosting.company_id)
        .subquery()
    )

    rows = (
        db.query(Company.id, Company.name, sub.c.first_seen)
        .join(sub, sub.c.company_id == Company.id)
        .filter(sub.c.first_seen >= cutoff)
        .order_by(sub.c.first_seen.desc())
        .all()
    )
    return [
        {"company_id": r.id, "company_name": r.name, "first_seen": r.first_seen}
        for r in rows
    ]


def list_active_top_new(
    db: Session,
    role_family: str = "SDE",
    days: int = 7,
    limit: int = 50,
) -> List[Dict]:
    """Top companies (by latest HiringScore) that are 'new' in the last N days.

    'New' = company's first_seen = MIN(job_postings.created_at) within the last N days.
    Also require at least 1 current OPEN posting for the role family.
    """
    since = datetime.now(timezone.utc) - timedelta(days=days)

    # First seen per company
    first_seen_sq = (
        db.query(
            JobPosting.company_id.label("cid"),
            func.min(JobPosting.created_at).label("first_seen"),
        )
        .group_by(JobPosting.company_id)
        .subquery()
    )

    # Only companies with first_seen >= since
    new_companies_sq = (
        db.query(first_seen_sq.c.cid.label("company_id"))
        .filter(first_seen_sq.c.first_seen >= since)
        .subquery()
    )

    # Latest score per company for this role
    latest_ts = (
        db.query(
            HiringScore.company_id,
            func.max(HiringScore.computed_at).label("max_ts"),
        )
        .filter(HiringScore.role_family == role_family)
        .group_by(HiringScore.company_id)
        .subquery()
    )
    latest_scores = (
        db.query(HiringScore)
        .join(
            latest_ts,
            (HiringScore.company_id == latest_ts.c.company_id)
            & (HiringScore.computed_at == latest_ts.c.max_ts),
        )
        .subquery()
    )

    # Live open counts for the role family
    open_counts = (
        db.query(
            JobPosting.company_id.label("cid"),
            func.count().label("open_count"),
        )
        .filter(
            JobPosting.status == "OPEN",
            JobPosting.role_family == role_family,
        )
        .group_by(JobPosting.company_id)
        .subquery()
    )

    # Join everything; restrict to "new" companies
    rows = (
        db.query(
            latest_scores.c.company_id,
            latest_scores.c.role_family,
            latest_scores.c.score,
            latest_scores.c.details_json,
            Company.name.label("company_name"),
            open_counts.c.open_count,
        )
        .join(Company, Company.id == latest_scores.c.company_id)
        .join(open_counts, open_counts.c.cid == latest_scores.c.company_id)
        .join(new_companies_sq, new_companies_sq.c.company_id == latest_scores.c.company_id)
        .filter(open_counts.c.open_count > 0)
        .order_by(desc(latest_scores.c.score), Company.name.asc())
        .limit(limit)
        .all()
    )

    return [
        {
            "company_id": r.company_id,
            "company_name": r.company_name,
            "role_family": r.role_family,
            "score": r.score,
            "details_json": r.details_json,
            "open_count": int(r.open_count),
        }
        for r in rows
    ]