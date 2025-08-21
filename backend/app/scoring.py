from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from .models import JobPosting, HiringScore

def compute_active_score_for_company(db: Session, company_id: int, role_family="SDE") -> tuple[int, dict]:
    now = datetime.utcnow()
    since = now - timedelta(days=28)
    q = db.query(JobPosting).filter(
        JobPosting.company_id==company_id,
        JobPosting.status=="OPEN",
        JobPosting.role_family==role_family
    )
    open_now = q.count()
    new_last_4w = q.filter(JobPosting.created_at >= since).count()
    score = 0
    score += min(40, new_last_4w * 10)   # each new post worth 10 up to 40
    score += min(40, open_now * 8)       # open roles up to 40
    details = {"open_now": open_now, "new_last_4w": new_last_4w}
    return max(0, min(100, score)), details

def write_score(db: Session, company_id: int, role_family="SDE"):
    score, details = compute_active_score_for_company(db, company_id, role_family)
    db.add(HiringScore(company_id=company_id, role_family=role_family, score=score, details_json=details))
    db.commit()
