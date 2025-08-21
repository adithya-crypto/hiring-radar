# backend/app/jobs/run_forecast.py

from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import func
from ..models import Company, JobPosting, HiringScore, Forecast, Signal
from ..forecast import forecast_month

def _score_active(db: Session, company_id: int, role_family: str = "SDE") -> dict:
    now = datetime.utcnow()
    d28 = now - timedelta(days=28)
    d56 = now - timedelta(days=56)

    open_now = db.query(JobPosting).filter_by(
        company_id=company_id, role_family=role_family, status="OPEN"
    ).count()

    new_28 = db.query(JobPosting).filter(
        JobPosting.company_id == company_id,
        JobPosting.role_family == role_family,
        JobPosting.created_at >= d28,
        JobPosting.status == "OPEN",
    ).count()

    new_56 = db.query(JobPosting).filter(
        JobPosting.company_id == company_id,
        JobPosting.role_family == role_family,
        JobPosting.created_at >= d56,
        JobPosting.status == "OPEN",
    ).count()

    # simple trend proxy using last 56d split into two 28d windows
    prev_28 = max(0, new_56 - new_28)
    trend_ratio = (new_28 / prev_28) if prev_28 > 0 else 2.0  # favor increased recent activity, cap later
    trend_norm = max(0.0, min(1.0, (trend_ratio - 0.5) / 1.5))  # ~map [0.5..2.0] -> [0..1]

    # feature caps to keep score stable
    f_postings = min(1.0, new_28 / 50.0)   # 50 new in 28d -> full credit
    f_open     = min(1.0, open_now / 200.0)

    now = datetime.utcnow()
    recent_layoff = db.query(Signal).filter(
        Signal.company_id==company_id,
        Signal.kind=="layoff",
        Signal.happened_at >= now - timedelta(days=120)
    ).count() > 0

    hn_presence = db.query(Signal).filter(
        Signal.company_id==company_id,
        Signal.kind=="hn_whos_hiring",
        Signal.happened_at >= now - timedelta(days=45)
    ).count() > 0

    f_hn = 1.0 if hn_presence else 0.0
    penalty = 0.25 if recent_layoff else 0.0

    score_0to1 = 0.40 * f_postings + 0.35 * f_open + 0.15 * trend_norm + 0.10 * f_hn
    score_0to1 = max(0.0, score_0to1 - penalty)
    score = round(score_0to1 * 100)

def run_forecast_now(db: Session) -> int:
    companies = db.query(Company).order_by(Company.name).all()
    saved = 0
    for c in companies:
        try:
            f = forecast_month(db, c.id)  # returns dict with likely_month, confidence, etc.
            # store into forecast table (or update your HiringScore if that’s the design)
            row = Forecast(
                company_id=c.id,
                role_family="SDE",
                prob_next_8w=f.get("prob_next_8w", 0.0),
                likely_month=f.get("likely_month_readable") or f.get("likely_month"),
                method=f.get("method", "rules:ema4+momentum"),
                ci_low=f.get("ci_low"),
                ci_high=f.get("ci_high"),
                features_json=f.get("features_json") or f,
            )
            db.add(row)
            saved += 1
        except Exception as e:
            print(f"[forecast] {c.name} failed: {e}")
            continue
    db.commit()
    return saved