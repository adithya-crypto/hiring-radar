# backend/app/forecast.py

import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime, timedelta, date
from calendar import month_name
from .models import JobPosting


def build_weekly_series(db: Session, company_id: int, role_family: str = "SDE") -> pd.Series:
    """
    Build a 26-week time series of the OPEN postings snapshot per week.
    We approximate weekly 'openings' by counting postings with status OPEN
    whose created_at is before the end of each week.
    """
    weeks = []
    start = datetime.utcnow() - timedelta(weeks=26)
    for w in range(27):
        ws = start + timedelta(weeks=w)
        we = ws + timedelta(weeks=1)
        sde_openings = db.query(JobPosting).filter(
            JobPosting.company_id == company_id,
            JobPosting.role_family == role_family,
            JobPosting.status == "OPEN",
            JobPosting.created_at < we,
        ).count()
        weeks.append((ws.date(), sde_openings))
    # Ensure index is sorted by date
    s = pd.Series({k: v for k, v in weeks}).sort_index()
    return s


def forecast_month(db: Session, company_id: int, role_family: str = "SDE") -> dict:
    """
    Rule-based + smoothed momentum:
    - Exponential smoothing (span=4) on last 26 weeks of 'open' counts
    - Momentum = delta over last ~4 weeks
    - prob_next_8w scaled by recent level + positive momentum
    - likely_month chosen 6–10 weeks ahead depending on probability
    """
    s = build_weekly_series(db, company_id, role_family)

    # Fallback if we have no history at all
    if s.sum() == 0 or len(s) < 4:
        likely_dt = date.today() + timedelta(weeks=10)
        return {
            "prob_next_8w": 0.30,
            "likely_month": month_name[likely_dt.month],
            "method": "rules:no-history-fallback",
            "features_json": {"history_weeks": int(len(s)), "sum": int(s.sum())},
        }

    # Smooth with an exponential moving average for stability
    smoothed = s.ewm(span=4, adjust=False).mean()

    # Momentum: average weekly delta over last 4 weeks (or as many as we have)
    tail = min(4, len(smoothed) - 1)
    if tail <= 0:
        momentum = 0.0
    else:
        momentum = (float(smoothed.iloc[-1]) - float(smoothed.iloc[-1 - tail])) / tail

    level_now = float(smoothed.iloc[-1])

    # Produce a simple 12-week ahead linear projection using momentum
    future = [max(0.0, level_now + (i + 1) * momentum) for i in range(12)]
    weeks_ahead = [datetime.utcnow() + timedelta(weeks=i + 1) for i in range(12)]

    # “Probability of ramp in next ~8 weeks” — scale with level & positive momentum
    # Cap for stability; keep within [0.1, 0.95]
    pos_mom = max(0.0, momentum)
    prob = 0.25 + min(0.5, 0.04 * level_now + 0.06 * pos_mom)
    prob = round(max(0.10, min(0.95, prob)), 2)

    # Likely month: if probability is stronger, assume earlier ramp (6 weeks), else later (10)
    likely_weeks = 6 if prob >= 0.5 else 10
    likely_dt = datetime.utcnow().date() + timedelta(weeks=likely_weeks)
    likely = month_name[likely_dt.month]

    # Optionally pick the peak week in the 12-week projection to report as context
    best_idx = int(pd.Series(future).idxmax())
    best_month = month_name[weeks_ahead[best_idx].month]

    return {
        "prob_next_8w": prob,
        "likely_month": likely,
        "method": "rules:ema4+momentum",
        "features_json": {
            "role_family": role_family,
            "level_now": round(level_now, 2),
            "momentum_per_week": round(momentum, 3),
            "future_peak_month": best_month,
            "history_weeks": int(len(s)),
        },
    }
