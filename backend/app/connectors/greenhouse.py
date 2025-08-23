# backend/app/connectors/greenhouse.py
import requests
from datetime import datetime, timezone

def _parse_dt(s: str | None):
    if not s:
        return None
    try:
        # GH timestamps are ISO8601; let requests/json give us strings; parse conservatively
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def fetch_greenhouse(board_token: str):
    """
    Yields normalized postings dicts for the given Greenhouse board.
    """
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json() or {}
    jobs = data.get("jobs") or []
    for j in jobs:
        dep = None
        # departments may be a list of objects (name)
        depts = j.get("departments") or []
        if isinstance(depts, list) and depts:
            dep = (depts[0] or {}).get("name")

        loc = None
        if isinstance(j.get("location"), dict):
            loc = j["location"].get("name")
        elif isinstance(j.get("location"), str):
            loc = j.get("location")

        created = _parse_dt(j.get("updated_at") or j.get("updated_on")) or datetime.now(timezone.utc)
        updated = _parse_dt(j.get("updated_at") or j.get("updated_on")) or created

        yield {
            "source_job_id": j.get("id"),
            "title": j.get("title"),
            "department": dep,
            "location": loc,
            "apply_url": j.get("absolute_url"),
            "created_at": created,
            "updated_at": updated,
            "status": "OPEN",
        }
