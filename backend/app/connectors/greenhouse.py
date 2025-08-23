import requests
from typing import Iterator, Dict

def fetch_greenhouse(board_token: str) -> Iterator[Dict]:
    """
    Pulls published jobs from Greenhouse Job Board API for a given board token.
    Normalizes a minimal posting record.
    """
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json().get("jobs", []) or []
    for j in data:
        dept = (j.get("departments") or [{}])[0].get("name")
        loc  = (j.get("location") or {}).get("name")
        yield {
            "source": "greenhouse",
            "source_job_id": str(j.get("id")),
            "title": j.get("title"),
            "department": dept,
            "location": loc,
            "apply_url": j.get("absolute_url"),
            "created_at": j.get("updated_at") or j.get("updated_on"),
            "updated_at": j.get("updated_at") or j.get("updated_on"),
            "status": "OPEN",
        }
