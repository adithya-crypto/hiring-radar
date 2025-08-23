import requests
from typing import Iterator, Dict

def fetch_lever(company_handle: str) -> Iterator[Dict]:
    url = f"https://api.lever.co/v0/postings/{company_handle}?mode=json"
    r = requests.get(url, timeout=30); r.raise_for_status()
    for j in (r.json() or []):
        cats = j.get("categories") or {}
        yield {
            "source": "lever",
            "source_job_id": j.get("id"),
            "title": j.get("text"),
            "department": cats.get("team"),
            "location": cats.get("location"),
            "apply_url": j.get("hostedUrl"),
            "created_at": j.get("createdAt"),
            "updated_at": j.get("updatedAt"),
            "status": "OPEN",
        }
