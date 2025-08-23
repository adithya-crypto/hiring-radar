import requests
from typing import Iterator, Dict

SR_API = "https://api.smartrecruiters.com"

def fetch_smartrecruiters(company_id: str) -> Iterator[Dict]:
    params = {"companyId": company_id, "limit": 200}
    r = requests.get(f"{SR_API}/postings", params=params, timeout=30); r.raise_for_status()
    for p in (r.json().get("content") or []):
        pid = p.get("id")
        if not pid:
            continue
        d = requests.get(f"{SR_API}/postings/{pid}", timeout=30); d.raise_for_status()
        j = d.json()
        yield {
            "source": "smartrecruiters",
            "source_job_id": j.get("id"),
            "title": j.get("name"),
            "department": (j.get("department") or {}).get("label"),
            "location": (j.get("location") or {}).get("city"),
            "apply_url": j.get("applyUrl"),
            "created_at": j.get("createdOn"),
            "updated_at": j.get("updatedOn"),
            "status": "OPEN",
        }
