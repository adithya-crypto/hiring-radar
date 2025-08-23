import requests
from typing import Iterator, Dict

def fetch_ashby(org_slug: str, include_comp: bool = False) -> Iterator[Dict]:
    """
    Pulls published jobs from Ashby public Job Posting API for a given organizationSlug.
    """
    url = f"https://api.ashbyhq.com/public/job-postings?organizationSlug={org_slug}"
    if include_comp:
        url += "&includeCompensation=true"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    for p in (r.json() or []):
        info = p.get("jobPosting") or {}
        yield {
            "source": "ashby",
            "source_job_id": str(info.get("id") or info.get("jobId")),
            "title": info.get("title"),
            "department": (info.get("department") or {}).get("name"),
            "location": (info.get("location") or {}).get("name"),
            "apply_url": info.get("jobUrl"),
            "created_at": info.get("updatedAt") or info.get("publishedAt"),
            "updated_at": info.get("updatedAt") or info.get("publishedAt"),
            "status": "OPEN",
        }
