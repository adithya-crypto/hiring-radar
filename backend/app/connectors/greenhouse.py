import requests


def _gh_get(board_token: str):
    url = "https://boards-api.greenhouse.io/v1/boards/{}/jobs?content=true".format(board_token)
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json().get("jobs", [])


def fetch_greenhouse(board_token: str):
    """
    Normalized iterator over Greenhouse board postings.
    """
    try:
        jobs = _gh_get(board_token)
    except Exception:
        return []

    out = []
    for j in jobs:
        out.append({
            "source_job_id": str(j.get("id")),
            "title": j.get("title"),
            "department": ((j.get("departments") or [{}])[0] or {}).get("name"),
            "location": (j.get("location") or {}).get("name"),
            "apply_url": j.get("absolute_url"),
            "created_at": j.get("updated_at") or j.get("updated_on"),
            "updated_at": j.get("updated_at") or j.get("updated_on"),
            "status": "OPEN",
        })
    return out
