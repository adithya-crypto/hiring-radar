import requests


def _lever_get(company_handle: str):
    url = "https://api.lever.co/v0/postings/{}?mode=json".format(company_handle)
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def fetch_lever(company_handle: str):
    try:
        data = _lever_get(company_handle)
    except Exception:
        return []

    out = []
    for j in data:
        cats = j.get("categories") or {}
        out.append({
            "source_job_id": str(j.get("id")),
            "title": j.get("text"),
            "department": cats.get("team"),
            "location": cats.get("location"),
            "apply_url": j.get("hostedUrl"),
            "created_at": j.get("createdAt"),
            "updated_at": j.get("updatedAt"),
            "status": "OPEN",
        })
    return out
