import requests


def _ashby_try_endpoints(handle: str):
    # Preferred public API
    urls = [
        # Newer public API
        "https://api.ashbyhq.com/posting-api/job-board/company/{}".format(handle),
        # Older external careers API
        "https://jobs.ashbyhq.com/api/external/careers/{}/jobs".format(handle),
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=25)
            if r.status_code >= 400:
                continue
            return url, r.json()
        except Exception:
            continue
    return None, None


def fetch_ashby(handle: str, include_comp: bool = True):
    url, data = _ashby_try_endpoints(handle)
    if not data:
        return []

    out = []
    # Newer API shape: { jobs: [ { id, title, ... , jobUrl, createdDate, updatedDate, compensation, ... } ] }
    jobs = []
    if isinstance(data, dict) and "jobs" in data and isinstance(data["jobs"], list):
        jobs = data["jobs"]
        for j in jobs:
            cats = j.get("categories") or {}
            loc = j.get("location") or {}
            out.append({
                "source_job_id": str(j.get("id") or j.get("jobId") or j.get("slug") or j.get("title")),
                "title": j.get("title"),
                "department": cats.get("team") or j.get("team"),
                "location": loc.get("name") or j.get("locationName"),
                "apply_url": j.get("jobUrl") or j.get("url") or j.get("applyUrl"),
                "created_at": j.get("createdDate") or j.get("createdAt"),
                "updated_at": j.get("updatedDate") or j.get("updatedAt"),
                "status": "OPEN",
            })
        return out

    # Older external careers API: array of jobs
    if isinstance(data, list):
        for j in data:
            cats = j.get("job") or {}
            loc = j.get("location") or {}
            out.append({
                "source_job_id": str(j.get("id") or cats.get("id") or j.get("slug") or j.get("title")),
                "title": j.get("title") or cats.get("title"),
                "department": (j.get("department") or {}).get("name"),
                "location": loc.get("name") or j.get("locationName"),
                "apply_url": j.get("jobUrl") or j.get("applyUrl") or j.get("url"),
                "created_at": j.get("createdDate") or j.get("createdAt"),
                "updated_at": j.get("updatedDate") or j.get("updatedAt"),
                "status": "OPEN",
            })
        return out

    return out
