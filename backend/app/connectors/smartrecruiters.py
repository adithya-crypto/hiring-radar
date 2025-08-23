import requests


def _sr_list(company: str, limit: int = 200, offset: int = 0):
    url = "https://api.smartrecruiters.com/v1/companies/{}/postings".format(company)
    params = {"limit": limit, "offset": offset}
    r = requests.get(url, params=params, timeout=20)
    if r.status_code >= 400:
        return None
    return r.json()


def fetch_smartrecruiters(company: str):
    """
    Iterates v1 postings endpoint (public) and normalizes.
    """
    out = []
    offset = 0
    while True:
        data = _sr_list(company, 200, offset)
        if not data:
            break
        content = data.get("content") or []
        if not content:
            break
        for j in content:
            # Typical fields present
            jid = j.get("id") or (j.get("ref") or {}).get("id") or j.get("postingId")
            loc = j.get("location") or {}
            func = j.get("function") or {}
            out.append({
                "source_job_id": str(jid),
                "title": j.get("name"),
                "department": func.get("label"),
                "location": loc.get("city") or loc.get("region") or loc.get("country"),
                "apply_url": (j.get("applyUrl") or (j.get("jobAd") or {}).get("sections", {}).get("companyDescription", {}).get("ref")) or (j.get("ref") or {}).get("jobAd"),
                "created_at": j.get("releasedDate") or j.get("createdOn"),
                "updated_at": j.get("updatedOn") or j.get("releasedDate"),
                "status": "OPEN",
            })
        # Pagination: SmartRecruiters returns total found in 'totalFound' and 'offset'
        total = data.get("totalFound", 0)
        offset = data.get("offset", 0) + len(content)
        if offset >= total:
            break
    return out
