import re, requests
from typing import Optional, Tuple

# Simple, polite fetch
def fetch(url: str, timeout=15) -> Optional[str]:
    try:
        r = requests.get(url, timeout=timeout, allow_redirects=True, headers={
            "User-Agent": "HiringRadarBot/1.0 (+contact: support@yourdomain)"
        })
        if r.status_code >= 200 and r.status_code < 400:
            return r.text or ""
    except Exception:
        pass
    return None

# Extract (kind, handle) from an HTML blob
def detect_from_html(html: str) -> Optional[Tuple[str, str]]:
    # Greenhouse: boards.greenhouse.io/<token>
    m = re.search(r'boards\.greenhouse\.io/([a-zA-Z0-9\-_/]+)', html)
    if m:
        token = m.group(1).split('/')[0]
        return ("greenhouse", token)

    # Lever: jobs.lever.co/<handle>
    m = re.search(r'jobs\.lever\.co/([a-zA-Z0-9\-\._/]+)', html)
    if m:
        handle = m.group(1).split('/')[0]
        return ("lever", handle)

    # Ashby: jobs.ashbyhq.com/<org> OR api.ashbyhq.com/...organizationSlug=<org>
    m = re.search(r'jobs\.ashbyhq\.com/([a-zA-Z0-9\-\._/]+)', html)
    if m:
        org = m.group(1).split('/')[0]
        return ("ashby", org)
    m = re.search(r'organizationSlug=([a-zA-Z0-9\-\._]+)', html)
    if m:
        return ("ashby", m.group(1))

    # SmartRecruiters: careers.smartrecruiters.com/<CompanyId>
    m = re.search(r'careers\.smartrecruiters\.com/([a-zA-Z0-9\-\._/]+)', html)
    if m:
        cid = m.group(1).split('/')[0]
        return ("smartrecruiters", cid)

    return None

# Try common careers paths on a domain
COMMON_PATHS = ["/careers", "/jobs", "/join", "/join-us", "/work-with-us"]

def detect_from_domain(domain: str) -> Optional[Tuple[str,str,str]]:
    """
    Returns (kind, handle, display_name) or None.
    domain: e.g. 'example.com'
    """
    base = domain if domain.startswith("http") else f"https://{domain}"
    tried = set()
    for path in [""] + COMMON_PATHS:
        url = base if not path else base.rstrip("/") + path
        if url in tried: continue
        tried.add(url)
        html = fetch(url)
        if not html: continue
        hit = detect_from_html(html)
        if hit:
            kind, handle = hit
            name_guess = domain.split("//")[-1]
            return (kind, handle, name_guess)
    return None
