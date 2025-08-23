import re
import requests
from urllib.parse import urlparse

from bs4 import BeautifulSoup

UA = "Mozilla/5.0 (compatible; HiringRadarBot/1.0; +https://example.com/bot)"


def fetch_url(url: str, timeout: int = 15):
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout)
        if r.status_code >= 400:
            return None
        return r.text
    except Exception:
        return None


def parse_ats_from_url(url: str):
    """
    Returns (kind, handle) if URL is a direct ATS board.
    Supports Greenhouse/Lever/Ashby/SmartRecruiters.
    """
    try:
        u = url.strip()
        low = u.lower()

        # Greenhouse board
        m = re.search(r"boards\.greenhouse\.io/([^/?#]+)", low)
        if m:
            return "greenhouse", m.group(1)

        # Lever board
        m = re.search(r"jobs\.lever\.co/([^/?#]+)", low)
        if m:
            return "lever", m.group(1)

        # Ashby
        m = re.search(r"jobs\.ashbyhq\.com/([^/?#]+)", low)
        if m:
            return "ashby", m.group(1)

        # SmartRecruiters
        m = re.search(r"careers\.smartrecruiters\.com/([^/?#]+)", low)
        if m:
            return "smartrecruiters", m.group(1)

        return None, None
    except Exception:
        return None, None


def detect_from_html(html: str):
    """
    Scans HTML for links to supported ATS and extracts (kind, handle).
    """
    out = []
    try:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            k, h = parse_ats_from_url(a["href"])
            if k and h:
                item = {"kind": k, "handle": h}
                if item not in out:
                    out.append(item)
    except Exception:
        pass
    return out


def detect_from_domain(domain: str):
    """
    Visits common careers paths for the given domain and tries to detect ATS.
    """
    candidates = [
        f"https://{domain}/careers",
        f"https://{domain}/jobs",
        f"https://{domain}/company/careers",
        f"https://{domain}/about/careers",
        f"https://{domain}/join-us",
    ]
    seen = set()
    out = []
    for url in candidates:
        html = fetch_url(url)
        if not html:
            continue
        hits = detect_from_html(html)
        for it in hits:
            key = (it["kind"], it["handle"])
            if key not in seen:
                seen.add(key)
                out.append(it)
    return out
