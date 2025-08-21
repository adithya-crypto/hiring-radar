# backend/scripts/seed_companies.py
"""
Seed many companies into Hiring Radar.
- Tries GREENHOUSE first with a board token, then LEVER with a handle.
- If the endpoint returns JSON and contains postings, we create the company with that ATS.
- Safe to re-run; /companies endpoint is idempotent (returns existing id).

Usage (from backend venv):
  python scripts/seed_companies.py
"""

import os, time, json
from typing import Optional, Tuple
import requests

API = os.getenv("API", "http://localhost:8000")

# --- Candidate companies & tokens/handles ---
# NOTE: token/handle is usually lowercase brand (e.g., stripe, uber).
# This list is intentionally large; any that don't match will be auto-skipped.
CANDIDATES = [
    # Greenhouse-heavy crowd (token is usually the board slug)
    "airbnb","figma","stripe","cloudflare","coinbase","snowflake","doordash","robinhood",
    "notion","asana","rippling","brex","datadog","pinterest","peloton","canva","zoom",
    "atlassian","shopify","okta","twilio","plaid","instacart","uber","lyft","airtable",
    "squareup","affirm","opendoor","turo","segment","mercury","linear","databricks",
    "sentry","zapier","loom","pilot","benchling","confluent","elastic","mongodb",
    "cloudera","digitalocean","fastly","github","gitlab","snyk","circleci","launchdarkly",
    "mural","monday","miro","trello","pagerduty","newrelic","evernote","intercom",
    "wise","transferwise","revolut","monzo","nubank","klarna","brevo","typeform",
    "reddit","hashicorp","discord","roblox","nvidia","openai","anthropic","datastax",
    "snowplow","segment","bolt","postman","retool","vercel","netlify","zapier",
]

TIMEOUT = 12

def probe_greenhouse(token: str) -> Tuple[bool, int]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            return False, r.status_code
        data = r.json()
        jobs = data.get("jobs", [])
        return (len(jobs) > 0, 200)
    except Exception:
        return False, 0

def probe_lever(handle: str) -> Tuple[bool, int]:
    url = f"https://api.lever.co/v0/postings/{handle}?mode=json"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            return False, r.status_code
        data = r.json()
        return (isinstance(data, list) and len(data) > 0, 200)
    except Exception:
        return False, 0

def title_case(name: str) -> str:
    # simple Title Case without mangling known brands
    parts = name.replace('-', ' ').replace('_',' ').split()
    return ' '.join(p.capitalize() if p.isalpha() else p for p in parts)

def add_company(name: str, careers_url: str, ats_kind: str) -> dict:
    payload = {"name": name, "careers_url": careers_url, "ats_kind": ats_kind}
    r = requests.post(f"{API}/companies", json=payload, timeout=TIMEOUT)
    try:
        return r.json()
    except Exception:
        return {"status": r.status_code, "text": r.text}

def main():
    created, skipped = 0, 0
    results = []
    for token in CANDIDATES:
        nice = title_case(token)
        # Try Greenhouse first
        gh_ok, _ = probe_greenhouse(token)
        if gh_ok:
            res = add_company(nice, token, "greenhouse")
            results.append((nice, "greenhouse", res))
            print(f"[ADDED] {nice:<20} greenhouse  -> {res}")
            created += 1
            time.sleep(0.2)
            continue
        # Try Lever
        lv_ok, _ = probe_lever(token)
        if lv_ok:
            res = add_company(nice, token, "lever")
            results.append((nice, "lever", res))
            print(f"[ADDED] {nice:<20} lever       -> {res}")
            created += 1
            time.sleep(0.2)
            continue

        print(f"[SKIP ] {nice:<20} no GH/Lever match")
        skipped += 1

    print("\nSummary:", {"created": created, "skipped": skipped})

    # Optional: run ingest & forecast automatically
    try:
        ing = requests.post(f"{API}/tasks/ingest", timeout=60).json()
        fct = requests.post(f"{API}/tasks/forecast", timeout=60).json()
        print("\nIngest:", json.dumps(ing, indent=2))
        print("Forecast:", json.dumps(fct, indent=2))
    except Exception as e:
        print("Skipped auto ingest/forecast:", e)

if __name__ == "__main__":
    main()
