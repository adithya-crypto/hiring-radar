import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(5))
def fetch_lever_jobs(company_handle: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{company_handle}?mode=json"
    with httpx.Client(timeout=30) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.json()
