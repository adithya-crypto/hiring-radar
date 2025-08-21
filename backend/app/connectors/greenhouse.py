import httpx
from tenacity import retry, wait_exponential, stop_after_attempt

@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(5))
def fetch_greenhouse_jobs(board_token: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true"
    with httpx.Client(timeout=30) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.json().get("jobs", [])
