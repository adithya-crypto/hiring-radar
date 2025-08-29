# backend/app/config.py
import os
from typing import List
from dotenv import load_dotenv

# Load .env that sits in backend/
BASE_DIR = os.path.dirname(__file__)
ENV_PATH = os.path.join(os.path.dirname(BASE_DIR), ".env")
load_dotenv(ENV_PATH)


def _csv(name: str, default: str) -> List[str]:
    raw = os.environ.get(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


class Settings:
    # REQUIRED
    DATABASE_URL: str = os.environ.get("DATABASE_URL", "")

    # CORS
    # Comma-separated list. Defaults to localhost:3000 for the Next.js app.
    ALLOWED_ORIGINS: List[str] = _csv("ALLOWED_ORIGINS", "http://localhost:3000", "https://hiring-radar.vercel.app")

    # You can add other toggles here if you want:
    DEBUG: bool = os.environ.get("DEBUG", "false").lower() in ("1", "true", "yes")
    SCHEDULER_ENABLED: bool = os.environ.get("SCHEDULER_ENABLED", "true").lower() in (
        "1",
        "true",
        "yes",
    )


settings = Settings()
