# backend/app/routes/tasks.py
from fastapi import APIRouter
from ..db import SessionLocal

from ..jobs.run_ingest import run_ingest_now

router = APIRouter(prefix="/tasks", tags=["tasks"])

@router.post("/ingest")
def run_ingest():
    db = SessionLocal()
    try:
        return run_ingest_now(db)
    finally:
        db.close()

@router.post("/forecast")
def run_forecast():
    # Stub — keep for compatibility
    return {"ok": True, "note": "forecast job stub"}
