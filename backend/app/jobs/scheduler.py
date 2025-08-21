from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from sqlalchemy.orm import Session
from ..db import SessionLocal
from .run_ingest import run_ingest_now
from .run_forecast import run_forecast_now
from apscheduler.triggers.cron import CronTrigger

def attach_scheduler(app):
    from apscheduler.schedulers.background import BackgroundScheduler
    from .run_discovery import run_discovery_now
    from .run_ingest import run_ingest_now
    from .run_forecast import run_forecast_now
    from ..db import SessionLocal

    scheduler = BackgroundScheduler(timezone="UTC")

    def job():
        db = SessionLocal()
        try:
            run_discovery_now(db)
            run_ingest_now(db)
            run_forecast_now(db)
        finally:
            db.close()

    # every hour at minute 7 (staggered vs. other jobs)
    scheduler.add_job(job, CronTrigger(minute="7"))
    scheduler.start()
    app.state.scheduler = scheduler