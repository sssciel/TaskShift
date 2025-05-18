import scheduler.core as core
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from configs.logging import log
from fastapi import FastAPI
from scheduler.scheduler import scheduler

app = FastAPI(
    title="TaskShift Scheduler Control API",
    description="REST API for configure service in runtime.",
    version="1.0.0",
)


@app.on_event("startup")
def on_startup():
    log.info("Starting scheduler and API…")
    scheduler.start()


@app.on_event("shutdown")
def on_shutdown():
    log.info("Shutting down scheduler…")
    scheduler.shutdown(wait=False)
