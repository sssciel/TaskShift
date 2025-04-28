from fastapi import FastAPI, HTTPException
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from configs.logging import log
import scheduler.core as core
from pydantic import BaseModel, conint

app = FastAPI(
    title="TaskShift Scheduler Control API",
    description="REST API to pause/resume the task_scheduler policy",
    version="1.0.0",
)

scheduler = BackgroundScheduler()

scheduler.add_job(
    core.compute_forecasts,
    id="compute_forecasts",
    trigger=CronTrigger(day_of_week="fri", hour=23, minute=30),
    replace_existing=True,
)
scheduler.add_job(
    core.task_scheduler,
    id="task_scheduler",
    trigger=CronTrigger(day_of_week="sat,sun", minute="*/10"),
    replace_existing=True,
)

@app.on_event("startup")
def on_startup():
    log.info("Starting scheduler and API…")
    scheduler.start() 

@app.on_event("shutdown")
def on_shutdown():
    log.info("Shutting down scheduler…")
    scheduler.shutdown(wait=False)

@app.post("/scheduler/enable", summary="Resume shifter job")
def enable_scheduler():
    job = scheduler.get_job("task_scheduler")
    if not job:
        raise HTTPException(status_code=404, detail="Shifter job not found")

    scheduler.resume_job("task_scheduler")
    log.info("Shifter resumed via API")
    return {"status": "True"}

@app.post("/scheduler/disable", summary="Pause Shifter job")
def disable_scheduler():
    job = scheduler.get_job("task_scheduler")
    if not job:
        raise HTTPException(status_code=404, detail="Shifter job not found")

    scheduler.pause_job("task_scheduler")
    log.info("Shifter paused via API")
    return {"status": "True"}

@app.get("/scheduler/status", summary="Get status of Shifter job")
def status_scheduler():
    job = scheduler.get_job("task_scheduler")
    if not job:
        return {"exists": False}

    paused = job.next_run_time is None
    return {
        "exists": True,
        "paused": paused,
        "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
    }

class ResourceOverride(BaseModel):
    cpu: conint(ge=0, le=100) | None = None
    gpu: conint(ge=0, le=100) | None = None

@app.get("/scheduler/resources", summary="Get current CPU/GPU overrides")
def get_resources():
    return {
        "override_cpu": core.OVERRIDE_CPU,
        "override_gpu": core.OVERRIDE_GPU,
    }

@app.put("/scheduler/resources", summary="Set CPU/GPU overrides")
def set_resources(data: ResourceOverride):
    core.OVERRIDE_CPU = data.cpu
    core.OVERRIDE_GPU = data.gpu
    log.info(f"Resource overrides updated: cpu={data.cpu}, gpu={data.gpu}")
    return {
        "override_cpu": core.OVERRIDE_CPU,
        "override_gpu": core.OVERRIDE_GPU,
    }

@app.delete("/scheduler/resources", summary="Reset CPU/GPU overrides")
def reset_resources():
    core.OVERRIDE_CPU = None
    core.OVERRIDE_GPU = None
    log.info("Resource overrides reset to defaults")
    return {"override_cpu": None, "override_gpu": None}