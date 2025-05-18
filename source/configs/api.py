import scheduler.core as core
from configs.logging import log
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, conint
from scheduler.scheduler import scheduler

router = APIRouter(prefix="/scheduler")


@router.post("/scheduler/enable", summary="Resume shifter job")
def enable_scheduler():
    job = scheduler.get_job("task_scheduler")
    if not job:
        raise HTTPException(status_code=404, detail="Shifter job not found")

    scheduler.resume_job("task_scheduler")
    log.info("Shifter resumed via API")
    return {"status": "True"}


@router.post("/scheduler/disable", summary="Pause Shifter job")
def disable_scheduler():
    job = scheduler.get_job("task_scheduler")
    if not job:
        raise HTTPException(status_code=404, detail="Shifter job not found")

    scheduler.pause_job("task_scheduler")
    log.info("Shifter paused via API")
    return {"status": "True"}


@router.get("/scheduler/status", summary="Get status of Shifter job")
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


@router.get("/scheduler/resources", summary="Get current CPU/GPU overrides")
def get_resources():
    return {
        "override_cpu": core.OVERRIDE_CPU,
        "override_gpu": core.OVERRIDE_GPU,
    }


@router.put("/scheduler/resources", summary="Set CPU/GPU overrides")
def set_resources(data: ResourceOverride):
    core.OVERRIDE_CPU = data.cpu
    core.OVERRIDE_GPU = data.gpu
    log.info(f"Resource overrides updated: cpu={data.cpu}, gpu={data.gpu}")
    return {
        "override_cpu": core.OVERRIDE_CPU,
        "override_gpu": core.OVERRIDE_GPU,
    }


@router.delete("/scheduler/resources", summary="Reset CPU/GPU overrides")
def reset_resources():
    core.OVERRIDE_CPU = None
    core.OVERRIDE_GPU = None
    log.info("Resource overrides reset to defaults")
    return {"override_cpu": None, "override_gpu": None}
