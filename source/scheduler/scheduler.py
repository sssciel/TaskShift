import scheduler.core as core
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from configs.logging import log

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
