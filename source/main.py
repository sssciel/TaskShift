import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from configs.logging import log
from data_agregator.core import get_full_data_db, save_data_db
from scheduler.core import compute_forecasts, task_scheduler
from scheduler.integration import (get_sessionid_pending_tasks_test,
                                   get_sessionid_running_tasks_test, run_task)
from scheduler.utils import UniqueQueue


def main():
    scheduler = BlockingScheduler()

    def shutdown(signum, frame):
        scheduler.shutdown(wait=False)
        sys.exit(0)

    scheduler.add_job(
        compute_forecasts,
        trigger=CronTrigger(day_of_week="fri", hour=23, minute=30),
        replace_existing=True,
    )

    scheduler.add_job(
        task_scheduler,
        trigger=CronTrigger(day_of_week="sat,sun", minute="*/10"),
        replace_existing=True,
    )

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        log.info("TaskShift is running.")
        compute_forecasts()
        task_scheduler()
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("TaskShift is shutting down.")
        scheduler.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    log.info("TaskShift is starting.")
    main()
