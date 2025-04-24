from .integration import get_sessionid_running_tasks_test, get_sessionid_pending_tasks_test, run_task
from forecaster.model import ForecastModel
from .utils import UniqueQueue, get_devices_count
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
import signal
import sys
from datetime import datetime, timedelta
from configs.config import ClusterConfig

task_queue = UniqueQueue()
cluster_config = ClusterConfig()

forecast_model = None
last_job = None

def compute_forecasts():
    global forecast_model
    forecast_model = ForecastModel()

def task_scheduler():
    global last_job
    global task_queue
    cpu_avg, gpu_avg = 0, 0

    # The forecasts for both days are different.
    if datetime.now().weekday() == 5: # saturday
        cpu_avg, gpu_avg = forecast_model.get_saturday_avg()
    elif datetime.now().weekday() == 6: # sunday
        cpu_avg, gpu_avg = forecast_model.get_sunday_avg()

    available_cpu, available_gpu = 100 - cpu_avg, 100 - gpu_avg

    # To check how many tasks have already been started by the TaskShift.
    cpu_used, gpu_used = 0, 0

    tasks_running = get_sessionid_running_tasks_test()
    tasks_queue = get_sessionid_pending_tasks_test()

    # Задача будет в ожидании только если она не запустилась.
    # Если задача не запустилась, ее нужно опустить в низ очереди,
    # Чтобы постоянно не пытаться повторять ее запуск.
    if (last_job is not None) and (last_job in task_queue):
        task_queue.put(last_job)

    last_job = None

    # Добавляю новые задачи в очередь сервиса и удаляю уже запущенные.
    task_queue.rebuild(tasks_queue)

    all_cpu_count, all_gpu_count = ClusterConfig.get_devices_count()

    # Когда будет следующий понедельник. 
    # Нужно для расчета оставшегося времени.
    next_monday = (datetime.now() + timedelta(days=7 - datetime.now().weekday()))\
                                .replace(microsecond=0, second=0, minute=0, hour=0)

    while (not task_queue.empty()):
        task = task_queue.pop()
        cpu_req = task["cpu_cores_count"]
        gpu_req = task["gpu_count"]
        # Сколько ресурсов займет задача и влезет ли она в уже занятые
        cpu_load_req = cpu_req / all_cpu_count * 100
        gpu_load_req = gpu_req / all_gpu_count * 100

        if (cpu_used + cpu_load_req < available_cpu) and \
           (gpu_used + gpu_load_req < available_gpu) and \
           (datetime.now() + timedelta(minutes=task["time_limit"]) < next_monday):
            job_id = task["job_id"]
            last_job = job_id
            run_task(job_id)
        else:
            task_queue.put(task)

def main():
    scheduler = BlockingScheduler()

    def shutdown(signum, frame):
        scheduler.shutdown(wait=False)
        sys.exit(0)

    scheduler.add_job(
        compute_forecasts,
        trigger=CronTrigger(day_of_week="fri", hour=23, minute=50),
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
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        sys.exit(0)