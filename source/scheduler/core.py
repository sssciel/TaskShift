from datetime import datetime, timedelta

from configs.config import ClusterConfig
from configs.logging import log
from forecaster.model import ForecastModel

from .integration import (get_sessionid_pending_tasks_test,
                          get_sessionid_running_tasks_test, run_task)
from .utils import UniqueQueue

task_queue = UniqueQueue()
cluster_config = ClusterConfig()

# NeuralProphet model with forecasts
forecast_model = None

# Last started scheduler's job.
last_job = None
next_monday = None

OVERRIDE_CPU = None
OVERRIDE_GPU = None

# compute_forecasts creates and trains a forecasting model.
def compute_forecasts():
    global next_monday

    log.info("It’s the end of Friday. Forecast calculation has just begun.")

    global forecast_model
    forecast_model = ForecastModel()

    # Calculate the date of the next Monday
    # Needed to compute the remaining time
    next_monday = (
        datetime.now() + timedelta(days=7 - datetime.now().weekday())
    ).replace(microsecond=0, second=0, minute=0, hour=0)


def task_scheduler():
    if forecast_model is None:
        log.debug(
            "Scheduler ran before forecasts were computed. Launching compute_forecasts"
        )
        compute_forecasts()

    log.debug(f"It's {datetime.now().strftime("%A")} now. New cycle is running.")

    global last_job
    global task_queue
    cpu_avg, gpu_avg = 0, 0

    # The forecasts for both days are different.
    if datetime.now().weekday() == 5:  # saturday
        cpu_avg, gpu_avg = forecast_model.get_saturday_avg()
    elif datetime.now().weekday() == 6:  # sunday
        cpu_avg, gpu_avg = forecast_model.get_sunday_avg()

    forecasted_cpu = 100 - cpu_avg
    forecasted_gpu = 100 - gpu_avg

    # "Free" resources for the remaining time.
    available_cpu = OVERRIDE_CPU if OVERRIDE_CPU is not None else forecasted_cpu
    available_gpu = OVERRIDE_GPU if OVERRIDE_GPU is not None else forecasted_gpu

    # To track how many resources were consumed by tasks started by the scheduler.
    cpu_used, gpu_used = 0, 0

    tasks_running = get_sessionid_running_tasks_test()
    tasks_queue = get_sessionid_pending_tasks_test()

    # If the last task didn't start, put it back at the end of the queue
    if (last_job is not None) and (last_job in task_queue):
        log.debug(f"Task {last_job.get('job_id', 'UNDEFINED')} didn't start.")
        task_queue.put(last_job)

    last_job = None

    # Add new tasks to the service queue and remove already started ones
    task_queue.rebuild(tasks_queue)

    all_cpu_count, all_gpu_count = cluster_config.get_devices_count()

    while not task_queue.empty():
        task = task_queue.pop()
        cpu_req = task["cpu_cores_count"]
        gpu_req = task["gpu_count"]

        # How many resources the task requires and whether it fits
        cpu_load_req = cpu_req / all_cpu_count * 100
        gpu_load_req = gpu_req / all_gpu_count * 100

        if (
            (cpu_used + cpu_load_req < available_cpu)
            and (gpu_used + gpu_load_req < available_gpu)
            and (datetime.now() + timedelta(minutes=task["time_limit"]) < next_monday)
        ):
            log.debug(f"Try to start task {task["job_id"]}.")

            last_job = task
            run_task(task["job_id"])
            break
        else:
            log.debug(f"Can't run task {task['job_id']}. Moving to next task.")
            task_queue.put(task)
