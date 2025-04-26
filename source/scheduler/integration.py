import json
import os
import time
from enum import Enum
from configs.logging import log

import requests


class TaskStates(Enum):
    queue: 0
    running: 0


MAX_ERROR_COUNT = 10

# Test env
sessionid = os.getenv("TASKMASTER_SESSIONID", "")
csrf = os.getenv("TASKMASTER_CRFT", "")

service_host = os.getenv("TASKMASTER_HOST")
user_token = os.getenv("TASKMASTER_TOKEN", "")


# get_url_from_dict create URL from filter dict 
def get_url_from_dict(query: dict):
    return "&".join(f"{k}={v}" for k, v in query.items())


def make_api_call(
    request: str,
    cookies: dict = {},
    headers: dict = {},
    correct_status: int = 200,
):
    error_count = 0
    while error_count < MAX_ERROR_COUNT:
        response = requests.get(
            service_host + str(request),
            cookies=cookies,
            headers=headers,
        )

        if response.status_code == correct_status:
            break
        else:
            error_count += 1
            log.error(f"Request {request}.\nCan't connect to HPC TaskMaster. Retrying...")
            time.sleep(6)
    else:
        log.critical(f"Request {request}.\nCan't connect to HPC TaskMaster. Aborted. Error: {response.text}")

    return response


def get_tasks_with_state(state: TaskStates = 0):
    filters = {"state": state}

    result = make_api_call(
        request=f"job/?{get_url_from_dict(filters)}", headers={"UserToken": user_token}
    ).json()["results"]

    return result


def get_pending_tasks():
    result = make_api_call(
        request="pending/", headers={"UserToken": user_token}
    ).json()["results"]

    return result


def run_task(jobid: str):
    result = make_api_call(request=jobid, headers={"UserToken": user_token}).json()[
        "results"
    ]

    return result


## Test Zone
## TO DO: Remove these functions

def get_sessionid_running_tasks_test():
    with open("test_http_query.json", encoding="UTF-8") as f:
        r = json.load(f)
    return r[0]


def get_sessionid_pending_tasks_test():
    with open("test_http_pending.json", encoding="UTF-8") as f:
        response = json.load(f)

    result = {}
    for task in response["results"]:
        result[task["job_id"]] = {
            "job_id": task["job_id"],  # second time for Queue
            "cpu_cores_count": task["cpu_cores_count"],
            "gpu_count": task["gpu_count"],
            "time_limit": task["time_limit"],
        }

    return result

# Test function that uses a user's cookie instead of a token.
def get_sessionid_running_tasks():
    filters = {"state": 1}
    cookies = {"sessionid": sessionid, "csrftoken": csrf}

    result = make_api_call(
        request=f"job/?{get_url_from_dict(filters)}", cookies=cookies
    ).json()["results"]

    return result