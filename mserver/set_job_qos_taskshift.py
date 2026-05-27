#!/usr/bin/env python3
import ast
import re
import subprocess
import sys


ALLOWED_PARTITIONS = ["test"]
COMMAND_TIMEOUT_SECONDS = 60


def run_command(command):
    if isinstance(command, str):
        raise TypeError("run_command() expects a sequence of command arguments")
    try:
        p = subprocess.run(
            list(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("command timed out")
    stdout = p.stdout.strip().strip("\n")
    stderr = p.stderr.strip(" \n")
    if p.returncode != 0:
        raise RuntimeError(stderr or stdout or f"command failed with code {p.returncode}")
    return stdout


def get_job_partitions(job_spec):
    output = run_command(["squeue", "-h", "-j", job_spec, "-O", "partition:#"])
    return [
        partition.strip().strip("#").strip()
        for partition in output.splitlines()
        if partition.strip().strip("#").strip()
    ]


def jobs_are_in_allowed_partitions(job_specs):
    for job_spec in job_specs:
        partitions = get_job_partitions(job_spec)
        if not partitions:
            return False
        if any(partition not in ALLOWED_PARTITIONS for partition in partitions):
            return False
    return True


if __name__ == "__main__":
    RESULT = {"success": False}
    try:
        data = ast.literal_eval(sys.argv[1])
        if not isinstance(data, dict):
            raise AssertionError("invalid input format: expected dict")

        jobs = data["jobs"]
        if not isinstance(jobs, (list, tuple)) or not jobs:
            raise AssertionError("invalid jobs format: expected non-empty list")

        job_specs = [str(j).replace(" ", "") for j in jobs]
        job_str = ",".join(job_specs)
        qos = str(data.get("qos", "")).strip()

        if not re.fullmatch(r"[\d,\_\[\]\-]+", job_str):
            raise AssertionError("invalid job list format: only digits, ',', '_', '[', ']', '-' are allowed")

        if not re.fullmatch(r"[A-Za-z0-9_\-]+", qos):
            raise AssertionError("invalid qos format: expected [A-Za-z0-9_-]+")

        if not jobs_are_in_allowed_partitions(job_specs):
            raise AssertionError("Access denied")

        output = run_command(["sudo", "scontrol", "update", "job", job_str, f"qos={qos}"])
        if not output:
            RESULT = {"success": True}
        else:
            raise AssertionError(output)

    except Exception as err:
        RESULT = {"success": False, "error": err.__str__()}

    print(RESULT)
