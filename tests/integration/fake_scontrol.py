#!/usr/bin/env python3

from __future__ import annotations

import json
import os
import sys
import time

import mysql.connector


PENDING_STATE = 0
RUNNING_STATE = 1
COMPLETED_STATE = 3


def parse_kv_args(args: list[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for arg in args:
        if "=" not in arg:
            continue
        key, value = arg.split("=", 1)
        parsed[key] = value
    return parsed


def connect():
    return mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWD"],
        database=os.environ["DB_DATABASE"],
        charset=os.environ.get("DB_CHARSET", "utf8mb4"),
        collation=os.environ.get("DB_COLLATION", "utf8mb4_general_ci"),
    )


def build_tres_alloc(cpus: int, nodes_count: int, gpus: int) -> str:
    entries = [f"1={cpus}", f"4={max(nodes_count, 1)}", f"5={cpus}"]
    if gpus > 0:
        entries.append(f"1001={gpus}")
    return ",".join(entries)


def log_event(connection, *, command_name: str, job_id: int, action_name: str, qos_value: str | None, feature_name: str | None, node_names: str | None, raw_args: list[str]):
    cursor = connection.cursor()
    cursor.execute(
        """
        INSERT INTO fake_slurm_events (
            event_time, command_name, job_id, action_name, qos_value,
            feature_name, node_names, raw_args
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            int(time.time()),
            command_name,
            int(job_id),
            action_name,
            qos_value,
            feature_name,
            node_names,
            json.dumps(raw_args, ensure_ascii=False),
        ),
    )
    connection.commit()
    cursor.close()


def update_job_state(connection, *, job_id: int, action_name: str, now_ts: int, node_names: str, cpus: int, gpus: int):
    nodes_count = len([node for node in node_names.split(",") if node.strip()]) if node_names else 0
    tres_alloc = build_tres_alloc(cpus=cpus, nodes_count=nodes_count, gpus=gpus)

    cursor = connection.cursor()
    if action_name == "start":
        cursor.execute(
            """
            UPDATE linux_job_table
            SET state = %s,
                time_start = CASE WHEN time_start = 0 THEN %s ELSE time_start END,
                time_end = 0,
                nodes_alloc = %s,
                mod_time = %s,
                nodelist = %s,
                tres_alloc = %s,
                deleted = 0
            WHERE id_job = %s
            """,
            (
                RUNNING_STATE,
                now_ts,
                max(nodes_count, 1),
                now_ts,
                node_names or "None assigned",
                tres_alloc,
                int(job_id),
            ),
        )
    elif action_name == "complete":
        cursor.execute(
            """
            UPDATE linux_job_table
            SET state = %s,
                time_start = CASE WHEN time_start = 0 THEN %s ELSE time_start END,
                time_end = %s,
                mod_time = %s,
                deleted = 0
            WHERE id_job = %s
            """,
            (
                COMPLETED_STATE,
                max(now_ts - 60, 1),
                now_ts,
                now_ts,
                int(job_id),
            ),
        )
    elif action_name == "start_and_complete":
        cursor.execute(
            """
            UPDATE linux_job_table
            SET state = %s,
                time_start = CASE WHEN time_start = 0 THEN %s ELSE time_start END,
                time_end = %s,
                nodes_alloc = %s,
                mod_time = %s,
                nodelist = %s,
                tres_alloc = %s,
                deleted = 0
            WHERE id_job = %s
            """,
            (
                COMPLETED_STATE,
                max(now_ts - 60, 1),
                now_ts,
                max(nodes_count, 1),
                now_ts,
                node_names or "None assigned",
                tres_alloc,
                int(job_id),
            ),
        )
    else:
        raise ValueError(f"Unsupported TaskShiftAction: {action_name}")

    connection.commit()
    cursor.close()


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("usage: fake_scontrol.py update JobId=<id> [Key=Value...]", file=sys.stderr)
        return 2

    command_name = argv[1]
    kv_args = parse_kv_args(argv[2:])
    job_id = int(kv_args.get("JobId") or os.environ.get("TASKSHIFT_JOB_ID") or "0")
    if job_id <= 0:
        print("error: JobId is required", file=sys.stderr)
        return 2

    action_name = kv_args.get("TaskShiftAction") or os.environ.get("FAKE_SLURM_ACTION") or "start"
    now_ts = int(kv_args.get("TaskShiftNow") or os.environ.get("TASKSHIFT_TEST_NOW") or int(time.time()))
    feature_name = os.environ.get("TASKSHIFT_FEATURE", "")
    node_names = os.environ.get("TASKSHIFT_NODES", "")
    qos_value = kv_args.get("QOS") or os.environ.get("TASKSHIFT_QOS")
    cpus = int(os.environ.get("TASKSHIFT_CPUS", "0") or "0")
    gpus = int(os.environ.get("TASKSHIFT_GPUS", "0") or "0")

    connection = connect()
    try:
        log_event(
            connection,
            command_name=command_name,
            job_id=job_id,
            action_name=action_name,
            qos_value=qos_value,
            feature_name=feature_name,
            node_names=node_names,
            raw_args=argv[1:],
        )
        if command_name == "update":
            update_job_state(
                connection,
                job_id=job_id,
                action_name=action_name,
                now_ts=now_ts,
                node_names=node_names,
                cpus=cpus,
                gpus=gpus,
            )
    finally:
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
