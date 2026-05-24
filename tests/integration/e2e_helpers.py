from __future__ import annotations

import subprocess
from pathlib import Path

from storage.models import RawHistoricalJobRow


def insert_raw_job_rows(connection, rows: list[RawHistoricalJobRow]):
    cursor = connection.cursor()
    cursor.executemany(
        """
        INSERT INTO linux_job_table (
            job_db_inx, id_job, job_name, timelimit, state, priority, constraints,
            cpus_req, nodes_alloc, time_start, time_end, time_submit, time_eligible,
            mod_time, tres_req, tres_alloc, nodelist, `partition`, deleted
        ) VALUES (
            %(job_db_inx)s, %(id_job)s, %(job_name)s, %(timelimit)s, %(state)s, %(priority)s, %(constraints)s,
            %(cpus_req)s, %(nodes_alloc)s, %(time_start)s, %(time_end)s, %(time_submit)s, %(time_eligible)s,
            %(mod_time)s, %(tres_req)s, %(tres_alloc)s, %(nodelist)s, %(partition)s, 0
        )
        """,
        [row.to_dict() for row in rows],
    )
    connection.commit()
    cursor.close()


def fetch_job_row(connection, job_id: int) -> dict | None:
    cursor = connection.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT job_db_inx, id_job, job_name, timelimit, state, priority, constraints,
               cpus_req, nodes_alloc, time_start, time_end, time_submit, time_eligible,
               mod_time, tres_req, tres_alloc, nodelist, `partition`, deleted
        FROM linux_job_table
        WHERE id_job = %s
        ORDER BY job_db_inx DESC
        LIMIT 1
        """,
        (job_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    return row


def fetch_fake_slurm_events(connection) -> list[dict]:
    cursor = connection.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT id, event_time, command_name, job_id, action_name, qos_value,
               feature_name, node_names, raw_args
        FROM fake_slurm_events
        ORDER BY id ASC
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def run_fake_scontrol(repo_root: Path, env: dict[str, str], *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(repo_root / "tests" / "integration" / "fake_scontrol.sh"), *args],
        cwd=str(repo_root),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
