from pathlib import Path

PENDING_STATE = 0
DEFAULT_BUCKET_MINUTES = 15
DEFAULT_EXPORT_ROOT = Path("exports") / "historical_utilization" / "current"
RAW_JOBS_CACHE_FILE = "raw_job_rows.json"
STATE_FILE = "state.json"
SERIES_DIR = "series"
METADATA_FILE = "metadata.json"

GET_JOBS_WITH_STATE_QUERY = """SELECT id_job, job_name, timelimit, priority, constraints, cpus_req, tres_req, `partition`
                        FROM linux_job_table
                        WHERE state=%s ORDER BY timelimit ASC"""

GET_HISTORICAL_JOBS_BASE_QUERY = """SELECT job_db_inx, id_job, job_name, timelimit, state, priority, constraints,
                        cpus_req, nodes_alloc, time_start, time_end, time_submit, time_eligible,
                        mod_time, tres_req, tres_alloc, nodelist, `partition`
                        FROM linux_job_table
                        WHERE deleted=0 AND time_start > 0"""

GET_ACTIVE_JOBS_BASE_QUERY = """SELECT job_db_inx, id_job, job_name, timelimit, state, priority, constraints,
                        cpus_req, nodes_alloc, time_start, time_end, time_submit, time_eligible,
                        mod_time, tres_req, tres_alloc, nodelist, `partition`
                        FROM linux_job_table
                        WHERE deleted=0 AND time_start > 0 AND (time_end = 0 OR time_end > %s)"""
