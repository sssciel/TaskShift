"""
Test fixtures for storage module
"""

from storage.models import HistoricalJob, Job, RawHistoricalJobRow

# Sample pending jobs (state=0, time_start=0)
PENDING_JOB_RAW = {
    "job_db_inx": 3518976,
    "id_job": 3984401,
    "job_name": "wm_merge",
    "timelimit": 1440,
    "state": 0,
    "priority": 420,
    "constraints": "type_a|type_b|type_d",
    "cpus_req": 2,
    "tres_req": "1=2,2=1,4=1,5=2",
    "nodelist": None,
    "partition": "normal",
}

PENDING_JOB_RAW_2 = {
    "job_db_inx": 3406336,
    "id_job": 3984399,
    "job_name": "wm_bases",
    "timelimit": 1440,
    "state": 0,
    "priority": 420,
    "constraints": "type_a|type_b|type_d",
    "cpus_req": 2,
    "tres_req": "1=2,2=1,4=1,5=2",
    "nodelist": None,
    "partition": "normal",
}

# Sample completed jobs (state=3, time_end > 0)
COMPLETED_JOB_RAW = {
    "job_db_inx": 3584,
    "id_job": 3984258,
    "job_name": "run_m.sh",
    "timelimit": 1440,
    "state": 3,
    "priority": 138,
    "constraints": "type_a|type_b|type_d",
    "cpus_req": 1,
    "nodes_alloc": 1,
    "time_start": 1778493027,
    "time_end": 1778493048,
    "time_submit": 1778492997,
    "time_eligible": 1778492997,
    "mod_time": 1778493048,
    "tres_req": "1=1,2=1,4=1,5=1",
    "tres_alloc": "1=1,4=1,5=1",
    "nodelist": "cn-032",
    "partition": "normal",
}

COMPLETED_JOB_RAW_2 = {
    "job_db_inx": 2565888,
    "id_job": 3984257,
    "job_name": "run_m.sh",
    "timelimit": 1440,
    "state": 3,
    "priority": 142,
    "constraints": "type_a|type_b|type_d",
    "cpus_req": 12,
    "nodes_alloc": 1,
    "time_start": 1778492907,
    "time_end": 1778492992,
    "time_submit": 1778492902,
    "time_eligible": 1778492902,
    "mod_time": 1778492992,
    "tres_req": "1=12,2=1,4=1,5=12",
    "tres_alloc": "1=12,4=1,5=12",
    "nodelist": "cn-023",
    "partition": "normal",
}

# Sample old jobs (different TRES format)
OLD_JOB_RAW = {
    "job_db_inx": 14,
    "id_job": 23,
    "job_name": "bash",
    "timelimit": 1440,
    "state": 3,
    "priority": 4294901758,
    "constraints": None,
    "cpus_req": 14,
    "nodes_alloc": 14,
    "time_start": 1550484270,
    "time_end": 1550484275,
    "time_submit": 1550484270,
    "time_eligible": 1550484270,
    "mod_time": 1550484275,
    "tres_req": "1=14,4=14",
    "tres_alloc": "1=616,4=14,5=616",
    "nodelist": "cn-[001-014]",
    "partition": "normal",
}


# Helper functions to create model instances
def create_pending_job(**kwargs):
    """Create a pending Job instance"""
    data = {
        "jobID": kwargs.get("jobID", 3984401),
        "jobName": kwargs.get("jobName", "test_job"),
        "timelimit": kwargs.get("timelimit", 1440),
        "state": kwargs.get("state", 0),
        "priority": kwargs.get("priority", 100),
        "constraints": kwargs.get("constraints", "type_a"),
        "cpusReq": kwargs.get("cpusReq", 2),
        "tresReq": kwargs.get("tresReq", "1=2,4=1"),
        "partition": kwargs.get("partition", "normal"),
    }
    return Job(**data)


def create_historical_job(**kwargs):
    """Create a HistoricalJob instance"""
    data = {
        "dbIndex": kwargs.get("dbIndex", 12345),
        "jobID": kwargs.get("jobID", 3984258),
        "jobName": kwargs.get("jobName", "test_job"),
        "timelimit": kwargs.get("timelimit", 1440),
        "state": kwargs.get("state", 3),
        "priority": kwargs.get("priority", 100),
        "constraints": kwargs.get("constraints", "type_a"),
        "cpusReq": kwargs.get("cpusReq", 1),
        "nodesAlloc": kwargs.get("nodesAlloc", 1),
        "timeStart": kwargs.get("timeStart", 1778493027),
        "timeEnd": kwargs.get("timeEnd", 1778493048),
        "timeSubmit": kwargs.get("timeSubmit", 1778492997),
        "timeEligible": kwargs.get("timeEligible", 1778492997),
        "modTime": kwargs.get("modTime", 1778493048),
        "tresReq": kwargs.get("tresReq", "1=1,4=1"),
        "tresAlloc": kwargs.get("tresAlloc", "1=1,4=1"),
        "nodelist": kwargs.get("nodelist", "cn-001"),
        "partition": kwargs.get("partition", "normal"),
    }
    return HistoricalJob(**data)


def create_raw_historical_job_row(**kwargs):
    """Create a RawHistoricalJobRow instance"""
    data = {
        "job_db_inx": kwargs.get("job_db_inx", 12345),
        "id_job": kwargs.get("id_job", 3984258),
        "job_name": kwargs.get("job_name", "test_job"),
        "timelimit": kwargs.get("timelimit", 1440),
        "state": kwargs.get("state", 3),
        "priority": kwargs.get("priority", 100),
        "constraints": kwargs.get("constraints", "type_a"),
        "cpus_req": kwargs.get("cpus_req", 1),
        "nodes_alloc": kwargs.get("nodes_alloc", 1),
        "time_start": kwargs.get("time_start", 1778493027),
        "time_end": kwargs.get("time_end", 1778493048),
        "time_submit": kwargs.get("time_submit", 1778492997),
        "time_eligible": kwargs.get("time_eligible", 1778492997),
        "mod_time": kwargs.get("mod_time", 1778493048),
        "tres_req": kwargs.get("tres_req", "1=1,4=1"),
        "tres_alloc": kwargs.get("tres_alloc", "1=1,4=1"),
        "nodelist": kwargs.get("nodelist", "cn-001"),
        "partition": kwargs.get("partition", "normal"),
    }
    return RawHistoricalJobRow(**data)


# TRES test data
TRES_TEST_CASES = [
    # (tres_string, expected_parsed_map)
    ("1=2,4=1,5=2", {"1": 2, "4": 1, "5": 2}),
    ("1=12,2=1,4=1,5=12", {"1": 12, "2": 1, "4": 1, "5": 12}),
    ("1=616,4=14,5=616", {"1": 616, "4": 14, "5": 616}),
    ("", {}),
    (None, {}),
    ("1=invalid", {}),  # Invalid value
    ("invalid=1", {}),  # Invalid key
]

# GPU TRES aliases
GPU_ALIASES = {"1001", "gres/gpu", "gpu"}

# Node TRES aliases
NODE_ALIASES = {"4", "node", "nodes"}

# CPU TRES aliases
CPU_ALIASES = {"1", "cpu"}
