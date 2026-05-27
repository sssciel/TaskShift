from __future__ import annotations

from storage.service import slurmStorage
from tests.integration.conftest import TEST_NOW
from tests.integration.e2e_helpers import (
    fetch_fake_slurm_events,
    fetch_job_row,
    insert_raw_job_rows,
    run_fake_scontrol,
)
from tests.integration.synthetic_data import SyntheticJobFactory


class TestSchedulerEndToEnd:
    def test_scheduler_requests_mserver_qos_for_pending_gpu_job(
        self,
        db_connection,
        taskshift_runner,
    ):
        factory = SyntheticJobFactory(base_time=TEST_NOW - 600)
        pending_job = factory.pending_job(
            feature="type_a",
            cpus=4,
            gpus=1,
            start_offset=0,
        )
        insert_raw_job_rows(db_connection, [pending_job])

        result = taskshift_runner("run-scheduler-once")

        assert result.returncode == 0, result.stderr or result.stdout

        updated_row = fetch_job_row(db_connection, pending_job.id_job)
        assert updated_row is not None
        assert updated_row["state"] == 0
        assert updated_row["time_start"] == 0
        assert updated_row["time_end"] == 0

        events = fetch_fake_slurm_events(db_connection)
        assert len(events) == 1
        assert events[0]["command_name"] == "mserver"
        assert events[0]["action_name"] == "set_qos"
        assert events[0]["job_id"] == pending_job.id_job
        assert events[0]["qos_value"] == "taskshift"

        storage = slurmStorage().create()
        try:
            pending_ids = [job.getID() for job in storage.getPendingJobs()]
            active_ids = [job.getLogicalKey() for job in storage.getRunningJobs(nowTimestamp=TEST_NOW)]
        finally:
            storage.close()

        assert pending_job.id_job in pending_ids
        assert pending_job.id_job not in active_ids

    def test_scheduler_respects_gpu_capacity_and_leaves_oversized_job_pending(
        self,
        db_connection,
        taskshift_runner,
    ):
        factory = SyntheticJobFactory(base_time=TEST_NOW - 7200)
        running_job = factory.multi_node_job(
            feature="type_a",
            nodes="cn-[001-003]",
            cpus_per_node=4,
            gpus_per_node=2,
            num_nodes=3,
            start_offset=0,
            duration_seconds=3600,
        )
        running_job.state = 1
        running_job.time_end = 0
        running_job.mod_time = TEST_NOW - 300
        running_job.tres_alloc = "1=12,4=3,5=12,1001=6"

        small_job = factory.pending_job(
            feature="type_a",
            cpus=4,
            gpus=1,
            start_offset=60,
        )
        small_job.timelimit = 30

        large_job = factory.pending_job(
            feature="type_a",
            cpus=4,
            gpus=2,
            start_offset=120,
        )
        large_job.timelimit = 60

        insert_raw_job_rows(db_connection, [running_job, small_job, large_job])

        result = taskshift_runner("run-scheduler-once")

        assert result.returncode == 0, result.stderr or result.stdout

        small_row = fetch_job_row(db_connection, small_job.id_job)
        large_row = fetch_job_row(db_connection, large_job.id_job)

        assert small_row is not None and small_row["state"] == 0
        assert large_row is not None and large_row["state"] == 0
        assert large_row["time_start"] == 0

        events = fetch_fake_slurm_events(db_connection)
        assert [event["job_id"] for event in events] == [small_job.id_job]
        assert [event["action_name"] for event in events] == ["set_qos"]

    def test_fake_slurm_can_start_and_complete_a_qos_marked_job(
        self,
        db_connection,
        taskshift_runner,
        taskshift_test_env,
        repo_root,
    ):
        factory = SyntheticJobFactory(base_time=TEST_NOW - 900)
        pending_job = factory.pending_job(
            feature="type_a",
            cpus=4,
            gpus=1,
            start_offset=0,
        )
        insert_raw_job_rows(db_connection, [pending_job])

        launch_result = taskshift_runner("run-scheduler-once")
        assert launch_result.returncode == 0, launch_result.stderr or launch_result.stdout

        start_env = dict(taskshift_test_env)
        start_env.update(
            {
                "TASKSHIFT_JOB_ID": str(pending_job.id_job),
                "TASKSHIFT_CPUS": "4",
                "TASKSHIFT_GPUS": "1",
                "TASKSHIFT_FEATURE": "type_a",
                "TASKSHIFT_NODES": "cn-001",
            }
        )
        start_result = run_fake_scontrol(
            repo_root,
            start_env,
            "update",
            f"JobId={pending_job.id_job}",
            "QOS=taskshift",
            "TaskShiftAction=start",
            f"TaskShiftNow={TEST_NOW}",
        )
        assert start_result.returncode == 0, start_result.stderr or start_result.stdout

        completion_env = dict(taskshift_test_env)
        completion_env.update(
            {
                "TASKSHIFT_JOB_ID": str(pending_job.id_job),
                "TASKSHIFT_CPUS": "4",
                "TASKSHIFT_GPUS": "1",
                "TASKSHIFT_FEATURE": "type_a",
                "TASKSHIFT_NODES": "cn-001",
            }
        )
        complete_result = run_fake_scontrol(
            repo_root,
            completion_env,
            "update",
            f"JobId={pending_job.id_job}",
            "TaskShiftAction=complete",
            f"TaskShiftNow={TEST_NOW + 300}",
        )

        assert complete_result.returncode == 0, complete_result.stderr or complete_result.stdout

        completed_row = fetch_job_row(db_connection, pending_job.id_job)
        assert completed_row is not None
        assert completed_row["state"] == 3
        assert completed_row["time_end"] == TEST_NOW + 300

        storage = slurmStorage().create()
        try:
            active_ids = [job.getLogicalKey() for job in storage.getRunningJobs(nowTimestamp=TEST_NOW + 301)]
        finally:
            storage.close()

        assert pending_job.id_job not in active_ids

        events = fetch_fake_slurm_events(db_connection)
        assert [event["action_name"] for event in events] == [
            "set_qos",
            "start",
            "complete",
        ]

    def test_scheduler_skips_jobs_above_timelimit_without_touching_queue(
        self,
        db_connection,
        taskshift_runner,
    ):
        factory = SyntheticJobFactory(base_time=TEST_NOW - 1200)
        pending_job = factory.pending_job(
            feature="type_a",
            cpus=4,
            gpus=1,
            start_offset=0,
        )
        pending_job.timelimit = 600
        insert_raw_job_rows(db_connection, [pending_job])

        result = taskshift_runner("run-scheduler-once")

        assert result.returncode == 0, result.stderr or result.stdout

        untouched_row = fetch_job_row(db_connection, pending_job.id_job)
        assert untouched_row is not None
        assert untouched_row["state"] == 0
        assert untouched_row["time_start"] == 0
        assert fetch_fake_slurm_events(db_connection) == []
