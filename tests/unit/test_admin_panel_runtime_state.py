"""
Unit tests for scheduler.runtime_state.SchedulerControlPlane

These tests verify the end-to-end data flow from job runner result
to the runtime state file, which is then read by the admin panel
to display the last run status.
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from scheduler.runtime_state import (
    SCHEDULER_RUNTIME_STATE_FILE_NAME,
    SchedulerControlPlane,
)

# ════════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════════


INTERVAL_MINUTES = 15


def _make_control_plane(tmp_path):
    """Create a SchedulerControlPlane with a real state store in tmp_path."""
    cp = SchedulerControlPlane(
        projectRoot=str(tmp_path),
        intervalMinutes=INTERVAL_MINUTES,
    )
    return cp


def _sample_job_summary(**overrides):
    """Build a realistic job runner summary dict."""
    summary = {
        "pending_job_count": 8,
        "running_job_count": 3,
        "launched_count": 2,
        "skipped_by_timelimit": 1,
        "skipped_by_resources": 2,
        "skipped_by_failed_attempt_pool": 1,
        "failed_job_pool_size": 1,
        "attempted_job_ids": [1001, 1002],
        "pending_jobs": [
            {
                "job_id": 1001,
                "job_name": "train_a",
                "partition": "normal",
                "constraints": "type_a",
                "requested_cpus": 4,
                "requested_gpus": 1,
                "requested_nodes": 1,
                "timelimit_minutes": 120,
                "status": "ATTEMPTED",
                "was_attempted": True,
                "in_failed_attempt_pool": False,
            },
            {
                "job_id": 1002,
                "job_name": "train_b",
                "partition": "gpu_only",
                "constraints": "type_a",
                "requested_cpus": 2,
                "requested_gpus": 2,
                "requested_nodes": 1,
                "timelimit_minutes": 60,
                "status": "ATTEMPTED",
                "was_attempted": True,
                "in_failed_attempt_pool": False,
            },
        ],
        "effective_max_launched_jobs": 10,
    }
    summary.update(overrides)
    return summary


# ════════════════════════════════════════════════════════════════════════════════
# SchedulerControlPlane._execute — successful run writes summary to state
# ════════════════════════════════════════════════════════════════════════════════


class TestSchedulerControlPlaneExecuteSuccess:
    def test_successful_run_writes_summary_to_last_run(self, tmp_path):
        """When the job runner returns a summary dict, all fields must appear
        in the runtime state file under last_run — this is what the admin
        panel reads to display 'last run' status."""
        cp = _make_control_plane(tmp_path)
        summary = _sample_job_summary()
        cp.set_job_runner(lambda maxLaunchedJobs=None, trigger=None: summary)

        # Need a scheduler bound so can_run_now works
        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp._execute(trigger="scheduled", maxLaunchedJobs=None, failIfBusy=False)

        state = cp.get_state()
        lr = state["last_run"]

        assert lr["status"] == "success"
        assert lr["trigger"] == "scheduled"
        assert lr["finished_at"] is not None
        assert lr["duration_seconds"] is not None
        assert lr["duration_seconds"] >= 0

        # These are the critical fields that were missing when run_scheduler_once
        # did not return the summary dict
        assert lr["pending_job_count"] == 8
        assert lr["running_job_count"] == 3
        assert lr["launched_count"] == 2
        assert lr["skipped_by_timelimit"] == 1
        assert lr["skipped_by_resources"] == 2
        assert lr["skipped_by_failed_attempt_pool"] == 1
        assert lr["failed_job_pool_size"] == 1
        assert lr["attempted_job_ids"] == [1001, 1002]
        assert len(lr["pending_jobs"]) == 2
        assert lr["effective_max_launched_jobs"] == 10

    def test_successful_run_with_custom_max_launched_jobs(self, tmp_path):
        cp = _make_control_plane(tmp_path)
        summary = _sample_job_summary(effective_max_launched_jobs=5, launched_count=5)
        cp.set_job_runner(lambda maxLaunchedJobs=None, trigger=None: summary)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp._execute(trigger="manual", maxLaunchedJobs=5, failIfBusy=False)

        state = cp.get_state()
        lr = state["last_run"]

        assert lr["status"] == "success"
        assert lr["trigger"] == "manual"
        assert lr["requested_max_launched_jobs"] == 5
        assert lr["effective_max_launched_jobs"] == 5
        assert lr["launched_count"] == 5

    def test_job_runner_returning_none_writes_defaults(self, tmp_path):
        """If the job runner returns None (the old bug), summary becomes {}
        and all counts default to 0/[] — the admin panel would show empty data."""
        cp = _make_control_plane(tmp_path)
        cp.set_job_runner(lambda maxLaunchedJobs=None, trigger=None: None)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp._execute(trigger="scheduled", maxLaunchedJobs=None, failIfBusy=False)

        state = cp.get_state()
        lr = state["last_run"]

        assert lr["status"] == "success"
        # When job runner returns None, summary = {} → all defaults are 0/[]
        assert lr["pending_job_count"] == 0
        assert lr["running_job_count"] == 0
        assert lr["launched_count"] == 0
        assert lr["attempted_job_ids"] == []
        assert lr["pending_jobs"] == []

    def test_job_runner_returning_empty_dict_writes_defaults(self, tmp_path):
        cp = _make_control_plane(tmp_path)
        cp.set_job_runner(lambda maxLaunchedJobs=None, trigger=None: {})

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp._execute(trigger="scheduled", maxLaunchedJobs=None, failIfBusy=False)

        state = cp.get_state()
        lr = state["last_run"]
        assert lr["pending_job_count"] == 0
        assert lr["running_job_count"] == 0
        assert lr["launched_count"] == 0


# ════════════════════════════════════════════════════════════════════════════════
# SchedulerControlPlane._execute — failed run
# ════════════════════════════════════════════════════════════════════════════════


class TestSchedulerControlPlaneExecuteError:
    def test_failed_run_writes_error_to_last_run(self, tmp_path):
        cp = _make_control_plane(tmp_path)
        cp.set_job_runner(
            lambda maxLaunchedJobs=None, trigger=None: (_ for _ in ()).throw(
                RuntimeError("DB down")
            )
        )

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp._execute(trigger="scheduled", maxLaunchedJobs=None, failIfBusy=False)

        state = cp.get_state()
        lr = state["last_run"]

        assert lr["status"] == "error"
        assert lr["error_message"] == "DB down"
        assert lr["error_traceback"] is not None
        assert "RuntimeError" in lr["error_traceback"]
        assert lr["finished_at"] is not None
        assert lr["duration_seconds"] is not None

    def test_db_connection_error_classified(self, tmp_path):
        """MySQL connector errors should be classified as db_connection."""
        cp = _make_control_plane(tmp_path)

        def failing_runner(maxLaunchedJobs=None, trigger=None):
            raise ConnectionError("Can't connect to MySQL server")

        cp.set_job_runner(failing_runner)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp._execute(trigger="manual", maxLaunchedJobs=None, failIfBusy=False)

        state = cp.get_state()
        lr = state["last_run"]
        assert lr["status"] == "error"
        # ConnectionError with MySQL message is classified as db_connection
        assert lr["error_kind"] == "db_connection"

    def test_failed_run_clears_counts(self, tmp_path):
        cp = _make_control_plane(tmp_path)

        def failing_runner(maxLaunchedJobs=None, trigger=None):
            raise RuntimeError("boom")

        cp.set_job_runner(failing_runner)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp._execute(trigger="scheduled", maxLaunchedJobs=None, failIfBusy=False)

        state = cp.get_state()
        lr = state["last_run"]
        assert lr["pending_job_count"] == 0
        assert lr["running_job_count"] == 0
        assert lr["launched_count"] == 0
        assert lr["attempted_job_ids"] == []
        assert lr["pending_jobs"] == []


# ════════════════════════════════════════════════════════════════════════════════
# SchedulerControlPlane — runtime state file persistence
# ════════════════════════════════════════════════════════════════════════════════


class TestSchedulerControlPlanePersistence:
    def test_state_file_updated_after_run(self, tmp_path):
        """The runtime state file must be updated after each run so that
        the admin panel (which reads the file) sees fresh data."""
        cp = _make_control_plane(tmp_path)
        summary = _sample_job_summary(launched_count=7)
        cp.set_job_runner(lambda maxLaunchedJobs=None, trigger=None: summary)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp._execute(trigger="scheduled", maxLaunchedJobs=None, failIfBusy=False)

        # Read the file directly (not through SchedulerControlPlane)
        state_file = tmp_path / "logs" / SCHEDULER_RUNTIME_STATE_FILE_NAME
        assert state_file.exists()
        raw = json.loads(state_file.read_text(encoding="utf-8"))
        assert raw["last_run"]["status"] == "success"
        assert raw["last_run"]["launched_count"] == 7

    def test_admin_panel_reads_same_state(self, tmp_path):
        """Simulate what the admin panel does: create a new
        SchedulerRuntimeStateStore and read the file that the control plane
        wrote. The data must match."""
        from admin_panel.system_status import build_scheduler_system_status_payload

        cp = _make_control_plane(tmp_path)
        summary = _sample_job_summary(pending_job_count=12, launched_count=3)
        cp.set_job_runner(lambda maxLaunchedJobs=None, trigger=None: summary)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp._execute(trigger="scheduled", maxLaunchedJobs=None, failIfBusy=False)

        # Now read as the admin panel would
        with (
            patch("admin_panel.system_status.get_scheduler_service_status") as mock_svc,
            patch("admin_panel.system_status.getSchedulerConfig") as mock_cfg,
        ):
            mock_svc.return_value = {
                "running": True,
                "pid": 12345,
                "pid_file": str(tmp_path / "scheduler.pid"),
                "log_file": str(tmp_path / "logs" / "taskshift.log"),
            }
            mock_cfg.return_value.max_launched_jobs = 10

            payload = build_scheduler_system_status_payload(
                projectRoot=str(tmp_path),
                schedulerController=cp,
            )

        lr = payload["last_run"]
        assert lr["status"] == "success"
        assert lr["pending_job_count"] == 12
        assert lr["launched_count"] == 3
        assert lr["attempted_job_ids"] == [1001, 1002]
        assert len(lr["pending_jobs"]) == 2


# ════════════════════════════════════════════════════════════════════════════════
# SchedulerControlPlane — startup tick
# ════════════════════════════════════════════════════════════════════════════════


class TestSchedulerControlPlaneStartupTick:
    def test_startup_tick_writes_summary(self, tmp_path):
        cp = _make_control_plane(tmp_path)
        summary = _sample_job_summary(launched_count=1)
        cp.set_job_runner(lambda maxLaunchedJobs=None, trigger=None: summary)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        result = cp.run_startup_tick()

        state = cp.get_state()
        assert state["last_run"]["trigger"] == "startup"
        assert state["last_run"]["launched_count"] == 1

    def test_scheduled_tick_writes_summary(self, tmp_path):
        cp = _make_control_plane(tmp_path)
        summary = _sample_job_summary(launched_count=4)
        cp.set_job_runner(lambda maxLaunchedJobs=None, trigger=None: summary)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp.run_scheduled_tick()

        state = cp.get_state()
        assert state["last_run"]["trigger"] == "scheduled"
        assert state["last_run"]["launched_count"] == 4


# ════════════════════════════════════════════════════════════════════════════════
# SchedulerControlPlane — manual run
# ════════════════════════════════════════════════════════════════════════════════


class TestSchedulerControlPlaneManualRun:
    def test_manual_run_writes_summary(self, tmp_path):
        cp = _make_control_plane(tmp_path)
        summary = _sample_job_summary(launched_count=2, effective_max_launched_jobs=5)
        cp.set_job_runner(lambda maxLaunchedJobs=None, trigger=None: summary)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        cp.request_manual_run(maxLaunchedJobs=5)

        # Manual run happens in a background thread; wait briefly
        time.sleep(0.5)

        state = cp.get_state()
        assert state["last_run"]["trigger"] == "manual"
        assert state["last_run"]["launched_count"] == 2

    def test_manual_run_unavailable_without_scheduler(self, tmp_path):
        cp = _make_control_plane(tmp_path)
        # No set_job_runner, no bind_scheduler
        with pytest.raises(RuntimeError, match="unavailable"):
            cp.request_manual_run()

    def test_manual_run_rejected_when_busy(self, tmp_path):
        cp = _make_control_plane(tmp_path)

        # A runner that blocks for a while
        def slow_runner(maxLaunchedJobs=None, trigger=None):
            time.sleep(2)
            return _sample_job_summary()

        cp.set_job_runner(slow_runner)

        mock_scheduler = MagicMock()
        cp.bind_scheduler(
            scheduler=mock_scheduler,
            schedulerJobId="test-job",
            pid=12345,
            nextRunAt=datetime.now() + timedelta(minutes=INTERVAL_MINUTES),
        )

        # Start a slow run in the background
        import threading

        worker = threading.Thread(
            target=cp.run_scheduled_tick,
            daemon=True,
        )
        worker.start()

        # Try a manual run while the scheduled one is in progress
        time.sleep(0.1)
        with pytest.raises(RuntimeError, match="already in progress"):
            cp.request_manual_run()

        worker.join(timeout=5)
