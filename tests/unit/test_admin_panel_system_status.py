"""
Unit tests for admin_panel.system_status module
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from admin_panel.system_status import (
    _parse_iso_timestamp,
    build_scheduler_system_status_payload,
)

# ════════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════════


def _make_runtime_state(
    *,
    running=False,
    pid=None,
    next_run_at=None,
    last_run_status="idle",
    last_run_trigger=None,
    last_run_started_at=None,
    last_run_finished_at=None,
    last_run_duration_seconds=None,
    last_run_pending_job_count=0,
    last_run_running_job_count=0,
    last_run_launched_count=0,
    last_run_attempted_job_ids=None,
    last_run_pending_jobs=None,
    last_run_failed_job_pool_size=0,
    last_run_error_kind=None,
    last_run_error_message=None,
    last_run_error_traceback=None,
    last_run_effective_max_launched_jobs=None,
):
    """Build a minimal runtime-state dict."""
    return {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "service": {
            "running": running,
            "pid": pid,
            "interval_minutes": 15,
            "next_run_at": next_run_at,
            "manual_run_available": False,
        },
        "last_run": {
            "status": last_run_status,
            "trigger": last_run_trigger,
            "started_at": last_run_started_at,
            "finished_at": last_run_finished_at,
            "duration_seconds": last_run_duration_seconds,
            "requested_max_launched_jobs": None,
            "effective_max_launched_jobs": last_run_effective_max_launched_jobs,
            "error_kind": last_run_error_kind,
            "error_message": last_run_error_message,
            "error_traceback": last_run_error_traceback,
            "pending_job_count": last_run_pending_job_count,
            "running_job_count": last_run_running_job_count,
            "launched_count": last_run_launched_count,
            "skipped_by_timelimit": 0,
            "skipped_by_resources": 0,
            "skipped_by_failed_attempt_pool": 0,
            "failed_job_pool_size": last_run_failed_job_pool_size,
            "attempted_job_ids": last_run_attempted_job_ids or [],
            "pending_jobs": last_run_pending_jobs or [],
        },
    }


def _sample_pending_jobs():
    return [
        {
            "job_id": 1001,
            "job_name": "train_model_a",
            "partition": "normal",
            "constraints": "type_a",
            "requested_cpus": 4,
            "requested_gpus": 1,
            "requested_nodes": 1,
            "timelimit_minutes": 120,
            "status": "LAUNCH_ATTEMPTED",
            "was_attempted": True,
            "in_failed_attempt_pool": False,
        },
        {
            "job_id": 1002,
            "job_name": "train_model_b",
            "partition": "normal",
            "constraints": "type_a",
            "requested_cpus": 2,
            "requested_gpus": 0,
            "requested_nodes": 1,
            "timelimit_minutes": 60,
            "status": "LAUNCH_FAILED",
            "was_attempted": True,
            "in_failed_attempt_pool": True,
        },
        {
            "job_id": 1003,
            "job_name": "data_prep",
            "partition": "normal",
            "constraints": "type_b",
            "requested_cpus": 4,
            "requested_gpus": 0,
            "requested_nodes": 1,
            "timelimit_minutes": 30,
            "status": "LEFT_PENDING_QUEUE",
            "was_attempted": True,
            "in_failed_attempt_pool": False,
        },
    ]


def _build_payload(
    runtime_state,
    controller=None,
    service_running=False,
    service_pid=None,
    max_launched_jobs=10,
):
    """Build system status payload with all necessary mocks."""
    with (
        patch("admin_panel.system_status.get_scheduler_service_status") as mock_status,
        patch("admin_panel.system_status.getSchedulerConfig") as mock_config,
        patch("admin_panel.system_status.SchedulerRuntimeStateStore") as mock_store_cls,
    ):
        mock_status.return_value = {
            "running": service_running,
            "pid": service_pid if service_running else None,
            "pid_file": "/p",
            "log_file": "/l",
        }
        mock_config.return_value.max_launched_jobs = max_launched_jobs
        mock_store = MagicMock()
        mock_store.read.return_value = runtime_state
        mock_store.filePath = Path("/logs/scheduler_runtime_state.json")
        mock_store_cls.return_value = mock_store

        return build_scheduler_system_status_payload(
            projectRoot="/project",
            schedulerController=controller,
        )


# ════════════════════════════════════════════════════════════════════════════════
# _parse_iso_timestamp
# ════════════════════════════════════════════════════════════════════════════════


class TestParseIsoTimestamp:
    def test_valid_iso(self):
        result = _parse_iso_timestamp("2025-01-15T10:30:00")
        assert result is not None
        assert result.year == 2025

    def test_with_timezone(self):
        result = _parse_iso_timestamp("2025-01-15T10:30:00+03:00")
        assert result is not None

    def test_none(self):
        assert _parse_iso_timestamp(None) is None

    def test_empty_string(self):
        assert _parse_iso_timestamp("") is None

    def test_invalid_string(self):
        assert _parse_iso_timestamp("not-a-date") is None


# ════════════════════════════════════════════════════════════════════════════════
# build_scheduler_system_status_payload — service status field
# ════════════════════════════════════════════════════════════════════════════════


class TestBuildSystemStatusPayloadServiceField:
    def test_inactive_service_when_not_running_no_last_run(self):
        state = _make_runtime_state()
        payload = _build_payload(state, service_running=False)

        assert payload["service"]["status"] == "inactive"
        assert payload["service"]["running"] is False
        assert payload["service"]["countdown_seconds"] is None
        assert payload["service"]["next_run_at"] is None

    def test_active_status_when_service_running_and_last_success(self):
        finished = datetime.now() - timedelta(minutes=5)
        next_run = datetime.now() + timedelta(minutes=10)
        state = _make_runtime_state(
            running=True,
            pid=12345,
            next_run_at=next_run.isoformat(timespec="seconds"),
            last_run_status="success",
            last_run_finished_at=finished.isoformat(timespec="seconds"),
        )
        payload = _build_payload(state, service_running=True, service_pid=12345)

        assert payload["service"]["status"] == "active"
        assert payload["service"]["running"] is True
        assert payload["service"]["pid"] == 12345
        assert payload["service"]["countdown_seconds"] is not None
        assert payload["service"]["countdown_seconds"] >= 0

    def test_running_status_when_last_run_in_progress(self):
        started = datetime.now() - timedelta(seconds=30)
        state = _make_runtime_state(
            running=True,
            last_run_status="running",
            last_run_started_at=started.isoformat(timespec="seconds"),
        )
        payload = _build_payload(state, service_running=True, service_pid=9999)

        assert payload["service"]["status"] == "running"
        assert payload["service"]["countdown_seconds"] is None

    def test_error_status_when_last_run_failed(self):
        finished = datetime.now() - timedelta(minutes=2)
        state = _make_runtime_state(
            running=True,
            last_run_status="error",
            last_run_finished_at=finished.isoformat(timespec="seconds"),
            last_run_error_kind="scheduler_error",
            last_run_error_message="Test error",
        )
        payload = _build_payload(state, service_running=True, service_pid=123)

        assert payload["service"]["status"] == "error"

    def test_db_connection_error_status(self):
        finished = datetime.now() - timedelta(minutes=1)
        state = _make_runtime_state(
            running=True,
            last_run_status="error",
            last_run_finished_at=finished.isoformat(timespec="seconds"),
            last_run_error_kind="db_connection",
            last_run_error_message="Access denied for user",
        )
        payload = _build_payload(state, service_running=True, service_pid=123)

        assert payload["service"]["status"] == "db_connection_error"

    def test_active_status_when_no_last_run_and_service_running(self):
        state = _make_runtime_state(running=True, last_run_status="idle")
        payload = _build_payload(state, service_running=True, service_pid=42)

        assert payload["service"]["status"] == "active"


# ════════════════════════════════════════════════════════════════════════════════
# build_scheduler_system_status_payload — last_run field
# ════════════════════════════════════════════════════════════════════════════════


class TestBuildSystemStatusPayloadLastRun:
    def test_idle_last_run(self):
        state = _make_runtime_state()
        payload = _build_payload(state)

        assert payload["last_run"]["status"] == "idle"
        assert payload["last_run"]["finished_at"] is None

    def test_successful_run_counts(self):
        finished = datetime.now().isoformat(timespec="seconds")
        state = _make_runtime_state(
            last_run_status="success",
            last_run_trigger="scheduled",
            last_run_finished_at=finished,
            last_run_duration_seconds=12.5,
            last_run_pending_job_count=5,
            last_run_running_job_count=3,
            last_run_launched_count=2,
            last_run_failed_job_pool_size=1,
            last_run_attempted_job_ids=[1001, 1002],
            last_run_pending_jobs=_sample_pending_jobs(),
        )
        payload = _build_payload(state)

        lr = payload["last_run"]
        assert lr["status"] == "success"
        assert lr["trigger"] == "scheduled"
        assert lr["finished_at"] == finished
        assert lr["duration_seconds"] == 12.5
        assert lr["pending_job_count"] == 5
        assert lr["running_job_count"] == 3
        assert lr["launched_count"] == 2
        assert lr["failed_job_pool_size"] == 1
        assert lr["attempted_job_ids"] == [1001, 1002]
        assert len(lr["pending_jobs"]) == 3

    def test_error_run_with_traceback(self):
        finished = datetime.now().isoformat(timespec="seconds")
        tb_text = "Traceback (most recent call last):\n  File 'x.py', line 1\nValueError: boom"
        state = _make_runtime_state(
            last_run_status="error",
            last_run_trigger="manual",
            last_run_finished_at=finished,
            last_run_duration_seconds=0.3,
            last_run_error_kind="scheduler_error",
            last_run_error_message="boom",
            last_run_error_traceback=tb_text,
        )
        payload = _build_payload(state)

        lr = payload["last_run"]
        assert lr["status"] == "error"
        assert lr["trigger"] == "manual"
        assert lr["error_kind"] == "scheduler_error"
        assert lr["error_message"] == "boom"
        assert lr["error_traceback"] == tb_text

    def test_manual_trigger(self):
        state = _make_runtime_state(
            last_run_trigger="manual", last_run_status="success"
        )
        payload = _build_payload(state)
        assert payload["last_run"]["trigger"] == "manual"

    def test_startup_trigger(self):
        state = _make_runtime_state(
            last_run_trigger="startup", last_run_status="success"
        )
        payload = _build_payload(state)
        assert payload["last_run"]["trigger"] == "startup"


# ════════════════════════════════════════════════════════════════════════════════
# build_scheduler_system_status_payload — controls and manual run
# ════════════════════════════════════════════════════════════════════════════════


class TestBuildSystemStatusPayloadControls:
    def test_no_controller_manual_run_unavailable(self):
        payload = _build_payload(_make_runtime_state(), controller=None)
        assert payload["controls"]["can_run_now"] is False
        assert payload["service"]["manual_run_available"] is False

    def test_controller_present_can_run(self):
        ctrl = MagicMock()
        ctrl.can_run_now.return_value = True
        payload = _build_payload(_make_runtime_state(), controller=ctrl)
        assert payload["controls"]["can_run_now"] is True
        assert payload["service"]["manual_run_available"] is True

    def test_controller_present_cannot_run(self):
        ctrl = MagicMock()
        ctrl.can_run_now.return_value = False
        payload = _build_payload(_make_runtime_state(), controller=ctrl)
        assert payload["controls"]["can_run_now"] is False

    def test_default_max_launched_jobs_from_config(self):
        payload = _build_payload(_make_runtime_state(), max_launched_jobs=42)
        assert payload["controls"]["default_max_launched_jobs"] == 42

    def test_runtime_file_path_included(self):
        payload = _build_payload(_make_runtime_state())
        assert "runtime_file" in payload
        assert "scheduler_runtime_state.json" in payload["runtime_file"]


# ════════════════════════════════════════════════════════════════════════════════
# build_scheduler_system_status_payload — countdown computation
# ════════════════════════════════════════════════════════════════════════════════


class TestBuildSystemStatusPayloadCountdown:
    def test_future_next_run_gives_positive_countdown(self):
        future = (datetime.now() + timedelta(minutes=5)).isoformat(timespec="seconds")
        state = _make_runtime_state(running=True, next_run_at=future)
        payload = _build_payload(state, service_running=True, service_pid=1)
        assert payload["service"]["countdown_seconds"] > 0

    def test_past_next_run_gives_zero_countdown(self):
        past = (datetime.now() - timedelta(minutes=1)).isoformat(timespec="seconds")
        state = _make_runtime_state(running=True, next_run_at=past)
        payload = _build_payload(state, service_running=True, service_pid=1)
        assert payload["service"]["countdown_seconds"] == 0

    def test_no_next_run_gives_none_countdown(self):
        state = _make_runtime_state(running=True, next_run_at=None)
        payload = _build_payload(state, service_running=True, service_pid=1)
        assert payload["service"]["countdown_seconds"] is None
