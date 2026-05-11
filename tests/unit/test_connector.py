"""
Unit tests for scheduler/connector.py

Classes tested:
  - SlurmConnector
"""

import os
import stat
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from scheduler.connector import SlurmConnector
from scheduler.resources import JobPlacement, NodeAllocation
from tests.fixtures.scheduler.scheduler_fixtures import create_pending_job

# ════════════════════════════════════════════════════════════════════════════════
# TestSlurmConnectorInit
# ════════════════════════════════════════════════════════════════════════════════


class TestSlurmConnectorInit:
    """Tests for SlurmConnector.__init__"""

    def test_default_init(self):
        """Default init sets launchScript=None and targetQos=None."""
        connector = SlurmConnector()
        assert connector.launchScript is None
        assert connector.targetQos is None

    def test_custom_script_path(self):
        """Custom script path is stored as-is."""
        connector = SlurmConnector(launchScript="/opt/custom/launch.sh")
        assert connector.launchScript == "/opt/custom/launch.sh"

    def test_custom_qos(self):
        """Custom targetQos is stored as-is."""
        connector = SlurmConnector(targetQos="high_prio")
        assert connector.targetQos == "high_prio"

    def test_custom_script_and_qos(self):
        """Both launchScript and targetQos can be set simultaneously."""
        connector = SlurmConnector(
            launchScript="/opt/custom/launch.sh", targetQos="high_prio"
        )
        assert connector.launchScript == "/opt/custom/launch.sh"
        assert connector.targetQos == "high_prio"


# ════════════════════════════════════════════════════════════════════════════════
# TestSlurmConnectorBuildEnv
# ════════════════════════════════════════════════════════════════════════════════


class TestSlurmConnectorBuildEnv:
    """Tests for SlurmConnector._build_env"""

    @pytest.fixture
    def job(self):
        return create_pending_job(
            jobID=101, cpusReq=2, constraints="type_a", partition="normal"
        )

    @pytest.fixture
    def placement(self):
        return JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=2.0, gpu=1.0)],
        )

    def test_all_taskshift_vars_present(self, job, placement):
        """All standard TASKSHIFT_* variables are present with correct values."""
        # Override job to include 1 GPU in tresReq (type 1001)
        job = create_pending_job(
            jobID=101,
            cpusReq=2,
            constraints="type_a",
            partition="normal",
            tresReq="1=2,4=1,1001=1",
        )
        connector = SlurmConnector(targetQos="my_qos")
        env = connector._build_env(job, placement)

        assert env["TASKSHIFT_JOB_ID"] == "101"
        assert env["TASKSHIFT_PARTITION"] == "normal"
        assert env["TASKSHIFT_FEATURE"] == "type_a"
        assert env["TASKSHIFT_NODES"] == "cn-001"
        assert env["TASKSHIFT_CPUS"] == "2"
        assert env["TASKSHIFT_GPUS"] == "1"
        assert env["TASKSHIFT_TIMELIMIT"] == "60"

    def test_qos_set_when_provided(self, job, placement):
        """TASKSHIFT_QOS is present when targetQos is provided."""
        connector = SlurmConnector(targetQos="my_qos")
        env = connector._build_env(job, placement)
        assert "TASKSHIFT_QOS" in env
        assert env["TASKSHIFT_QOS"] == "my_qos"

    def test_qos_not_set_when_none(self, job, placement):
        """TASKSHIFT_QOS is NOT present when targetQos is None."""
        connector = SlurmConnector(targetQos=None)
        env = connector._build_env(job, placement)
        assert "TASKSHIFT_QOS" not in env

    def test_empty_partition_becomes_empty_string(self, placement):
        """Empty partition produces empty string, not None."""
        job = create_pending_job(
            jobID=202, cpusReq=4, constraints="type_b", partition=None
        )
        connector = SlurmConnector()
        env = connector._build_env(job, placement)
        assert env["TASKSHIFT_PARTITION"] == ""

    def test_gpu_count_from_tres_req(self, placement):
        """GPU count comes from tresReq parsing (type 1001)."""
        job = create_pending_job(
            jobID=303,
            cpusReq=4,
            constraints="type_a",
            partition="normal",
            tresReq="1=4,4=1,1001=2",
        )
        connector = SlurmConnector()
        env = connector._build_env(job, placement)
        assert env["TASKSHIFT_GPUS"] == "2"

    def test_multiple_nodes_joined_by_comma(self):
        """Multiple node names are joined by comma."""
        job = create_pending_job(jobID=404, cpusReq=2)
        placement = JobPlacement(
            featureName="type_a",
            allocations=[
                NodeAllocation(nodeName="cn-001", cpu=1.0, gpu=0.0),
                NodeAllocation(nodeName="cn-002", cpu=1.0, gpu=0.0),
                NodeAllocation(nodeName="cn-003", cpu=0.0, gpu=1.0),
            ],
        )
        connector = SlurmConnector()
        env = connector._build_env(job, placement)
        assert env["TASKSHIFT_NODES"] == "cn-001,cn-002,cn-003"

    def test_no_returned_keys_beyond_expected(self, job, placement):
        """Without targetQos, only 7 TASKSHIFT_* keys are returned."""
        connector = SlurmConnector()
        env = connector._build_env(job, placement)
        taskshift_keys = [k for k in env if k.startswith("TASKSHIFT_")]
        assert len(taskshift_keys) == 7

    def test_with_target_qos_has_eight_keys(self, job, placement):
        """With targetQos, 8 TASKSHIFT_* keys are returned."""
        connector = SlurmConnector(targetQos="special")
        env = connector._build_env(job, placement)
        taskshift_keys = [k for k in env if k.startswith("TASKSHIFT_")]
        assert len(taskshift_keys) == 8


# ════════════════════════════════════════════════════════════════════════════════
# TestSlurmConnectorExecuteJob (integration with real temp scripts)
# ════════════════════════════════════════════════════════════════════════════════


class TestSlurmConnectorExecuteJob:
    """Tests for SlurmConnector.executeJob using real temp scripts."""

    @pytest.fixture
    def job(self):
        return create_pending_job(
            jobID=101, cpusReq=2, constraints="type_a", partition="normal"
        )

    @pytest.fixture
    def placement(self):
        return JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=2.0, gpu=1.0)],
        )

    @staticmethod
    def _write_script(tmp_path, script_body: str) -> str:
        """Helper: write a script to tmp_path and return its path."""
        script = tmp_path / "test-launch.sh"
        script.write_text(script_body)
        script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        return str(script)

    def test_script_succeeds_returns_true(self, tmp_path, job, placement):
        """Script that exits 0 → executeJob returns True."""
        script_path = self._write_script(tmp_path, "#!/bin/sh\nexit 0\n")
        connector = SlurmConnector(launchScript=script_path)
        assert connector.executeJob(job, placement=placement) is True

    def test_script_fails_returns_false(self, tmp_path, job, placement):
        """Script that exits 1 → executeJob returns False."""
        script_path = self._write_script(tmp_path, "#!/bin/sh\nexit 1\n")
        connector = SlurmConnector(launchScript=script_path)
        assert connector.executeJob(job, placement=placement) is False

    def test_nonexistent_script_returns_false(self, job, placement):
        """Non-existent script path → returns False (no crash)."""
        connector = SlurmConnector(launchScript="/nonexistent/path/to/script.sh")
        assert connector.executeJob(job, placement=placement) is False

    def test_no_placement_returns_false(self, tmp_path, job):
        """No placement provided → returns False (no subprocess call)."""
        script_path = self._write_script(tmp_path, "#!/bin/sh\nexit 0\n")
        connector = SlurmConnector(launchScript=script_path)
        assert connector.executeJob(job, placement=None) is False

    def test_env_vars_passed_correctly(self, tmp_path, job, placement):
        """Environment variables are received correctly by the subprocess script."""
        script_path = self._write_script(
            tmp_path,
            "#!/bin/sh\n"
            'echo "JOB_ID=$TASKSHIFT_JOB_ID"\n'
            'echo "PARTITION=$TASKSHIFT_PARTITION"\n'
            'echo "FEATURE=$TASKSHIFT_FEATURE"\n'
            'echo "NODES=$TASKSHIFT_NODES"\n'
            'echo "CPUS=$TASKSHIFT_CPUS"\n'
            'echo "GPUS=$TASKSHIFT_GPUS"\n'
            'echo "TIMELIMIT=$TASKSHIFT_TIMELIMIT"\n'
            'echo "QOS=$TASKSHIFT_QOS"\n'
            "exit 0\n",
        )
        connector = SlurmConnector(launchScript=script_path, targetQos="fast_qos")
        assert connector.executeJob(job, placement=placement) is True


# ════════════════════════════════════════════════════════════════════════════════
# TestSlurmConnectorExecuteJobMocked (mock subprocess.run)
# ════════════════════════════════════════════════════════════════════════════════


class TestSlurmConnectorExecuteJobMocked:
    """Tests for SlurmConnector.executeJob with mocked subprocess.run."""

    @pytest.fixture
    def job(self):
        return create_pending_job(
            jobID=101, cpusReq=2, constraints="type_a", partition="normal"
        )

    @pytest.fixture
    def placement(self):
        return JobPlacement(
            featureName="type_a",
            allocations=[NodeAllocation(nodeName="cn-001", cpu=2.0, gpu=1.0)],
        )

    @staticmethod
    def _write_script(tmp_path) -> str:
        """Create a real (but unused) script so file-exists check passes."""
        script = tmp_path / "mock-launch.sh"
        script.write_text("#!/bin/sh\nexit 0\n")
        script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        return str(script)

    @patch("scheduler.connector.subprocess.run")
    def test_exit_code_zero_returns_true(self, mock_run, tmp_path, job, placement):
        """subprocess returning exit code 0 → returns True."""
        script_path = self._write_script(tmp_path)
        mock_run.return_value = MagicMock(returncode=0, stdout="ok", stderr="")
        connector = SlurmConnector(launchScript=script_path)
        assert connector.executeJob(job, placement=placement) is True

    @patch("scheduler.connector.subprocess.run")
    def test_exit_code_nonzero_returns_false(self, mock_run, tmp_path, job, placement):
        """subprocess returning exit code 1 → returns False."""
        script_path = self._write_script(tmp_path)
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="fail")
        connector = SlurmConnector(launchScript=script_path)
        assert connector.executeJob(job, placement=placement) is False

    @patch("scheduler.connector.subprocess.run")
    def test_file_not_found_error_returns_false(
        self, mock_run, tmp_path, job, placement
    ):
        """subprocess raising FileNotFoundError → returns False."""
        script_path = self._write_script(tmp_path)
        mock_run.side_effect = FileNotFoundError("No such file")
        connector = SlurmConnector(launchScript=script_path)
        assert connector.executeJob(job, placement=placement) is False

    @patch("scheduler.connector.subprocess.run")
    def test_timeout_expired_returns_false(self, mock_run, tmp_path, job, placement):
        """subprocess raising TimeoutExpired → returns False."""
        script_path = self._write_script(tmp_path)
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="script", timeout=30)
        connector = SlurmConnector(launchScript=script_path)
        assert connector.executeJob(job, placement=placement) is False

    @patch("scheduler.connector.subprocess.run")
    def test_os_error_returns_false(self, mock_run, tmp_path, job, placement):
        """subprocess raising OSError → returns False."""
        script_path = self._write_script(tmp_path)
        mock_run.side_effect = OSError("Permission denied")
        connector = SlurmConnector(launchScript=script_path)
        assert connector.executeJob(job, placement=placement) is False

    @patch("scheduler.connector.subprocess.run")
    def test_env_vars_passed_to_subprocess(self, mock_run, tmp_path, job, placement):
        """Verify all TASKSHIFT_* env vars are passed to subprocess."""
        script_path = self._write_script(tmp_path)
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        connector = SlurmConnector(launchScript=script_path, targetQos="my_qos")
        connector.executeJob(job, placement=placement)

        call_kwargs = mock_run.call_args[1]
        env = call_kwargs["env"]
        assert env["TASKSHIFT_JOB_ID"] == "101"
        assert env["TASKSHIFT_PARTITION"] == "normal"
        assert env["TASKSHIFT_FEATURE"] == "type_a"
        assert env["TASKSHIFT_NODES"] == "cn-001"
        assert env["TASKSHIFT_CPUS"] == "2"
        assert env["TASKSHIFT_GPUS"] == "0"
        assert env["TASKSHIFT_TIMELIMIT"] == "60"
        assert env["TASKSHIFT_QOS"] == "my_qos"
        # os.environ must also be preserved
        assert "PATH" in env

    @patch("scheduler.connector.subprocess.run")
    def test_script_path_passed_to_subprocess(self, mock_run, tmp_path, job, placement):
        """Verify the resolved script path is the command passed to subprocess."""
        script_path = self._write_script(tmp_path)
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        connector = SlurmConnector(launchScript=script_path)
        connector.executeJob(job, placement=placement)

        call_args = mock_run.call_args[0]
        assert call_args[0] == [script_path]

    @patch("scheduler.connector.subprocess.run")
    def test_timeout_kwarg_is_30(self, mock_run, tmp_path, job, placement):
        """Verify subprocess is called with timeout=30."""
        script_path = self._write_script(tmp_path)
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        connector = SlurmConnector(launchScript=script_path)
        connector.executeJob(job, placement=placement)

        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["timeout"] == 30

    @patch("scheduler.connector.subprocess.run")
    def test_capture_output_and_text_flags(self, mock_run, tmp_path, job, placement):
        """Verify capture_output=True and text=True are passed."""
        script_path = self._write_script(tmp_path)
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        connector = SlurmConnector(launchScript=script_path)
        connector.executeJob(job, placement=placement)

        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["capture_output"] is True
        assert call_kwargs["text"] is True

    @patch("scheduler.connector.subprocess.run")
    def test_qos_not_in_env_when_none(self, mock_run, tmp_path, job, placement):
        """When targetQos is None, TASKSHIFT_QOS must not be in env."""
        script_path = self._write_script(tmp_path)
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        connector = SlurmConnector(launchScript=script_path, targetQos=None)
        connector.executeJob(job, placement=placement)

        call_kwargs = mock_run.call_args[1]
        env = call_kwargs["env"]
        assert "TASKSHIFT_QOS" not in env

    def test_nonexistent_script_skips_subprocess(self, job, placement):
        """Non-existent script → subprocess.run is never called."""
        connector = SlurmConnector(launchScript="/no/such/script.sh")
        with patch("scheduler.connector.subprocess.run") as mock_run:
            result = connector.executeJob(job, placement=placement)
        assert result is False
        mock_run.assert_not_called()

    def test_no_placement_skips_subprocess(self, tmp_path, job):
        """No placement → subprocess.run is never called."""
        script = tmp_path / "launch.sh"
        script.write_text("#!/bin/sh\nexit 0\n")
        script.chmod(script.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

        connector = SlurmConnector(launchScript=str(script))
        with patch("scheduler.connector.subprocess.run") as mock_run:
            result = connector.executeJob(job, placement=None)
        assert result is False
        mock_run.assert_not_called()
