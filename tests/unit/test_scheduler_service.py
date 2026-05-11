"""Unit tests for scheduler.service module - Scheduler class"""

from unittest.mock import MagicMock, patch

from scheduler.attempt_cache import reset_cache, save_launch_attempts
from scheduler.resources import ResourceAvailabilityTree
from scheduler.service import Scheduler
from tests.fixtures.scheduler.scheduler_fixtures import (
    TIMESTAMP_NOW,
    build_mini_cluster_config,
    build_mini_cluster_config_mock,
    create_pending_job,
    create_running_gpu_job,
)

# ════════════════════════════════════════════════════════════════════════════════
# Helper: create a Scheduler without touching real filesystem / config
# ════════════════════════════════════════════════════════════════════════════════


def _make_scheduler(
    mock_get_cluster_config,
    mock_append_event,
    mock_storage,
    mock_connector,
    timelimit=60,
    max_launched=None,
):
    """Build a Scheduler with mocked-out cluster config and event logger."""
    mock_get_cluster_config.return_value = build_mini_cluster_config_mock()

    scheduler_config = MagicMock()
    scheduler_config.timelimit = timelimit
    scheduler_config.max_launched_jobs = max_launched

    return Scheduler(
        storage=mock_storage,
        connector=mock_connector,
        forecastDataDir=None,
        schedulerConfig=scheduler_config,
    )


# ════════════════════════════════════════════════════════════════════════════════
# 1. TestCalculateRequestedPercent
# ════════════════════════════════════════════════════════════════════════════════


@patch("scheduler.service.append_job_launch_event")
@patch("scheduler.service.getClusterConfig")
class TestCalculateRequestedPercent:
    def test_zero_requested(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()
        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )

        assert scheduler._calculateRequestedPercent(0, 100) == 0.0

    def test_fifty_percent(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()
        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )

        assert scheduler._calculateRequestedPercent(50, 100) == 50.0

    def test_zero_total_returns_none(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()
        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )

        assert scheduler._calculateRequestedPercent(50, 0) is None

    def test_full_capacity(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()
        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )

        assert scheduler._calculateRequestedPercent(100, 100) == 100.0


# ════════════════════════════════════════════════════════════════════════════════
# 2. TestBuildPendingJobPayload
# ════════════════════════════════════════════════════════════════════════════════


@patch("scheduler.service.append_job_launch_event")
@patch("scheduler.service.getClusterConfig")
class TestBuildPendingJobPayload:
    def test_basic_fields(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()
        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )

        job = create_pending_job(jobID=42, jobName="test", cpusReq=4)
        result = scheduler._build_pending_job_payload(job)

        assert result["job_id"] == 42
        assert result["job_name"] == "test"
        assert result["requested_cpus"] == 4
        assert result["status"] == "PENDING"
        assert result["was_attempted"] is False
        assert result["in_failed_attempt_pool"] is False

    def test_gpu_and_partition_fields(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()
        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )

        job = create_pending_job(
            jobID=99,
            jobName="gpu_test",
            cpusReq=8,
            tresReq="1=8,4=2,1001=4",
            partition="gpu_only",
        )
        result = scheduler._build_pending_job_payload(job)

        assert result["job_id"] == 99
        assert result["partition"] == "gpu_only"
        assert result["requested_cpus"] == 8
        assert result["requested_gpus"] == 4
        assert result["requested_nodes"] == 2

    def test_defaults_for_no_gpu(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()
        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )

        job = create_pending_job(jobID=1, cpusReq=2, tresReq="1=2,4=1")
        result = scheduler._build_pending_job_payload(job)

        assert result["requested_gpus"] == 0
        assert result["timelimit_minutes"] == 60
        assert result["constraints"] == "type_a"


# ════════════════════════════════════════════════════════════════════════════════
# 3. TestSchedulerSchedule
# ════════════════════════════════════════════════════════════════════════════════


@patch("scheduler.service.append_job_launch_event")
@patch("scheduler.service.getClusterConfig")
class TestSchedulerSchedule:
    def setup_method(self):
        reset_cache()

    # (a) No pending jobs → launched=0
    def test_no_pending_jobs(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_storage.getPendingJobs.return_value = []
        mock_storage.getRunningJobs.return_value = []
        mock_connector = MagicMock()

        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )
        result = scheduler.schedule()

        assert result["launched_count"] == 0
        assert result["pending_job_count"] == 0
        assert result["skipped_by_timelimit"] == 0
        assert result["skipped_by_resources"] == 0
        assert result["skipped_by_failed_attempt_pool"] == 0
        mock_connector.executeJob.assert_not_called()

    # (b) Single job fits → launched=1
    def test_single_job_fits(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        job = create_pending_job(jobID=101, cpusReq=2, timelimit=30)
        mock_storage.getPendingJobs.return_value = [job]
        mock_storage.getRunningJobs.return_value = []

        scheduler = _make_scheduler(
            mock_get_cluster_config,
            mock_append_event,
            mock_storage,
            mock_connector,
            timelimit=60,
        )
        result = scheduler.schedule()

        assert result["launched_count"] == 1
        assert result["attempted_job_ids"] == [101]
        mock_connector.executeJob.assert_called_once_with(job)

    # (c) Two jobs fit → launched=2
    def test_two_jobs_fit(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        job1 = create_pending_job(jobID=101, cpusReq=2, timelimit=30)
        job2 = create_pending_job(jobID=102, cpusReq=2, timelimit=30)
        mock_storage.getPendingJobs.return_value = [job1, job2]
        mock_storage.getRunningJobs.return_value = []

        scheduler = _make_scheduler(
            mock_get_cluster_config,
            mock_append_event,
            mock_storage,
            mock_connector,
            timelimit=60,
        )
        result = scheduler.schedule()

        assert result["launched_count"] == 2
        assert result["attempted_job_ids"] == [101, 102]
        assert mock_connector.executeJob.call_count == 2

    # (d) Job exceeds timelimit → skipped_by_timelimit=1
    def test_job_exceeds_timelimit(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        job = create_pending_job(jobID=201, timelimit=1440)
        mock_storage.getPendingJobs.return_value = [job]
        mock_storage.getRunningJobs.return_value = []

        scheduler = _make_scheduler(
            mock_get_cluster_config,
            mock_append_event,
            mock_storage,
            mock_connector,
            timelimit=60,
        )
        result = scheduler.schedule()

        assert result["launched_count"] == 0
        assert result["skipped_by_timelimit"] == 1
        mock_connector.executeJob.assert_not_called()
        # Verify the pending_jobs payload reflects the skip
        entry = result["pending_jobs"][0]
        assert entry["status"] == "SKIPPED_TIMELIMIT"

    # (e) max_launched_jobs=1 → only 1 launched
    def test_max_launched_jobs_limit(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        job1 = create_pending_job(jobID=301, cpusReq=2, timelimit=30)
        job2 = create_pending_job(jobID=302, cpusReq=2, timelimit=30)
        mock_storage.getPendingJobs.return_value = [job1, job2]
        mock_storage.getRunningJobs.return_value = []

        scheduler = _make_scheduler(
            mock_get_cluster_config,
            mock_append_event,
            mock_storage,
            mock_connector,
            timelimit=60,
            max_launched=1,
        )
        result = scheduler.schedule()

        assert result["launched_count"] == 1
        assert result["attempted_job_ids"] == [301]
        assert result["effective_max_launched_jobs"] == 1
        mock_connector.executeJob.assert_called_once()

    # (f) Running job consumes all resources of a feature → pending job for that
    #     feature is skipped; but a job on another feature can still fit
    def test_running_jobs_consume_resources(
        self, mock_get_cluster_config, mock_append_event
    ):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        # Running job on ALL type_a nodes: cn-001..cn-004, each fully loaded
        # type_a: 4 CPU / 2 GPU per node → 16 CPU / 8 GPU total
        running_a1 = create_running_gpu_job(
            jobID=5001, cpusReq=8, gpusRequested=4, nodelist="cn-[001-002]"
        )
        running_a2 = create_running_gpu_job(
            jobID=5002, cpusReq=8, gpusRequested=4, nodelist="cn-[003-004]"
        )
        mock_storage.getRunningJobs.return_value = [running_a1, running_a2]

        # Pending job wants type_a (the only feature in its constraints)
        # All type_a CPU/GPU are consumed → no resources
        pending = create_pending_job(
            jobID=401, cpusReq=4, constraints="type_a", tresReq="1=4,4=1,1001=1"
        )
        mock_storage.getPendingJobs.return_value = [pending]

        scheduler = _make_scheduler(
            mock_get_cluster_config,
            mock_append_event,
            mock_storage,
            mock_connector,
            timelimit=60,
        )
        result = scheduler.schedule()

        assert result["launched_count"] == 0
        assert result["skipped_by_resources"] == 1
        mock_connector.executeJob.assert_not_called()

    # (g) Mixed: one fits, one too long, one no resources
    def test_mixed_scenario(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        # Running job consuming all type_a GPUs: 4 GPUs on cn-001
        running = create_running_gpu_job(
            jobID=6001, cpusReq=4, gpusRequested=8, nodelist="cn-[001-004]"
        )
        mock_storage.getRunningJobs.return_value = [running]

        # Job A: fits on type_b (no GPU needed, enough CPU)
        job_a = create_pending_job(
            jobID=701, cpusReq=4, constraints="type_b", tresReq="1=4,4=1"
        )

        # Job B: exceeds timelimit
        job_b = create_pending_job(jobID=702, cpusReq=2, timelimit=1440)

        # Job C: needs GPU on type_a but all GPUs consumed
        job_c = create_pending_job(
            jobID=703, cpusReq=4, constraints="type_a", tresReq="1=4,4=1,1001=2"
        )

        mock_storage.getPendingJobs.return_value = [job_a, job_b, job_c]

        scheduler = _make_scheduler(
            mock_get_cluster_config,
            mock_append_event,
            mock_storage,
            mock_connector,
            timelimit=60,
        )
        result = scheduler.schedule()

        assert result["launched_count"] == 1
        assert result["attempted_job_ids"] == [701]
        assert result["skipped_by_timelimit"] == 1
        assert result["skipped_by_resources"] == 1


# ════════════════════════════════════════════════════════════════════════════════
# 4. TestSchedulerReconcileLaunchAttempts
# ════════════════════════════════════════════════════════════════════════════════


@patch("scheduler.service.append_job_launch_event")
@patch("scheduler.service.getClusterConfig")
class TestSchedulerReconcileLaunchAttempts:
    def setup_method(self):
        reset_cache()

    # (a) No previous attempts → failed pool empty
    def test_no_previous_attempts(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_storage.getPendingJobs.return_value = []
        mock_storage.getRunningJobs.return_value = []
        mock_connector = MagicMock()

        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )
        result = scheduler.schedule()

        assert result["failed_job_pool_size"] == 0
        assert result["skipped_by_failed_attempt_pool"] == 0

    # (b) Previous attempt, job still pending → job goes to failed pool
    def test_previous_attempt_job_still_pending(
        self, mock_get_cluster_config, mock_append_event
    ):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        # Save a launch attempt from a "previous" scheduler tick
        save_launch_attempts(
            [
                {
                    "job_id": 1001,
                    "status": "LAUNCH_ATTEMPTED",
                }
            ]
        )

        job = create_pending_job(jobID=1001, cpusReq=2, timelimit=30)
        mock_storage.getPendingJobs.return_value = [job]
        mock_storage.getRunningJobs.return_value = []

        scheduler = _make_scheduler(
            mock_get_cluster_config,
            mock_append_event,
            mock_storage,
            mock_connector,
            timelimit=60,
        )
        result = scheduler.schedule()

        # Job should be in the failed pool and skipped
        assert result["skipped_by_failed_attempt_pool"] == 1
        assert result["failed_job_pool_size"] == 1
        assert result["launched_count"] == 0
        mock_connector.executeJob.assert_not_called()

        # The pending_jobs payload should reflect the blocked status
        entry = result["pending_jobs"][0]
        assert entry["status"] == "BLOCKED_FAILED_POOL"
        assert entry["in_failed_attempt_pool"] is True

    # (c) Previous attempt, job left queue → status LEFT_PENDING_QUEUE
    def test_previous_attempt_job_left_queue(
        self, mock_get_cluster_config, mock_append_event
    ):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        save_launch_attempts(
            [
                {
                    "job_id": 1001,
                    "status": "LAUNCH_ATTEMPTED",
                }
            ]
        )

        # Job 1001 is NO LONGER pending — it left the queue
        mock_storage.getPendingJobs.return_value = []
        mock_storage.getRunningJobs.return_value = []

        scheduler = _make_scheduler(
            mock_get_cluster_config, mock_append_event, mock_storage, mock_connector
        )
        result = scheduler.schedule()

        # Failed pool should still be empty (job left queue, so no penalty)
        assert result["failed_job_pool_size"] == 0
        assert result["skipped_by_failed_attempt_pool"] == 0
        # The reconciled event should have been logged with LEFT_PENDING_QUEUE
        assert mock_append_event.call_count == 1
        reconciled_call = mock_append_event.call_args
        assert reconciled_call[0][0]["status"] == "LEFT_PENDING_QUEUE"

    # (d) Integration: attempt → job stays pending → next schedule skips it
    def test_integration_attempt_then_skip_on_next_tick(
        self, mock_get_cluster_config, mock_append_event
    ):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        # ── First tick: launch the job ──
        job = create_pending_job(jobID=1001, cpusReq=2, timelimit=30)
        mock_storage.getPendingJobs.return_value = [job]
        mock_storage.getRunningJobs.return_value = []

        scheduler = _make_scheduler(
            mock_get_cluster_config,
            mock_append_event,
            mock_storage,
            mock_connector,
            timelimit=60,
        )
        result1 = scheduler.schedule()

        assert result1["launched_count"] == 1
        assert result1["attempted_job_ids"] == [1001]
        mock_connector.executeJob.assert_called_once_with(job)

        # ── Second tick: job is still pending (slurm didn't start it) ──
        mock_connector.reset_mock()
        mock_storage.getPendingJobs.return_value = [job]

        result2 = scheduler.schedule()

        assert result2["launched_count"] == 0
        assert result2["skipped_by_failed_attempt_pool"] == 1
        assert result2["failed_job_pool_size"] == 1
        mock_connector.executeJob.assert_not_called()


# ════════════════════════════════════════════════════════════════════════════════
# 5. TestFindRunnablePlacement
# ════════════════════════════════════════════════════════════════════════════════


@patch("scheduler.service.append_job_launch_event")
@patch("scheduler.service.getClusterConfig")
class TestFindRunnablePlacement:
    # (a) Job fits → placement returned
    def test_job_fits(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        cluster_config = build_mini_cluster_config()
        mock_get_cluster_config.return_value = cluster_config

        scheduler_config = MagicMock()
        scheduler_config.timelimit = 60
        scheduler_config.max_launched_jobs = None
        scheduler = Scheduler(
            storage=mock_storage,
            connector=mock_connector,
            forecastDataDir=None,
            schedulerConfig=scheduler_config,
        )

        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [], TIMESTAMP_NOW
        )

        job = create_pending_job(cpusReq=2, constraints="type_a")
        placement = scheduler._findRunnablePlacement(job, tree, TIMESTAMP_NOW)

        assert placement is not None
        assert placement.featureName == "type_a"
        assert len(placement.nodeNames) >= 1

    # (b) Job needs too many GPUs → None
    def test_too_many_gpus(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        cluster_config = build_mini_cluster_config()
        mock_get_cluster_config.return_value = cluster_config

        scheduler_config = MagicMock()
        scheduler_config.timelimit = 60
        scheduler_config.max_launched_jobs = None
        scheduler = Scheduler(
            storage=mock_storage,
            connector=mock_connector,
            forecastDataDir=None,
            schedulerConfig=scheduler_config,
        )

        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [], TIMESTAMP_NOW
        )

        # type_a has only 8 GPU total across 4 nodes — 100 GPUs is impossible
        job = create_pending_job(
            cpusReq=2, constraints="type_a", tresReq="1=2,4=1,1001=100"
        )
        placement = scheduler._findRunnablePlacement(job, tree, TIMESTAMP_NOW)

        assert placement is None

    # (c) Job on unknown partition → None
    def test_unknown_partition(self, mock_get_cluster_config, mock_append_event):
        mock_storage = MagicMock()
        mock_connector = MagicMock()

        cluster_config = build_mini_cluster_config()
        mock_get_cluster_config.return_value = cluster_config

        scheduler_config = MagicMock()
        scheduler_config.timelimit = 60
        scheduler_config.max_launched_jobs = None
        scheduler = Scheduler(
            storage=mock_storage,
            connector=mock_connector,
            forecastDataDir=None,
            schedulerConfig=scheduler_config,
        )

        tree = ResourceAvailabilityTree.fromClusterAndJobs(
            cluster_config, [], TIMESTAMP_NOW
        )

        job = create_pending_job(cpusReq=2, partition="nonexistent_partition")
        placement = scheduler._findRunnablePlacement(job, tree, TIMESTAMP_NOW)

        assert placement is None
