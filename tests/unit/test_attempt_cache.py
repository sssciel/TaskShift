import time

import scheduler.attempt_cache as cache_module

# ---------------------------------------------------------------------------
# Fixtures (plain helpers called per-test for isolation)
# ---------------------------------------------------------------------------


def _reset():
    """Wipe module-level state so every test starts clean."""
    cache_module.reset_cache()


# ---------------------------------------------------------------------------
# 1. TestResetCache
# ---------------------------------------------------------------------------


class TestResetCache:
    def test_launch_attempts_empty_after_reset(self):
        cache_module.save_launch_attempts([{"job_id": 1}])
        _reset()
        assert cache_module.load_launch_attempts() == []

    def test_failed_job_pool_empty_after_reset(self):
        cache_module.save_failed_job_pool([1, 2, 3])
        _reset()
        assert cache_module.load_failed_job_pool() == set()

    def test_multiple_resets_are_idempotent(self):
        _reset()
        _reset()
        _reset()
        assert cache_module.load_launch_attempts() == []
        assert cache_module.load_failed_job_pool() == set()


# ---------------------------------------------------------------------------
# 2. TestLaunchAttempts
# ---------------------------------------------------------------------------


class TestLaunchAttempts:
    def setup_method(self):
        _reset()

    def test_save_then_load_returns_same_data(self):
        data = [{"job_id": 1, "status": "ok"}, {"job_id": 2}]
        cache_module.save_launch_attempts(data)
        assert cache_module.load_launch_attempts() == data

    def test_save_empty_list(self):
        cache_module.save_launch_attempts([])
        assert cache_module.load_launch_attempts() == []

    def test_save_overwrites_previous_data(self):
        cache_module.save_launch_attempts([{"job_id": 1}])
        cache_module.save_launch_attempts([{"job_id": 2}, {"job_id": 3}])
        assert cache_module.load_launch_attempts() == [{"job_id": 2}, {"job_id": 3}]

    def test_load_returns_copy(self):
        """Modifying the returned list must not affect internal state."""
        cache_module.save_launch_attempts([{"job_id": 1}])
        loaded = cache_module.load_launch_attempts()
        loaded.append({"job_id": 999})
        assert cache_module.load_launch_attempts() == [{"job_id": 1}]

    def test_save_stores_copy(self):
        """Modifying the input list after saving must not affect internal state."""
        data = [{"job_id": 1}]
        cache_module.save_launch_attempts(data)
        data.append({"job_id": 999})
        assert cache_module.load_launch_attempts() == [{"job_id": 1}]


# ---------------------------------------------------------------------------
# 3. TestFailedJobPool
# ---------------------------------------------------------------------------


class TestFailedJobPool:
    def setup_method(self):
        _reset()

    def test_save_set_then_load_returns_same_data(self):
        pool = {10, 20, 30}
        cache_module.save_failed_job_pool(pool)
        assert cache_module.load_failed_job_pool() == pool

    def test_save_list_then_load_returns_set(self):
        cache_module.save_failed_job_pool([1, 2, 3])
        assert isinstance(cache_module.load_failed_job_pool(), set)
        assert cache_module.load_failed_job_pool() == {1, 2, 3}

    def test_save_overwrites_previous_data(self):
        cache_module.save_failed_job_pool([1, 2, 3])
        cache_module.save_failed_job_pool({4, 5})
        assert cache_module.load_failed_job_pool() == {4, 5}

    def test_save_normalizes_to_int(self):
        cache_module.save_failed_job_pool(["1", 2, 3.0])
        assert cache_module.load_failed_job_pool() == {1, 2, 3}

    def test_load_returns_copy(self):
        """Modifying the returned set must not affect internal state."""
        cache_module.save_failed_job_pool([1, 2])
        loaded = cache_module.load_failed_job_pool()
        loaded.add(999)
        assert cache_module.load_failed_job_pool() == {1, 2}

    def test_save_empty_set(self):
        cache_module.save_failed_job_pool(set())
        assert cache_module.load_failed_job_pool() == set()

    def test_save_with_duplicates_deduped(self):
        cache_module.save_failed_job_pool([1, 2, 2, 3, 3, 3])
        assert cache_module.load_failed_job_pool() == {1, 2, 3}

    def test_reset_failed_job_pool_only_clears_failed_pool(self):
        cache_module.save_launch_attempts([{"job_id": 1}])
        cache_module.save_failed_job_pool([1, 2, 3])
        cache_module.reset_failed_job_pool()
        assert cache_module.load_failed_job_pool() == set()
        assert cache_module.load_launch_attempts() == [{"job_id": 1}]


# ---------------------------------------------------------------------------
# 4. TestCleanupInterval
# ---------------------------------------------------------------------------


class TestCleanupInterval:
    def setup_method(self):
        _reset()

    def test_no_cleanup_on_first_operation(self):
        """First operation initialises _initialized_at; no data is cleared."""
        cache_module.save_launch_attempts([{"job_id": 1}])
        cache_module.save_failed_job_pool([5, 6])
        # Both should still be present — cleanup did NOT run.
        assert cache_module.load_launch_attempts() == [{"job_id": 1}]
        assert cache_module.load_failed_job_pool() == {5, 6}

    def test_cleanup_clears_data_when_initialized_at_is_old(self):
        """When _initialized_at is old enough, the next I/O op triggers cleanup."""
        cache_module.save_launch_attempts([{"job_id": 1}])
        cache_module.save_failed_job_pool([5, 6])

        # Pretend the cache was initialized a very long time ago.
        cache_module._initialized_at = 0.0

        # Any load/save should now trigger cleanup.
        assert cache_module.load_launch_attempts() == []
        assert cache_module.load_failed_job_pool() == set()

    def test_new_data_can_be_stored_after_cleanup(self):
        cache_module.save_launch_attempts([{"job_id": 1}])
        cache_module._initialized_at = 0.0  # force cleanup on next call

        # Trigger cleanup via a load.
        cache_module.load_launch_attempts()  # returns []

        # Store new data — it must survive.
        cache_module.save_launch_attempts([{"job_id": 42}])
        assert cache_module.load_launch_attempts() == [{"job_id": 42}]

        cache_module.save_failed_job_pool([7, 8, 9])
        assert cache_module.load_failed_job_pool() == {7, 8, 9}

    def test_cleanup_status_without_initialized_cache(self):
        status = cache_module.get_failed_job_pool_cleanup_status()
        assert status["cleanup_interval_seconds"] == cache_module.CLEANUP_INTERVAL_SECONDS
        assert status["initialized_at"] is None
        assert status["next_cleanup_at"] is None

    def test_cleanup_status_with_initialized_cache(self):
        cache_module.save_failed_job_pool([5, 6])
        status = cache_module.get_failed_job_pool_cleanup_status()
        assert status["cleanup_interval_seconds"] == cache_module.CLEANUP_INTERVAL_SECONDS
        assert status["initialized_at"] is not None
        assert status["next_cleanup_at"] is not None
