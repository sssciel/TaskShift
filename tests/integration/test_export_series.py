"""Integration tests for historical utilization series export.

These tests exercise the full pipeline (synthetic data → cache sync →
series build → file export) **without** requiring a live SLURM database.
MySQL is bypassed entirely via the ``jobsOverride`` parameter accepted by
:py:meth:`slurmStorage.syncHistoricalJobsCache` and
:py:meth:`slurmStorage.exportIncrementalHistoricalUtilization`.
"""

import json
from datetime import datetime

from storage.cache import load_cached_historical_job_rows
from storage.models import HistoricalJob
from storage.series import (
    build_historical_utilization_series,
    export_historical_utilization_series,
)
from storage.service import slurmStorage
from tests.fixtures.scheduler.scheduler_fixtures import build_mini_cluster_config
from tests.integration.synthetic_data import (
    BASE_TIME,
    build_incremental_dataset,
    build_standard_test_dataset,
)

# ─── Series-building tests ──────────────────────────────────────────────────────


class TestExportSeriesBuild:
    """Tests for :func:`storage.series.build_historical_utilization_series`."""

    def test_completed_jobs_produce_series(self, tmp_path):
        """Completed jobs with nodes produce non-empty utilization series."""
        rows = build_standard_test_dataset()
        jobs = [row.toHistoricalJob() for row in rows]
        config = build_mini_cluster_config()

        series = build_historical_utilization_series(
            jobs=jobs,
            clusterConfig=config,
            intervalMinutes=15,
        )

        # Should have series for every feature + overall
        for feature in ["type_a", "type_b", "type_d", "overall"]:
            assert feature in series, f"Missing feature '{feature}' in series"

        # type_a: 6 jobs (2 cpu + 1 gpu + 1 multi-node + 1 running cpu + 1 running gpu)
        assert len(series["type_a"]) > 0
        # type_b: 2 jobs (1 completed cpu + 1 failed)
        assert len(series["type_b"]) > 0
        # type_d: 1 completed GPU job
        assert len(series["type_d"]) > 0

    def test_series_values_are_percentages(self, tmp_path):
        """All series values should be between 0 % and 100 %."""
        rows = build_standard_test_dataset()
        jobs = [row.toHistoricalJob() for row in rows]
        config = build_mini_cluster_config()

        series = build_historical_utilization_series(
            jobs=jobs,
            clusterConfig=config,
            intervalMinutes=15,
        )

        for feature, points in series.items():
            for point in points:
                assert 0.0 <= point["cpu"] <= 100.0, f"{feature} cpu={point['cpu']}"
                assert 0.0 <= point["gpu"] <= 100.0, f"{feature} gpu={point['gpu']}"

    def test_unstarted_jobs_excluded(self, tmp_path):
        """Pending and cancelled jobs (time_start=0) don't appear in series."""
        rows = build_standard_test_dataset()
        unstarted = [r for r in rows if r.time_start == 0]
        jobs = [row.toHistoricalJob() for row in unstarted]
        config = build_mini_cluster_config()

        series = build_historical_utilization_series(
            jobs=jobs,
            clusterConfig=config,
            intervalMinutes=15,
        )

        for feature, points in series.items():
            assert len(points) == 0, (
                f"{feature} should have no points for unstarted jobs"
            )

    def test_series_has_correct_time_range(self, tmp_path):
        """Series time range covers all job timestamps."""
        fmt = "%H:%M:%S %d.%m.%y"

        rows = build_standard_test_dataset()
        jobs = [row.toHistoricalJob() for row in rows]
        config = build_mini_cluster_config()

        series = build_historical_utilization_series(
            jobs=jobs,
            clusterConfig=config,
            intervalMinutes=15,
        )

        type_a_series = series["type_a"]
        assert len(type_a_series) >= 2

        first_time = datetime.strptime(type_a_series[0]["time"], fmt)
        last_time = datetime.strptime(type_a_series[-1]["time"], fmt)
        assert first_time < last_time

    def test_15_minute_intervals(self, tmp_path):
        """Series points are spaced at 15-minute intervals."""
        fmt = "%H:%M:%S %d.%m.%y"

        rows = build_standard_test_dataset()
        jobs = [row.toHistoricalJob() for row in rows]
        config = build_mini_cluster_config()

        series = build_historical_utilization_series(
            jobs=jobs,
            clusterConfig=config,
            intervalMinutes=15,
        )

        type_a = series["type_a"]
        if len(type_a) >= 2:
            times = [datetime.strptime(p["time"], fmt) for p in type_a]
            for i in range(1, len(times)):
                diff = (times[i] - times[i - 1]).total_seconds()
                assert diff == 900, f"Interval {i}: {diff}s != 900s"


# ─── File-export tests ──────────────────────────────────────────────────────────


class TestExportSeriesFiles:
    """Tests for :func:`storage.series.export_historical_utilization_series`."""

    def test_exports_json_files_per_feature(self, tmp_path):
        """Export creates one JSON file per feature plus metadata."""
        rows = build_standard_test_dataset()
        jobs = [row.toHistoricalJob() for row in rows]
        config = build_mini_cluster_config()

        output_dir = tmp_path / "series"
        result = export_historical_utilization_series(
            outputDir=output_dir,
            jobs=jobs,
            clusterConfig=config,
            intervalMinutes=15,
        )

        assert result.exists()

        # Check per-feature files
        for feature in ["type_a", "type_b", "type_d", "overall"]:
            feature_file = output_dir / f"{feature}.json"
            assert feature_file.exists(), f"Missing {feature_file}"

            with open(feature_file) as f:
                data = json.load(f)
            assert isinstance(data, list)

        # Check metadata written by export_historical_utilization_series
        metadata_file = output_dir / "metadata.json"
        assert metadata_file.exists()
        with open(metadata_file) as f:
            metadata = json.load(f)
        assert "features" in metadata
        assert "interval_minutes" in metadata
        assert metadata["interval_minutes"] == 15

    def test_exported_series_matches_built(self, tmp_path):
        """Exported JSON content matches build_historical_utilization_series output."""
        rows = build_standard_test_dataset()
        jobs = [row.toHistoricalJob() for row in rows]
        config = build_mini_cluster_config()

        series = build_historical_utilization_series(
            jobs=jobs,
            clusterConfig=config,
            intervalMinutes=15,
        )

        output_dir = tmp_path / "series"
        export_historical_utilization_series(
            outputDir=output_dir,
            jobs=jobs,
            clusterConfig=config,
            intervalMinutes=15,
        )

        for feature in ["type_a", "type_b", "type_d", "overall"]:
            feature_file = output_dir / f"{feature}.json"
            with open(feature_file) as f:
                file_data = json.load(f)
            assert file_data == series[feature]


# ─── Incremental-sync tests ─────────────────────────────────────────────────────


class TestIncrementalExport:
    """Tests for the incremental cache-sync + export pipeline.

    The :class:`slurmStorage` constructor creates a
    :class:`SlurmDBRepository` **but** ``.create()`` is never called, so
    no MySQL connection is opened.  All database queries are bypassed via
    the ``jobsOverride`` parameter.
    """

    def test_first_export_creates_cache_and_series(self, tmp_path):
        """First export creates raw cache, state file, and series files."""
        rows = build_standard_test_dataset()
        config = build_mini_cluster_config()

        storage = slurmStorage()
        # Do NOT call storage.create() — no MySQL needed

        materialized_jobs, output_path, state = storage.syncHistoricalJobsCache(
            outputDir=tmp_path,
            jobsOverride=rows,
        )

        # Cache file should exist
        cache_file = tmp_path / "raw_job_rows.json"
        assert cache_file.exists()

        # State file should exist
        state_file = tmp_path / "state.json"
        assert state_file.exists()

        # State should have last_mod_time
        assert state["last_mod_time"] > 0
        assert state["job_count"] > 0

        # All rows should be in the raw cache
        loaded_rows = load_cached_historical_job_rows(cache_file)
        assert len(loaded_rows) == len(rows)

    def test_incremental_export_adds_new_jobs(self, tmp_path):
        """Second export with new jobs appends them without duplicates."""
        rows_initial = build_standard_test_dataset()
        config = build_mini_cluster_config()

        storage = slurmStorage()

        # ── First export ──
        storage.syncHistoricalJobsCache(
            outputDir=tmp_path,
            jobsOverride=rows_initial,
        )

        # ── Second export with additional jobs ──
        rows_incremental = build_incremental_dataset()
        all_rows = rows_initial + rows_incremental

        storage.syncHistoricalJobsCache(
            outputDir=tmp_path,
            jobsOverride=rows_incremental,  # Only NEW rows
        )

        # Cache should now have ALL jobs (initial + incremental)
        cache_file = tmp_path / "raw_job_rows.json"
        loaded_rows = load_cached_historical_job_rows(cache_file)

        assert len(loaded_rows) == len(all_rows)

        # Verify no duplicates by checking unique job_db_inx
        db_inx_set = {r.job_db_inx for r in loaded_rows}
        assert len(db_inx_set) == len(loaded_rows), "Duplicate job_db_inx found"

    def test_state_last_mod_time_updates(self, tmp_path):
        """State ``last_mod_time`` should reflect the latest job ``mod_time``."""
        rows = build_standard_test_dataset()
        storage = slurmStorage()

        # First export
        _, _, state1 = storage.syncHistoricalJobsCache(
            outputDir=tmp_path,
            jobsOverride=rows,
        )

        # Add incremental jobs with later mod_time
        rows_inc = build_incremental_dataset()
        _, _, state2 = storage.syncHistoricalJobsCache(
            outputDir=tmp_path,
            jobsOverride=rows_inc,
        )

        assert state2["last_mod_time"] >= state1["last_mod_time"]

    def test_full_incremental_pipeline(self, tmp_path):
        """Full pipeline: initial export → incremental export → verify series.

        Both exports write to the **same** directory so that the second
        one reads the cache and state produced by the first.
        """
        rows_initial = build_standard_test_dataset()
        config = build_mini_cluster_config()
        storage = slurmStorage()

        export_dir = tmp_path / "export"

        # ── Initial export ──
        output1 = storage.exportIncrementalHistoricalUtilization(
            outputDir=export_dir,
            clusterConfig=config,
            jobsOverride=rows_initial,
            intervalMinutes=15,
        )

        series_dir = export_dir / "series"
        assert (series_dir / "type_a.json").exists()
        assert (export_dir / "metadata.json").exists()

        # Snapshot the initial series
        with open(series_dir / "type_a.json") as f:
            series1 = json.load(f)

        # ── Incremental export (same directory — picks up existing cache) ──
        rows_inc = build_incremental_dataset()
        output2 = storage.exportIncrementalHistoricalUtilization(
            outputDir=export_dir,
            clusterConfig=config,
            jobsOverride=rows_inc,
            intervalMinutes=15,
        )

        # Read the updated series
        with open(series_dir / "type_a.json") as f:
            series2 = json.load(f)

        # The incremental export includes all initial + new jobs,
        # so the series should have at least as many data points.
        assert len(series2) >= len(series1)

    def test_repeated_export_with_same_data_is_idempotent(self, tmp_path):
        """Exporting the same data twice produces identical cache."""
        rows = build_standard_test_dataset()
        storage = slurmStorage()

        # First export
        storage.syncHistoricalJobsCache(
            outputDir=tmp_path,
            jobsOverride=rows,
        )

        cache_file = tmp_path / "raw_job_rows.json"
        loaded1 = load_cached_historical_job_rows(cache_file)

        # Second export with same data (simulates re-querying)
        storage.syncHistoricalJobsCache(
            outputDir=tmp_path,
            jobsOverride=rows,
        )

        loaded2 = load_cached_historical_job_rows(cache_file)

        # Same count — deduped by id_job
        assert len(loaded1) == len(loaded2)
