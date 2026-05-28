from datetime import datetime

import pandas as pd

from forecast.training import (
    DEFAULT_INSIGHTS_FILENAME,
    DEFAULT_METADATA_FILENAME,
    DEFAULT_MODEL_FILENAME,
    DEFAULT_MODEL_VERSIONS_DIRNAME,
    _save_artifact,
    build_seasonality_components,
    ensure_fresh_forecast_model,
    is_artifact_stale,
    list_artifact_versions,
    load_artifact,
    load_forecast_insights,
    resolve_artifact_refresh_due_at,
)


class TestForecastTrainingSchedule:
    def test_resolve_artifact_refresh_due_at_uses_interval_hours(self):
        metadata = {"trained_at": "2026-05-26T00:00:00+03:00"}
        result = resolve_artifact_refresh_due_at(metadata, updateIntervalHours=84)
        assert result is not None
        assert result.isoformat() == "2026-05-29T12:00:00+03:00"

    def test_is_artifact_stale_when_interval_elapsed(self):
        now = datetime.fromisoformat("2026-05-29T12:00:01+03:00")
        metadata = {"trained_at": "2026-05-26T00:00:00+03:00"}
        assert is_artifact_stale(metadata, now=now, updateIntervalHours=84) is True

    def test_is_artifact_fresh_when_interval_not_elapsed(self):
        now = datetime.fromisoformat("2026-05-29T11:59:59+03:00")
        metadata = {"trained_at": "2026-05-26T00:00:00+03:00"}
        assert is_artifact_stale(metadata, now=now, updateIntervalHours=84) is False


class TestEnsureFreshForecastModel:
    def test_skip_startup_training_without_existing_artifact_returns_none(
        self, monkeypatch, tmp_path
    ):
        monkeypatch.setattr("forecast.training.load_artifact", lambda *args, **kwargs: None)
        train_called = {"value": False}

        def fake_train(*args, **kwargs):
            train_called["value"] = True
            return object()

        monkeypatch.setattr("forecast.training.train_gradient_boosting_forecast", fake_train)

        result = ensure_fresh_forecast_model(
            dataDir=tmp_path / "data",
            modelDir=tmp_path / "model",
            projectRoot=tmp_path,
            skipStartupTraining=True,
            now=datetime(2026, 6, 11, 12, 0, 0),
            modelUpdateIntervalHours=84,
        )

        assert result is None
        assert train_called["value"] is False

    def test_skip_startup_training_reuses_stale_artifact(self, monkeypatch, tmp_path):
        stale_metadata = {"trained_at": "2026-05-26T00:00:00", "last_prediction_gpu_percent": 77.0}
        sentinel_artifact = type("Artifact", (), {"metadata": stale_metadata})()
        monkeypatch.setattr(
            "forecast.training.load_artifact",
            lambda *args, **kwargs: sentinel_artifact,
        )
        train_called = {"value": False}

        def fake_train(*args, **kwargs):
            train_called["value"] = True
            return object()

        monkeypatch.setattr("forecast.training.train_gradient_boosting_forecast", fake_train)

        result = ensure_fresh_forecast_model(
            dataDir=tmp_path / "data",
            modelDir=tmp_path / "model",
            projectRoot=tmp_path,
            skipStartupTraining=True,
            now=datetime(2026, 6, 11, 12, 0, 0),
            modelUpdateIntervalHours=84,
        )

        assert result is sentinel_artifact
        assert train_called["value"] is False

    def test_stale_artifact_triggers_retraining_without_skip_flag(self, monkeypatch, tmp_path):
        stale_metadata = {"trained_at": "2026-05-26T00:00:00", "last_prediction_gpu_percent": 77.0}
        stale_artifact = type("Artifact", (), {"metadata": stale_metadata})()
        fresh_artifact = object()

        monkeypatch.setattr(
            "forecast.training.load_artifact",
            lambda *args, **kwargs: stale_artifact,
        )

        captured = {}

        def fake_train(**kwargs):
            captured.update(kwargs)
            return fresh_artifact

        monkeypatch.setattr("forecast.training.train_gradient_boosting_forecast", fake_train)

        result = ensure_fresh_forecast_model(
            dataDir=tmp_path / "data",
            modelDir=tmp_path / "model",
            projectRoot=tmp_path,
            skipStartupTraining=False,
            now=datetime(2026, 6, 11, 12, 0, 0),
            modelUpdateIntervalHours=84,
        )

        assert result is fresh_artifact
        assert captured["refreshData"] is True
        assert captured["dataDir"] == tmp_path / "data"


class TestForecastArtifactVersioning:
    def test_save_artifact_preserves_latest_and_versioned_copy(self, tmp_path):
        metadata = {
            "trained_at": "2026-06-09T00:30:00+03:00",
            "model_kind": "catboost_regressor",
            "target_name": "gpu_target_mean_6h",
            "training_row_count": 10,
        }

        artifact = _save_artifact(tmp_path, {"model": "sentinel"}, metadata)
        versionId = artifact.metadata["model_version_id"]
        versionDir = tmp_path / DEFAULT_MODEL_VERSIONS_DIRNAME / versionId

        assert (tmp_path / DEFAULT_MODEL_FILENAME).exists()
        assert (tmp_path / DEFAULT_METADATA_FILENAME).exists()
        assert (versionDir / DEFAULT_MODEL_FILENAME).exists()
        assert (versionDir / DEFAULT_METADATA_FILENAME).exists()
        assert artifact.metadata["model_version_dir"] == str(versionDir)

        loadedLatest = load_artifact(tmp_path, includeModel=True)
        loadedVersion = load_artifact(tmp_path, includeModel=True, versionId=versionId)
        versions = list_artifact_versions(tmp_path)

        assert loadedLatest.metadata["model_version_id"] == versionId
        assert loadedLatest.model == {"model": "sentinel"}
        assert loadedVersion.metadata["model_version_id"] == versionId
        assert versions[0]["model_version_id"] == versionId

    def test_save_artifact_writes_latest_and_versioned_insights_json(self, tmp_path):
        metadata = {
            "trained_at": "2026-06-09T00:30:00+03:00",
            "model_kind": "catboost_regressor",
            "target_name": "gpu_target_mean_6h",
            "training_row_count": 10,
        }
        insights = {
            "available": True,
            "future_forecast": [{"window_start_at": "2026-06-09T00:30:00+03:00"}],
            "seasonality": {"daily": [], "weekly": [], "yearly": []},
        }

        artifact = _save_artifact(tmp_path, {"model": "sentinel"}, metadata, insights=insights)
        versionId = artifact.metadata["model_version_id"]
        versionDir = tmp_path / DEFAULT_MODEL_VERSIONS_DIRNAME / versionId

        assert (tmp_path / DEFAULT_INSIGHTS_FILENAME).exists()
        assert (versionDir / DEFAULT_INSIGHTS_FILENAME).exists()
        assert load_forecast_insights(tmp_path)["future_forecast"] == insights["future_forecast"]
        assert load_forecast_insights(tmp_path, versionId=versionId)["model_version_id"] == versionId


class TestSeasonalityHelpers:
    def test_build_seasonality_components_uses_current_year(self, monkeypatch):
        frame = pd.DataFrame(
            {
                "time": pd.date_range("2026-01-01", periods=96 * 40, freq="15min"),
                "cpu": [0.0] * (96 * 40),
                "gpu": [float(index % 96) * 0.2 + 20.0 for index in range(96 * 40)],
            }
        )
        monkeypatch.setattr("forecast.training.load_overall_series_frame", lambda seriesDir: frame)
        result = build_seasonality_components(
            seriesDir="/tmp/series",
            now=datetime(2026, 6, 8, 12, 0, 0),
        )
        assert result["year"] == 2026
        assert result["daily"]
        assert result["weekly"]
        assert result["yearly"]
