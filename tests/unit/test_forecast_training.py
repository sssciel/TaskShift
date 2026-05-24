from datetime import datetime

from forecast.training import (
    ensure_fresh_forecast_model,
    is_artifact_stale,
    latest_scheduled_training_at,
)


class TestForecastTrainingSchedule:
    def test_latest_scheduled_training_at_uses_latest_tuesday_or_friday_midnight(self):
        now = datetime(2026, 5, 28, 10, 30, 0)
        result = latest_scheduled_training_at(now=now)
        assert result == datetime(2026, 5, 26, 0, 0, 0)

    def test_latest_scheduled_training_at_returns_same_day_slot_after_midnight(self):
        now = datetime(2026, 5, 29, 0, 5, 0)
        result = latest_scheduled_training_at(now=now)
        assert result == datetime(2026, 5, 29, 0, 0, 0)

    def test_is_artifact_stale_when_trained_before_latest_slot(self):
        now = datetime(2026, 5, 29, 12, 0, 0)
        metadata = {"trained_at": "2026-05-26T00:00:00"}
        assert is_artifact_stale(metadata, now=now) is True

    def test_is_artifact_fresh_when_trained_on_latest_slot(self):
        now = datetime(2026, 5, 29, 12, 0, 0)
        metadata = {"trained_at": "2026-05-29T00:00:00"}
        assert is_artifact_stale(metadata, now=now) is False


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
        )

        assert result is fresh_artifact
        assert captured["refreshData"] is True
        assert captured["dataDir"] == tmp_path / "data"
