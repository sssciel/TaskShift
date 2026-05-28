from argparse import Namespace
from pathlib import Path
from unittest.mock import MagicMock, patch

from cli import bootstrap_forecast_runtime, resolve_export_output_dir


class TestResolveExportOutputDir:
    def test_uses_managed_default_path_without_scheduler_config_dir(self):
        result = resolve_export_output_dir(Namespace(output_dir=None, data_dir=None))
        assert result.endswith("exports/historical_utilization/current")

    def test_prefers_explicit_cli_output_dir(self):
        result = resolve_export_output_dir(Namespace(output_dir="tmp/export-root", data_dir=None))
        assert result.endswith("tmp/export-root")


class TestBootstrapForecastRuntime:
    def _scheduler_config(self):
        config = MagicMock()
        config.forecast_enabled = True
        config.forecast_model_dir = "artifacts/forecast_model"
        config.forecast_model_update_interval_hours = 84
        config.forecast_prediction_horizon_hours = 72
        config.forecast_skip_startup_training = False
        config.timezone = "Europe/Moscow"
        config.cluster_config_refresh_time = "00:30"
        return config

    def test_trains_model_before_scheduler_work_when_forecast_enabled(self):
        schedulerConfig = self._scheduler_config()
        artifact = MagicMock()
        artifact.metadata = {
            "trained_at": "2026-06-08T12:00:00",
            "last_prediction_gpu_percent": 42.0,
        }

        with (
            patch("cli.load_artifact", return_value=None),
            patch("cli.train_gradient_boosting_forecast", return_value=artifact) as mock_train,
        ):
            result = bootstrap_forecast_runtime(
                args=Namespace(without_forecast=False, output_dir=None, data_dir=None, model_dir=None),
                schedulerConfig=schedulerConfig,
                startupReason="service_startup",
            )

        assert result is artifact
        assert mock_train.call_count == 1
        kwargs = mock_train.call_args.kwargs
        assert kwargs["refreshData"] is True
        assert kwargs["dataDir"].endswith("exports/historical_utilization/current")
        assert Path(kwargs["modelDir"]).name == "forecast_model"
        assert kwargs["forecastPredictionHorizonHours"] == 72

    def test_uses_existing_artifact_when_startup_training_is_disabled(self):
        schedulerConfig = self._scheduler_config()
        schedulerConfig.forecast_skip_startup_training = True
        artifact = MagicMock()
        artifact.metadata = {
            "trained_at": "2026-06-07T00:00:00",
            "last_prediction_gpu_percent": 33.5,
        }

        with (
            patch("cli.load_artifact", return_value=artifact) as mock_load,
            patch("cli.train_gradient_boosting_forecast") as mock_train,
        ):
            result = bootstrap_forecast_runtime(
                args=Namespace(without_forecast=False, output_dir=None, data_dir=None, model_dir=None),
                schedulerConfig=schedulerConfig,
                startupReason="service_startup",
            )

        assert result is artifact
        assert mock_load.call_count == 1
        mock_train.assert_not_called()

    def test_fails_when_startup_training_is_disabled_and_artifact_is_missing(self):
        schedulerConfig = self._scheduler_config()
        schedulerConfig.forecast_skip_startup_training = True

        with patch("cli.load_artifact", return_value=None):
            try:
                bootstrap_forecast_runtime(
                    args=Namespace(without_forecast=False, output_dir=None, data_dir=None, model_dir=None),
                    schedulerConfig=schedulerConfig,
                    startupReason="service_startup",
                )
            except RuntimeError as error:
                assert "no saved model artifact was found" in str(error)
            else:
                raise AssertionError("Expected bootstrap_forecast_runtime to fail without an artifact")
