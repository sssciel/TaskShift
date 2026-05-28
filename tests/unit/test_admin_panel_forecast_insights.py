from pathlib import Path
from unittest.mock import MagicMock, patch

from admin_panel.forecast_insights import build_forecast_insights_payload


class TestBuildForecastInsightsPayload:
    def test_returns_unavailable_when_artifact_missing(self):
        schedulerConfig = MagicMock()
        schedulerConfig.forecast_model_dir = "artifacts/forecast_model"
        schedulerConfig.forecast_data_dir = "exports/historical_utilization/current"

        with (
            patch("admin_panel.forecast_insights.getSchedulerConfig", return_value=schedulerConfig),
            patch("admin_panel.forecast_insights.load_forecast_insights", return_value=None),
        ):
            payload = build_forecast_insights_payload("/project")

        assert payload["available"] is False
        assert "forecast_insights.json" in payload["error"]
        assert payload["data_dir"] == "/project/exports/historical_utilization/current"

    def test_returns_summary_and_chart_data(self):
        schedulerConfig = MagicMock()
        schedulerConfig.forecast_model_dir = "artifacts/forecast_model"
        schedulerConfig.forecast_data_dir = "exports/historical_utilization/current"
        insights = {
            "available": True,
            "trained_at": "2026-06-08T12:00:00",
            "model_kind": "catboost_regressor",
            "target_name": "gpu_target_mean_6h",
            "target_horizon_minutes": 360,
            "forecast_prediction_horizon_hours": 72,
            "training_row_count": 1234,
            "latest_observation_time": "2026-06-08T06:00:00",
            "current_gpu_percent": 91.0,
            "future_forecast": [
                {
                    "time": "2026-06-08T12:00:00",
                    "window_start_at": "2026-06-08T12:00:00",
                    "window_end_at": "2026-06-08T18:00:00",
                    "predicted_gpu_mean_6h": 44.2,
                }
            ],
            "seasonality": {
                "year": 2026,
                "daily": [{"x": "00:00", "y": 1.2}],
                "weekly": [{"x": 0, "label": "Пн", "y": 0.5}],
                "yearly": [{"x": 1, "label": "Янв", "y": -0.3}],
                "method": "fourier_additive_profiles",
            },
            "forecast_data_dir": "/tmp/data",
            "forecast_window": {
                "start_at": "2026-06-08T12:00:00",
                "end_at": "2026-06-08T18:00:00",
                "predicted_gpu_percent": 44.2,
            },
        }

        with (
            patch("admin_panel.forecast_insights.getSchedulerConfig", return_value=schedulerConfig),
            patch("admin_panel.forecast_insights.load_forecast_insights", return_value=insights) as mock_load,
        ):
            payload = build_forecast_insights_payload(Path("/project"))

        mock_load.assert_called_once()
        assert payload["available"] is True
        assert payload["model_kind"] == "catboost_regressor"
        assert payload["future_forecast"]
        assert payload["seasonality"]["year"] == 2026
        assert payload["forecast_window"]["predicted_gpu_percent"] == 44.2
