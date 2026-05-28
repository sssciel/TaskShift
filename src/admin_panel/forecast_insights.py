from pathlib import Path

from config import getSchedulerConfig
from forecast.training import (
    DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
    load_forecast_insights,
    resolve_data_dir,
    resolve_model_dir,
)


def build_forecast_insights_payload(projectRoot: str | Path) -> dict:
    projectPath = Path(projectRoot).resolve()
    schedulerConfig = getSchedulerConfig()
    modelDirValue = getattr(schedulerConfig, "forecast_model_dir", None)
    dataDirValue = getattr(schedulerConfig, "forecast_data_dir", None)
    resolvedModelDir = (
        resolve_model_dir(projectPath, modelDirValue) if modelDirValue else None
    )
    resolvedDataDir = resolve_data_dir(projectPath, dataDirValue)

    if resolvedModelDir is None:
        return {
            "available": False,
            "model_dir": None,
            "data_dir": str(resolvedDataDir),
            "error": "Forecast model directory is not configured.",
        }

    insights = load_forecast_insights(resolvedModelDir)
    if insights is None:
        return {
            "available": False,
            "model_dir": str(resolvedModelDir),
            "data_dir": str(resolvedDataDir),
            "error": (
                "Forecast insights JSON is not available. "
                "Retrain the model to create forecast_insights.json for ML charts."
            ),
        }

    payload = dict(insights)
    payload["available"] = True
    payload.setdefault("model_dir", str(resolvedModelDir))
    payload.setdefault("forecast_data_dir", str(resolvedDataDir))
    payload.setdefault(
        "forecast_prediction_horizon_hours",
        getattr(
            schedulerConfig,
            "forecast_prediction_horizon_hours",
            DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
        ),
    )
    payload.setdefault("future_forecast", [])
    payload.setdefault(
        "seasonality",
        {
            "year": None,
            "daily": [],
            "weekly": [],
            "yearly": [],
            "method": "fourier_additive_profiles",
        },
    )
    payload.setdefault("forecast_window", None)
    payload.setdefault("prediction_error", None)
    payload.setdefault("seasonality_error", None)
    payload.setdefault(
        "seasonality_method_note",
        "Seasonality uses cached decomposition saved at model training time.",
    )
    return payload
