import json
import logging
from pathlib import Path
import numpy as np
from datetime import datetime
from config.logger import append_forecast_runtime_event, build_runtime_log_event

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from .models import FeatureForecast
from .training import (
    DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
    DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
    TARGET_HORIZON_MINUTES,
    load_artifact,
    load_forecast_insights,
    resolve_model_dir,
    resolve_data_dir,
    resolve_series_dir,
)


class ForecastService:
    def __init__(
        self,
        dataDir: str | None = None,
        modelDir: str | None = None,
        schedulerConfig=None,
        projectRoot: str | Path | None = None,
    ):
        self.projectRoot = Path(projectRoot).resolve() if projectRoot else Path(__file__).resolve().parents[2]
        self.dataDir = resolve_data_dir(self.projectRoot, dataDir) if dataDir else None
        configModelDir = getattr(schedulerConfig, "forecast_model_dir", None)
        resolvedModelDirInput = modelDir or configModelDir
        self.modelDir = (
            resolve_model_dir(self.projectRoot, resolvedModelDirInput)
            if resolvedModelDirInput
            else None
        )
        self.timezoneName = getattr(schedulerConfig, "timezone", "Europe/Moscow")
        self.modelUpdateIntervalHours = getattr(
            schedulerConfig,
            "forecast_model_update_interval_hours",
            DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
        )
        self.forecastPredictionHorizonHours = getattr(
            schedulerConfig,
            "forecast_prediction_horizon_hours",
            DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
        )
        self.averageLoadsByFeature = self._loadAverageLoads()
        self.forecastArtifact = self._loadArtifact()
        self.forecastInsights = self._loadInsights()

    def buildFeatureForecast(self, featureName: str, horizonMinutes: int) -> FeatureForecast:
        averageLoads = self.averageLoadsByFeature.get(featureName, {})
        overallLoads = self.averageLoadsByFeature.get("overall", {})
        gpuLoadPercent = self._resolveGpuLoadPercent(
            featureName=featureName,
            horizonMinutes=horizonMinutes,
            fallbackAverage=overallLoads.get("gpu", averageLoads.get("gpu", 0.0)),
        )
        return FeatureForecast(
            featureName=featureName,
            horizonMinutes=horizonMinutes,
            maxCpuLoadPercent=0.0,
            maxGpuLoadPercent=gpuLoadPercent,
        )

    def trainModelNow(self, refreshData: bool = True):
        if self.dataDir is None or self.modelDir is None:
            raise RuntimeError("Forecast data/model directories must be configured before training")
        from .training import train_gradient_boosting_forecast

        artifact = train_gradient_boosting_forecast(
            dataDir=self.dataDir,
            modelDir=self.modelDir,
            projectRoot=self.projectRoot,
            refreshData=refreshData,
            eventWriter=append_forecast_runtime_event,
            timezoneName=self.timezoneName,
            modelUpdateIntervalHours=self.modelUpdateIntervalHours,
            forecastPredictionHorizonHours=self.forecastPredictionHorizonHours,
        )
        self.forecastArtifact = artifact
        self.forecastInsights = self._loadInsights()
        self.averageLoadsByFeature = self._loadAverageLoads()
        return artifact

    def _loadAverageLoads(self) -> dict[str, dict[str, float]]:
        if self.dataDir is None:
            return {}

        seriesDir = self._resolveSeriesDir(self.dataDir)
        if seriesDir is None:
            logger.warning(f"Forecast data directory '{self.dataDir}' does not contain utilization series")
            return {}

        averageLoadsByFeature = {}
        for seriesFile in sorted(seriesDir.glob("*.json")):
            averageLoads = self._loadFeatureAverage(seriesFile)
            if averageLoads is None:
                continue

            averageLoadsByFeature[seriesFile.stem] = averageLoads

        logger.info(
            f"Loaded forecast history for {len(averageLoadsByFeature)} feature types from '{seriesDir}'"
        )
        return averageLoadsByFeature

    def _loadArtifact(self):
        if self.modelDir is None:
            return None
        artifact = load_artifact(self.modelDir)
        if artifact is None:
            logger.warning(
                f"Forecast model artifact is not available in '{self.modelDir}'. "
                "Scheduler will fall back to the historical GPU load baseline until startup bootstrap trains a model."
            )
            append_forecast_runtime_event(
                build_runtime_log_event(
                    category="forecast_runtime",
                    status="BOOTSTRAP_SKIPPED",
                    level="WARNING",
                    eventType="BOOTSTRAP_SKIPPED",
                    message=(
                        "Forecast model artifact is not available during scheduler initialization. "
                        "Using historical GPU baseline until a startup/bootstrap training completes."
                    ),
                    source="forecast.service",
                    model_dir=str(self.modelDir),
                )
            )
        return artifact

    def _loadInsights(self):
        if self.modelDir is None:
            return None
        try:
            return load_forecast_insights(self.modelDir)
        except Exception as error:
            logger.warning(f"Failed to load forecast insights from '{self.modelDir}': {error}")
            return None

    def _resolveSeriesDir(self, dataDir: Path) -> Path | None:
        if not dataDir.exists() or not dataDir.is_dir():
            return None

        return resolve_series_dir(dataDir)

    def _loadFeatureAverage(self, seriesFile: Path) -> dict[str, float] | None:
        try:
            with open(seriesFile, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except (OSError, json.JSONDecodeError) as error:
            logger.warning(f"Failed to load forecast series from '{seriesFile}': {error}")
            return None

        if not isinstance(payload, list) or not payload:
            return None

        cpuValues = []
        gpuValues = []
        for point in payload:
            if not isinstance(point, dict):
                continue

            cpu = self._parsePercentValue(point.get("cpu"))
            gpu = self._parsePercentValue(point.get("gpu"))
            if cpu is None or gpu is None:
                continue

            cpuValues.append(cpu)
            gpuValues.append(gpu)

        if not cpuValues or not gpuValues:
            return None

        return {
            "cpu": sum(cpuValues) / len(cpuValues),
            "gpu": sum(gpuValues) / len(gpuValues),
        }

    def _parsePercentValue(self, value) -> float | None:
        if value is None:
            return None

        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _resolveGpuLoadPercent(
        self,
        *,
        featureName: str,
        horizonMinutes: int,
        fallbackAverage: float,
    ) -> float:
        if self.forecastArtifact is None:
            featureAverageLoads = self.averageLoadsByFeature.get(featureName, {})
            return float(featureAverageLoads.get("gpu", fallbackAverage or 0.0))

        forecastPoints = self._selectForecastPointsForHorizon(horizonMinutes)
        if forecastPoints:
            return float(np.mean([point for point in forecastPoints]))

        return float(self.forecastArtifact.last_prediction_gpu_percent)

    def _selectForecastPointsForHorizon(self, horizonMinutes: int) -> list[float]:
        if self.forecastArtifact is None:
            return []
        metadata = self.forecastArtifact.metadata
        points = []
        if self.forecastInsights is not None:
            points = self.forecastInsights.get("future_forecast") or []
        if not points:
            points = metadata.get("forecast_points") or []
        if not points:
            return []
        now = datetime.now().replace(tzinfo=None)
        requiredWindows = max(
            1,
            int((horizonMinutes + TARGET_HORIZON_MINUTES - 1) // TARGET_HORIZON_MINUTES),
        )
        selected = []
        for point in points:
            windowEndValue = point.get("window_end_at")
            try:
                windowEnd = datetime.fromisoformat(str(windowEndValue)).replace(tzinfo=None)
            except (TypeError, ValueError):
                windowEnd = None
            if windowEnd is not None and windowEnd <= now:
                continue
            value = point.get("predicted_gpu_mean_6h")
            if value is None:
                continue
            selected.append(float(value))
            if len(selected) >= requiredWindows:
                break
        return selected
