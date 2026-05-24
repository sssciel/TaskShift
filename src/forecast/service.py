import json
import logging
from pathlib import Path
import numpy as np

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from .models import FeatureForecast
from .training import (
    TARGET_HORIZON_MINUTES,
    ensure_fresh_forecast_model,
    load_artifact,
    resolve_model_dir,
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
        self.dataDir = Path(dataDir).resolve() if dataDir else None
        configModelDir = getattr(schedulerConfig, "forecast_model_dir", None)
        resolvedModelDirInput = modelDir or configModelDir
        self.modelDir = (
            resolve_model_dir(self.projectRoot, resolvedModelDirInput)
            if resolvedModelDirInput
            else None
        )
        self.skipStartupTraining = bool(
            getattr(schedulerConfig, "forecast_skip_startup_training", False)
        )
        self.averageLoadsByFeature = self._loadAverageLoads()
        self.forecastArtifact = self._loadOrTrainArtifact()

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
        )
        self.forecastArtifact = artifact
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

    def _loadOrTrainArtifact(self):
        if self.modelDir is None:
            return None
        if self.dataDir is None:
            return load_artifact(self.modelDir)
        try:
            return ensure_fresh_forecast_model(
                dataDir=self.dataDir,
                modelDir=self.modelDir,
                projectRoot=self.projectRoot,
                skipStartupTraining=self.skipStartupTraining,
            )
        except Exception as error:
            logger.warning(f"Failed to refresh forecast model artifact: {error}")
            artifact = load_artifact(self.modelDir)
            if artifact is not None:
                logger.warning("Using the previously saved forecast artifact after refresh failure")
            return artifact

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

        prediction = float(self.forecastArtifact.last_prediction_gpu_percent)
        if horizonMinutes <= TARGET_HORIZON_MINUTES:
            return prediction

        requiredWindows = max(1, int((horizonMinutes + TARGET_HORIZON_MINUTES - 1) // TARGET_HORIZON_MINUTES))
        repeatedPredictions = [prediction] * requiredWindows
        return float(np.median(repeatedPredictions))
