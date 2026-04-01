import json
import logging
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from .models import FeatureForecast


class ForecastService:
    def __init__(self, dataDir: str | None = None):
        self.dataDir = Path(dataDir) if dataDir else None
        self.averageLoadsByFeature = self._loadAverageLoads()

    def buildFeatureForecast(self, featureName: str, horizonMinutes: int) -> FeatureForecast:
        averageLoads = self.averageLoadsByFeature.get(featureName, {})
        return FeatureForecast(
            featureName=featureName,
            horizonMinutes=horizonMinutes,
            maxCpuLoadPercent=averageLoads.get("cpu", 0.0),
            maxGpuLoadPercent=averageLoads.get("gpu", 0.0),
        )

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

    def _resolveSeriesDir(self, dataDir: Path) -> Path | None:
        if not dataDir.exists() or not dataDir.is_dir():
            return None

        nestedSeriesDir = dataDir / "series"
        if nestedSeriesDir.exists() and nestedSeriesDir.is_dir():
            return nestedSeriesDir

        return dataDir

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
