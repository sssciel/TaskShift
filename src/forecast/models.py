from dataclasses import dataclass


@dataclass
class FeatureForecast:
    featureName: str
    horizonMinutes: int
    maxCpuLoadPercent: float
    maxGpuLoadPercent: float

    @property
    def availableCpuPercent(self) -> float:
        return max(0.0, 100.0 - self.maxCpuLoadPercent)

    @property
    def availableGpuPercent(self) -> float:
        return max(0.0, 100.0 - self.maxGpuLoadPercent)
