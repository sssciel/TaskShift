import json
import logging
import pickle
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from config.logger import build_runtime_log_event
from config.timezone import coerce_datetime_timezone, now_in_timezone

from config.calendar import ConferenceCalendarConfig
from config.paths import academicCalendarRoot
from storage import slurmStorage
from storage.constants import DEFAULT_EXPORT_ROOT

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info


INTERVAL_MINUTES = 15
MIN_HISTORY_STEPS = 672
TARGET_HORIZON_MINUTES = 360
TARGET_HORIZON_STEPS = TARGET_HORIZON_MINUTES // INTERVAL_MINUTES
TARGET_AGGREGATION = "mean"
TARGET_COLUMN = f"gpu_target_{TARGET_AGGREGATION}_6h"
DEFAULT_MODEL_UPDATE_INTERVAL_HOURS = 84
DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS = 72
DEFAULT_MODEL_FILENAME = "model.pkl"
DEFAULT_METADATA_FILENAME = "metadata.json"
DEFAULT_INSIGHTS_FILENAME = "forecast_insights.json"
DEFAULT_MODEL_VERSIONS_DIRNAME = "versions"
DEFAULT_MODEL_KIND = "catboost_regressor"
DEFAULT_FEATURE_NAME = "overall"
MIN_UTILIZATION = 0.0
MAX_UTILIZATION = 100.0
MAINTENANCE_MIN_ZERO_POINTS = 2
DEADLINE_LOOKBACK_DAYS = 20
DEFAULT_FORBIDDEN_FEATURE_TOKENS = (
    "target",
    "future",
    "t_plus",
    "prediction",
    "fold",
    "train",
    "test",
)
KNOWN_FUTURE_FEATURE_CANDIDATES = (
    "hour_sin",
    "hour_cos",
    "day_of_week_sin",
    "day_of_week_cos",
    "month_sin",
    "month_cos",
    "is_weekend",
    "is_session",
    "is_vacation",
    "is_holiday",
    "is_maintenance",
    "deadline_count",
)


@dataclass
class ForecastArtifact:
    metadata: dict
    model: object | None = None

    @property
    def last_prediction_gpu_percent(self) -> float:
        forecastPoints = self.metadata.get("forecast_points") or []
        if forecastPoints:
            firstPoint = forecastPoints[0]
            value = firstPoint.get("predicted_gpu_mean_6h")
            if value is not None:
                return float(value)
        return float(self.metadata.get("last_prediction_gpu_percent", 0.0))

    @property
    def trained_at(self) -> datetime | None:
        value = self.metadata.get("trained_at")
        if not value:
            return None
        return datetime.fromisoformat(str(value))


def resolve_series_dir(dataDir: str | Path) -> Path:
    dataPath = Path(dataDir).resolve()
    nestedSeriesDir = dataPath / "series"
    if nestedSeriesDir.exists() and nestedSeriesDir.is_dir():
        return nestedSeriesDir
    return dataPath


def resolve_data_dir(
    projectRoot: str | Path,
    dataDir: str | Path | None = None,
) -> Path:
    if dataDir:
        path = Path(dataDir)
        if path.is_absolute():
            return path.resolve()
        return (Path(projectRoot).resolve() / path).resolve()
    return (Path(projectRoot).resolve() / DEFAULT_EXPORT_ROOT).resolve()


def resolve_model_dir(projectRoot: str | Path, modelDir: str | Path) -> Path:
    path = Path(modelDir)
    if path.is_absolute():
        return path
    return (Path(projectRoot).resolve() / path).resolve()


def artifact_paths(modelDir: str | Path, versionId: str | None = None) -> dict[str, Path]:
    root = Path(modelDir).resolve()
    if versionId:
        root = root / DEFAULT_MODEL_VERSIONS_DIRNAME / versionId
    return {
        "root": root,
        "model": root / DEFAULT_MODEL_FILENAME,
        "metadata": root / DEFAULT_METADATA_FILENAME,
        "insights": root / DEFAULT_INSIGHTS_FILENAME,
    }


def _safe_model_version_token(value: str) -> str:
    result = []
    for char in str(value):
        if char.isalnum():
            result.append(char)
        else:
            result.append("-")
    return "-".join("".join(result).strip("-").split("-"))


def _build_model_version_id(metadata: dict) -> str:
    trainedAtValue = metadata.get("trained_at") or datetime.now().isoformat(timespec="seconds")
    modelKind = metadata.get("model_kind") or DEFAULT_MODEL_KIND
    return _safe_model_version_token(f"{trainedAtValue}-{modelKind}")


def _resolve_unique_model_version_id(modelDir: str | Path, preferredVersionId: str) -> str:
    versionsRoot = Path(modelDir).resolve() / DEFAULT_MODEL_VERSIONS_DIRNAME
    candidate = preferredVersionId
    suffix = 2
    while (versionsRoot / candidate).exists():
        candidate = f"{preferredVersionId}-{suffix}"
        suffix += 1
    return candidate


def resolve_artifact_refresh_due_at(
    metadata: dict | None,
    *,
    updateIntervalHours: int | float = DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
    timezoneName: str = "Europe/Moscow",
) -> datetime | None:
    if not metadata:
        return None
    trainedAtValue = metadata.get("trained_at")
    if not trainedAtValue:
        return None
    trainedAt = coerce_datetime_timezone(
        datetime.fromisoformat(str(trainedAtValue)),
        timezoneName,
    )
    return trainedAt + timedelta(hours=float(updateIntervalHours))


def is_artifact_stale(
    metadata: dict | None,
    now: datetime | None = None,
    *,
    updateIntervalHours: int | float = DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
    timezoneName: str = "Europe/Moscow",
) -> bool:
    dueAt = resolve_artifact_refresh_due_at(
        metadata,
        updateIntervalHours=updateIntervalHours,
        timezoneName=timezoneName,
    )
    if dueAt is None:
        return True
    effectiveNow = coerce_datetime_timezone(now or now_in_timezone(timezoneName), timezoneName)
    return effectiveNow >= dueAt


def import_catboost():
    try:
        from catboost import CatBoostRegressor
    except (ImportError, OSError):
        return None
    return CatBoostRegressor


def make_catboost_regressor():
    CatBoostRegressor = import_catboost()
    if CatBoostRegressor is None:
        raise ImportError("catboost is not available")
    return CatBoostRegressor(
        loss_function="RMSE",
        eval_metric="RMSE",
        iterations=400,
        learning_rate=0.05,
        depth=6,
        l2_leaf_reg=5.0,
        min_data_in_leaf=20,
        random_strength=0.0,
        random_seed=42,
        allow_writing_files=False,
        thread_count=-1,
    )


def _parse_time_column(rawTime: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(rawTime, format="%H:%M:%S %d.%m.%y", errors="coerce")
    missingMask = parsed.isna()
    if missingMask.any():
        parsed.loc[missingMask] = pd.to_datetime(rawTime.loc[missingMask], errors="coerce")
    return parsed


def _load_overall_series(seriesDir: Path) -> pd.DataFrame:
    overallPath = seriesDir / f"{DEFAULT_FEATURE_NAME}.json"
    if not overallPath.exists():
        raise FileNotFoundError(f"Overall utilization series not found: {overallPath}")
    with open(overallPath, "r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, list):
        raise ValueError(f"Overall series must contain a JSON list: {overallPath}")
    frame = pd.DataFrame(payload)
    requiredColumns = {"time", "cpu", "gpu"}
    missingColumns = requiredColumns - set(frame.columns)
    if missingColumns:
        raise ValueError(f"Overall series is missing columns: {sorted(missingColumns)}")
    frame = frame[["time", "cpu", "gpu"]].copy()
    frame["time"] = _parse_time_column(frame["time"])
    frame["cpu"] = pd.to_numeric(frame["cpu"], errors="coerce")
    frame["gpu"] = pd.to_numeric(frame["gpu"], errors="coerce")
    frame = frame.dropna(subset=["time"]).sort_values("time").drop_duplicates(subset=["time"], keep="last")
    frame["cpu"] = frame["cpu"].clip(lower=MIN_UTILIZATION, upper=MAX_UTILIZATION)
    frame["gpu"] = frame["gpu"].clip(lower=MIN_UTILIZATION, upper=MAX_UTILIZATION)
    return frame.reset_index(drop=True)


def load_overall_series_frame(seriesDir: str | Path) -> pd.DataFrame:
    return _load_overall_series(resolve_series_dir(seriesDir))


def _regularize_15m_grid(frame: pd.DataFrame) -> pd.DataFrame:
    indexed = frame.set_index("time").sort_index()
    fullIndex = pd.date_range(indexed.index.min(), indexed.index.max(), freq=f"{INTERVAL_MINUTES}min")
    reindexed = indexed.reindex(fullIndex)
    result = reindexed.copy()
    for metric in ("cpu", "gpu"):
        result[metric] = (
            pd.to_numeric(result[metric], errors="coerce")
            .interpolate(method="time", limit_direction="both")
            .ffill()
            .bfill()
            .clip(lower=MIN_UTILIZATION, upper=MAX_UTILIZATION)
        )
    result.index.name = "time"
    return result.reset_index()


def _add_time_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    iso = result["time"].dt.isocalendar()
    result["date"] = result["time"].dt.normalize()
    result["year"] = result["time"].dt.year
    result["quarter"] = result["time"].dt.quarter
    result["month"] = result["time"].dt.month
    result["week_of_year"] = iso.week.astype(int)
    result["day"] = result["time"].dt.day
    result["day_of_year"] = result["time"].dt.dayofyear
    result["day_of_week"] = result["time"].dt.dayofweek
    result["hour"] = result["time"].dt.hour
    result["minute"] = result["time"].dt.minute
    result["slot_of_day"] = result["hour"] * (60 // INTERVAL_MINUTES) + result["minute"] // INTERVAL_MINUTES
    result["slot_of_week"] = result["day_of_week"] * 24 * (60 // INTERVAL_MINUTES) + result["slot_of_day"]
    result["is_weekend"] = result["day_of_week"].isin([5, 6])
    hourFraction = result["hour"] + result["minute"] / 60.0
    result["hour_sin"] = np.sin(2 * np.pi * hourFraction / 24.0)
    result["hour_cos"] = np.cos(2 * np.pi * hourFraction / 24.0)
    result["day_of_week_sin"] = np.sin(2 * np.pi * result["day_of_week"] / 7.0)
    result["day_of_week_cos"] = np.cos(2 * np.pi * result["day_of_week"] / 7.0)
    result["month_sin"] = np.sin(2 * np.pi * (result["month"] - 1) / 12.0)
    result["month_cos"] = np.cos(2 * np.pi * (result["month"] - 1) / 12.0)
    return result


def _add_lag_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for metric in ("cpu", "gpu"):
        for lagStep in (1, 2, 4, 16, 96, 672):
            result[f"{metric}_lag_{lagStep}"] = result[metric].shift(lagStep)
    return result


def _add_rolling_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    rollingWindows = {"1h": 4, "4h": 16, "24h": 96, "7d": 672}
    for metric in ("cpu", "gpu"):
        history = result[metric].shift(1)
        for windowName, windowSize in rollingWindows.items():
            rolling = history.rolling(window=windowSize, min_periods=1)
            result[f"{metric}_roll_mean_{windowName}"] = rolling.mean()
            result[f"{metric}_roll_max_{windowName}"] = rolling.max()
            result[f"{metric}_roll_min_{windowName}"] = rolling.min()
            result[f"{metric}_roll_std_{windowName}"] = rolling.std(ddof=0)
            result[f"{metric}_roll_p90_{windowName}"] = rolling.quantile(0.90)
            result[f"{metric}_roll_p95_{windowName}"] = rolling.quantile(0.95)
    return result


def _infer_step_timedelta(frame: pd.DataFrame) -> pd.Timedelta:
    diffs = frame["time"].sort_values().diff().dropna()
    if diffs.empty:
        return pd.Timedelta(minutes=INTERVAL_MINUTES)
    return diffs.mode().iloc[0]


def _detect_zero_utilization_windows(frame: pd.DataFrame) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    zeroMask = frame["cpu"].eq(0) & frame["gpu"].eq(0)
    if not zeroMask.any():
        return []
    runId = zeroMask.ne(zeroMask.shift(fill_value=False)).cumsum()
    zeroRuns = frame.loc[zeroMask, ["time"]].copy()
    zeroRuns["run_id"] = runId[zeroMask].to_numpy()
    grouped = (
        zeroRuns.groupby("run_id", as_index=False)
        .agg(start_time=("time", "min"), last_zero_time=("time", "max"), points=("time", "size"))
    )
    grouped = grouped.loc[grouped["points"] >= MAINTENANCE_MIN_ZERO_POINTS].copy()
    if grouped.empty:
        return []
    step = _infer_step_timedelta(frame)
    grouped["end_time"] = grouped["last_zero_time"] + step
    return [
        (pd.Timestamp(row.start_time), pd.Timestamp(row.end_time))
        for row in grouped.itertuples(index=False)
    ]


def _build_maintenance_mask(timestamps: pd.Series, maintenanceWindows: list[tuple[pd.Timestamp, pd.Timestamp]]) -> pd.Series:
    mask = pd.Series(False, index=timestamps.index)
    for startTime, endTime in maintenanceWindows:
        mask |= timestamps.between(startTime, endTime, inclusive="left")
    return mask


def _load_year_calendar_flags(calendarRoot: Path, year: int) -> tuple[set[pd.Timestamp], set[pd.Timestamp], set[pd.Timestamp]]:
    yearPath = calendarRoot / str(year)
    if not yearPath.exists():
        return set(), set(), set()
    from config.calendar import AcademicCalendarConfig

    calendarConfig = AcademicCalendarConfig(calendarRoot).loadYear(year)
    holidays = {pd.Timestamp(value).normalize() for value in calendarConfig.holidays}
    sessions = {pd.Timestamp(value).normalize() for value in calendarConfig.sessions}
    vacations = {pd.Timestamp(value).normalize() for value in calendarConfig.vacations}
    return holidays, sessions, vacations


def _load_conference_deadlines(calendarRoot: Path, year: int) -> list[pd.Timestamp]:
    yearPath = calendarRoot / str(year)
    if not yearPath.exists():
        return []
    deadlines = ConferenceCalendarConfig(calendarRoot).loadYear(year).toList()
    return [pd.Timestamp(value).normalize() for value in deadlines]


def _build_calendar_daily_frame(calendarRoot: Path, startTime: pd.Timestamp, endTime: pd.Timestamp) -> pd.DataFrame:
    dailyIndex = pd.date_range(start=startTime.normalize(), end=endTime.normalize(), freq="D")
    dailyFrame = pd.DataFrame({"date": dailyIndex})
    holidays = set()
    sessions = set()
    vacations = set()
    for year in range(startTime.year, endTime.year + 1):
        yearHolidays, yearSessions, yearVacations = _load_year_calendar_flags(calendarRoot, year)
        holidays |= yearHolidays
        sessions |= yearSessions
        vacations |= yearVacations
    deadlines = []
    for year in range(startTime.year, endTime.year + 2):
        deadlines.extend(_load_conference_deadlines(calendarRoot, year))
    dailyFrame["is_holiday"] = dailyFrame["date"].isin(holidays)
    dailyFrame["is_session"] = dailyFrame["date"].isin(sessions)
    dailyFrame["is_vacation"] = dailyFrame["date"].isin(vacations)
    dailyFrame["deadline_count"] = 0
    for deadline in deadlines:
        start = deadline - pd.Timedelta(days=DEADLINE_LOOKBACK_DAYS)
        mask = dailyFrame["date"].between(start, deadline, inclusive="both")
        dailyFrame.loc[mask, "deadline_count"] += 1
    dailyFrame["has_deadline_pressure"] = dailyFrame["deadline_count"] > 0
    dailyFrame["is_special_day"] = (
        dailyFrame["is_holiday"]
        | dailyFrame["is_session"]
        | dailyFrame["is_vacation"]
        | dailyFrame["has_deadline_pressure"]
    )
    return dailyFrame


def _future_mean_target(series: pd.Series, horizonSteps: int) -> pd.Series:
    shifted = series.shift(-1)
    values = shifted.rolling(window=horizonSteps, min_periods=horizonSteps).mean()
    return values.shift(-(horizonSteps - 1))


def _select_feature_columns(frame: pd.DataFrame) -> list[str]:
    featureColumns = []
    for column in frame.columns:
        columnLower = column.lower()
        if column == "time":
            continue
        if any(token in columnLower for token in DEFAULT_FORBIDDEN_FEATURE_TOKENS):
            continue
        if pd.api.types.is_datetime64_any_dtype(frame[column]):
            continue
        if pd.api.types.is_object_dtype(frame[column]):
            continue
        featureColumns.append(column)
    return featureColumns


def _build_feature_frame(
    regularFrame: pd.DataFrame,
    *,
    calendarPath: Path,
    maintenanceWindows: list[tuple[pd.Timestamp, pd.Timestamp]],
) -> pd.DataFrame:
    frame = _add_time_features(regularFrame)
    frame = _add_lag_features(frame)
    frame = _add_rolling_features(frame)
    frame["is_maintenance"] = _build_maintenance_mask(frame["time"], maintenanceWindows)
    calendarDailyFrame = _build_calendar_daily_frame(
        calendarPath,
        pd.Timestamp(frame["time"].min()),
        pd.Timestamp(frame["time"].max()),
    )
    frame = frame.merge(calendarDailyFrame, on="date", how="left")
    for column in ("is_holiday", "is_session", "is_vacation", "has_deadline_pressure", "is_special_day"):
        frame[column] = frame[column].fillna(False).astype(bool)
    frame["deadline_count"] = frame["deadline_count"].fillna(0).astype(int)
    frame["is_holiday_or_weekend"] = frame["is_holiday"] | frame["is_weekend"]
    frame = frame.iloc[MIN_HISTORY_STEPS:].reset_index(drop=True)
    return frame


def build_training_frame(seriesDir: str | Path, calendarRoot: str | Path | None = None) -> tuple[pd.DataFrame, list[str]]:
    resolvedSeriesDir = resolve_series_dir(seriesDir)
    calendarPath = Path(calendarRoot or academicCalendarRoot).resolve()
    rawFrame = _load_overall_series(resolvedSeriesDir)
    regularFrame = _regularize_15m_grid(rawFrame)
    maintenanceWindows = _detect_zero_utilization_windows(rawFrame)
    frame = _build_feature_frame(
        regularFrame,
        calendarPath=calendarPath,
        maintenanceWindows=maintenanceWindows,
    )
    frame[TARGET_COLUMN] = _future_mean_target(frame["gpu"], horizonSteps=TARGET_HORIZON_STEPS)
    featureColumns = _select_feature_columns(frame)
    return frame.sort_values("time").reset_index(drop=True), featureColumns


def _floor_timestamp_to_interval(value: pd.Timestamp) -> pd.Timestamp:
    return value.floor(f"{INTERVAL_MINUTES}min")


def _make_naive_timestamp(value: datetime | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert(None)
    return timestamp


def _clip_prediction(value: float) -> float:
    return float(np.clip(float(value), MIN_UTILIZATION, MAX_UTILIZATION))


def build_future_forecast_points(
    *,
    model,
    seriesDir: str | Path,
    featureColumns: list[str],
    now: datetime | None = None,
    horizonHours: int | float = DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
    calendarRoot: str | Path | None = None,
) -> dict:
    if model is None:
        return {"points": [], "current_gpu_percent": None, "current_observation_time": None}

    resolvedSeriesDir = resolve_series_dir(seriesDir)
    calendarPath = Path(calendarRoot or academicCalendarRoot).resolve()
    rawFrame = _load_overall_series(resolvedSeriesDir)
    if rawFrame.empty:
        return {"points": [], "current_gpu_percent": None, "current_observation_time": None}

    regularHistory = _regularize_15m_grid(rawFrame)
    latestObservation = pd.Timestamp(regularHistory["time"].max())
    effectiveNow = _floor_timestamp_to_interval(
        _make_naive_timestamp(now or datetime.now())
    )
    forecastStart = max(effectiveNow, latestObservation)
    horizonMinutes = int(float(horizonHours) * 60)
    forecastEnd = forecastStart + pd.Timedelta(minutes=max(horizonMinutes, TARGET_HORIZON_MINUTES))
    fullIndex = pd.date_range(
        start=pd.Timestamp(regularHistory["time"].min()),
        end=forecastEnd,
        freq=f"{INTERVAL_MINUTES}min",
    )
    combined = regularHistory.set_index("time").reindex(fullIndex)
    for metric in ("cpu", "gpu"):
        combined[metric] = (
            pd.to_numeric(combined[metric], errors="coerce")
            .interpolate(method="time", limit_direction="forward")
            .ffill()
            .bfill()
            .clip(lower=MIN_UTILIZATION, upper=MAX_UTILIZATION)
        )
    combined.index.name = "time"
    combinedFrame = combined.reset_index()
    maintenanceWindows = _detect_zero_utilization_windows(rawFrame)

    points = []
    anchorTimes = pd.date_range(
        start=forecastStart,
        end=forecastEnd - pd.Timedelta(minutes=TARGET_HORIZON_MINUTES),
        freq=f"{TARGET_HORIZON_MINUTES}min",
    )
    for anchorTime in anchorTimes:
        featureFrame = _build_feature_frame(
            combinedFrame,
            calendarPath=calendarPath,
            maintenanceWindows=maintenanceWindows,
        )
        rowFrame = featureFrame.loc[featureFrame["time"].eq(anchorTime)].copy()
        if rowFrame.empty:
            continue
        modelFeatureColumns = [column for column in featureColumns if column in rowFrame.columns]
        if len(modelFeatureColumns) != len(featureColumns):
            continue
        rowFrame = rowFrame.dropna(subset=modelFeatureColumns)
        if rowFrame.empty:
            continue
        predictionRaw = float(model.predict(rowFrame[modelFeatureColumns].to_numpy(dtype=float))[0])
        prediction = _clip_prediction(predictionRaw)
        windowEnd = anchorTime + pd.Timedelta(minutes=TARGET_HORIZON_MINUTES)
        points.append(
            {
                "time": pd.Timestamp(anchorTime).isoformat(),
                "window_start_at": pd.Timestamp(anchorTime).isoformat(),
                "window_end_at": pd.Timestamp(windowEnd).isoformat(),
                "predicted_gpu_mean_6h": round(prediction, 2),
                "predicted_gpu_mean_6h_raw": predictionRaw,
            }
        )
        futureMask = combinedFrame["time"].gt(anchorTime) & combinedFrame["time"].le(windowEnd)
        combinedFrame.loc[futureMask, "gpu"] = prediction

    latestRow = regularHistory.iloc[-1]
    return {
        "points": points,
        "current_gpu_percent": round(float(latestRow["gpu"]), 2),
        "current_observation_time": pd.Timestamp(latestRow["time"]).isoformat(),
        "forecast_start_at": pd.Timestamp(forecastStart).isoformat(),
        "forecast_end_at": pd.Timestamp(forecastEnd).isoformat(),
    }


def build_current_year_seasonality_points(
    *,
    seriesDir: str | Path,
    now: datetime | None = None,
    pointLimit: int = 366,
) -> dict:
    effectiveNow = now or datetime.now()
    overallFrame = load_overall_series_frame(seriesDir)
    if overallFrame.empty:
        return {"year": effectiveNow.year, "points": [], "method": "daily_median_minus_21d_trend"}

    currentYear = int(effectiveNow.year)
    currentYearFrame = overallFrame.loc[overallFrame["time"].dt.year == currentYear].copy()
    yearUsed = currentYear
    if currentYearFrame.empty:
        latestTimestamp = overallFrame["time"].max()
        yearUsed = int(pd.Timestamp(latestTimestamp).year)
        currentYearFrame = overallFrame.loc[overallFrame["time"].dt.year == yearUsed].copy()

    dailyFrame = (
        currentYearFrame.set_index("time")[["gpu"]]
        .resample("D")
        .median()
        .rename(columns={"gpu": "daily_gpu_percent"})
        .reset_index()
    )
    if dailyFrame.empty:
        return {"year": yearUsed, "points": [], "method": "daily_median_minus_21d_trend"}

    dailyFrame["trend_gpu_percent"] = (
        dailyFrame["daily_gpu_percent"]
        .rolling(window=21, center=True, min_periods=7)
        .median()
        .interpolate(limit_direction="both")
        .ffill()
        .bfill()
    )
    dailyFrame["seasonal_gpu_percent"] = (
        dailyFrame["daily_gpu_percent"] - dailyFrame["trend_gpu_percent"]
    )
    dailyFrame["seasonal_gpu_percent"] = (
        dailyFrame["seasonal_gpu_percent"]
        .rolling(window=7, center=True, min_periods=3)
        .mean()
        .interpolate(limit_direction="both")
        .ffill()
        .bfill()
    )

    if len(dailyFrame) > pointLimit:
        sampleIndexes = np.linspace(0, len(dailyFrame) - 1, pointLimit, dtype=int)
        dailyFrame = dailyFrame.iloc[sampleIndexes].copy()

    points = []
    for _, row in dailyFrame.iterrows():
        points.append(
            {
                "date": pd.Timestamp(row["time"]).date().isoformat(),
                "daily_gpu_percent": round(float(row["daily_gpu_percent"]), 2),
                "trend_gpu_percent": round(float(row["trend_gpu_percent"]), 2),
                "seasonal_gpu_percent": round(float(row["seasonal_gpu_percent"]), 2),
            }
        )

    return {
        "year": yearUsed,
        "points": points,
        "method": "daily_median_minus_21d_trend",
    }


def _build_centered_fourier_block(phase: np.ndarray, order: int) -> np.ndarray:
    columns = []
    for harmonic in range(1, order + 1):
        angle = 2.0 * np.pi * harmonic * phase
        columns.append(np.sin(angle))
        columns.append(np.cos(angle))
    if not columns:
        return np.zeros((len(phase), 0), dtype=float)
    block = np.column_stack(columns).astype(float)
    block -= block.mean(axis=0, keepdims=True)
    return block


def _orthogonalize_block(block: np.ndarray, referenceBlocks: list[np.ndarray]) -> np.ndarray:
    if block.shape[1] == 0:
        return block
    result = block.astype(float).copy()
    for reference in referenceBlocks:
        if reference.shape[1] == 0:
            continue
        projectionCoefficients, *_ = np.linalg.lstsq(reference, result, rcond=None)
        result = result - reference @ projectionCoefficients
    result -= result.mean(axis=0, keepdims=True)
    return result


def _build_piecewise_linear_trend_block(timeIndex: pd.Series, knots: int = 24) -> np.ndarray:
    positions = np.linspace(0.0, 1.0, len(timeIndex), dtype=float)
    columns = [positions]
    for knot in np.linspace(0.0, 1.0, knots + 2, dtype=float)[1:-1]:
        columns.append(np.clip(positions - knot, a_min=0.0, a_max=None))
    block = np.column_stack(columns).astype(float)
    block -= block.mean(axis=0, keepdims=True)
    return block


def _solve_weighted_ridge(X: np.ndarray, y: np.ndarray, weights: np.ndarray, alpha: float) -> np.ndarray:
    if X.shape[1] == 0:
        return np.zeros(0, dtype=float)
    safeWeights = np.clip(weights.astype(float), 1e-6, None)
    sqrtWeights = np.sqrt(safeWeights)[:, None]
    weightedX = X * sqrtWeights
    weightedY = y * sqrtWeights[:, 0]
    gram = weightedX.T @ weightedX
    regularization = alpha * np.eye(X.shape[1], dtype=float)
    return np.linalg.solve(gram + regularization, weightedX.T @ weightedY)


def _robust_mad_scale(values: np.ndarray) -> float:
    median = np.median(values)
    mad = np.median(np.abs(values - median))
    return max(1.4826 * mad, 1e-6)


def _build_huber_weights(residual: np.ndarray, scale: float, deltaMultiplier: float = 1.5) -> np.ndarray:
    cutoff = max(deltaMultiplier * scale, 1e-6)
    absoluteResidual = np.abs(residual)
    weights = np.ones_like(absoluteResidual, dtype=float)
    mask = absoluteResidual > cutoff
    weights[mask] = cutoff / absoluteResidual[mask]
    return np.clip(weights, 0.05, 1.0)


def _decompose_gpu_multiseasonality(frame: pd.DataFrame) -> pd.DataFrame:
    source = frame.loc[~frame["is_maintenance"]].copy().sort_values("time").reset_index(drop=True)
    if source.empty:
        return source
    y = source["gpu"].astype(float).to_numpy()
    minutesSinceMidnight = (
        source["time"].dt.hour.to_numpy() * 60.0
        + source["time"].dt.minute.to_numpy()
        + source["time"].dt.second.to_numpy() / 60.0
    )
    dayFraction = minutesSinceMidnight / 1440.0
    phaseDay = dayFraction
    phaseWeek = (source["time"].dt.dayofweek.to_numpy() + dayFraction) / 7.0
    yearLength = np.where(source["time"].dt.is_leap_year.to_numpy(), 366.0, 365.0)
    phaseYear = ((source["time"].dt.dayofyear.to_numpy() - 1.0) + dayFraction) / yearLength
    dailyBlock = _build_centered_fourier_block(phaseDay, 10)
    weeklyBlock = _orthogonalize_block(_build_centered_fourier_block(phaseWeek, 6), [dailyBlock])
    yearlyBlock = _orthogonalize_block(_build_centered_fourier_block(phaseYear, 8), [dailyBlock, weeklyBlock])
    trendBlock = _build_piecewise_linear_trend_block(source["time"], knots=24)
    blocks = {
        "trend": trendBlock,
        "daily_seasonality": dailyBlock,
        "weekly_seasonality": weeklyBlock,
        "yearly_seasonality": yearlyBlock,
    }
    alphas = {
        "trend": 10.0,
        "daily_seasonality": 1.5,
        "weekly_seasonality": 2.5,
        "yearly_seasonality": 6.0,
    }
    components = {name: np.zeros_like(y, dtype=float) for name in blocks}
    weights = np.ones_like(y, dtype=float)
    for _ in range(10):
        for blockName, X in blocks.items():
            other = [components[name] for name in blocks if name != blockName]
            otherSum = np.sum(np.column_stack(other), axis=1) if other else np.zeros_like(y)
            beta = _solve_weighted_ridge(
                X,
                y - otherSum,
                weights,
                float(alphas.get(blockName, 1.0)),
            )
            component = X @ beta
            if blockName.endswith("seasonality"):
                component = component - component.mean()
            components[blockName] = component
        fitted = np.sum(np.column_stack([components[name] for name in blocks]), axis=1)
        residual = y - fitted
        weights = _build_huber_weights(residual, _robust_mad_scale(residual), deltaMultiplier=1.5)
    result = source[["time", "gpu"]].copy()
    result["slot_of_day"] = (minutesSinceMidnight // INTERVAL_MINUTES).astype(int) % (24 * 60 // INTERVAL_MINUTES)
    result["slot_of_week"] = source["time"].dt.dayofweek.to_numpy() * (24 * 60 // INTERVAL_MINUTES) + result["slot_of_day"]
    result["month"] = source["time"].dt.month.to_numpy()
    result["gpu_daily_seasonality"] = components["daily_seasonality"]
    result["gpu_weekly_seasonality"] = components["weekly_seasonality"]
    result["gpu_yearly_seasonality"] = components["yearly_seasonality"]
    return result


def build_seasonality_components(
    *,
    seriesDir: str | Path,
    now: datetime | None = None,
) -> dict:
    effectiveNow = now or datetime.now()
    rawFrame = load_overall_series_frame(seriesDir)
    if rawFrame.empty:
        return {"year": effectiveNow.year, "daily": [], "weekly": [], "yearly": [], "method": "fourier_additive_profiles"}
    regularFrame = _regularize_15m_grid(rawFrame)
    maintenanceWindows = _detect_zero_utilization_windows(rawFrame)
    featureFrame = _build_feature_frame(
        regularFrame,
        calendarPath=Path(academicCalendarRoot).resolve(),
        maintenanceWindows=maintenanceWindows,
    )
    currentYear = int(effectiveNow.year)
    yearFrame = featureFrame.loc[featureFrame["time"].dt.year == currentYear].copy()
    yearUsed = currentYear
    if yearFrame.empty:
        latestTimestamp = featureFrame["time"].max()
        yearUsed = int(pd.Timestamp(latestTimestamp).year)
        yearFrame = featureFrame.loc[featureFrame["time"].dt.year == yearUsed].copy()
    decomposition = _decompose_gpu_multiseasonality(yearFrame)
    if decomposition.empty:
        return {"year": yearUsed, "daily": [], "weekly": [], "yearly": [], "method": "fourier_additive_profiles"}

    dailyFrame = decomposition.groupby("slot_of_day", as_index=False)["gpu_daily_seasonality"].mean()
    daily = [
        {
            "x": f"{int(row.slot_of_day) // 4:02d}:{(int(row.slot_of_day) % 4) * 15:02d}",
            "y": round(float(row.gpu_daily_seasonality), 2),
        }
        for row in dailyFrame.itertuples(index=False)
    ]
    weeklyFrame = decomposition.groupby("slot_of_week", as_index=False)["gpu_weekly_seasonality"].mean()
    weekly = [
        {
            "x": round(float(row.slot_of_week) / (24 * 60 / INTERVAL_MINUTES), 2),
            "label": ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"][int(row.slot_of_week) // (24 * 60 // INTERVAL_MINUTES)],
            "y": round(float(row.gpu_weekly_seasonality), 2),
        }
        for row in weeklyFrame.itertuples(index=False)
    ]
    yearlyFrame = decomposition.groupby("month", as_index=False)["gpu_yearly_seasonality"].mean()
    yearly = [
        {
            "x": int(row.month),
            "label": ["Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"][int(row.month) - 1],
            "y": round(float(row.gpu_yearly_seasonality), 2),
        }
        for row in yearlyFrame.itertuples(index=False)
    ]
    return {
        "year": yearUsed,
        "daily": daily,
        "weekly": weekly,
        "yearly": yearly,
        "method": "fourier_additive_profiles",
    }


def _build_forecast_window_payload(
    *,
    forecastPoints: list[dict],
    latestObservationTime: str | None,
    latestPredictionClipped: float,
) -> dict | None:
    if forecastPoints:
        firstPoint = forecastPoints[0]
        return {
            "start_at": firstPoint.get("window_start_at") or firstPoint.get("time"),
            "end_at": firstPoint.get("window_end_at"),
            "predicted_gpu_percent": round(float(firstPoint.get("predicted_gpu_mean_6h", 0.0)), 2),
        }
    if not latestObservationTime:
        return None
    try:
        latestObservation = datetime.fromisoformat(str(latestObservationTime))
    except ValueError:
        return None
    return {
        "start_at": latestObservation.isoformat(timespec="seconds"),
        "end_at": (
            latestObservation + timedelta(minutes=TARGET_HORIZON_MINUTES)
        ).isoformat(timespec="seconds"),
        "predicted_gpu_percent": round(float(latestPredictionClipped), 2),
    }


def load_artifact(
    modelDir: str | Path,
    includeModel: bool = False,
    versionId: str | None = None,
) -> ForecastArtifact | None:
    paths = artifact_paths(modelDir, versionId=versionId)
    if not paths["metadata"].exists():
        return None
    with open(paths["metadata"], "r", encoding="utf-8") as file:
        metadata = json.load(file)
    model = None
    if includeModel and paths["model"].exists():
        with open(paths["model"], "rb") as file:
            model = pickle.load(file)
    return ForecastArtifact(metadata=metadata, model=model)


def load_forecast_insights(modelDir: str | Path, versionId: str | None = None) -> dict | None:
    paths = artifact_paths(modelDir, versionId=versionId)
    if not paths["insights"].exists():
        return None
    with open(paths["insights"], "r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        return None
    return payload


def list_artifact_versions(modelDir: str | Path) -> list[dict]:
    versionsRoot = Path(modelDir).resolve() / DEFAULT_MODEL_VERSIONS_DIRNAME
    if not versionsRoot.exists():
        return []
    versions = []
    for metadataPath in sorted(versionsRoot.glob(f"*/{DEFAULT_METADATA_FILENAME}")):
        try:
            with open(metadataPath, "r", encoding="utf-8") as file:
                metadata = json.load(file)
        except (OSError, json.JSONDecodeError):
            continue
        versions.append(
            {
                "model_version_id": metadata.get("model_version_id") or metadataPath.parent.name,
                "trained_at": metadata.get("trained_at"),
                "model_kind": metadata.get("model_kind"),
                "target_name": metadata.get("target_name"),
                "training_row_count": metadata.get("training_row_count"),
                "path": str(metadataPath.parent),
            }
        )
    return sorted(versions, key=lambda item: str(item.get("trained_at") or ""), reverse=True)


def _write_artifact_files(paths: dict[str, Path], model, metadata: dict, insights: dict | None = None):
    paths["root"].mkdir(parents=True, exist_ok=True)
    with open(paths["model"], "wb") as file:
        pickle.dump(model, file)
    with open(paths["metadata"], "w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2, ensure_ascii=False)
    if insights is not None:
        with open(paths["insights"], "w", encoding="utf-8") as file:
            json.dump(insights, file, indent=2, ensure_ascii=False)


def _save_artifact(
    modelDir: str | Path,
    model,
    metadata: dict,
    insights: dict | None = None,
) -> ForecastArtifact:
    latestPaths = artifact_paths(modelDir)
    modelVersionId = _resolve_unique_model_version_id(
        modelDir,
        metadata.get("model_version_id") or _build_model_version_id(metadata),
    )
    versionPaths = artifact_paths(modelDir, versionId=modelVersionId)
    savedMetadata = dict(metadata)
    savedMetadata["model_version_id"] = modelVersionId
    savedMetadata["model_version_dir"] = str(versionPaths["root"])
    savedMetadata["latest_model_dir"] = str(latestPaths["root"])
    savedInsights = dict(insights or {})
    if savedInsights:
        savedInsights["model_version_id"] = modelVersionId
        savedInsights["model_version_dir"] = str(versionPaths["root"])
        savedInsights["latest_model_dir"] = str(latestPaths["root"])
    _write_artifact_files(versionPaths, model, savedMetadata, savedInsights or None)
    _write_artifact_files(latestPaths, model, savedMetadata, savedInsights or None)
    return ForecastArtifact(metadata=savedMetadata, model=model)


def _fit_gradient_boosting_model(
    trainingFrame: pd.DataFrame,
    featureColumns: list[str],
    eventWriter=None,
) -> tuple[object, str]:
    XTrain = trainingFrame[featureColumns].to_numpy(dtype=float)
    yTrain = trainingFrame[TARGET_COLUMN].to_numpy(dtype=float)
    model = make_catboost_regressor()

    class _CatBoostLogStream:
        def __init__(self, eventWriter):
            self.eventWriter = eventWriter
            self.buffer = ""

        def write(self, chunk):
            text = str(chunk or "")
            if not text:
                return
            self.buffer += text
            while "\n" in self.buffer:
                line, self.buffer = self.buffer.split("\n", 1)
                self._emit_line(line)

        def flush(self):
            if self.buffer:
                self._emit_line(self.buffer)
                self.buffer = ""

        def _emit_line(self, line):
            message = str(line).strip()
            if not message:
                return
            logger.info(f"CatBoost training | {message}")
            if eventWriter is not None:
                eventWriter(
                    build_runtime_log_event(
                        category="forecast_runtime",
                        status="CATBOOST_TRAINING_LOG",
                        eventType="CATBOOST_TRAINING_LOG",
                        message=message,
                        source="forecast.training.catboost",
                    )
                )

    logStream = _CatBoostLogStream(eventWriter)
    model.fit(XTrain, yTrain, verbose=25, log_cout=logStream, log_cerr=logStream)
    logStream.flush()
    return model, DEFAULT_MODEL_KIND


def train_gradient_boosting_forecast(
    *,
    dataDir: str | Path,
    modelDir: str | Path,
    projectRoot: str | Path,
    refreshData: bool = True,
    now: datetime | None = None,
    eventWriter=None,
    timezoneName: str = "Europe/Moscow",
    modelUpdateIntervalHours: int | float = DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
    forecastPredictionHorizonHours: int | float = DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
) -> ForecastArtifact:
    effectiveNow = coerce_datetime_timezone(now or now_in_timezone(timezoneName), timezoneName)
    resolvedModelDir = resolve_model_dir(projectRoot, modelDir)
    resolvedDataDir = resolve_data_dir(projectRoot, dataDir)
    eventTimestamp = int(effectiveNow.timestamp())

    if eventWriter is not None:
        eventWriter(
            build_runtime_log_event(
                category="forecast_runtime",
                status="TRAINING_STARTED",
                eventType="TRAINING_STARTED",
                message="Forecast model training started.",
                timestamp=eventTimestamp,
                source="forecast.training",
                data_dir=str(resolvedDataDir),
                model_dir=str(resolvedModelDir),
                refresh_data=bool(refreshData),
            )
        )

    if refreshData:
        logger.info(f"Refreshing utilization export before model training into '{resolvedDataDir}'")
        storage = slurmStorage().create()
        try:
            storage.exportIncrementalHistoricalUtilization(outputDir=str(resolvedDataDir))
        finally:
            storage.close()

    trainingFrame, featureColumns = build_training_frame(resolvedDataDir)
    fitFrame = trainingFrame.dropna(subset=[TARGET_COLUMN]).copy()
    if fitFrame.empty:
        raise RuntimeError("Forecast training frame is empty after dropping rows without target")

    model, modelKind = _fit_gradient_boosting_model(fitFrame, featureColumns, eventWriter)

    futureForecast = build_future_forecast_points(
        model=model,
        seriesDir=resolvedDataDir,
        featureColumns=featureColumns,
        now=effectiveNow,
        horizonHours=forecastPredictionHorizonHours,
    )
    forecastPoints = futureForecast.get("points", [])
    firstPrediction = forecastPoints[0] if forecastPoints else {}
    latestPredictionClipped = float(firstPrediction.get("predicted_gpu_mean_6h", 0.0))
    latestPredictionRaw = float(firstPrediction.get("predicted_gpu_mean_6h_raw", latestPredictionClipped))
    seasonality = {
        "year": None,
        "daily": [],
        "weekly": [],
        "yearly": [],
        "method": "fourier_additive_profiles",
    }
    seasonalityError = None
    try:
        seasonality = build_seasonality_components(
            seriesDir=resolvedDataDir,
            now=effectiveNow,
        )
    except Exception as error:
        seasonalityError = str(error)
        logger.warning(f"Failed to cache forecast seasonality in forecast insights JSON: {error}")

    metadata = {
        "artifact_version": 1,
        "trained_at": effectiveNow.isoformat(timespec="seconds"),
        "model_kind": modelKind,
        "target_name": TARGET_COLUMN,
        "target_horizon_minutes": TARGET_HORIZON_MINUTES,
        "target_horizon_steps": TARGET_HORIZON_STEPS,
        "target_aggregation": TARGET_AGGREGATION,
        "forecast_prediction_horizon_hours": forecastPredictionHorizonHours,
        "interval_minutes": INTERVAL_MINUTES,
        "trained_feature_name": DEFAULT_FEATURE_NAME,
        "feature_columns": featureColumns,
        "known_future_features": [
            featureName
            for featureName in KNOWN_FUTURE_FEATURE_CANDIDATES
            if featureName in featureColumns
        ],
        "training_row_count": int(len(fitFrame)),
        "latest_observation_time": futureForecast.get("current_observation_time"),
        "current_gpu_percent": futureForecast.get("current_gpu_percent"),
        "forecast_start_at": futureForecast.get("forecast_start_at"),
        "forecast_end_at": futureForecast.get("forecast_end_at"),
        "last_prediction_gpu_percent": latestPredictionClipped,
        "last_prediction_gpu_percent_raw": latestPredictionRaw,
        "forecast_data_dir": str(resolvedDataDir),
        "model_dir": str(resolvedModelDir),
        "model_update_interval_hours": modelUpdateIntervalHours,
        "timezone": timezoneName,
        "next_refresh_due_at": (
            effectiveNow + timedelta(hours=float(modelUpdateIntervalHours))
        ).isoformat(timespec="seconds"),
    }
    insights = {
        "available": True,
        "generated_at": effectiveNow.isoformat(timespec="seconds"),
        "trained_at": metadata["trained_at"],
        "model_kind": modelKind,
        "target_name": TARGET_COLUMN,
        "target_horizon_minutes": TARGET_HORIZON_MINUTES,
        "forecast_prediction_horizon_hours": forecastPredictionHorizonHours,
        "training_row_count": int(len(fitFrame)),
        "latest_observation_time": futureForecast.get("current_observation_time"),
        "current_gpu_percent": futureForecast.get("current_gpu_percent"),
        "forecast_data_dir": str(resolvedDataDir),
        "model_dir": str(resolvedModelDir),
        "future_forecast": forecastPoints,
        "forecast_start_at": futureForecast.get("forecast_start_at"),
        "forecast_end_at": futureForecast.get("forecast_end_at"),
        "seasonality": seasonality,
        "forecast_window": _build_forecast_window_payload(
            forecastPoints=forecastPoints,
            latestObservationTime=futureForecast.get("current_observation_time"),
            latestPredictionClipped=latestPredictionClipped,
        ),
        "prediction_error": None if forecastPoints else "Forecast points are empty.",
        "seasonality_error": seasonalityError,
        "seasonality_method_note": (
            "Seasonality uses the notebook-style additive Fourier decomposition split into daily, weekly, and yearly GPU components. "
            "Calendar effects are kept out of the seasonal profiles."
        ),
    }
    artifact = _save_artifact(resolvedModelDir, model, metadata, insights=insights)
    logger.success(
        f"Trained forecast model '{modelKind}' on {len(fitFrame)} rows. "
        f"Forecast GPU mean load for the next 6h: {latestPredictionClipped:.2f}%"
    )
    if eventWriter is not None:
        eventWriter(
            build_runtime_log_event(
                category="forecast_runtime",
                status="TRAINING_FINISHED",
                eventType="TRAINING_FINISHED",
                message=(
                    f"Forecast model training finished with '{modelKind}'. "
                    f"Next 6h mean GPU forecast: {latestPredictionClipped:.2f}%."
                ),
                timestamp=eventTimestamp,
                source="forecast.training",
                model_kind=modelKind,
                target_name=TARGET_COLUMN,
                training_row_count=int(len(fitFrame)),
                model_version_id=artifact.metadata.get("model_version_id"),
                model_version_dir=artifact.metadata.get("model_version_dir"),
                last_prediction_gpu_percent=latestPredictionClipped,
                forecast_point_count=len(forecastPoints),
                forecast_prediction_horizon_hours=forecastPredictionHorizonHours,
                forecast_data_dir=str(resolvedDataDir),
                model_dir=str(resolvedModelDir),
                model_update_interval_hours=modelUpdateIntervalHours,
                timezone=timezoneName,
            )
        )
    return artifact


def ensure_fresh_forecast_model(
    *,
    dataDir: str | Path,
    modelDir: str | Path,
    projectRoot: str | Path,
    skipStartupTraining: bool = False,
    now: datetime | None = None,
    eventWriter=None,
    timezoneName: str = "Europe/Moscow",
    modelUpdateIntervalHours: int | float = DEFAULT_MODEL_UPDATE_INTERVAL_HOURS,
    forecastPredictionHorizonHours: int | float = DEFAULT_FORECAST_PREDICTION_HORIZON_HOURS,
) -> ForecastArtifact | None:
    effectiveNow = coerce_datetime_timezone(now or now_in_timezone(timezoneName), timezoneName)
    artifact = load_artifact(resolve_model_dir(projectRoot, modelDir))
    if artifact is not None and not is_artifact_stale(
        artifact.metadata,
        now=effectiveNow,
        updateIntervalHours=modelUpdateIntervalHours,
        timezoneName=timezoneName,
    ):
        return artifact
    if skipStartupTraining:
        if artifact is None:
            logger.warning(
                "Forecast startup training is disabled and no existing model artifact was found. "
                "Scheduler will fall back to the historical GPU load baseline until a model is trained manually."
            )
            return None
        logger.warning(
            "Forecast model is stale, but startup training is disabled. "
            "Using the last cached prediction from the existing artifact."
        )
        return artifact
    return train_gradient_boosting_forecast(
        dataDir=dataDir,
        modelDir=modelDir,
        projectRoot=projectRoot,
        refreshData=True,
        now=effectiveNow,
        eventWriter=eventWriter,
        timezoneName=timezoneName,
        modelUpdateIntervalHours=modelUpdateIntervalHours,
        forecastPredictionHorizonHours=forecastPredictionHorizonHours,
    )
