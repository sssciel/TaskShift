import json
import logging
import pickle
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from config.calendar import ConferenceCalendarConfig
from config.paths import academicCalendarRoot
from storage import slurmStorage

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
TARGET_AGGREGATION = "median"
TARGET_QUANTILE = 0.50
TRAINING_WEEKDAYS = {1, 4}
TRAINING_TIME = time(hour=0, minute=0)
DEFAULT_MODEL_FILENAME = "model.pkl"
DEFAULT_METADATA_FILENAME = "metadata.json"
DEFAULT_MODEL_KIND = "catboost_regressor"
DEFAULT_LIGHTGBM_MODEL_KIND = "lightgbm_regressor"
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


def resolve_model_dir(projectRoot: str | Path, modelDir: str | Path) -> Path:
    path = Path(modelDir)
    if path.is_absolute():
        return path
    return (Path(projectRoot).resolve() / path).resolve()


def artifact_paths(modelDir: str | Path) -> dict[str, Path]:
    root = Path(modelDir).resolve()
    return {
        "root": root,
        "model": root / DEFAULT_MODEL_FILENAME,
        "metadata": root / DEFAULT_METADATA_FILENAME,
    }


def latest_scheduled_training_at(now: datetime | None = None) -> datetime:
    current = now or datetime.now()
    for dayOffset in range(0, 14):
        candidateDate = current.date() - timedelta(days=dayOffset)
        if candidateDate.weekday() not in TRAINING_WEEKDAYS:
            continue
        candidate = datetime.combine(candidateDate, TRAINING_TIME)
        if candidate <= current:
            return candidate
    raise RuntimeError("Could not resolve latest scheduled forecast training time")


def is_artifact_stale(metadata: dict | None, now: datetime | None = None) -> bool:
    if not metadata:
        return True
    trainedAtValue = metadata.get("trained_at")
    if not trainedAtValue:
        return True
    trainedAt = datetime.fromisoformat(str(trainedAtValue))
    return trainedAt < latest_scheduled_training_at(now=now)


def import_catboost():
    try:
        from catboost import CatBoostRegressor
    except (ImportError, OSError):
        return None
    return CatBoostRegressor


def import_lightgbm():
    try:
        import lightgbm as lgb
    except (ImportError, OSError):
        return None
    return lgb


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
        verbose=False,
        allow_writing_files=False,
        thread_count=-1,
    )


def make_lightgbm_regressor():
    lgb = import_lightgbm()
    if lgb is None:
        raise ImportError("lightgbm is not available")
    return lgb.LGBMRegressor(
        objective="regression",
        n_estimators=400,
        learning_rate=0.05,
        num_leaves=63,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_samples=50,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
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


def _future_quantile_target(series: pd.Series, horizonSteps: int, quantile: float) -> pd.Series:
    shifted = series.shift(-1)
    values = shifted.rolling(window=horizonSteps, min_periods=horizonSteps).quantile(quantile)
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


def build_training_frame(seriesDir: str | Path, calendarRoot: str | Path | None = None) -> tuple[pd.DataFrame, list[str]]:
    resolvedSeriesDir = resolve_series_dir(seriesDir)
    calendarPath = Path(calendarRoot or academicCalendarRoot).resolve()
    rawFrame = _load_overall_series(resolvedSeriesDir)
    regularFrame = _regularize_15m_grid(rawFrame)
    frame = _add_time_features(regularFrame)
    frame = _add_lag_features(frame)
    frame = _add_rolling_features(frame)
    maintenanceWindows = _detect_zero_utilization_windows(rawFrame)
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
    frame["gpu_target_median_6h"] = _future_quantile_target(
        frame["gpu"], horizonSteps=TARGET_HORIZON_STEPS, quantile=TARGET_QUANTILE
    )
    featureColumns = _select_feature_columns(frame)
    return frame.sort_values("time").reset_index(drop=True), featureColumns


def load_artifact(modelDir: str | Path, includeModel: bool = False) -> ForecastArtifact | None:
    paths = artifact_paths(modelDir)
    if not paths["metadata"].exists():
        return None
    with open(paths["metadata"], "r", encoding="utf-8") as file:
        metadata = json.load(file)
    model = None
    if includeModel and paths["model"].exists():
        with open(paths["model"], "rb") as file:
            model = pickle.load(file)
    return ForecastArtifact(metadata=metadata, model=model)


def _save_artifact(modelDir: str | Path, model, metadata: dict) -> ForecastArtifact:
    paths = artifact_paths(modelDir)
    paths["root"].mkdir(parents=True, exist_ok=True)
    with open(paths["model"], "wb") as file:
        pickle.dump(model, file)
    with open(paths["metadata"], "w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2, ensure_ascii=False)
    return ForecastArtifact(metadata=metadata, model=model)


def _fit_gradient_boosting_model(trainingFrame: pd.DataFrame, featureColumns: list[str]) -> tuple[object, str]:
    XTrain = trainingFrame[featureColumns].to_numpy(dtype=float)
    yTrain = trainingFrame["gpu_target_median_6h"].to_numpy(dtype=float)
    if import_catboost() is not None:
        model = make_catboost_regressor()
        model.fit(XTrain, yTrain)
        return model, DEFAULT_MODEL_KIND
    if import_lightgbm() is not None:
        model = make_lightgbm_regressor()
        model.fit(XTrain, yTrain)
        return model, DEFAULT_LIGHTGBM_MODEL_KIND
    raise ImportError("Neither catboost nor lightgbm is available for forecast model training")


def train_gradient_boosting_forecast(
    *,
    dataDir: str | Path,
    modelDir: str | Path,
    projectRoot: str | Path,
    refreshData: bool = True,
    now: datetime | None = None,
) -> ForecastArtifact:
    effectiveNow = now or datetime.now()
    resolvedModelDir = resolve_model_dir(projectRoot, modelDir)
    resolvedDataDir = Path(dataDir).resolve()

    if refreshData:
        logger.info(f"Refreshing utilization export before model training into '{resolvedDataDir}'")
        storage = slurmStorage().create()
        try:
            storage.exportIncrementalHistoricalUtilization(outputDir=str(resolvedDataDir))
        finally:
            storage.close()

    trainingFrame, featureColumns = build_training_frame(resolvedDataDir)
    fitFrame = trainingFrame.dropna(subset=["gpu_target_median_6h"]).copy()
    if fitFrame.empty:
        raise RuntimeError("Forecast training frame is empty after dropping rows without target")

    model, modelKind = _fit_gradient_boosting_model(fitFrame, featureColumns)

    latestFeatureFrame = trainingFrame.dropna(subset=featureColumns).copy()
    if latestFeatureFrame.empty:
        raise RuntimeError("No rows are available for forecast inference after feature engineering")
    latestRow = latestFeatureFrame.iloc[[-1]].copy()
    latestPredictionRaw = float(model.predict(latestRow[featureColumns].to_numpy(dtype=float))[0])
    latestPredictionClipped = float(np.clip(latestPredictionRaw, MIN_UTILIZATION, MAX_UTILIZATION))

    metadata = {
        "artifact_version": 1,
        "trained_at": effectiveNow.isoformat(timespec="seconds"),
        "model_kind": modelKind,
        "target_name": "gpu_target_median_6h",
        "target_horizon_minutes": TARGET_HORIZON_MINUTES,
        "target_horizon_steps": TARGET_HORIZON_STEPS,
        "target_aggregation": TARGET_AGGREGATION,
        "interval_minutes": INTERVAL_MINUTES,
        "trained_feature_name": DEFAULT_FEATURE_NAME,
        "feature_columns": featureColumns,
        "known_future_features": [
            featureName
            for featureName in KNOWN_FUTURE_FEATURE_CANDIDATES
            if featureName in featureColumns
        ],
        "training_row_count": int(len(fitFrame)),
        "latest_observation_time": pd.Timestamp(latestRow.iloc[0]["time"]).isoformat(),
        "last_prediction_gpu_percent": latestPredictionClipped,
        "last_prediction_gpu_percent_raw": latestPredictionRaw,
        "forecast_data_dir": str(resolvedDataDir),
        "model_dir": str(resolvedModelDir),
        "scheduled_due_at": latest_scheduled_training_at(now=effectiveNow).isoformat(timespec="seconds"),
    }
    artifact = _save_artifact(resolvedModelDir, model, metadata)
    logger.success(
        f"Trained forecast model '{modelKind}' on {len(fitFrame)} rows. "
        f"Cached GPU prediction for the next 6h median load: {latestPredictionClipped:.2f}%"
    )
    return artifact


def ensure_fresh_forecast_model(
    *,
    dataDir: str | Path,
    modelDir: str | Path,
    projectRoot: str | Path,
    skipStartupTraining: bool = False,
    now: datetime | None = None,
) -> ForecastArtifact | None:
    effectiveNow = now or datetime.now()
    artifact = load_artifact(resolve_model_dir(projectRoot, modelDir))
    if artifact is not None and not is_artifact_stale(artifact.metadata, now=effectiveNow):
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
    )
