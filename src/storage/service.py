import logging
from datetime import datetime
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from config import getClusterConfig

from .cache import (
    build_state_payload,
    load_cached_historical_job_rows,
    load_state,
    resolve_history_start,
    save_cached_historical_job_rows,
    save_state,
)
from .constants import (
    DEFAULT_BUCKET_MINUTES,
    DEFAULT_EXPORT_ROOT,
    METADATA_FILE,
    PENDING_STATE,
    RAW_JOBS_CACHE_FILE,
    SERIES_DIR,
    STATE_FILE,
)
from .repository import SlurmDBRepository
from .series import build_historical_utilization_series, export_historical_utilization_series
from .timeutils import parse_time_value


class slurmStorage:
    def __init__(self):
        self.repository = SlurmDBRepository()

    def create(self):
        self.repository.create()
        return self

    def getJobsWithState(self, state):
        return self.repository.get_jobs_with_state(state)

    def getPendingJobs(self):
        logger.debug("Get pending jobs from slurm queue")
        return self.getJobsWithState(PENDING_STATE)

    def getHistoricalJobs(self, modifiedAfter=None, modifiedFrom=None, modifiedUntil=None):
        return self._materializeHistoricalJobs(
            self.repository.get_historical_job_rows(
                modifiedAfter=modifiedAfter,
                modifiedFrom=modifiedFrom,
                modifiedUntil=modifiedUntil,
            )
        )

    def getHistoricalJobRows(self, modifiedAfter=None, modifiedFrom=None, modifiedUntil=None):
        return self.repository.get_historical_job_rows(
            modifiedAfter=modifiedAfter,
            modifiedFrom=modifiedFrom,
            modifiedUntil=modifiedUntil,
        )

    def getRunningJobs(self, nowTimestamp=None):
        if nowTimestamp is None:
            nowTimestamp = int(datetime.now().timestamp())

        return self.repository.get_active_jobs(nowTimestamp)

    def syncHistoricalJobsCache(self, outputDir=None, historyStart=None, modifiedUntil=None, jobsOverride=None):
        outputPath = Path(outputDir) if outputDir is not None else DEFAULT_EXPORT_ROOT
        outputPath.mkdir(parents=True, exist_ok=True)

        statePath = outputPath / STATE_FILE
        rawJobsPath = outputPath / RAW_JOBS_CACHE_FILE

        state = load_state(statePath)
        cachedRows = load_cached_historical_job_rows(rawJobsPath)
        rowsById = {row.getLogicalKey(): row for row in cachedRows}

        historyStartTimestamp = resolve_history_start(historyStart, state)
        modifiedUntilTimestamp = parse_time_value(modifiedUntil)

        if jobsOverride is not None:
            incrementalRows = jobsOverride
        elif state:
            incrementalRows = self.getHistoricalJobRows(
                modifiedAfter=state.get("last_mod_time"),
                modifiedUntil=modifiedUntilTimestamp,
            )
        else:
            incrementalRows = self.getHistoricalJobRows(
                modifiedFrom=historyStartTimestamp,
                modifiedUntil=modifiedUntilTimestamp,
            )

        for row in incrementalRows:
            rowsById[row.getLogicalKey()] = row

        mergedRows = sorted(
            rowsById.values(),
            key=lambda row: (row.time_start, row.id_job, row.job_db_inx if row.job_db_inx is not None else -1),
        )
        save_cached_historical_job_rows(rawJobsPath, mergedRows)
        materializedJobs = self._materializeHistoricalJobs(mergedRows)

        newState = build_state_payload(
            previousState=state,
            mergedRows=mergedRows,
            historyStartTimestamp=historyStartTimestamp,
            modifiedUntilTimestamp=modifiedUntilTimestamp,
        )
        save_state(statePath, newState)

        logger.success(
            f"Synchronized historical jobs cache in '{outputPath}': "
            f"{len(incrementalRows)} new/updated raw rows, "
            f"{len(mergedRows)} total cached raw rows, {len(materializedJobs)} logical jobs"
        )
        return materializedJobs, outputPath, newState

    def buildHistoricalUtilizationSeries(self, jobs=None, clusterConfig=None, intervalMinutes=DEFAULT_BUCKET_MINUTES, nowTimestamp=None):
        return build_historical_utilization_series(
            jobs=self.getHistoricalJobs() if jobs is None else jobs,
            clusterConfig=clusterConfig,
            intervalMinutes=intervalMinutes,
            nowTimestamp=nowTimestamp,
        )

    def exportHistoricalUtilizationSeries(self, outputDir=None, jobs=None, clusterConfig=None, intervalMinutes=DEFAULT_BUCKET_MINUTES, nowTimestamp=None):
        return export_historical_utilization_series(
            outputDir=DEFAULT_EXPORT_ROOT if outputDir is None else outputDir,
            jobs=self.getHistoricalJobs() if jobs is None else jobs,
            clusterConfig=getClusterConfig() if clusterConfig is None else clusterConfig,
            intervalMinutes=intervalMinutes,
            nowTimestamp=nowTimestamp,
        )

    def exportIncrementalHistoricalUtilization(
        self,
        outputDir=None,
        intervalMinutes=DEFAULT_BUCKET_MINUTES,
        nowTimestamp=None,
        historyStart=None,
        modifiedUntil=None,
        clusterConfig=None,
        jobsOverride=None,
    ):
        cachedJobs, outputPath, state = self.syncHistoricalJobsCache(
            outputDir=outputDir,
            historyStart=historyStart,
            modifiedUntil=modifiedUntil,
            jobsOverride=jobsOverride,
        )

        seriesOutputPath = outputPath / SERIES_DIR
        export_historical_utilization_series(
            outputDir=seriesOutputPath,
            jobs=cachedJobs,
            clusterConfig=getClusterConfig() if clusterConfig is None else clusterConfig,
            intervalMinutes=intervalMinutes,
            nowTimestamp=parse_time_value(nowTimestamp),
        )

        metadataPath = outputPath / METADATA_FILE
        with open(metadataPath, "w", encoding="utf-8") as file:
            import json

            json.dump(
                {
                    "generated_at": datetime.now().isoformat(),
                    "interval_minutes": intervalMinutes,
                    "history_start": state.get("history_start"),
                    "modified_until": state.get("modified_until"),
                    "last_mod_time": state.get("last_mod_time"),
                    "job_count": state.get("job_count"),
                    "series_dir": SERIES_DIR,
                    "raw_rows_file": RAW_JOBS_CACHE_FILE,
                    "state_file": STATE_FILE,
                },
                file,
                indent=2,
                ensure_ascii=False,
            )

        logger.success(f"Incremental utilization export completed in '{outputPath}'")
        return outputPath

    def rebuildHistoricalUtilizationFromCache(
        self,
        outputDir=None,
        intervalMinutes=DEFAULT_BUCKET_MINUTES,
        nowTimestamp=None,
        clusterConfig=None,
    ):
        outputPath = Path(outputDir) if outputDir is not None else DEFAULT_EXPORT_ROOT
        rawJobsPath = outputPath / RAW_JOBS_CACHE_FILE
        statePath = outputPath / STATE_FILE

        cachedRows = load_cached_historical_job_rows(rawJobsPath)
        if not cachedRows:
            raise FileNotFoundError(
                f"Raw cache file '{rawJobsPath}' not found or empty. Run full export first."
            )

        state = load_state(statePath)
        materializedJobs = self._materializeHistoricalJobs(cachedRows)

        seriesOutputPath = outputPath / SERIES_DIR
        export_historical_utilization_series(
            outputDir=seriesOutputPath,
            jobs=materializedJobs,
            clusterConfig=getClusterConfig() if clusterConfig is None else clusterConfig,
            intervalMinutes=intervalMinutes,
            nowTimestamp=parse_time_value(nowTimestamp),
        )

        metadataPath = outputPath / METADATA_FILE
        with open(metadataPath, "w", encoding="utf-8") as file:
            import json

            json.dump(
                {
                    "generated_at": datetime.now().isoformat(),
                    "interval_minutes": intervalMinutes,
                    "history_start": state.get("history_start"),
                    "modified_until": state.get("modified_until"),
                    "last_mod_time": state.get("last_mod_time"),
                    "job_count": len(cachedRows),
                    "logical_job_count": len(materializedJobs),
                    "series_dir": SERIES_DIR,
                    "raw_rows_file": RAW_JOBS_CACHE_FILE,
                    "state_file": STATE_FILE,
                    "source": "raw_cache_only",
                },
                file,
                indent=2,
                ensure_ascii=False,
            )

        logger.success(f"Historical utilization series rebuilt from raw cache in '{outputPath}'")
        return outputPath

    def close(self):
        self.repository.close()

    def _materializeHistoricalJobs(self, rawRows):
        if not rawRows:
            return []

        rowsByJobId = {}
        duplicateVersions = 0
        for row in rawRows:
            logicalKey = row.getLogicalKey()
            current = rowsByJobId.get(logicalKey)
            if current is None:
                rowsByJobId[logicalKey] = row
                continue

            duplicateVersions += 1
            rowsByJobId[logicalKey] = self._selectPreferredRawRow(current, row)

        if duplicateVersions > 0:
            logger.warning(
                f"Collapsed {duplicateVersions} duplicate raw job versions while materializing historical jobs"
            )

        logicalJobs = [row.toHistoricalJob() for row in rowsByJobId.values()]
        return sorted(
            logicalJobs,
            key=lambda job: (job.timeStart, job.jobID, job.dbIndex if job.dbIndex is not None else -1),
        )

    def _selectPreferredRawRow(self, left, right):
        leftScore = self._scoreRawRow(left)
        rightScore = self._scoreRawRow(right)
        if rightScore > leftScore:
            return right

        if leftScore > rightScore:
            return left

        if (right.mod_time or 0) > (left.mod_time or 0):
            return right

        return left

    def _scoreRawRow(self, row):
        hasAssignedNodes = bool(row.nodelist) and row.nodelist != "None assigned"
        return (
            1 if hasAssignedNodes else 0,
            1 if (row.time_end or 0) > 0 else 0,
            1 if row.tres_alloc else 0,
            int(row.mod_time or 0),
        )
