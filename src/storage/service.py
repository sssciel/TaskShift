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
    load_cached_historical_jobs,
    load_state,
    resolve_history_start,
    save_cached_historical_jobs,
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
        return self.repository.get_historical_jobs(
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
        cachedJobs = load_cached_historical_jobs(rawJobsPath)
        jobsById = {job.jobID: job for job in cachedJobs}

        historyStartTimestamp = resolve_history_start(historyStart, state)
        modifiedUntilTimestamp = parse_time_value(modifiedUntil)

        if jobsOverride is not None:
            incrementalJobs = jobsOverride
        elif state:
            incrementalJobs = self.getHistoricalJobs(
                modifiedAfter=state.get("last_mod_time"),
                modifiedUntil=modifiedUntilTimestamp,
            )
        else:
            incrementalJobs = self.getHistoricalJobs(
                modifiedFrom=historyStartTimestamp,
                modifiedUntil=modifiedUntilTimestamp,
            )

        for job in incrementalJobs:
            jobsById[job.jobID] = job

        mergedJobs = sorted(jobsById.values(), key=lambda job: (job.timeStart, job.jobID))
        save_cached_historical_jobs(rawJobsPath, mergedJobs)

        newState = build_state_payload(
            previousState=state,
            mergedJobs=mergedJobs,
            historyStartTimestamp=historyStartTimestamp,
            modifiedUntilTimestamp=modifiedUntilTimestamp,
        )
        save_state(statePath, newState)

        logger.success(
            f"Synchronized historical jobs cache in '{outputPath}': "
            f"{len(incrementalJobs)} new/updated rows, {len(mergedJobs)} total cached jobs"
        )
        return mergedJobs, outputPath, newState

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
                    "raw_jobs_file": RAW_JOBS_CACHE_FILE,
                    "state_file": STATE_FILE,
                },
                file,
                indent=2,
                ensure_ascii=False,
            )

        logger.success(f"Incremental utilization export completed in '{outputPath}'")
        return outputPath

    def close(self):
        self.repository.close()
