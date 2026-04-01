import logging

try:
    import mysql.connector
    from mysql.connector import errorcode
except ModuleNotFoundError:
    mysql = None
    errorcode = None
else:
    mysql = mysql.connector

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from config import getDBConfig

from .constants import GET_ACTIVE_JOBS_BASE_QUERY, GET_HISTORICAL_JOBS_BASE_QUERY, GET_JOBS_WITH_STATE_QUERY
from .models import HistoricalJob, Job


class SlurmDBRepository:
    def __init__(self):
        self.connection = None
        self.config = None

    def create(self):
        if mysql is None:
            raise ModuleNotFoundError("mysql-connector-python is required to connect to slurmDB.")

        self.config = getDBConfig()
        logger.debug("Create connection to slurmDB")

        try:
            self.connection = mysql.connect(**self.config.getParameters())
        except mysql.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.critical("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.critical("Database does not exist")
            else:
                logger.critical(err)

        logger.success("Connection to slurmDB was created")
        return self

    def get_jobs_with_state(self, state) -> list[Job]:
        self._require_connection()

        result = []
        cursor = self.connection.cursor()
        logger.debug(f"Executing WithStateQuery with state: {state}")
        cursor.execute(GET_JOBS_WITH_STATE_QUERY, (state,))

        for (id_job, job_name, timelimit, priority, constraints, cpus_req, tres_req, partition) in cursor:
            result.append(
                Job(
                    jobID=id_job,
                    jobName=job_name,
                    timelimit=timelimit,
                    state=state,
                    priority=priority,
                    constraints=constraints,
                    cpusReq=cpus_req or 0,
                    tresReq=tres_req,
                    partition=partition,
                )
            )

        cursor.close()
        logger.success(f"Got {len(result)} results")
        return result

    def get_historical_jobs(
        self,
        modifiedAfter: int | None = None,
        modifiedFrom: int | None = None,
        modifiedUntil: int | None = None,
    ) -> list[HistoricalJob]:
        self._require_connection()

        result = []
        cursor = self.connection.cursor()
        query = GET_HISTORICAL_JOBS_BASE_QUERY
        params = []

        if modifiedAfter is not None:
            query += " AND mod_time > %s"
            params.append(modifiedAfter)

        if modifiedFrom is not None:
            query += " AND mod_time >= %s"
            params.append(modifiedFrom)

        if modifiedUntil is not None:
            query += " AND mod_time <= %s"
            params.append(modifiedUntil)

        query += " ORDER BY mod_time ASC, id_job ASC"
        logger.debug(
            "Loading historical jobs for utilization aggregation"
            f" (modifiedAfter={modifiedAfter}, modifiedFrom={modifiedFrom}, modifiedUntil={modifiedUntil})"
        )
        cursor.execute(query, tuple(params))

        for row in cursor:
            result.append(
                HistoricalJob(
                    jobID=row[0],
                    jobName=row[1],
                    timelimit=row[2],
                    state=row[3],
                    priority=row[4],
                    constraints=row[5],
                    cpusReq=row[6] or 0,
                    nodesAlloc=row[7] or 0,
                    timeStart=row[8] or 0,
                    timeEnd=row[9] or 0,
                    timeSubmit=row[10] or 0,
                    timeEligible=row[11] or 0,
                    modTime=row[12] or 0,
                    tresReq=row[13],
                    nodelist=row[14],
                    partition=row[15],
                )
            )

        cursor.close()
        logger.success(f"Got {len(result)} historical jobs")
        return result

    def get_active_jobs(self, nowTimestamp: int) -> list[HistoricalJob]:
        self._require_connection()

        result = []
        cursor = self.connection.cursor()
        query = GET_ACTIVE_JOBS_BASE_QUERY + " ORDER BY time_start ASC, id_job ASC"

        logger.debug(f"Loading active jobs at timestamp {nowTimestamp}")
        cursor.execute(query, (nowTimestamp,))

        for row in cursor:
            result.append(
                HistoricalJob(
                    jobID=row[0],
                    jobName=row[1],
                    timelimit=row[2],
                    state=row[3],
                    priority=row[4],
                    constraints=row[5],
                    cpusReq=row[6] or 0,
                    nodesAlloc=row[7] or 0,
                    timeStart=row[8] or 0,
                    timeEnd=row[9] or 0,
                    timeSubmit=row[10] or 0,
                    timeEligible=row[11] or 0,
                    modTime=row[12] or 0,
                    tresReq=row[13],
                    nodelist=row[14],
                    partition=row[15],
                )
            )

        cursor.close()
        logger.success(f"Got {len(result)} active jobs")
        return result

    def close(self):
        if self.connection is None:
            return

        logger.info("Closing connection to slurmDB")
        self.connection.close()

    def _require_connection(self):
        if self.connection is None:
            raise RuntimeError("slurmDB connection is not created.")
