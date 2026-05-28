import ast
import json
import logging
import socket
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from config.logger import build_runtime_log_event

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info


DEFAULT_TIMEOUT_SECONDS = 30


class SlurmConnector:
    """
    Sends SLURM QoS changes to the external mserver API.

    TaskShift schedules one job at a time. For each runnable job the connector
    posts the mserver payload:

        {"jobs": [JOB_ID], "qos": TARGET_QOS}

    mserver is responsible for running the cluster-side script that applies the
    QoS change with SLURM.
    """

    def __init__(
        self,
        mserverUrl: str | None = None,
        apiToken: str | None = None,
        targetQos: str | None = None,
        timeoutSeconds: int | float = DEFAULT_TIMEOUT_SECONDS,
        jobRuntimeEventWriter=None,
    ):
        self.mserverUrl = mserverUrl
        self.apiToken = apiToken
        self.targetQos = targetQos
        self.timeoutSeconds = float(timeoutSeconds)
        self.jobRuntimeEventWriter = jobRuntimeEventWriter

    def _build_payload(self, job) -> dict:
        return {
            "jobs": [int(job.getID())],
            "qos": self.targetQos,
        }

    def _build_request(self, payload: dict) -> Request:
        body = json.dumps(payload).encode("utf-8")
        return Request(
            self.mserverUrl,
            data=body,
            method="POST",
            headers={
                "API_TOKEN": self.apiToken,
                "Content-Type": "application/json",
            },
        )

    def executeJob(self, job, placement=None, runId: str | None = None):
        if not self.targetQos:
            logger.warning(
                f"Target QoS is not configured, skipping mserver request for job {job.getID()}"
            )
            self._write_job_runtime_event(
                build_runtime_log_event(
                    category="job_runtime",
                    status="MSERVER_SKIPPED",
                    level="WARNING",
                    eventType="MSERVER_REQUEST",
                    message=f"Skipped mserver request for job {job.getID()}: target QoS is not configured.",
                    run_id=runId,
                    source="scheduler.connector",
                    job_id=job.getID(),
                    job_name=getattr(job, "jobName", None),
                    partition=getattr(job, "partition", None),
                    reason="target_qos_not_configured",
                )
            )
            return False

        if not self.mserverUrl:
            logger.error(
                f"mserver URL is not configured, skipping job {job.getID()}"
            )
            self._write_job_runtime_event(
                build_runtime_log_event(
                    category="job_runtime",
                    status="MSERVER_SKIPPED",
                    level="ERROR",
                    eventType="MSERVER_REQUEST",
                    message=f"Skipped mserver request for job {job.getID()}: URL is not configured.",
                    run_id=runId,
                    source="scheduler.connector",
                    job_id=job.getID(),
                    job_name=getattr(job, "jobName", None),
                    partition=getattr(job, "partition", None),
                    reason="mserver_url_not_configured",
                )
            )
            return False

        if not self.apiToken:
            logger.error(
                f"mserver API token is not configured, skipping job {job.getID()}"
            )
            self._write_job_runtime_event(
                build_runtime_log_event(
                    category="job_runtime",
                    status="MSERVER_SKIPPED",
                    level="ERROR",
                    eventType="MSERVER_REQUEST",
                    message=f"Skipped mserver request for job {job.getID()}: API token is not configured.",
                    run_id=runId,
                    source="scheduler.connector",
                    job_id=job.getID(),
                    job_name=getattr(job, "jobName", None),
                    partition=getattr(job, "partition", None),
                    reason="mserver_api_token_not_configured",
                )
            )
            return False

        payload = self._build_payload(job)
        logger.info(
            f"Setting QoS for job {job.getID()} via mserver '{self.mserverUrl}' "
            f"(QoS={self.targetQos})"
        )
        self._write_job_runtime_event(
            build_runtime_log_event(
                category="job_runtime",
                status="MSERVER_REQUEST",
                eventType="MSERVER_REQUEST",
                message=f"Sending mserver QoS request for job {job.getID()} to '{self.mserverUrl}'.",
                run_id=runId,
                source="scheduler.connector",
                job_id=job.getID(),
                job_name=getattr(job, "jobName", None),
                partition=getattr(job, "partition", None),
                qos=self.targetQos,
                request_url=self.mserverUrl,
                request_payload=payload,
                feature=getattr(placement, "featureName", None),
                nodes=getattr(placement, "nodeNames", None),
            )
        )

        try:
            request = self._build_request(payload)
            with urlopen(request, timeout=self.timeoutSeconds) as response:
                statusCode = getattr(response, "status", None)
                if statusCode is None:
                    statusCode = response.getcode()
                responseBody = response.read().decode("utf-8", errors="replace").strip()
        except HTTPError as error:
            errorBody = error.read().decode("utf-8", errors="replace").strip()
            logger.error(
                f"mserver rejected QoS request for job {job.getID()} "
                f"(HTTP {error.code}): {errorBody or getattr(error, 'reason', error.msg)}"
            )
            self._write_job_runtime_event(
                build_runtime_log_event(
                    category="job_runtime",
                    status="MSERVER_REJECTED",
                    level="ERROR",
                    eventType="MSERVER_RESPONSE",
                    message=f"mserver rejected job {job.getID()} with HTTP {error.code}.",
                    run_id=runId,
                    source="scheduler.connector",
                    job_id=job.getID(),
                    job_name=getattr(job, "jobName", None),
                    http_status=error.code,
                    response_body=errorBody,
                )
            )
            return False
        except (URLError, TimeoutError, socket.timeout, OSError) as error:
            logger.error(
                f"Failed to call mserver for job {job.getID()} "
                f"(url={self.mserverUrl}, timeout={self.timeoutSeconds:g}s): {error}"
            )
            self._write_job_runtime_event(
                build_runtime_log_event(
                    category="job_runtime",
                    status="MSERVER_ERROR",
                    level="ERROR",
                    eventType="MSERVER_RESPONSE",
                    message=f"mserver request failed for job {job.getID()}: {error}",
                    run_id=runId,
                    source="scheduler.connector",
                    job_id=job.getID(),
                    job_name=getattr(job, "jobName", None),
                    request_url=self.mserverUrl,
                    timeout_seconds=self.timeoutSeconds,
                    error=str(error),
                )
            )
            return False

        if statusCode < 200 or statusCode >= 300:
            logger.error(
                f"mserver returned HTTP {statusCode} for job {job.getID()}: {responseBody}"
            )
            self._write_job_runtime_event(
                build_runtime_log_event(
                    category="job_runtime",
                    status="MSERVER_ERROR",
                    level="ERROR",
                    eventType="MSERVER_RESPONSE",
                    message=f"mserver returned HTTP {statusCode} for job {job.getID()}.",
                    run_id=runId,
                    source="scheduler.connector",
                    job_id=job.getID(),
                    job_name=getattr(job, "jobName", None),
                    http_status=statusCode,
                    response_body=responseBody,
                )
            )
            return False

        responseError = self._extract_response_error(responseBody)
        if responseError:
            logger.error(
                f"mserver failed to set QoS for job {job.getID()}: {responseError}"
            )
            self._write_job_runtime_event(
                build_runtime_log_event(
                    category="job_runtime",
                    status="MSERVER_ERROR",
                    level="ERROR",
                    eventType="MSERVER_RESPONSE",
                    message=f"mserver reported a QoS application error for job {job.getID()}.",
                    run_id=runId,
                    source="scheduler.connector",
                    job_id=job.getID(),
                    job_name=getattr(job, "jobName", None),
                    response_body=responseBody,
                    error=responseError,
                )
            )
            return False

        if responseBody:
            logger.debug(f"mserver response for job {job.getID()}: {responseBody}")
        self._write_job_runtime_event(
            build_runtime_log_event(
                category="job_runtime",
                status="MSERVER_RESPONSE",
                eventType="MSERVER_RESPONSE",
                message=f"mserver accepted job {job.getID()} QoS request.",
                run_id=runId,
                source="scheduler.connector",
                job_id=job.getID(),
                job_name=getattr(job, "jobName", None),
                http_status=statusCode,
                response_body=responseBody,
            )
        )
        return True

    def _extract_response_error(self, responseBody: str) -> str | None:
        if not responseBody:
            return None

        parsed = None
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(responseBody)
                break
            except (ValueError, SyntaxError):
                continue

        if not isinstance(parsed, dict):
            return None

        if parsed.get("success") is False:
            return str(parsed.get("error") or parsed)

        return None

    def _write_job_runtime_event(self, event: dict):
        if self.jobRuntimeEventWriter is None:
            return
        self.jobRuntimeEventWriter(event)
