import ast
import json
import logging
import socket
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

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
    ):
        self.mserverUrl = mserverUrl
        self.apiToken = apiToken
        self.targetQos = targetQos
        self.timeoutSeconds = float(timeoutSeconds)

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

    def executeJob(self, job, placement=None):
        if not self.targetQos:
            logger.warning(
                f"Target QoS is not configured, skipping mserver request for job {job.getID()}"
            )
            return False

        if not self.mserverUrl:
            logger.error(
                f"mserver URL is not configured, skipping job {job.getID()}"
            )
            return False

        if not self.apiToken:
            logger.error(
                f"mserver API token is not configured, skipping job {job.getID()}"
            )
            return False

        payload = self._build_payload(job)
        logger.info(
            f"Setting QoS for job {job.getID()} via mserver '{self.mserverUrl}' "
            f"(QoS={self.targetQos})"
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
            return False
        except (URLError, TimeoutError, socket.timeout, OSError) as error:
            logger.error(
                f"Failed to call mserver for job {job.getID()} "
                f"(url={self.mserverUrl}, timeout={self.timeoutSeconds:g}s): {error}"
            )
            return False

        if statusCode < 200 or statusCode >= 300:
            logger.error(
                f"mserver returned HTTP {statusCode} for job {job.getID()}: {responseBody}"
            )
            return False

        responseError = self._extract_response_error(responseBody)
        if responseError:
            logger.error(
                f"mserver failed to set QoS for job {job.getID()}: {responseError}"
            )
            return False

        if responseBody:
            logger.debug(f"mserver response for job {job.getID()}: {responseBody}")
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
