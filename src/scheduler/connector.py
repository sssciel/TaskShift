import logging
import os
import subprocess
from pathlib import Path

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info


DEFAULT_LAUNCH_SCRIPT_NAME = "slurm-launch-job.sh"


class SlurmConnector:
    """
    Executes SLURM QoS changes by calling an external shell script.

    The script is called with environment variables describing the job
    and the desired scheduling action:

        TASKSHIFT_JOB_ID       – SLURM job ID
        TASKSHIFT_PARTITION    – job partition
        TASKSHIFT_FEATURE      – resolved feature (node type)
        TASKSHIFT_NODES        – comma-separated list of target nodes
        TASKSHIFT_QOS          – target QoS (from scheduler config)
        TASKSHIFT_PRIORITY     – target nice/priority boost
        TASKSHIFT_CPUS         – requested CPUs
        TASKSHIFT_GPUS         – requested GPUs
        TASKSHIFT_TIMELIMIT    – job timelimit in minutes

    Cluster administrators must provide a script at the path configured
    in scheduler.yaml under `connector.launch_script`. A template is
    shipped as `slurm-launch-job.sh.example` in the project root.
    """

    def __init__(self, launchScript: str | None = None, targetQos: str | None = None):
        self.launchScript = launchScript
        self.targetQos = targetQos

    def _resolve_script_path(self) -> Path | None:
        if self.launchScript:
            return Path(self.launchScript).resolve()

        projectRoot = Path(__file__).resolve().parents[2]
        return projectRoot / DEFAULT_LAUNCH_SCRIPT_NAME

    def _build_env(self, job, placement) -> dict[str, str]:
        env = {
            "TASKSHIFT_JOB_ID": str(job.getID()),
            "TASKSHIFT_PARTITION": job.partition or "",
            "TASKSHIFT_FEATURE": placement.featureName,
            "TASKSHIFT_NODES": ",".join(placement.nodeNames),
            "TASKSHIFT_CPUS": str(job.getRequestedCpus()),
            "TASKSHIFT_GPUS": str(job.getRequestedGpus()),
            "TASKSHIFT_TIMELIMIT": str(job.getTimelimit()),
        }

        if self.targetQos:
            env["TASKSHIFT_QOS"] = self.targetQos

        return env

    def executeJob(self, job, placement=None):
        scriptPath = self._resolve_script_path()

        if scriptPath is None or not scriptPath.exists():
            logger.warning(
                f"Launch script not found at '{scriptPath}', skipping job {job.getID()}. "
                f"Place a script there or set 'connector.launch_script' in scheduler.yaml"
            )
            return False

        if placement is None:
            logger.warning(
                f"No placement provided for job {job.getID()}, skipping launch"
            )
            return False

        env = self._build_env(job, placement)

        logger.info(
            f"Launching job {job.getID()} via script '{scriptPath}' "
            f"(QoS={self.targetQos or 'inherit'}, feature={placement.featureName})"
        )

        try:
            result = subprocess.run(
                [str(scriptPath)],
                capture_output=True,
                text=True,
                timeout=30,
                env={**os.environ, **env},
            )
        except FileNotFoundError:
            logger.error(
                f"Launch script '{scriptPath}' not found or not executable for job {job.getID()}"
            )
            return False
        except subprocess.TimeoutExpired:
            logger.error(f"Launch script timed out for job {job.getID()} (timeout=30s)")
            return False
        except OSError as error:
            logger.error(
                f"Failed to execute launch script for job {job.getID()}: {error}"
            )
            return False

        if result.returncode == 0:
            if result.stdout.strip():
                logger.debug(
                    f"Launch script output for job {job.getID()}: {result.stdout.strip()}"
                )
            return True

        logger.error(
            f"Launch script failed for job {job.getID()} "
            f"(exit code {result.returncode}): {result.stderr.strip() or result.stdout.strip()}"
        )
        return False
