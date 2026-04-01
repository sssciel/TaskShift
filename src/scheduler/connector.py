import logging

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info


class SlurmConnector:
    def __init__(self):
        self.ip = None
        self.port = None

    def executeJob(self, job):
        logger.success(f"Job ran: {job.jobID}")
