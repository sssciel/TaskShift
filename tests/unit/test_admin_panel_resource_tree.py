from unittest.mock import patch

from admin_panel.resource_tree import build_resource_tree_payload
from config import ClusterConfig
from tests.fixtures.config.slurm_fixtures import VALID_SLURM_CONF
from tests.fixtures.scheduler.scheduler_fixtures import create_running_gpu_job


class _FakeStorage:
    def __init__(self, running_jobs):
        self.running_jobs = running_jobs
        self.closed = False

    def create(self):
        return self

    def getRunningJobs(self, timestamp):
        return list(self.running_jobs)

    def close(self):
        self.closed = True


def test_resource_tree_payload_subtracts_running_jobs():
    cluster_config = ClusterConfig()
    cluster_config.loadFromSlurmText(VALID_SLURM_CONF)
    running_job = create_running_gpu_job(
        jobID=2001,
        cpusReq=4,
        gpusRequested=2,
        nodelist="cn-007",
        partition="normal",
    )
    fake_storage = _FakeStorage([running_job])

    with (
        patch("admin_panel.resource_tree.getClusterConfig", return_value=cluster_config),
        patch("admin_panel.resource_tree.slurmStorage", return_value=fake_storage),
    ):
        payload = build_resource_tree_payload()

    assert fake_storage.closed is True
    assert payload["running_job_count"] == 1
    assert payload["available_cpu"] == payload["total_cpu"] - 4
    assert payload["available_gpu"] == payload["total_gpu"] - 2

    type_a = next(feature for feature in payload["features"] if feature["name"] == "type_a")
    cn007 = next(node for node in type_a["nodes"] if node["name"] == "cn-007")
    assert cn007["used_cpu"] == 4
    assert cn007["used_gpu"] == 2
    assert cn007["available_cpu"] == cn007["total_cpu"] - 4
    assert cn007["available_gpu"] == cn007["total_gpu"] - 2
