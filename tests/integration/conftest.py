"""End-to-end integration test fixtures.

These fixtures intentionally avoid mocks for the storage layer and run
TaskShift against a real MariaDB instance provided by docker-compose.
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import mysql.connector
import pytest

from tests.fixtures.scheduler.scheduler_fixtures import build_mini_cluster_config


TEST_DB_HOST = os.getenv("DB_HOST", "mariadb")
TEST_DB_PORT = int(os.getenv("DB_PORT", "3306"))
TEST_DB_USER = os.getenv("DB_USER", "taskshift")
TEST_DB_PASSWORD = os.getenv("DB_PASSWD", "taskshift")
TEST_DB_NAME = os.getenv("DB_DATABASE", "slurm_acct_db")
TEST_NOW = 1_700_000_000


def _make_executable(path: Path):
    current_mode = path.stat().st_mode
    path.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def integration_runtime_dir(tmp_path_factory: pytest.TempPathFactory, repo_root: Path) -> Path:
    runtime_dir = tmp_path_factory.mktemp("taskshift-e2e-runtime")
    configs_dir = runtime_dir / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "cluster_backups").mkdir(parents=True, exist_ok=True)
    (runtime_dir / "exports").mkdir(parents=True, exist_ok=True)
    (runtime_dir / "logs").mkdir(parents=True, exist_ok=True)

    cluster_config_path = configs_dir / "cluster.yaml"
    scheduler_config_path = configs_dir / "scheduler.yaml"

    build_mini_cluster_config().saveConfig(cluster_config_path)
    scheduler_config_path.write_text(
        "\n".join(
            [
                "timelimit: 240",
                "max_launched_jobs: 10",
                "forecast_enabled: false",
                "forecast_data_dir: exports/historical_utilization/current",
                "forecast_model_dir: artifacts/forecast_model",
                "forecast_skip_startup_training: true",
                "cluster_config_snapshot_interval_hours: 0",
                "web_panel_enabled: false",
                "hot_reload_enabled: false",
                "cluster_config_refresh_command:",
                f"  - cat",
                f"  - {cluster_config_path}",
                "connector:",
                f"  launch_script: {repo_root / 'tests' / 'integration' / 'slurm-launch-job.sh'}",
                "  target_qos: taskshift",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    _make_executable(repo_root / "taskshift")
    _make_executable(repo_root / "tests" / "integration" / "slurm-launch-job.sh")
    _make_executable(repo_root / "tests" / "integration" / "fake_scontrol.sh")

    return runtime_dir


@pytest.fixture(scope="session")
def taskshift_test_env(repo_root: Path, integration_runtime_dir: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "DB_HOST": TEST_DB_HOST,
            "DB_PORT": str(TEST_DB_PORT),
            "DB_USER": TEST_DB_USER,
            "DB_PASSWD": TEST_DB_PASSWORD,
            "DB_DATABASE": TEST_DB_NAME,
            "DB_CHARSET": "utf8mb4",
            "DB_COLLATION": "utf8mb4_general_ci",
            "TASKSHIFT_CLUSTER_CONFIG_FILE": str(integration_runtime_dir / "configs" / "cluster.yaml"),
            "TASKSHIFT_SCHEDULER_CONFIG_FILE": str(integration_runtime_dir / "configs" / "scheduler.yaml"),
            "TASKSHIFT_CLUSTER_CONFIG_BACKUP_ROOT": str(integration_runtime_dir / "cluster_backups"),
            "TASKSHIFT_ACADEMIC_CALENDAR_ROOT": str(repo_root / "configs" / "calendar"),
            "FAKE_SLURM_CONTROL_BIN": str(repo_root / "tests" / "integration" / "fake_scontrol.sh"),
            "TASKSHIFT_TEST_NOW": str(TEST_NOW),
        }
    )
    return env


@pytest.fixture(scope="session")
def db_connection(taskshift_test_env: dict[str, str]):
    connection = mysql.connector.connect(
        host=taskshift_test_env["DB_HOST"],
        port=int(taskshift_test_env["DB_PORT"]),
        user=taskshift_test_env["DB_USER"],
        password=taskshift_test_env["DB_PASSWD"],
        database=taskshift_test_env["DB_DATABASE"],
        charset=taskshift_test_env["DB_CHARSET"],
        collation=taskshift_test_env["DB_COLLATION"],
    )
    yield connection
    connection.close()


@pytest.fixture(autouse=True)
def reset_database_tables(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("TRUNCATE TABLE fake_slurm_events")
    cursor.execute("TRUNCATE TABLE linux_job_table")
    db_connection.commit()
    cursor.close()


@pytest.fixture
def taskshift_runner(repo_root: Path, taskshift_test_env: dict[str, str]):
    def _run(*args: str, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        env = dict(taskshift_test_env)
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            [str(repo_root / "taskshift"), *args],
            cwd=str(repo_root),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    return _run
