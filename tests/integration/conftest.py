"""End-to-end integration test fixtures.

These fixtures intentionally avoid mocks for the storage layer and run
TaskShift against a real MariaDB instance provided by docker-compose.
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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
TEST_MSERVER_TOKEN = "integration-token"


def _make_executable(path: Path):
    current_mode = path.stat().st_mode
    path.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def fake_mserver_endpoint():
    class FakeMserverHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return

        def do_POST(self):
            if self.path != "/slurm_set_job_qos":
                self._send_json(404, {"success": False, "error": "not found"})
                return

            if self.headers.get("API_TOKEN") != TEST_MSERVER_TOKEN:
                self._send_json(403, {"success": False, "error": "invalid token"})
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0") or "0")
                request_body = self.rfile.read(content_length).decode("utf-8")
                payload = json.loads(request_body)
                jobs = payload["jobs"]
                qos = str(payload["qos"])
                if not isinstance(jobs, list) or not qos:
                    raise ValueError("invalid payload")
            except Exception as error:
                self._send_json(400, {"success": False, "error": str(error)})
                return

            connection = mysql.connector.connect(
                host=TEST_DB_HOST,
                port=TEST_DB_PORT,
                user=TEST_DB_USER,
                password=TEST_DB_PASSWORD,
                database=TEST_DB_NAME,
                charset=os.getenv("DB_CHARSET", "utf8mb4"),
                collation=os.getenv("DB_COLLATION", "utf8mb4_general_ci"),
            )
            try:
                cursor = connection.cursor()
                for job_id in jobs:
                    cursor.execute(
                        """
                        INSERT INTO fake_slurm_events (
                            event_time, command_name, job_id, action_name, qos_value,
                            feature_name, node_names, raw_args
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            int(time.time()),
                            "mserver",
                            int(job_id),
                            "set_qos",
                            qos,
                            None,
                            None,
                            json.dumps(payload, ensure_ascii=False),
                        ),
                    )
                connection.commit()
                cursor.close()
            finally:
                connection.close()

            self._send_json(200, {"success": True})

        def _send_json(self, status_code: int, payload: dict):
            response_body = json.dumps(payload).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(response_body)))
            self.end_headers()
            self.wfile.write(response_body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeMserverHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="taskshift-fake-mserver",
    )
    thread.start()

    try:
        yield {
            "url": f"http://127.0.0.1:{server.server_port}/slurm_set_job_qos",
            "token": TEST_MSERVER_TOKEN,
        }
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2.0)


@pytest.fixture(scope="session")
def integration_runtime_dir(
    tmp_path_factory: pytest.TempPathFactory,
    repo_root: Path,
    fake_mserver_endpoint: dict[str, str],
) -> Path:
    runtime_dir = tmp_path_factory.mktemp("taskshift-e2e-runtime")
    configs_dir = runtime_dir / "configs"
    configs_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "cluster_backups").mkdir(parents=True, exist_ok=True)
    (runtime_dir / "exports").mkdir(parents=True, exist_ok=True)
    (runtime_dir / "logs").mkdir(parents=True, exist_ok=True)

    cluster_config_path = configs_dir / "cluster.yaml"
    scheduler_config_path = configs_dir / "scheduler.yaml"
    env_config_path = configs_dir / ".env"

    build_mini_cluster_config().saveConfig(cluster_config_path)
    env_config_path.write_text(
        f'TASKSHIFT_MSERVER_API_TOKEN="{fake_mserver_endpoint["token"]}"\n',
        encoding="utf-8",
    )
    scheduler_config_path.write_text(
        "\n".join(
            [
                "timelimit: 240",
                "max_launched_jobs: 10",
                "forecast_enabled: false",
                "forecast_data_dir: exports/historical_utilization/current",
                "forecast_model_dir: artifacts/forecast_model",
                "forecast_model_update_interval_hours: 84",
                "forecast_skip_startup_training: true",
                "timezone: Europe/Moscow",
                "cluster_config_refresh_time: '00:30'",
                "web_panel_enabled: false",
                "hot_reload_enabled: false",
                "cluster_config_refresh_command:",
                f"  - cat",
                f"  - {cluster_config_path}",
                "connector:",
                f"  mserver_url: {fake_mserver_endpoint['url']}",
                "  timeout_seconds: 5",
                "  target_qos: taskshift",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    _make_executable(repo_root / "taskshift")
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
            "TASKSHIFT_DB_CONFIG_FILE": str(integration_runtime_dir / "configs" / ".env"),
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
