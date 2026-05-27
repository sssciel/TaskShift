"""
Unit tests for scheduler/connector.py.
"""

import ast
import importlib.util
import threading
from http.server import ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from scheduler.connector import (
    DEFAULT_TIMEOUT_SECONDS,
    SlurmConnector,
)
from tests.fixtures.scheduler.scheduler_fixtures import create_pending_job


class FakeResponse:
    def __init__(self, status=200, body=b""):
        self.status = status
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def getcode(self):
        return self.status

    def read(self):
        return self._body


class TestSlurmConnectorInit:
    def test_default_init(self):
        connector = SlurmConnector()

        assert connector.mserverUrl is None
        assert connector.apiToken is None
        assert connector.targetQos is None
        assert connector.timeoutSeconds == DEFAULT_TIMEOUT_SECONDS

    def test_custom_init(self):
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
            timeoutSeconds=5,
        )

        assert connector.mserverUrl == "http://mserver/slurm_set_job_qos"
        assert connector.apiToken == "secret"
        assert connector.targetQos == "normal"
        assert connector.timeoutSeconds == 5


class TestSlurmConnectorRequest:
    def test_build_payload_uses_mserver_format(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(apiToken="secret", targetQos="normal")

        assert connector._build_payload(job) == {
            "jobs": [4038802],
            "qos": "normal",
        }

    def test_build_request_sets_post_json_and_api_token_header(self):
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
        )

        request = connector._build_request({"jobs": [4038802], "qos": "normal"})

        assert request.full_url == "http://mserver/slurm_set_job_qos"
        assert request.get_method() == "POST"
        assert request.data == b'{"jobs": [4038802], "qos": "normal"}'
        assert request.get_header("Api_token") == "secret"
        assert request.get_header("Content-type") == "application/json"


class TestSlurmConnectorExecuteJob:
    def test_successful_mserver_call_returns_true(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
            timeoutSeconds=7,
        )

        with patch("scheduler.connector.urlopen") as mock_urlopen:
            mock_urlopen.return_value = FakeResponse(body=b'{"success": true}')
            result = connector.executeJob(job)

        assert result is True
        request = mock_urlopen.call_args[0][0]
        assert request.full_url == "http://mserver/slurm_set_job_qos"
        assert request.data == b'{"jobs": [4038802], "qos": "normal"}'
        assert mock_urlopen.call_args[1]["timeout"] == 7

    def test_successful_python_dict_response_returns_true(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
        )

        with patch("scheduler.connector.urlopen") as mock_urlopen:
            mock_urlopen.return_value = FakeResponse(body=b"{'success': True}")
            result = connector.executeJob(job)

        assert result is True

    def test_no_placement_is_allowed_because_mserver_payload_does_not_use_it(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
        )

        with patch("scheduler.connector.urlopen") as mock_urlopen:
            mock_urlopen.return_value = FakeResponse()
            result = connector.executeJob(job, placement=None)

        assert result is True
        mock_urlopen.assert_called_once()

    def test_missing_qos_skips_request(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(apiToken="secret", targetQos=None)

        with patch("scheduler.connector.urlopen") as mock_urlopen:
            result = connector.executeJob(job)

        assert result is False
        mock_urlopen.assert_not_called()

    def test_missing_mserver_url_skips_request(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(apiToken="secret", targetQos="normal")

        with patch("scheduler.connector.urlopen") as mock_urlopen:
            result = connector.executeJob(job)

        assert result is False
        mock_urlopen.assert_not_called()

    def test_missing_api_token_skips_request(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken=None,
            targetQos="normal",
        )

        with patch("scheduler.connector.urlopen") as mock_urlopen:
            result = connector.executeJob(job)

        assert result is False
        mock_urlopen.assert_not_called()

    def test_http_error_returns_false(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
        )
        error = HTTPError(
            url="http://mserver/slurm_set_job_qos",
            code=500,
            msg="server error",
            hdrs=None,
            fp=BytesIO(b"boom"),
        )

        with patch("scheduler.connector.urlopen", side_effect=error):
            result = connector.executeJob(job)

        assert result is False

    def test_network_error_returns_false(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
        )

        with patch("scheduler.connector.urlopen", side_effect=URLError("offline")):
            result = connector.executeJob(job)

        assert result is False

    def test_non_2xx_response_returns_false(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
        )

        with patch("scheduler.connector.urlopen") as mock_urlopen:
            mock_urlopen.return_value = FakeResponse(status=503, body=b"unavailable")
            result = connector.executeJob(job)

        assert result is False

    def test_json_success_false_returns_false(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
        )

        with patch("scheduler.connector.urlopen") as mock_urlopen:
            mock_urlopen.return_value = FakeResponse(
                body=b'{"success": false, "error": "Access denied"}'
            )
            result = connector.executeJob(job)

        assert result is False

    def test_python_dict_success_false_returns_false(self):
        job = create_pending_job(jobID=4038802)
        connector = SlurmConnector(
            mserverUrl="http://mserver/slurm_set_job_qos",
            apiToken="secret",
            targetQos="normal",
        )

        with patch("scheduler.connector.urlopen") as mock_urlopen:
            mock_urlopen.return_value = FakeResponse(
                body=b"{'success': False, 'error': 'Access denied'}"
            )
            result = connector.executeJob(job)

        assert result is False


class TestSlurmConnectorMserverCompatibility:
    def test_connector_request_is_accepted_by_checked_in_mserver_handler(self):
        server_module = self._load_mserver_module()
        captured_commands = []

        def fake_run_command(command):
            captured_commands.append(command)
            return "{'success': True}"

        server_module.TASKSHIFT_TOKEN = "secret"
        server_module.run_command = fake_run_command
        server_module.TaskshiftServer.log_message = lambda *args: None

        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), server_module.TaskshiftServer
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            connector = SlurmConnector(
                mserverUrl=f"http://127.0.0.1:{server.server_port}/slurm_set_job_qos",
                apiToken="secret",
                targetQos="normal",
                timeoutSeconds=5,
            )

            result = connector.executeJob(create_pending_job(jobID=4038802))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        assert result is True
        assert len(captured_commands) == 1
        assert Path(captured_commands[0][1]).name == "set_job_qos_taskshift.py"
        assert ast.literal_eval(captured_commands[0][2]) == {
            "jobs": [4038802],
            "qos": "normal",
        }

    @staticmethod
    def _load_mserver_module():
        server_path = Path("mserver/server_taskshift.py").resolve()
        spec = importlib.util.spec_from_file_location(
            "server_taskshift_under_test", server_path
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
