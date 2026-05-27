#!/usr/bin/env python3
import ast
import json
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


TASKSHIFT_TOKEN = "TEST_TASKSHIFT_TOKEN"
TASKSHIFT_ENDPOINT = "/slurm_set_job_qos"
TASKSHIFT_SCRIPT = "set_job_qos_taskshift.py"
MAX_BODY_BYTES = 8192
COMMAND_TIMEOUT_SECONDS = 60


def run_command(command):
    if isinstance(command, str):
        raise TypeError("run_command() expects a sequence of command arguments")
    try:
        p = subprocess.run(
            list(command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError("command timed out")
    stdout = p.stdout.strip().strip("\n")
    stderr = p.stderr.strip(" \n")
    if p.returncode != 0:
        raise RuntimeError(stderr or stdout or f"command failed with code {p.returncode}")
    return stdout


def get_taskshift_command(data):
    return [
        sys.executable,
        TASKSHIFT_SCRIPT,
        str(data),
    ]


class TaskshiftServer(BaseHTTPRequestHandler):

    def _send_json(self, status_code, data):
        self.send_response(status_code)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def _check_token(self):
        if self.headers.get("API_TOKEN") == TASKSHIFT_TOKEN:
            return True
        self._send_json(403, {"success": False, "error": "incorrect token"})
        return False

    def _get_post_data(self):
        try:
            content_length = int(self.headers.get("Content-Length", 0))
        except ValueError:
            raise ValueError("invalid content length")
        if content_length <= 0:
            raise ValueError("empty request body")
        if content_length > MAX_BODY_BYTES:
            raise ValueError("request body too large")
        post_data = self.rfile.read(content_length)
        return json.loads(post_data.decode())

    def _process_request(self, method):
        if not self._check_token():
            return

        parsed_request = urlparse(self.path)
        if method != "POST" or parsed_request.path.rstrip("/") != TASKSHIFT_ENDPOINT:
            self._send_json(404, {"success": False, "error": "handler not found"})
            return

        try:
            json_data = self._get_post_data()
            if not isinstance(json_data, dict):
                raise ValueError("invalid request body")
            output = run_command(get_taskshift_command(json_data))
            self._send_json(200, ast.literal_eval(output))
        except Exception as err:
            self._send_json(200, {"success": False, "error": err.__str__()})

    def do_GET(self):
        self._process_request("GET")

    def do_POST(self):
        self._process_request("POST")


def run_server(addr="0.0.0.0", port=9426):
    server_address = (addr, port)
    httpd = ThreadingHTTPServer(server_address, TaskshiftServer)
    httpd.serve_forever()


if __name__ == "__main__":
    run_server()
