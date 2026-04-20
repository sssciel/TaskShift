import json
import logging
import secrets
import threading
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

try:
    from loguru import logger
except ModuleNotFoundError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    logger.success = logger.info

from config import getAdminPanelAccessConfig, getServerConfig

from .calendars import (
    create_calendar_file,
    create_calendar_year,
    get_calendar_catalog_payload,
    read_calendar_file,
    write_calendar_file,
)
from .cluster import build_cluster_tree_payload, get_cluster_snapshot_sources_payload
from .config_store import get_config_targets, read_config_target, write_config_target
from .logs import build_job_logs_payload, build_taskshift_log_payload, normalize_log_limit, normalize_page
from .pages import build_app_page_html, build_login_page_html
from .system_status import build_scheduler_system_status_payload


PANEL_COOKIE_NAME = "taskshift_admin_token"


class AdminPanelServer:
    def __init__(self, projectRoot: str | Path, schedulerController=None):
        self.projectRoot = Path(projectRoot).resolve()
        self.schedulerController = schedulerController
        self.serverConfig = getServerConfig()
        self.authToken = getAdminPanelAccessConfig().requireToken()
        self.httpd = ThreadingHTTPServer(
            (self.serverConfig.host, self.serverConfig.port),
            self._build_handler_class(),
        )
        self.httpd.daemon_threads = True
        self._thread = None

    @property
    def base_url(self) -> str:
        host = self.serverConfig.host
        if host == "0.0.0.0":
            host = "127.0.0.1"
        return f"http://{host}:{self.httpd.server_port}"

    def start_background(self):
        if self._thread is not None:
            return self

        self._thread = threading.Thread(
            target=self.serve_forever,
            daemon=True,
            name="taskshift-admin-panel",
        )
        self._thread.start()
        return self

    def serve_forever(self):
        logger.info(f"TaskShift admin panel started at {self.base_url}")
        self.httpd.serve_forever(poll_interval=0.5)

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _build_handler_class(self):
        app = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                app.handle_get(self)

            def do_POST(self):
                app.handle_post(self)

            def log_message(self, format, *args):
                logger.info("Admin panel | " + format % args)

        return Handler

    def handle_get(self, handler: BaseHTTPRequestHandler):
        parsed = urlparse(handler.path)
        path = parsed.path

        if path == "/":
            if self._is_authenticated(handler):
                self._send_html(handler, build_app_page_html())
            else:
                self._send_html(handler, build_login_page_html())
            return

        if not self._require_auth(handler):
            return

        params = parse_qs(parsed.query)

        if path == "/api/cluster-tree":
            self._handle_cluster_tree_get(handler, params)
            return

        if path == "/api/cluster-sources":
            self._send_json(handler, get_cluster_snapshot_sources_payload())
            return

        if path == "/api/system-status":
            self._send_json(handler, self._build_system_status_payload())
            return

        if path == "/api/config-targets":
            self._send_json(handler, {"targets": get_config_targets()})
            return

        if path == "/api/calendar-years":
            self._send_json(handler, get_calendar_catalog_payload())
            return

        if path == "/api/logs/taskshift":
            self._send_json(
                handler,
                build_taskshift_log_payload(
                    query=params.get("q", [""])[0],
                    statuses=params.get("statuses", []),
                    page=normalize_page(params.get("page", [1])[0]),
                    limit=normalize_log_limit(params.get("limit", [100])[0], default=100, maximum=500),
                ),
            )
            return

        if path == "/api/logs/jobs":
            self._send_json(
                handler,
                build_job_logs_payload(
                    query=params.get("q", [""])[0],
                    jobId=params.get("job_id", [""])[0],
                    statuses=params.get("statuses", []),
                    page=normalize_page(params.get("page", [1])[0]),
                    limit=normalize_log_limit(params.get("limit", [100])[0], default=100, maximum=500),
                ),
            )
            return

        if path.startswith("/api/config-targets/"):
            targetId = unquote(path.rsplit("/", maxsplit=1)[-1])
            try:
                self._send_json(handler, read_config_target(targetId))
            except KeyError:
                self._send_json(handler, {"error": "Unknown config target"}, status=HTTPStatus.NOT_FOUND)
            return

        calendarRoute = self._parse_calendar_file_route(path)
        if calendarRoute is not None:
            year, filename = calendarRoute
            try:
                self._send_json(handler, read_calendar_file(year, filename))
            except FileNotFoundError:
                self._send_json(handler, {"error": "Calendar file not found"}, status=HTTPStatus.NOT_FOUND)
            except Exception as error:
                self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
            return

        self._send_json(handler, {"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def handle_post(self, handler: BaseHTTPRequestHandler):
        parsed = urlparse(handler.path)
        path = parsed.path

        if path == "/login":
            form = parse_qs(self._read_body(handler).decode("utf-8"))
            token = form.get("token", [""])[0]
            if token and secrets.compare_digest(token, self.authToken):
                self._redirect(
                    handler,
                    "/",
                    cookies=[f"{PANEL_COOKIE_NAME}={token}; Path=/; HttpOnly; SameSite=Strict"],
                )
                return

            self._send_html(
                handler,
                build_login_page_html("Invalid token."),
                status=HTTPStatus.UNAUTHORIZED,
            )
            return

        if path == "/logout":
            self._redirect(
                handler,
                "/",
                cookies=[f"{PANEL_COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict"],
            )
            return

        if not self._require_auth(handler):
            return

        if path.startswith("/api/config-targets/"):
            targetId = unquote(path.rsplit("/", maxsplit=1)[-1])
            payload = self._read_json_body(handler)
            if payload is None:
                return

            try:
                result = write_config_target(targetId, payload.get("content", ""))
            except KeyError:
                self._send_json(handler, {"error": "Unknown config target"}, status=HTTPStatus.NOT_FOUND)
                return
            except Exception as error:
                self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
                return

            self._send_json(handler, result)
            return

        if path == "/api/system-status/run":
            payload = self._read_json_body(handler)
            if payload is None:
                return

            if self.schedulerController is None:
                self._send_json(
                    handler,
                    {
                        "error": "Manual scheduler run is unavailable because the panel is not attached to the active scheduler service."
                    },
                    status=HTTPStatus.CONFLICT,
                )
                return

            normalizedMaxLaunchedJobs = self._normalize_manual_run_limit(handler, payload.get("max_launched_jobs"))
            if normalizedMaxLaunchedJobs is ...:
                return

            try:
                self.schedulerController.request_manual_run(maxLaunchedJobs=normalizedMaxLaunchedJobs)
            except Exception as error:
                self._send_json(handler, {"error": str(error)}, status=HTTPStatus.CONFLICT)
                return

            self._send_json(handler, self._build_system_status_payload())
            return

        if path == "/api/calendar-years":
            payload = self._read_json_body(handler)
            if payload is None:
                return

            try:
                result = create_calendar_year(
                    payload.get("year", ""),
                    copyFromYear=payload.get("copy_from_year"),
                )
            except Exception as error:
                self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
                return

            self._send_json(handler, result)
            return

        pathParts = [part for part in path.split("/") if part]
        if len(pathParts) >= 4 and pathParts[:2] == ["api", "calendar-years"] and pathParts[3] == "files":
            payload = self._read_json_body(handler)
            if payload is None:
                return

            year = unquote(pathParts[2])
            if len(pathParts) == 4:
                try:
                    result = create_calendar_file(
                        year,
                        payload.get("filename", ""),
                        copyFromYear=payload.get("copy_from_year"),
                    )
                except Exception as error:
                    self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
                    return

                self._send_json(handler, result)
                return

            if len(pathParts) == 5:
                filename = unquote(pathParts[4])
                try:
                    result = write_calendar_file(year, filename, payload.get("content", ""))
                except Exception as error:
                    self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)
                    return

                self._send_json(handler, result)
                return

        self._send_json(handler, {"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_cluster_tree_get(self, handler: BaseHTTPRequestHandler, params: dict[str, list[str]]):
        try:
            self._send_json(handler, build_cluster_tree_payload(params.get("path", [""])[0]))
        except Exception as error:
            self._send_json(handler, {"error": str(error)}, status=HTTPStatus.BAD_REQUEST)

    def _build_system_status_payload(self) -> dict:
        return build_scheduler_system_status_payload(
            projectRoot=self.projectRoot,
            schedulerController=self.schedulerController,
        )

    def _parse_calendar_file_route(self, path: str) -> tuple[str, str] | None:
        pathParts = [part for part in path.split("/") if part]
        if len(pathParts) == 5 and pathParts[:2] == ["api", "calendar-years"] and pathParts[3] == "files":
            return unquote(pathParts[2]), unquote(pathParts[4])
        return None

    def _read_json_body(self, handler: BaseHTTPRequestHandler) -> dict | None:
        body = self._read_body(handler)
        try:
            return json.loads(body.decode("utf-8") or "{}")
        except json.JSONDecodeError:
            self._send_json(handler, {"error": "Invalid JSON payload"}, status=HTTPStatus.BAD_REQUEST)
            return None

    def _normalize_manual_run_limit(self, handler: BaseHTTPRequestHandler, value):
        if value in {"", None}:
            return None

        try:
            normalizedValue = int(value)
        except (TypeError, ValueError):
            self._send_json(handler, {"error": "max_launched_jobs must be an integer"}, status=HTTPStatus.BAD_REQUEST)
            return ...

        if normalizedValue <= 0:
            self._send_json(
                handler,
                {"error": "max_launched_jobs must be greater than zero"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return ...

        return normalizedValue

    def _is_authenticated(self, handler: BaseHTTPRequestHandler) -> bool:
        cookieHeader = handler.headers.get("Cookie", "")
        cookie = SimpleCookie()
        cookie.load(cookieHeader)
        cookieValue = cookie.get(PANEL_COOKIE_NAME)
        if cookieValue is not None and secrets.compare_digest(cookieValue.value, self.authToken):
            return True

        headerToken = handler.headers.get("X-Admin-Token")
        return bool(headerToken) and secrets.compare_digest(headerToken, self.authToken)

    def _require_auth(self, handler: BaseHTTPRequestHandler) -> bool:
        if self._is_authenticated(handler):
            return True

        self._send_json(handler, {"error": "Unauthorized"}, status=HTTPStatus.UNAUTHORIZED)
        return False

    def _send_html(self, handler: BaseHTTPRequestHandler, html: str, status: HTTPStatus = HTTPStatus.OK):
        payload = html.encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)

    def _send_json(self, handler: BaseHTTPRequestHandler, payload: dict, status: HTTPStatus = HTTPStatus.OK):
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Cache-Control", "no-store")
        handler.send_header("Content-Length", str(len(raw)))
        handler.end_headers()
        handler.wfile.write(raw)

    def _redirect(self, handler: BaseHTTPRequestHandler, location: str, cookies: list[str] | None = None):
        handler.send_response(HTTPStatus.SEE_OTHER)
        handler.send_header("Location", location)
        for cookie in cookies or []:
            handler.send_header("Set-Cookie", cookie)
        handler.end_headers()

    def _read_body(self, handler: BaseHTTPRequestHandler) -> bytes:
        contentLength = int(handler.headers.get("Content-Length", "0"))
        if contentLength <= 0:
            return b""

        return handler.rfile.read(contentLength)
