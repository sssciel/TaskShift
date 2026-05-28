from html import escape
from pathlib import Path

ASSETS_ROOT = Path(__file__).resolve().parent / "assets"

APP_JS_FILES = [
    "js/translations.js",
    "js/shared.js",
    "js/toast.js",
    "js/system.js",
    "js/cluster.js",
    "js/resource_tree.js",
    "js/configs.js",
    "js/calendars.js",
    "js/logs.js",
    "js/bootstrap.js",
]

LOGIN_JS_FILES = [
    "js/translations.js",
    "js/login.js",
]


def build_login_page_html(errorMessage: str = "") -> str:
    return (
        _read_asset("login.html")
        .replace("__LOGIN_CSS__", _read_asset("login.css"))
        .replace("__LOGIN_JS__", _bundle_js(LOGIN_JS_FILES))
        .replace("__ERROR_MESSAGE__", escape(errorMessage))
    )


def build_app_page_html() -> str:
    return (
        _read_asset("app.html")
        .replace("__APP_CSS__", _read_asset("app.css"))
        .replace("__APP_JS__", _bundle_js(APP_JS_FILES))
    )


def _bundle_js(assetPaths: list[str]) -> str:
    return "\n\n".join(_read_asset(assetPath) for assetPath in assetPaths)


def _read_asset(relativePath: str) -> str:
    return (ASSETS_ROOT / relativePath).read_text(encoding="utf-8")
