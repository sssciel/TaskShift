from datetime import datetime
from zoneinfo import ZoneInfo


DEFAULT_TIMEZONE_NAME = "Europe/Moscow"


def resolve_timezone(timezoneName: str | None = None) -> ZoneInfo:
    effectiveName = str(timezoneName or DEFAULT_TIMEZONE_NAME).strip() or DEFAULT_TIMEZONE_NAME
    try:
        return ZoneInfo(effectiveName)
    except Exception:
        return ZoneInfo(DEFAULT_TIMEZONE_NAME)


def now_in_timezone(timezoneName: str | None = None) -> datetime:
    return datetime.now(resolve_timezone(timezoneName))


def coerce_datetime_timezone(value: datetime, timezoneName: str | None = None) -> datetime:
    timezone = resolve_timezone(timezoneName)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone)
    return value.astimezone(timezone)
