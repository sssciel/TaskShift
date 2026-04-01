from datetime import datetime


def floor_timestamp(timestamp: int, step: int) -> int:
    return timestamp - (timestamp % step)


def ceil_timestamp(timestamp: int, step: int) -> int:
    remainder = timestamp % step
    if remainder == 0:
        return timestamp

    return timestamp + (step - remainder)


def format_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%H:%M:%S %d.%m.%y")


def parse_time_value(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return int(value)

    value = str(value).strip()
    if not value:
        return None

    if value.isdigit():
        return int(value)

    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
