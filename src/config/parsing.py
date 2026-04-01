import re
from datetime import datetime


def parse_timestamp(value):
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return int(value)

    value = str(value).strip()
    if not value:
        return None

    if value.isdigit():
        return int(value)

    normalizedValue = value.replace("Z", "+00:00")
    return int(datetime.fromisoformat(normalizedValue).timestamp())


def split_hostlist_expression(expression: str) -> list[str]:
    parts = []
    current = []
    depth = 0

    for char in expression:
        if char == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue

        if char == "[":
            depth += 1
        elif char == "]":
            depth -= 1

        current.append(char)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)

    return parts


def expand_hostlist(expression: str) -> list[str]:
    expression = expression.strip()
    if not expression or expression == "None assigned":
        return []

    expandedNodes = []
    for part in split_hostlist_expression(expression):
        match = re.search(r"\[(.+)\]", part)
        if match is None:
            expandedNodes.append(part)
            continue

        prefix = part[:match.start()]
        suffix = part[match.end():]

        for chunk in match.group(1).split(","):
            chunk = chunk.strip()
            if not chunk:
                continue

            if "-" not in chunk:
                expandedNodes.append(f"{prefix}{chunk}{suffix}")
                continue

            start, end = chunk.split("-", maxsplit=1)
            width = len(start)
            for value in range(int(start), int(end) + 1):
                expandedNodes.append(f"{prefix}{value:0{width}d}{suffix}")

    return expandedNodes
