from pathlib import Path

from config import getLatestClusterConfigFile, schedulerConfigFile, serverConfigFile
from config.models import get_yaml_module


def get_config_targets() -> list[dict]:
    return [
        {
            "id": "cluster_active",
            "label": "Active cluster config",
            "path": str(Path(getLatestClusterConfigFile()).resolve()),
            "description": "Cluster snapshot currently used by the scheduler.",
        },
        {
            "id": "scheduler",
            "label": "Scheduler config",
            "path": str(Path(schedulerConfigFile).resolve()),
            "description": "Scheduler limits, forecast path, and snapshot cadence.",
        },
        {
            "id": "server",
            "label": "Web server config",
            "path": str(Path(serverConfigFile).resolve()),
            "description": "Host and port for the admin panel.",
        },
    ]


def resolve_config_target(targetId: str) -> dict:
    for target in get_config_targets():
        if target["id"] == targetId:
            return target

    raise KeyError(f"Unknown config target: {targetId}")


def read_config_target(targetId: str) -> dict:
    target = resolve_config_target(targetId)
    path = Path(target["path"])
    if path.exists():
        content = path.read_text(encoding="utf-8")
    elif targetId == "server":
        content = 'host: "127.0.0.1"\nport: 8000\n'
    else:
        content = ""

    return {
        **target,
        "content": content,
    }


def write_config_target(targetId: str, content: str) -> dict:
    target = resolve_config_target(targetId)
    validate_config_content(targetId, content)
    path = Path(target["path"])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return {
        **target,
        "content": content,
    }


def validate_config_content(targetId: str, content: str):
    get_yaml_module().safe_load(content or "")
