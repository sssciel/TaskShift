"""
Unit tests for admin_panel.config_store module
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from admin_panel.config_store import (
    get_config_targets,
    read_config_target,
    resolve_config_target,
    validate_config_content,
    write_config_target,
)

# ════════════════════════════════════════════════════════════════════════════════
# Helpers
#
# getLatestClusterConfigFile is a function, while DBConfigFile/schedulerConfigFile/
# serverConfigFile are plain string module-level variables. Patch accordingly.
# ════════════════════════════════════════════════════════════════════════════════


def _patched_config_paths(
    cluster_path="/tmp/cluster.yaml",
    scheduler_path="/tmp/scheduler.yaml",
    server_path="/tmp/server.yaml",
    env_path="/tmp/.env",
):
    """Return a context manager that patches all four config paths."""
    from contextlib import ExitStack

    class _PatchedPaths:
        def __init__(self):
            self._stack = ExitStack()

        def __enter__(self):
            s = self._stack
            s.enter_context(
                patch(
                    "admin_panel.config_store.getLatestClusterConfigFile",
                    return_value=cluster_path,
                )
            )
            s.enter_context(
                patch("admin_panel.config_store.schedulerConfigFile", scheduler_path)
            )
            s.enter_context(
                patch("admin_panel.config_store.serverConfigFile", server_path)
            )
            s.enter_context(patch("admin_panel.config_store.DBConfigFile", env_path))
            return self

        def __exit__(self, *args):
            self._stack.__exit__(*args)

    return _PatchedPaths()


# ════════════════════════════════════════════════════════════════════════════════
# get_config_targets
# ════════════════════════════════════════════════════════════════════════════════


class TestGetConfigTargets:
    def test_returns_four_targets(self):
        with _patched_config_paths():
            targets = get_config_targets()
        assert len(targets) == 4

    def test_expected_ids(self):
        with _patched_config_paths():
            targets = get_config_targets()
        ids = [t["id"] for t in targets]
        assert ids == ["cluster_active", "scheduler", "server", "env"]

    def test_each_target_has_required_keys(self):
        with _patched_config_paths():
            targets = get_config_targets()
        for target in targets:
            assert "id" in target
            assert "label" in target
            assert "path" in target
            assert "description" in target

    def test_paths_are_strings(self):
        with _patched_config_paths():
            targets = get_config_targets()
        for target in targets:
            assert isinstance(target["path"], str)

    def test_cluster_active_label(self):
        with _patched_config_paths():
            targets = get_config_targets()
        cluster = next(t for t in targets if t["id"] == "cluster_active")
        assert "cluster" in cluster["label"].lower()

    def test_env_target_label(self):
        with _patched_config_paths():
            targets = get_config_targets()
        env = next(t for t in targets if t["id"] == "env")
        assert "Environment" in env["label"]


# ════════════════════════════════════════════════════════════════════════════════
# resolve_config_target
# ════════════════════════════════════════════════════════════════════════════════


class TestResolveConfigTarget:
    def test_valid_id_returns_target(self):
        with _patched_config_paths():
            target = resolve_config_target("scheduler")
        assert target["id"] == "scheduler"

    def test_all_ids_resolvable(self):
        with _patched_config_paths():
            for tid in ["cluster_active", "scheduler", "server", "env"]:
                target = resolve_config_target(tid)
                assert target["id"] == tid

    def test_unknown_id_raises_key_error(self):
        with _patched_config_paths():
            with pytest.raises(KeyError, match="Unknown config target"):
                resolve_config_target("nonexistent")

    def test_empty_string_raises_key_error(self):
        with _patched_config_paths():
            with pytest.raises(KeyError):
                resolve_config_target("")


# ════════════════════════════════════════════════════════════════════════════════
# read_config_target
# ════════════════════════════════════════════════════════════════════════════════


class TestReadConfigTarget:
    def test_reads_existing_file(self, tmp_path):
        cluster_file = tmp_path / "cluster.yaml"
        cluster_file.write_text("node_groups: []\n", encoding="utf-8")

        with _patched_config_paths(cluster_path=str(cluster_file)):
            result = read_config_target("cluster_active")

        assert result["content"] == "node_groups: []\n"
        assert result["id"] == "cluster_active"

    def test_nonexistent_server_returns_default_content(self, tmp_path):
        server_file = tmp_path / "nonexistent_server.yaml"

        with _patched_config_paths(server_path=str(server_file)):
            result = read_config_target("server")

        assert result["content"] == 'host: "127.0.0.1"\nport: 8000\n'

    def test_nonexistent_non_server_returns_empty(self, tmp_path):
        scheduler_file = tmp_path / "nonexistent_scheduler.yaml"

        with _patched_config_paths(scheduler_path=str(scheduler_file)):
            result = read_config_target("scheduler")

        assert result["content"] == ""

    def test_unknown_target_raises_key_error(self):
        with _patched_config_paths():
            with pytest.raises(KeyError):
                read_config_target("does_not_exist")

    def test_result_includes_path(self, tmp_path):
        cluster_file = tmp_path / "cluster.yaml"
        cluster_file.write_text("node_groups: []\n", encoding="utf-8")

        with _patched_config_paths(cluster_path=str(cluster_file)):
            result = read_config_target("cluster_active")

        assert "path" in result
        assert cluster_file.name in result["path"]


# ════════════════════════════════════════════════════════════════════════════════
# write_config_target
# ════════════════════════════════════════════════════════════════════════════════


class TestWriteConfigTarget:
    def test_writes_valid_yaml_to_disk(self, tmp_path):
        scheduler_file = tmp_path / "scheduler.yaml"
        content = "timelimit: 60\nmax_launched_jobs: 5\n"

        with _patched_config_paths(scheduler_path=str(scheduler_file)):
            result = write_config_target("scheduler", content)

        assert result["content"] == content
        assert scheduler_file.read_text(encoding="utf-8") == content

    def test_writes_env_without_yaml_validation(self, tmp_path):
        env_file = tmp_path / ".env"
        content = "DB_HOST=localhost\nDB_USER=admin\nINVALID ][ YAML\n"

        with _patched_config_paths(env_path=str(env_file)):
            result = write_config_target("env", content)

        assert result["content"] == content
        assert env_file.read_text(encoding="utf-8") == content

    def test_invalid_yaml_raises_for_non_env(self, tmp_path):
        scheduler_file = tmp_path / "scheduler.yaml"
        content = "invalid: ][ yaml"

        with _patched_config_paths(scheduler_path=str(scheduler_file)):
            with pytest.raises(Exception):
                write_config_target("scheduler", content)

    def test_creates_parent_directories(self, tmp_path):
        nested_file = tmp_path / "deep" / "nested" / "scheduler.yaml"
        content = "timelimit: 60\n"

        with _patched_config_paths(scheduler_path=str(nested_file)):
            result = write_config_target("scheduler", content)

        assert nested_file.exists()
        assert result["content"] == content

    def test_unknown_target_raises_key_error(self):
        with _patched_config_paths():
            with pytest.raises(KeyError):
                write_config_target("does_not_exist", "content")

    def test_overwrites_existing_file(self, tmp_path):
        scheduler_file = tmp_path / "scheduler.yaml"
        scheduler_file.write_text("old: content\n", encoding="utf-8")
        new_content = "new: content\n"

        with _patched_config_paths(scheduler_path=str(scheduler_file)):
            result = write_config_target("scheduler", new_content)

        assert scheduler_file.read_text(encoding="utf-8") == new_content
        assert result["content"] == new_content

    def test_empty_yaml_is_valid(self, tmp_path):
        scheduler_file = tmp_path / "scheduler.yaml"

        with _patched_config_paths(scheduler_path=str(scheduler_file)):
            result = write_config_target("scheduler", "")

        assert result["content"] == ""


# ════════════════════════════════════════════════════════════════════════════════
# validate_config_content
# ════════════════════════════════════════════════════════════════════════════════


class TestValidateConfigContent:
    def test_valid_yaml_passes(self):
        validate_config_content("scheduler", "key: value\n")

    def test_empty_string_passes(self):
        validate_config_content("scheduler", "")

    def test_invalid_yaml_raises(self):
        with pytest.raises(Exception):
            validate_config_content("scheduler", "invalid: ][ yaml")

    def test_env_target_always_passes_with_invalid_yaml(self):
        validate_config_content("env", "INVALID ][ YAML\nexport FOO=bar\n")

    def test_env_target_passes_with_empty_content(self):
        validate_config_content("env", "")

    def test_cluster_active_validates_yaml(self):
        with pytest.raises(Exception):
            validate_config_content("cluster_active", "bad: ][ yaml")

    def test_server_validates_yaml(self):
        with pytest.raises(Exception):
            validate_config_content("server", "bad: ][ yaml")
