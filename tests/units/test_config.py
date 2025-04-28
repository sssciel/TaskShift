import os
from pathlib import Path

import configs.config as config
import pytest
from yaml import YAMLError

DATA_DIR = Path(__file__).parent / "test_data"
cfg_path_good = DATA_DIR / "yaml_good.yml"
cfg_path_broken = DATA_DIR / "yaml_broken.yml"
cfg_path_params = DATA_DIR / "yaml_params.yml"
cfg_path_cluster = DATA_DIR / "yaml_cluster.yml"
env_path_file = DATA_DIR / "env_file"


def test_get_yaml_config_returns_expected_for_valid_yaml():
    result = config.get_yaml_config(cfg_path_good, "test_service")

    assert "a" in result
    work_dict = result["a"]

    assert work_dict["name"] == "ABC"
    assert work_dict["age"] == 18
    assert work_dict["location"] is None


@pytest.mark.parametrize(
    "path, service_name, default, expected, raises",
    [
        (Path("nonexistent.yml"), "test_name", None, None, FileNotFoundError),
        (Path("nonexistent.yml"), "test_name", {"foo": 42}, {"foo": 42}, None),
        (cfg_path_broken, "test_name", None, None, YAMLError),
        (cfg_path_broken, "test_name", {"foo": 42}, {"foo": 42}, None),
    ],
    ids=[
        "No exist, without default",
        "No exist, with default",
        "Broken YAML, without default",
        "Broken YAML, with default",
    ],
)
def test_get_yaml_config_various(path, service_name, default, expected, raises):
    if raises:
        with pytest.raises(raises):
            config.get_yaml_config(path, service_name, default)
    else:
        result = config.get_yaml_config(path, service_name, default)
        assert result == expected


def test_hyperparameter_config_class_behaviour():
    hyperparams = config.HyperparameterConfig(file=cfg_path_params)

    assert hyperparams.get_name() == "hyperparameter configuration file"

    params_config = hyperparams.get_config()

    assert params_config["n_lags"] == 1


def test_cluster_config():
    cc = config.ClusterConfig(file=cfg_path_cluster)

    assert cc.get_cluster_info() == {"foo": 123}

    cpu, gpu = cc.get_devices_count()
    assert cpu == 12
    assert gpu == 2


def test_load_env_config_creates_env_vars():
    config.load_env_config(file=env_path_file)

    assert os.getenv("FOO") == "BAR"
    assert os.getenv("BAR") == "42"
