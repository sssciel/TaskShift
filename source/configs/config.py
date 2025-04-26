from enum import StrEnum
from pathlib import Path

from dotenv import load_dotenv
from yaml import YAMLError, safe_load

# Files, where configs are stored.
# The files must be in the "source/config" folder!
cluster_config_file = "cluster.yml"
hyperparams_config_file = "hyperparams.yml"
service_config_file = "service.yml"
database_config_file = ".env"


hyperparams_config = {}
service_config = {}


# Loading environment variables from .env instead of forming another config
def load_env_config(file=None):
    if file is None:
        file = Path(__file__).absolute().parent.joinpath(database_config_file)

    load_dotenv(dotenv_path=file)


def get_absolute_path(file_name):
    return Path(__file__).absolute().parent.joinpath(file_name)


default_hyperparams_config = {
    "n_lags": 96,
    "learning_rate": "None",
    "epochs": "None",
    "batch_size": "None",
    "seasonality_reg": 0,
    "n_changepoints": 10,
    "trend_reg": 0,
    "n_forecasts": 192,
}


class Device(StrEnum):
    CPU = "cpu"
    GPU = "gpu"


def get_yaml_config(file_path: str, service_name: str, default_params=None):
    file = Path(file_path)

    if not file.exists():
        if default_params is None:
            raise FileNotFoundError(
                f"There is no {service_name}. Import has been reset."
            )
        else:
            print(f"There is no {service_name}. Standard parameters are used.")
            return default_params

    try:
        with open(file) as f:
            return safe_load(f)
    except YAMLError as ecx:
        if default_params is None:
            print(f"It is not possible to parse the {service_name}.")
            raise ecx
        else:
            print(
                f"It is not possible to parse the {service_name}. Standard parameters are used."
            )
            return default_params


class ConfigurationBase:
    def __init__(self, file: Path, service_name: str, default_params=None):
        self.name = service_name
        self.config = get_yaml_config(
            get_absolute_path(file), service_name, default_params
        )

    def get_config(self):
        return self.config

    def get_name(self):
        return self.name


class HyperparameterConfig(ConfigurationBase):
    def __init__(self, file=hyperparams_config_file):
        super().__init__(
            file=file,
            service_name="hyperparameter configuration file",
            default_params=default_hyperparams_config,
        )


class ServiceConfig(ConfigurationBase):
    def __init__(self, file=service_config_file):
        super().__init__(
            file=file,
            service_name="service configuration file",
            default_params={"country": "RU"},
        )


class ClusterConfig(ConfigurationBase):
    def __init__(self, file=cluster_config_file):
        super().__init__(file=file, service_name="cluster configuration file")

    def get_nodes_info(self):
        return self.config["nodes"]

    def get_cluster_info(self):
        return self.config["cluster"]

    def get_devices_count(self):
        cpu_count, gpu_count = 0, 0

        for node in self.config["nodes"]:
            node_count = node["count"]
            cpu_count += (
                node["cpu"]["sockets"] * node["cpu"]["cores_per_cpu"] * node_count
            )

            if node["gpu"] is not None:
                gpu_count += node["gpu"]["count"] * node_count

        return cpu_count, gpu_count
