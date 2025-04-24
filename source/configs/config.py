from yaml import safe_load, YAMLError
from pathlib import Path
from enum import StrEnum
from dotenv import load_dotenv


# Files, where configs are stored.
# The files must be in the "source/config" folder!
cluster_config_file = "cluster.yml"
hyperparams_config_file = "hyperparams.yml"
service_config_file = "service.yml"
database_config_file = ".env"


hyperparams_config = {}
service_config = {}


# Loading environment variables from .env instead of forming another config
def load_env_config():
    load_dotenv(
        dotenv_path=Path(__file__).absolute().parent.joinpath(database_config_file)
    )


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


def get_yaml_config(file_name: str, service_name: str, default_params=None):
    file = Path(__file__).absolute().parent.joinpath(file_name)

    if not file.exists():
        if default_params == None:
            raise f"There is no {service_name}. Import has been reset."
        else:
            print(f"There is no {service_name}. Standard parameters are used.")
            return default_params

    try:
        with open(file) as f:
            return safe_load(f)
    except YAMLError as ecx:
        if default_params == None:
            print(f"It is not possible to parse the {service_name}.")
            raise ecx
        else:
            print(
                f"It is not possible to parse the {service_name}. Standard parameters are used."
            )
            return default_params


class ConfigurationBase:
    def __init__(self, file: Path, service_name: str, default_params=None):
        self.config = get_yaml_config(file, service_name, default_params)
        print(type(self.config))

    def get_config(self):
        return self.config


class HyperparameterConfig(ConfigurationBase):
    def __init__(self):
        super().__init__(
            file=hyperparams_config_file,
            service_name="hyperparameter configuration file",
            default_params=default_hyperparams_config,
        )


class ServiceConfig(ConfigurationBase):
    def __init__(self):
        super().__init__(
            file=service_config_file,
            service_name="service configuration file",
            default_params={"country": "RU"},
        )

class ClusterConfig(ConfigurationBase):
    def __init__(self):
        super().__init__(
            file=cluster_config_file,
            service_name="cluster configuration file"
        )

    def get_nodes_info(self):
        return self.config["nodes"]

    def get_cluster_info(self):
        return self.config["cluster"]