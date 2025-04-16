from yaml import safe_load, YAMLError
from pathlib import Path


# Files, where configs are stored.
# The files must be in the "source/config" folder!
hyperparams_config_file = "hyperparams.yml"
serivice_config_file = "config.yml"
database_config_file = ".env"


hyperparams_config = {}
service_config = {}
database_config = {}


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
