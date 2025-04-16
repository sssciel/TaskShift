from neuralprophet import NeuralProphet, set_log_level
import numpy as np
from .core import get_data, Device
from configs.config import HyperparameterConfig

model_config = HyperparameterConfig().get_config()

ts_dataframe_cpu = get_data(Device.CPU).copy()
ts_dataframe_gpu = get_data(Device.GPU).copy()

# NeuralProphet config to disable logs.
set_log_level("CRITICAL")


# So that the model is not re-trained every time.
_model_cpu, _model_gpu = None, None
_forecasts_cpu, _forecasts_gpu = None, None


def train_model():
    global _model_cpu
    global _model_gpu

    model_cpu = NeuralProphet(
        **model_config,
    )

    model_gpu = NeuralProphet(
        **model_config,
    )

    # Get holidays in Russia (RU) via https://github.com/vacanza/holidays
    model_cpu = model_cpu.add_country_holidays("RU")
    model_gpu = model_gpu.add_country_holidays("RU")

    model_cpu.fit(ts_dataframe_cpu)
    model_gpu.fit(ts_dataframe_gpu)

    _model_cpu = model_cpu
    _model_gpu = model_gpu


def get_model():
    global _model_cpu
    global _model_gpu

    if (_model_cpu == None) or (_model_gpu == None):
        print("There is no already created model, a new one is being trained.")
        train_model()

    return _model_cpu, _model_gpu


# Create_forecast creates a frame of future dates and fills
# them based on the created model.
def create_forecasts():
    global _forecasts_cpu
    global _forecasts_gpu

    model_cpu, model_gpu = get_model()
    forecasts_count = model_config["n_forecasts"]

    future_dataframe_cpu = model_cpu.make_future_dataframe(
        ts_dataframe_cpu, periods=forecasts_count
    )

    future_dataframe_gpu = model_gpu.make_future_dataframe(
        ts_dataframe_gpu, periods=forecasts_count
    )

    forecast_model_cpu = model_cpu.predict(future_dataframe_cpu)
    forecast_model_gpu = model_gpu.predict(future_dataframe_gpu)

    # I do not know why, but the predictions are saved in the origin-0 column.
    forecasts_cpu = model_cpu.get_latest_forecast(forecast_model_cpu)[
        "origin-0"
    ].astype(float)
    forecasts_gpu = model_gpu.get_latest_forecast(forecast_model_gpu)[
        "origin-0"
    ].astype(float)

    if (len(forecasts_cpu) != forecasts_count) or (
        len(forecasts_gpu) != forecasts_count
    ):
        raise "An error in receiving predictions."

    _forecasts_cpu = forecasts_cpu.clip(0, 100)
    _forecasts_gpu = forecasts_gpu.clip(0, 100)


def get_forecasts():
    global _forecasts_cpu
    global _forecasts_gpu

    if (_forecasts_cpu == None) or (_forecasts_gpu == None):
        print(
            """There are no predictions that have 
              already been created. New ones are being created."""
        )
        create_forecasts()

    return _forecasts_cpu, _forecasts_gpu


class ForecastModel:
    def __init__(self):
        self.model = get_model()
        self.forecast_cpu, self.forecast_gpu = get_forecasts()

    def get_forecasts(self):
        return self.forecast_cpu, self.forecast_gpu

    def get_cpu_forecast(self):
        return self.forecast_gpu
