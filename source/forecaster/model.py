import numpy as np
from configs.config import HyperparameterConfig
from neuralprophet import NeuralProphet, set_log_level

from .core import Device, get_device_data

model_config = HyperparameterConfig().get_config()

ts_dataframe_cpu = get_device_data(Device.CPU).copy()
ts_dataframe_gpu = get_device_data(Device.GPU).copy()

# NeuralProphet config to disable logs.
set_log_level("CRITICAL")


FORECASTS_IN_ONE_DAY = 96


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
        return self.forecast_cpu

    def get_gpu_forecast(self):
        return self.forecast_gpu

    def get_avg_window(self, start=0, end=None, df_list=[]):
        if not df_list:
            df_list = list(self.get_forecasts())

        avg_list = []

        for df in df_list:
            if end is None:
                end = len(df)

            avg_list.append(np.mean(df[start:end]))

        return avg_list

    def get_saturday_avg(self):
        return self.get_avg_window(end=FORECASTS_IN_ONE_DAY)

    def get_sunday_avg(self):
        return self.get_avg_window(
            start=FORECASTS_IN_ONE_DAY, end=FORECASTS_IN_ONE_DAY * 2
        )

    def get_weekend_avg(self):
        return self.get_avg_window()
