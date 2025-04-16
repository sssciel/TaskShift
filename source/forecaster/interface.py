from .model import ForecastModel
import numpy as np

FORECASTS_IN_ONE_DAY = 96

model = ForecastModel()


def get_saturday_avg():
    forecasts_cpu, forecasts_gpu = model.get_forecasts()

    saturday_avg_cpu = np.mean(forecasts_cpu[:FORECASTS_IN_ONE_DAY])
    saturday_avg_gpu = np.mean(forecasts_gpu[:FORECASTS_IN_ONE_DAY])

    return saturday_avg_cpu, saturday_avg_gpu


def get_sunday_avg():
    forecasts_cpu, forecasts_gpu = model.get_forecasts()

    sunday_avg_cpu = np.mean(
        forecasts_cpu[FORECASTS_IN_ONE_DAY : FORECASTS_IN_ONE_DAY * 2]
    )
    sunday_avg_gpu = np.mean(
        forecasts_gpu[FORECASTS_IN_ONE_DAY : FORECASTS_IN_ONE_DAY * 2]
    )

    return sunday_avg_cpu, sunday_avg_gpu


def get_weekend_avg():
    forecasts_cpu, forecasts_gpu = model.get_forecasts()

    weekend_avg_cpu = np.mean(forecasts_cpu)
    weekend_avg_gpu = np.mean(forecasts_gpu)

    return weekend_avg_cpu, weekend_avg_gpu
