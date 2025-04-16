from .model import ForecastModel
import numpy as np

FORECASTS_IN_ONE_DAY = 96

model = ForecastModel()

def get_avg_window(start=0, end=None, df_list = []):
    if not df_list:
        df_list = list(model.get_forecasts())

    avg_list = []

    for df in df_list:
        if end is None:
            end = len(df)

        avg_list.append(np.mean(df[start:end]))

    return avg_list

def get_saturday_avg():
    return get_avg_window(end=FORECASTS_IN_ONE_DAY)


def get_sunday_avg():
    return get_avg_window(start=FORECASTS_IN_ONE_DAY, end=FORECASTS_IN_ONE_DAY * 2)


def get_weekend_avg():
    return get_avg_window()
