import pandas as pd
from configs.config import Device
from data_agregator.core import get_full_data_db


# get_full_ts returns df with data and cpu/gpu loads.
def get_full_ts() -> pd.DataFrame:
    df = get_full_data_db()

    # InfluxDB returns trash columns
    df = df.drop(["result", "table", "_start", "_stop", "_field"], axis=1)

    df = df.rename(columns={"_time": "ds"}).copy()

    df["ds"] = pd.to_datetime(df["ds"], format="%Y/%m/%d %H:%M")
    df = df.sort_values("ds")

    return df


# prepare_ts prepares df for NeuralProphet.
def prepare_ts(df: pd.DataFrame, device: Device) -> pd.DataFrame:
    # There should only be one device left
    df = df.drop([f"{d.value}_load" for d in Device if d.value != device], axis=1)

    # NP reads df only with ds and y columns
    df = df.rename(columns={f"{device}_load": "y"}).copy()

    # Exclude anomalies when the cluster was undergoing preventive maintenance.
    df.loc[df["y"] < 10.58, "y"] = pd.NA

    # IDK why, but ts sometimes has duplicates
    df = df.drop_duplicates(subset="ds", keep="first")

    # TS has missed measurements due to preventive maintenance
    df = df.set_index("ds").asfreq("15min")
    df["y"] = df["y"].interpolate(method="time")
    df = df.reset_index()

    return df


def get_device_data(device: Device) -> pd.DataFrame:
    df = get_full_ts()
    data_ts = prepare_ts(df, device)

    return data_ts
