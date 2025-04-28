import pandas as pd
import pytest
import numpy as np
from datetime import timedelta
import forecaster.core as core
import forecaster.model as model_module
from configs.config import Device


@pytest.fixture
def sample_raw_df():
    times = pd.to_datetime(
        [
            "2025/01/01 00:00",
            "2025/01/01 00:15",
            "2025/01/01 00:30",
            "2025/01/01 00:45",
        ],
        format="%Y/%m/%d %H:%M",
    )
    data = {
        "_time": times,
        "cpu_load": [50, 5, 60, 70],
        "gpu_load": [30, 90, 40, 10],
        "result": [0, 0, 0, 0],
        "table": [1, 1, 1, 1],
        "_start": [0, 0, 0, 0],
        "_stop": [0, 0, 0, 0],
        "_field": ["y", "y", "y", "y"],
    }
    return pd.DataFrame(data)


def test_get_full_ts(monkeypatch, sample_raw_df):
    monkeypatch.setattr(core, "get_full_data_db", lambda: sample_raw_df.copy())
    df = core.get_full_ts()

    assert list(df.columns) == ["ds", "cpu_load", "gpu_load"]
    assert pd.api.types.is_datetime64_any_dtype(df["ds"])
    assert df["ds"].is_monotonic_increasing


def test_prepare_ts_cpu(sample_raw_df):
    raw = sample_raw_df.copy()
    df = raw.drop(["result", "table", "_start", "_stop", "_field"], axis=1).rename(
        columns={"_time": "ds"}
    )
    out = core.prepare_ts(df, Device.CPU)
    assert list(out.columns) == ["ds", "y"]
    assert out["y"].min() >= 10.58 or pd.isna(out.loc[out["y"] < 10.58, "y"]).any()
    assert out["ds"].is_unique
    diffs = out["ds"].diff().dropna()
    assert all(diffs == timedelta(minutes=15))


def test_prepare_ts_gpu(sample_raw_df):
    raw = sample_raw_df.copy()
    df = raw.drop(["result", "table", "_start", "_stop", "_field"], axis=1).rename(
        columns={"_time": "ds"}
    )

    out = core.prepare_ts(df, Device.GPU)
    assert list(out.columns) == ["ds", "y"]
    assert (out["y"] >= 10.58).all() or out["y"].isna().any()


def test_get_device_data(monkeypatch, sample_raw_df):
    monkeypatch.setattr(core, "get_full_ts", lambda: sample_raw_df.copy())
    calls = []

    def fake_prep(df, device):
        calls.append((df.copy(), device))
        return pd.DataFrame({"ds": [], "y": []})

    monkeypatch.setattr(core, "prepare_ts", fake_prep)
    result = core.get_device_data(Device.CPU)
    assert isinstance(result, pd.DataFrame)
    assert len(calls) == 1
    assert calls[0][1] == Device.CPU


class FakeModel:
    def __init__(self, **kwargs):
        self.fitted = False

    def add_country_holidays(self, country):
        return self

    def fit(self, df):
        self.fitted = True
        return self

    def make_future_dataframe(self, df, periods):
        times = pd.date_range(
            start=df["ds"].iloc[-1] + pd.Timedelta(minutes=15),
            periods=periods,
            freq="15min",
        )
        return pd.DataFrame({"ds": times, "origin-0": np.arange(periods, dtype=float)})

    def predict(self, future_df):
        return future_df

    def get_latest_forecast(self, forecast_df):
        return forecast_df


@pytest.fixture(autouse=True)
def stub_neuralprophet(monkeypatch):
    monkeypatch.setattr(model_module, "NeuralProphet", FakeModel)
    monkeypatch.setattr(model_module, "model_config", {"n_forecasts": 4})
    monkeypatch.setattr(model_module, "FORECASTS_IN_ONE_DAY", 2)
    yield


def make_dummy_ts():
    times = pd.date_range("2025-01-01", periods=3, freq="15min")
    return pd.DataFrame({"ds": times, "y": [10, 20, 30]})


@pytest.fixture(autouse=True)
def stub_get_device_data(monkeypatch):
    monkeypatch.setattr(
        model_module, "get_device_data", lambda device: make_dummy_ts().copy()
    )
    yield


def test_get_model():
    m_cpu, m_gpu = model_module.get_model()
    assert isinstance(m_cpu, FakeModel) and m_cpu.fitted
    assert isinstance(m_gpu, FakeModel) and m_gpu.fitted


def test_get_forecasts_length_and_values():
    m_cpu, m_gpu = FakeModel(), FakeModel()
    fc_cpu, fc_gpu = model_module.get_forecasts(m_cpu, m_gpu)

    assert isinstance(fc_cpu, pd.Series)
    assert isinstance(fc_gpu, pd.Series)
    assert len(fc_cpu) == 4
    assert len(fc_gpu) == 4

    assert fc_cpu.tolist() == [0.0, 1.0, 2.0, 3.0]
    assert fc_gpu.tolist() == [0.0, 1.0, 2.0, 3.0]


def test_forecast_model_wrapper_and_averages():
    fm = model_module.ForecastModel()
    cpu, gpu = fm.get_forecasts()
    assert cpu is fm.get_cpu_forecast()
    assert gpu is fm.get_gpu_forecast()

    avg = fm.get_avg_window()
    assert pytest.approx(avg) == [1.5, 1.5]

    sat = fm.get_saturday_avg()
    assert pytest.approx(sat) == [0.5, 0.5]

    sun = fm.get_sunday_avg()
    assert pytest.approx(sun) == [2.5, 2.5]

    weekend = fm.get_weekend_avg()
    assert pytest.approx(weekend) == [1.5, 1.5]
