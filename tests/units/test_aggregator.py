import os
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

import data_agregator.core as db_core
import data_agregator.client as db_module


@pytest.fixture(autouse=True)
def clear_client():
    db_module.db_client = None
    yield
    db_module.db_client = None


def test_get_db_client_singleton(monkeypatch):
    fake_client = object()
    monkeypatch.setattr(
        db_module.influxdb_client, "InfluxDBClient", lambda **kwargs: fake_client
    )

    c1 = db_module.get_db_client()
    c2 = db_module.get_db_client()
    assert c1 is fake_client
    assert c2 is fake_client


def test_save_data_db_calls_write(monkeypatch):
    fake_write_api = MagicMock()
    fake_client = MagicMock(write_api=MagicMock(return_value=fake_write_api))
    monkeypatch.setattr(db_core, "get_db_client", lambda: fake_client)

    df = pd.DataFrame(
        {
            "ds": pd.to_datetime(["2025-01-01T00:00:00Z", "2025-01-01T00:15:00Z"]),
            "y": [1.23, 4.56],
        }
    )
    db_core.save_data_db("test_measure", df)

    fake_write_api.write.assert_called_once()
    _, kwargs = fake_write_api.write.call_args

    assert kwargs["bucket"] == db_module.bucket
    assert kwargs["org"] == db_module.org
    assert len(kwargs["record"]) == len(df)


def test_get_full_data_db_queries(monkeypatch):
    fake_df = pd.DataFrame({"_time": [], "cpu_load": [], "gpu_load": []})
    fake_query_api = MagicMock(query_data_frame=MagicMock(return_value=fake_df))
    fake_client = MagicMock(query_api=MagicMock(return_value=fake_query_api))
    monkeypatch.setattr(db_core, "get_db_client", lambda: fake_client)

    result = db_core.get_full_data_db()
    assert result is fake_df
    fake_query_api.query_data_frame.assert_called_once()
    call_args = fake_query_api.query_data_frame.call_args.kwargs

    assert call_args["org"] == db_module.org
    assert "from(bucket:" in call_args["query"]
