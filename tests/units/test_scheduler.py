import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import scheduler.core as sch
from scheduler.utils import UniqueQueue


@pytest.fixture(autouse=True)
def patch_uniquequeue_contains(monkeypatch):
    monkeypatch.setattr(
        UniqueQueue, "__contains__", lambda self, item: item["job_id"] in self._set
    )
    yield


class DummyForecast:
    def get_saturday_avg(self):
        return (10, 20)

    def get_sunday_avg(self):
        return (5, 5)


@pytest.fixture(autouse=True)
def reset_scheduler(monkeypatch):
    sch.forecast_model = None
    sch.last_job = None
    sch.next_monday = None
    sch.task_queue = UniqueQueue()
    monkeypatch.setattr(
        sch, "cluster_config", MagicMock(get_devices_count=lambda: (10, 10))
    )
    yield
    sch.forecast_model = None
    sch.last_job = None
    sch.next_monday = None
    sch.task_queue = UniqueQueue()


def test_compute_forecasts_sets_model_and_next_monday(monkeypatch):
    monkeypatch.setattr(sch, "ForecastModel", lambda: DummyForecast())
    fake_now = datetime(2025, 5, 9, 12, 0)

    class FakeDateTime:
        @classmethod
        def now(cls):
            return fake_now

    monkeypatch.setattr(sch, "datetime", FakeDateTime)

    sch.compute_forecasts()

    assert isinstance(sch.forecast_model, DummyForecast)
    assert sch.next_monday == datetime(2025, 5, 12, 0, 0)


def test_task_scheduler_invokes_compute_and_schedules(monkeypatch):
    calls = []
    fake_now = datetime(2025, 5, 10, 9, 0)

    def stub_compute():
        calls.append(True)
        sch.forecast_model = DummyForecast()
        sch.next_monday = fake_now + timedelta(days=7)

    monkeypatch.setattr(sch, "compute_forecasts", stub_compute)

    class FakeDateTime:
        @classmethod
        def now(cls):
            return fake_now

    monkeypatch.setattr(sch, "datetime", FakeDateTime)
    monkeypatch.setattr(sch, "get_sessionid_running_tasks_test", lambda: [])
    monkeypatch.setattr(sch, "get_sessionid_pending_tasks_test", lambda: {})
    monkeypatch.setattr(sch, "run_task", lambda jobid: None)

    sch.task_scheduler()

    assert calls == [True]


def test_task_scheduler_runs_one_fitting_task(monkeypatch):
    sch.forecast_model = DummyForecast()
    fake_now = datetime(2025, 5, 10, 9, 0)

    class FakeDateTime:
        @classmethod
        def now(cls):
            return fake_now

    monkeypatch.setattr(sch, "datetime", FakeDateTime)
    sch.next_monday = fake_now + timedelta(days=7)

    tasks = {
        "job1": {
            "job_id": "job1",
            "cpu_cores_count": 1,
            "gpu_count": 1,
            "time_limit": 10000,
        },
        "job2": {
            "job_id": "job2",
            "cpu_cores_count": 1000,
            "gpu_count": 1,
            "time_limit": 1,
        },
    }
    monkeypatch.setattr(sch, "get_sessionid_running_tasks_test", lambda: [])
    monkeypatch.setattr(sch, "get_sessionid_pending_tasks_test", lambda: tasks)
    calls = []
    monkeypatch.setattr(sch, "run_task", lambda jobid: calls.append(jobid))

    sch.task_scheduler()

    assert calls == ["job1"]
    assert sch.last_job == tasks["job1"]

def test_put_and_pop():
    q = UniqueQueue()
    q.put({"job_id": "a"})
    q.put({"job_id": "a"})
    q.put({"job_id": "b"})
    assert len(q) == 2
    x = q.pop()
    y = q.pop()
    assert {x["job_id"], y["job_id"]} == {"a", "b"}

def test_rebuild_adds_and_removes():
    tasks = {
        "x": {"job_id": "x"},
        "y": {"job_id": "y"},
    }
    q = UniqueQueue()
    q.rebuild(tasks)
    assert len(q) == 2
    ids = [item["job_id"] for item in q._dq]
    assert set(ids) == {"x", "y"}

def test_empty_and_len():
    q = UniqueQueue()
    assert q.empty()
    q.put({"job_id": "test"})
    assert not q.empty()
    assert len(q) == 1

def test_contains():
    q = UniqueQueue()
    q.put({"job_id": "z"})
    assert "z" in q._set
    q.pop()
    assert "z" not in q._set