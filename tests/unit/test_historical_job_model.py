"""
Unit tests for storage.models module - HistoricalJob and RawHistoricalJobRow
"""

import pytest

from storage.models import HistoricalJob, RawHistoricalJobRow
from tests.fixtures.storage.storage_fixtures import (
    COMPLETED_JOB_RAW,
    OLD_JOB_RAW,
    create_historical_job,
    create_raw_historical_job_row,
)


class TestHistoricalJobBasics:
    """Tests for HistoricalJob model basic functionality"""

    def test_historical_job_creation(self):
        """Test creating a HistoricalJob instance"""
        job = HistoricalJob(
            dbIndex=12345,
            jobID=3984258,
            jobName="test_job",
            timelimit=1440,
            state=3,
            priority=100,
            constraints="type_a",
            cpusReq=1,
            nodesAlloc=1,
            timeStart=1778493027,
            timeEnd=1778493048,
            timeSubmit=1778492997,
            timeEligible=1778492997,
            modTime=1778493048,
            tresReq="1=1,4=1",
            tresAlloc="1=1,4=1",
            nodelist="cn-001",
            partition="normal",
        )

        assert job.dbIndex == 12345
        assert job.jobID == 3984258
        assert job.jobName == "test_job"
        assert job.state == 3

    def test_get_logical_key(self):
        """Test getLogicalKey() method"""
        job = create_historical_job(jobID=54321)
        assert job.getLogicalKey() == 54321


class TestHistoricalJobStatus:
    """Tests for HistoricalJob status methods"""

    def test_has_started_true(self):
        """Test hasStarted() returns True when timeStart > 0"""
        job = create_historical_job(timeStart=1778493027)
        assert job.hasStarted() is True

    def test_has_started_false(self):
        """Test hasStarted() returns False when timeStart == 0"""
        job = create_historical_job(timeStart=0)
        assert job.hasStarted() is False

    def test_has_assigned_nodes_true(self):
        """Test hasAssignedNodes() returns True when nodelist is set"""
        job = create_historical_job(nodelist="cn-001")
        assert job.hasAssignedNodes() is True

    def test_has_assigned_nodes_false(self):
        """Test hasAssignedNodes() returns False when nodelist is None"""
        job = create_historical_job(nodelist=None)
        assert job.hasAssignedNodes() is False

    def test_has_assigned_nodes_none_assigned(self):
        """Test hasAssignedNodes() returns False when nodelist is 'None assigned'"""
        job = create_historical_job(nodelist="None assigned")
        assert job.hasAssignedNodes() is False

    def test_has_assigned_nodes_empty_string(self):
        """Test hasAssignedNodes() returns False when nodelist is empty"""
        job = create_historical_job(nodelist="")
        assert job.hasAssignedNodes() is False


class TestHistoricalJobResources:
    """Tests for HistoricalJob resource methods"""

    def test_get_requested_gpus_numeric_code(self):
        """Test getting requested GPUs with numeric code 1001"""
        job = create_historical_job(tresReq="1001=4")
        assert job.getRequestedGpus() == 4

    def test_get_requested_gpus_gres_format(self):
        """Test getting requested GPUs with gres/gpu format"""
        job = create_historical_job(tresReq="gres/gpu=2")
        assert job.getRequestedGpus() == 2

    def test_get_requested_gpus_no_gpus(self):
        """Test getting requested GPUs when no GPUs requested"""
        job = create_historical_job(tresReq="1=4,4=1")
        assert job.getRequestedGpus() == 0

    def test_get_allocated_cpus_from_tres(self):
        """Test getting allocated CPUs from tresAlloc"""
        job = create_historical_job(tresAlloc="1=8,4=1")
        assert job.getAllocatedCpus() == 8

    def test_get_allocated_cpus_fallback_to_cpureq(self):
        """Test getting allocated CPUs falls back to cpusReq when tresAlloc is empty"""
        job = create_historical_job(cpusReq=12, tresAlloc=None)
        assert job.getAllocatedCpus() == 12

    def test_get_allocated_cpus_empty_tres(self):
        """Test getting allocated CPUs with empty tresAlloc"""
        job = create_historical_job(cpusReq=4, tresAlloc="")
        assert job.getAllocatedCpus() == 4

    def test_get_allocated_gpus_from_tres(self):
        """Test getting allocated GPUs from tresAlloc"""
        job = create_historical_job(tresAlloc="1001=2,1=4,4=1")
        assert job.getAllocatedGpus() == 2

    def test_get_allocated_gpus_fallback_to_requested(self):
        """Test getting allocated GPUs falls back to requested GPUs"""
        job = create_historical_job(tresReq="gres/gpu=3", tresAlloc=None)
        assert job.getAllocatedGpus() == 3

    def test_get_allocated_gpus_billing_code_ignored(self):
        """Test that billing TRES code 5 is NOT treated as GPU"""
        # Code 5 = billing, code 1001 = gpu
        # Real SLURM data often has billing but no GPUs
        job = create_historical_job(tresAlloc="1=12,4=1,5=12", tresReq="1=12,4=1,5=12")
        assert job.getAllocatedGpus() == 0  # Code 5 is billing, not GPU

    def test_get_allocated_cpus_from_tres_code_1(self):
        """Test getting allocated CPUs from TRES code 1 in tresAlloc"""
        job = create_historical_job(tresAlloc="1=616,4=14,5=616")
        assert job.getAllocatedCpus() == 616  # Code 1 = cpu


class TestHistoricalJobEffectiveEnd:
    """Tests for HistoricalJob.getEffectiveEnd()"""

    def test_get_effective_end_with_time_end(self):
        """Test getEffectiveEnd() returns timeEnd when set"""
        job = create_historical_job(timeStart=1000, timeEnd=2000, modTime=3000)
        assert job.getEffectiveEnd(1500) == 2000

    def test_get_effective_end_without_time_end(self):
        """Test getEffectiveEnd() returns nowTimestamp when timeEnd is 0"""
        job = create_historical_job(timeStart=1000, timeEnd=0, modTime=3000)
        assert job.getEffectiveEnd(1500) == 1500

    def test_get_effective_end_returns_max_of_now_and_start(self):
        """Test getEffectiveEnd() returns max of now and timeStart"""
        job = create_historical_job(timeStart=1000, timeEnd=0, modTime=3000)
        # now < timeStart, should return timeStart
        assert job.getEffectiveEnd(500) == 1000
        # now > timeStart, should return now
        assert job.getEffectiveEnd(2000) == 2000

    def test_get_effective_end_fallback_to_mod_time(self):
        """Test getEffectiveEnd() falls back to modTime when now is 0"""
        job = create_historical_job(timeStart=1000, timeEnd=0, modTime=3000)
        assert job.getEffectiveEnd(0) == 3000

    def test_get_effective_end_fallback_to_time_start(self):
        """Test getEffectiveEnd() falls back to timeStart when all else fails"""
        job = create_historical_job(timeStart=1000, timeEnd=0, modTime=0)
        assert job.getEffectiveEnd(0) == 1000


class TestHistoricalJobSerialization:
    """Tests for HistoricalJob serialization"""

    def test_to_dict(self):
        """Test to_dict() method"""
        job = create_historical_job(dbIndex=12345, jobID=3984258, jobName="test_job")

        result = job.to_dict()

        assert isinstance(result, dict)
        assert result["dbIndex"] == 12345
        assert result["jobID"] == 3984258
        assert result["jobName"] == "test_job"

    def test_from_dict(self):
        """Test from_dict() class method"""
        data = {
            "dbIndex": 12345,
            "jobID": 3984258,
            "jobName": "test_job",
            "timelimit": 1440,
            "state": 3,
            "priority": 100,
            "constraints": "type_a",
            "cpusReq": 1,
            "nodesAlloc": 1,
            "timeStart": 1778493027,
            "timeEnd": 1778493048,
            "timeSubmit": 1778492997,
            "timeEligible": 1778492997,
            "modTime": 1778493048,
            "tresReq": "1=1,4=1",
            "tresAlloc": "1=1,4=1",
            "nodelist": "cn-001",
            "partition": "normal",
        }

        job = HistoricalJob.from_dict(data)

        assert job.dbIndex == 12345
        assert job.jobID == 3984258
        assert job.jobName == "test_job"

    def test_to_dict_and_from_dict_roundtrip(self):
        """Test that to_dict() and from_dict() are inverses"""
        original = create_historical_job(
            dbIndex=12345,
            jobID=3984258,
            jobName="test_job",
            cpusReq=4,
            tresAlloc="1001=2,1=4",
        )

        data = original.to_dict()
        restored = HistoricalJob.from_dict(data)

        assert restored.dbIndex == original.dbIndex
        assert restored.jobID == original.jobID
        assert restored.jobName == original.jobName
        assert restored.cpusReq == original.cpusReq
        assert restored.tresAlloc == original.tresAlloc


class TestRawHistoricalJobRow:
    """Tests for RawHistoricalJobRow model"""

    def test_raw_row_creation(self):
        """Test creating a RawHistoricalJobRow instance"""
        row = RawHistoricalJobRow(
            job_db_inx=12345,
            id_job=3984258,
            job_name="test_job",
            timelimit=1440,
            state=3,
            priority=100,
            constraints="type_a",
            cpus_req=1,
            nodes_alloc=1,
            time_start=1778493027,
            time_end=1778493048,
            time_submit=1778492997,
            time_eligible=1778492997,
            mod_time=1778493048,
            tres_req="1=1,4=1",
            tres_alloc="1=1,4=1",
            nodelist="cn-001",
            partition="normal",
        )

        assert row.job_db_inx == 12345
        assert row.id_job == 3984258
        assert row.job_name == "test_job"

    def test_get_logical_key(self):
        """Test getLogicalKey() returns id_job"""
        row = create_raw_historical_job_row(id_job=54321)
        assert row.getLogicalKey() == 54321

    def test_to_historical_job(self):
        """Test toHistoricalJob() conversion"""
        row = create_raw_historical_job_row(
            job_db_inx=12345,
            id_job=3984258,
            job_name="test_job",
            cpus_req=4,
            time_start=1000,
        )

        job = row.toHistoricalJob()

        assert isinstance(job, HistoricalJob)
        assert job.dbIndex == 12345
        assert job.jobID == 3984258
        assert job.jobName == "test_job"
        assert job.cpusReq == 4
        assert job.timeStart == 1000

    def test_to_dict(self):
        """Test to_dict() method"""
        row = create_raw_historical_job_row(job_db_inx=12345, id_job=3984258)

        result = row.to_dict()

        assert isinstance(result, dict)
        assert result["job_db_inx"] == 12345
        assert result["id_job"] == 3984258

    def test_from_dict(self):
        """Test from_dict() class method"""
        data = {
            "job_db_inx": 12345,
            "id_job": 3984258,
            "job_name": "test_job",
            "timelimit": 1440,
            "state": 3,
            "priority": 100,
            "constraints": "type_a",
            "cpus_req": 1,
            "nodes_alloc": 1,
            "time_start": 1778493027,
            "time_end": 1778493048,
            "time_submit": 1778492997,
            "time_eligible": 1778492997,
            "mod_time": 1778493048,
            "tres_req": "1=1,4=1",
            "tres_alloc": "1=1,4=1",
            "nodelist": "cn-001",
            "partition": "normal",
        }

        row = RawHistoricalJobRow.from_dict(data)

        assert row.job_db_inx == 12345
        assert row.id_job == 3984258
        assert row.job_name == "test_job"


class TestRealWorldData:
    """Tests using real-world data from slurm database"""

    def test_completed_job_from_real_data(self):
        """Test creating HistoricalJob from real completed job data"""
        row = RawHistoricalJobRow(
            job_db_inx=3584,
            id_job=3984258,
            job_name="run_m.sh",
            timelimit=1440,
            state=3,
            priority=138,
            constraints="type_a|type_b|type_d",
            cpus_req=1,
            nodes_alloc=1,
            time_start=1778493027,
            time_end=1778493048,
            time_submit=1778492997,
            time_eligible=1778492997,
            mod_time=1778493048,
            tres_req="1=1,2=1,4=1,5=1",
            tres_alloc="1=1,4=1,5=1",
            nodelist="cn-032",
            partition="normal",
        )

        job = row.toHistoricalJob()

        assert job.jobID == 3984258
        assert job.hasStarted() is True
        assert job.hasAssignedNodes() is True
        assert job.getAllocatedCpus() == 1
        # No GPU allocated: code 5 = billing, code 1001 = gpu (not present)
        assert job.getAllocatedGpus() == 0

    def test_old_job_from_real_data(self):
        """Test creating HistoricalJob from old job data with different format"""
        row = RawHistoricalJobRow(
            job_db_inx=14,
            id_job=23,
            job_name="bash",
            timelimit=1440,
            state=3,
            priority=4294901758,
            constraints=None,
            cpus_req=14,
            nodes_alloc=14,
            time_start=1550484270,
            time_end=1550484275,
            time_submit=1550484270,
            time_eligible=1550484270,
            mod_time=1550484275,
            tres_req="1=14,4=14",
            tres_alloc="1=616,4=14,5=616",
            nodelist="cn-[001-014]",
            partition="normal",
        )

        job = row.toHistoricalJob()

        assert job.jobID == 23
        assert job.hasStarted() is True
        # TRES codes: 1=cpu (616), 4=node (14), 5=billing (616)
        assert job.getAllocatedCpus() == 616  # code 1 = cpu
        assert job.getAllocatedGpus() == 0  # no GPU code 1001 in alloc or req
