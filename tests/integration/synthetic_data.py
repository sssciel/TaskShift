"""Synthetic SLURM job data for integration tests.

Generates realistic job data matching the linux_job_table schema
without requiring a real SLURM database.
"""

from storage.models import RawHistoricalJobRow

# Base timestamp: 2023-11-14 22:13:20 UTC
BASE_TIME = 1700000000

# 15-minute intervals for series alignment
INTERVAL_15M = 15 * 60  # 900
INTERVAL_1H = 60 * 60  # 3600


class SyntheticJobFactory:
    """Factory for generating synthetic SLURM job data.

    Parameters
    ----------
    base_time:
        Reference timestamp for job start times.
    job_id_start:
        First ``id_job`` value.  Increments by one per job so that
        independent factory instances produce non-overlapping IDs.
    db_inx_start:
        First ``job_db_inx`` value.
    """

    def __init__(
        self,
        base_time: int = BASE_TIME,
        job_id_start: int = 10001,
        db_inx_start: int = 1,
    ):
        self.base_time = base_time
        self._next_db_inx = db_inx_start
        self._next_job_id = job_id_start

    def _db_inx(self) -> int:
        inx = self._next_db_inx
        self._next_db_inx += 1
        return inx

    def _job_id(self) -> int:
        jid = self._next_job_id
        self._next_job_id += 1
        return jid

    # ── Completed jobs ──────────────────────────────────────────────────────

    def completed_cpu_job(
        self,
        feature: str = "type_a",
        node: str = "cn-001",
        cpus: int = 4,
        start_offset: int = 0,
        duration_seconds: int = 3600,
        mod_time: int | None = None,
    ) -> RawHistoricalJobRow:
        """Create a completed CPU-only job."""
        t_start = self.base_time + start_offset
        t_end = t_start + duration_seconds
        t_submit = t_start - 60
        t_eligible = t_start - 60
        return RawHistoricalJobRow(
            job_db_inx=self._db_inx(),
            id_job=self._job_id(),
            job_name="cpu_job",
            timelimit=1440,
            state=3,  # completed
            priority=100,
            constraints=feature,
            cpus_req=cpus,
            nodes_alloc=1,
            time_start=t_start,
            time_end=t_end,
            time_submit=t_submit,
            time_eligible=t_eligible,
            mod_time=mod_time if mod_time is not None else t_end,
            tres_req=f"1={cpus},4=1",
            tres_alloc=f"1={cpus},4=1,5={cpus}",
            nodelist=node,
            partition="normal",
        )

    def completed_gpu_job(
        self,
        feature: str = "type_a",
        node: str = "cn-001",
        cpus: int = 4,
        gpus: int = 2,
        start_offset: int = 0,
        duration_seconds: int = 7200,
        mod_time: int | None = None,
    ) -> RawHistoricalJobRow:
        """Create a completed GPU job."""
        t_start = self.base_time + start_offset
        t_end = t_start + duration_seconds
        t_submit = t_start - 120
        t_eligible = t_start - 60
        return RawHistoricalJobRow(
            job_db_inx=self._db_inx(),
            id_job=self._job_id(),
            job_name="gpu_job",
            timelimit=1440,
            state=3,
            priority=200,
            constraints=feature,
            cpus_req=cpus,
            nodes_alloc=1,
            time_start=t_start,
            time_end=t_end,
            time_submit=t_submit,
            time_eligible=t_eligible,
            mod_time=mod_time if mod_time is not None else t_end,
            tres_req=f"1={cpus},4=1,1001={gpus}",
            tres_alloc=f"1={cpus},4=1,1001={gpus},5={cpus}",
            nodelist=node,
            partition="normal",
        )

    def multi_node_job(
        self,
        feature: str = "type_a",
        nodes: str = "cn-[001-003]",
        cpus_per_node: int = 4,
        gpus_per_node: int = 0,
        num_nodes: int = 3,
        start_offset: int = 0,
        duration_seconds: int = 5400,
    ) -> RawHistoricalJobRow:
        """Create a completed multi-node job."""
        total_cpus = cpus_per_node * num_nodes
        total_gpus = gpus_per_node * num_nodes
        t_start = self.base_time + start_offset
        t_end = t_start + duration_seconds
        return RawHistoricalJobRow(
            job_db_inx=self._db_inx(),
            id_job=self._job_id(),
            job_name="multi_node_job",
            timelimit=1440,
            state=3,
            priority=150,
            constraints=feature,
            cpus_req=total_cpus,
            nodes_alloc=num_nodes,
            time_start=t_start,
            time_end=t_end,
            time_submit=t_start - 300,
            time_eligible=t_start - 300,
            mod_time=t_end,
            tres_req=f"1={total_cpus},4={num_nodes}"
            + (f",1001={total_gpus}" if total_gpus > 0 else ""),
            tres_alloc=f"1={total_cpus},4={num_nodes},5={total_cpus}"
            + (f",1001={total_gpus}" if total_gpus > 0 else ""),
            nodelist=nodes,
            partition="normal",
        )

    # ── Running jobs ───────────────────────────────────────────────────────

    def running_cpu_job(
        self,
        feature: str = "type_a",
        node: str = "cn-001",
        cpus: int = 4,
        start_offset: int = 0,
    ) -> RawHistoricalJobRow:
        """Create a running CPU job (time_end=0)."""
        t_start = self.base_time + start_offset
        return RawHistoricalJobRow(
            job_db_inx=self._db_inx(),
            id_job=self._job_id(),
            job_name="running_cpu",
            timelimit=1440,
            state=1,  # running
            priority=100,
            constraints=feature,
            cpus_req=cpus,
            nodes_alloc=1,
            time_start=t_start,
            time_end=0,
            time_submit=t_start - 60,
            time_eligible=t_start - 60,
            mod_time=t_start + 300,
            tres_req=f"1={cpus},4=1",
            tres_alloc=f"1={cpus},4=1,5={cpus}",
            nodelist=node,
            partition="normal",
        )

    def running_gpu_job(
        self,
        feature: str = "type_a",
        node: str = "cn-001",
        cpus: int = 8,
        gpus: int = 2,
        start_offset: int = 0,
    ) -> RawHistoricalJobRow:
        """Create a running GPU job."""
        t_start = self.base_time + start_offset
        return RawHistoricalJobRow(
            job_db_inx=self._db_inx(),
            id_job=self._job_id(),
            job_name="running_gpu",
            timelimit=1440,
            state=1,
            priority=200,
            constraints=feature,
            cpus_req=cpus,
            nodes_alloc=1,
            time_start=t_start,
            time_end=0,
            time_submit=t_start - 120,
            time_eligible=t_start - 60,
            mod_time=t_start + 600,
            tres_req=f"1={cpus},4=1,1001={gpus}",
            tres_alloc=f"1={cpus},4=1,1001={gpus},5={cpus}",
            nodelist=node,
            partition="normal",
        )

    # ── Failed / Cancelled jobs ─────────────────────────────────────────────

    def failed_job(
        self,
        feature: str = "type_b",
        node: str = "cn-005",
        cpus: int = 8,
        start_offset: int = 0,
        duration_seconds: int = 600,
    ) -> RawHistoricalJobRow:
        """Create a failed job (state=5)."""
        t_start = self.base_time + start_offset
        t_end = t_start + duration_seconds
        return RawHistoricalJobRow(
            job_db_inx=self._db_inx(),
            id_job=self._job_id(),
            job_name="failed_job",
            timelimit=1440,
            state=5,  # failed
            priority=100,
            constraints=feature,
            cpus_req=cpus,
            nodes_alloc=1,
            time_start=t_start,
            time_end=t_end,
            time_submit=t_start - 30,
            time_eligible=t_start - 30,
            mod_time=t_end,
            tres_req=f"1={cpus},4=1",
            tres_alloc=f"1={cpus},4=1",
            nodelist=node,
            partition="normal",
        )

    def cancelled_job(
        self,
        feature: str = "type_d",
        cpus: int = 4,
        start_offset: int = 0,
    ) -> RawHistoricalJobRow:
        """Create a cancelled job (state=4) — never started."""
        t_submit = self.base_time + start_offset
        return RawHistoricalJobRow(
            job_db_inx=self._db_inx(),
            id_job=self._job_id(),
            job_name="cancelled_job",
            timelimit=1440,
            state=4,
            priority=100,
            constraints=feature,
            cpus_req=cpus,
            nodes_alloc=0,
            time_start=0,  # never started
            time_end=0,
            time_submit=t_submit,
            time_eligible=t_submit,
            mod_time=t_submit + 10,
            tres_req=f"1={cpus},4=1",
            tres_alloc="",
            nodelist="None assigned",
            partition="normal",
        )

    def pending_job(
        self,
        feature: str = "type_a",
        cpus: int = 2,
        gpus: int = 0,
        start_offset: int = 0,
    ) -> RawHistoricalJobRow:
        """Create a pending job (state=0) — never started."""
        t_submit = self.base_time + start_offset
        tres_req = f"1={cpus},4=1"
        if gpus > 0:
            tres_req += f",1001={gpus}"
        return RawHistoricalJobRow(
            job_db_inx=self._db_inx(),
            id_job=self._job_id(),
            job_name="pending_job",
            timelimit=60,
            state=0,  # pending
            priority=420,
            constraints=feature,
            cpus_req=cpus,
            nodes_alloc=0,
            time_start=0,
            time_end=0,
            time_submit=t_submit,
            time_eligible=t_submit,
            mod_time=t_submit,
            tres_req=tres_req,
            tres_alloc="",
            nodelist="None assigned",
            partition="normal",
        )


def build_standard_test_dataset(
    base_time: int = BASE_TIME,
) -> list[RawHistoricalJobRow]:
    """Build a standard set of synthetic jobs for integration testing.

    Timeline (all offsets from *base_time*):

    ============  ======================================================
    Hour 0        2 CPU jobs on type_a (cn-001, cn-002)
    Hour 1        1 GPU job on type_a cn-001
    Hour 2        1 multi-node job on type_a cn-[001-003]
    Hour 3        1 CPU job on type_b cn-005 (8 CPU, no GPU)
    Hour 4        1 GPU job on type_d cn-007
    Hour 5+       1 running CPU job, 1 running GPU job, 1 failed job
    Also          1 pending + 1 cancelled job (never started)
    ============  ======================================================

    Returns a list of :class:`RawHistoricalJobRow`.
    """
    f = SyntheticJobFactory(base_time=base_time)
    rows = [
        # ── Completed type_a jobs ──
        f.completed_cpu_job(
            feature="type_a",
            node="cn-001",
            cpus=4,
            start_offset=0,
            duration_seconds=3600,
        ),
        f.completed_cpu_job(
            feature="type_a",
            node="cn-002",
            cpus=4,
            start_offset=0,
            duration_seconds=1800,  # half hour
        ),
        f.completed_gpu_job(
            feature="type_a",
            node="cn-001",
            cpus=4,
            gpus=2,
            start_offset=INTERVAL_1H,
            duration_seconds=3600,  # hour 1-2
        ),
        f.multi_node_job(
            feature="type_a",
            nodes="cn-[001-003]",
            cpus_per_node=4,
            num_nodes=3,
            start_offset=2 * INTERVAL_1H,
            duration_seconds=3600,
        ),
        # ── Completed type_b jobs ──
        f.completed_cpu_job(
            feature="type_b",
            node="cn-005",
            cpus=8,
            start_offset=3 * INTERVAL_1H,
            duration_seconds=3600,
        ),
        # ── Completed type_d jobs ──
        f.completed_gpu_job(
            feature="type_d",
            node="cn-007",
            cpus=8,
            gpus=4,
            start_offset=4 * INTERVAL_1H,
            duration_seconds=3600,
        ),
        # ── Running jobs (time_end=0) ──
        f.running_cpu_job(
            feature="type_a",
            node="cn-001",
            cpus=4,
            start_offset=5 * INTERVAL_1H,
        ),
        f.running_gpu_job(
            feature="type_a",
            node="cn-002",
            cpus=4,
            gpus=2,
            start_offset=5 * INTERVAL_1H,
        ),
        # ── Failed job (consumes resources during its runtime) ──
        f.failed_job(
            feature="type_b",
            node="cn-005",
            cpus=8,
            start_offset=5 * INTERVAL_1H,
            duration_seconds=600,
        ),
        # ── Pending and cancelled (not started — excluded from series) ──
        f.pending_job(feature="type_a", cpus=2, start_offset=5 * INTERVAL_1H),
        f.cancelled_job(feature="type_d", cpus=4, start_offset=5 * INTERVAL_1H),
    ]
    return rows


def build_incremental_dataset(base_time: int = BASE_TIME) -> list[RawHistoricalJobRow]:
    """Build jobs that arrive *after* the standard dataset.

    Uses high starting ``job_id_start`` / ``db_inx_start`` so that the
    IDs never collide with :func:`build_standard_test_dataset`.  All
    ``mod_time`` values are guaranteed to be greater than the latest
    ``mod_time`` in the standard dataset.
    """
    f = SyntheticJobFactory(
        base_time=base_time,
        job_id_start=10020,
        db_inx_start=20,
    )
    rows = [
        f.completed_cpu_job(
            feature="type_a",
            node="cn-003",
            cpus=4,
            start_offset=6 * INTERVAL_1H,
            duration_seconds=1800,
            mod_time=base_time + 6 * INTERVAL_1H + 3600,
        ),
        f.completed_gpu_job(
            feature="type_d",
            node="cn-008",
            cpus=8,
            gpus=4,
            start_offset=7 * INTERVAL_1H,
            duration_seconds=3600,
        ),
    ]
    return rows
