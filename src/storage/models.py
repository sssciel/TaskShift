from dataclasses import dataclass


def _parse_tres_map(tresValue: str | None) -> dict[str, int]:
    if not tresValue:
        return {}

    parsed = {}
    for entry in tresValue.split(","):
        entry = entry.strip()
        if "=" not in entry:
            continue

        key, value = entry.split("=", maxsplit=1)
        try:
            parsed[key] = int(value)
        except ValueError:
            continue

    return parsed


def _get_tres_value(tresValue: str | None, aliases: set[str]) -> int:
    tresMap = _parse_tres_map(tresValue)
    for alias in aliases:
        if alias in tresMap:
            return tresMap[alias]

    return 0


@dataclass
class Job:
    jobID: int
    jobName: str
    timelimit: int
    state: int
    priority: int
    constraints: str | None = None
    cpusReq: int = 0
    tresReq: str | None = None
    partition: str | None = None

    def getID(self):
        return self.jobID

    def getState(self):
        return self.state

    def getTimelimit(self):
        return self.timelimit

    def getRequestedCpus(self) -> int:
        return self.cpusReq or 0

    def getRequestedGpus(self) -> int:
        return _get_tres_value(self.tresReq, {"1001", "gres/gpu", "gpu"})

    def getRequestedNodes(self) -> int:
        return _get_tres_value(self.tresReq, {"4", "node", "nodes"})

    def getRequestedFeatures(self, availableFeatures: list[str]) -> list[str]:
        if not self.constraints:
            return availableFeatures

        matchedFeatures = []
        knownFeatures = set(availableFeatures)

        for token in self.constraints.replace("&", "|").replace(",", "|").split("|"):
            token = token.strip().strip("()")
            if token and token in knownFeatures:
                matchedFeatures.append(token)

        return sorted(set(matchedFeatures))


@dataclass
class HistoricalJob:
    dbIndex: int | None
    jobID: int
    jobName: str
    timelimit: int
    state: int
    priority: int
    constraints: str | None
    cpusReq: int
    nodesAlloc: int
    timeStart: int
    timeEnd: int
    timeSubmit: int
    timeEligible: int
    modTime: int
    tresReq: str | None
    tresAlloc: str | None
    nodelist: str | None
    partition: str | None

    def getLogicalKey(self):
        return self.jobID

    def hasStarted(self) -> bool:
        return self.timeStart > 0

    def hasAssignedNodes(self) -> bool:
        return bool(self.nodelist) and self.nodelist != "None assigned"

    def getRequestedGpus(self) -> int:
        return _get_tres_value(self.tresReq, {"1001", "gres/gpu", "gpu"})

    def getAllocatedCpus(self) -> int:
        allocatedCpus = _get_tres_value(self.tresAlloc, {"1", "cpu"})
        if allocatedCpus > 0:
            return allocatedCpus

        return self.cpusReq or 0

    def getAllocatedGpus(self) -> int:
        allocatedGpus = _get_tres_value(self.tresAlloc, {"1001", "gres/gpu", "gpu"})
        if allocatedGpus > 0:
            return allocatedGpus

        return self.getRequestedGpus()

    def getEffectiveEnd(self, nowTimestamp: int) -> int:
        if self.timeEnd > 0:
            return self.timeEnd

        if nowTimestamp > 0:
            return max(nowTimestamp, self.timeStart)

        if self.modTime > self.timeStart:
            return self.modTime

        return self.timeStart

    def to_dict(self) -> dict:
        return {
            "dbIndex": self.dbIndex,
            "jobID": self.jobID,
            "jobName": self.jobName,
            "timelimit": self.timelimit,
            "state": self.state,
            "priority": self.priority,
            "constraints": self.constraints,
            "cpusReq": self.cpusReq,
            "nodesAlloc": self.nodesAlloc,
            "timeStart": self.timeStart,
            "timeEnd": self.timeEnd,
            "timeSubmit": self.timeSubmit,
            "timeEligible": self.timeEligible,
            "modTime": self.modTime,
            "tresReq": self.tresReq,
            "tresAlloc": self.tresAlloc,
            "nodelist": self.nodelist,
            "partition": self.partition,
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            dbIndex=data.get("dbIndex"),
            jobID=data["jobID"],
            jobName=data["jobName"],
            timelimit=data["timelimit"],
            state=data["state"],
            priority=data["priority"],
            constraints=data.get("constraints"),
            cpusReq=data.get("cpusReq", 0),
            nodesAlloc=data.get("nodesAlloc", 0),
            timeStart=data.get("timeStart", 0),
            timeEnd=data.get("timeEnd", 0),
            timeSubmit=data.get("timeSubmit", 0),
            timeEligible=data.get("timeEligible", 0),
            modTime=data.get("modTime", 0),
            tresReq=data.get("tresReq"),
            tresAlloc=data.get("tresAlloc"),
            nodelist=data.get("nodelist"),
            partition=data.get("partition"),
        )


@dataclass
class RawHistoricalJobRow:
    job_db_inx: int | None
    id_job: int
    job_name: str
    timelimit: int
    state: int
    priority: int
    constraints: str | None
    cpus_req: int
    nodes_alloc: int
    time_start: int
    time_end: int
    time_submit: int
    time_eligible: int
    mod_time: int
    tres_req: str | None
    tres_alloc: str | None
    nodelist: str | None
    partition: str | None

    def getLogicalKey(self):
        return self.id_job

    def toHistoricalJob(self) -> HistoricalJob:
        return HistoricalJob(
            dbIndex=self.job_db_inx,
            jobID=self.id_job,
            jobName=self.job_name,
            timelimit=self.timelimit,
            state=self.state,
            priority=self.priority,
            constraints=self.constraints,
            cpusReq=self.cpus_req or 0,
            nodesAlloc=self.nodes_alloc or 0,
            timeStart=self.time_start or 0,
            timeEnd=self.time_end or 0,
            timeSubmit=self.time_submit or 0,
            timeEligible=self.time_eligible or 0,
            modTime=self.mod_time or 0,
            tresReq=self.tres_req,
            tresAlloc=self.tres_alloc,
            nodelist=self.nodelist,
            partition=self.partition,
        )

    def to_dict(self) -> dict:
        return {
            "job_db_inx": self.job_db_inx,
            "id_job": self.id_job,
            "job_name": self.job_name,
            "timelimit": self.timelimit,
            "state": self.state,
            "priority": self.priority,
            "constraints": self.constraints,
            "cpus_req": self.cpus_req,
            "nodes_alloc": self.nodes_alloc,
            "time_start": self.time_start,
            "time_end": self.time_end,
            "time_submit": self.time_submit,
            "time_eligible": self.time_eligible,
            "mod_time": self.mod_time,
            "tres_req": self.tres_req,
            "tres_alloc": self.tres_alloc,
            "nodelist": self.nodelist,
            "partition": self.partition,
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            job_db_inx=data.get("job_db_inx"),
            id_job=data["id_job"],
            job_name=data["job_name"],
            timelimit=data["timelimit"],
            state=data["state"],
            priority=data["priority"],
            constraints=data.get("constraints"),
            cpus_req=data.get("cpus_req", 0),
            nodes_alloc=data.get("nodes_alloc", 0),
            time_start=data.get("time_start", 0),
            time_end=data.get("time_end", 0),
            time_submit=data.get("time_submit", 0),
            time_eligible=data.get("time_eligible", 0),
            mod_time=data.get("mod_time", 0),
            tres_req=data.get("tres_req"),
            tres_alloc=data.get("tres_alloc"),
            nodelist=data.get("nodelist"),
            partition=data.get("partition"),
        )
