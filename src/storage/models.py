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
    nodelist: str | None
    partition: str | None

    def hasStarted(self) -> bool:
        return self.timeStart > 0

    def hasAssignedNodes(self) -> bool:
        return bool(self.nodelist) and self.nodelist != "None assigned"

    def getRequestedGpus(self) -> int:
        return _get_tres_value(self.tresReq, {"1001", "gres/gpu", "gpu"})

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
            "nodelist": self.nodelist,
            "partition": self.partition,
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
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
            nodelist=data.get("nodelist"),
            partition=data.get("partition"),
        )
