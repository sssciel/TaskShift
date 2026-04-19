from datetime import date, timedelta
from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:
    pd = None

try:
    import yaml
except ModuleNotFoundError:
    yaml = None


DEFAULT_CONFERENCE_FILENAME_CANDIDATES = (
    "hse_conferences.yaml",
    "hse_conferences.yml",
)


def _load_yaml_file(filePath: Path):
    if yaml is None:
        raise ModuleNotFoundError("pyyaml is required to load calendar configuration files.")

    if not filePath.exists():
        raise FileNotFoundError(f"Calendar configuration file not found: {filePath}")

    with open(filePath, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def _normalize_date_value(value) -> str:
    if isinstance(value, date):
        return value.isoformat()

    raise ValueError(
        f"Calendar date value must be a YAML date scalar without quotes, got {type(value).__name__}: {value!r}"
    )


def _expand_date_entries(entries) -> set[str]:
    expandedDates = set()
    for entry in entries:
        if isinstance(entry, str):
            raise ValueError(
                f"Calendar date entries must not be strings. Remove quotes from YAML date: {entry!r}"
            )

        if isinstance(entry, date):
            expandedDates.add(_normalize_date_value(entry))
            continue

        if not isinstance(entry, dict):
            raise ValueError(
                f"Calendar date entry must be a YAML date or a {{start, end}} mapping, got {type(entry).__name__}: {entry!r}"
            )

        if "start" not in entry:
            raise ValueError(f"Calendar range entry must include 'start': {entry!r}")

        start = _normalize_date_value(entry["start"])
        end = _normalize_date_value(entry.get("end", entry["start"]))
        currentDate = date.fromisoformat(start)
        endDate = date.fromisoformat(end)

        while currentDate <= endDate:
            expandedDates.add(currentDate.isoformat())
            currentDate += timedelta(days=1)

    return expandedDates


class AcademicCalendarConfig:
    def __init__(self, rootPath):
        self.rootPath = Path(rootPath)
        self.year = None
        self.holidays = set()
        self.sessions = set()
        self.vacations = set()

    def loadYear(self, year: int):
        if yaml is None:
            raise ModuleNotFoundError("pyyaml is required to load academic calendar configuration files.")

        yearPath = self.rootPath / str(year)
        if not yearPath.exists():
            raise FileNotFoundError(f"Calendar directory for year '{year}' not found: {yearPath}")

        holidaysConfig = _load_yaml_file(yearPath / "russian_holidays.yaml") or {}
        universityConfig = _load_yaml_file(yearPath / "university_calendar.yaml") or {}

        self.year = int(year)
        self.holidays = _expand_date_entries(holidaysConfig.get("dates", []))
        self.sessions = _expand_date_entries(universityConfig.get("session_dates", []))
        self.vacations = _expand_date_entries(universityConfig.get("vacation_dates", []))
        return self

    def toDataFrame(self):
        if pd is None:
            raise ModuleNotFoundError("pandas is required to build an academic schedule DataFrame.")

        if self.year is None:
            raise RuntimeError("Academic calendar is not loaded. Call loadYear(year) first.")

        startDate = date(self.year, 1, 1)
        endDate = date(self.year, 12, 31)
        dateRange = pd.date_range(start=startDate, end=endDate, freq="D")
        dateLabels = [timestamp.date().isoformat() for timestamp in dateRange]

        return pd.DataFrame(
            {
                "date": dateRange,
                "holiday": [dateLabel in self.holidays for dateLabel in dateLabels],
                "session": [dateLabel in self.sessions for dateLabel in dateLabels],
                "vacation": [dateLabel in self.vacations for dateLabel in dateLabels],
            }
        )

class ConferenceCalendarConfig:
    def __init__(self, rootPath, filenameCandidates=None):
        self.rootPath = Path(rootPath)
        self.filenameCandidates = tuple(filenameCandidates or DEFAULT_CONFERENCE_FILENAME_CANDIDATES)
        self.year = None
        self.filePath = None
        self.dates = []

    def loadYear(self, year: int):
        yearPath = self.rootPath / str(year)
        if not yearPath.exists():
            raise FileNotFoundError(f"Calendar directory for year '{year}' not found: {yearPath}")

        filePath = self._resolve_file_path(yearPath)
        rawConfig = _load_yaml_file(filePath)
        if rawConfig is None:
            rawConfig = []

        if not isinstance(rawConfig, list):
            raise ValueError(
                f"Conference calendar file must contain a YAML list of dates: {filePath}"
            )

        self.year = int(year)
        self.filePath = filePath
        self.dates = sorted(_expand_date_entries(rawConfig))
        return self

    def toList(self) -> list[str]:
        if self.year is None:
            raise RuntimeError("Conference calendar is not loaded. Call loadYear(year) first.")

        return list(self.dates)

    def _resolve_file_path(self, yearPath: Path) -> Path:
        for filename in self.filenameCandidates:
            candidatePath = yearPath / filename
            if candidatePath.exists():
                return candidatePath

        filenames = ", ".join(self.filenameCandidates)
        raise FileNotFoundError(
            f"Conference calendar file not found in {yearPath}. Checked: {filenames}"
        )
