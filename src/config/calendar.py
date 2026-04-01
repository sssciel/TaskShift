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

        holidaysConfig = self._load_yaml(yearPath / "russian_holidays.yaml")
        universityConfig = self._load_yaml(yearPath / "university_calendar.yaml")

        self.year = int(year)
        self.holidays = self._expand_date_entries(holidaysConfig.get("dates", []))
        self.sessions = self._expand_date_entries(universityConfig.get("session_dates", []))
        self.vacations = self._expand_date_entries(universityConfig.get("vacation_dates", []))
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

    def _load_yaml(self, filePath: Path) -> dict:
        if not filePath.exists():
            raise FileNotFoundError(f"Calendar configuration file not found: {filePath}")

        with open(filePath, "r", encoding="utf-8") as file:
            return yaml.safe_load(file) or {}

    def _expand_date_entries(self, entries) -> set[str]:
        expandedDates = set()
        for entry in entries:
            if isinstance(entry, (str, date)):
                expandedDates.add(self._normalize_date(entry))
                continue

            if not isinstance(entry, dict):
                continue

            start = self._normalize_date(entry["start"])
            end = self._normalize_date(entry.get("end", entry["start"]))
            currentDate = date.fromisoformat(start)
            endDate = date.fromisoformat(end)

            while currentDate <= endDate:
                expandedDates.add(currentDate.isoformat())
                currentDate += timedelta(days=1)

        return expandedDates

    def _normalize_date(self, value) -> str:
        return date.fromisoformat(str(value)).isoformat()
