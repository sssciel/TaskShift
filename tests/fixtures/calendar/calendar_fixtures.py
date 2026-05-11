"""
Test fixtures for academic and conference calendars
"""

# Sample russian holidays YAML content
RUSSIAN_HOLIDAYS_2024_YAML = """dates:
  - start: 2024-01-01
    end: 2024-01-08
  - 2024-02-23
  - 2024-03-08
  - 2024-05-01
  - 2024-05-09
  - 2024-06-12
  - 2024-11-04
"""

# Sample university calendar YAML content
UNIVERSITY_CALENDAR_2024_YAML = """session_dates:
  - start: 2024-03-25
    end: 2024-03-31
  - start: 2024-06-21
    end: 2024-06-30

vacation_dates:
  - start: 2024-01-01
    end: 2024-01-08
  - start: 2024-07-01
    end: 2024-08-31
"""

# Sample conferences YAML content
CONFERENCES_2024_YAML = """- 2024-02-26
- 2024-03-04
- 2024-04-02
- 2024-05-31
"""

# Expected expanded dates for testing
EXPECTED_HOLIDAYS_2024 = {
    "2024-01-01",
    "2024-01-02",
    "2024-01-03",
    "2024-01-04",
    "2024-01-05",
    "2024-01-06",
    "2024-01-07",
    "2024-01-08",
    "2024-02-23",
    "2024-03-08",
    "2024-05-01",
    "2024-05-09",
    "2024-06-12",
    "2024-11-04",
}

EXPECTED_SESSIONS_2024 = {
    # March session: 2024-03-25 to 2024-03-31 (7 days)
    "2024-03-25",
    "2024-03-26",
    "2024-03-27",
    "2024-03-28",
    "2024-03-29",
    "2024-03-30",
    "2024-03-31",
    # June session: 2024-06-21 to 2024-06-30 (10 days)
    "2024-06-21",
    "2024-06-22",
    "2024-06-23",
    "2024-06-24",
    "2024-06-25",
    "2024-06-26",
    "2024-06-27",
    "2024-06-28",
    "2024-06-29",
    "2024-06-30",
}

EXPECTED_VACATIONS_2024 = {
    # January vacation: 2024-01-01 to 2024-01-08 (8 days)
    "2024-01-01",
    "2024-01-02",
    "2024-01-03",
    "2024-01-04",
    "2024-01-05",
    "2024-01-06",
    "2024-01-07",
    "2024-01-08",
    # Summer vacation: 2024-07-01 to 2024-08-31 (62 days)
    # Just check a few dates
    "2024-07-01",
    "2024-07-15",
    "2024-08-01",
    "2024-08-31",
}

EXPECTED_CONFERENCES_2024 = [
    "2024-02-26",
    "2024-03-04",
    "2024-04-02",
    "2024-05-31",
]
