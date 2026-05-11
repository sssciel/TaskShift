"""
Unit tests for config.calendar module
"""

from datetime import date
from pathlib import Path

import pytest
import yaml

from config.calendar import AcademicCalendarConfig, ConferenceCalendarConfig
from tests.fixtures.calendar.calendar_fixtures import (
    CONFERENCES_2024_YAML,
    EXPECTED_CONFERENCES_2024,
    EXPECTED_HOLIDAYS_2024,
    EXPECTED_SESSIONS_2024,
    EXPECTED_VACATIONS_2024,
    RUSSIAN_HOLIDAYS_2024_YAML,
    UNIVERSITY_CALENDAR_2024_YAML,
)


class TestAcademicCalendarConfig:
    """Tests for AcademicCalendarConfig"""

    @pytest.fixture
    def temp_calendar_root(self, tmp_path):
        """Create temporary calendar directory structure"""
        year_dir = tmp_path / "2024"
        year_dir.mkdir()

        # Create russian_holidays.yaml
        holidays_file = year_dir / "russian_holidays.yaml"
        holidays_file.write_text(RUSSIAN_HOLIDAYS_2024_YAML)

        # Create university_calendar.yaml
        university_file = year_dir / "university_calendar.yaml"
        university_file.write_text(UNIVERSITY_CALENDAR_2024_YAML)

        return tmp_path

    def test_load_year(self, temp_calendar_root):
        """Test loading academic calendar for a year"""
        calendar = AcademicCalendarConfig(temp_calendar_root)
        calendar.loadYear(2024)

        assert calendar.year == 2024
        assert len(calendar.holidays) > 0
        assert len(calendar.sessions) > 0
        assert len(calendar.vacations) > 0

    def test_holidays_expansion(self, temp_calendar_root):
        """Test that holidays are properly expanded"""
        calendar = AcademicCalendarConfig(temp_calendar_root)
        calendar.loadYear(2024)

        # Check that holidays match expected
        assert calendar.holidays == EXPECTED_HOLIDAYS_2024

    def test_sessions_expansion(self, temp_calendar_root):
        """Test that session dates are properly expanded"""
        calendar = AcademicCalendarConfig(temp_calendar_root)
        calendar.loadYear(2024)

        # Check that sessions match expected
        assert calendar.sessions == EXPECTED_SESSIONS_2024

    def test_vacations_expansion(self, temp_calendar_root):
        """Test that vacation dates are properly expanded"""
        calendar = AcademicCalendarConfig(temp_calendar_root)
        calendar.loadYear(2024)

        # Check that vacations contain expected dates
        # Summer vacation should have 62 days (July + August)
        for expected_date in EXPECTED_VACATIONS_2024:
            assert expected_date in calendar.vacations

    def test_nonexistent_year(self, temp_calendar_root):
        """Test loading non-existent year raises error"""
        calendar = AcademicCalendarConfig(temp_calendar_root)

        with pytest.raises(FileNotFoundError):
            calendar.loadYear(2025)

    def test_to_dataframe(self, temp_calendar_root):
        """Test converting calendar to DataFrame"""
        try:
            import pandas as pd
        except ImportError:
            pytest.skip("pandas not installed")

        calendar = AcademicCalendarConfig(temp_calendar_root)
        calendar.loadYear(2024)

        df = calendar.toDataFrame()

        # Check that DataFrame has correct columns
        assert "date" in df.columns
        assert "holiday" in df.columns
        assert "session" in df.columns
        assert "vacation" in df.columns

        # Check that DataFrame has 366 days for 2024 (leap year)
        assert len(df) == 366

        # Check that some dates are correctly marked
        # 2024-01-01 should be holiday and vacation
        jan_1_row = df[df["date"].dt.date == date(2024, 1, 1)]
        assert jan_1_row["holiday"].iloc[0] == True
        assert jan_1_row["vacation"].iloc[0] == True

        # 2024-03-25 should be session
        mar_25_row = df[df["date"].dt.date == date(2024, 3, 25)]
        assert mar_25_row["session"].iloc[0] == True

    def test_to_dataframe_without_loading(self, temp_calendar_root):
        """Test calling toDataFrame without loading raises error"""
        try:
            import pandas as pd
        except ImportError:
            pytest.skip("pandas not installed")

        calendar = AcademicCalendarConfig(temp_calendar_root)

        with pytest.raises(RuntimeError):
            calendar.toDataFrame()


class TestConferenceCalendarConfig:
    """Tests for ConferenceCalendarConfig"""

    @pytest.fixture
    def temp_calendar_root(self, tmp_path):
        """Create temporary calendar directory structure"""
        year_dir = tmp_path / "2024"
        year_dir.mkdir()

        # Create hse_conferences.yaml
        conferences_file = year_dir / "hse_conferences.yaml"
        conferences_file.write_text(CONFERENCES_2024_YAML)

        return tmp_path

    def test_load_year(self, temp_calendar_root):
        """Test loading conference calendar for a year"""
        calendar = ConferenceCalendarConfig(temp_calendar_root)
        calendar.loadYear(2024)

        assert calendar.year == 2024
        assert len(calendar.dates) > 0

    def test_conferences_expansion(self, temp_calendar_root):
        """Test that conference dates are properly loaded"""
        calendar = ConferenceCalendarConfig(temp_calendar_root)
        calendar.loadYear(2024)

        # Check that conferences match expected
        assert calendar.dates == EXPECTED_CONFERENCES_2024

    def test_conferences_sorted(self, temp_calendar_root):
        """Test that conference dates are sorted"""
        calendar = ConferenceCalendarConfig(temp_calendar_root)
        calendar.loadYear(2024)

        # Check that dates are sorted
        assert calendar.dates == sorted(calendar.dates)

    def test_nonexistent_year(self, temp_calendar_root):
        """Test loading non-existent year raises error"""
        calendar = ConferenceCalendarConfig(temp_calendar_root)

        with pytest.raises(FileNotFoundError):
            calendar.loadYear(2025)

    def test_to_list(self, temp_calendar_root):
        """Test converting calendar to list"""
        calendar = ConferenceCalendarConfig(temp_calendar_root)
        calendar.loadYear(2024)

        dates_list = calendar.toList()

        assert isinstance(dates_list, list)
        assert dates_list == EXPECTED_CONFERENCES_2024

    def test_to_list_without_loading(self, temp_calendar_root):
        """Test calling toList without loading raises error"""
        calendar = ConferenceCalendarConfig(temp_calendar_root)

        with pytest.raises(RuntimeError):
            calendar.toList()

    def test_custom_filename_candidates(self, tmp_path):
        """Test loading conference calendar with custom filename"""
        year_dir = tmp_path / "2024"
        year_dir.mkdir()

        # Create file with custom name
        custom_file = year_dir / "custom_conferences.yaml"
        custom_file.write_text(CONFERENCES_2024_YAML)

        calendar = ConferenceCalendarConfig(
            tmp_path, filenameCandidates=["custom_conferences.yaml"]
        )
        calendar.loadYear(2024)

        assert calendar.dates == EXPECTED_CONFERENCES_2024


class TestCalendarDateExpansion:
    """Tests for date expansion logic"""

    def test_single_date(self, tmp_path):
        """Test expanding single date"""
        year_dir = tmp_path / "2024"
        year_dir.mkdir()

        yaml_content = """dates:
  - 2024-03-08
"""
        holidays_file = year_dir / "russian_holidays.yaml"
        holidays_file.write_text(yaml_content)

        university_file = year_dir / "university_calendar.yaml"
        university_file.write_text("session_dates: []\nvacation_dates: []")

        calendar = AcademicCalendarConfig(tmp_path)
        calendar.loadYear(2024)

        assert "2024-03-08" in calendar.holidays
        assert len(calendar.holidays) == 1

    def test_date_range(self, tmp_path):
        """Test expanding date range"""
        year_dir = tmp_path / "2024"
        year_dir.mkdir()

        yaml_content = """dates:
  - start: 2024-03-01
    end: 2024-03-05
"""
        holidays_file = year_dir / "russian_holidays.yaml"
        holidays_file.write_text(yaml_content)

        university_file = year_dir / "university_calendar.yaml"
        university_file.write_text("session_dates: []\nvacation_dates: []")

        calendar = AcademicCalendarConfig(tmp_path)
        calendar.loadYear(2024)

        # Should have 5 days
        assert "2024-03-01" in calendar.holidays
        assert "2024-03-02" in calendar.holidays
        assert "2024-03-03" in calendar.holidays
        assert "2024-03-04" in calendar.holidays
        assert "2024-03-05" in calendar.holidays
        assert len(calendar.holidays) == 5

    def test_range_without_end(self, tmp_path):
        """Test date range with start but no end"""
        year_dir = tmp_path / "2024"
        year_dir.mkdir()

        yaml_content = """dates:
  - start: 2024-03-08
"""
        holidays_file = year_dir / "russian_holidays.yaml"
        holidays_file.write_text(yaml_content)

        university_file = year_dir / "university_calendar.yaml"
        university_file.write_text("session_dates: []\nvacation_dates: []")

        calendar = AcademicCalendarConfig(tmp_path)
        calendar.loadYear(2024)

        # Should have just the start date
        assert "2024-03-08" in calendar.holidays
        assert len(calendar.holidays) == 1

    def test_mixed_dates_and_ranges(self, tmp_path):
        """Test mixed single dates and ranges"""
        year_dir = tmp_path / "2024"
        year_dir.mkdir()

        yaml_content = """dates:
  - start: 2024-01-01
    end: 2024-01-03
  - 2024-02-23
  - start: 2024-03-08
    end: 2024-03-10
"""
        holidays_file = year_dir / "russian_holidays.yaml"
        holidays_file.write_text(yaml_content)

        university_file = year_dir / "university_calendar.yaml"
        university_file.write_text("session_dates: []\nvacation_dates: []")

        calendar = AcademicCalendarConfig(tmp_path)
        calendar.loadYear(2024)

        # Check first range
        assert "2024-01-01" in calendar.holidays
        assert "2024-01-02" in calendar.holidays
        assert "2024-01-03" in calendar.holidays

        # Check single date
        assert "2024-02-23" in calendar.holidays

        # Check second range
        assert "2024-03-08" in calendar.holidays
        assert "2024-03-09" in calendar.holidays
        assert "2024-03-10" in calendar.holidays

        # Total: 3 + 1 + 3 = 7 dates
        assert len(calendar.holidays) == 7
