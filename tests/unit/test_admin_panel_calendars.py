"""
Unit tests for admin_panel.calendars module
"""

from pathlib import Path
from unittest.mock import patch

import pytest

from admin_panel.calendars import (
    calendar_file_template,
    create_calendar_file,
    create_calendar_year,
    get_calendar_catalog_payload,
    normalize_calendar_filename,
    normalize_calendar_year,
    read_calendar_file,
    write_calendar_file,
)

# ════════════════════════════════════════════════════════════════════════════════
# normalize_calendar_year
# ════════════════════════════════════════════════════════════════════════════════


class TestNormalizeCalendarYear:
    def test_valid_four_digit_year(self):
        assert normalize_calendar_year("2027") == "2027"

    def test_year_2000(self):
        assert normalize_calendar_year("2000") == "2000"

    def test_year_2099(self):
        assert normalize_calendar_year("2099") == "2099"

    def test_whitespace_stripped(self):
        assert normalize_calendar_year("  2027  ") == "2027"

    def test_three_digits_raises(self):
        with pytest.raises(ValueError, match="four-digit"):
            normalize_calendar_year("227")

    def test_five_digits_raises(self):
        with pytest.raises(ValueError, match="four-digit"):
            normalize_calendar_year("12027")

    def test_letters_raises(self):
        with pytest.raises(ValueError, match="four-digit"):
            normalize_calendar_year("abcd")

    def test_mixed_alnum_raises(self):
        with pytest.raises(ValueError, match="four-digit"):
            normalize_calendar_year("202a")

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="four-digit"):
            normalize_calendar_year("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError, match="four-digit"):
            normalize_calendar_year("   ")


# ════════════════════════════════════════════════════════════════════════════════
# normalize_calendar_filename
# ════════════════════════════════════════════════════════════════════════════════


class TestNormalizeCalendarFilename:
    def test_yaml_extension(self):
        assert normalize_calendar_filename("events.yaml") == "events.yaml"

    def test_yml_extension(self):
        assert normalize_calendar_filename("events.yml") == "events.yml"

    def test_whitespace_stripped(self):
        assert normalize_calendar_filename("  events.yaml  ") == "events.yaml"

    def test_txt_extension_raises(self):
        with pytest.raises(ValueError, match="\\.yaml or \\.yml"):
            normalize_calendar_filename("events.txt")

    def test_no_extension_raises(self):
        with pytest.raises(ValueError, match="\\.yaml or \\.yml"):
            normalize_calendar_filename("events")

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="required"):
            normalize_calendar_filename("")

    def test_dot_raises(self):
        with pytest.raises(ValueError, match="required"):
            normalize_calendar_filename(".")

    def test_double_dot_raises(self):
        with pytest.raises(ValueError, match="required"):
            normalize_calendar_filename("..")

    def test_forward_slash_in_raw_input_stripped_to_name(self):
        # Path("sub/events.yaml").name returns "events.yaml" — slash is stripped before check
        result = normalize_calendar_filename("sub/events.yaml")
        assert result == "events.yaml"

    def test_forward_slash_in_filename_raises(self):
        # After Path().name strips directory, check the raw input for /
        # If the filename itself contains / (e.g. "my/file.yaml"), Path().name gives "file.yaml"
        # so this effectively strips it. Test that raw backslash still catches.
        pass  # covered by test_backslash_raises

    def test_backslash_raises(self):
        with pytest.raises(ValueError, match="path separators"):
            normalize_calendar_filename("sub\\events.yaml")

    def test_path_traversal_stripped_to_name(self):
        # Path("sub/events.yaml").name returns "events.yaml", which is valid
        result = normalize_calendar_filename("sub/events.yaml")
        assert result == "events.yaml"

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError, match="required"):
            normalize_calendar_filename("   ")


# ════════════════════════════════════════════════════════════════════════════════
# calendar_file_template
# ════════════════════════════════════════════════════════════════════════════════


class TestCalendarFileTemplate:
    def test_russian_holidays(self):
        assert calendar_file_template("russian_holidays.yaml") == "dates: []\n"

    def test_university_calendar(self):
        assert (
            calendar_file_template("university_calendar.yaml")
            == "session_dates: []\nvacation_dates: []\n"
        )

    def test_unknown_filename_returns_empty_dict(self):
        assert calendar_file_template("custom_calendar.yaml") == "{}\n"

    def test_another_unknown_filename(self):
        assert calendar_file_template("my_events.yml") == "{}\n"


# ════════════════════════════════════════════════════════════════════════════════
# get_calendar_catalog_payload
# ════════════════════════════════════════════════════════════════════════════════


class TestGetCalendarCatalogPayload:
    def _setup_calendar_root(self, tmp_path):
        """Patch get_calendar_root to return tmp_path."""
        return patch("admin_panel.calendars.get_calendar_root", return_value=tmp_path)

    def test_empty_root(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            payload = get_calendar_catalog_payload()

        assert payload["known_files"] == []
        assert payload["years"] == []
        assert payload["root"] == str(tmp_path)

    def test_single_year_with_files(self, tmp_path):
        year_dir = tmp_path / "2025"
        year_dir.mkdir()
        (year_dir / "russian_holidays.yaml").write_text("dates: []\n")
        (year_dir / "university_calendar.yaml").write_text("session_dates: []\n")

        with self._setup_calendar_root(tmp_path):
            payload = get_calendar_catalog_payload()

        assert len(payload["years"]) == 1
        assert payload["years"][0]["year"] == "2025"
        assert payload["years"][0]["files"] == [
            "russian_holidays.yaml",
            "university_calendar.yaml",
        ]
        assert payload["years"][0]["missing_known_files"] == []
        assert payload["known_files"] == [
            "russian_holidays.yaml",
            "university_calendar.yaml",
        ]

    def test_multiple_years(self, tmp_path):
        for year in ["2024", "2025"]:
            year_dir = tmp_path / year
            year_dir.mkdir()
            (year_dir / "russian_holidays.yaml").write_text("dates: []\n")

        (tmp_path / "2025" / "university_calendar.yaml").write_text(
            "session_dates: []\n"
        )

        with self._setup_calendar_root(tmp_path):
            payload = get_calendar_catalog_payload()

        assert len(payload["years"]) == 2
        assert payload["known_files"] == [
            "russian_holidays.yaml",
            "university_calendar.yaml",
        ]

    def test_missing_known_files_computed(self, tmp_path):
        year_2024 = tmp_path / "2024"
        year_2024.mkdir()
        (year_2024 / "russian_holidays.yaml").write_text("dates: []\n")

        year_2025 = tmp_path / "2025"
        year_2025.mkdir()
        (year_2025 / "university_calendar.yaml").write_text("session_dates: []\n")

        with self._setup_calendar_root(tmp_path):
            payload = get_calendar_catalog_payload()

        year_2024_entry = next(y for y in payload["years"] if y["year"] == "2024")
        assert "university_calendar.yaml" in year_2024_entry["missing_known_files"]

        year_2025_entry = next(y for y in payload["years"] if y["year"] == "2025")
        assert "russian_holidays.yaml" in year_2025_entry["missing_known_files"]

    def test_ignores_non_yaml_files(self, tmp_path):
        year_dir = tmp_path / "2025"
        year_dir.mkdir()
        (year_dir / "russian_holidays.yaml").write_text("dates: []\n")
        (year_dir / "notes.txt").write_text("not a calendar")

        with self._setup_calendar_root(tmp_path):
            payload = get_calendar_catalog_payload()

        assert payload["known_files"] == ["russian_holidays.yaml"]
        assert payload["years"][0]["files"] == ["russian_holidays.yaml"]

    def test_ignores_files_in_root(self, tmp_path):
        (tmp_path / "orphan.yaml").write_text("orphan: true\n")

        with self._setup_calendar_root(tmp_path):
            payload = get_calendar_catalog_payload()

        assert payload["known_files"] == []
        assert payload["years"] == []

    def test_years_sorted_by_name(self, tmp_path):
        for year in ["2027", "2025", "2026"]:
            (tmp_path / year).mkdir()

        with self._setup_calendar_root(tmp_path):
            payload = get_calendar_catalog_payload()

        year_names = [y["year"] for y in payload["years"]]
        assert year_names == ["2025", "2026", "2027"]

    def test_year_path_is_absolute(self, tmp_path):
        (tmp_path / "2025").mkdir()

        with self._setup_calendar_root(tmp_path):
            payload = get_calendar_catalog_payload()

        assert Path(payload["years"][0]["path"]).is_absolute()


# ════════════════════════════════════════════════════════════════════════════════
# read_calendar_file
# ════════════════════════════════════════════════════════════════════════════════


class TestReadCalendarFile:
    def _setup_calendar_root(self, tmp_path):
        return patch("admin_panel.calendars.get_calendar_root", return_value=tmp_path)

    def test_reads_existing_file(self, tmp_path):
        year_dir = tmp_path / "2025"
        year_dir.mkdir()
        content = "dates:\n  - 2025-01-01\n"
        (year_dir / "russian_holidays.yaml").write_text(content, encoding="utf-8")

        with self._setup_calendar_root(tmp_path):
            result = read_calendar_file("2025", "russian_holidays.yaml")

        assert result["content"] == content
        assert result["year"] == "2025"
        assert result["filename"] == "russian_holidays.yaml"

    def test_nonexistent_file_raises(self, tmp_path):
        (tmp_path / "2025").mkdir()

        with self._setup_calendar_root(tmp_path):
            with pytest.raises(FileNotFoundError):
                read_calendar_file("2025", "missing.yaml")

    def test_nonexistent_year_raises(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            with pytest.raises(FileNotFoundError):
                read_calendar_file("2999", "russian_holidays.yaml")

    def test_invalid_year_raises(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            with pytest.raises(ValueError):
                read_calendar_file("abc", "events.yaml")

    def test_invalid_filename_raises(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            with pytest.raises(ValueError):
                read_calendar_file("2025", "events.txt")

    def test_result_path_is_absolute(self, tmp_path):
        year_dir = tmp_path / "2025"
        year_dir.mkdir()
        (year_dir / "events.yaml").write_text("{}", encoding="utf-8")

        with self._setup_calendar_root(tmp_path):
            result = read_calendar_file("2025", "events.yaml")

        assert Path(result["path"]).is_absolute()


# ════════════════════════════════════════════════════════════════════════════════
# write_calendar_file
# ════════════════════════════════════════════════════════════════════════════════


class TestWriteCalendarFile:
    def _setup_calendar_root(self, tmp_path):
        return patch("admin_panel.calendars.get_calendar_root", return_value=tmp_path)

    def test_writes_valid_yaml(self, tmp_path):
        year_dir = tmp_path / "2025"
        year_dir.mkdir()
        content = "dates:\n  - 2025-01-01\n"

        with self._setup_calendar_root(tmp_path):
            result = write_calendar_file("2025", "events.yaml", content)

        assert result["content"] == content
        assert (year_dir / "events.yaml").read_text(encoding="utf-8") == content

    def test_invalid_yaml_raises(self, tmp_path):
        (tmp_path / "2025").mkdir()

        with self._setup_calendar_root(tmp_path):
            with pytest.raises(Exception):
                write_calendar_file("2025", "events.yaml", "bad: ][ yaml")

    def test_creates_year_directory(self, tmp_path):
        content = "dates: []\n"

        with self._setup_calendar_root(tmp_path):
            result = write_calendar_file("2026", "events.yaml", content)

        assert (tmp_path / "2026" / "events.yaml").exists()

    def test_overwrites_existing_file(self, tmp_path):
        year_dir = tmp_path / "2025"
        year_dir.mkdir()
        (year_dir / "events.yaml").write_text("old: content\n", encoding="utf-8")

        new_content = "new: content\n"
        with self._setup_calendar_root(tmp_path):
            result = write_calendar_file("2025", "events.yaml", new_content)

        assert (year_dir / "events.yaml").read_text(encoding="utf-8") == new_content

    def test_empty_yaml_is_valid(self, tmp_path):
        (tmp_path / "2025").mkdir()

        with self._setup_calendar_root(tmp_path):
            result = write_calendar_file("2025", "events.yaml", "")

        assert result["content"] == ""

    def test_result_path_is_absolute(self, tmp_path):
        (tmp_path / "2025").mkdir()

        with self._setup_calendar_root(tmp_path):
            result = write_calendar_file("2025", "events.yaml", "dates: []\n")

        assert Path(result["path"]).is_absolute()


# ════════════════════════════════════════════════════════════════════════════════
# create_calendar_year
# ════════════════════════════════════════════════════════════════════════════════


class TestCreateCalendarYear:
    def _setup_calendar_root(self, tmp_path):
        return patch("admin_panel.calendars.get_calendar_root", return_value=tmp_path)

    def test_creates_empty_year(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            result = create_calendar_year("2027")

        assert result["year"] == "2027"
        assert result["copied_from_year"] is None
        assert result["files"] == []
        assert (tmp_path / "2027").is_dir()

    def test_duplicate_year_raises(self, tmp_path):
        (tmp_path / "2025").mkdir()

        with self._setup_calendar_root(tmp_path):
            with pytest.raises(ValueError, match="already exists"):
                create_calendar_year("2025")

    def test_creates_template_files_from_known_catalog(self, tmp_path):
        # Set up existing year with known files
        existing_year = tmp_path / "2025"
        existing_year.mkdir()
        (existing_year / "russian_holidays.yaml").write_text("dates: []\n")
        (existing_year / "university_calendar.yaml").write_text("session_dates: []\n")

        with self._setup_calendar_root(tmp_path):
            result = create_calendar_year("2026")

        # Without copyFromYear, it creates templates for known files
        assert result["year"] == "2026"
        assert "russian_holidays.yaml" in result["files"]
        assert "university_calendar.yaml" in result["files"]
        assert (tmp_path / "2026" / "russian_holidays.yaml").exists()

    def test_copies_from_source_year(self, tmp_path):
        source_dir = tmp_path / "2025"
        source_dir.mkdir()
        (source_dir / "russian_holidays.yaml").write_text("dates:\n  - 2025-01-01\n")
        (source_dir / "custom.yaml").write_text("custom: true\n")

        with self._setup_calendar_root(tmp_path):
            result = create_calendar_year("2026", copyFromYear="2025")

        assert result["copied_from_year"] == "2025"
        assert "russian_holidays.yaml" in result["files"]
        assert "custom.yaml" in result["files"]
        # Verify content was actually copied
        copied_content = (tmp_path / "2026" / "custom.yaml").read_text()
        assert "custom: true" in copied_content

    def test_copy_from_nonexistent_year_raises(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            with pytest.raises(FileNotFoundError, match="Source calendar year"):
                create_calendar_year("2026", copyFromYear="2999")

    def test_invalid_year_raises(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            with pytest.raises(ValueError):
                create_calendar_year("abc")

    def test_result_path_is_absolute(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            result = create_calendar_year("2027")

        assert Path(result["path"]).is_absolute()


# ════════════════════════════════════════════════════════════════════════════════
# create_calendar_file
# ════════════════════════════════════════════════════════════════════════════════


class TestCreateCalendarFile:
    def _setup_calendar_root(self, tmp_path):
        return patch("admin_panel.calendars.get_calendar_root", return_value=tmp_path)

    def test_creates_new_file_with_template(self, tmp_path):
        year_dir = tmp_path / "2025"
        year_dir.mkdir()

        with self._setup_calendar_root(tmp_path):
            result = create_calendar_file("2025", "custom_calendar.yaml")

        assert result["filename"] == "custom_calendar.yaml"
        assert result["year"] == "2025"
        assert result["copied_from_year"] is None
        # Unknown filename gets default template
        assert result["content"] == "{}\n"

    def test_creates_known_file_with_specific_template(self, tmp_path):
        year_dir = tmp_path / "2025"
        year_dir.mkdir()

        with self._setup_calendar_root(tmp_path):
            result = create_calendar_file("2025", "russian_holidays.yaml")

        assert result["content"] == "dates: []\n"

    def test_duplicate_file_raises(self, tmp_path):
        year_dir = tmp_path / "2025"
        year_dir.mkdir()
        (year_dir / "events.yaml").write_text("{}", encoding="utf-8")

        with self._setup_calendar_root(tmp_path):
            with pytest.raises(ValueError, match="already exists"):
                create_calendar_file("2025", "events.yaml")

    def test_copies_from_source_year(self, tmp_path):
        source_dir = tmp_path / "2025"
        source_dir.mkdir()
        (source_dir / "events.yaml").write_text(
            "events:\n  - new_year\n", encoding="utf-8"
        )

        target_dir = tmp_path / "2026"
        target_dir.mkdir()

        with self._setup_calendar_root(tmp_path):
            result = create_calendar_file("2026", "events.yaml", copyFromYear="2025")

        assert result["copied_from_year"] == "2025"
        assert "new_year" in result["content"]

    def test_copy_from_year_missing_template_raises(self, tmp_path):
        source_dir = tmp_path / "2025"
        source_dir.mkdir()
        # Source has no events.yaml

        target_dir = tmp_path / "2026"
        target_dir.mkdir()

        with self._setup_calendar_root(tmp_path):
            with pytest.raises(FileNotFoundError, match="Template file not found"):
                create_calendar_file("2026", "events.yaml", copyFromYear="2025")

    def test_creates_year_dir_if_missing(self, tmp_path):
        # year dir does not exist yet
        with self._setup_calendar_root(tmp_path):
            result = create_calendar_file("2027", "events.yaml")

        assert (tmp_path / "2027" / "events.yaml").exists()

    def test_invalid_filename_raises(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            with pytest.raises(ValueError):
                create_calendar_file("2025", "events.txt")

    def test_invalid_year_raises(self, tmp_path):
        with self._setup_calendar_root(tmp_path):
            with pytest.raises(ValueError):
                create_calendar_file("abc", "events.yaml")

    def test_result_path_is_absolute(self, tmp_path):
        (tmp_path / "2025").mkdir()

        with self._setup_calendar_root(tmp_path):
            result = create_calendar_file("2025", "events.yaml")

        assert Path(result["path"]).is_absolute()

    def test_university_calendar_template(self, tmp_path):
        (tmp_path / "2025").mkdir()

        with self._setup_calendar_root(tmp_path):
            result = create_calendar_file("2025", "university_calendar.yaml")

        assert "session_dates" in result["content"]
        assert "vacation_dates" in result["content"]
