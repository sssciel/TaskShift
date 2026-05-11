"""Unit tests for admin_panel.logs module"""

import json
from unittest.mock import patch

from admin_panel.logs import (
    TASKSHIFT_LOG_PATTERN,
    build_job_logs_payload,
    build_taskshift_log_payload,
    normalize_job_log_status,
    normalize_log_limit,
    normalize_page,
    normalize_status_filters,
)

# ════════════════════════════════════════════════════════════════════════════════
# 1. TestNormalizeLogLimit
# ════════════════════════════════════════════════════════════════════════════════


class TestNormalizeLogLimit:
    def test_valid_int(self):
        assert normalize_log_limit(100) == 100

    def test_string_int(self):
        assert normalize_log_limit("50") == 50

    def test_default_on_none(self):
        assert normalize_log_limit(None) == 300

    def test_default_on_non_numeric_string(self):
        assert normalize_log_limit("abc") == 300

    def test_default_on_empty_string(self):
        assert normalize_log_limit("") == 300

    def test_default_on_float_string(self):
        assert normalize_log_limit("3.14") == 300

    def test_default_on_list(self):
        assert normalize_log_limit([1, 2]) == 300

    def test_clamp_below_one(self):
        assert normalize_log_limit(0) == 1

    def test_clamp_negative(self):
        assert normalize_log_limit(-5) == 1

    def test_clamp_above_maximum(self):
        assert normalize_log_limit(5000) == 2000

    def test_at_maximum(self):
        assert normalize_log_limit(2000) == 2000

    def test_one_above_maximum(self):
        assert normalize_log_limit(2001) == 2000

    def test_custom_default_and_maximum(self):
        assert normalize_log_limit(None, default=50, maximum=100) == 50
        assert normalize_log_limit(150, default=50, maximum=100) == 100
        assert normalize_log_limit(-10, default=50, maximum=100) == 1

    def test_string_at_boundary(self):
        assert normalize_log_limit("1") == 1


# ════════════════════════════════════════════════════════════════════════════════
# 2. TestNormalizePage
# ════════════════════════════════════════════════════════════════════════════════


class TestNormalizePage:
    def test_valid_int(self):
        assert normalize_page(3) == 3

    def test_string_int(self):
        assert normalize_page("5") == 5

    def test_default_on_none(self):
        assert normalize_page(None) == 1

    def test_default_on_non_numeric(self):
        assert normalize_page("abc") == 1

    def test_default_on_empty_string(self):
        assert normalize_page("") == 1

    def test_default_on_float_string(self):
        assert normalize_page("3.14") == 1

    def test_zero_clamped_to_one(self):
        assert normalize_page(0) == 1

    def test_negative_clamped_to_one(self):
        assert normalize_page(-10) == 1

    def test_custom_default(self):
        assert normalize_page(None, default=5) == 5

    def test_custom_default_negative_uses_default(self):
        assert normalize_page(-3, default=5) == 1


# ════════════════════════════════════════════════════════════════════════════════
# 3. TestNormalizeStatusFilters
# ════════════════════════════════════════════════════════════════════════════════


class TestNormalizeStatusFilters:
    def test_empty_list(self):
        assert normalize_status_filters([]) == []

    def test_none_input(self):
        assert normalize_status_filters(None) == []

    def test_valid_single_status(self):
        assert normalize_status_filters(["INFO"]) == ["INFO"]

    def test_multiple_statuses(self):
        result = normalize_status_filters(["INFO", "ERROR", "WARN"])
        assert result == ["INFO", "ERROR", "WARN"]

    def test_mixed_case_uppercased(self):
        result = normalize_status_filters(["info", "Error", "warn"])
        assert result == ["INFO", "ERROR", "WARN"]

    def test_comma_separated_values(self):
        result = normalize_status_filters(["INFO,ERROR,WARN"])
        assert result == ["INFO", "ERROR", "WARN"]

    def test_duplicates_removed(self):
        result = normalize_status_filters(["INFO", "INFO", "ERROR"])
        assert result == ["INFO", "ERROR"]

    def test_comma_separated_duplicates(self):
        result = normalize_status_filters(["INFO,INFO,ERROR"])
        assert result == ["INFO", "ERROR"]

    def test_none_in_list_skipped(self):
        result = normalize_status_filters([None, "INFO", None])
        assert result == ["INFO"]

    def test_whitespace_trimmed(self):
        result = normalize_status_filters([" INFO ", " ERROR "])
        assert result == ["INFO", "ERROR"]

    def test_empty_strings_ignored(self):
        result = normalize_status_filters(["", "INFO", ""])
        assert result == ["INFO"]

    def test_integers_converted_and_uppercased(self):
        result = normalize_status_filters([123])
        assert result == ["123"]

    def test_mixed_comma_and_whitespace(self):
        result = normalize_status_filters(["INFO , ERROR , WARN"])
        assert result == ["INFO", "ERROR", "WARN"]


# ════════════════════════════════════════════════════════════════════════════════
# 4. TestNormalizeJobLogStatus
# ════════════════════════════════════════════════════════════════════════════════


class TestNormalizeJobLogStatus:
    def test_attempted_alias(self):
        assert normalize_job_log_status("ATTEMPTED") == "LAUNCH_ATTEMPTED"

    def test_attempted_lowercase(self):
        assert normalize_job_log_status("attempted") == "LAUNCH_ATTEMPTED"

    def test_failed_alias(self):
        assert normalize_job_log_status("FAILED") == "LAUNCH_FAILED"

    def test_failed_lowercase(self):
        assert normalize_job_log_status("failed") == "LAUNCH_FAILED"

    def test_succeeded_alias(self):
        assert normalize_job_log_status("SUCCEEDED") == "LEFT_PENDING_QUEUE"

    def test_success_alias(self):
        assert normalize_job_log_status("SUCCESS") == "LEFT_PENDING_QUEUE"

    def test_success_lowercase(self):
        assert normalize_job_log_status("success") == "LEFT_PENDING_QUEUE"

    def test_already_normalized_launch_attempted(self):
        assert normalize_job_log_status("LAUNCH_ATTEMPTED") == "LAUNCH_ATTEMPTED"

    def test_already_normalized_launch_failed(self):
        assert normalize_job_log_status("LAUNCH_FAILED") == "LAUNCH_FAILED"

    def test_already_normalized_left_pending_queue(self):
        assert normalize_job_log_status("LEFT_PENDING_QUEUE") == "LEFT_PENDING_QUEUE"

    def test_none_returns_unknown(self):
        assert normalize_job_log_status(None) == "UNKNOWN"

    def test_empty_string_returns_unknown(self):
        assert normalize_job_log_status("") == "UNKNOWN"

    def test_whitespace_only_returns_unknown(self):
        assert normalize_job_log_status("   ") == "UNKNOWN"

    def test_unknown_status_passthrough(self):
        assert normalize_job_log_status("RUNNING") == "RUNNING"

    def test_unknown_status_lowercase(self):
        assert normalize_job_log_status("running") == "RUNNING"


# ════════════════════════════════════════════════════════════════════════════════
# 5. TestBuildTaskshiftLogPayload
# ════════════════════════════════════════════════════════════════════════════════


class TestBuildTaskshiftLogPayload:
    """Tests for build_taskshift_log_payload with mocked get_logs_root."""

    def _write_log(self, tmp_path, content):
        """Helper: write content to the taskshift.log file in tmp_path."""
        (tmp_path / "taskshift.log").write_text(content, encoding="utf-8")

    def test_missing_log_file_returns_empty_entries(self, tmp_path):
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        assert result["entries"] == []
        assert result["total_entries"] == 0
        assert result["filtered_entries"] == 0
        assert result["shown_entries"] == 0

    def test_empty_log_file_returns_empty_entries(self, tmp_path):
        self._write_log(tmp_path, "")
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        assert result["entries"] == []
        assert result["total_entries"] == 0

    def test_parses_single_structured_line(self, tmp_path):
        line = "2025-01-15 10:30:00.123 | INFO | scheduler | Job started successfully"
        self._write_log(tmp_path, line)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        assert len(result["entries"]) == 1
        entry = result["entries"][0]
        assert entry["timestamp"] == "2025-01-15 10:30:00.123"
        assert entry["level"] == "INFO"
        assert entry["source"] == "scheduler"
        assert entry["message"] == "Job started successfully"
        assert entry["raw"] == line

    def test_parses_multiple_structured_lines(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | First message\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Second message\n"
            "2025-01-15 10:30:02.000 | WARN | monitor | Third message"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        assert result["total_entries"] == 3
        assert len(result["entries"]) == 3

    def test_entries_reversed_newest_first(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | First\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Second\n"
            "2025-01-15 10:30:02.000 | WARN | monitor | Third"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        entries = result["entries"]
        assert entries[0]["timestamp"] == "2025-01-15 10:30:02.000"
        assert entries[1]["timestamp"] == "2025-01-15 10:30:01.000"
        assert entries[2]["timestamp"] == "2025-01-15 10:30:00.000"

    def test_multiline_message_continuation(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Main message\n"
            "  continuation line 1\n"
            "  continuation line 2\n"
            "2025-01-15 10:30:01.000 | INFO | scheduler | Next entry"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        assert result["total_entries"] == 2
        first = result["entries"][1]  # reversed, so first entry is last
        assert (
            first["message"]
            == "Main message\n  continuation line 1\n  continuation line 2"
        )
        assert "  continuation line 1" in first["raw"]

    def test_unstructured_line_before_any_entry(self, tmp_path):
        lines = (
            "Some random unstructured text\n"
            "2025-01-15 10:30:00.000 | INFO | scheduler | Structured entry"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        assert result["total_entries"] == 2
        # Find the OTHER entry
        other_entries = [e for e in result["entries"] if e["level"] == "OTHER"]
        assert len(other_entries) == 1
        assert other_entries[0]["message"] == "Some random unstructured text"
        assert other_entries[0]["source"] == "unstructured"
        assert other_entries[0]["timestamp"] is None

    def test_unstructured_lines_handled_as_other_level(self, tmp_path):
        """Unstructured lines that follow a structured line are appended as
        continuation text to the preceding entry. Only unstructured lines that
        appear *before* any structured header become standalone OTHER entries."""
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Entry 1\n"
            "random noise without pattern\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Entry 2"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        # The unstructured line is a continuation of Entry 1, so only 2 entries
        assert result["total_entries"] == 2
        # Entry 1's message includes the continuation
        first_entry = [e for e in result["entries"] if e["level"] == "INFO"][0]
        assert "random noise without pattern" in first_entry["message"]

    def test_filter_by_query_text_matches_timestamp(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Message A\n"
            "2025-12-31 23:59:59.999 | ERROR | executor | Message B"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(query="2025-12-31")
        assert result["filtered_entries"] == 1
        assert result["entries"][0]["message"] == "Message B"

    def test_filter_by_query_text_matches_level(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Message A\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Message B"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(query="ERROR")
        assert result["filtered_entries"] == 1
        assert result["entries"][0]["message"] == "Message B"

    def test_filter_by_query_text_matches_source(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Message A\n"
            "2025-01-15 10:30:01.000 | INFO | executor | Message B"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(query="executor")
        assert result["filtered_entries"] == 1

    def test_filter_by_query_text_matches_message(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Hello world\n"
            "2025-01-15 10:30:01.000 | INFO | executor | Goodbye world"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(query="hello")
        assert result["filtered_entries"] == 1
        assert "Hello" in result["entries"][0]["message"]

    def test_filter_by_query_text_no_match(self, tmp_path):
        lines = "2025-01-15 10:30:00.000 | INFO | scheduler | Hello world"
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(query="nonexistent")
        assert result["filtered_entries"] == 0
        assert result["entries"] == []

    def test_filter_by_status_levels_single(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Info message\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Error message\n"
            "2025-01-15 10:30:02.000 | WARN | monitor | Warn message"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(statuses=["ERROR"])
        assert result["filtered_entries"] == 1
        assert result["entries"][0]["level"] == "ERROR"
        assert result["selected_statuses"] == ["ERROR"]

    def test_filter_by_status_levels_multiple(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Info message\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Error message\n"
            "2025-01-15 10:30:02.000 | WARN | monitor | Warn message"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(statuses=["INFO", "WARN"])
        assert result["filtered_entries"] == 2

    def test_filter_by_status_levels_none_selected_shows_all(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Info message\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Error message"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(statuses=None)
        assert result["filtered_entries"] == 2

    def test_filter_combined_query_and_status(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | hello world\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | goodbye world\n"
            "2025-01-15 10:30:02.000 | INFO | monitor | hello again"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(query="hello", statuses=["INFO"])
        assert result["filtered_entries"] == 2

    def test_pagination_page_size(self, tmp_path):
        lines = "\n".join(
            f"2025-01-15 10:30:0{i}.000 | INFO | scheduler | Message {i}"
            for i in range(5)
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(limit=2)
        assert result["page_size"] == 2
        assert result["shown_entries"] == 2

    def test_pagination_page_number(self, tmp_path):
        lines = "\n".join(
            f"2025-01-15 10:30:0{i}.000 | INFO | scheduler | Message {i}"
            for i in range(5)
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(page=2, limit=2)
        # Entries are reversed, so page 2 has messages 2,3 (reversed: 3,2)
        assert result["page"] == 2
        assert result["shown_entries"] == 2

    def test_pagination_total_pages(self, tmp_path):
        lines = "\n".join(
            f"2025-01-15 10:30:0{i}.000 | INFO | scheduler | Message {i}"
            for i in range(5)
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(limit=2)
        # 5 entries, page_size=2 -> ceil(5/2)=3 pages
        assert result["total_pages"] == 3

    def test_pagination_has_prev_and_next(self, tmp_path):
        lines = "\n".join(
            f"2025-01-15 10:30:0{i}.000 | INFO | scheduler | Message {i}"
            for i in range(5)
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result_page1 = build_taskshift_log_payload(page=1, limit=2)
            result_page2 = build_taskshift_log_payload(page=2, limit=2)
            result_page3 = build_taskshift_log_payload(page=3, limit=2)
        assert result_page1["has_prev_page"] is False
        assert result_page1["has_next_page"] is True
        assert result_page2["has_prev_page"] is True
        assert result_page2["has_next_page"] is True
        assert result_page3["has_prev_page"] is True
        assert result_page3["has_next_page"] is False

    def test_pagination_page_beyond_total_clamped(self, tmp_path):
        lines = "2025-01-15 10:30:00.000 | INFO | scheduler | Only one entry"
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(page=99, limit=10)
        assert result["page"] == 1
        assert result["total_pages"] == 1

    def test_status_counts_computed_correctly(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Info 1\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Error 1\n"
            "2025-01-15 10:30:02.000 | INFO | monitor | Info 2\n"
            "2025-01-15 10:30:03.000 | ERROR | scheduler | Error 2\n"
            "2025-01-15 10:30:04.000 | WARN | executor | Warn 1"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        counts = result["status_counts"]
        assert counts["INFO"] == 2
        assert counts["ERROR"] == 2
        assert counts["WARN"] == 1

    def test_status_counts_reflect_filtered_results(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Info 1\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Error 1\n"
            "2025-01-15 10:30:02.000 | INFO | monitor | Info 2"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(statuses=["INFO"])
        counts = result["status_counts"]
        assert counts.get("INFO") == 2
        assert counts.get("ERROR") is None

    def test_available_statuses_populated(self, tmp_path):
        lines = (
            "2025-01-15 10:30:00.000 | INFO | scheduler | Info\n"
            "2025-01-15 10:30:01.000 | ERROR | executor | Error\n"
            "2025-01-15 10:30:02.000 | DEBUG | monitor | Debug"
        )
        self._write_log(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        assert result["available_statuses"] == ["DEBUG", "ERROR", "INFO"]

    def test_file_path_in_result(self, tmp_path):
        self._write_log(tmp_path, "2025-01-15 10:30:00.000 | INFO | s | m")
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        assert "taskshift.log" in result["file"]

    def test_query_preserved_in_result(self, tmp_path):
        self._write_log(tmp_path, "2025-01-15 10:30:00.000 | INFO | s | m")
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(query="test search")
        assert result["query"] == "test search"

    def test_pagination_with_no_entries(self, tmp_path):
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload(page=1, limit=10)
        assert result["total_pages"] == 1
        assert result["has_prev_page"] is False
        assert result["has_next_page"] is False

    def test_timestamp_with_comma_format(self, tmp_path):
        line = "2025-01-15 10:30,00,123 | INFO | scheduler | Comma timestamp"
        self._write_log(tmp_path, line)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_taskshift_log_payload()
        assert len(result["entries"]) == 1
        assert result["entries"][0]["timestamp"] == "2025-01-15 10:30,00,123"


# ════════════════════════════════════════════════════════════════════════════════
# 6. TestBuildJobLogsPayload
# ════════════════════════════════════════════════════════════════════════════════


class TestBuildJobLogsPayload:
    """Tests for build_job_logs_payload with mocked get_logs_root."""

    def _write_jsonl(self, tmp_path, content):
        """Helper: write content to the job_launches.jsonl file in tmp_path."""
        (tmp_path / "job_launches.jsonl").write_text(content, encoding="utf-8")

    def _make_entry(self, **overrides):
        """Helper: build a JSONL entry dict with defaults."""
        entry = {
            "job_id": "job-001",
            "job_name": "test_job",
            "status": "LAUNCH_ATTEMPTED",
            "partition": "gpu",
            "feature": "default",
            "nodes": ["node01", "node02"],
            "reason": "",
        }
        entry.update(overrides)
        return entry

    def test_missing_log_file_returns_empty_entries(self, tmp_path):
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["entries"] == []
        assert result["total_entries"] == 0
        assert result["filtered_entries"] == 0

    def test_empty_log_file_returns_empty_entries(self, tmp_path):
        self._write_jsonl(tmp_path, "")
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["entries"] == []
        assert result["total_entries"] == 0

    def test_parses_single_jsonl_entry(self, tmp_path):
        entry = self._make_entry()
        self._write_jsonl(
            tmp_path,
            '{"job_id": "job-001", "job_name": "test_job", '
            '"status": "LAUNCH_ATTEMPTED", "partition": "gpu", '
            '"feature": "default", "nodes": ["node01"], "reason": ""}',
        )
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["total_entries"] == 1
        assert result["entries"][0]["job_id"] == "job-001"
        assert result["entries"][0]["status"] == "LAUNCH_ATTEMPTED"

    def test_parses_multiple_jsonl_entries(self, tmp_path):
        entry1 = self._make_entry(job_id="job-001")
        entry2 = self._make_entry(job_id="job-002")
        lines = json.dumps(entry1) + "\n" + json.dumps(entry2)
        self._write_jsonl(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["total_entries"] == 2

    def test_status_normalization_attempted(self, tmp_path):
        self._write_jsonl(tmp_path, '{"job_id": "job-001", "status": "attempted"}')
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["entries"][0]["status"] == "LAUNCH_ATTEMPTED"

    def test_status_normalization_failed(self, tmp_path):
        self._write_jsonl(tmp_path, '{"job_id": "job-001", "status": "failed"}')
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["entries"][0]["status"] == "LAUNCH_FAILED"

    def test_status_normalization_succeeded(self, tmp_path):
        self._write_jsonl(tmp_path, '{"job_id": "job-001", "status": "succeeded"}')
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["entries"][0]["status"] == "LEFT_PENDING_QUEUE"

    def test_status_normalization_success(self, tmp_path):
        self._write_jsonl(tmp_path, '{"job_id": "job-001", "status": "success"}')
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["entries"][0]["status"] == "LEFT_PENDING_QUEUE"

    def test_filter_by_job_id(self, tmp_path):
        entry1 = self._make_entry(job_id="job-001")
        entry2 = self._make_entry(job_id="job-002")
        lines = json.dumps(entry1) + "\n" + json.dumps(entry2)
        self._write_jsonl(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(jobId="job-002")
        assert result["filtered_entries"] == 1
        assert result["entries"][0]["job_id"] == "job-002"

    def test_filter_by_job_id_partial_match(self, tmp_path):
        entry1 = self._make_entry(job_id="job-001")
        entry2 = self._make_entry(job_id="job-002")
        lines = json.dumps(entry1) + "\n" + json.dumps(entry2)
        self._write_jsonl(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(jobId="job-00")
        assert result["filtered_entries"] == 2

    def test_filter_by_job_id_no_match(self, tmp_path):
        entry = self._make_entry(job_id="job-001")
        self._write_jsonl(tmp_path, json.dumps(entry))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(jobId="job-999")
        assert result["filtered_entries"] == 0

    def test_filter_by_query_text_matches_job_name(self, tmp_path):
        entry1 = self._make_entry(job_name="important_job")
        entry2 = self._make_entry(job_name="other_job")
        lines = json.dumps(entry1) + "\n" + json.dumps(entry2)
        self._write_jsonl(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(query="important")
        assert result["filtered_entries"] == 1
        assert result["entries"][0]["job_name"] == "important_job"

    def test_filter_by_query_text_matches_status(self, tmp_path):
        self._write_jsonl(
            tmp_path, '{"job_id": "job-001", "status": "LAUNCH_ATTEMPTED"}'
        )
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(query="attempted")
        assert result["filtered_entries"] == 1

    def test_filter_by_query_text_matches_partition(self, tmp_path):
        entry = self._make_entry(partition="highmem")
        self._write_jsonl(tmp_path, json.dumps(entry))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(query="highmem")
        assert result["filtered_entries"] == 1

    def test_filter_by_query_text_matches_feature(self, tmp_path):
        entry = self._make_entry(feature="special_feature")
        self._write_jsonl(tmp_path, json.dumps(entry))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(query="special")
        assert result["filtered_entries"] == 1

    def test_filter_by_query_text_matches_nodes(self, tmp_path):
        entry = self._make_entry(nodes=["node42", "node43"])
        self._write_jsonl(tmp_path, json.dumps(entry))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(query="node42")
        assert result["filtered_entries"] == 1

    def test_filter_by_query_text_matches_reason(self, tmp_path):
        entry = self._make_entry(reason="Insufficient GPU memory")
        self._write_jsonl(tmp_path, json.dumps(entry))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(query="insufficient")
        assert result["filtered_entries"] == 1

    def test_filter_by_query_text_case_insensitive(self, tmp_path):
        entry = self._make_entry(job_name="MyJob")
        self._write_jsonl(tmp_path, json.dumps(entry))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(query="myjob")
        assert result["filtered_entries"] == 1

    def test_filter_by_statuses_after_normalization(self, tmp_path):
        entry1 = self._make_entry(job_id="job-001", status="attempted")
        entry2 = self._make_entry(job_id="job-002", status="failed")
        entry3 = self._make_entry(job_id="job-003", status="succeeded")
        lines = "\n".join(json.dumps(e) for e in [entry1, entry2, entry3])
        self._write_jsonl(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(statuses=["attempted"])
        assert result["filtered_entries"] == 1
        assert result["entries"][0]["status"] == "LAUNCH_ATTEMPTED"

    def test_filter_by_statuses_multiple(self, tmp_path):
        entry1 = self._make_entry(job_id="job-001", status="attempted")
        entry2 = self._make_entry(job_id="job-002", status="failed")
        entry3 = self._make_entry(job_id="job-003", status="succeeded")
        lines = "\n".join(json.dumps(e) for e in [entry1, entry2, entry3])
        self._write_jsonl(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(statuses=["attempted", "failed"])
        assert result["filtered_entries"] == 2

    def test_filter_by_statuses_with_alias_success(self, tmp_path):
        entry = self._make_entry(status="LEFT_PENDING_QUEUE")
        self._write_jsonl(tmp_path, json.dumps(entry))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(statuses=["success"])
        assert result["filtered_entries"] == 1

    def test_entries_reversed_newest_first(self, tmp_path):
        entry1 = self._make_entry(job_id="job-001")
        entry2 = self._make_entry(job_id="job-002")
        entry3 = self._make_entry(job_id="job-003")
        lines = "\n".join(json.dumps(e) for e in [entry1, entry2, entry3])
        self._write_jsonl(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        entries = result["entries"]
        assert entries[0]["job_id"] == "job-003"
        assert entries[1]["job_id"] == "job-002"
        assert entries[2]["job_id"] == "job-001"

    def test_pagination_page_size(self, tmp_path):
        entries = [self._make_entry(job_id=f"job-{i:03d}") for i in range(5)]
        self._write_jsonl(tmp_path, "\n".join(json.dumps(e) for e in entries))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(limit=2)
        assert result["page_size"] == 2
        assert result["shown_entries"] == 2

    def test_pagination_page_number(self, tmp_path):
        entries = [self._make_entry(job_id=f"job-{i:03d}") for i in range(5)]
        self._write_jsonl(tmp_path, "\n".join(json.dumps(e) for e in entries))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(page=2, limit=2)
        assert result["page"] == 2
        assert result["shown_entries"] == 2

    def test_pagination_total_pages(self, tmp_path):
        entries = [self._make_entry(job_id=f"job-{i:03d}") for i in range(5)]
        self._write_jsonl(tmp_path, "\n".join(json.dumps(e) for e in entries))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(limit=2)
        assert result["total_pages"] == 3

    def test_pagination_has_prev_and_next(self, tmp_path):
        entries = [self._make_entry(job_id=f"job-{i:03d}") for i in range(5)]
        self._write_jsonl(tmp_path, "\n".join(json.dumps(e) for e in entries))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            r1 = build_job_logs_payload(page=1, limit=2)
            r2 = build_job_logs_payload(page=2, limit=2)
            r3 = build_job_logs_payload(page=3, limit=2)
        assert r1["has_prev_page"] is False
        assert r1["has_next_page"] is True
        assert r2["has_prev_page"] is True
        assert r2["has_next_page"] is True
        assert r3["has_prev_page"] is True
        assert r3["has_next_page"] is False

    def test_status_counts_computed_correctly(self, tmp_path):
        e1 = self._make_entry(job_id="job-001", status="attempted")
        e2 = self._make_entry(job_id="job-002", status="failed")
        e3 = self._make_entry(job_id="job-003", status="attempted")
        self._write_jsonl(tmp_path, "\n".join(json.dumps(e) for e in [e1, e2, e3]))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        counts = result["status_counts"]
        assert counts["LAUNCH_ATTEMPTED"] == 2
        assert counts["LAUNCH_FAILED"] == 1

    def test_status_counts_reflect_filtered(self, tmp_path):
        e1 = self._make_entry(job_id="job-001", status="attempted")
        e2 = self._make_entry(job_id="job-002", status="failed")
        self._write_jsonl(tmp_path, "\n".join(json.dumps(e) for e in [e1, e2]))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(statuses=["attempted"])
        counts = result["status_counts"]
        assert counts.get("LAUNCH_ATTEMPTED") == 1
        assert counts.get("LAUNCH_FAILED") is None

    def test_blank_lines_skipped(self, tmp_path):
        entry = self._make_entry(job_id="job-001")
        self._write_jsonl(tmp_path, "\n\n" + json.dumps(entry) + "\n\n")
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["total_entries"] == 1

    def test_invalid_json_lines_skipped(self, tmp_path):
        entry = self._make_entry(job_id="job-001")
        self._write_jsonl(
            tmp_path,
            "this is not json\n" + json.dumps(entry) + "\n{broken json",
        )
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["total_entries"] == 1

    def test_available_statuses_populated(self, tmp_path):
        e1 = self._make_entry(job_id="job-001", status="attempted")
        e2 = self._make_entry(job_id="job-002", status="failed")
        e3 = self._make_entry(job_id="job-003", status="succeeded")
        self._write_jsonl(tmp_path, "\n".join(json.dumps(e) for e in [e1, e2, e3]))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        # Normalized: LAUNCH_ATTEMPTED, LAUNCH_FAILED, LEFT_PENDING_QUEUE
        assert "LAUNCH_ATTEMPTED" in result["available_statuses"]
        assert "LAUNCH_FAILED" in result["available_statuses"]
        assert "LEFT_PENDING_QUEUE" in result["available_statuses"]

    def test_raw_field_preserved(self, tmp_path):
        raw_line = '{"job_id": "job-001", "status": "attempted"}'
        self._write_jsonl(tmp_path, raw_line)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert result["entries"][0]["raw"] == raw_line

    def test_file_path_in_result(self, tmp_path):
        self._write_jsonl(tmp_path, '{"job_id": "job-001"}')
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        assert "job_launches.jsonl" in result["file"]

    def test_query_preserved_in_result(self, tmp_path):
        self._write_jsonl(tmp_path, '{"job_id": "job-001"}')
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(query="my search")
        assert result["query"] == "my search"

    def test_job_id_preserved_in_result(self, tmp_path):
        self._write_jsonl(tmp_path, '{"job_id": "job-001"}')
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(jobId="job-001")
        assert result["job_id"] == "job-001"

    def test_entry_missing_status_field(self, tmp_path):
        self._write_jsonl(tmp_path, '{"job_id": "job-001"}')
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload()
        # Missing status -> normalize_job_log_status(None) -> "UNKNOWN"
        assert result["entries"][0]["status"] == "UNKNOWN"

    def test_pagination_page_beyond_total_clamped(self, tmp_path):
        entry = self._make_entry(job_id="job-001")
        self._write_jsonl(tmp_path, json.dumps(entry))
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(page=99, limit=10)
        assert result["page"] == 1
        assert result["total_pages"] == 1

    def test_pagination_with_no_entries(self, tmp_path):
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(page=1, limit=10)
        assert result["total_pages"] == 1
        assert result["has_prev_page"] is False
        assert result["has_next_page"] is False

    def test_filter_combined_job_id_and_query(self, tmp_path):
        e1 = self._make_entry(job_id="job-001", job_name="alpha")
        e2 = self._make_entry(job_id="job-002", job_name="beta")
        lines = "\n".join(json.dumps(e) for e in [e1, e2])
        self._write_jsonl(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(jobId="job", query="alpha")
        assert result["filtered_entries"] == 1
        assert result["entries"][0]["job_id"] == "job-001"

    def test_filter_combined_job_id_and_status(self, tmp_path):
        e1 = self._make_entry(job_id="job-001", status="attempted")
        e2 = self._make_entry(job_id="job-002", status="failed")
        lines = "\n".join(json.dumps(e) for e in [e1, e2])
        self._write_jsonl(tmp_path, lines)
        with patch("admin_panel.logs.get_logs_root", return_value=tmp_path):
            result = build_job_logs_payload(jobId="job", statuses=["failed"])
        assert result["filtered_entries"] == 1
        assert result["entries"][0]["status"] == "LAUNCH_FAILED"
