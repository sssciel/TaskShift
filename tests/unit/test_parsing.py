"""
Unit tests for config.parsing module
"""

import pytest

from config.parsing import expand_hostlist, parse_timestamp, split_hostlist_expression


class TestSplitHostlistExpression:
    """Tests for split_hostlist_expression function"""

    def test_single_node(self):
        """Test splitting single node without brackets"""
        result = split_hostlist_expression("cn-001")
        assert result == ["cn-001"]

    def test_single_range(self):
        """Test splitting single range with brackets"""
        result = split_hostlist_expression("cn-[001-003]")
        assert result == ["cn-[001-003]"]

    def test_multiple_nodes(self):
        """Test splitting multiple nodes separated by comma"""
        result = split_hostlist_expression("cn-001,cn-002,cn-003")
        assert result == ["cn-001", "cn-002", "cn-003"]

    def test_multiple_ranges(self):
        """Test splitting multiple ranges separated by comma"""
        result = split_hostlist_expression("cn-[001-003],cn-[010-015]")
        assert result == ["cn-[001-003]", "cn-[010-015]"]

    def test_mixed_nodes_and_ranges(self):
        """Test splitting mixed nodes and ranges"""
        result = split_hostlist_expression("cn-001,cn-[003-005],cn-010")
        assert result == ["cn-001", "cn-[003-005]", "cn-010"]

    def test_complex_expression(self):
        """Test splitting complex hostlist expression"""
        result = split_hostlist_expression("cn-[001-002,004-006],cn-010,cn-[020-025]")
        assert result == ["cn-[001-002,004-006]", "cn-010", "cn-[020-025]"]

    def test_empty_string(self):
        """Test splitting empty string"""
        result = split_hostlist_expression("")
        assert result == []

    def test_whitespace_handling(self):
        """Test that whitespace is properly stripped"""
        result = split_hostlist_expression("cn-001 , cn-002 , cn-003")
        assert result == ["cn-001", "cn-002", "cn-003"]


class TestExpandHostlist:
    """Tests for expand_hostlist function"""

    def test_single_node(self):
        """Test expanding single node"""
        result = expand_hostlist("cn-001")
        assert result == ["cn-001"]

    def test_single_range(self):
        """Test expanding single range"""
        result = expand_hostlist("cn-[001-003]")
        assert result == ["cn-001", "cn-002", "cn-003"]

    def test_range_with_padding(self):
        """Test that numeric padding is preserved"""
        result = expand_hostlist("cn-[001-003]")
        assert result == ["cn-001", "cn-002", "cn-003"]
        assert "cn-1" not in result  # Should have padding

    def test_range_without_padding(self):
        """Test range without padding"""
        result = expand_hostlist("node-[1-5]")
        assert result == ["node-1", "node-2", "node-3", "node-4", "node-5"]

    def test_multiple_ranges(self):
        """Test expanding multiple ranges in same bracket"""
        result = expand_hostlist("cn-[001-002,004-006]")
        assert result == ["cn-001", "cn-002", "cn-004", "cn-005", "cn-006"]

    def test_mixed_ranges_and_singletons(self):
        """Test expanding mixed ranges and single values"""
        result = expand_hostlist("cn-[001-002,005,010-011]")
        assert result == ["cn-001", "cn-002", "cn-005", "cn-010", "cn-011"]

    def test_multiple_hostlists(self):
        """Test expanding multiple hostlists separated by comma"""
        result = expand_hostlist("cn-[001-002],cn-[010-012]")
        assert result == ["cn-001", "cn-002", "cn-010", "cn-011", "cn-012"]

    def test_complex_expression(self):
        """Test expanding complex expression from slurm.conf"""
        result = expand_hostlist("cn-[001-006,041,043-051]")
        expected_nodes = (
            ["cn-001", "cn-002", "cn-003", "cn-004", "cn-005", "cn-006"]
            + ["cn-041"]
            + [
                "cn-043",
                "cn-044",
                "cn-045",
                "cn-046",
                "cn-047",
                "cn-048",
                "cn-049",
                "cn-050",
                "cn-051",
            ]
        )
        assert result == expected_nodes

    def test_empty_string(self):
        """Test expanding empty string"""
        result = expand_hostlist("")
        assert result == []

    def test_none_assigned(self):
        """Test expanding 'None assigned' string"""
        result = expand_hostlist("None assigned")
        assert result == []

    def test_single_node_with_suffix(self):
        """Test expanding node with suffix"""
        result = expand_hostlist("cn-[001-003]-gpu")
        assert result == ["cn-001-gpu", "cn-002-gpu", "cn-003-gpu"]

    def test_whitespace_handling(self):
        """Test that whitespace is properly handled"""
        result = expand_hostlist("  cn-001  ")
        assert result == ["cn-001"]


class TestParseTimestamp:
    """Tests for parse_timestamp function"""

    def test_none_value(self):
        """Test parsing None value"""
        result = parse_timestamp(None)
        assert result is None

    def test_integer_timestamp(self):
        """Test parsing integer timestamp"""
        result = parse_timestamp(1609459200)  # 2021-01-01 00:00:00 UTC
        assert result == 1609459200

    def test_float_timestamp(self):
        """Test parsing float timestamp"""
        result = parse_timestamp(1609459200.123)
        assert result == 1609459200  # Should convert to int

    def test_string_integer(self):
        """Test parsing string integer"""
        result = parse_timestamp("1609459200")
        assert result == 1609459200

    def test_iso_datetime(self):
        """Test parsing ISO datetime string"""
        # "2021-01-01T00:00:00" without timezone uses local time
        result = parse_timestamp("2021-01-01T00:00:00")
        # Should return a valid timestamp
        assert isinstance(result, int)
        assert result > 0
        # Should be close to the expected UTC timestamp (within 24 hours)
        assert abs(result - 1609459200) < 86400  # Within 1 day

    def test_iso_datetime_with_z(self):
        """Test parsing ISO datetime string with Z suffix"""
        result = parse_timestamp("2021-01-01T00:00:00Z")
        assert result == 1609459200

    def test_iso_datetime_with_timezone(self):
        """Test parsing ISO datetime string with timezone"""
        result = parse_timestamp("2021-01-01T00:00:00+00:00")
        assert result == 1609459200

    def test_empty_string(self):
        """Test parsing empty string"""
        result = parse_timestamp("")
        assert result is None

    def test_whitespace_string(self):
        """Test parsing whitespace string"""
        result = parse_timestamp("   ")
        assert result is None

    def test_date_only(self):
        """Test parsing date-only string"""
        result = parse_timestamp("2021-01-01")
        # Should parse as ISO date
        assert isinstance(result, int)
        assert result > 0
