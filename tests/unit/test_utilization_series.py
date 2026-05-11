"""
Unit tests for storage.series module - utilization calculation and overflow handling
"""

import pytest

from storage.series import (
    _calculate_utilization,
    _cleanup_overflow_points,
    _count_overflow_points,
)


class TestCalculateUtilization:
    """Tests for _calculate_utilization function"""

    def test_basic_calculation(self):
        """Test basic utilization calculation"""
        # 50 used, 100 total = 50%
        result = _calculate_utilization(50.0, 100)
        assert result == 50.0

    def test_zero_usage(self):
        """Test with zero used capacity"""
        result = _calculate_utilization(0.0, 100)
        assert result == 0.0

    def test_full_usage(self):
        """Test with 100% usage"""
        result = _calculate_utilization(100.0, 100)
        assert result == 100.0

    def test_overflow(self):
        """Test with usage > total capacity (overflow)"""
        # 150 used, 100 total = 150%
        result = _calculate_utilization(150.0, 100)
        assert result == 150.0

    def test_zero_total_capacity(self):
        """Test with zero total capacity"""
        result = _calculate_utilization(50.0, 0)
        assert result == 0.0

    def test_negative_total_capacity(self):
        """Test with negative total capacity"""
        result = _calculate_utilization(50.0, -10)
        assert result == 0.0

    def test_rounding(self):
        """Test rounding to 2 decimal places"""
        # 1/3 = 33.333... -> 33.33
        result = _calculate_utilization(1.0, 3)
        assert result == 33.33

    def test_large_numbers(self):
        """Test with large numbers"""
        result = _calculate_utilization(50000.0, 100000)
        assert result == 50.0

    def test_partial_usage(self):
        """Test with partial usage"""
        # 25 used, 80 total = 31.25%
        result = _calculate_utilization(25.0, 80)
        assert result == 31.25


class TestCleanupOverflowPoints:
    """Tests for _cleanup_overflow_points function"""

    def test_empty_series(self):
        """Test with empty series"""
        result = _cleanup_overflow_points([])
        assert result == []

    def test_no_overflow(self):
        """Test series with no overflow points"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 50.0, "gpu": 30.0},
            {"time": "2024-01-01T00:15:00", "cpu": 60.0, "gpu": 40.0},
            {"time": "2024-01-01T00:30:00", "cpu": 70.0, "gpu": 50.0},
        ]
        result = _cleanup_overflow_points(series)
        assert result == series

    def test_single_consecutive_overflow_at_start(self):
        """Test single overflow point at the start"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 110.0, "gpu": 30.0},
            {"time": "2024-01-01T00:15:00", "cpu": 50.0, "gpu": 40.0},
            {"time": "2024-01-01T00:30:00", "cpu": 60.0, "gpu": 50.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should remove first point
        assert len(result) == 2
        assert result[0]["time"] == "2024-01-01T00:15:00"
        assert result[0]["cpu"] == 50.0

    def test_multiple_consecutive_overflows_at_start(self):
        """Test multiple consecutive overflow points at the start"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 110.0, "gpu": 120.0},
            {"time": "2024-01-01T00:15:00", "cpu": 105.0, "gpu": 30.0},
            {"time": "2024-01-01T00:30:00", "cpu": 108.0, "gpu": 40.0},
            {"time": "2024-01-01T00:45:00", "cpu": 50.0, "gpu": 50.0},
            {"time": "2024-01-01T01:00:00", "cpu": 60.0, "gpu": 60.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should remove first 3 points
        assert len(result) == 2
        assert result[0]["time"] == "2024-01-01T00:45:00"
        assert result[0]["cpu"] == 50.0

    def test_overflow_after_normal_points(self):
        """Test overflow points after normal points (should be clipped)"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 50.0, "gpu": 30.0},
            {"time": "2024-01-01T00:15:00", "cpu": 60.0, "gpu": 40.0},
            {"time": "2024-01-01T00:30:00", "cpu": 110.0, "gpu": 50.0},  # Overflow
            {"time": "2024-01-01T00:45:00", "cpu": 70.0, "gpu": 60.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should not trim, only clip
        assert len(result) == 4
        assert result[2]["cpu"] == 100.0  # Clipped from 110
        assert result[2]["gpu"] == 50.0  # Not changed

    def test_mixed_strategy(self):
        """Test combination of trimming and clipping"""
        series = [
            {
                "time": "2024-01-01T00:00:00",
                "cpu": 110.0,
                "gpu": 30.0,
            },  # Consecutive overflow
            {
                "time": "2024-01-01T00:15:00",
                "cpu": 105.0,
                "gpu": 40.0,
            },  # Consecutive overflow
            {"time": "2024-01-01T00:30:00", "cpu": 50.0, "gpu": 50.0},  # Normal
            {
                "time": "2024-01-01T00:45:00",
                "cpu": 115.0,
                "gpu": 60.0,
            },  # Overflow after normal
            {"time": "2024-01-01T01:00:00", "cpu": 70.0, "gpu": 70.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should trim first 2 points
        assert len(result) == 3
        assert result[0]["time"] == "2024-01-01T00:30:00"

        # Should clip overflow after normal
        assert result[1]["cpu"] == 100.0  # Clipped from 115
        assert result[1]["gpu"] == 60.0

    def test_gpu_overflow_only(self):
        """Test with only GPU overflow"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 30.0, "gpu": 110.0},  # GPU overflow
            {"time": "2024-01-01T00:15:00", "cpu": 50.0, "gpu": 40.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should trim first point
        assert len(result) == 1
        assert result[0]["time"] == "2024-01-01T00:15:00"

    def test_both_cpu_and_gpu_overflow(self):
        """Test with both CPU and GPU overflow"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 110.0, "gpu": 120.0},
            {"time": "2024-01-01T00:15:00", "cpu": 50.0, "gpu": 60.0},
        ]
        result = _cleanup_overflow_points(series)

        assert len(result) == 1
        assert result[0]["time"] == "2024-01-01T00:15:00"

    def test_all_overflow_points(self):
        """Test series with all overflow points"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 110.0, "gpu": 120.0},
            {"time": "2024-01-01T00:15:00", "cpu": 105.0, "gpu": 110.0},
            {"time": "2024-01-01T00:30:00", "cpu": 108.0, "gpu": 115.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should trim all points
        assert len(result) == 0

    def test_exactly_100_percent(self):
        """Test that exactly 100% is not considered overflow"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 100.0, "gpu": 100.0},
            {"time": "2024-01-01T00:15:00", "cpu": 50.0, "gpu": 60.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should not trim (100 is not overflow)
        assert len(result) == 2

    def test_slightly_over_100(self):
        """Test values slightly over 100"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 100.01, "gpu": 30.0},
            {"time": "2024-01-01T00:15:00", "cpu": 50.0, "gpu": 40.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should trim first point
        assert len(result) == 1

    def test_rounding_edge_case(self):
        """Test rounding edge case"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 99.99, "gpu": 100.01},
            {"time": "2024-01-01T00:15:00", "cpu": 50.0, "gpu": 60.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should trim first point (gpu = 100.01 > 100)
        assert len(result) == 1

    def test_multiple_overflows_after_normal(self):
        """Test multiple overflow points after normal points"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 50.0, "gpu": 30.0},
            {"time": "2024-01-01T00:15:00", "cpu": 110.0, "gpu": 40.0},  # Overflow
            {"time": "2024-01-01T00:30:00", "cpu": 120.0, "gpu": 50.0},  # Overflow
            {"time": "2024-01-01T00:45:00", "cpu": 70.0, "gpu": 60.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should not trim, only clip
        assert len(result) == 4
        assert result[1]["cpu"] == 100.0  # Clipped
        assert result[2]["cpu"] == 100.0  # Clipped


class TestCountOverflowPoints:
    """Tests for _count_overflow_points function"""

    def test_no_overflow(self):
        """Test with no overflow points"""
        series = {
            "type_a": [
                {"time": "2024-01-01T00:00:00", "cpu": 50.0, "gpu": 30.0},
                {"time": "2024-01-01T00:15:00", "cpu": 60.0, "gpu": 40.0},
            ]
        }
        result = _count_overflow_points(series)
        assert result == {}

    def test_single_feature_overflow(self):
        """Test with overflow in single feature"""
        series = {
            "type_a": [
                {"time": "2024-01-01T00:00:00", "cpu": 110.0, "gpu": 30.0},
                {"time": "2024-01-01T00:15:00", "cpu": 60.0, "gpu": 40.0},
            ]
        }
        result = _count_overflow_points(series)
        assert result == {"type_a": 1}

    def test_multiple_features_overflow(self):
        """Test with overflow in multiple features"""
        series = {
            "type_a": [
                {"time": "2024-01-01T00:00:00", "cpu": 110.0, "gpu": 30.0},
            ],
            "type_b": [
                {"time": "2024-01-01T00:00:00", "cpu": 50.0, "gpu": 120.0},
                {"time": "2024-01-01T00:15:00", "cpu": 115.0, "gpu": 40.0},
            ],
        }
        result = _count_overflow_points(series)
        assert result == {"type_a": 1, "type_b": 2}

    def test_both_cpu_and_gpu_overflow(self):
        """Test counting when both CPU and GPU overflow in same point"""
        series = {
            "type_a": [
                {"time": "2024-01-01T00:00:00", "cpu": 110.0, "gpu": 120.0},
            ]
        }
        result = _count_overflow_points(series)
        # Should count as 1 point (not 2)
        assert result == {"type_a": 1}

    def test_empty_series(self):
        """Test with empty series"""
        series = {"type_a": []}
        result = _count_overflow_points(series)
        assert result == {}


class TestCleanupIntegration:
    """Integration tests for overflow cleanup with realistic scenarios"""

    def test_realistic_scenario_1(self):
        """Test realistic scenario: old jobs with missing config"""
        # Scenario: First 3 points are corrupted (no config snapshot)
        # Later points are normal with one minor overflow
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 150.0, "gpu": 140.0},
            {"time": "2024-01-01T00:15:00", "cpu": 145.0, "gpu": 135.0},
            {"time": "2024-01-01T00:30:00", "cpu": 160.0, "gpu": 150.0},
            {"time": "2024-01-01T00:45:00", "cpu": 45.0, "gpu": 30.0},  # Normal
            {"time": "2024-01-01T01:00:00", "cpu": 50.0, "gpu": 35.0},  # Normal
            {
                "time": "2024-01-01T01:15:00",
                "cpu": 102.5,
                "gpu": 40.0,
            },  # Minor overflow
            {"time": "2024-01-01T01:30:00", "cpu": 55.0, "gpu": 45.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should trim first 3 points
        assert len(result) == 4
        assert result[0]["time"] == "2024-01-01T00:45:00"

        # Should clip the minor overflow
        assert result[2]["cpu"] == 100.0  # Clipped from 102.5

    def test_realistic_scenario_2(self):
        """Test realistic scenario: all data valid"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 45.0, "gpu": 30.0},
            {"time": "2024-01-01T00:15:00", "cpu": 50.0, "gpu": 35.0},
            {"time": "2024-01-01T00:30:00", "cpu": 55.0, "gpu": 40.0},
            {"time": "2024-01-01T00:45:00", "cpu": 60.0, "gpu": 45.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should not change
        assert len(result) == 4
        assert result == series

    def test_realistic_scenario_3(self):
        """Test realistic scenario: alternating overflows"""
        series = [
            {"time": "2024-01-01T00:00:00", "cpu": 110.0, "gpu": 30.0},
            {"time": "2024-01-01T00:15:00", "cpu": 50.0, "gpu": 35.0},
            {"time": "2024-01-01T00:30:00", "cpu": 120.0, "gpu": 40.0},
            {"time": "2024-01-01T00:45:00", "cpu": 60.0, "gpu": 45.0},
        ]
        result = _cleanup_overflow_points(series)

        # Should trim first point only (consecutive from start)
        assert len(result) == 3
        assert result[0]["time"] == "2024-01-01T00:15:00"

        # Should clip the overflow after normal
        assert result[1]["cpu"] == 100.0  # Clipped from 120
