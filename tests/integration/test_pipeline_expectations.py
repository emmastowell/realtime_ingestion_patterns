"""Integration tests for DLT pipeline expectations — run on a Databricks cluster."""

from __future__ import annotations

from nh_dataflows.pipelines.expectations import (
    BRONZE_EXPECTATIONS,
    GOLD_EXPECTATIONS,
    SILVER_EXPECTATIONS,
)


class TestExpectationDefinitions:
    """Validate that expectation dictionaries are well-formed."""

    def test_bronze_expectations_non_empty(self) -> None:
        assert len(BRONZE_EXPECTATIONS) > 0
        for name, expr in BRONZE_EXPECTATIONS.items():
            assert isinstance(name, str)
            assert isinstance(expr, str)
            assert len(expr) > 0

    def test_silver_expectations_non_empty(self) -> None:
        assert len(SILVER_EXPECTATIONS) > 0
        for name, expr in SILVER_EXPECTATIONS.items():
            assert isinstance(name, str)
            assert "NULL" in expr or "BETWEEN" in expr or ">=" in expr

    def test_gold_expectations_non_empty(self) -> None:
        assert len(GOLD_EXPECTATIONS) > 0

    def test_silver_speed_range(self) -> None:
        """Speed expectation allows 0-120 mph."""
        expr = SILVER_EXPECTATIONS["valid_speed"]
        assert "120" in expr
        assert "0" in expr

    def test_silver_coordinate_bounds(self) -> None:
        """Latitude/longitude bounds cover Great Britain."""
        lat_expr = SILVER_EXPECTATIONS["valid_latitude"]
        lon_expr = SILVER_EXPECTATIONS["valid_longitude"]
        assert "49" in lat_expr  # Southern tip of UK
        assert "61" in lat_expr  # Northern Scotland
        assert "-8" in lon_expr  # Western Ireland
        assert "2" in lon_expr  # Eastern England
