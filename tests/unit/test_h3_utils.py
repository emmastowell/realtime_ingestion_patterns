"""Tests for H3 utility functions (expression generation only — no Spark needed)."""

from __future__ import annotations

from nh_dataflows.config import H3_RESOLUTION_COARSE, H3_RESOLUTION_FINE
from nh_dataflows.geospatial.h3_utils import (
    h3_index_coarse,
    h3_index_fine,
)


def test_h3_index_fine_uses_resolution_10() -> None:
    col = h3_index_fine()
    expr_str = str(col)
    assert str(H3_RESOLUTION_FINE) in expr_str


def test_h3_index_coarse_uses_resolution_7() -> None:
    col = h3_index_coarse()
    expr_str = str(col)
    assert str(H3_RESOLUTION_COARSE) in expr_str


def test_custom_column_names() -> None:
    col = h3_index_fine(lat_col="lat", lon_col="lon")
    expr_str = str(col)
    assert "lon" in expr_str
    assert "lat" in expr_str
