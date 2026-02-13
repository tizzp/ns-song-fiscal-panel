"""Tests for end-to-end Song fiscal panel pipeline."""

from __future__ import annotations


import pandas as pd

from pipeline_end_to_end import PANEL_PATH, run_pipeline


REQUIRED_PANEL_COLUMNS = {
    "period",
    "region",
    "revenue_total",
    "liangshui",
    "shangshui",
    "share_liangshui_in_total",
    "share_shangshui_in_total",
}


def test_outputs_generated() -> None:
    """Pipeline run should generate panel output with expected schema."""
    panel = run_pipeline()
    assert PANEL_PATH.exists()
    assert REQUIRED_PANEL_COLUMNS.issubset(set(panel.columns))


def test_derived_shares_between_zero_and_one() -> None:
    """Share metrics should stay within [0, 1]."""
    run_pipeline()
    panel = pd.read_csv(PANEL_PATH)

    for column in ["share_liangshui_in_total", "share_shangshui_in_total"]:
        assert panel[column].between(0, 1, inclusive="both").all()


def test_period_region_primary_key_uniqueness() -> None:
    """(period, region) must be unique in output panel."""
    run_pipeline()
    panel = pd.read_csv(PANEL_PATH)

    duplicates = panel.duplicated(subset=["period", "region"])
    assert not duplicates.any()
