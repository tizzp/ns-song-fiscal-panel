"""Tests for end-to-end Song fiscal panel pipeline."""

from __future__ import annotations

import math

import pandas as pd

from pipeline_end_to_end import INTERMEDIATE_PATH, PANEL_PATH, compute_panel, run_pipeline

REQUIRED_PANEL_COLUMNS = {
    "period",
    "region",
    "revenue_total",
    "liangshui",
    "shangshui",
    "share_liangshui_in_total",
    "share_shangshui_in_total",
}

SHARE_COLUMNS = [
    "share_liangshui_in_total",
    "share_shangshui_in_total",
]


def test_run_pipeline_executes_and_outputs_exist() -> None:
    """Pipeline run should succeed and create both output files."""
    panel = run_pipeline()

    assert not panel.empty
    assert REQUIRED_PANEL_COLUMNS.issubset(set(panel.columns))
    assert INTERMEDIATE_PATH.exists()
    assert PANEL_PATH.exists()


def test_period_region_primary_key_uniqueness() -> None:
    """(period, region) must be unique in output panel."""
    run_pipeline()
    panel = pd.read_csv(PANEL_PATH)

    duplicates = panel.duplicated(subset=["period", "region"])
    assert not duplicates.any()


def test_share_columns_respect_bounds_when_revenue_positive() -> None:
    """Shares must be in [0, 1] for rows with strictly positive revenue_total."""
    run_pipeline()
    panel = pd.read_csv(PANEL_PATH)

    positive_revenue = panel["revenue_total"] > 0
    for column in SHARE_COLUMNS:
        assert panel.loc[positive_revenue, column].between(0, 1, inclusive="both").all()


def test_share_columns_are_nan_when_revenue_not_positive_and_never_inf() -> None:
    """Shares should be NaN when revenue_total <= 0 and must never be infinite."""
    extracts = pd.DataFrame(
        [
            {
                "extract_id": "t-001",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "revenue_total",
                "value": 0.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "placeholder://seed/test/revenue",
            },
            {
                "extract_id": "t-002",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "liangshui",
                "value": 5.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "placeholder://seed/test/liangshui",
            },
            {
                "extract_id": "t-003",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "shangshui",
                "value": 3.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "placeholder://seed/test/shangshui",
            },
        ]
    )

    panel = compute_panel(extracts)
    row = panel.iloc[0]

    for column in SHARE_COLUMNS:
        assert pd.isna(row[column])

    for column in SHARE_COLUMNS:
        non_null = panel[column].dropna()
        assert not non_null.map(math.isinf).any()
