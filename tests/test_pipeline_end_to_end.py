"""Tests for auto and verified panel generation behavior."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from pipeline_end_to_end import run_panel_mode, run_pipeline


REQUIRED_FACT_COLUMNS = [
    "extract_id",
    "period",
    "region",
    "topic",
    "value",
    "unit",
    "confidence",
    "source_ref",
]


def test_verified_mode_uses_seed_fallback_and_writes_legacy_panel(tmp_path: Path, monkeypatch) -> None:
    """run_pipeline should use seed facts if verified facts are absent and write legacy panel."""
    verified_facts = tmp_path / "extracts_songshi_juan186.csv"
    seed_facts = tmp_path / "extracts_seed.csv"
    verified_panel = tmp_path / "panel_verified.csv"
    legacy_panel = tmp_path / "panel_legacy.csv"

    seed_rows = pd.DataFrame(
        [
            {
                "extract_id": "seed-rt",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "revenue_total",
                "value": 100.0,
                "unit": "guan",
                "confidence": "B",
                "source_ref": "seed#rt",
            }
        ],
        columns=REQUIRED_FACT_COLUMNS,
    )
    seed_rows.to_csv(seed_facts, index=False)

    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_FACTS_PATH", verified_facts)
    monkeypatch.setattr("pipeline_end_to_end.SEED_FACTS_PATH", seed_facts)
    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_PANEL_PATH", verified_panel)
    monkeypatch.setattr("pipeline_end_to_end.LEGACY_PANEL_PATH", legacy_panel)

    panel = run_pipeline()

    assert not panel.empty
    assert set(panel["period"]) == {"XINNING"}
    assert verified_panel.exists()
    assert legacy_panel.exists()


def test_auto_mode_keeps_national_north_south_rows(tmp_path: Path, monkeypatch) -> None:
    """Auto panel should keep NATIONAL, NORTH, and SOUTH rows and drop unknown."""
    auto_facts_path = tmp_path / "auto_facts.csv"
    auto_panel_path = tmp_path / "auto_panel.csv"

    auto_rows = pd.DataFrame(
        [
            {
                "extract_id": "a-national",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "revenue_total",
                "value": 100.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "ref#n",
            },
            {
                "extract_id": "a-north",
                "period": "XINNING",
                "region": "NORTH",
                "topic": "revenue_total",
                "value": 60.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "ref#north",
            },
            {
                "extract_id": "a-south",
                "period": "XINNING",
                "region": "SOUTH",
                "topic": "revenue_total",
                "value": 40.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "ref#south",
            },
            {
                "extract_id": "a-unknown",
                "period": "XINNING",
                "region": "unknown",
                "topic": "revenue_total",
                "value": 10.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "ref#u",
            },
        ],
        columns=REQUIRED_FACT_COLUMNS,
    )
    auto_rows.to_csv(auto_facts_path, index=False)

    monkeypatch.setattr("pipeline_end_to_end.AUTO_FACTS_PATH", auto_facts_path)
    monkeypatch.setattr("pipeline_end_to_end.AUTO_PANEL_PATH", auto_panel_path)

    panel = run_panel_mode("auto")

    assert auto_panel_path.exists()
    assert set(panel["region"]) == {"NATIONAL", "NORTH", "SOUTH"}


def test_panel_shares_are_finite_and_bounded_when_revenue_positive(tmp_path: Path, monkeypatch) -> None:
    """Share columns should avoid inf and remain within [0,1] for positive revenue rows."""
    auto_facts_path = tmp_path / "auto_facts.csv"
    auto_panel_path = tmp_path / "auto_panel.csv"

    rows = pd.DataFrame(
        [
            {
                "extract_id": "a-total",
                "period": "YUANFENG",
                "region": "NATIONAL",
                "topic": "revenue_total",
                "value": 200.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "ref#1",
            },
            {
                "extract_id": "a-liang",
                "period": "YUANFENG",
                "region": "NATIONAL",
                "topic": "liangshui",
                "value": 50.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "ref#2",
            },
            {
                "extract_id": "a-shang",
                "period": "YUANFENG",
                "region": "NATIONAL",
                "topic": "shangshui",
                "value": 25.0,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "ref#3",
            },
        ],
        columns=REQUIRED_FACT_COLUMNS,
    )
    rows.to_csv(auto_facts_path, index=False)

    monkeypatch.setattr("pipeline_end_to_end.AUTO_FACTS_PATH", auto_facts_path)
    monkeypatch.setattr("pipeline_end_to_end.AUTO_PANEL_PATH", auto_panel_path)

    panel = run_panel_mode("auto")

    for share_col in ["share_liangshui_in_total", "share_shangshui_in_total"]:
        assert not panel[share_col].dropna().map(math.isinf).any()

    positive = panel["revenue_total"] > 0
    assert panel.loc[positive, "share_liangshui_in_total"].between(0, 1, inclusive="both").all()
    assert panel.loc[positive, "share_shangshui_in_total"].between(0, 1, inclusive="both").all()
