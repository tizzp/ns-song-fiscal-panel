"""Tests for auto and verified panel generation."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from pipeline_end_to_end import run_panel_mode


def test_auto_panel_uniqueness_and_share_bounds(tmp_path: Path, monkeypatch) -> None:
    """Auto panel should have unique keys and valid share behavior."""
    auto_facts_path = tmp_path / "auto_facts.csv"
    auto_panel_path = tmp_path / "auto_panel.csv"

    auto_facts = pd.DataFrame(
        [
            {
                "extract_id": "a-1",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "revenue_total",
                "value": 100.0,
                "unit": "guan",
                "confidence": "C",
                "review_status": "unreviewed",
                "source_ref": "u#1",
                "rule_trace": "period:熙宁|topic:課入|region:京師",
            },
            {
                "extract_id": "a-2",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "liangshui",
                "value": 20.0,
                "unit": "guan",
                "confidence": "C",
                "review_status": "unreviewed",
                "source_ref": "u#2",
                "rule_trace": "period:熙宁|topic:兩稅",
            },
            {
                "extract_id": "a-3",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "shangshui",
                "value": 10.0,
                "unit": "guan",
                "confidence": "C",
                "review_status": "unreviewed",
                "source_ref": "u#3",
                "rule_trace": "period:熙宁|topic:商税",
            },
        ]
    )
    auto_facts.to_csv(auto_facts_path, index=False)

    monkeypatch.setattr("pipeline_end_to_end.AUTO_FACTS_PATH", auto_facts_path)
    monkeypatch.setattr("pipeline_end_to_end.AUTO_PANEL_PATH", auto_panel_path)

    panel = run_panel_mode("auto")

    assert not panel.empty
    assert not panel.duplicated(subset=["period", "region"]).any()

    positive_revenue = panel["revenue_total"] > 0
    for column in ["share_liangshui_in_total", "share_shangshui_in_total"]:
        assert panel.loc[positive_revenue, column].between(0, 1, inclusive="both").all()
        assert not panel[column].dropna().map(math.isinf).any()


def test_verified_panel_empty_when_no_approved_facts(tmp_path: Path, monkeypatch) -> None:
    """Verified mode should not crash and may emit an empty panel."""
    verified_facts = tmp_path / "missing_verified.csv"
    verified_panel = tmp_path / "verified_panel.csv"

    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_FACTS_PATH", verified_facts)
    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_PANEL_PATH", verified_panel)

    panel = run_panel_mode("verified")

    assert panel.empty
    assert verified_panel.exists()
