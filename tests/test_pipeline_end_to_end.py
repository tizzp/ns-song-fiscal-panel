"""Tests for verified and auto panel generation modes."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from extract.songshi_candidates import SOURCE_URL, extract_candidates
from organize.auto_facts_songshi_juan186 import auto_organize_facts
from pipeline_end_to_end import run_auto_panel, run_panel_mode


def test_verified_mode_allows_empty_panel_when_no_input_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Verified mode should not crash when both verified and seed facts are absent."""
    verified_facts = tmp_path / "extracts_songshi_juan186.csv"
    seed_facts = tmp_path / "extracts_seed.csv"
    verified_panel = tmp_path / "panel_verified.csv"
    legacy_panel = tmp_path / "panel_legacy.csv"

    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_FACTS_PATH", verified_facts)
    monkeypatch.setattr("pipeline_end_to_end.SEED_FACTS_PATH", seed_facts)
    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_PANEL_PATH", verified_panel)
    monkeypatch.setattr("pipeline_end_to_end.LEGACY_PANEL_PATH", legacy_panel)

    panel = run_panel_mode("verified")

    assert panel.empty
    assert verified_panel.exists()
    assert not legacy_panel.exists()


def test_verified_mode_falls_back_to_seed_and_writes_legacy_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Verified mode should use seed facts when verified facts are missing."""
    verified_facts = tmp_path / "extracts_songshi_juan186.csv"
    seed_facts = tmp_path / "extracts_seed.csv"
    verified_panel = tmp_path / "panel_verified.csv"
    legacy_panel = tmp_path / "panel_legacy.csv"

    pd.DataFrame(
        [
            {
                "extract_id": "seed-1",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "revenue_total",
                "value": 10,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "seed#1",
            }
        ]
    ).to_csv(seed_facts, index=False)

    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_FACTS_PATH", verified_facts)
    monkeypatch.setattr("pipeline_end_to_end.SEED_FACTS_PATH", seed_facts)
    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_PANEL_PATH", verified_panel)
    monkeypatch.setattr("pipeline_end_to_end.LEGACY_PANEL_PATH", legacy_panel)

    panel = run_panel_mode("verified")

    assert not panel.empty
    assert verified_panel.exists()
    assert legacy_panel.exists()


def test_verified_mode_prefers_verified_facts_when_present(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Verified mode should use verified facts when available and ignore seed fallback."""
    verified_facts = tmp_path / "extracts_songshi_juan186.csv"
    seed_facts = tmp_path / "extracts_seed.csv"
    verified_panel = tmp_path / "panel_verified.csv"
    legacy_panel = tmp_path / "panel_legacy.csv"

    pd.DataFrame(
        [
            {
                "extract_id": "verified-1",
                "period": "YUANFENG",
                "region": "NATIONAL",
                "topic": "revenue_total",
                "value": 100,
                "unit": "guan",
                "confidence": "B",
                "source_ref": "verified#1",
            }
        ]
    ).to_csv(verified_facts, index=False)

    pd.DataFrame(
        [
            {
                "extract_id": "seed-1",
                "period": "XINNING",
                "region": "NATIONAL",
                "topic": "revenue_total",
                "value": 10,
                "unit": "guan",
                "confidence": "C",
                "source_ref": "seed#1",
            }
        ]
    ).to_csv(seed_facts, index=False)

    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_FACTS_PATH", verified_facts)
    monkeypatch.setattr("pipeline_end_to_end.SEED_FACTS_PATH", seed_facts)
    monkeypatch.setattr("pipeline_end_to_end.VERIFIED_PANEL_PATH", verified_panel)
    monkeypatch.setattr("pipeline_end_to_end.LEGACY_PANEL_PATH", legacy_panel)

    panel = run_panel_mode("verified")

    assert set(panel["period"]) == {"YUANFENG"}
    assert verified_panel.exists()
    assert legacy_panel.exists()


def test_auto_mode_from_fixture_candidates_generates_valid_panel(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Auto mode should produce a stable panel from fixture-based candidates."""
    txt_path = Path("tests/fixtures/juan186_sample.txt")
    rules_path = Path("metadata/rules_songshi_juan186.yml")

    candidates_path = tmp_path / "candidates_songshi_juan186.csv"
    auto_facts_path = tmp_path / "auto_facts_songshi_juan186.csv"
    auto_panel_path = tmp_path / "panel_revenue_period_region_auto.csv"

    extract_candidates(txt_path=txt_path, out_csv=candidates_path, source_ref=SOURCE_URL)
    auto_facts = auto_organize_facts(candidates_path, auto_facts_path, rules_path)

    assert not auto_facts.empty

    monkeypatch.setattr("pipeline_end_to_end.AUTO_FACTS_PATH", auto_facts_path)
    monkeypatch.setattr("pipeline_end_to_end.AUTO_PANEL_PATH", auto_panel_path)

    panel = run_auto_panel()

    assert auto_panel_path.exists()
    expected_columns = {
        "period",
        "region",
        "revenue_total",
        "liangshui",
        "shangshui",
        "supporting_extract_ids",
        "share_liangshui_in_total",
        "share_shangshui_in_total",
    }
    assert expected_columns.issubset(panel.columns)
    assert not panel.duplicated(subset=["period", "region"]).any()
    assert panel["region"].isin({"NATIONAL", "NORTH", "SOUTH"}).all()

    for col in ["share_liangshui_in_total", "share_shangshui_in_total"]:
        assert not panel[col].dropna().map(math.isinf).any()

    non_positive = panel["revenue_total"] <= 0
    if non_positive.any():
        assert panel.loc[non_positive, "share_liangshui_in_total"].isna().all()
        assert panel.loc[non_positive, "share_shangshui_in_total"].isna().all()

    positive = panel["revenue_total"] > 0
    if positive.any():
        assert panel.loc[positive, "share_liangshui_in_total"].between(0, 1, inclusive="both").all()
        assert panel.loc[positive, "share_shangshui_in_total"].between(0, 1, inclusive="both").all()
