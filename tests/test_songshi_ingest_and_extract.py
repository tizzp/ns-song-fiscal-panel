"""Tests for Songshi Juan 186 ingestion, extraction, and promotion workflow."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from extract.songshi_candidates import extract_candidates, parse_chinese_numeral
from ingest.wikisource_fetch import fetch_wikisource_page
from review.promote_reviewed_to_facts import FACT_COLUMNS, promote_reviewed_to_facts

REQUIRED_COLUMNS = {
    "candidate_id",
    "source_work",
    "source_url",
    "source_ref",
    "juan",
    "char_start",
    "char_end",
    "snippet",
    "snippet_hash",
    "value_raw",
    "value_num",
    "unit_raw",
    "unit_std",
    "keywords",
    "candidate_topic",
    "candidate_period",
    "region",
    "confidence",
    "notes",
}


class _DummyResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


def test_fetch_creates_nonempty_txt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Fetcher should write non-empty plain text output."""
    fixture_html = Path("tests/fixtures/juan186_sample.html").read_text(encoding="utf-8")

    def _fake_get(*args: object, **kwargs: object) -> _DummyResponse:
        return _DummyResponse(fixture_html)

    monkeypatch.setattr("ingest.wikisource_fetch.requests.get", _fake_get)

    out_html = tmp_path / "juan186.html"
    out_txt = tmp_path / "juan186.txt"

    fetch_wikisource_page(
        url="https://zh.wikisource.org/zh-hans/宋史/卷186",
        out_html=out_html,
        out_txt=out_txt,
        sleep_seconds=0,
        force=True,
    )

    assert out_txt.exists()
    assert out_txt.read_text(encoding="utf-8").strip() != ""


def test_extract_candidates_schema_nonempty_and_confidence_c(tmp_path: Path) -> None:
    """Extractor should produce non-empty candidate CSV with required schema."""
    txt_path = Path("tests/fixtures/juan186_sample.txt")
    out_csv = tmp_path / "candidates.csv"

    df = extract_candidates(
        txt_path=txt_path,
        out_csv=out_csv,
        source_ref="https://zh.wikisource.org/zh-hans/宋史/卷186",
    )

    assert not df.empty
    assert out_csv.exists()
    assert REQUIRED_COLUMNS.issubset(df.columns)

    loaded = pd.read_csv(out_csv)
    assert not loaded.empty
    assert REQUIRED_COLUMNS.issubset(loaded.columns)
    assert (loaded["confidence"] == "C").all()


@pytest.mark.parametrize(
    ("value_raw", "expected"),
    [
        ("二百", 200.0),
        ("一千二百三", 1203.0),
        ("廿五", 25.0),
        ("3", 3.0),
    ],
)
def test_parse_chinese_numeral(value_raw: str, expected: float) -> None:
    """Chinese and Arabic numeral parser should parse common forms."""
    assert parse_chinese_numeral(value_raw) == expected


def test_promote_only_approved_rows(tmp_path: Path) -> None:
    """Promotion should include only approved rows with complete final fields."""
    review_path = tmp_path / "review_sheet.csv"
    out_facts = tmp_path / "extracts_songshi_juan186.csv"

    review_df = pd.DataFrame(
        [
            {
                "candidate_id": "c-approved",
                "source_ref": "https://zh.wikisource.org/zh-hans/宋史/卷186#start=10&end=12&cid=c-approved",
                "approve": 1,
                "final_period": "YUANFENG",
                "final_topic": "shangshui",
                "final_region": "NATIONAL",
                "final_value_std": "300",
                "final_unit_std": "guan",
                "confidence_override": "B",
            },
            {
                "candidate_id": "c-not-approved",
                "source_ref": "https://zh.wikisource.org/zh-hans/宋史/卷186#start=20&end=24&cid=c-not-approved",
                "approve": 0,
                "final_period": "XINNING",
                "final_topic": "liangshui",
                "final_region": "NATIONAL",
                "final_value_std": "200",
                "final_unit_std": "guan",
                "confidence_override": "",
            },
        ]
    )
    review_df.to_csv(review_path, index=False)

    facts = promote_reviewed_to_facts(review_path, out_facts)

    assert out_facts.exists()
    assert list(facts.columns) == FACT_COLUMNS
    assert len(facts) == 1
    assert facts.iloc[0]["extract_id"] == "songshi-juan186-c-approved"
    assert facts.iloc[0]["confidence"] == "B"
