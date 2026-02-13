"""Tests for Songshi Juan 186 ingestion and candidate extraction."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from extract.songshi_candidates import extract_candidates
from ingest.wikisource_fetch import fetch_wikisource_page

REQUIRED_COLUMNS = {
    "candidate_id",
    "source_work",
    "source_url",
    "source_ref",
    "juan",
    "snippet",
    "value_raw",
    "unit_raw",
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
    assert (~loaded["candidate_topic"].astype(str).str.fullmatch("fact", case=False)).all()
