"""CLI entry point for Songshi Juan 186 ingestion and candidate extraction."""

from __future__ import annotations

from pathlib import Path

from extract.songshi_candidates import SOURCE_URL, extract_candidates
from ingest.wikisource_fetch import fetch_wikisource_page

BASE_DIR = Path(__file__).resolve().parents[1]
HTML_PATH = BASE_DIR / "data" / "01_raw" / "wikisource" / "songshi" / "juan186.html"
TXT_PATH = BASE_DIR / "data" / "01_raw" / "wikisource" / "songshi" / "juan186.txt"
CANDIDATES_PATH = BASE_DIR / "data" / "02_intermediate" / "candidates_songshi_juan186.csv"


def run_songshi_juan186_pipeline() -> None:
    """Fetch Songshi Juan 186 text and extract numeric candidates for review."""
    fetch_wikisource_page(
        url=SOURCE_URL,
        out_html=HTML_PATH,
        out_txt=TXT_PATH,
        sleep_seconds=1.0,
        force=False,
    )
    extract_candidates(
        txt_path=TXT_PATH,
        out_csv=CANDIDATES_PATH,
        source_ref=SOURCE_URL,
    )

    print(f"raw_txt: {TXT_PATH}")
    print(f"candidates_csv: {CANDIDATES_PATH}")


def main() -> None:
    """Entrypoint wrapper."""
    run_songshi_juan186_pipeline()


if __name__ == "__main__":
    main()
