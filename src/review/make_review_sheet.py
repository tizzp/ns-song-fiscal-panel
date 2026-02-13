"""Generate a human review sheet from Songshi candidates or auto-facts."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_CANDIDATES = BASE_DIR / "data" / "02_intermediate" / "candidates_songshi_juan186.csv"
INPUT_AUTO_FACTS = BASE_DIR / "data" / "02_intermediate" / "auto_facts_songshi_juan186.csv"
OUTPUT_REVIEW_SHEET = BASE_DIR / "data" / "02_intermediate" / "candidates_songshi_juan186_review_sheet.csv"

REVIEW_COLUMNS = [
    "approve",
    "final_period",
    "final_topic",
    "final_region",
    "final_value_std",
    "final_unit_std",
    "interpretation_note",
    "confidence_override",
]


def _select_review_input(prefer_auto_facts: bool = True) -> Path:
    """Choose review source file (auto-facts first by default)."""
    if prefer_auto_facts and INPUT_AUTO_FACTS.exists() and INPUT_AUTO_FACTS.stat().st_size > 0:
        return INPUT_AUTO_FACTS
    return INPUT_CANDIDATES


def make_review_sheet(input_csv: Path, output_csv: Path) -> pd.DataFrame:
    """Create a review sheet with blank human-annotation columns."""
    source_df = pd.read_csv(input_csv)
    review_sheet = source_df.copy()
    review_sheet["approve"] = 0
    for column in REVIEW_COLUMNS[1:]:
        review_sheet[column] = ""

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    review_sheet.to_csv(output_csv, index=False)
    return review_sheet


def main() -> None:
    """CLI wrapper for review sheet generation."""
    input_csv = _select_review_input(prefer_auto_facts=True)
    make_review_sheet(input_csv, OUTPUT_REVIEW_SHEET)
    print(f"review_source_csv: {input_csv}")
    print(f"review_sheet_csv: {OUTPUT_REVIEW_SHEET}")


if __name__ == "__main__":
    main()
