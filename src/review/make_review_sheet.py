"""Generate a human review sheet from extracted Songshi candidates."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_CANDIDATES = BASE_DIR / "data" / "02_intermediate" / "candidates_songshi_juan186.csv"
OUTPUT_REVIEW_SHEET = (
    BASE_DIR / "data" / "02_intermediate" / "candidates_songshi_juan186_review_sheet.csv"
)

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


def make_review_sheet(input_csv: Path, output_csv: Path) -> pd.DataFrame:
    """Create a review sheet with blank human-annotation columns."""
    candidates = pd.read_csv(input_csv)

    review_sheet = candidates.copy()
    review_sheet["approve"] = 0
    for column in REVIEW_COLUMNS[1:]:
        review_sheet[column] = ""

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    review_sheet.to_csv(output_csv, index=False)
    return review_sheet


def main() -> None:
    """CLI wrapper for review sheet generation."""
    make_review_sheet(INPUT_CANDIDATES, OUTPUT_REVIEW_SHEET)
    print(f"review_sheet_csv: {OUTPUT_REVIEW_SHEET}")


if __name__ == "__main__":
    main()
