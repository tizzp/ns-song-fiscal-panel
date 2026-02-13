"""Promote approved Songshi candidates from review sheet into facts table."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

LOGGER = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_REVIEW_SHEET = (
    BASE_DIR / "data" / "02_intermediate" / "candidates_songshi_juan186_review_sheet.csv"
)
OUTPUT_FACTS = BASE_DIR / "data" / "01_raw" / "extracts_songshi_juan186.csv"

REQUIRED_FINAL_COLUMNS = [
    "final_period",
    "final_topic",
    "final_region",
    "final_value_std",
    "final_unit_std",
]

FACT_COLUMNS = [
    "extract_id",
    "period",
    "region",
    "topic",
    "value",
    "unit",
    "confidence",
    "source_ref",
]


def _is_approved(value: object) -> bool:
    """Return True for explicit review approvals."""
    as_text = str(value).strip().lower()
    return as_text in {"1", "true", "yes", "y"}


def promote_reviewed_to_facts(input_csv: Path, output_csv: Path) -> pd.DataFrame:
    """Create facts table from approved review rows with complete final fields."""
    review_df = pd.read_csv(input_csv)
    rows: list[dict[str, object]] = []

    for _, row in review_df.iterrows():
        if not _is_approved(row.get("approve", 0)):
            continue

        missing = [
            col
            for col in REQUIRED_FINAL_COLUMNS
            if pd.isna(row.get(col)) or str(row.get(col)).strip() == ""
        ]
        if missing:
            LOGGER.warning("Skipping approved row due to missing final fields: %s", missing)
            continue

        try:
            value = float(row["final_value_std"])
        except (TypeError, ValueError):
            LOGGER.warning("Skipping approved row due to invalid final_value_std: %s", row["final_value_std"])
            continue

        confidence_override = row.get("confidence_override", "")
        confidence = str(confidence_override).strip() if not pd.isna(confidence_override) else ""
        confidence = confidence if confidence else "C"

        rows.append(
            {
                "extract_id": f"songshi-juan186-{row['candidate_id']}",
                "period": str(row["final_period"]).strip(),
                "region": str(row["final_region"]).strip(),
                "topic": str(row["final_topic"]).strip(),
                "value": value,
                "unit": str(row["final_unit_std"]).strip(),
                "confidence": confidence,
                "source_ref": str(row["source_ref"]).strip(),
            }
        )

    facts = pd.DataFrame(rows, columns=FACT_COLUMNS)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    facts.to_csv(output_csv, index=False)
    return facts


def main() -> None:
    """CLI wrapper for review promotion step."""
    facts = promote_reviewed_to_facts(INPUT_REVIEW_SHEET, OUTPUT_FACTS)
    print(f"facts_csv: {OUTPUT_FACTS}")
    print(f"promoted_rows: {len(facts)}")


if __name__ == "__main__":
    main()
