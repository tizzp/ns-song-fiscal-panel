"""Run a minimal traceable end-to-end fiscal panel pipeline."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pandas as pd
from pydantic import BaseModel, ValidationError

REQUIRED_COLUMNS: Final[list[str]] = [
    "extract_id",
    "period",
    "region",
    "topic",
    "value",
    "unit",
    "confidence",
    "source_ref",
]

BASE_DIR: Final[Path] = Path(__file__).resolve().parents[1]
RAW_PATH: Final[Path] = BASE_DIR / "data" / "01_raw" / "extracts_seed.csv"
INTERMEDIATE_PATH: Final[Path] = BASE_DIR / "data" / "02_intermediate" / "fact_numeric_extracts.parquet"
PANEL_PATH: Final[Path] = BASE_DIR / "data" / "03_primary" / "panel_revenue_period_region.csv"


class ExtractRecord(BaseModel):
    """Schema for each numeric extract record."""

    extract_id: str
    period: str
    region: str
    topic: str
    value: float
    unit: str
    confidence: str
    source_ref: str


def validate_columns(df: pd.DataFrame) -> None:
    """Ensure required columns are present."""
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_rows(df: pd.DataFrame) -> None:
    """Validate row-level schema using pydantic."""
    for row in df.to_dict(orient="records"):
        try:
            ExtractRecord(**row)
        except ValidationError as exc:
            raise ValueError(f"Invalid extract row: {row}") from exc


def compute_panel(extracts: pd.DataFrame) -> pd.DataFrame:
    """Aggregate extracts, pivot to wide, and compute share metrics."""
    grouped = (
        extracts.groupby(["period", "region", "topic"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "topic_value"})
    )

    panel = grouped.pivot(index=["period", "region"], columns="topic", values="topic_value").reset_index()
    panel.columns.name = None

    panel["share_liangshui_in_total"] = panel["liangshui"] / panel["revenue_total"]
    panel["share_shangshui_in_total"] = panel["shangshui"] / panel["revenue_total"]

    panel = panel.sort_values(["period", "region"]).reset_index(drop=True)
    return panel


def run_pipeline() -> pd.DataFrame:
    """Execute pipeline from seed extracts to panel output."""
    extracts = pd.read_csv(RAW_PATH)
    validate_columns(extracts)
    validate_rows(extracts)

    INTERMEDIATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PANEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    extracts.to_parquet(INTERMEDIATE_PATH, index=False)

    panel = compute_panel(extracts)
    panel.to_csv(PANEL_PATH, index=False)
    return panel


if __name__ == "__main__":
    run_pipeline()
