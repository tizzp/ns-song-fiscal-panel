"""Run a minimal traceable end-to-end fiscal panel pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Final

import pandas as pd
from pydantic import BaseModel, ValidationError

LOGGER = logging.getLogger(__name__)

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

EXPECTED_TOPIC_COLUMNS: Final[list[str]] = [
    "revenue_total",
    "liangshui",
    "shangshui",
]

BASE_DIR: Final[Path] = Path(__file__).resolve().parents[1]
RAW_PATH: Final[Path] = BASE_DIR / "data" / "01_raw" / "extracts_seed.csv"
INTERMEDIATE_PATH: Final[Path] = (
    BASE_DIR / "data" / "02_intermediate" / "fact_numeric_extracts.parquet"
)
PANEL_PATH: Final[Path] = (
    BASE_DIR / "data" / "03_primary" / "panel_revenue_period_region.csv"
)


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


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Return numerator / denominator with NaN where denominator is not positive."""
    return numerator.div(denominator.where(denominator > 0))


def compute_panel(extracts: pd.DataFrame) -> pd.DataFrame:
    """Aggregate extracts, pivot to wide, and compute share metrics."""
    grouped = (
        extracts.groupby(["period", "region", "topic"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "topic_value"})
    )

    panel = (
        grouped.pivot(
            index=["period", "region"],
            columns="topic",
            values="topic_value",
        )
        .reset_index()
    )
    panel.columns.name = None

    missing_topics = [
        topic_name for topic_name in EXPECTED_TOPIC_COLUMNS if topic_name not in panel.columns
    ]
    if missing_topics:
        LOGGER.warning(
            "Missing topic columns after pivot; filling with 0 values: %s",
            missing_topics,
        )
        for topic_name in missing_topics:
            panel[topic_name] = 0.0

    panel["share_liangshui_in_total"] = _safe_divide(
        panel["liangshui"],
        panel["revenue_total"],
    )
    panel["share_shangshui_in_total"] = _safe_divide(
        panel["shangshui"],
        panel["revenue_total"],
    )

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
