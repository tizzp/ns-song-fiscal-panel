"""Build period-region panel outputs from auto or verified Songshi facts."""

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

PERIODS: Final[set[str]] = {"XINNING", "YUANFENG", "SHAOSHENG", "HUIZONG"}
TOPICS: Final[set[str]] = {"revenue_total", "liangshui", "shangshui"}
EXPECTED_TOPIC_COLUMNS: Final[list[str]] = ["revenue_total", "liangshui", "shangshui"]

BASE_DIR: Final[Path] = Path(__file__).resolve().parents[1]
VERIFIED_FACTS_PATH: Final[Path] = BASE_DIR / "data" / "01_raw" / "extracts_songshi_juan186.csv"
AUTO_FACTS_PATH: Final[Path] = BASE_DIR / "data" / "02_intermediate" / "auto_facts_songshi_juan186.csv"
SEED_FACTS_PATH: Final[Path] = BASE_DIR / "data" / "01_raw" / "extracts_seed.csv"
INTERMEDIATE_PATH: Final[Path] = BASE_DIR / "data" / "02_intermediate" / "fact_numeric_extracts.parquet"
AUTO_PANEL_PATH: Final[Path] = BASE_DIR / "data" / "03_primary" / "panel_revenue_period_region_auto.csv"
VERIFIED_PANEL_PATH: Final[Path] = BASE_DIR / "data" / "03_primary" / "panel_revenue_period_region_verified.csv"
LEGACY_PANEL_PATH: Final[Path] = BASE_DIR / "data" / "03_primary" / "panel_revenue_period_region.csv"
INTERMEDIATE_PATH: Final[Path] = BASE_DIR / "data" / "02_intermediate" / "fact_numeric_extracts.parquet"
AUTO_PANEL_PATH: Final[Path] = BASE_DIR / "data" / "03_primary" / "panel_revenue_period_region_auto.csv"
VERIFIED_PANEL_PATH: Final[Path] = BASE_DIR / "data" / "03_primary" / "panel_revenue_period_region_verified.csv"


class ExtractRecord(BaseModel):
    """Schema for facts rows consumed by panel pipeline."""

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


def _filtered_for_panel(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    """Filter facts to panel-supported periods/topics and mode-specific region rules."""
    filtered = df[df["period"].isin(PERIODS) & df["topic"].isin(TOPICS)].copy()
    if mode == "auto":
        filtered = filtered[filtered["region"] == "NATIONAL"]
    return filtered


def compute_panel(extracts: pd.DataFrame) -> pd.DataFrame:
    """Aggregate extracts, pivot to wide, compute shares, and attach supporting ids."""
    if extracts.empty:
        return pd.DataFrame(
            columns=[
                "period",
                "region",
                "revenue_total",
                "liangshui",
                "shangshui",
                "supporting_extract_ids",
                "share_liangshui_in_total",
                "share_shangshui_in_total",
            ]
        )

    grouped = (
        extracts.groupby(["period", "region", "topic"], as_index=False)
        .agg(topic_value=("value", "sum"), supporting_extract_ids=("extract_id", lambda x: "|".join(sorted(set(x)))))
    )

    value_panel = grouped.pivot(index=["period", "region"], columns="topic", values="topic_value").reset_index()
    value_panel.columns.name = None

    ids_panel = grouped.groupby(["period", "region"], as_index=False).agg(
        supporting_extract_ids=(
            "supporting_extract_ids",
            lambda x: "|".join(sorted(set("|".join(x).split("|")))),
        )
    ids_panel = (
        grouped.groupby(["period", "region"], as_index=False)["supporting_extract_ids"]
        .agg(lambda x: "|".join(sorted(set("|".join(x).split("|")))))
        .rename(columns={"supporting_extract_ids": "supporting_extract_ids"})
    )

    for topic_name in EXPECTED_TOPIC_COLUMNS:
        if topic_name not in value_panel.columns:
            LOGGER.warning("Missing topic column after pivot; filling with 0: %s", topic_name)
            value_panel[topic_name] = 0.0

    panel = value_panel.merge(ids_panel, on=["period", "region"], how="left")
    panel["share_liangshui_in_total"] = _safe_divide(panel["liangshui"], panel["revenue_total"])
    panel["share_shangshui_in_total"] = _safe_divide(panel["shangshui"], panel["revenue_total"])
    panel = panel.sort_values(["period", "region"]).reset_index(drop=True)
    return panel


def _resolve_verified_input() -> Path:
    """Return verified facts path, falling back to seed facts for compatibility."""
    if VERIFIED_FACTS_PATH.exists() and VERIFIED_FACTS_PATH.stat().st_size > 0:
        return VERIFIED_FACTS_PATH
    return SEED_FACTS_PATH


def run_panel_mode(mode: str) -> pd.DataFrame:
    """Run a single panel mode: auto or verified."""
    if mode not in {"auto", "verified"}:
        raise ValueError("mode must be one of {'auto','verified'}")

    input_path = AUTO_FACTS_PATH if mode == "auto" else _resolve_verified_input()
    input_path = AUTO_FACTS_PATH if mode == "auto" else VERIFIED_FACTS_PATH
    output_path = AUTO_PANEL_PATH if mode == "auto" else VERIFIED_PANEL_PATH

    if not input_path.exists() or input_path.stat().st_size == 0:
        empty = pd.DataFrame(
            columns=[
                "period",
                "region",
                "revenue_total",
                "liangshui",
                "shangshui",
                "supporting_extract_ids",
                "share_liangshui_in_total",
                "share_shangshui_in_total",
            ]
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        empty.to_csv(output_path, index=False)
        return empty

    extracts = pd.read_csv(input_path)
    validate_columns(extracts)
    validate_rows(extracts)
    filtered = _filtered_for_panel(extracts, mode=mode)

    if mode == "verified":
        INTERMEDIATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        extracts.to_parquet(INTERMEDIATE_PATH, index=False)

    panel = compute_panel(filtered)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(output_path, index=False)
    if mode == "verified":
        panel.to_csv(LEGACY_PANEL_PATH, index=False)
    return panel


def run_pipeline() -> pd.DataFrame:
    """Run verified mode for backwards-compatible command behavior."""
    return run_panel_mode("verified")


def run_auto_panel() -> pd.DataFrame:
    """Run auto mode panel aggregation from provisional auto-facts."""
    return run_panel_mode("auto")


if __name__ == "__main__":
    run_pipeline()
