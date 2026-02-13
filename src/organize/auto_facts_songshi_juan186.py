"""Auto-organize Songshi Juan 186 candidates into provisional facts."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml

BASE_DIR = Path(__file__).resolve().parents[2]
CANDIDATES_PATH = BASE_DIR / "data" / "02_intermediate" / "candidates_songshi_juan186.csv"
AUTO_FACTS_PATH = BASE_DIR / "data" / "02_intermediate" / "auto_facts_songshi_juan186.csv"
RULES_PATH = BASE_DIR / "metadata" / "rules_songshi_juan186.yml"

TARGET_TOPICS = {"revenue_total", "liangshui", "shangshui"}

AUTO_FACT_COLUMNS = [
    "extract_id",
    "period",
    "region",
    "topic",
    "value",
    "unit",
    "confidence",
    "review_status",
    "source_ref",
    "rule_trace",
]


def _load_rules(rules_path: Path) -> dict[str, Any]:
    """Load YAML rules for era/topic/region mapping."""
    return yaml.safe_load(rules_path.read_text(encoding="utf-8"))


def _match_first(text: str, mapping: dict[str, list[str]]) -> tuple[str, str]:
    """Return first matching label and matched keyword."""
    for label, keywords in mapping.items():
        for keyword in keywords:
            if keyword in text:
                return label, keyword
    return "unknown", ""


def auto_organize_facts(candidates_csv: Path, out_csv: Path, rules_path: Path) -> pd.DataFrame:
    """Map candidates into provisional auto-facts using conservative rules."""
    candidates = pd.read_csv(candidates_csv)
    rules = _load_rules(rules_path)

    rows: list[dict[str, object]] = []
    for _, row in candidates.iterrows():
        snippet = str(row.get("snippet", ""))

        period, period_kw = _match_first(snippet, rules["era_keywords"])
        topic, topic_kw = _match_first(snippet, rules["topic_keywords"])
        region, region_kw = _match_first(snippet, rules["region_keywords"])

        value = row.get("value_num")
        if pd.isna(value):
            value = None

        unit = str(row.get("unit_std", "unknown") or "unknown")

        if period == "unknown":
            continue
        if topic not in TARGET_TOPICS:
            continue
        if value is None:
            continue

        trace_parts = []
        if period_kw:
            trace_parts.append(f"period:{period_kw}")
        if topic_kw:
            trace_parts.append(f"topic:{topic_kw}")
        if region_kw:
            trace_parts.append(f"region:{region_kw}")
        trace_parts.append(f"unit:{unit}")

        rows.append(
            {
                "extract_id": f"auto-songshi-juan186-{row['candidate_id']}",
                "period": period,
                "region": region,
                "topic": topic,
                "value": float(value),
                "unit": unit,
                "confidence": "C",
                "review_status": "unreviewed",
                "source_ref": str(row.get("source_ref", "")),
                "rule_trace": "|".join(trace_parts),
            }
        )

    auto_facts = pd.DataFrame(rows, columns=AUTO_FACT_COLUMNS)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    auto_facts.to_csv(out_csv, index=False)
    return auto_facts


def main() -> None:
    """CLI wrapper for auto-facts organization."""
    auto_facts = auto_organize_facts(CANDIDATES_PATH, AUTO_FACTS_PATH, RULES_PATH)
    print(f"auto_facts_csv: {AUTO_FACTS_PATH}")
    print(f"auto_facts_rows: {len(auto_facts)}")


if __name__ == "__main__":
    main()
