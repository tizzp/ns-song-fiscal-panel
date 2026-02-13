"""Auto-organize Songshi Juan 186 candidates into provisional facts."""

from __future__ import annotations

from pathlib import Path
import ast
from typing import Any

import pandas as pd

try:
    import yaml
except ImportError:  # pragma: no cover - optional runtime dependency in offline envs
    yaml = None  # type: ignore[assignment]

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
    content = rules_path.read_text(encoding="utf-8")
    if yaml is not None:
        return yaml.safe_load(content)

    parsed: dict[str, dict[str, list[str]]] = {
        "era_keywords": {},
        "topic_keywords": {},
        "region_keywords": {},
    }
    section = ""
    label = ""

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        if indent == 0 and stripped.endswith(":"):
            key = stripped[:-1]
            if key in parsed:
                section = key
            continue

        if indent == 2 and section:
            if ":" not in stripped:
                continue
            key_part, value_part = stripped.split(":", 1)
            label = key_part.strip()
            value_part = value_part.strip()
            parsed[section][label] = []
            if value_part.startswith("[") and value_part.endswith("]"):
                parsed_list = ast.literal_eval(value_part)
                parsed[section][label] = [str(item) for item in parsed_list]
                label = ""
            continue

        if indent >= 4 and stripped.startswith("-") and section and label:
            value = stripped[1:].strip().strip('"').strip("'")
            if value:
                parsed[section][label].append(value)

    return parsed


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
