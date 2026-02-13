"""Extract candidate numeric statements from Songshi text for human review."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pandas as pd

SOURCE_WORK = "宋史"
SOURCE_URL = "https://zh.wikisource.org/zh-hans/宋史/卷186"
JUAN = "186"

CANDIDATE_TOPICS = {
    "unknown",
    "shangshui",
    "liangshui",
    "revenue_total",
    "salt",
    "wine",
    "tea",
    "grain",
    "transport",
    "other",
}

PERIOD_MAP = {
    "熙宁": "XINNING",
    "元丰": "YUANFENG",
    "绍圣": "SHAOSHENG",
    "崇宁": "HUIZONG",
    "政和": "HUIZONG",
}

KEYWORDS = {
    "商税": "shangshui",
    "两税": "liangshui",
    "租税": "revenue_total",
    "盐": "salt",
    "酒": "wine",
    "茶": "tea",
    "漕": "transport",
    "籴": "grain",
    "京师": "other",
    "边": "other",
}

UNITS = ["貫", "石", "斛", "匹", "緡", "两", "兩", "斤", "文", "錢"]

NUMBER_PATTERN = re.compile(r"([0-9]{1,}|[零〇一二三四五六七八九十百千萬万億亿兩两廿卅卌]{1,})")


REQUIRED_COLUMNS = [
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
]

def _window(text: str, start: int, end: int, window_size: int) -> str:
    """Return context window around a match."""
    left = max(0, start - window_size)
    right = min(len(text), end + window_size)
    return text[left:right]


def _detect_keywords(context: str) -> list[str]:
    """Return matched keywords in deterministic order."""
    found = [keyword for keyword in KEYWORDS if keyword in context]
    return sorted(found)


def _detect_topic(found_keywords: list[str]) -> str:
    """Infer candidate topic only when a clear keyword rule triggers."""
    for keyword in found_keywords:
        topic = KEYWORDS[keyword]
        if topic in CANDIDATE_TOPICS and topic != "other":
            return topic
    return "unknown"


def _detect_period(context: str) -> str:
    """Infer period from explicit era names near the candidate."""
    for era_name, period in PERIOD_MAP.items():
        if era_name in context:
            return period
    return "unknown"


def _detect_unit(context: str) -> str:
    """Detect the first matched unit in local context."""
    for unit in UNITS:
        if unit in context:
            return unit
    return ""


def _candidate_id(source_ref: str, start: int, end: int, value_raw: str) -> str:
    """Build a stable candidate id from source pointer and match position."""
    payload = f"{source_ref}|{start}|{end}|{value_raw}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def extract_candidates(txt_path: Path, out_csv: Path, source_ref: str) -> pd.DataFrame:
    """Extract numeric candidate mentions from text into a CSV for review."""
    text = txt_path.read_text(encoding="utf-8")
    rows: list[dict[str, str]] = []

    for match in NUMBER_PATTERN.finditer(text):
        value_raw = match.group(1)
        start, end = match.span(1)
        local_context = _window(text, start, end, window_size=25)
        snippet = _window(text, start, end, window_size=60)

        found_keywords = _detect_keywords(local_context)
        topic = _detect_topic(found_keywords)
        period = _detect_period(local_context)
        unit_raw = _detect_unit(local_context)

        candidate_id = _candidate_id(source_ref, start, end, value_raw)
        row_source_ref = f"{source_ref}#candidate={candidate_id}"

        rows.append(
            {
                "candidate_id": candidate_id,
                "source_work": SOURCE_WORK,
                "source_url": SOURCE_URL,
                "source_ref": row_source_ref,
                "juan": JUAN,
                "snippet": snippet,
                "value_raw": value_raw,
                "unit_raw": unit_raw,
                "keywords": "|".join(found_keywords),
                "candidate_topic": topic,
                "candidate_period": period,
                "region": "unknown",
                "confidence": "C",
                "notes": "",
            }
        )

    candidates = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    candidates.to_csv(out_csv, index=False)
    return candidates
