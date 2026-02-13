"""Extract candidate numeric statements from Songshi text for human review."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Optional

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

NUMBER_PATTERN = re.compile(r"([0-9]{1,}|[零〇一二三四五六七八九十百千萬万億亿兩两廿卅卌]{1,})")

DIGIT_MAP = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
    "两": 2,
    "兩": 2,
}

SMALL_UNIT_MAP = {
    "十": 10,
    "百": 100,
    "千": 1000,
}

BIG_UNIT_MAP = {
    "万": 10_000,
    "萬": 10_000,
    "亿": 100_000_000,
    "億": 100_000_000,
}

UNIT_MAP = {
    "貫": "guan",
    "緡": "guan",
    "石": "shi",
    "斛": "hu",
    "匹": "pi",
    "斤": "jin",
    "两": "liang",
    "兩": "liang",
    "文": "wen",
    "錢": "qian",
}

REQUIRED_COLUMNS = [
    "candidate_id",
    "source_work",
    "source_url",
    "source_ref",
    "juan",
    "char_start",
    "char_end",
    "snippet",
    "snippet_hash",
    "value_raw",
    "value_num",
    "unit_raw",
    "unit_std",
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
    for token in UNIT_MAP:
        if token in context:
            return token
    return ""


def _standardize_unit(unit_raw: str) -> str:
    """Map raw unit token to controlled unit enum."""
    return UNIT_MAP.get(unit_raw, "unknown")


def _candidate_id(source_ref: str, start: int, end: int, value_raw: str) -> str:
    """Build a stable candidate id from source pointer and match position."""
    payload = f"{source_ref}|{start}|{end}|{value_raw}".encode("utf-8")
    return hashlib.sha1(payload).hexdigest()


def _snippet_hash(snippet: str) -> str:
    """Build a stable hash for the snippet text."""
    return hashlib.sha1(snippet.encode("utf-8")).hexdigest()


def parse_chinese_numeral(value_raw: str) -> Optional[float]:
    """Parse Arabic or Chinese numeral text into numeric value; return None if ambiguous."""
    text = value_raw.strip()
    if text == "":
        return None

    if text.isdigit():
        return float(text)

    normalized = text.replace("廿", "二十").replace("卅", "三十").replace("卌", "四十")

    valid_chars = set(DIGIT_MAP) | set(SMALL_UNIT_MAP) | set(BIG_UNIT_MAP)
    if any(char not in valid_chars for char in normalized):
        return None

    total = 0
    section = 0
    number = 0

    for char in normalized:
        if char in DIGIT_MAP:
            number = DIGIT_MAP[char]
            continue

        if char in SMALL_UNIT_MAP:
            unit_value = SMALL_UNIT_MAP[char]
            if number == 0:
                number = 1
            section += number * unit_value
            number = 0
            continue

        if char in BIG_UNIT_MAP:
            section += number
            if section == 0:
                return None
            total += section * BIG_UNIT_MAP[char]
            section = 0
            number = 0
            continue

    value = total + section + number
    if value == 0:
        return None
    return float(value)


def extract_candidates(txt_path: Path, out_csv: Path, source_ref: str) -> pd.DataFrame:
    """Extract numeric candidate mentions from text into a CSV for review."""
    text = txt_path.read_text(encoding="utf-8")
    rows: list[dict[str, object]] = []

    for match in NUMBER_PATTERN.finditer(text):
        value_raw = match.group(1)
        start, end = match.span(1)
        local_context = _window(text, start, end, window_size=25)
        snippet = _window(text, start, end, window_size=60)

        found_keywords = _detect_keywords(local_context)
        topic = _detect_topic(found_keywords)
        period = _detect_period(local_context)
        unit_raw = _detect_unit(local_context)
        unit_std = _standardize_unit(unit_raw)

        candidate_id = _candidate_id(source_ref, start, end, value_raw)
        row_source_ref = f"{source_ref}#start={start}&end={end}&cid={candidate_id}"

        rows.append(
            {
                "candidate_id": candidate_id,
                "source_work": SOURCE_WORK,
                "source_url": SOURCE_URL,
                "source_ref": row_source_ref,
                "juan": JUAN,
                "char_start": start,
                "char_end": end,
                "snippet": snippet,
                "snippet_hash": _snippet_hash(snippet),
                "value_raw": value_raw,
                "value_num": parse_chinese_numeral(value_raw),
                "unit_raw": unit_raw,
                "unit_std": unit_std,
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
