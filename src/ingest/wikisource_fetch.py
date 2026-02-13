"""Fetch and normalize Wikisource pages to plain text."""

from __future__ import annotations

import time
from pathlib import Path
from urllib.request import Request, urlopen

try:
    import requests
except ImportError:  # pragma: no cover - optional runtime dependency in offline envs
    requests = None  # type: ignore[assignment]

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - optional runtime dependency in offline envs
    BeautifulSoup = None  # type: ignore[assignment]

import requests
from bs4 import BeautifulSoup

DEFAULT_USER_AGENT = "ns-song-fiscal-panel/0.1 (+https://github.com/tizzp/ns-song-fiscal-panel)"


def _extract_readable_text(html: str) -> str:
    """Extract readable body text from a Wikisource HTML page."""
    if BeautifulSoup is None:
        text = html
        for tag in ["script", "style", "noscript"]:
            text = text.replace(f"<{tag}", "\n<")
        text = text.replace("<", "\n<")
        text = text.replace(">", ">\n")
        lines = [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("<")]
        return "\n".join(lines)

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "noscript", "header", "footer", "nav"]):
        tag.decompose()

    main = soup.select_one("div.mw-parser-output")
    if main is None:
        main = soup.body or soup

    for noisy in main.select(".reference, .mw-editsection, .toc, table.navbox"):
        noisy.decompose()

    text = main.get_text("\n", strip=True)
    return text


def _download_html(url: str) -> str:
    """Download page HTML with requests when available, otherwise urllib fallback."""
    if requests is not None:
        response = requests.get(
            url,
            timeout=30,
            headers={"User-Agent": DEFAULT_USER_AGENT},
        )
        response.raise_for_status()
        return response.text

    req = Request(url, headers={"User-Agent": DEFAULT_USER_AGENT})
    with urlopen(req, timeout=30) as response:  # nosec B310 - controlled URL input
        return response.read().decode("utf-8", errors="replace")


def fetch_wikisource_page(
    url: str,
    out_html: Path,
    out_txt: Path,
    sleep_seconds: float = 1.0,
    force: bool = False,
) -> None:
    """Fetch a Wikisource page and save HTML + plain text with cache-aware behavior."""
    out_txt.parent.mkdir(parents=True, exist_ok=True)
    out_html.parent.mkdir(parents=True, exist_ok=True)

    if out_txt.exists() and out_txt.stat().st_size > 0 and not force:
        return

    time.sleep(max(sleep_seconds, 0.0))

    html = _download_html(url)
    response = requests.get(
        url,
        timeout=30,
        headers={"User-Agent": DEFAULT_USER_AGENT},
    )
    response.raise_for_status()

    html = response.text
    out_html.write_text(html, encoding="utf-8")

    text = _extract_readable_text(html)
    out_txt.write_text(text, encoding="utf-8")
