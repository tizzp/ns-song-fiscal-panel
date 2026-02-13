"""CLI helpers for Songshi Juan 186 ingest/auto/verified workflows."""

from __future__ import annotations

from organize.auto_facts_songshi_juan186 import (
    AUTO_FACTS_PATH,
    CANDIDATES_PATH,
    RULES_PATH,
    auto_organize_facts,
)
from ingest_songshi_juan186 import run_songshi_juan186_pipeline
from pipeline_end_to_end import run_auto_panel, run_panel_mode


def run_songshi_juan186_ingest() -> None:
    """Fetch and extract Songshi Juan 186 candidates."""
    run_songshi_juan186_pipeline()


def run_songshi_juan186_auto() -> None:
    """Auto-organize candidates to provisional facts and build auto panel."""
    auto_organize_facts(CANDIDATES_PATH, AUTO_FACTS_PATH, RULES_PATH)
    panel = run_auto_panel()
    print(f"auto_panel_rows: {len(panel)}")


def run_songshi_juan186_verified() -> None:
    """Build verified panel from approved facts (or fallback seed when absent)."""
    panel = run_panel_mode("verified")
    print(f"verified_panel_rows: {len(panel)}")


def run_songshi_juan186_all() -> None:
    """Run ingest + auto organization + auto panel in one command."""
    run_songshi_juan186_ingest()
    run_songshi_juan186_auto()


if __name__ == "__main__":
    run_songshi_juan186_all()
