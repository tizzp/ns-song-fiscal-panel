# ns-song-fiscal-panel

Minimal, traceable Python pipelines for Song fiscal research scaffolding.

## Core safety rule

Do **not** fabricate historical facts or interpretations.

- **Auto outputs are provisional** and must remain unreviewed (`confidence=C`, `review_status=unreviewed`).
- **Verified outputs** come only from explicitly approved review-sheet rows.

## Auto vs verified separation

- **Auto path** (rule-based, no human review required):
  - `candidates_songshi_juan186.csv`
  - `auto_facts_songshi_juan186.csv`
  - `panel_revenue_period_region_auto.csv`
- **Verified path** (human approved):
  - `candidates_songshi_juan186_review_sheet.csv`
  - `extracts_songshi_juan186.csv`
  - `panel_revenue_period_region_verified.csv`

Do not use the auto panel for publication without human review.
Auto outputs remain provisional by design (`confidence=C`, `review_status=unreviewed`).

## Commands

Install:

```bash
python -m pip install -e .[dev]
```

### Ingest only (fetch + candidates)

```bash
run-songshi-juan186-ingest
```

### Auto provisional workflow (candidates -> auto_facts -> auto panel)

```bash
run-songshi-juan186-auto
```

### All-in-one auto workflow (ingest + auto)

```bash
run-songshi-juan186-all
```

### Review workflow

```bash
run-songshi-juan186-review
run-songshi-juan186-promote
run-songshi-juan186-verified
```

### Backward-compatible panel command

```bash
run-song-pipeline
```

This command runs the **verified** mode and keeps compatibility with older flows:

- Uses `data/01_raw/extracts_songshi_juan186.csv` when available.
- Falls back to `data/01_raw/extracts_seed.csv` when verified facts are absent.
- Writes both:
  - `data/03_primary/panel_revenue_period_region_verified.csv`
  - `data/03_primary/panel_revenue_period_region.csv` (legacy path)

## Output locations (generated, not committed)

- `data/01_raw/wikisource/songshi/juan186.txt`
- `data/02_intermediate/candidates_songshi_juan186.csv`
- `data/02_intermediate/auto_facts_songshi_juan186.csv`
- `data/02_intermediate/candidates_songshi_juan186_review_sheet.csv`
- `data/01_raw/extracts_songshi_juan186.csv`
- `data/03_primary/panel_revenue_period_region_auto.csv`
- `data/03_primary/panel_revenue_period_region_verified.csv`

## Rule-based auto organization (MVP)

- Period inferred from era keywords (XINNING/YUANFENG/SHAOSHENG/HUIZONG).
- Topic inferred from conservative fiscal keywords.
- Region inferred heuristically from macro-region keywords as `NATIONAL`, `NORTH`, or `SOUTH`.
- If no conservative region keyword is found, region remains `unknown` and is excluded from auto panel.
- Auto-facts are emitted only when period/topic/value are safely available.
- Region inference is provisional and should be treated as a review candidate, not publication-ready fact.
- Region defaults to `unknown`; auto panel includes only `NATIONAL` rows.
- Auto-facts are emitted only when period/topic/value are safely available.

## Tests

```bash
pytest
```
