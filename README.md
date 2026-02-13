# ns-song-fiscal-panel

Minimal, traceable Python pipelines for Song fiscal research scaffolding.

## Core safety rule

Do **not** fabricate historical facts or interpretations.

- **Auto outputs are provisional** and must remain unreviewed (`confidence=C`, `review_status=unreviewed`).
- **Verified outputs** come from approved facts (`extracts_songshi_juan186.csv`), with fallback to seed facts only for compatibility when verified facts are absent.

## Auto vs verified separation

- **Auto path** (rule-based, provisional):
  - `data/02_intermediate/candidates_songshi_juan186.csv`
  - `data/02_intermediate/auto_facts_songshi_juan186.csv`
  - `data/03_primary/panel_revenue_period_region_auto.csv`
- **Verified path** (approved or seed fallback compatibility):
  - `data/02_intermediate/candidates_songshi_juan186_review_sheet.csv`
  - `data/01_raw/extracts_songshi_juan186.csv`
  - `data/01_raw/extracts_seed.csv` (fallback input only)
  - `data/03_primary/panel_revenue_period_region_verified.csv`
  - `data/03_primary/panel_revenue_period_region.csv` (legacy compatibility output)

Do not use the auto panel for publication without human review.

## Commands

Install:

```bash
python -m pip install -e .[dev]
```

Ingest only (fetch + candidates):

```bash
run-songshi-juan186-ingest
```

Expected outputs:
- `data/01_raw/wikisource/songshi/juan186.txt`
- `data/02_intermediate/candidates_songshi_juan186.csv`

Auto provisional workflow (candidates -> auto facts -> auto panel):

```bash
run-songshi-juan186-auto
```

Expected outputs:
- `data/02_intermediate/auto_facts_songshi_juan186.csv`
- `data/03_primary/panel_revenue_period_region_auto.csv`

All-in-one auto workflow (ingest + auto):

```bash
run-songshi-juan186-all
```

Review and verified workflow:

```bash
run-songshi-juan186-review
run-songshi-juan186-promote
run-songshi-juan186-verified
```

Expected verified outputs:
- `data/03_primary/panel_revenue_period_region_verified.csv`
- `data/03_primary/panel_revenue_period_region.csv`

Backward-compatible panel command:

```bash
run-song-pipeline
```

This command runs verified mode:
- Uses `data/01_raw/extracts_songshi_juan186.csv` when available.
- Falls back to `data/01_raw/extracts_seed.csv` when verified facts are absent.
- Writes both verified and legacy panel files.

## Output locations (generated, not committed)

Pipeline artifacts under `data/` are generated locally and are not meant to be committed. Primary outputs include:

- `data/01_raw/wikisource/songshi/juan186.txt`
- `data/02_intermediate/candidates_songshi_juan186.csv`
- `data/02_intermediate/auto_facts_songshi_juan186.csv`
- `data/02_intermediate/candidates_songshi_juan186_review_sheet.csv`
- `data/01_raw/extracts_songshi_juan186.csv`
- `data/03_primary/panel_revenue_period_region_auto.csv`
- `data/03_primary/panel_revenue_period_region_verified.csv`
- `data/03_primary/panel_revenue_period_region.csv`

## Rule-based auto organization (MVP)

- Period inferred from era keywords (`XINNING`, `YUANFENG`, `SHAOSHENG`, `HUIZONG`).
- Topic inferred from conservative fiscal keywords.
- Region inferred heuristically as `NATIONAL`, `NORTH`, `SOUTH`, or `unknown`.
- Auto-facts keep `unknown` regions for review traceability.
- Auto panel excludes `unknown` and includes only `NATIONAL` / `NORTH` / `SOUTH`.
- Auto-facts are emitted only when period/topic/value are safely available.
- All auto-facts stay provisional (`confidence=C`, `review_status=unreviewed`).

## Tests

```bash
pytest
```
