# ns-song-fiscal-panel

Minimal, traceable Python pipeline to build a fiscal panel by **period x region**.

## Scope

- Periods: `XINNING`, `YUANFENG`, `SHAOSHENG`, `HUIZONG`
- Regions: `NATIONAL`, `NORTH`, `SOUTH`
- Topics: `revenue_total`, `liangshui`, `shangshui`

The pipeline starts from a numeric extract facts table and produces:

1. Intermediate facts parquet
2. Primary panel CSV with derived share metrics

## Repository layout

- `data/01_raw/`: input extracts (seed placeholder included)
- `data/02_intermediate/`: generated facts parquet
- `data/03_primary/`: generated panel output
- `data/04_features/`: reserved for later feature outputs
- `metadata/taxonomy.yml`: controlled vocabulary
- `metadata/schema.md`: schema contract
- `src/pipeline_end_to_end.py`: runnable pipeline
- `tests/`: baseline unit tests

## Data model + traceability

Input facts schema is documented in `metadata/schema.md`.
Each extract row must include:

- `extract_id`
- `source_ref`
- `confidence`

The included seed data is placeholder-only and uses `confidence=C`.

### No fabricated historical facts

Do **not** invent historical values or claims.
Use seed data only as runnable scaffolding until real extracts are available.

### Transformation logic

Panel rows are deterministic aggregates from extract rows grouped by:

- `period`
- `region`
- `topic`

Topics are pivoted to wide columns and shares are computed:

- `share_liangshui_in_total = liangshui / revenue_total`
- `share_shangshui_in_total = shangshui / revenue_total`

Share handling is safe:

- If `revenue_total > 0`, shares are finite values in `[0, 1]`.
- If `revenue_total <= 0`, shares are `NaN` (never `inf`).

## Reproducible setup

```bash
python -m pip install -e .[dev]
```

## Single command to run pipeline

```bash
run-song-pipeline
```

Outputs created:

- `data/02_intermediate/fact_numeric_extracts.parquet`
- `data/03_primary/panel_revenue_period_region.csv`

## Run tests

```bash
pytest
```

## Replace seed with real extracts

1. Replace `data/01_raw/extracts_seed.csv` with real extract rows under the same schema.
2. Keep required columns and taxonomy values aligned with `metadata/taxonomy.yml`.
3. Ensure every row has a valid evidence pointer in `source_ref` and a confidence rating.
4. Re-run `run-song-pipeline` and then `pytest`.
