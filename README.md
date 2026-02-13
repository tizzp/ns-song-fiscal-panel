# ns-song-fiscal-panel

Minimal, traceable Python pipelines for Song fiscal research scaffolding.

## Scope

- Period panel scaffold: `XINNING`, `YUANFENG`, `SHAOSHENG`, `HUIZONG` × `NATIONAL/NORTH/SOUTH`
- Text ingestion MVP: Songshi (宋史) Juǎn 186 from Wikisource

## Rule: no fabricated historical facts

Do **not** invent historical values or interpretations.

- Candidates are provisional by default (`confidence=C`).
- No candidate becomes a fact without explicit human approval in the review sheet.

## Panel pipeline

```bash
run-song-pipeline
```

Behavior:

- If `data/01_raw/extracts_songshi_juan186.csv` exists and is non-empty, it is used.
- Otherwise, the pipeline falls back to `data/01_raw/extracts_seed.csv`.

Outputs:

- `data/02_intermediate/fact_numeric_extracts.parquet`
- `data/03_primary/panel_revenue_period_region.csv`

## Songshi Juǎn 186 workflow (human-in-the-loop)

### 1) Ingest + extract candidates

```bash
run-songshi-juan186
```

Generates:

- `data/01_raw/wikisource/songshi/juan186.txt`
- `data/02_intermediate/candidates_songshi_juan186.csv`

Candidate rows include char offsets, snippet hash, raw value/unit, parsed `value_num`,
`unit_std`, rule-based topic/period hints (default `unknown`), and `confidence=C`.

### 2) Generate review sheet

```bash
run-songshi-juan186-review
```

Generates:

- `data/02_intermediate/candidates_songshi_juan186_review_sheet.csv`

This sheet adds blank reviewer fields:

- `approve` (0/1)
- `final_period`, `final_topic`, `final_region`
- `final_value_std`, `final_unit_std`
- `interpretation_note`, `confidence_override`

### 3) Promote approved rows to facts

```bash
run-songshi-juan186-promote
```

Generates:

- `data/01_raw/extracts_songshi_juan186.csv`

Promotion rules:

- Only `approve == 1` rows are considered.
- Required final fields must be present (`period/topic/region/value/unit`) or row is skipped.
- Confidence defaults to `C` unless explicit `confidence_override` is set.

### 4) Build panel from promoted facts

```bash
run-song-pipeline
```

## Install and test

```bash
python -m pip install -e .[dev]
pytest
```
