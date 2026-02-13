# ns-song-fiscal-panel

Minimal, traceable Python pipelines for Song fiscal research scaffolding.

## Scope

- Period panel scaffold: `XINNING`, `YUANFENG`, `SHAOSHENG`, `HUIZONG` × `NATIONAL/NORTH/SOUTH`
- Text ingestion MVP: Songshi (宋史) Juǎn 186 from Wikisource

## Repository layout

- `data/01_raw/`: raw inputs (seed CSV + fetched text cache)
- `data/02_intermediate/`: generated intermediate outputs
- `data/03_primary/`: generated panel outputs
- `metadata/`: taxonomy, schema, and source registry
- `src/`: pipeline and ingestion/extraction code
- `tests/`: unit tests and fixtures

## Rule: no fabricated historical facts

Do **not** invent historical values or interpretations.

- Seed values are placeholder-only and must remain `confidence=C`.
- Songshi numeric extraction output is **candidate-only** for human review.

## Panel pipeline (existing MVP)

Install:

```bash
python -m pip install -e .[dev]
```

Run:

```bash
run-song-pipeline
```

Outputs:

- `data/02_intermediate/fact_numeric_extracts.parquet`
- `data/03_primary/panel_revenue_period_region.csv`

## Ingest: Songshi Juǎn 186 (Wikisource)

Command:

```bash
run-songshi-juan186
```

This command:

1. Fetches `https://zh.wikisource.org/zh-hans/宋史/卷186`
2. Stores normalized raw text at `data/01_raw/wikisource/songshi/juan186.txt`
3. Extracts machine-readable numeric candidates to
   `data/02_intermediate/candidates_songshi_juan186.csv`

Candidate CSV columns include candidate id, source URL/reference, snippet, raw number/unit,
keyword flags, rule-based candidate topic/period (default `unknown`), region (`unknown`),
and confidence (`C`).

### How to review candidates

- Treat every row as a **candidate mention**, not a final fact.
- Verify each candidate against source text context manually.
- Promote candidates to facts only after explicit human validation.

## Tests

```bash
pytest
```
