# Agent rules for this repository

## 1) Evidence pointers are mandatory

- Every extract or candidate row must include a non-empty `source_ref` pointer.
- For textual ingestion, always preserve `source_url` and candidate-level traceability.

## 2) Schema discipline

- Do not rename required columns without updating all of:
  - `metadata/schema.md`
  - tests
  - pipeline/extraction code
- Keep controlled vocabulary aligned with `metadata/taxonomy.yml`.

## 3) No silent changes

- Any logic change must be documented in `README.md`.
- Any schema change must be documented in `metadata/schema.md`.

## 4) Traceability first

- Output rows must be reproducible from deterministic transformations.
- Keep transformations explicit and auditable.

## 5) No fabricated historical facts

- Never invent historical values or interpretations.
- Seed data and extracted candidates remain provisional until human review.

## 6) Ingestion rules (strict)

- Preserve raw text artifacts from source pages.
- Store source URL and license context in metadata.
- Rate limit network requests (sleep between requests).
- Do **not** convert candidates into facts without explicit human review.
