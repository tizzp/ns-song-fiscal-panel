# Agent rules for this repository

## 1) Evidence pointers are mandatory

- Every extract or candidate row must include a non-empty `source_ref` pointer.
- For textual ingestion, keep candidate-level source pointers with URL + char offsets.

## 2) Schema discipline

- Do not rename required columns without updating all of:
  - schema docs
  - tests
  - pipeline/extraction/review code
- Keep controlled vocabulary aligned with metadata files.

## 3) No silent changes

- Any logic change must be documented in `README.md`.
- Any schema change must be documented in metadata docs.

## 4) Traceability first

- Outputs must be reproducible from deterministic transformations.
- Preserve raw text and source URL/license provenance.

## 5) No fabricated historical facts

- Never invent historical values or interpretations.
- Candidate extraction output is provisional and defaults to `confidence=C`.

## 6) Candidates vs facts boundary (strict)

- Candidates do **not** become facts automatically.
- Only rows explicitly marked `approve=1` in the review sheet can be promoted.
- Promotion must use reviewer-provided `final_*` values.
- Without human review, candidates must remain candidates.

## 7) Ingestion rules

- Preserve raw text artifacts from source pages.
- Store source URL and license context in metadata.
- Rate limit network requests.
- Do not broaden ingestion beyond Songshi Juǎn 186 in this MVP.
