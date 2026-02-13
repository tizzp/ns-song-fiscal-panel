# Agent rules for this repository

## 1) Evidence pointers are mandatory

- Every extract row must include a non-empty `source_ref` pointer.
- Do not create derived values unless they can be traced to extract rows.
- For real data, prefer `book:<book_id>|locator:<locator_text>` style pointers.

## 2) Schema discipline

- Do not rename required columns without updating all of:
  - `metadata/schema.md`
  - tests
  - pipeline code
- Keep controlled vocabulary aligned with `metadata/taxonomy.yml`.

## 3) No silent changes

- Any logic change must be documented in `README.md`.
- Any schema change must be documented in `metadata/schema.md`.

## 4) Traceability first

- Output rows in the panel must be reproducible from the facts table via deterministic aggregation.
- Keep transformations explicit and easy to audit.

## 5) No fabricated historical facts

- Never invent historical values or claims.
- Seed values are placeholders only and must remain `confidence=C` until replaced by real extracts.
