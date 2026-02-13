# Agent rules for this repository

1. **Evidence pointers are mandatory**
   - Every extract row must include a non-empty `source_ref` pointer.
   - Do not create derived values unless they can be traced to extract rows.

2. **Schema discipline**
   - Do not rename required columns without updating `metadata/schema.md`, tests, and pipeline code.
   - Keep taxonomy terms aligned with `metadata/taxonomy.yml`.

3. **No silent changes**
   - Any logic change must be documented in `README.md`.
   - Any schema change must be documented in `metadata/schema.md`.

4. **Traceability first**
   - Output rows in the panel must be reproducible from the facts table via deterministic aggregation.
   - Seed data is placeholder only and must remain confidence `C` until replaced by real extracts.
