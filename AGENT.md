# Agent rules for this repository

## 1) No fabricated historical facts

- Never invent historical values or interpretations.
- Keep auto outputs provisional (`confidence=C`, `review_status=unreviewed`).

## 2) Candidates vs facts boundary

- Candidates and auto-facts are **not verified facts**.
- Only rows with explicit `approve=1` in review sheet may be promoted to facts.
- Promotion must use reviewer-provided `final_*` fields.

## 3) Auto vs verified separation

- Auto panel is provisional and must not be used for publication without review.
- Auto-facts generation is allowed only as provisional output (`review_status=unreviewed`, `confidence=C`).
- Verified panel must be built only from approved facts (`extracts_songshi_juan186.csv`).
- Do not let verified mode consume auto-facts directly.

## 4) Evidence and traceability

- Every output row must have a source pointer (`source_ref`).
- Preserve candidate offsets (`char_start`, `char_end`) and snippet traceability.
- Keep transformations deterministic and auditable.

## 5) Ingestion hygiene

- Preserve raw source text artifacts.
- Keep source URL and license metadata.
- Rate limit requests.
- Do not broaden beyond Songshi Juǎn 186 in this MVP.

## 6) Change discipline

- Update docs/tests whenever schema or logic changes.
- Do not silently change controlled vocabularies or rules.
