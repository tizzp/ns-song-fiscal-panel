# Schema reference

## Facts table used by panel pipeline

Primary facts schema (`extracts_seed.csv` or promoted `extracts_songshi_juan186.csv`):

- `extract_id` (string): unique id for a facts row.
- `period` (string): e.g., XINNING, YUANFENG, SHAOSHENG, HUIZONG.
- `region` (string): NATIONAL, NORTH, SOUTH, or approved value.
- `topic` (string): e.g., revenue_total, liangshui, shangshui.
- `value` (number): numeric value.
- `unit` (string): unit label.
- `confidence` (string): confidence grade (`C` default unless reviewed override).
- `source_ref` (string): source evidence pointer.

## Candidate extraction table (Songshi Juǎn 186)

`candidates_songshi_juan186.csv` columns:

- `candidate_id`, `source_work`, `source_url`, `source_ref`
- `juan`, `char_start`, `char_end`, `snippet`, `snippet_hash`
- `value_raw`, `value_num`, `unit_raw`, `unit_std`
- `keywords`, `candidate_topic`, `candidate_period`, `region`
- `confidence`, `notes`

Candidate defaults and boundary:

- Candidates are provisional; `confidence` defaults to `C`.
- Candidates are not facts until explicitly approved in review sheet.

## Review sheet columns

`candidates_songshi_juan186_review_sheet.csv` keeps all candidate columns and adds:

- `approve` (0/1)
- `final_period`, `final_topic`, `final_region`
- `final_value_std`, `final_unit_std`
- `interpretation_note`, `confidence_override`

Promotion rule:

- Only `approve == 1` rows with complete `final_*` fields are promoted to facts.
