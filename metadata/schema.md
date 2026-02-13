# Schema reference

## Candidate extraction output

`data/02_intermediate/candidates_songshi_juan186.csv`:

- `candidate_id`
- `source_work`, `source_url`, `source_ref`
- `juan`, `char_start`, `char_end`, `snippet`, `snippet_hash`
- `value_raw`, `value_num`, `unit_raw`, `unit_std`
- `keywords`, `candidate_topic`, `candidate_period`, `region`
- `confidence`, `notes`

Rules:

- Candidate rows are provisional and default to `confidence=C`.
- `source_ref` must include URL + character offsets + candidate id.

## Auto-facts output (provisional)

`data/02_intermediate/auto_facts_songshi_juan186.csv`:

- `extract_id`
- `period` (`XINNING|YUANFENG|SHAOSHENG|HUIZONG|unknown`)
- `region` (`NATIONAL|NORTH|SOUTH|unknown`)
- `topic` (`revenue_total|liangshui|shangshui|unknown`)
- `value` (float)
- `unit` (standardized unit or `unknown`)
- `confidence` (always `C`)
- `review_status` (always `unreviewed`)
- `source_ref`
- `rule_trace`

## Verified facts output

`data/01_raw/extracts_songshi_juan186.csv` (only from approved review rows):

- `extract_id`, `period`, `region`, `topic`, `value`, `unit`, `confidence`, `source_ref`

## Panels

- Auto panel: `data/03_primary/panel_revenue_period_region_auto.csv` (provisional)
- Verified panel: `data/03_primary/panel_revenue_period_region_verified.csv` (approved facts path)
