# Schema reference

## Input facts table (`extracts_seed.csv`)
- `extract_id` (string): unique id for a source extract row.
- `period` (string): one of XINNING, YUANFENG, SHAOSHENG, HUIZONG.
- `region` (string): one of NATIONAL, NORTH, SOUTH.
- `topic` (string): one of revenue_total, liangshui, shangshui.
- `value` (number): numeric value extracted from source.
- `unit` (string): measurement unit (placeholder uses `guan`).
- `confidence` (string): evidence confidence grade (placeholder must be `C`).
- `source_ref` (string): pointer back to source evidence.

## Output panel (`panel_revenue_period_region.csv`)
- Primary key: (`period`, `region`).
- Base topic columns: `revenue_total`, `liangshui`, `shangshui`.
- Derived columns:
  - `share_liangshui_in_total` = liangshui / revenue_total
  - `share_shangshui_in_total` = shangshui / revenue_total
