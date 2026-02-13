# Schema reference

## Input facts table (`extracts_seed.csv`)

Required columns:

- `extract_id` (string): unique id for a source extract row.
- `period` (string): one of XINNING, YUANFENG, SHAOSHENG, HUIZONG.
- `region` (string): one of NATIONAL, NORTH, SOUTH.
- `topic` (string): one of revenue_total, liangshui, shangshui.
- `value` (number): numeric value extracted from source.
- `unit` (string): measurement unit (placeholder uses `guan`).
- `confidence` (string): evidence confidence grade (placeholder must be `C`).
- `source_ref` (string): source evidence pointer.

### Recommended `source_ref` structure

Use a structured pointer so humans can trace each row quickly:

- Format: `book:<book_id>|locator:<locator_text>`
- Example: `book:SongShi-ZhiShiHuo|locator:juan-173-folio-12b`

For placeholder seed rows only, `placeholder://...` values are allowed.
Real extract rows should use book + locator format.

## Output panel (`panel_revenue_period_region.csv`)

- Primary key: (`period`, `region`).
- Base topic columns: `revenue_total`, `liangshui`, `shangshui`.
- Derived columns:
  - `share_liangshui_in_total` = liangshui / revenue_total
  - `share_shangshui_in_total` = shangshui / revenue_total
- Share behavior:
  - If `revenue_total > 0`, shares are finite values in `[0, 1]`.
  - If `revenue_total <= 0`, shares are `NaN` (never `inf`).
