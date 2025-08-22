
# Business-day (20:00–20:00) Support — Patch

**Date:** 2025-08-22

This patch adds **business-day boundaries** to your Kaspi orders service so a "day" can start at any time
(e.g., **20:00**), preventing orders from 20:00 → 00:00 from being dropped when aggregating.

## What’s included
- `app/utils/business_day.py` — utilities to parse `"HH:MM"`, compute offsets, and shift timestamps to business-day buckets.
- `app/schemas/settings.py` — Pydantic schema for store settings (business-day start, timezone).
- `app/models/store_settings.py` — SQLAlchemy model for persisted store settings.
- `app/api/settings.py` — FastAPI routes to GET/POST store settings.
- `app/api/orders_summary.py` — example summary endpoint that **respects business-day start** and timezone.
- `migrations/20250822_add_store_settings.sql` — SQL migration for `store_settings` table.
- Frontend snippets (optional): `ui/snippets/business-day-filter.html`, `ui/assets/js/business-day.js`, `ui/assets/css/business-day.css`.

## How it works (core idea)
We **shift timestamps** by the configured offset (e.g., 20:00 → offset 20h) before bucketizing by date.
So orders at `2025-08-22 21:15` local time are treated as `2025-08-22` (same business day that started at 20:00).

- For querying a date range by *business days*, we subtract the offset from the range and convert to UTC before hitting the DB.
- For grouping per day, we subtract the offset from each row’s timestamp (in local timezone) and then take `.date()`.

This is DB-agnostic and avoids complex SQL per dialect.

## Integration steps
1. **Backend**
   - Drop-in the files under `app/...` (adjust imports to your project’s structure if needed).
   - Ensure your existing `Session` / `Base` are imported correctly inside the provided modules (see TODO comments).
   - Run migration in `migrations/20250822_add_store_settings.sql`.
   - Restart your service.

2. **Frontend (optional)**
   - Add the snippet HTML wherever your filters live.
   - Include `ui/assets/js/business-day.js` and `ui/assets/css/business-day.css` in your bundle/build.
   - The UI calls `/api/settings/store-hours` to persist the start time & timezone.

## New/updated endpoints
- `GET  /api/settings/store-hours` — get saved `business_day_start` and `timezone`.
- `POST /api/settings/store-hours` — save settings:
  ```json
  {"business_day_start": "20:00", "timezone": "Asia/Almaty"}
  ```
- `GET  /api/orders/summary` — respects business-day boundaries via params or persisted settings:
  - Query params (override): `start=2025-08-01&end=2025-08-31&business_day_start=20:00&tz=Asia/Almaty`
  - If omitted, backend uses persisted settings (or defaults).

## Defaults
- `business_day_start`: `"20:00"`
- `timezone`: `"Asia/Almaty"`

## Notes
- Utilities work with Python 3.9+ (`zoneinfo`). For 3.8, add dependency `backports.zoneinfo`.
- If your DB stores naive UTC datetimes, treat them as UTC when converting to local time.
- If you already have a settings/config table, you can merge the provided model into it.
