
-- SQL migration for store settings. Adjust types/dialect for your DB if needed (PostgreSQL syntax).
CREATE TABLE IF NOT EXISTS store_settings (
    id SERIAL PRIMARY KEY,
    business_day_start VARCHAR(5) NOT NULL DEFAULT '20:00',
    timezone VARCHAR(64) NOT NULL DEFAULT 'Asia/Almaty'
);
-- If you plan a single-row table, you can insert defaults once:
INSERT INTO store_settings (business_day_start, timezone)
SELECT '20:00', 'Asia/Almaty'
WHERE NOT EXISTS (SELECT 1 FROM store_settings);
