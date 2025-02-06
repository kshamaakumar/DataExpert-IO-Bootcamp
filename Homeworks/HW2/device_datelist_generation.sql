INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM user_devices_cumulated
    WHERE date = DATE('2022-12-31')
),
today AS (
    SELECT
        CAST(e.user_id AS TEXT) AS user_id,
        d.browser_type AS browser_type,
        ARRAY_AGG(DATE(CAST(e.event_time AS TIMESTAMP))) AS dates_active
    FROM devices d
    JOIN events e ON d.device_id = e.device_id
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
    AND e.user_id IS NOT NULL
    GROUP BY user_id, d.browser_type
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    CASE
        WHEN y.dates_active IS NULL THEN t.dates_active
        WHEN t.dates_active IS NULL THEN y.dates_active
        ELSE t.dates_active || y.dates_active
    END AS dates_active,
    COALESCE(t.dates_active[1], y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id AND t.browser_type = y.browser_type;