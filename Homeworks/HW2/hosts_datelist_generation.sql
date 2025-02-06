INSERT INTO hosts_cumulated
WITH yesterday AS (
    SELECT
        *
    FROM hosts_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT
        CAST(e.host AS TEXT) AS host,
        ARRAY_AGG(DATE(CAST(e.event_time AS TIMESTAMP))) AS host_activity
    FROM events e
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
    AND e.host IS NOT NULL
    GROUP BY host
)
SELECT
    COALESCE(t.host, y.host) AS host,
    CASE
        WHEN y.host_activity IS NULL THEN t.host_activity
        WHEN t.host_activity IS NULL THEN y.host_activity
        ELSE t.host_activity || y.host_activity
    END AS host_activity,
    COALESCE(t.host_activity[1], y.date + INTERVAL '1 day') AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.host = y.host;