WITH users AS(
    SELECT * FROM user_devices_cumulated
             WHERE date = DATE('2023-01-31')
),
    series AS(
        SELECT *
        FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day')
            AS series_date
    ),
    place_holder_ints AS (SELECT
                              CASE
                                  WHEN
                                      dates_active @> ARRAY [DATE(series_date)]
                                      THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
                                  ELSE 0
                                  END AS datelist_int,
                              *
                          FROM users
                                   CROSS JOIN series)
SELECT user_id,
       browser_type,
       CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32)) AS datelist_int,
       BIT_COUNT(CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active,
       BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) &
            CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active,
      BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) &
            CAST(CAST(SUM(datelist_int) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
FROM place_holder_ints
GROUP BY user_id, browser_type ;