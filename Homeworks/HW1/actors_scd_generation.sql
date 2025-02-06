INSERT INTO actors_history_scd
WITH with_previous AS (SELECT
                           actor_name,
                           year,
                           quality_class,
                           is_active,
                           LAG(quality_class, 1) OVER (PARTITION BY actor_name ORDER BY year) AS previous_quality_class,
                           LAG(is_active, 1) OVER (PARTITION BY actor_name ORDER BY year) AS previous_is_active
                       FROM actors
                       WHERE year <= 2020),
    with_indicators AS (
                     SELECT *,
                     CASE
                         WHEN quality_class <> previous_quality_class THEN 1
                         WHEN is_active <> previous_is_active THEN 1
                        ELSE 0
                     END AS change_indicator
        FROM with_previous
        ),

    with_streaks AS (SELECT
                         *,
                         SUM(change_indicator) OVER (PARTITION BY actor_name ORDER BY year) AS streak_identifier
                     FROM with_indicators)

SELECT actor_name,
       quality_class,
       is_active,
       MIN(year) AS start_year,
       MAX(year) AS end_year,
       2020 AS current_year
       FROM with_streaks
GROUP BY actor_name,streak_identifier, is_active, quality_class
ORDER BY actor_name, streak_identifier;