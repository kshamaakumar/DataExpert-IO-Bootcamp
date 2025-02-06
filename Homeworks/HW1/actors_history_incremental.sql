WITH last_year_scd AS(
    SELECT * FROM actors_history_scd
             WHERE current_year = 2020
             AND end_year = 2020
),
    historical_scd AS(
        SELECT actor_name,
               quality_class,
               is_active,
               start_year,
               end_year
            FROM actors_history_scd
             WHERE current_year = 2020
             AND end_year < 2020
    ),
    this_year_data AS(
        SELECT * FROM actors
             WHERE year = 2021
    ),
    unchanged_records AS (
        SELECT tyd.actor_name,
               tyd.quality_class,
               tyd.is_active,
               lys.start_year,
               tyd.year as end_year
        FROM last_year_scd lys
        JOIN this_year_data tyd ON lys.actor_name = tyd.actor_name
        WHERE tyd.quality_class = lys.quality_class
        AND tyd.is_active = lys.is_active
    ),
    changed_records AS (
        SELECT tyd.actor_name,
               unnest(
                       ARRAY [
                           ROW (
                               lys.quality_class,
                               lys.is_active,
                               lys.start_year,
                               lys.end_year
                               )::SCD_TYPE,
                           ROW (
                               tyd.quality_class,
                               tyd.is_active,
                               tyd.year,
                               tyd.year
                               )::SCD_TYPE
                           ]
               ) AS records
        FROM last_year_scd lys
        LEFT JOIN this_year_data tyd ON lys.actor_name = tyd.actor_name
        WHERE (tyd.quality_class <> lys.quality_class
        OR tyd.is_active <> lys.is_active)
    ),
    unnested_change_records AS(
        SELECT actor_name,
               (records::scd_type).quality_class,
               (records::scd_type).is_active,
               (records::scd_type).start_year,
               (records::scd_type).end_year
        FROM changed_records
    ),
    new_records AS (
        SELECT tyd.actor_name,
               tyd.quality_class,
               tyd.is_active,
               tyd.year AS start_year,
               tyd.year AS end_year
        FROM this_year_data tyd
        LEFT JOIN last_year_scd lys ON tyd.actor_name = lys.actor_name
        WHERE lys.actor_name IS NULL
    )

SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_change_records
UNION ALL
SELECT * FROM new_records;