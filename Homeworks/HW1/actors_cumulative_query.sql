WITH last_year AS (
    SELECT
        actor_name,
        actor_id,
        films,
        quality_class
    FROM actors
    WHERE year = 2020
),
this_year AS (
    SELECT
        actorid,
        actor,
        year,
        array_agg(row(film, rating, votes, filmid)::films) AS films,
        CASE
            WHEN AVG(rating) > 8 THEN 'star'
            WHEN AVG(rating) > 7 AND AVG(rating) <= 8 THEN 'good'
            WHEN AVG(rating) > 6 AND AVG(rating) <= 7 THEN 'average'
            WHEN AVG(rating) <= 6 THEN 'bad'
        END::quality_class AS quality_class
    FROM actor_films
    WHERE year = 2021
    GROUP BY actorid, actor, year
)
INSERT INTO actors
SELECT
    COALESCE(ly.actor_name, ty.actor) AS actor_name,
    COALESCE(ly.actor_id, ty.actorid) AS actor_id,
    COALESCE(ly.films, ARRAY[]::films[]) || COALESCE(ty.films, ARRAY[]::films[]) AS films,
    COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
    ty.year IS NOT NULL AS is_active,
    2021 AS year
FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actor_id = ty.actorid;