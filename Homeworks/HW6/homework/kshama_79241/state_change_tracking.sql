WITH previous_season AS (
    SELECT 
        player_name, 
        MAX(season) AS last_season
    FROM player_seasons
    GROUP BY player_name
),
current_season AS (
    SELECT 
        player_name, 
        current_season
    FROM players
),
player_state AS (
    SELECT 
        COALESCE(cs.player_name, ps.player_name) AS player_name,
        COALESCE(cs.current_season, ps.last_season + 1) AS season_year,
        CASE
            WHEN ps.player_name IS NULL THEN 'New'  -- Player entering the league
            WHEN cs.player_name IS NULL AND ps.last_season IS NOT NULL THEN 'Retired'  -- Player left the league
            WHEN ps.last_season = cs.current_season - 1 THEN 'Continued Playing'  -- Active player
            WHEN ps.last_season < cs.current_season - 1 THEN 'Returned from Retirement'  -- Came back after retirement
            WHEN ps.last_season IS NOT NULL AND cs.player_name IS NULL THEN 'Stayed Retired'  -- Stayed out of the league
            ELSE 'Unknown'
        END AS player_status
    FROM previous_season ps
    FULL OUTER JOIN current_season cs
    ON ps.player_name = cs.player_name
)
SELECT * FROM player_state
ORDER BY season_year DESC, player_name;
