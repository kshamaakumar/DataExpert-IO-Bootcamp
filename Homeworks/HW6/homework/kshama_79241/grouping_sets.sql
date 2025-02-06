WITH game_results AS (
    SELECT 
        game_id,
        team_id,
        MAX(CASE WHEN gd.pts IS NOT NULL THEN 1 ELSE 0 END) AS win
    FROM game_details gd
    GROUP BY game_id, team_id
)

SELECT 
    CASE
        WHEN GROUPING(gd.player_name) = 0 AND GROUPING(gd.team_id) = 0 THEN 'player_team'
        WHEN GROUPING(gd.player_name) = 0 AND GROUPING(ps.season) = 0 THEN 'player_season'
        WHEN GROUPING(gd.team_id) = 0 THEN 'team'
    END AS aggregation_level,
    COALESCE(gd.player_name, '(overall)') AS player_name,
    COALESCE(gd.team_id, '(overall)') AS team_id,
    COALESCE(ps.season, '(overall)') AS season,
    SUM(gd.pts) AS total_points,
    SUM(gr.win) AS total_wins
FROM game_details gd
LEFT JOIN game_results gr ON gd.game_id = gr.game_id AND gd.team_id = gr.team_id
LEFT JOIN player_seasons ps ON gd.player_name = ps.player_name
GROUP BY GROUPING SETS (
    (gd.player_name, gd.team_id),
    (gd.player_name, ps.season),
    (gd.team_id)
ORDER BY total_points DESC, total_wins DESC;
