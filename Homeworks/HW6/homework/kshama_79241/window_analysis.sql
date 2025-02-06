WITH game_results AS (
    SELECT 
        game_id,
        team_id,
        MAX(CASE WHEN gd.pts IS NOT NULL THEN 1 ELSE 0 END) AS win
    FROM game_details gd
    GROUP BY game_id, team_id
),
team_wins AS (
    SELECT 
        gr.team_id,
        gd.game_id,
        ROW_NUMBER() OVER (PARTITION BY gr.team_id ORDER BY gd.game_id) AS game_number,
        SUM(gr.win) OVER (
            PARTITION BY gr.team_id 
            ORDER BY gd.game_id 
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_last_90_games
    FROM game_details gd
    JOIN game_results gr ON gd.game_id = gr.game_id AND gd.team_id = gr.team_id
),
lebron_streaks AS (
    SELECT 
        gd.player_name,
        gd.game_id,
        gd.pts AS points,
        CASE 
            WHEN gd.pts > 10 THEN 1 
            ELSE 0 
        END AS over_10_points,
        SUM(CASE WHEN gd.pts <= 10 THEN 1 ELSE 0 END) OVER (
            PARTITION BY gd.player_name 
            ORDER BY gd.game_id
        ) AS streak_group
    FROM game_details gd
    WHERE gd.player_name = 'LeBron James'
)
SELECT 
    (SELECT MAX(wins_last_90_games) FROM team_wins) AS max_wins_in_90_games,
    (SELECT MAX(COUNT(*)) 
     FROM lebron_streaks 
     WHERE over_10_points = 1 
     GROUP BY streak_group) AS longest_lebron_10pt_streak;
