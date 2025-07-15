# Week 4 Applying Analytical Patterns
The homework this week will be using the `players`, `players_scd`, and `player_seasons` tables from week 1

- A query that does state change tracking for `players`
  - A player entering the league should be `New`
  - A player leaving the league should be `Retired`
  - A player staying in the league should be `Continued Playing`
  - A player that comes out of retirement should be `Returned from Retirement`
  - A player that stays out of the league should be `Stayed Retired`
  
```sql
WITH 
players_indicators AS (

	SELECT
		player_name,
		current_season,
		years_since_last_active,
		LAG(years_since_last_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) as lag_y,
		ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY current_season) player_row_num,
		is_active
	
	FROM players

)

SELECT
	player_name,
	current_season,
	CASE 
		WHEN player_row_num = 1 THEN 'New'
		WHEN years_since_last_active = 1 THEN 'Retired'
		WHEN years_since_last_active = 0 AND lag_y >= 1 THEN 'Returned from Retirement'
		WHEN (years_since_last_active - lag_y) = 0 THEN 'Continued Playing'
		ELSE 'Stayed Retired'
	END AS player_status


FROM players_indicators AS pi

-- WHERE 
-- TRUE
-- -- and player_name = 'Aaron Brooks'
-- -- draft_year::INT < current_season::INT
-- -- and current_season = 2007
-- -- AND draft_year <> 'Undrafted'
-- -- AND draft_year = '2007'
-- ORDER BY player_name desc, current_season
-- LIMIT 100



```

- A query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data
  - Aggregate this dataset along the following dimensions
    - player and team
      - Answer questions like who scored the most points playing for one team?
    - player and season
      - Answer questions like who scored the most points in one season?
    - team
      - Answer questions like which team has won the most games?

```sql
WITH
games_details_deduped AS (

	SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY game_id, player_name) AS row_num
	FROM game_details
),


grouped_game_details AS (
	SELECT
		COALESCE(gd.player_name, '(overall)') AS player_name,
		COALESCE(gd.team_abbreviation, '(overall)') AS team_abbreviation,
		COALESCE(g.season::TEXT, '(overall)') AS season,
		COALESCE(SUM(gd.pts), 0) as pts,
		COALESCE(SUM(
					CASE
					  WHEN gd.team_id = g.home_team_id AND g.pts_home > g.pts_away THEN 1
					  WHEN gd.team_id = g.team_id_away AND g.pts_away > g.pts_home THEN 1
					  ELSE 0
					END)
		,0) as player_wins,
				
		COALESCE(COUNT(DISTINCT
			CASE
			  WHEN gd.team_id = g.home_team_id AND g.pts_home > g.pts_away THEN g.game_id
			  WHEN gd.team_id = g.team_id_away AND g.pts_away > g.pts_home THEN g.game_id
			  ELSE NULL
			END)
		,0) as team_wins
	
	FROM games_details_deduped AS gd
	
	JOIN games AS g
		on g.game_id = gd.game_id
		
	WHERE 
		TRUE
		AND row_num = 1 
		-- and g.game_id = 22000692 -- for testing

	GROUP BY GROUPING SETS (
		(gd.player_name, gd.team_abbreviation),
		(gd.player_name, g.season),
	 	(gd.team_abbreviation)
	)
)



(
SELECT *, 'most points ever by a player' AS description FROM grouped_game_details
WHERE 
	TRUE
	AND season = '(overall)'
	AND player_name <> '(overall)'

ORDER BY 4 DESC
LIMIT 1
)
UNION ALL
(
SELECT *, 'most points in a season by a player' AS description FROM grouped_game_details
WHERE 
	TRUE
	AND season <> '(overall)'
	AND player_name <> '(overall)'


ORDER BY 4 DESC
LIMIT 1
)
UNION ALL
(
SELECT *, 'most wins by a team' AS description FROM grouped_game_details
WHERE 
	TRUE
	AND season = '(overall)'
	AND player_name = '(overall)'


ORDER BY 4 DESC
LIMIT 1
)


```
      
- A query that uses window functions on `game_details` to find out the following things:
  - What is the most games a team has won in a 90 game stretch? 



  - How many games in a row did LeBron James score over 10 points a game?

```sql
-- Analysis of LeBron James' consecutive games scoring 10+ points
-- Uses "islands and gaps" technique to identify scoring streaks

WITH 
-- Remove duplicate records for same game/player combinations
games_details_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER(PARTITION BY game_id, player_name) AS row_num
    FROM game_details
),

-- Prepare base dataset with game sequences and win/loss determination
data_setup AS (
    SELECT
        DENSE_RANK() OVER(PARTITION BY gd.team_abbreviation ORDER BY g.game_date_est) AS team_games_sequence,
        CASE
            WHEN gd.team_id = g.home_team_id AND g.pts_home > g.pts_away THEN 1
            WHEN gd.team_id = g.team_id_away AND g.pts_away > g.pts_home THEN 1
            ELSE 0
        END AS is_win,
        *
    FROM games_details_deduped AS gd
    JOIN games AS g ON g.game_id = gd.game_id
    WHERE row_num = 1  -- Only keep first occurrence of each game/player
),

-- Filter for LeBron James games and create chronological sequence
lebron_games AS (
    SELECT
        game_date_est,
        team_abbreviation,
        COALESCE(pts, 0) AS pts,  -- Handle null points as 0
        DENSE_RANK() OVER(ORDER BY game_date_est) AS lj_games_sequence
    FROM data_setup
    WHERE player_name = 'LeBron James'
    ORDER BY game_date_est
),

-- Group consecutive games with 10+ points using islands and gaps technique
lebron_streak_groups AS (
    SELECT
        -- Key formula: consecutive games scoring 10+ will have same group_id
        lj_games_sequence - ROW_NUMBER() OVER(ORDER BY game_date_est) AS group_id
    FROM lebron_games
    WHERE pts > 10  -- Only games with more than 10 points
),

-- Calculate the length of each streak
streaks AS (
    SELECT
        group_id,
        COUNT(*) AS streak_length
    FROM lebron_streak_groups
    GROUP BY group_id
)

-- Final output: distribution of streak lengths
SELECT
    streak_length,
    COUNT(*) AS num_occurrences
FROM streaks
GROUP BY streak_length
ORDER BY streak_length;


```


Please add these queries into a folder `homework/<discord-username>`