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
grouped_game_details AS (
	SELECT
		COALESCE(gd.player_name, '(overall)') AS player_name,
		COALESCE(gd.team_abbreviation, '(overall)') AS team_abbreviation,
		COALESCE(g.season::TEXT, '(overall)') AS season,
		COALESCE(SUM(pts), 0) as pts
	
	FROM game_details AS gd
	
	JOIN games AS g
		on g.game_id = gd.game_id
	
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
```
      
- A query that uses window functions on `game_details` to find out the following things:
  - What is the most games a team has won in a 90 game stretch? 
  - How many games in a row did LeBron James score over 10 points a game?


Please add these queries into a folder `homework/<discord-username>`