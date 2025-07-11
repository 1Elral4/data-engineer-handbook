# Dimensional Data Modeling - Week 1

This week's assignment involves working with the `actor_films` dataset. Your task is to construct a series of SQL queries and table definitions that will allow us to model the actor_films dataset in a way that facilitates efficient analysis. This involves creating new tables, defining data types, and writing queries to populate these tables with data from the actor_films dataset

## Dataset Overview
The `actor_films` dataset contains the following fields:

- `actor`: The name of the actor.
- `actorid`: A unique identifier for each actor.
- `film`: The name of the film.
- `year`: The year the film was released.
- `votes`: The number of votes the film received.
- `rating`: The rating of the film.
- `filmid`: A unique identifier for each film.

The primary key for this dataset is (`actor_id`, `film_id`).

## Assignment Tasks

1. **DDL for `actors` table:** Create a DDL for an `actors` table with the following fields:
    - `films`: An array of `struct` with the following fields:
		- film: The name of the film.
		- votes: The number of votes the film received.
		- rating: The rating of the film.
		- filmid: A unique identifier for each film.

    - `quality_class`: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:
		- `star`: Average rating > 8.
		- `good`: Average rating > 7 and ≤ 8.
		- `average`: Average rating > 6 and ≤ 7.
		- `bad`: Average rating ≤ 6.
    - `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).

```sql
CREATE TYPE film_info AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	film_id TEXT
);

CREATE TYPE quality_class AS ENUM(
	'star', 'good', 'average', 'bad'
);


CREATE TABLE actors (
	actor_id TEXT,
	actor_name TEXT,
	year INTEGER,
	films film_info[],
	quality_class quality_class,
	is_active BOOLEAN

);
```
    
2. **Cumulative table generation query:** Write a query that populates the `actors` table one year at a time.

```sql


-- year: min 1970, max 2021

DO $$
DECLARE
  yr INT := 1970;  -- starting current_year
BEGIN
  WHILE yr <= 2021 LOOP
    INSERT INTO actors (
      actor_id,
      actor_name,
      year,
      films,
      quality_class,
      is_active
    )

WITH
last_year AS (
	SELECT * FROM actors
	WHERE year = yr - 1
),
current_year AS (
	SELECT
    actorid,
    actor,
    ARRAY_AGG(ROW(film, votes, rating, filmid)::film_info) AS films,
    AVG(rating) AS avg_rating,
	year
  FROM actor_films
  WHERE year = yr
  GROUP BY actorid, actor, year
)

SELECT

	COALESCE(ly.actor_id, cy.actorid) AS actor_id,
	COALESCE(ly.actor_name, cy.actor) AS actor_name,
	yr AS year,

	COALESCE(ly.films, ARRAY[]::film_info[]) || 
		CASE WHEN cy.year IS NOT NULL THEN cy.films 
		END 
	AS films,
	
 	CASE 
		WHEN cy.year IS NOT NULL THEN (
			CASE
			    WHEN cy.avg_rating > 8 THEN 'star'
			    WHEN cy.avg_rating > 7 THEN 'good'
			    WHEN cy.avg_rating > 6 THEN 'average'
			    ELSE 'bad'
			END::quality_class
		)
		ELSE ly.quality_class
  	END AS quality_class,
	
	cy.year IS NOT NULL AS is_active

	

FROM last_year AS ly
FULL OUTER JOIN current_year AS cy
	ON cy.actorid = ly.actor_id;



    yr := yr + 1;  -- increment year
  END LOOP;
END $$;

```
    
3. **DDL for `actors_history_scd` table:** Create a DDL for an `actors_history_scd` table with the following features:
    - Implements type 2 dimension modeling (i.e., includes `start_date` and `end_date` fields).
    - Tracks `quality_class` and `is_active` status for each actor in the `actors` table.

```sql
CREATE TABLE actors_history_scd (
	actor_id TEXT,
	actor_name TEXT,
	quality_class quality_class,
	is_active BOOL,
	startdate DATE,
	endate DATE
);
```
      
4. **Backfill query for `actors_history_scd`:** Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.
    
5. **Incremental query for `actors_history_scd`:** Write an "incremental" query that combines the previous year's SCD data with new incoming data from the `actors` table.
