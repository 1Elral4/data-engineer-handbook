# Week 2 Fact Data Modeling
The homework this week will be using the `devices` and `events` dataset

Construct the following eight queries:

- A query to deduplicate `game_details` from Day 1 so there's no duplicates

```sql
WITH deduped AS (
    SELECT 
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY gd.game_id) AS row_num
    FROM game_details gd
    JOIN games g on gd.game_id = g.game_id
)
SELECT * FROM deduped WHERE row_num = 1
```

- A DDL for an `user_devices_cumulated` table that has:
  - a `device_activity_datelist` which tracks a users active days by `browser_type`
  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

```sql

CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type TEXT,
    date DATE,
    activity_dates DATE[],
	  PRIMARY KEY(user_id, browser_type, date)
);
```

Another approach:

```sql

CREATE TABLE user_devices_cumulated_map (
    user_id TEXT,
    browser_activity JSONB,
    PRIMARY KEY(user_id)
);

```

- A cumulative query to generate `device_activity_datelist` from `events`

```sql


-- DROP TABLE user_devices_cumulated;
-- CREATE TABLE user_devices_cumulated (
--     user_id TEXT,
--     browser_type TEXT,
--     date DATE,
--     activity_dates DATE[],
-- 	  PRIMARY KEY(user_id, browser_type, date)
-- );

DO $$
DECLARE
    d DATE;
BEGIN
    FOR d IN SELECT generate_series('2022-12-31'::DATE, '2023-01-30'::DATE, '1 day'::INTERVAL) LOOP

        WITH 
        yesterday AS (
            SELECT *
            FROM user_devices_cumulated
            WHERE date = d
        ),
        
        today AS (
            SELECT
                CAST(e.user_id AS TEXT) AS user_id,
                dvc.browser_type,
                DATE(e.event_time) AS date_active
            FROM events AS e
            JOIN devices AS dvc ON dvc.device_id = e.device_id
            WHERE
                e.user_id IS NOT NULL
                AND DATE(e.event_time) = d + INTERVAL '1 day'
            GROUP BY e.user_id, dvc.browser_type, DATE(e.event_time)
        )

        INSERT INTO user_devices_cumulated (user_id, browser_type, date, activity_dates)
        SELECT
            COALESCE(t.user_id, y.user_id) AS user_id,
            COALESCE(t.browser_type, y.browser_type) AS browser_type,
            COALESCE(t.date_active, y.date + INTERVAL '1 day')::DATE AS date,
            CASE
                WHEN y.activity_dates IS NULL THEN ARRAY[t.date_active]
                WHEN t.date_active IS NULL THEN y.activity_dates
                ELSE ARRAY[t.date_active] || y.activity_dates
            END AS activity_dates
        FROM today AS t
        FULL OUTER JOIN yesterday y
            ON y.user_id = t.user_id AND y.browser_type = t.browser_type;

    END LOOP;
END $$;


```

- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

```sql

WITH

users AS (

	SELECT * 
	FROM user_devices_cumulated
	WHERE date = '2023-01-31'
),

series AS (

	SELECT DATE(generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, '1 day'::INTERVAL)) as series_date
)

SELECT 
	-- create a binary code if in the date the user was active
	CASE 
		-- Does the array activity_dates contain the date series_date?
		WHEN activity_dates @> ARRAY[series_date] 
		THEN POW(2, 32 - (date - series_date))
		ELSE 0 
	END AS activity_bitmask, 
	s.*,
	u.*
FROM users AS u
CROSS JOIN series AS s
ORDER BY user_id


```

- A DDL for `hosts_cumulated` table 
  - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity

```sql
CREATE TABLE host_cumulated (
	host_name TEXT,
	date DATE,
	events_count BIGINT,
	PRIMARY KEY(host_name, date)
);
```
  
- The incremental query to generate `host_activity_datelist`

```sql
DO $$
DECLARE
    d DATE;
BEGIN
    FOR d IN SELECT generate_series('2022-12-31'::DATE, '2023-01-30'::DATE, '1 day'::INTERVAL) LOOP

	WITH
	
	yesterday AS (
	
		SELECT * 
		FROM host_cumulated
		WHERE date = d
	
	),
	
	today AS (
	
		SELECT
			host as host_name,
			date(event_time) as date,
			count(1) events_count
	
		FROM events
		WHERE 
			date(event_time) = d + INTERVAL '1 DAY'
		GROUP BY host, date(event_time)
			
	)
	
	INSERT INTO host_cumulated
	
	SELECT 
		COALESCE(t.host_name, y.host_name) AS host_name,
		COALESCE(t.date, y.date + INTERVAL '1 DAY')::DATE AS date,
	
		CASE
			WHEN t.events_count IS NULL THEN 0
			ELSE t.events_count
		END AS events_count
		
	FROM today AS t
	FULL OUTER JOIN yesterday AS Y
		ON y.host_name = t.host_name;

    END LOOP;
END $$;

-- SELECT * FROM host_cumulated

```

- A monthly, reduced fact table DDL `host_activity_reduced`
   - month
   - host
   - hit_array - think COUNT(1)
   - unique_visitors array -  think COUNT(DISTINCT user_id)

```sql
CREATE TABLE host_activity_reduced (
    month DATE,
    host_name TEXT,
    hit_array INTEGER[],           
    unique_visitors INTEGER[],
    PRIMARY KEY (month, host_name)
);


```

- An incremental query that loads `host_activity_reduced`
  - day-by-day

```sql
-- DROP TABLE host_activity_reduced;
-- CREATE TABLE host_activity_reduced (
--     month DATE,
--     host_name TEXT,
--     hit_array INTEGER[],           
--     unique_visitors INTEGER[],
--     PRIMARY KEY (month, host_name)
-- );

DO $$
DECLARE
    d DATE;
BEGIN
    FOR d IN SELECT generate_series('2022-12-31'::DATE, '2023-01-30'::DATE, '1 day'::INTERVAL) LOOP
    WITH

    yesterday AS (

        SELECT *
        FROM host_activity_reduced
        WHERE month = DATE_TRUNC('month', d::DATE)

    ),

    today AS (

        SELECT
            DATE_TRUNC('month', event_time::DATE)::DATE AS month,
            host as host_name,
            ARRAY[COUNT(1)] AS hits,
            ARRAY[COUNT(DISTINCT user_id)] AS unique_visitors

        FROM events
        WHERE
            TRUE
            AND event_time::DATE = d + INTERVAL '1 DAY'
            AND user_id IS NOT NULL
        GROUP BY month, host_name

    )

    INSERT INTO host_activity_reduced (
        month,
        host_name,
        hit_array,
        unique_visitors
    )
    SELECT
        COALESCE(t.month, y.month) AS month,
        COALESCE(t.host_name, y.host_name) AS host_name,

        CASE
            WHEN y.hit_array IS NULL AND t.hits IS NOT NULL THEN t.hits
            WHEN t.hits IS NULL THEN y.hit_array || ARRAY[0]
            ELSE y.hit_array || t.hits
        END AS hit_array,

        CASE
            WHEN y.unique_visitors IS NULL AND t.unique_visitors IS NOT NULL THEN t.unique_visitors
            WHEN t.unique_visitors IS NULL THEN y.unique_visitors || ARRAY[0]
            ELSE y.unique_visitors || t.unique_visitors
        END AS unique_visitors

    FROM today t
    FULL OUTER JOIN yesterday y
        ON t.host_name = y.host_name

    ON CONFLICT (month, host_name)
    DO UPDATE SET
        hit_array = EXCLUDED.hit_array,
        unique_visitors = EXCLUDED.unique_visitors;
    END LOOP;
END $$;

SELECT *, CARDINALITY(hit_array), CARDINALITY(unique_visitors) FROM host_activity_reduced;

```

Please add these queries into a folder, zip them up and submit [here](https://bootcamp.techcreator.io)