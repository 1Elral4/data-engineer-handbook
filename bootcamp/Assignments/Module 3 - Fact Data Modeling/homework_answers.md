# Week 2 Fact Data Modeling
The homework this week will be using the `devices` and `events` dataset

Construct the following eight queries:

- A query to deduplicate `game_details` from Day 1 so there's no duplicates

```sql
 SELECT 
 	g.game_date_est,
	g.season,
	g.home_team_id,
	gd.*,
	ROW_NUMBER() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY gd.game_id) AS row_num
FROM game_details gd
JOIN games g on gd.game_id = g.game_id
```

- A DDL for an `user_devices_cumulated` table that has:
  - a `device_activity_datelist` which tracks a users active days by `browser_type`
  - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
    - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)

```sql
CREATE TYPE browser_types AS ENUM (
	'Spider_Bot',
    'LinkedInBot',
    'ZoominfoBot',
    '3+bottle',
    'Maxthon',
    'Applebot',
    'Yeti',
    'DataForSeoBot',
    'SeekportBot',
    'Facebook'
)

CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_type browser_types,
    activity_dates DATE[],
	  PRIMARY KEY(user_id)
);
```

Another approach:

```sql

CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    browser_activity JSONB,
    PRIMARY KEY(user_id)
);

```

- A cumulative query to generate `device_activity_datelist` from `events`

- A `datelist_int` generation query. Convert the `device_activity_datelist` column into a `datelist_int` column 

- A DDL for `hosts_cumulated` table 
  - a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
  
- The incremental query to generate `host_activity_datelist`

- A monthly, reduced fact table DDL `host_activity_reduced`
   - month
   - host
   - hit_array - think COUNT(1)
   - unique_visitors array -  think COUNT(DISTINCT user_id)

- An incremental query that loads `host_activity_reduced`
  - day-by-day

Please add these queries into a folder, zip them up and submit [here](https://bootcamp.techcreator.io)