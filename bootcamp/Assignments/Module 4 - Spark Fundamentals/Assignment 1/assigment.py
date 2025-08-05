#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


# In[2]:


# start the spark session
spark = (SparkSession
         .builder
         .appName("Assignment")
         .getOrCreate())


# In[3]:


# disable automatic broadcast join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

# Align Shuffle Partitions to Bucket Count
spark.conf.set("spark.sql.shuffle.partitions", "16")


# In[4]:


# tell the location of each .csv file
dir_matches = "/home/iceberg/data/matches.csv"
dir_match_details = "/home/iceberg/data/match_details.csv"
dir_medals_matches_players = "/home/iceberg/data/medals_matches_players.csv"
dir_medals = "/home/iceberg/data/medals.csv"
dir_maps = "/home/iceberg/data/maps.csv"

# read csv, create dataframes
matches_df = spark.read.csv(dir_matches, header=True, inferSchema=True)
match_details_df = spark.read.csv(dir_match_details, header=True, inferSchema=True)
medals_matches_players_df = spark.read.csv(dir_medals_matches_players, header=True, inferSchema=True)
medals_df = spark.read.csv(dir_medals, header=True, inferSchema=True)
maps_df = spark.read.csv(dir_maps, header=True, inferSchema=True)




# In[5]:


dfs = [matches_df, match_details_df, medals_matches_players_df, medals_df, maps_df]


# In[6]:


def descriptor(df_list):
    for df in df_list:
        df.printSchema()
        print(f"Rows: {df.count()}, Columns: {len(df.columns)}")
        df.show(5)


# In[7]:


# some exploration on the data
descriptor(dfs)


# In[8]:


# explicitly perform a broadcast join
result_df = medals_df.join(broadcast(maps_df))
result_df.show(5)


# In[10]:


matches_df.select('match_id', 'is_team_game', 'playlist_id', 'completion_date').count()


# ## Performing a bucket join

# To perform a bucket join there are steps to follow, in preparation for it:
# 1) DDL the tables
# 2) Repartition the data on different buckets, < 16
# 3) Write the repartitioned data into your tables
# 4) Load the data from the tables into dataframes
# 5) Perform the bucket join of your tables and verify in your execution that there is no shuffling

# In[45]:


spark.sql("""DROP TABLE IF EXISTS bootcamp.matches_bucketed""")

matches_bucketedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.matches_bucketed (
     match_id STRING,
     map_id STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     completion_date TIMESTAMP
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql(matches_bucketedDDL)


# Repartition to improve shuffle handling even on small data
matches_df = matches_df.repartition(16, "match_id")  # match bucket count
# print(matches_df.rdd.getNumPartitions())  # Should now say 16

# write df to a table
# selectExpr solves the issue of renaming here
(matches_df.selectExpr('match_id', 'mapid as map_id', 'is_team_game', 'playlist_id', 'completion_date')
   .write
   .format("iceberg")
   .mode("append")
   .save("bootcamp.matches_bucketed"))



# In[17]:


spark.sql("""DROP TABLE IF EXISTS bootcamp.match_details_bucketed""")

match_detaials_bucketedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.match_details_bucketed (
     match_id STRING,
     player_gamertag STRING,
     player_total_kills INTEGER,
     player_total_deaths INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql(match_detaials_bucketedDDL)


# Repartition to improve shuffle handling even on small data
match_details_df = match_details_df.repartition(16, "match_id")  # match bucket count
# print(matches_df.rdd.getNumPartitions())  # Should now say 16

# write df to a table
(match_details_df.select('match_id', 'player_gamertag', 'player_total_kills', 'player_total_deaths')
   .write
   .format("iceberg")
   .mode("append")
   .save("bootcamp.match_details_bucketed"))


# In[43]:


spark.sql("""DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed""")

medals_matches_playersbucketedDDL = """
CREATE TABLE IF NOT EXISTS bootcamp.medals_matches_players_bucketed (
     match_id STRING,
     player_gamertag STRING,
     medal_id BIGINT,
     medal_count INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql(medals_matches_playersbucketedDDL)

# Repartition to improve shuffle handling even on small data
medals_matches_players_df = medals_matches_players_df.repartition(16, "match_id")  # match bucket count
# print(matches_df.rdd.getNumPartitions())  # Should now say 16

# write df to a table
(medals_matches_players_df.selectExpr('match_id', 'player_gamertag', 'medal_id', 'count as medal_count')
   .write
   .format("iceberg")
   .mode("append")
   .save("bootcamp.medals_matches_players_bucketed"))


# Bucketed joins only work when data is written to disk as a bucketed table, because Spark only optimizes joins on bucketed + saved data.
# Confirm bucket-based joins in the Spark UI → SQL → Physical Plan — it should say SortMergeJoin without shuffle on both sides.
#     
# Spark will skip shuffling if all bucketed tables:
# 
#     Use the same number of buckets,
#     Use the same join key,
#     Are written with the same sort order (optional, but helps).
# 
# 

# In[5]:


# load the bucketed data
df1_bucketed = spark.table("bootcamp.matches_bucketed")
df2_bucketed = spark.table("bootcamp.match_details_bucketed")
df3_bucketed = spark.table("bootcamp.medals_matches_players_bucketed")


# performing the bucket joins, one at time.
# selecting unique columns to avoid duplicates downstream, really important step
df12 = (df1_bucketed.join(df2_bucketed, "match_id")
                    .select(df1_bucketed["*"], 
                            df2_bucketed["player_gamertag"], 
                            df2_bucketed["player_total_kills"],
                            df2_bucketed["player_total_deaths"])
                            
       )

# df12.explain()

df_all = (df12.join(df3_bucketed, (df12.match_id == df3_bucketed.match_id) 
                    & (df12.player_gamertag == df3_bucketed.player_gamertag))
              .select(df12["*"],
                      df3_bucketed["medal_id"],
                      df3_bucketed["medal_count"])
         )
# df_all.explain()

# Look for:
    # SortMergeJoin — which is fine for bucket joins
    # No Exchange or Shuffle steps before the join inputs
    # Optional: InputPartitioning showing HashPartitioning(match_id, 16)

# all is good


# ## Analytical queries

# In[6]:


# register dfs as a temp view, to work with the data for analytical purposes
df_all.createOrReplaceTempView("df_all")
maps_df.createOrReplaceTempView("maps_df")
medals_df.createOrReplaceTempView("medals_df")

# test df_all
dql_1 = """

SELECT
    *
FROM df_all
WHERE
    df_all.match_id = 'a4f925d4-36f8-4d45-ad4d-c2ea2ef7e487'
LIMIT 10

"""


spark.sql(dql_1).show(truncate=False)


# In[33]:


# get the player that averages the most kills per match
dql_2 = """

SELECT
    player_gamertag,
    AVG(player_total_kills) as avg_player_kills
FROM df_all
GROUP BY player_gamertag
ORDER BY avg_player_kills DESC
LIMIT 10

"""


spark.sql(dql_2).show()


# In[7]:


# which playlist gets played the most
dql_3 = """
WITH
unique_matches_playlist AS (
    SELECT
       match_id, 
       playlist_id
    FROM df_all
    GROUP BY match_id, playlist_id

)

SELECT
    ump.playlist_id,
    COUNT(1) as times_played
FROM unique_matches_playlist AS ump
GROUP BY ump.playlist_id
ORDER BY times_played DESC
LIMIT 10

"""


spark.sql(dql_3).show(truncate=False)


# In[12]:


# which map gets played the most
dql_4 = """
WITH
unique_matches_map AS (
    SELECT
       match_id, 
       map_id
    FROM df_all
    GROUP BY match_id, map_id

)

SELECT
    maps_df.name AS map_name,
    COUNT(1) as times_played
FROM unique_matches_map AS umm
LEFT JOIN maps_df -- With this join in here we avoid processing extra data
    ON maps_df.mapid = umm.map_id
GROUP BY maps_df.name
ORDER BY times_played DESC
LIMIT 10

"""


spark.sql(dql_4).show(truncate=False)


# In[42]:


# which map players get the most 'Killing Spree' medal
dql_5 = """
WITH
target_medal_id as (

    SELECT DISTINCT medal_id FROM medals_df where name = 'Killing Spree'
),
test AS (

    SELECT
       maps_df.name AS map_name,
       COUNT(1) as target_medal_count
       
    FROM df_all 
    LEFT JOIN maps_df -- With this join in here we avoid processing extra data
        ON maps_df.mapid = df_all.map_id
    
    WHERE 
        TRUE
        -- AND df_all.match_id = '0000e3cf-727c-491a-9de8-43fe6ea611cc'
        AND df_all.medal_id = (SELECT medal_id FROM target_medal_id)

    GROUP BY map_name
    ORDER BY COUNT(1) DESC

)


SELECT * FROM test LIMIT 10

"""


spark.sql(dql_5).show(truncate=False)


# ## sortWithinPartitions
# to see which sort gets the most compression of data
# It also helps to avoid cost of full sort

# In[7]:


print(df_all.rdd.getNumPartitions())


# In[13]:


df_all.schema


# In[22]:


# create two different sortings

sort_a = df_all.sortWithinPartitions('match_id','map_id')
sort_b = df_all.sortWithinPartitions('playlist_id','map_id')
sort_c = df_all.sortWithinPartitions('map_id','playlist_id')


# ddls for creating tables for writing the data into

# In[15]:


get_ipython().run_cell_magic('sql', '', '\n-- \n    \nCREATE TABLE IF NOT EXISTS bootcamp.all_unsorted (\n    match_id            STRING,\n    map_id              STRING,\n    is_team_game        BOOLEAN,\n    playlist_id         STRING,\n    completion_date     TIMESTAMP,\n    player_gamertag     STRING,\n    player_total_kills  INT,\n    player_total_deaths INT,\n    medal_id            BIGINT,\n    medal_count         INT\n)\nUSING iceberg\nPARTITIONED BY (map_id);\n\n')


# In[16]:


get_ipython().run_cell_magic('sql', '', '\nCREATE TABLE IF NOT EXISTS bootcamp.all_sorted_a (\n    match_id            STRING,\n    map_id              STRING,\n    is_team_game        BOOLEAN,\n    playlist_id         STRING,\n    completion_date     TIMESTAMP,\n    player_gamertag     STRING,\n    player_total_kills  INT,\n    player_total_deaths INT,\n    medal_id            BIGINT,\n    medal_count         INT\n)\nUSING iceberg\nPARTITIONED BY (map_id);\n')


# In[17]:


get_ipython().run_cell_magic('sql', '', '    \nCREATE TABLE IF NOT EXISTS bootcamp.all_sorted_b (\n    match_id            STRING,\n    map_id              STRING,\n    is_team_game        BOOLEAN,\n    playlist_id         STRING,\n    completion_date     TIMESTAMP,\n    player_gamertag     STRING,\n    player_total_kills  INT,\n    player_total_deaths INT,\n    medal_id            BIGINT,\n    medal_count         INT\n)\nUSING iceberg\nPARTITIONED BY (map_id);\n')


# In[23]:


get_ipython().run_cell_magic('sql', '', '    \nCREATE TABLE IF NOT EXISTS bootcamp.all_sorted_c (\n    match_id            STRING,\n    map_id              STRING,\n    is_team_game        BOOLEAN,\n    playlist_id         STRING,\n    completion_date     TIMESTAMP,\n    player_gamertag     STRING,\n    player_total_kills  INT,\n    player_total_deaths INT,\n    medal_id            BIGINT,\n    medal_count         INT\n)\nUSING iceberg\nPARTITIONED BY (map_id);\n')


# Write the data into the tables

# In[24]:


df_all.write.mode('overwrite').saveAsTable('bootcamp.all_unsorted')
sort_a.write.mode('overwrite').saveAsTable('bootcamp.all_sorted_a')
sort_b.write.mode('overwrite').saveAsTable('bootcamp.all_sorted_b')
sort_c.write.mode('overwrite').saveAsTable('bootcamp.all_sorted_c')


# In[25]:


get_ipython().run_cell_magic('sql', '', "\nSELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' \nFROM demo.bootcamp.all_unsorted.files\n\nUNION ALL\nSELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_a' \nFROM demo.bootcamp.all_sorted_a.files\nUNION ALL\nSELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_b' \nFROM demo.bootcamp.all_sorted_b.files\nUNION ALL\nSELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted_c' \nFROM demo.bootcamp.all_sorted_c.files\n")


# sorted_b got us a compression of about 7%
# 

# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




