from pyspark.sql import SparkSession


# Original Postgres query
"""
WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
)
SELECT
    player_id AS subject_identifier,
    'player'::vertex_type as subject_type,
    game_id AS object_identifier,
    'game'::vertex_type AS object_type,
    'plays_in'::edge_type AS edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
        ) as properties
FROM deduped
WHERE row_num = 1;
"""

query = """
WITH deduped AS (
    SELECT *, row_number() OVER (PARTITION BY player_id, game_id ORDER BY player_id) AS row_num
    FROM game_details
)
SELECT
    player_id AS subject_identifier,
    'player' AS subject_type,
    game_id AS object_identifier,
    'game' AS object_type,
    'plays_in' AS edge_type,
    to_json(named_struct(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    )) AS properties
FROM deduped
WHERE row_num = 1
"""

def generate_game_graph_edges(spark, dataframe):
    dataframe.createOrReplaceTempView("game_details")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("generate_game_graph_edges") \
      .getOrCreate()
      
    # Replace with actual DataFrame registration or reading
    input_df = spark.table("game_details")
    
    output_df = generate_game_graph_edges(spark, input_df)
    
    # Replace with your desired output table or sink
    output_df.write.mode("overwrite").insertInto("player_game_edges")