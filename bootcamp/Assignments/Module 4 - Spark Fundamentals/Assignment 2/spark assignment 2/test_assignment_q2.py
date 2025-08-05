import pytest
from datetime import date
from chispa import assert_df_equality
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from ..jobs.assignment_q2_job import generate_game_graph_edges


@pytest.fixture(autouse=True)
def with_empty_game_details(spark):
    schema = StructType([
        StructField("player_id", StringType(), True),
        StructField("game_id", StringType(), True),
        StructField("start_position", StringType(), True),
        StructField("pts", IntegerType(), True),
        StructField("team_id", StringType(), True),
        StructField("team_abbreviation", StringType(), True),
    ])
    empty_df = spark.createDataFrame([], schema)
    empty_df.createOrReplaceTempView("game_details")


def test_generate_game_graph_edges_basic(spark, with_empty_game_details):
    input_data = [
        ("p1", "g1", "F", 12, "T1", "ABC"),
        ("p2", "g2", "G", 25, "T2", "XYZ"),
    ]
    input_df = spark.createDataFrame(input_data, ["player_id", "game_id", "start_position", "pts", "team_id", "team_abbreviation"])

    result_df = generate_game_graph_edges(spark, input_df)

    expected_data = [
        ("p1", "player", "g1", "game", "plays_in", '{"start_position":"F","pts":12,"team_id":"T1","team_abbreviation":"ABC"}'),
        ("p2", "player", "g2", "game", "plays_in", '{"start_position":"G","pts":25,"team_id":"T2","team_abbreviation":"XYZ"}'),
    ]
    expected_schema = StructType([
        StructField("subject_identifier", StringType(), True),
        StructField("subject_type", StringType(), True),
        StructField("object_identifier", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("edge_type", StringType(), True),
        StructField("properties", StringType(), True),
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert_df_equality(result_df, expected_df, ignore_nullable=True)


def test_generate_game_graph_edges_deduplicates(spark, with_empty_game_details):
    input_data = [
        ("p1", "g1", "F", 10, "T1", "ABC"),
        ("p1", "g1", "F", 15, "T1", "ABC"),  # Duplicate player_id + game_id
    ]
    input_df = spark.createDataFrame(input_data, ["player_id", "game_id", "start_position", "pts", "team_id", "team_abbreviation"])

    result_df = generate_game_graph_edges(spark, input_df)

    # Only one row per (player_id, game_id) after deduplication
    expected_data = [
        ("p1", "player", "g1", "game", "plays_in", '{"start_position":"F","pts":10,"team_id":"T1","team_abbreviation":"ABC"}'),
    ]
    expected_schema = StructType([
        StructField("subject_identifier", StringType(), True),
        StructField("subject_type", StringType(), True),
        StructField("object_identifier", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("edge_type", StringType(), True),
        StructField("properties", StringType(), True),
    ])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert_df_equality(result_df, expected_df, ignore_nullable=True)
