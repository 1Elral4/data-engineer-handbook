import pytest
from ..jobs.assignment_q1_job import do_generate_monthly_array_metrics
from chispa import assert_df_equality
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType, DateType
from pyspark.sql.functions import col, to_date
from datetime import date


@pytest.fixture(autouse=True)
def with_empty_monthly_user_site_hits(spark):
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("hit_array", ArrayType(LongType()), True),
        StructField("month_start", DateType(), True),
        StructField("first_found_date", DateType(), True),
        StructField("date_partition", DateType(), True)
    ])
    empty_df = spark.createDataFrame([], schema)
    empty_df.createOrReplaceTempView("monthly_user_site_hits")

def test_do_generate_monthly_array_metrics(spark, with_empty_monthly_user_site_hits):
    input_data = [
        (123, "2023-03-03 10:00:00"),
        (123, "2023-03-03 11:00:00"),
        (456, "2023-03-03 12:00:00")
    ]
    input_df = spark.createDataFrame(input_data, ["user_id", "event_time"])
    input_df = input_df.withColumn("event_time", col("event_time").cast("timestamp"))

    result_df = do_generate_monthly_array_metrics(spark, input_df)

    # Cast dates in result to ensure types match
    result_df = result_df \
        .withColumn("month_start", to_date(col("month_start"))) \
        .withColumn("first_found_date", to_date(col("first_found_date"))) \
        .withColumn("date_partition", to_date(col("date_partition")))

    expected_data = [
        (123, [None, None, 2], date(2023, 3, 1), date(2023, 3, 3), date(2023, 3, 3)),
        (456, [None, None, 1], date(2023, 3, 1), date(2023, 3, 3), date(2023, 3, 3)),
    ]
    expected_schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("hits_array", ArrayType(LongType(), containsNull=True), True),
        StructField("month_start", DateType(), True),
        StructField("first_found_date", DateType(), True),
        StructField("date_partition", DateType(), True),
    ])

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    assert_df_equality(result_df, expected_df, ignore_nullable=True)
