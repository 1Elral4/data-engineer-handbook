from pyspark.sql import SparkSession


# Original Postgres query
"""
WITH yesterday AS (
    SELECT *
    FROM monthly_user_site_hits
    WHERE date_partition = '2023-03-02'
),
     today AS (
         SELECT user_id,
                DATE_TRUNC('day', event_time) AS today_date,
                COUNT(1) as num_hits
         FROM events
         WHERE DATE_TRUNC('day', event_time) = DATE('2023-03-03')
         AND user_id IS NOT NULL
         GROUP BY user_id, DATE_TRUNC('day', event_time)
     )
INSERT INTO monthly_user_site_hits
SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
       COALESCE(y.hit_array,
           array_fill(NULL::BIGINT, ARRAY[DATE('2023-03-03') - DATE('2023-03-01')]))
        || ARRAY[t.num_hits] AS hits_array,
    DATE('2023-03-01') as month_start,
    CASE WHEN y.first_found_date < t.today_date
        THEN y.first_found_date
        ELSE t.today_date
            END as first_found_date,
    DATE('2023-03-03') AS date_partition
    FROM yesterday y
    FULL OUTER JOIN today t
        ON y.user_id = t.user_id
"""

query = """

WITH yesterday AS (
    SELECT *
    FROM monthly_user_site_hits
    WHERE date_partition = '2023-03-02'
),
today AS (
    SELECT user_id,
           DATE_TRUNC('day', event_time) AS today_date,
           COUNT(1) as num_hits
    FROM events
    WHERE DATE_TRUNC('day', event_time) = '2023-03-03'
      AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time)
)

SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
    COALESCE(y.hit_array, array(NULL, NULL)) || ARRAY(t.num_hits) AS hits_array,
    '2023-03-01' as month_start,
    CAST(
        CASE 
            WHEN y.first_found_date < t.today_date THEN y.first_found_date
            ELSE t.today_date
        END AS DATE
    ) AS first_found_date,
    '2023-03-03' AS date_partition
FROM yesterday y
FULL OUTER JOIN today t
    ON y.user_id = t.user_id

"""

def do_generate_monthly_array_metrics(spark, dataframe):
    dataframe.createOrReplaceTempView("events")
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("assignment_q1") \
      .getOrCreate()
    output_df = do_generate_monthly_array_metrics(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("monthly_array_metrics")