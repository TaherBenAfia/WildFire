from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    from_unixtime, year, month
)
from ..spark_session.spark_session import create_spark_session
from ..schemas.schemas import NOAA_SCHEMA
from ...config.spark.noaa_spark_config import (
    NOAA_SESSION_CONFIG,
    NOAA_STREAM_CONFIG,
    NOAA_SINK_CONFIG,
    NOAA_JARS,
)
from ...data_process.features.noaa_features import apply_noaa_features


def parse_noaa(raw_df):

    return (
        raw_df
        .select(
            from_json(
                col("value").cast("string"),
                NOAA_SCHEMA
            ).alias("data"),

            col("timestamp").alias("kafka_timestamp"),
        )

        .select("data.*", "kafka_timestamp")

        .withColumn(
            "DATE_TS",
            to_timestamp(
                col("DATE"),
                "yyyy-MM-dd'T'HH:mm:ss"
            )
        )

        .withColumn(
            "ingested_at",
            from_unixtime("ingested_at")
            .cast("timestamp")
        )

        .withColumn("year", year(col("DATE_TS")))
        .withColumn("month", month(col("DATE_TS")))

        .filter(col("STATION").isNotNull())
    )

def write_noaa_to_delta(spark: SparkSession):
    """
    Read climate.weather topic → parse → write to Delta Lake.
    Partitioned by year/month/datatype for efficient feature queries.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .options(**NOAA_STREAM_CONFIG)
        .load()
    )

    noaa_df = parse_noaa(raw)
    noaa_df = apply_noaa_features(noaa_df)

    query = (
        noaa_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", NOAA_SINK_CONFIG["checkpoint_path"])
        .option("mergeSchema", "true")
        .partitionBy("year", "month")  # query by measurement type fast
        .trigger(processingTime=NOAA_SINK_CONFIG["trigger_interval"])
        .start(NOAA_SINK_CONFIG["output_path"])
    )

    return query


def run():
    spark = create_spark_session(
        app_name=NOAA_SESSION_CONFIG["app_name"],
        session_config=NOAA_SESSION_CONFIG,
        jars=NOAA_JARS,
    )

    print("[NOAA] Starting stream → Delta Lake...")
    query = write_noaa_to_delta(spark)
    query.awaitTermination()


if __name__ == "__main__":
    run()