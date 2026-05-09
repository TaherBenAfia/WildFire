from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp,
    from_unixtime, current_timestamp,
    year, month, dayofmonth
)
from ..spark_session.spark_session import create_spark_session
from ..schemas.schemas import FIRMS_SCHEMA
from ...config.spark.firms_spark_config import (
    FIRMS_SESSION_CONFIG,
    FIRMS_STREAM_CONFIG,
    FIRMS_SINK_CONFIG,
    FIRMS_JARS,
)
from ...data_process.features.firms_features import (
apply_firms_features,
)
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    from_unixtime,
    year,
    month,
    dayofmonth,
)

def parse_firms(raw_df):

    return (
        raw_df

        .select(
            from_json(
                col("value").cast("string"),
                FIRMS_SCHEMA
            ).alias("data"),

            col("timestamp").alias("kafka_timestamp"),
        )

        .select("data.*", "kafka_timestamp")

        # normalize timestamp
        .withColumn(
            "acq_date",
            to_timestamp(
                col("acq_date"),
                "yyyy-MM-dd"
            )
        )

        # convert unix epoch
        .withColumn(
            "ingested_at",
            from_unixtime("ingested_at")
            .cast("timestamp")
        )

        # partition columns
        .withColumn("year", year(col("acq_date")))
        .withColumn("month", month(col("acq_date")))
        .withColumn("day", dayofmonth(col("acq_date")))

        # validation
        .filter(col("latitude").isNotNull())
        .filter(col("longitude").isNotNull())
        .filter(col("acq_date").isNotNull())
    )

def write_firms_to_delta(spark: SparkSession):
    """
    Read climate.fires topic → parse → write to Delta Lake.
    Partitioned by year/month/day for efficient time-range queries.
    """
    # ── read raw stream from Kafka ────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .options(**FIRMS_STREAM_CONFIG)
        .load()
    )

    # ── parse and enrich ──────────────────────────────────────────────────────
    firms_df = parse_firms(raw)
    firms_df = apply_firms_features(firms_df)


    # ── write to Delta Lake ───────────────────────────────────────────────────
    query = (
        firms_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", FIRMS_SINK_CONFIG["checkpoint_path"])
        .option("mergeSchema", "true")        # handle schema evolution gracefully
        .partitionBy("year", "month", "day")  # efficient time-range reads later
        .trigger(processingTime=FIRMS_SINK_CONFIG["trigger_interval"])
        .start(FIRMS_SINK_CONFIG["output_path"])
    )

    return query


def run():
    spark = create_spark_session(
        app_name=FIRMS_SESSION_CONFIG["app_name"],
        session_config=FIRMS_SESSION_CONFIG,
        jars=FIRMS_JARS,
    )

    print("[FIRMS] Starting stream → Delta Lake...")
    query = write_firms_to_delta(spark)

    # log progress every trigger interval
    query.awaitTermination()


if __name__ == "__main__":
    run()