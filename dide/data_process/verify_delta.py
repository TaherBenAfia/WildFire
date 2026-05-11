from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VerifyDelta") #type: ignore
    .master("local[*]")

    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.kafka:kafka-clients:3.5.0"
        ])
    )

    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )

    .getOrCreate()
)

# READ DELTA TABLE
df = (
    spark.readStream
    .format("delta")
    .load("data/delta/firms")
)

query = (
    df.writeStream
    .format("console")
    .outputMode("append")
    .start()
)

query.awaitTermination()