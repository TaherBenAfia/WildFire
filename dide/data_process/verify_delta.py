from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("verify-delta") # type: ignore[attr-defined]
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.2.0"
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
df = spark.read.format("delta").load(
    "data/delta/noaa"
)

df.show()