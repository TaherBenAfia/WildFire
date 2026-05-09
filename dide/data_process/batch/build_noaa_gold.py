from pyspark.sql import SparkSession

from dide.data_process.features.noaa_features import (
    apply_noaa_features
)

spark = SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]

silver_df = spark.read.format("delta").load(
    "data/delta/noaa"
)

gold_df = apply_noaa_features(silver_df)

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/delta/gold/noaa_features")