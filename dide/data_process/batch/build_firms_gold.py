from pyspark.sql import SparkSession

from dide.data_process.features.firms_features import (
    apply_firms_features,

)


spark = SparkSession.builder.getOrCreate()  # type: ignore[attr-defined]

silver_df = spark.read.format("delta").load(
    "data/delta/firms"
)

gold_df = apply_firms_features(silver_df)

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/delta/gold/firms_features")