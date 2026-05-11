from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import when, col, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
spark = (
    SparkSession.builder
    .appName("Train_model") #type: ignore
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


try : 
    df = spark.read.format("delta").load(
        "data/delta/firms"
    )
except Exception as e:
    print(f"Error loading data: {e}")
    df = spark.read.format("delta").load(
        "data/delta/gold/firms_features"
    )
def add_confidence_features(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "confidence_numeric",
            when(col("confidence") == "l", lit(0))
            .when(col("confidence") == "n", lit(1))
            .when(col("confidence") == "h", lit(2))
            .otherwise(lit(-1))
        )
    )

df = add_confidence_features(df)
feature_cols = [
    "brightness",
    "confidence_numeric",
    "latitude",
    "longitude",
    "is_night",
    "month",
    "year",
    "event_hour",
]
target_col = "frp"
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features"
)

ml_df = assembler.transform(df)
train_df, test_df = ml_df.randomSplit(
    [0.8, 0.2],
    seed=42
)
model = RandomForestRegressor(
    featuresCol="features",
    labelCol=target_col,
    numTrees=50
)

trained_model = model.fit(train_df)
predictions = trained_model.transform(test_df)

predictions.select(
    "prediction",
    "frp"
).show()
evaluator = RegressionEvaluator(
    labelCol="frp",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(predictions)

print("RMSE:", rmse)
trained_model.write().overwrite().save(
    "models/firms_rf"
)
predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/delta/predictions/firms")