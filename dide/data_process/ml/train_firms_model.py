from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
spark = SparkSession.builder.getOrCreate() # type: ignore[attr-defined]

df = spark.read.format("delta").load(
    "data/delta/gold/firms_features"
)
feature_cols = [
    "brightness",
    "confidence_score",
    "latitude",
    "longitude",
    "is_day",
    "month",
    "year",
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