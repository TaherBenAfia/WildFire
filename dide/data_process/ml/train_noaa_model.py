#Predict MAX temperature
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.getOrCreate() # type: ignore[attr-defined]

df = spark.read.format("delta").load(
    "data/delta/gold/noaa_features"
)
season_indexer = StringIndexer(
    inputCol="season",
    outputCol="season_idx"
)

df = season_indexer.fit(df).transform(df)

feature_cols = [

    # weather
    "TEMP",
    "DEWP",
    "SLP",
    "STP",
    "VISIB",
    "WDSP",
    "MXSPD",
    "PRCP",
    "SNDP",

    # engineered
    "temp_range",
    "pressure_delta",
    "wind_gust_ratio",

    # temporal
    "month",
    "day",
    "day_of_week",
    "week_of_year",
    "quarter",

    # binary events
    "is_heatwave",
    "is_heavy_rain",
    "is_high_wind",
]
target_col = "MAX"

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
    labelCol="MAX",
    numTrees=50
)

trained_model = model.fit(train_df)

predictions = trained_model.transform(test_df)

evaluator = RegressionEvaluator(
    labelCol="MAX",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(predictions)

print("RMSE:", rmse)

trained_model.write().overwrite().save(
    "models/noaa_rf"
)
predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .save("data/delta/predictions/noaa")