from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    dayofweek,
    weekofyear,
    quarter,
    when,
    lag,
    avg,
    stddev,
)

from pyspark.sql.window import Window


# =========================================================
# TEMPORAL FEATURES
# =========================================================

def add_temporal_features(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("year", year(col("DATE_TS")))
        .withColumn("month", month(col("DATE_TS")))
        .withColumn("day", dayofmonth(col("DATE_TS")))
        .withColumn("day_of_week", dayofweek(col("DATE_TS")))
        .withColumn("week_of_year", weekofyear(col("DATE_TS")))
        .withColumn("quarter", quarter(col("DATE_TS")))
        .withColumn(
            "season",
            when(col("month").isin(12, 1, 2), "winter")
            .when(col("month").isin(3, 4, 5), "spring")
            .when(col("month").isin(6, 7, 8), "summer")
            .otherwise("autumn")
        )
    )


# =========================================================
# WEATHER FEATURES
# =========================================================

def add_weather_features(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("temp_range", col("MAX") - col("MIN"))
        .withColumn("pressure_delta", col("SLP") - col("STP"))
        .withColumn(
            "wind_gust_ratio",
            when(col("WDSP") > 0, col("GUST") / col("WDSP"))
        )
    )


# =========================================================
# EXTREME EVENTS
# =========================================================

def add_extreme_weather_flags(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "is_heatwave",
            when(col("MAX") >= 35, 1).otherwise(0)
        )
        .withColumn(
            "is_heavy_rain",
            when(col("PRCP") >= 20, 1).otherwise(0)
        )
        .withColumn(
            "is_high_wind",
            when(col("MXSPD") >= 40, 1).otherwise(0)
        )
    )


# =========================================================
# TIME SERIES FEATURES
# =========================================================

def add_time_series_features(df: DataFrame) -> DataFrame:

    window_1 = Window.partitionBy("STATION").orderBy("DATE_TS")

    rolling_7 = (
        Window.partitionBy("STATION")
        .orderBy("DATE_TS")
        .rowsBetween(-7, 0)
    )

    return (
        df
        .withColumn(
            "temp_lag_1",
            lag("TEMP", 1).over(window_1)
        )
        .withColumn(
            "temp_lag_7",
            lag("TEMP", 7).over(window_1)
        )
        .withColumn(
            "prcp_lag_3",
            lag("PRCP", 3).over(window_1)
        )
        .withColumn(
            "rolling_avg_temp_7",
            avg("TEMP").over(rolling_7)
        )
        .withColumn(
            "rolling_std_temp_7",
            stddev("TEMP").over(rolling_7)
        )
    )


# =========================================================
# MASTER PIPELINE
# =========================================================

def apply_noaa_features(df: DataFrame) -> DataFrame:

    df = add_temporal_features(df)
    df = add_weather_features(df)
    df = add_extreme_weather_flags(df)

    return df