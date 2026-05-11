from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    to_timestamp,
    hour,
    month,
    dayofweek,
    when,
    lit,
)

# =========================================================
# TIMESTAMP FEATURES
# =========================================================

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    hour,
    month,
    dayofweek,
    when,
    lit,
    floor,
    expr
)

# =========================================================
# TIMESTAMP FEATURES (FIXED)
# =========================================================

def add_timestamp_features(df: DataFrame) -> DataFrame:

    return (
        df

        # acq_date is already a timestamp → keep only date part
        .withColumn("base_date", to_timestamp(col("acq_date")))

        # FIX: acq_time is like 24.0, 25.0 → convert to valid hour
        .withColumn("clean_hour", floor(col("acq_time") % 24))

        # rebuild timestamp properly
        .withColumn(
            "event_timestamp",
            expr("timestampadd(HOUR, clean_hour, base_date)")
        )

        .withColumn("event_hour", hour(col("event_timestamp")))
        .withColumn("event_month", month(col("event_timestamp")))
        .withColumn("event_day_of_week", dayofweek(col("event_timestamp")))
    )
# =========================================================
# DAY/NIGHT FEATURES
# =========================================================

def add_daynight_features(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "is_night",
            when(col("daynight") == "N", 1).otherwise(0)
        )
    )


# =========================================================
# FIRE INTENSITY FEATURES
# =========================================================

def add_fire_intensity_features(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "fire_intensity_score",
            col("brightness") * col("frp")
        )
        .withColumn(
            "high_brightness",
            when(col("brightness") >= 350, 1).otherwise(0)
        )
        .withColumn(
            "high_frp",
            when(col("frp") >= 50, 1).otherwise(0)
        )
    )


# =========================================================
# CONFIDENCE FEATURES
# =========================================================

def add_confidence_features(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "confidence_numeric",
            when(col("confidence") == "low", lit(0))
            .when(col("confidence") == "nominal", lit(1))
            .when(col("confidence") == "high", lit(2))
            .otherwise(lit(-1))
        )
    )


# =========================================================
# GEO FEATURES
# =========================================================

def add_geo_features(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "lat_bucket",
            col("latitude").cast("int")
        )
        .withColumn(
            "lon_bucket",
            col("longitude").cast("int")
        )
    )


# =========================================================
# MASTER PIPELINE
# =========================================================

def apply_firms_features(df: DataFrame) -> DataFrame:

    df = add_timestamp_features(df)
    df = add_daynight_features(df)
    df = add_fire_intensity_features(df)
    df = add_confidence_features(df)
    df = add_geo_features(df)

    return df