#SCHEMAS
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType, DoubleType
)

FIRMS_SCHEMA = StructType([
    StructField("latitude",    FloatType(),  True),
    StructField("longitude",   FloatType(),  True),
    StructField("brightness",  FloatType(),  True),
    StructField("frp",         FloatType(),  True),
    StructField("confidence",  StringType(), True),
    StructField("acq_date",    StringType(), True),
    StructField("acq_time",    StringType(), True),
    StructField("satellite",   StringType(), True),
    StructField("instrument",  StringType(), True),
    StructField("daynight",    StringType(), True),
    StructField("ingested_at", DoubleType(), True),
])

NOAA_SCHEMA = StructType([
    StructField("STATION", StringType(), True),
    StructField("DATE", StringType(), True),

    StructField("DEWP", FloatType(), True),
    StructField("FRSHTT", StringType(), True),
    StructField("GUST", FloatType(), True),

    StructField("MAX", FloatType(), True),
    StructField("MIN", FloatType(), True),

    StructField("MXSPD", FloatType(), True),
    StructField("PRCP", FloatType(), True),

    StructField("SLP", FloatType(), True),
    StructField("SNDP", FloatType(), True),
    StructField("STP", FloatType(), True),

    StructField("TEMP", FloatType(), True),
    StructField("VISIB", FloatType(), True),
    StructField("WDSP", FloatType(), True),

    StructField("ingested_at", DoubleType(), True),
])