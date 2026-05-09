import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark =( 
    SparkSession.builder
    .appName("nasafirms") # type: ignore[attr-defined] 
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") 
    .getOrCreate()
)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "climate.fires") \
    .option("startingOffsets", "latest") \
    .load()