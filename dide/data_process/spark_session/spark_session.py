import os
from pyspark.sql import SparkSession

def create_spark_session(app_name: str, session_config: dict, jars: str) -> SparkSession:

    os.environ.setdefault("JAVA_TOOL_OPTIONS", " ".join([
        "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.security=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    ]))

    spark = (
        SparkSession.builder
        .appName(app_name)  # type: ignore
        .master(session_config["master"])
        .config("spark.jars.packages", jars)
        .config("spark.driver.memory", session_config["driver_memory"])
        .config("spark.executor.memory", session_config["executor_memory"])
        .config("spark.sql.shuffle.partitions", session_config["shuffle_partitions"])
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    return spark