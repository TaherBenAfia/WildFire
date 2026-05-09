
from .firms_stream import write_firms_to_delta
from .noaa_stream  import write_noaa_to_delta
from ..spark_session.spark_session import create_spark_session
from ...config.spark.firms_spark_config import FIRMS_SESSION_CONFIG, FIRMS_JARS
from ...config.spark.noaa_spark_config  import NOAA_SESSION_CONFIG,  NOAA_JARS


# reduce Spark logging noise
def run():
    # one shared Spark session — more efficient than two separate sessions
    spark = create_spark_session(
        app_name="WildFire-AllStreams",
        session_config={
            "master":              "local[*]",
            "driver_memory":       "3g",
            "executor_memory":     "3g",
            "shuffle_partitions":  "4",
        },
        jars=FIRMS_JARS,   # FIRMS jars include everything NOAA needs too
    )

    print("[ALL] Starting all streams...")

    firms_query = write_firms_to_delta(spark)
    noaa_query  = write_noaa_to_delta(spark)

    print("[ALL] Both streams running.")
    print(f"      FIRMS  → {firms_query.status}")
    print(f"      NOAA   → {noaa_query.status}")

    # wait for either stream to terminate (or Ctrl+C)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()