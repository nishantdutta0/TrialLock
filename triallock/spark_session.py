import os
from pyspark.sql import SparkSession

def get_spark(session_app_name="TrialLock"):
    builder = SparkSession.builder \
        .appName(session_app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "1g")
    # Low-resource settings
    builder = builder \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.default.parallelism", "2")
    # Disable web UI to save resources
    builder = builder.config("spark.ui.enabled", "false")
    try:
        spark = builder.getOrCreate()
        return spark
    except Exception as exc:
        # Return None; UI will report Spark unavailable
        return None
