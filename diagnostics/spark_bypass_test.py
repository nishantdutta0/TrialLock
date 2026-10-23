"""
Test bypassing Hadoop NativeIO on Windows.
"""
import sys
import os
from pathlib import Path

# Add triallock package to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Ensure HADOOP_HOME is set
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = f"C:\\hadoop\\bin;{os.environ.get('PATH', '')}"

from pyspark.sql import SparkSession
import shutil

print("Creating Spark session...")
spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("BypassTest")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

try:
    # Use Py4J to patch NativeIO
    print("Attempting to patch NativeIO via Py4J...")
    sc = spark.sparkContext
    _gateway = sc._gateway

    # Try to access NativeIO and disable it
    try:
        native_io = _gateway.jvm.org.apache.hadoop.io.nativeio.NativeIO
        print(f"NativeIO class found: {native_io}")

        # Check if Windows class exists
        windows_class = _gateway.jvm.org.apache.hadoop.io.nativeio.NativeIO.Windows
        print(f"NativeIO.Windows found")

    except Exception as e:
        print(f"Could not access NativeIO: {e}")

    # Test write with file:// URL format
    print("\nTesting write with file:/// URI format...")
    test_dir = Path(__file__).parent / "spark_bypass_output"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir(parents=True, exist_ok=True)

    # Use as_uri() to get proper file:/// URI
    parquet_uri = (test_dir / "test.parquet").as_uri()
    print(f"URI: {parquet_uri}")

    df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "val"])

    print("Writing Parquet...")
    df.write.mode("overwrite").parquet(parquet_uri)
    print("SUCCESS: Parquet write succeeded!")

    print("Reading Parquet...")
    df_read = spark.read.parquet(parquet_uri)
    print(f"Read count: {df_read.count()}")
    print("SUCCESS: Full cycle completed!")

except Exception as e:
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
