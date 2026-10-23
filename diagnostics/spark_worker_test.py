"""
Test with explicit PYSPARK_PYTHON setting.
"""
import sys
import os
from pathlib import Path

# IMPORTANT: Set these BEFORE any PySpark imports
venv_python = sys.executable
os.environ["PYSPARK_PYTHON"] = venv_python
os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

# Set Java/Hadoop environment
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-17"
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = f"C:\\hadoop\\bin;{os.environ.get('PATH', '')}"

print(f"PYSPARK_PYTHON: {os.environ['PYSPARK_PYTHON']}")
print(f"JAVA_HOME: {os.environ['JAVA_HOME']}")
print(f"HADOOP_HOME: {os.environ['HADOOP_HOME']}")

from pyspark.sql import SparkSession
import shutil

print("\nCreating Spark session...")
spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("PythonWorkerTest")
    .config("spark.ui.enabled", "false")
    .config("spark.pyspark.python", venv_python)
    .config("spark.pyspark.driver.python", venv_python)
    .getOrCreate()
)

try:
    print("Testing DataFrame creation and count...")
    df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "val"])
    count = df.count()
    print(f"DataFrame count: {count} (SUCCESS: Python workers work!)")

    # Now test file writing using file:// URI
    test_dir = Path(__file__).parent / "spark_worker_test_output"
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir(parents=True, exist_ok=True)

    parquet_uri = (test_dir / "test.parquet").as_uri()
    print(f"\nWriting to: {parquet_uri}")

    # Try using pandas as an alternative writer if Spark write fails
    try:
        df.write.mode("overwrite").parquet(parquet_uri)
        print("SUCCESS: Spark native Parquet write succeeded!")

        df_read = spark.read.parquet(parquet_uri)
        print(f"Read count: {df_read.count()}")
    except Exception as write_err:
        print(f"Native Spark write failed: {write_err}")
        print("Testing PyArrow/pandas fallback for Parquet writing...")

        # Convert to pandas and write via pyarrow
        pdf = df.toPandas()
        pdf.to_parquet(test_dir / "fallback.parquet")
        print(f"SUCCESS: PyArrow fallback wrote {len(pdf)} rows!")

        # Verify Spark can read it back
        df_read = spark.read.parquet((test_dir / "fallback.parquet").as_uri())
        print(f"Spark read fallback count: {df_read.count()}")

except Exception as e:
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()

finally:
    spark.stop()
    print("\nTest completed.")
