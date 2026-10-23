"""
Verify that Spark can write and read Parquet files on Windows.
"""
import sys
from pathlib import Path

# Add triallock package to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from triallock.spark_session import get_spark
import shutil

print("=" * 70)
print("Spark Write Verification Test")
print("=" * 70)

# Create Spark session with updated configuration
print("\n[1] Creating Spark session with Windows Hadoop configuration...")
spark = get_spark("VerifyTest")

if spark is None:
    print("FAILED: Could not create Spark session")
    sys.exit(1)

print("SUCCESS: Spark session created")

# Prepare test directory
test_dir = Path(__file__).parent / "spark_verify_output"
if test_dir.exists():
    shutil.rmtree(test_dir)
test_dir.mkdir(parents=True, exist_ok=True)

try:
    # Test DataFrame operations
    print("\n[2] Creating test DataFrame...")
    df = spark.createDataFrame(
        [
            (1, "alpha", 10.5),
            (2, "beta", 20.3),
            (3, "gamma", 15.7),
            (4, "delta", 30.2),
            (5, "epsilon", 25.9),
        ],
        ["id", "name", "value"]
    )
    print(f"  DataFrame created with {df.count()} rows")

    # Test Parquet write
    print("\n[3] Writing DataFrame to Parquet...")
    parquet_path = test_dir / "test.parquet"
    df.write.mode("overwrite").parquet(str(parquet_path))
    print(f"  Parquet written to: {parquet_path}")

    # Test Parquet read
    print("\n[4] Reading Parquet back...")
    df_read = spark.read.parquet(str(parquet_path))
    read_count = df_read.count()
    print(f"  Parquet read count: {read_count}")

    if read_count != 5:
        print(f"  ERROR: Expected 5 rows, got {read_count}")
        sys.exit(1)

    print("  SUCCESS: Parquet write/read cycle completed")

    # Test CSV write
    print("\n[5] Writing DataFrame to CSV...")
    csv_path = test_dir / "test.csv"
    df.write.mode("overwrite").option("header", "true").csv(str(csv_path))
    print(f"  CSV written to: {csv_path}")

    # Test CSV read
    print("\n[6] Reading CSV back...")
    df_csv = spark.read.option("header", "true").csv(str(csv_path))
    csv_count = df_csv.count()
    print(f"  CSV read count: {csv_count}")

    if csv_count != 5:
        print(f"  ERROR: Expected 5 rows, got {csv_count}")
        sys.exit(1)

    print("  SUCCESS: CSV write/read cycle completed")

    # Test simple aggregation
    print("\n[7] Testing DataFrame operations...")
    from pyspark.sql import functions as F
    result = df.agg(F.sum("value").alias("total")).collect()[0]["total"]
    print(f"  Sum of values: {result}")

    print("\n[8] Stopping Spark session...")
    spark.stop()
    print("  Spark session stopped")

    print("\n" + "=" * 70)
    print("ALL TESTS PASSED")
    print("=" * 70)

except Exception as e:
    print(f"\nFAILED: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

    if spark:
        spark.stop()

    sys.exit(1)
