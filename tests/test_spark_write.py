from pathlib import Path
import shutil

from pyspark.sql import SparkSession


def main():
    output = Path(__file__).resolve().parent / "spark_write_test_output"

    if output.exists():
        shutil.rmtree(output)

    output.mkdir(parents=True, exist_ok=True)

    parquet_uri = (output / "parquet").as_uri()
    csv_uri = (output / "csv").as_uri()

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("TrialLockWindowsWriteTest")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    try:
        spark.sparkContext.setLogLevel("ERROR")

        df = spark.createDataFrame(
            [(1, "alpha"), (2, "beta")],
            ["id", "label"],
        )

        # Test Parquet output and read-back.
        df.write.mode("overwrite").parquet(parquet_uri)
        parquet_count = spark.read.parquet(parquet_uri).count()

        # Test CSV output and read-back.
        df.write.mode("overwrite").option("header", "true").csv(csv_uri)
        csv_count = (
            spark.read
            .option("header", "true")
            .csv(csv_uri)
            .count()
        )

        assert parquet_count == 2, f"Expected 2 Parquet rows, got {parquet_count}"
        assert csv_count == 2, f"Expected 2 CSV rows, got {csv_count}"

        print("PASS: Parquet write and read succeeded.")
        print("PASS: CSV write and read succeeded.")
        print(f"Test output directory: {output}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()