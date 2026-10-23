"""
PySpark Windows Environment Diagnostic Script
This script diagnoses the Spark startup issue on native Windows.
Run from the project root with the virtual environment activated.
"""
import sys
import os
import subprocess
import platform
from pathlib import Path

print("=" * 70)
print("TrialLock PySpark Windows Diagnostic")
print("=" * 70)

# 1. Python Environment
print("\n[1] PYTHON ENVIRONMENT")
print(f"  Python executable: {sys.executable}")
print(f"  Python version: {sys.version}")
print(f"  Python platform: {platform.platform()}")
print(f"  Working directory: {os.getcwd()}")

# 2. Java Environment
print("\n[2] JAVA ENVIRONMENT")
java_home = os.environ.get("JAVA_HOME", "")
print(f"  JAVA_HOME: {java_home if java_home else '(not set)'}")

# Check common Java installation locations
java_candidates = [
    Path("C:/Program Files/Java/jdk-17"),
    Path("C:/Program Files/Java/jdk-17.0.12"),
    Path("C:/Program Files/Java/jre-17"),
    Path("C:/Program Files/Eclipse Adoptium/jdk-17"),
    Path("C:/Program Files/Microsoft/jdk-17"),
]

print("  Checking common Java locations:")
for candidate in java_candidates:
    java_exe = candidate / "bin" / "java.exe"
    if java_exe.exists():
        print(f"    FOUND: {candidate}")
        print(f"      java.exe exists: {java_exe.exists()}")
    else:
        print(f"    Not found: {candidate}")

# Try to find java via PATH
print("\n  Checking java via PATH:")
try:
    result = subprocess.run(
        ["java", "-version"],
        capture_output=True,
        text=True,
        timeout=10
    )
    print(f"    java -version output:")
    for line in (result.stderr + result.stdout).strip().split("\n"):
        print(f"      {line}")
except FileNotFoundError:
    print("    ERROR: java not found in PATH")
except Exception as e:
    print(f"    ERROR: {e}")

# Find where java.exe actually is
print("\n  Searching for java.exe in PATH:")
path_dirs = os.environ.get("PATH", "").split(os.pathsep)
for p in path_dirs:
    java_check = Path(p) / "java.exe"
    if java_check.exists():
        print(f"    Found: {java_check}")

# 3. PySpark Environment Variables
print("\n[3] SPARK ENVIRONMENT VARIABLES")
spark_vars = [
    "SPARK_HOME",
    "HADOOP_HOME",
    "PYSPARK_PYTHON",
    "PYSPARK_DRIVER_PYTHON",
    "PYSPARK_SUBMIT_ARGS",
]
for var in spark_vars:
    val = os.environ.get(var, "")
    print(f"  {var}: {val if val else '(not set)'}")

# 4. PySpark Package
print("\n[4] PYSPARK PACKAGE")
try:
    import pyspark
    print(f"  PySpark version: {pyspark.__version__}")
    print(f"  PySpark location: {pyspark.__file__}")
except ImportError as e:
    print(f"  ERROR: PySpark not importable: {e}")
    sys.exit(1)

# Check for spark-submit
pyspark_dir = Path(pyspark.__file__).parent
print(f"  PySpark package dir: {pyspark_dir}")
spark_submit_candidates = [
    pyspark_dir / "bin" / "spark-submit.cmd",
    pyspark_dir / "bin" / "spark-submit",
]
for candidate in spark_submit_candidates:
    exists = candidate.exists()
    print(f"  {candidate.name}: {'EXISTS' if exists else 'not found'}")

# 5. Hadoop DLL Check (common Windows issue)
print("\n[5] HADOOP NATIVE LIBRARIES (Windows)")
hadoop_home = os.environ.get("HADOOP_HOME", "")
if hadoop_home:
    hadoop_dll = Path(hadoop_home) / "bin" / "hadoop.dll"
    winutils_exe = Path(hadoop_home) / "bin" / "winutils.exe"
    print(f"  hadoop.dll: {'EXISTS' if hadoop_dll.exists() else 'MISSING'}")
    print(f"  winutils.exe: {'EXISTS' if winutils_exe.exists() else 'MISSING'}")
else:
    print("  HADOOP_HOME not set")
    print("  NOTE: PySpark on Windows may need winutils.exe for certain operations")

# 6. Minimal Spark Test
print("\n[6] MINIMAL SPARK SESSION TEST")
print("  Attempting to create SparkSession...")

# Set JAVA_HOME explicitly if not set
if not java_home:
    # Try to find Java installation
    for candidate in java_candidates:
        if (candidate / "bin" / "java.exe").exists():
            print(f"  Setting JAVA_HOME to: {candidate}")
            os.environ["JAVA_HOME"] = str(candidate)
            java_home = str(candidate)
            break

# Set PYSPARK_PYTHON if not set
if not os.environ.get("PYSPARK_PYTHON"):
    os.environ["PYSPARK_PYTHON"] = sys.executable
    print(f"  Setting PYSPARK_PYTHON to: {sys.executable}")

if not os.environ.get("PYSPARK_DRIVER_PYTHON"):
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    print(f"  Setting PYSPARK_DRIVER_PYTHON to: {sys.executable}")

try:
    from pyspark.sql import SparkSession

    # Capture detailed error
    import traceback

    print("  Creating SparkSession.builder...")
    builder = (
        SparkSession.builder
        .appName("TrialLockDiagnostic")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
    )

    print("  Calling getOrCreate()...")
    spark = builder.getOrCreate()

    print("  SUCCESS: SparkSession created!")

    # Test basic operation
    print("\n  Testing DataFrame creation...")
    df = spark.createDataFrame([(1, "test"), (2, "data")], ["id", "value"])
    count = df.count()
    print(f"  DataFrame count: {count}")

    # Test write/read
    print("\n  Testing Parquet write/read...")
    test_dir = Path(__file__).parent / "spark_test_output"
    test_dir.mkdir(exist_ok=True)

    df.write.mode("overwrite").parquet(str(test_dir / "test.parquet"))
    df_read = spark.read.parquet(str(test_dir / "test.parquet"))
    read_count = df_read.count()
    print(f"  Parquet read count: {read_count}")

    spark.stop()
    print("\n  Spark session stopped successfully.")

except Exception as e:
    print(f"\n  FAILED: {type(e).__name__}: {e}")
    print("\n  Full traceback:")
    traceback.print_exc()

    # Try to get more details from the Java gateway
    print("\n  Attempting to diagnose Java gateway issue...")
    try:
        from pyspark.java_gateway import launch_gateway
        print("  Checking launch_gateway function...")
    except Exception as ge:
        print(f"  Gateway import error: {ge}")

print("\n" + "=" * 70)
print("Diagnostic complete.")
print("=" * 70)
