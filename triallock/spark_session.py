import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession


def _configure_windows_environment():
    """Configure all necessary environment variables for Windows PySpark."""
    if sys.platform != "win32":
        return

    # 1. Configure Python executable for PySpark workers
    # Crucial on Windows to prevent using the Microsoft Store python stub
    venv_python = sys.executable
    os.environ["PYSPARK_PYTHON"] = venv_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = venv_python

    # 2. Configure JAVA_HOME if not already set
    if not os.environ.get("JAVA_HOME"):
        java_candidates = [
            Path("C:/Program Files/Java/jdk-17"),
            Path("C:/Program Files/Java/jdk-17.0.12"),
            Path("C:/Program Files/Eclipse Adoptium/jdk-17"),
            Path("C:/Program Files/Microsoft/jdk-17"),
        ]
        for candidate in java_candidates:
            if (candidate / "bin" / "java.exe").exists():
                os.environ["JAVA_HOME"] = str(candidate)
                break

    # 3. Configure HADOOP_HOME and PATH for Windows Hadoop native libraries
    hadoop_home = os.environ.get("HADOOP_HOME", "")
    if not hadoop_home or not Path(hadoop_home).exists():
        hadoop_candidates = [
            Path("C:/hadoop"),
            Path(os.environ.get("LOCALAPPDATA", "")) / "hadoop",
            Path.home() / "hadoop",
        ]
        for candidate in hadoop_candidates:
            bin_dir = candidate / "bin"
            if (bin_dir / "winutils.exe").exists() or (bin_dir / "hadoop.dll").exists():
                os.environ["HADOOP_HOME"] = str(candidate)
                hadoop_home = str(candidate)
                break

    # Ensure Hadoop bin is in PATH for DLL loading
    if hadoop_home:
        hadoop_bin = str(Path(hadoop_home) / "bin")
        current_path = os.environ.get("PATH", "")
        if hadoop_bin not in current_path:
            os.environ["PATH"] = f"{hadoop_bin};{current_path}"


def get_spark(session_app_name="TrialLock"):
    """
    Create or retrieve a SparkSession with Windows-compatible configuration.

    Returns:
        SparkSession or None: A Spark session if successful, None otherwise.
    """
    # Configure environment for Windows before Spark initialization
    _configure_windows_environment()

    venv_python = sys.executable

    builder = SparkSession.builder \
        .appName(session_app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "1g") \
        .config("spark.pyspark.python", venv_python) \
        .config("spark.pyspark.driver.python", venv_python)

    # Low-resource settings
    builder = builder \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.default.parallelism", "2")

    # Disable web UI to save resources
    builder = builder.config("spark.ui.enabled", "false")

    try:
        spark = builder.getOrCreate()
        # Set log level to reduce noise
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as exc:
        # Log the exception for debugging but don't crash the app
        import traceback
        traceback.print_exc()
        return None
