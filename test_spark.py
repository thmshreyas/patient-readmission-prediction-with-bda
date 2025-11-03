import os
import shutil
import sys

# Only set SPARK/HADOOP paths if you actually have them at these locations.
# Don't overwrite JAVA_HOME with a generic folder — it must point to a JDK root (e.g. C:\\Program Files\\Java\\jdk-17.0.8)
# If you have a Spark distribution and Hadoop installed, point these to the correct folders.
# Don't blindly set them to a path that doesn't exist — that causes PySpark to try to use the wrong jars.
_maybe_spark = r"D:\\spark\\spark-3.4.1-bin-hadoop3"
if os.path.isdir(_maybe_spark):
    os.environ["SPARK_HOME"] = _maybe_spark

_maybe_hadoop = r"D:\\hadoop\\hadoop"
if os.path.isdir(_maybe_hadoop):
    os.environ["HADOOP_HOME"] = _maybe_hadoop

# Use the same python interpreter running this script unless you have reasons not to
os.environ.setdefault("PYSPARK_PYTHON", sys.executable or "python")
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable or "python")


def find_java_executable():
    """Return path to a java executable if available, otherwise None."""
    # 1. Check JAVA_HOME
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = os.path.join(java_home, "bin", "java.exe" if os.name == "nt" else "java")
        if os.path.isfile(candidate):
            return candidate

    # 2. Look on PATH
    java_in_path = shutil.which("java")
    if java_in_path:
        return java_in_path

    return None


def check_winutils():
    """On Windows ensure winutils.exe exists under HADOOP_HOME\\bin (PySpark requirement on some setups)."""
    if os.name != "nt":
        return True
    hadoop = os.environ.get("HADOOP_HOME")
    if not hadoop:
        return False
    winutils = os.path.join(hadoop, "bin", "winutils.exe")
    return os.path.isfile(winutils)


if __name__ == "__main__":
    java_path = find_java_executable()
    if not java_path:
        print("ERROR: No Java runtime found. PySpark needs Java (a JDK).")
        print("Steps to fix:")
        print("  1) Install a JDK (e.g. Temurin/Eclipse Adoptium or Oracle JDK).")
        print(r"  2) Set JAVA_HOME to the JDK root, e.g. 'C:\\Program Files\\Java\\jdk-17.0.8'")
        print(r"  3) Add '%JAVA_HOME%\\bin' to your PATH and restart the shell/IDE.")
        print("Quick check (PowerShell):")
        print("  java -version")
        sys.exit(1)

    print(f"Found java executable at: {java_path}")

    if os.name == "nt" and not check_winutils():
        print("WARNING: winutils.exe not found under HADOOP_HOME\\bin. On Windows PySpark may need winutils.exe.")
        print(r"If you have Hadoop set up, ensure HADOOP_HOME points to it and winutils.exe exists in HADOOP_HOME\\bin.")
        print(r"You can download a compatible winutils.exe and place it in %HADOOP_HOME%\\bin if needed.")

    # Import pyspark only after we validate Java so the error is clearer
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("VSCodeTest") \
        .master("local[*]") \
        .getOrCreate()

    print("✅ Spark session created successfully!")
    print("Spark version:", spark.version)

    spark.stop()
