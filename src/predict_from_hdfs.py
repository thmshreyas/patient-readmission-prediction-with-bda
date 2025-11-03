from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import shutil
import sys
import pkgutil
import pandas as pd
import joblib


def find_java_executable():
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = os.path.join(java_home, "bin", "java.exe" if os.name == "nt" else "java")
        if os.path.isfile(candidate):
            return candidate

    java_in_path = shutil.which("java")
    if java_in_path:
        return java_in_path

    return None


def check_winutils():
    if os.name != "nt":
        return True
    hadoop = os.environ.get("HADOOP_HOME")
    if not hadoop:
        return False
    winutils = os.path.join(hadoop, "bin", "winutils.exe")
    return os.path.isfile(winutils)


def detect_spark_home_version(spark_home):
    """Try to detect Spark version from jars in SPARK_HOME/jars (returns version str or None)."""
    jars_dir = os.path.join(spark_home, "jars")
    if not os.path.isdir(jars_dir):
        return None
    for fname in os.listdir(jars_dir):
        if fname.startswith("spark-core") and fname.endswith(".jar"):
            # spark-core_2.13-3.5.7.jar or spark-core_2.13-4.0.1.jar
            parts = fname.split("-")
            if parts:
                ver = parts[-1].replace(".jar", "")
                return ver
    return None


def main():
    # 1) Basic Java check
    java_path = find_java_executable()
    if not java_path:
        print("ERROR: No Java runtime found. Install a JDK and set JAVA_HOME to the JDK root.")
        print(r"Example (PowerShell): setx JAVA_HOME 'C:\\Program Files\\Java'")
        sys.exit(1)

    print(f"Found java executable at: {java_path}")

    # 2) If on Windows, require winutils or instruct the user
    if os.name == "nt" and not check_winutils():
        print("ERROR: winutils.exe not found under HADOOP_HOME\\bin. For local Spark on Windows please provide winutils.exe and set HADOOP_HOME.")
        print(r"Place winutils.exe in D:\\hadoop\\bin and run: setx HADOOP_HOME 'D:\\hadoop\\hadoop'")
        sys.exit(1)

    # 3) Check for pyspark package and version (without creating SparkContext yet)
    pyspark_installed = pkgutil.find_loader("pyspark") is not None
    pyspark_version = None
    if pyspark_installed:
        try:
            import pyspark

            pyspark_version = getattr(pyspark, "__version__", None)
        except Exception:
            pyspark_version = None

    # 4) Detect SPARK_HOME mismatch: if SPARK_HOME is set and points to a different Spark
    #    distribution than the installed pyspark, unset it so pip-installed pyspark uses
    #    its bundled JVM jars and avoids JavaPackage errors.
    spark_home = os.environ.get("SPARK_HOME")
    if spark_home and os.path.isdir(spark_home):
        detected = detect_spark_home_version(spark_home)
        if detected and pyspark_version and not pyspark_version.startswith(detected.split(".")[0]):
            # Simple heuristic: major version mismatch
            print(f"Warning: SPARK_HOME at {spark_home} appears to be Spark {detected} while pyspark package is {pyspark_version}.")
            print("Unsetting SPARK_HOME for this process to avoid version mismatch. If you want to use the external Spark distribution, install matching pyspark.")
            os.environ.pop("SPARK_HOME", None)

    # 5) Ensure PYSPARK_PYTHON uses the current interpreter
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable or "python")
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable or "python")

    # 6) Now import PySpark and create session
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    # === Spark Setup ===
    # Bind driver to localhost on Windows to avoid RPC/BlockManager connectivity issues.
    try:
        spark = SparkSession.builder \
            .appName("Patient Readmission Prediction") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-client:3.3.1") \
            .config("spark.driver.memory", "2g") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .master("local[*]") \
            .getOrCreate()
    except TypeError as e:
        # Common symptom when pyspark client and JVM Spark distribution mismatch
        print("ERROR: Failed to start SparkSession — possible pyspark/SPARK_HOME version mismatch or Java gateway problem.")
        print("Exception:", e)
        print("Suggestion: try unsetting SPARK_HOME for this process, or install a pyspark package that matches your Spark distribution.")
        print("You can temporarily unset SPARK_HOME in PowerShell with: $env:SPARK_HOME=$null")
        sys.exit(1)
    except Exception as e:
        print("ERROR: Failed to start SparkSession:", e)
        sys.exit(1)

    # === Load Data from HDFS ===
    hdfs_path = "hdfs://localhost:9000/patient_data/cleaned/streamed_data.csv"

    print("Loading streamed patient data from HDFS...")
    df = spark.read.option("header", "false").csv(hdfs_path, inferSchema=True)
    df.show(5)

    # === Convert Spark DF to Pandas for model inference ===
    data_pd = df.toPandas()

    # === Load Model and Preprocessing Objects ===
    print("Loading trained model and preprocessing pipeline...")
    scaler = joblib.load(os.path.join("..", "models", "scaler.pkl")) if not os.path.exists("models/scaler.pkl") else joblib.load("models/scaler.pkl")
    feature_columns = joblib.load(os.path.join("..", "models", "feature_columns.pkl")) if not os.path.exists("models/feature_columns.pkl") else joblib.load("models/feature_columns.pkl")
    model = joblib.load(os.path.join("..", "models", "readmission_model.pkl")) if not os.path.exists("models/readmission_model.pkl") else joblib.load("models/readmission_model.pkl")

    # === Ensure columns match training features ===
    # If the CSV didn't have headers, make sure the DataFrame columns map correctly.
    data_pd.columns = feature_columns[: len(data_pd.columns)] if len(feature_columns) >= len(data_pd.columns) else data_pd.columns

    # === Apply scaling ===
    data_scaled = scaler.transform(data_pd)

    # === Make predictions ===
    predictions = model.predict(data_scaled)
    data_pd["Predicted_Readmission"] = predictions

    # === Save predictions ===
    output_csv = "predicted_readmission_results.csv"
    data_pd.to_csv(output_csv, index=False)

    print(f"✅ Predictions complete! Saved results to {output_csv}")


if __name__ == "__main__":
    main()
