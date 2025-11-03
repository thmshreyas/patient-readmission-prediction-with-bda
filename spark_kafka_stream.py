import os
import sys
import time
import json
import joblib
import subprocess
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

# ------------------------------------------------------------
# Configuration: do not forcibly override SPARK_HOME here to avoid
# client/distribution mismatches. Set HADOOP_HOME and Python used.
# ------------------------------------------------------------
os.environ.setdefault("HADOOP_HOME", r"D:\hadoop\hadoop")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable or "python")
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable or "python")

# Ensure Kafka connector is available to JVM (use package matching Spark 3.5.7 / Scala 2.12)
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 pyspark-shell",
)

# ---------- Spark session with Windows-friendly network settings ----------
spark = SparkSession.builder \
    .appName("KafkaPatientStream") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.local.ip", "127.0.0.1") \
    .config("spark.network.timeout", "120s") \
    .config("spark.executor.heartbeatInterval", "30s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session started successfully.")

# ------------------ Define Kafka source & schema ------------------
print("⏳ Waiting for messages from Kafka topic: patient-data ...")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "patient-data") \
    .option("startingOffsets", "latest") \
    .load()

# decode value
json_df = raw_df.selectExpr("CAST(value AS STRING) as json_data")

schema = StructType() \
    .add("patient_id", StringType()) \
    .add("age", IntegerType()) \
    .add("gender", StringType()) \
    .add("blood_pressure", DoubleType()) \
    .add("readmitted", StringType())

parsed_df = json_df.select(from_json(col("json_data"), schema).alias("data")).select("data.*")


def process_batch(batch_df, epoch_id):
    """Process each micro-batch: make predictions and persist results.
    This runs on the driver. Keep it idempotent (use epoch_id).
    """
    try:
        # fast check for emptiness
        if batch_df.rdd.isEmpty():
            return
    except Exception:
        if batch_df.count() == 0:
            return

    # Convert to pandas for existing sklearn model
    pdf = batch_df.toPandas()
    if pdf.empty:
        return

    # Load model objects from models/ (assumes they exist)
    models_dir = Path(__file__).resolve().parents[1] / "models"
    try:
        feature_columns = joblib.load(models_dir / "feature_columns.pkl")
        scaler = joblib.load(models_dir / "scaler.pkl")
        model = joblib.load(models_dir / "readmission_model.pkl")
    except Exception as e:
        print("ERROR: Failed to load model artifacts:", e)
        return

    # Align columns
    cols = [c for c in feature_columns if c in pdf.columns]
    if not cols:
        print("WARNING: None of the model feature columns found in batch; skipping")
        return

    X = pdf[cols].copy()
    try:
        X_scaled = scaler.transform(X)
        preds = model.predict(X_scaled)
    except Exception as e:
        print("ERROR during model predict:", e)
        return

    pdf["Predicted_Readmission"] = preds

    # Persist locally
    out_dir = Path.cwd() / "predictions"
    out_dir.mkdir(exist_ok=True)
    out_file = out_dir / f"predictions_epoch_{int(time.time())}_{epoch_id}.csv"
    pdf.to_csv(out_file, index=False)
    print(f"✅ Wrote predictions to {out_file}")

    # If HADOOP/HDFS is available, try to upload
    hadoop_bin = Path(os.environ.get("HADOOP_HOME", "")) / "bin" / "hdfs.cmd"
    if hadoop_bin.exists():
        try:
            subprocess.run([str(hadoop_bin), "dfs", "-mkdir", "-p", "/patient_data/predictions"], check=False)
            subprocess.run([str(hadoop_bin), "dfs", "-put", "-f", str(out_file), "/patient_data/predictions/"], check=False)
            print("✅ Uploaded predictions to HDFS /patient_data/predictions/")
        except Exception as e:
            print("WARN: failed to upload to HDFS:", e)


# Start streaming with foreachBatch to apply ML model per micro-batch
query = parsed_df.writeStream.foreachBatch(process_batch).outputMode("append").start()
print("✅ Streaming job started (foreachBatch). Waiting for data...")
query.awaitTermination()
