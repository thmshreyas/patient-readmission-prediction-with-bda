import os
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
import subprocess  # <-- 1. IMPORT SUBPROCESS

# --- Path to your hdfs.cmd ---
HADOOP_BIN = r"D:\hadoop\hadoop\bin\hdfs.cmd"  # <-- 2. ADD PATH TO HDFS.CMD

# --- IMPORTANT: Set paths to your models ---
base_dir = r"D:\bda project\patient-readmission-prediction-with-bda\models"
model_path = os.path.join(base_dir, "readmission_model.pkl")
scaler_path = os.path.join(base_dir, "scaler.pkl")
features_path = os.path.join(base_dir, "feature_columns.pkl")

# --- HDFS Paths ---
HDFS_RAW_PATH = "hdfs://localhost:9000/patient_data/raw_records/"
# This is the path for the *folder*
HDFS_CLEANED_FOLDER = "/patient_data/cleaned_predictions" # No trailing slash

# --- Local Workaround Path ---
LOCAL_OUTPUT_FILE = r"D:\bda_output\predictions.csv"
LOCAL_OUTPUT_DIR = r"D:\bda_output"

def process_data():
    spark = SparkSession.builder \
        .appName("PatientReadmissionBatchJob") \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"✅ Spark Job Started. Reading from {HDFS_RAW_PATH}")

    # --- 1. Load Models ---
    try:
        model = joblib.load(model_path)
        scaler = joblib.load(scaler_path)
        feature_columns = joblib.load(features_path)
        print(f"✅ Models loaded successfully with {len(feature_columns)} features.")
    except Exception as e:
        print(f"❌ Error loading models: {e}")
        spark.stop()
        exit(1)

    # --- 2. Read new data from HDFS "raw" folder ---
    try:
        df = spark.read.option("header", "true").csv(HDFS_RAW_PATH)
        if df.count() == 0:
            print("No new data to process. Exiting.")
            spark.stop()
            return
        print(f"Found {df.count()} new records to process.")
    except Exception as e:
        print(f"No data in {HDFS_RAW_PATH}. Did you send any from the producer? Exiting.")
        spark.stop()
        return

    # --- 3. Pre-process Data ---
    print("➡️ Starting Spark Pre-processing...")
    df_processed = df
    for col_name in feature_columns:
        if col_name in df.columns:
            df_processed = df_processed.withColumn(
                col_name,
                col(col_name).cast(StringType())
            ).withColumn(
                col_name,
                when(col(col_name) == '?', None).otherwise(col(col_name))
            )
    
    pdf = df_processed.toPandas()

    pdf['gender'] = pdf['gender'].map({'Male': 1, 'Female': 0, 'Unknown/Invalid': -1}).fillna(-1)
    pdf['race'] = pdf['race'].map({'Caucasian': 0, 'AfricanAmerican': 1, 'Hispanic': 2, 'Asian': 3, 'Other': 4}).fillna(0)
    pdf['age'] = pdf['age'].map({'[0-10)': 5, '[10-20)': 15, '[20-30)': 25, '[30-40)': 35, '[40-50)': 45, '[50-60)': 55, '[60-70)': 65, '[70-80)': 75, '[80-90)': 85, '[90-100)': 95}).fillna(0)
    pdf['change'] = pdf['change'].map({'Ch': 1, 'No': 0}).fillna(0)
    pdf['diabetesMed'] = pdf['diabetesMed'].map({'Yes': 1, 'No': 0}).fillna(0)
    pdf['insulin'] = pdf['insulin'].map({'No': 0, 'Steady': 1, 'Up': 2, 'Down': 3}).fillna(0)
    # ... (add any other text mappings here) ...

    for col_name in feature_columns:
        if col_name not in pdf.columns:
            pdf[col_name] = 0
            
    pdf_features = pdf[feature_columns]
    
    pdf_features = pdf_features.apply(pd.to_numeric, errors='coerce')
    pdf_features = pdf_features.fillna(0)
    print("✅ Pre-processing complete.")

    # --- 4. Predict ---
    try:
        scaled_data = scaler.transform(pdf_features)
        predictions = model.predict(scaled_data)
        probabilities = model.predict_proba(scaled_data)[:, 1] * 100

        result_pdf = pdf[['encounter_id', 'patient_nbr', 'age', 'gender', 'race', 'time_in_hospital']]
        result_pdf["readmission_prediction"] = predictions
        result_pdf["readmission_probability(%)"] = probabilities
        
        print("✅ Predictions generated:")
        print(result_pdf.head())

        # --- 5. SAVE LOCALLY (THE WORKAROUND) ---
        print(f"WORKAROUND: Saving predictions to local file: {LOCAL_OUTPUT_FILE}")
        os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)
        # We overwrite the local file with the *full* set of predictions
        result_pdf.to_csv(LOCAL_OUTPUT_FILE, index=False) 
        print("✅ Local save complete.")

        # --- 6. UPLOAD TO HDFS (THE FIX) ---
        print(f"Uploading {LOCAL_OUTPUT_FILE} to {HDFS_CLEANED_FOLDER} using hdfs.cmd...")
        # Use subprocess to call the hdfs.cmd, which we know works
        # -f flag means 'force' (overwrite)
        subprocess.run([
            HADOOP_BIN, 'dfs', '-put', '-f', LOCAL_OUTPUT_FILE, HDFS_CLEANED_FOLDER
        ], shell=True, check=True) # 'check=True' will raise an error if this fails
        print(f"✅ Successfully uploaded 'predictions.csv' to HDFS.")

    except Exception as e:
        print(f"❌ Error during prediction or upload: {e}")

    spark.stop()
    print("🛑 Spark Job Finished.")

if __name__ == "__main__":
    process_data()

