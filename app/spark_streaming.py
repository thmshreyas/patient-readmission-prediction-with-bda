import os
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructType, StructField, IntegerType

# --- IMPORTANT: Set paths to your models ---
# Spark needs the absolute paths
base_dir = r"D:\bda project\patient-readmission-prediction-with-bda\models"
model_path = os.path.join(base_dir, "readmission_model.pkl")
scaler_path = os.path.join(base_dir, "scaler.pkl")
features_path = os.path.join(base_dir, "feature_columns.pkl")

# --- HDFS Output Path ---
# ⬇️⬇️ THIS IS THE FIX ⬇️⬇️
# HDFS NameNode runs on port 9000 (not 9092, which is Kafka)
HDFS_OUTPUT_PATH = "hdfs://localhost:9000/patient_data/streaming_predictions/"

# --- 1. Load Models (Broadcast) ---
# Load models ONCE on the driver
try:
    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)
    feature_columns = joblib.load(features_path)
    print(f"✅ Models loaded successfully with {len(feature_columns)} features.")
except Exception as e:
    print(f"❌ Error loading models: {e}")
    exit(1)

# --- 2. Define Kafka JSON Schema ---
# This schema MUST match the JSON from your 'producer.py'
all_columns = [
    'encounter_id', 'patient_nbr', 'race', 'gender', 'age', 'weight',
    'admission_type_id', 'discharge_disposition_id', 'admission_source_id',
    'time_in_hospital', 'payer_code', 'medical_specialty', 'num_lab_procedures',
    'num_procedures', 'num_medications', 'number_outpatient', 'number_emergency',
    'number_inpatient', 'diag_1', 'diag_2', 'diag_3', 'number_diagnoses',
    'max_glu_serum', 'A1Cresult', 'metformin', 'repaglinide', 'nateglinide',
    'chlorpropamide', 'glimepiride', 'acetohexamide', 'glipizide', 'glyburide',
    'tolbutamide', 'pioglitazone', 'rosiglitazone', 'acarbose', 'miglitol',
    'troglitazone', 'tolazamide', 'examide', 'citoglipton', 'insulin',
    'glyburide-metformin', 'glipizide-metformin', 'glimepiride-pioglitazone',
    'metformin-rosiglitazone', 'metformin-pioglitazone', 'change',
    'diabetesMed', 'readmitted' # Your producer sends 'readmitted', so we must include it
]
# All data from Kafka is text
json_schema = StructType([StructField(c, StringType(), True) for c in all_columns])


# --- 3. Define the Prediction Function ---
def process_batch(df, epoch_id):
    # Get the active Spark session within the batch function
    spark = SparkSession.getActiveSession()
    
    if df.count() == 0:
        return

    print(f"--- Processing Batch {epoch_id} ---")
    
    # 1. Convert Spark micro-batch to Pandas
    pdf = df.toPandas()
    if pdf.empty:
        return

    # 2. Pre-process Data (The logic from our last script)
    
    # --- 2a. Replace all '?' with None ---
    pdf = pdf.replace('?', None)
    
    # --- 2b. Map text to numbers ---
    pdf['gender'] = pdf['gender'].map({'Male': 1, 'Female': 0, 'Unknown/Invalid': -1}).fillna(-1)
    pdf['race'] = pdf['race'].map({'Caucasian': 0, 'AfricanAmerican': 1, 'Hispanic': 2, 'Asian': 3, 'Other': 4}).fillna(0)
    pdf['age'] = pdf['age'].map({'[0-10)': 5, '[10-20)': 15, '[20-30)': 25, '[30-40)': 35, '[40-50)': 45, '[50-60)': 55, '[60-70)': 65, '[70-80)': 75, '[80-90)': 85, '[90-100)': 95}).fillna(0)
    pdf['change'] = pdf['change'].map({'Ch': 1, 'No': 0}).fillna(0)
    pdf['diabetesMed'] = pdf['diabetesMed'].map({'Yes': 1, 'No': 0}).fillna(0)
    # ... (Add all other text mappings: insulin, metformin, etc.) ...
    pdf['insulin'] = pdf['insulin'].map({'No': 0, 'Steady': 1, 'Up': 2, 'Down': 3}).fillna(0)

    # --- 2c. Select features and fill nulls ---
    # Ensure all required columns exist, even if not in JSON
    for col in feature_columns:
        if col not in pdf.columns:
            pdf[col] = 0 # Add missing column
            
    # Select only the feature columns
    pdf_features = pdf[feature_columns]
    
    # --- 2d. Cast all to numeric and fill NAs ---
    pdf_features = pdf_features.apply(pd.to_numeric, errors='coerce') # Convert all to numbers
    pdf_features = pdf_features.fillna(0) # Fill all remaining None/NaN with 0
    
    # 3. Predict
    try:
        scaled_data = scaler.transform(pdf_features)
        predictions = model.predict(scaled_data)
        probabilities = model.predict_proba(scaled_data)[:, 1] * 100

        # 4. Create Result DataFrame
        result_pdf = pdf[['encounter_id', 'patient_nbr', 'age', 'gender', 'race', 'time_in_hospital']]
        result_pdf["readmission_prediction"] = predictions
        result_pdf["readmission_probability(%)"] = probabilities
        
        print("✅ Predictions generated:")
        print(result_pdf.head())

        # 5. Convert back to Spark and write to HDFS
        # The 'spark' variable is now defined and this will work
        result_df_spark = spark.createDataFrame(result_pdf)
        
        # Write to HDFS as a single CSV file in append mode
        result_df_spark.coalesce(1).write.mode("append").option("header", "true").csv(HDFS_OUTPUT_PATH)
        print(f"✅ Batch {epoch_id} written to HDFS.")

    except Exception as e:
        print(f"❌ Error during prediction or writing: {e}")
        print("--- problematic data ---")
        pdf_features.info()


# --- 4. Spark Session & Streaming Query ---
def start_streaming():
    spark = SparkSession.builder \
        .appName("PatientReadmissionStream") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
    
    print("✅ Spark streaming session started.")

    # Read from Kafka topic 'patient_input'
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "patient_input") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse the JSON from Kafka
    data_df = kafka_df.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")

    # Start the streaming query
    query = data_df.writeStream \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .foreachBatch(process_batch) \
        .start()

    print("🚀 Kafka streaming query started. Listening for data...")
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()
