from pyspark.sql import SparkSession
# ⬇️ Import these
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when
import pandas as pd
import joblib
import os

# ====================================
# 1️⃣ Start Spark Session
# ====================================
spark = SparkSession.builder \
    .appName("PatientReadmissionSparkJob") \
    .master("local[*]") \
    .getOrCreate()

print("✅ Spark started successfully!")

# ====================================
# 2️⃣ Load Model & Feature List (Moved Earlier)
# ====================================
try:
    model_path = r"D:\bda project\patient-readmission-prediction-with-bda\models\readmission_model.pkl"
    scaler_path = r"D:\bda project\patient-readmission-prediction-with-bda\models\scaler.pkl"
    features_path = r"D:\bda project\patient-readmission-prediction-with-bda\models\feature_columns.pkl"
    
    model = joblib.load(model_path)
    scaler = joblib.load(scaler_path)
    feature_columns = joblib.load(features_path)
    print(f"✅ Model and scaler loaded successfully with {len(feature_columns)} features!")
except Exception as e:
    print(f"❌ Error loading model/scaler: {e}")
    spark.stop()
    exit(1)

# ====================================
# 2.5 ➡️ Read and Pre-process Data in Spark
# ====================================
hdfs_path = "hdfs://localhost:9000/patient_data/cleaned/streamed_data.csv"

# Define the schema for your 50-column headerless CSV
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
    'diabetesMed', 'readmitted'
]

# ⬇️⬇️ THIS IS THE FIX ⬇️⬇️
# Use a StructType object instead of an invalid string
schema = StructType([StructField(c, StringType(), True) for c in all_columns])

# Read with header=False
df = spark.read.csv(hdfs_path, header=False, schema=schema)

print("➡️ Starting Spark Pre-processing...")

# --- 1. Replace all '?' with 'null' in feature columns ---
df_processed = df
for col_name in feature_columns:
    if col_name in df.columns: # Check if column exists
        df_processed = df_processed.withColumn(
            col_name,
            when(col(col_name) == '?', None).otherwise(col(col_name))
        )
print("✅ Step 1/4: Replaced all '?' with 'null'.")

# --- 2. Perform text-to-number mappings ---
df_processed = df_processed.withColumn(
    "gender",
    when(col("gender") == "Male", 1)
    .when(col("gender") == "Female", 0)
    .otherwise(-1)
).withColumn(
    "race",
    when(col("race") == "Caucasian", 0)
    .when(col("race") == "AfricanAmerican", 1)
    .when(col("race") == "Hispanic", 2)
    .when(col("race") == "Asian", 3)
    .when(col("race") == "Other", 4)
    .otherwise(0) 
).withColumn(
    "age",
    when(col("age") == "[0-10)", 5)
    .when(col("age") == "[10-20)", 15)
    .when(col("age") == "[20-30)", 25)
    .when(col("age") == "[30-40)", 35)
    .when(col("age") == "[40-50)", 45)
    .when(col("age") == "[50-60)", 55)
    .when(col("age") == "[60-70)", 65)
    .when(col("age") == "[70-80)", 75)
    .when(col("age") == "[80-90)", 85)
    .when(col("age") == "[90-100)", 95)
    .otherwise(0)
).withColumn(
    "change",
    when(col("change") == "Ch", 1)
    .when(col("change") == "No", 0)
    .otherwise(0)
).withColumn(
    "diabetesMed",
    when(col("diabetesMed") == "Yes", 1)
    .when(col("diabetesMed") == "No", 0)
    .otherwise(0)
)
# ... ADD ALL OTHER TEXT MAPPINGS HERE (insulin, metformin, etc.) ...
print("✅ Step 2/4: Performed text-to-number mappings.")

# --- 3. Cast ALL feature columns to a numeric type ---
for col_name in feature_columns:
    if col_name in df_processed.columns:
        df_processed = df_processed.withColumn(col_name, col(col_name).cast(IntegerType()))
print("✅ Step 3/4: Cast all feature columns to Integer.")

# --- 4. Fill all 'null' values with 0 ---
df_processed = df_processed.select(feature_columns).fillna(0)
print("✅ Step 4/4: Filled all 'null' values with 0.")
df_processed.show(5)

# ====================================
# 3️⃣ Convert to Pandas
# ====================================
pdf = df_processed.limit(50).toPandas()
print(f"Converted to pandas DataFrame with shape: {pdf.shape}")

# ====================================
# 4️⃣ Predict using the trained model
# ====================================
try:
    scaled_data = scaler.transform(pdf)
    predictions = model.predict(scaled_data)
    probabilities = model.predict_proba(scaled_data)[:, 1] * 100

    pdf["readmission_prediction"] = predictions
    pdf["readmission_probability(%)"] = probabilities

    print("✅ Predictions generated successfully!")
    print(pdf[['readmission_prediction', 'readmission_probability(%)']].head())

except Exception as e:
    print(f"❌ Prediction failed: {e}")
    spark.stop()
    exit(1)

# ====================================
# 5️⃣ Save results for dashboard
# ====================================
try:
    output_dir = os.path.dirname(__file__)
except NameError:
    output_dir = r"D:\bda project\patient-readmission-prediction-with-bda"
    print(f"⚠️  __file__ not defined. Saving to fallback directory: {output_dir}")

output_path = os.path.join(output_dir, "predicted_output.csv")
pdf.to_csv(output_path, index=False)
print(f"✅ Predictions saved to {output_path}")

# ====================================
# 6️⃣ Stop Spark
# ====================================
spark.stop()
print("🛑 Spark session stopped successfully!")