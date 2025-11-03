import streamlit as st
import pandas as pd
import numpy as np
import joblib
import os

# ================================
# 1. Load Model, Scaler & Features
# ================================
st.set_page_config(page_title="Patient Readmission Prediction Dashboard", layout="centered")
st.title("🏥 Patient Readmission Prediction Dashboard")
st.markdown("Predict whether a patient is likely to be readmitted based on clinical details.")

# Paths
base_dir = os.path.dirname(os.path.abspath(__file__))
model_dir = os.path.join(base_dir, "..", "models")

try:
    model = joblib.load(os.path.join(model_dir, "readmission_model.pkl"))
    scaler = joblib.load(os.path.join(model_dir, "scaler.pkl"))
    feature_columns = joblib.load(os.path.join(model_dir, "feature_columns.pkl"))
    st.success(f"✅ Model loaded successfully with {len(feature_columns)} features.")
except Exception as e:
    st.error(f"❌ Failed to load model or scaler: {e}")
    st.stop()

# ================================
# 2. User Input Section
# ================================
st.header("🧍 Enter Patient Details")

gender = st.selectbox("Gender", ["Male", "Female"])
age = st.slider("Age", 0, 100, 45)
admission_type = st.selectbox("Admission Type", ["Emergency", "Urgent", "Elective", "Newborn", "Trauma"])
discharge_disposition = st.selectbox("Discharge Disposition", ["Home", "Transferred", "Expired", "Rehab"])
time_in_hospital = st.slider("Time in Hospital (days)", 1, 14, 5)
num_lab_procedures = st.number_input("Number of Lab Procedures", 0, 150, 40)
num_procedures = st.number_input("Number of Procedures", 0, 20, 2)
num_medications = st.number_input("Number of Medications", 0, 100, 10)
number_outpatient = st.number_input("Number of Outpatient Visits", 0, 20, 1)
number_emergency = st.number_input("Number of Emergency Visits", 0, 10, 0)
number_inpatient = st.number_input("Number of Inpatient Visits", 0, 10, 0)

# Encode categorical inputs
admission_map = {"Emergency": 1, "Urgent": 2, "Elective": 3, "Newborn": 4, "Trauma": 5}
discharge_map = {"Home": 1, "Transferred": 2, "Expired": 3, "Rehab": 4}

input_data = {
    'gender': [1 if gender == "Male" else 0],
    'age': [age],
    'admission_type_id': [admission_map[admission_type]],
    'discharge_disposition_id': [discharge_map[discharge_disposition]],
    'time_in_hospital': [time_in_hospital],
    'num_lab_procedures': [num_lab_procedures],
    'num_procedures': [num_procedures],
    'num_medications': [num_medications],
    'number_outpatient': [number_outpatient],
    'number_emergency': [number_emergency],
    'number_inpatient': [number_inpatient],
}

# Convert input to DataFrame
input_df = pd.DataFrame(input_data)

# Align with training features (fill missing columns with 0)
for col in feature_columns:
    if col not in input_df.columns:
        input_df[col] = 0
input_df = input_df[feature_columns]

# ================================
# 3. Prediction Section
# ================================
if st.button("🔍 Predict Readmission"):
    try:
        scaled_input = scaler.transform(input_df)
        prediction = model.predict(scaled_input)[0]
        probability = model.predict_proba(scaled_input)[0][1] * 100  # Probability of readmission

        pred_label = "Readmitted" if prediction == 1 else "Not Readmitted"
        st.subheader("📊 Prediction Result")
        st.success(f"**Prediction:** {pred_label}")
        st.info(f"**Readmission Probability:** {probability:.2f}%")

        # ================================
        # 4. Save Streamed Data
        # ================================
        result_row = input_df.copy()
        result_row["readmission_prediction"] = prediction
        result_row["readmission_probability(%)"] = probability

        csv_path = os.path.join(base_dir, "streamed_data.csv")
        if not os.path.exists(csv_path):
            result_row.to_csv(csv_path, index=False)
        else:
            result_row.to_csv(csv_path, mode='a', header=False, index=False)

        st.success(f"📁 Data saved to streamed_data.csv")

    except Exception as e:
        st.error(f"Prediction failed: {e}")
