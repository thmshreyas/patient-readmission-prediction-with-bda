import streamlit as st
from kafka import KafkaProducer
import json
import time

st.set_page_config(layout="wide")
st.title("🏥 Patient Readmission Data Entry (Producer)")
st.write("Enter patient details below to send to the Kafka Stream.")

# Kafka Producer Setup
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    st.info("✅ Connected to Kafka topic 'patient_input'")
except Exception as e:
    st.error(f"❌ Failed to connect to Kafka: {e}")
    st.stop()

# --- We must send ALL the features your model was trained on ---
# --- These inputs must match your training data! ---

st.header("Patient Demographics")
col1, col2, col3 = st.columns(3)
with col1:
    race = st.selectbox("Race", ["Caucasian", "AfricanAmerican", "Hispanic", "Asian", "Other", "?"], index=0)
with col2:
    gender = st.selectbox("Gender", ["Male", "Female", "Unknown/Invalid"], index=1)
with col3:
    age = st.selectbox("Age Range", ["[70-80)", "[60-70)", "[50-60)", "[80-90)", "[40-50)", "[30-40)", "[90-100)", "[20-30)", "[10-20)", "[0-10)", "?"], index=0)

st.header("Admission & Discharge Details")
col1, col2, col3, col4 = st.columns(4)
with col1:
    admission_type_id = st.selectbox("Admission Type ID", ["1", "3", "2", "5", "6", "8", "4", "7", "?"], index=0)
with col2:
    discharge_disposition_id = st.selectbox("Discharge Disposition ID", ["1", "3", "6", "2", "5", "18", "4", "11", "7", "?"], index=0)
with col3:
    admission_source_id = st.selectbox("Admission Source ID", ["7", "1", "4", "6", "2", "17", "3", "5", "9", "?"], index=0)
with col4:
    time_in_hospital = st.number_input("Time in Hospital (days)", 1, 30, 4)

st.header("Medical Details")
col1, col2, col3, col4 = st.columns(4)
with col1:
    num_lab_procedures = st.number_input("Num Lab Procedures", 0, 150, 43)
with col2:
    num_procedures = st.number_input("Num Procedures", 0, 10, 1)
with col3:
    num_medications = st.number_input("Num Medications", 0, 100, 16)
with col4:
    number_diagnoses = st.number_input("Number of Diagnoses", 1, 20, 9)

st.header("Visit History")
col1, col2, col3 = st.columns(3)
with col1:
    number_outpatient = st.number_input("Number of Outpatient Visits", 0, 50, 0)
with col2:
    number_emergency = st.number_input("Number of Emergency Visits", 0, 50, 0)
with col3:
    number_inpatient = st.number_input("Number of Inpatient Visits", 0, 50, 0)

st.header("Key Vitals & Meds")
col1, col2, col3, col4 = st.columns(4)
with col1:
    diag_1 = st.text_input("Primary Diagnosis (diag_1)", "410")
with col2:
    diag_2 = st.text_input("Secondary Diagnosis (diag_2)", "411")
with col3:
    diag_3 = st.text_input("Tertiary Diagnosis (diag_3)", "250")
with col4:
    insulin = st.selectbox("Insulin", ["No", "Steady", "Up", "Down"], index=1)

# This dictionary creates the JSON data.
# The keys MUST match what your spark_streaming.py schema expects
data = {
    'encounter_id': str(int(time.time())), # Unique ID
    'patient_nbr': str(int(time.time()) + 123), # Unique ID
    'race': race,
    'gender': gender,
    'age': age,
    'weight': "?", # We left this out of the form
    'admission_type_id': admission_type_id,
    'discharge_disposition_id': discharge_disposition_id,
    'admission_source_id': admission_source_id,
    'time_in_hospital': str(time_in_hospital),
    'payer_code': "?",
    'medical_specialty': "?",
    'num_lab_procedures': str(num_lab_procedures),
    'num_procedures': str(num_procedures),
    'num_medications': str(num_medications),
    'number_outpatient': str(number_outpatient),
    'number_emergency': str(number_emergency),
    'number_inpatient': str(number_inpatient),
    'diag_1': diag_1,
    'diag_2': diag_2,
    'diag_3': diag_3,
    'number_diagnoses': str(number_diagnoses),
    'max_glu_serum': "None",
    'A1Cresult': "None",
    'metformin': "No",
    'repaglinide': "No",
    'nateglinide': "No",
    'chlorpropamide': "No",
    'glimepiride': "No",
    'acetohexamide': "No",
    'glipizide': "No",
    'glyburide': "No",
    'tolbutamide': "No",
    'pioglitazone': "No",
    'rosiglitazone': "No",
    'acarbose': "No",
    'miglitol': "No",
    'troglitazone': "No",
    'tolazamide': "No",
    'examide': "No",
    'citoglipton': "No",
    'insulin': insulin,
    'glyburide-metformin': "No",
    'glipizide-metformin': "No",
    'glimepiride-pioglitazone': "No",
    'metformin-rosiglitazone': "No",
    'metformin-pioglitazone': "No",
    'change': "No",
    'diabetesMed': "Yes",
    'readmitted': '?' # Add this key to match the schema
}

# Button
if st.button("🚀 Send Data to Kafka"):
    try:
        # Send to the 'patient_input' topic
        producer.send("patient_input", data)
        producer.flush()
        st.success(f"✅ Data for encounter {data['encounter_id']} sent to Kafka topic 'patient_input'")
        st.json(data)
    except Exception as e:
        st.error(f"❌ Failed to send to Kafka: {e}")

