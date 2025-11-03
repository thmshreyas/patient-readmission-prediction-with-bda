import streamlit as st
import pandas as pd
from hdfs import InsecureClient
import os

st.set_page_config(layout="wide")
st.title("📈 Readmission Prediction Dashboard (Results)")
st.write("This dashboard reads predictions from the Spark batch job.")

# --- HDFS Path (Primary) ---
HDFS_PREDICTION_PATH = '/patient_data/cleaned_predictions'
BASE_HDFS_PATH = '/patient_data'

# --- Local Path (Workaround) ---
LOCAL_PREDICTION_FILE = r"D:\bda_output\predictions.csv"

# Connect to HDFS
try:
    # Connect to the HDFS Web UI port (default 9870)
    client = InsecureClient('http://localhost:9870', user='hadoop')
    st.info(f"Connected to HDFS at http://localhost:9870")
    HDFS_CONNECTED = True
except Exception as e:
    st.error(f"❌ Failed to connect to HDFS: {e}")
    HDFS_CONNECTED = False

@st.cache_data(ttl=10) # Cache data for 10 seconds
def load_data():
    # --- Try to load from HDFS first ---
    if HDFS_CONNECTED:
        try:
            # Check if base /patient_data/ folder exists
            if BASE_HDFS_PATH.split('/')[-1] not in client.list('/'):
                 st.warning(f"HDFS base folder {BASE_HDFS_PATH} not found.")
                 raise FileNotFoundError("HDFS base folder not found")
            
            # Check if the /cleaned_predictions/ folder exists inside /patient_data/
            if HDFS_PREDICTION_PATH.split('/')[-1] not in client.list(BASE_HDFS_PATH):
                st.warning("HDFS prediction folder not found. Running Spark job?")
                raise FileNotFoundError("HDFS prediction folder not found")

            # List all CSV files in the prediction folder
            files = [f for f in client.list(HDFS_PREDICTION_PATH) if f.endswith('.csv')]
            if not files:
                st.info("No predictions found in HDFS.")
                raise FileNotFoundError("No CSVs in HDFS prediction folder")

            # Read all CSVs and combine them
            df_list = []
            for file in files:
                file_path = os.path.join(HDFS_PREDICTION_PATH, file)
                with client.read(file_path, encoding='utf-8') as reader:
                    df_list.append(pd.read_csv(reader))
            
            if df_list:
                full_df = pd.concat(df_list, ignore_index=True)
                full_df = full_df.sort_values(by='encounter_id', ascending=False)
                # Drop duplicates based on patient, keeping the most recent record
                full_df = full_df.drop_duplicates(subset=['patient_nbr'], keep='first')
                st.success("Loaded predictions from HDFS.")
                return full_df

        except Exception as e:
            st.error(f"HDFS Read Failed: {e}. Trying local workaround...")
    
    # --- WORKAROUND: Load from local file ---
    try:
        if os.path.exists(LOCAL_PREDICTION_FILE):
            full_df = pd.read_csv(LOCAL_PREDICTION_FILE)
            full_df = full_df.sort_values(by='encounter_id', ascending=False)
            full_df = full_df.drop_duplicates(subset=['patient_nbr'], keep='first')
            st.success(f"Loaded predictions from LOCAL file: {LOCAL_PREDICTION_FILE}")
            return full_df
        else:
            st.info(f"Local file {LOCAL_PREDICTION_FILE} not found.")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error reading from local file: {e}")
        return pd.DataFrame()

# --- Dashboard UI ---
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()

df = load_data()

if not df.empty:
    st.dataframe(df)
    
    # Show stats
    # Check your model's output. The log shows '2' as a prediction, which might mean '>30'
    # Let's assume 1 is '<30' (high risk) and 2 is '>30' (low risk)
    # Adjust this logic based on your model's output
    high_risk_count = df[df['readmission_prediction'] == 1].shape[0] # Assuming 1 is high risk
    if len(df) > 0:
        high_risk_percent = (high_risk_count / len(df)) * 100
        st.subheader("Prediction Summary")
        st.metric(label="Total Patients Processed", value=len(df))
        st.metric(label="High Risk Patients", value=f"{high_risk_count} ({high_risk_percent:.1f}%)")
else:
    st.info("Waiting for data... (1) Send data from producer. (2) Run the spark_prediction_job.py. (3) Refresh this dashboard.")

