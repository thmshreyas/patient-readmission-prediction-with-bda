import streamlit as st
import pandas as pd
from hdfs import InsecureClient
import os

st.set_page_config(layout="wide")
st.title("📈 Real-time Readmission Predictions")
st.write("This dashboard reads live predictions from the HDFS folder.")

# HDFS Path where Spark saves results
HDFS_PREDICTION_PATH = '/patient_data/streaming_predictions'

# Connect to HDFS
try:
    # Connect to the HDFS Web UI port
    client = InsecureClient('http://localhost:9870', user='hadoop')
    st.info(f"Connected to HDFS at {HDFS_PREDICTION_PATH}")
except Exception as e:
    st.error(f"❌ Failed to connect to HDFS: {e}")
    st.stop()

@st.cache_data(ttl=10) # Cache data for 10 seconds
def load_data_from_hdfs():
    try:
        # Check if the base directory exists
        if '/patient_data' not in client.list('/'):
             st.error("Base directory /patient_data not found in HDFS.")
             return pd.DataFrame()
             
        # Check if the streaming_predictions directory exists
        if HDFS_PREDICTION_PATH.split('/')[-1] not in client.list('/patient_data/'):
            st.warning("Prediction folder doesn't exist yet. Send data from the producer.")
            return pd.DataFrame()

        # Get all .csv files from the prediction directory
        files = [f for f in client.list(HDFS_PREDICTION_PATH) if f.endswith('.csv')]
        
        if not files:
            st.info("No predictions found. Send data from the producer.")
            return pd.DataFrame()

        # Read all CSVs and combine them
        df_list = []
        for file in files:
            file_path = os.path.join(HDFS_PREDICTION_PATH, file)
            with client.read(file_path, encoding='utf-8') as reader:
                df = pd.read_csv(reader)
                df_list.append(df)
        
        if not df_list:
            return pd.DataFrame()
            
        full_df = pd.concat(df_list, ignore_index=True)
        # Remove duplicates, keeping the latest one (based on encounter_id)
        full_df = full_df.sort_values(by='encounter_id', ascending=False)
        full_df = full_df.drop_duplicates(subset=['patient_nbr'], keep='first') # Show latest for each patient
        return full_df

    except Exception as e:
        st.error(f"Error reading from HDFS: {e}")
        return pd.DataFrame()

# --- Dashboard UI ---
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()

df = load_data_from_hdfs()

if not df.empty:
    st.success(f"Loaded {len(df)} unique predictions from HDFS.")
    
    # Display predictions
    st.dataframe(df.sort_values(by='encounter_id', ascending=False))
    
    # Show some stats
    if not df.empty:
        # Check your model's output. 
        # The log shows '2' as a prediction, which might mean '>30'
        # Let's assume 1 is '<30' (high risk) and 2 is '>30' (low risk)
        high_risk_count = df[df['readmission_prediction'] == 1].shape[0]
        if len(df) > 0:
            high_risk_percent = (high_risk_count / len(df)) * 100
            st.subheader("Prediction Summary")
            st.metric(label="Total Patients Processed", value=len(df))
            st.metric(label="High Risk Patients (<30)", value=f"{high_risk_count} ({high_risk_percent:.1f}%)")
            
else:
    st.info("Waiting for data. Please send data from the 'producer.py' dashboard.")
    st.write("The Spark Streaming job must also be running in your terminal.")
