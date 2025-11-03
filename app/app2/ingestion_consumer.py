from kafka import KafkaConsumer
import json
import subprocess
import os

# Paths
HADOOP_BIN = r"D:\hadoop\hadoop\bin\hdfs.cmd"  # Your working HDFS command
UPLOAD_PATH = '/patient_data/raw_records/' # Your "raw" folder
LOCAL_FILE = 'temp_raw_data.csv'

# Create Kafka Consumer
consumer = KafkaConsumer(
    'patient_readmission', # Topic from your producer
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest' # Start from the beginning
)

print("✅ Ingestion Consumer started. Listening for patient data...")
print(f"Will upload new records to HDFS at {UPLOAD_PATH}")

# Create the HDFS folder if it doesn't exist
subprocess.run([HADOOP_BIN, 'dfs', '-mkdir', '-p', UPLOAD_PATH], shell=True)

for message in consumer:
    data = message.value
    print(f"Processing message for encounter: {data.get('encounter_id')}")
    
    # Create the CSV line. We need to be careful with column order.
    # Get all values in the order of your producer's 'data' dict
    record = ','.join([str(v) for v in data.values()])
    
    # Write this single record to a local temp file
    # 'w' mode overwrites the file for each new message
    with open(LOCAL_FILE, 'w', encoding='utf-8') as f:
        # We need a header for Spark to read it
        f.write(','.join(data.keys()) + '\n') # Write header
        f.write(record + '\n') # Write data

    # Create a unique filename for HDFS
    hdfs_filename = f"record_{data.get('encounter_id', 'unknown')}.csv"
    
    # Upload this single file to HDFS
    # This uses '-f' to overwrite if it exists, which is fine
    subprocess.run([
        HADOOP_BIN, 'dfs', '-put', '-f', LOCAL_FILE, os.path.join(UPLOAD_PATH, hdfs_filename).replace("\\", "/")
    ], shell=True)

    print(f"📤 Uploaded record {hdfs_filename} to HDFS.")

    # Clean up local file
    if os.path.exists(LOCAL_FILE):
        os.remove(LOCAL_FILE)
