from kafka import KafkaConsumer
import json
import subprocess
import time
import os

# Paths
HADOOP_BIN = r"D:\hadoop\hadoop\bin\hdfs.cmd"  # ✅ full path to hdfs.cmd
BATCH_SIZE = 50
UPLOAD_PATH = '/patient_data/cleaned/'
LOCAL_FILE = 'streamed_data.csv'

# Create Kafka Consumer
consumer = KafkaConsumer(
    'patient_readmission',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("✅ Consumer started. Listening for patient data...")

batch = []

for message in consumer:
    data = message.value
    record = ','.join([str(v) for v in data.values()])
    batch.append(record)

    if len(batch) >= BATCH_SIZE:
        with open(LOCAL_FILE, 'a', encoding='utf-8') as f:
            f.write('\n'.join(batch) + '\n')

        batch = []  # clear after writing

        # Upload to HDFS using full path
        subprocess.run([
            HADOOP_BIN, 'dfs', '-put', '-f', LOCAL_FILE, UPLOAD_PATH
        ], shell=True)

        print(f"📤 Uploaded {BATCH_SIZE} records to HDFS.")
        time.sleep(2)  # small delay to reduce load
