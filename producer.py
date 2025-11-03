from kafka import KafkaProducer
import pandas as pd
import time
import json

# Initialize producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Load dataset
df = pd.read_csv('data/diabetic_data.csv')

# Limit for testing — stream only first 100 records (you can remove this later)
for i, (_, row) in enumerate(df.iterrows()):
    if i >= 100:
        break
    record = row.to_dict()
    producer.send('patient_readmission', value=record)
    print(f"Sent record {i+1}")
    time.sleep(0.2)  # small delay to simulate real-time streaming

print("✅ Finished streaming data.")
