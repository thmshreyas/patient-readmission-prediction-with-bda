

---


# 🏥 Patient Readmission Prediction using Big Data Technologies

This project predicts the **probability of patient readmission** using **Machine Learning** integrated with **Apache Kafka**, **Apache Spark**, and **Hadoop (HDFS)** for real-time Big Data processing.  

It features two interactive dashboards:
1. **Data Input Dashboard** – to send patient details to Kafka  
2. **Prediction Dashboard** – to view predictions streamed from Kafka via Spark ML  

---

## 🚀 Project Overview

### 🔄 Architecture Workflow

1. **User Input Dashboard** → Publishes patient details to a Kafka topic (`patient-input`)  
2. **Apache Kafka** → Acts as a real-time message broker for patient data  
3. **Apache Spark Structured Streaming** → Reads from Kafka, applies a trained ML model, and predicts readmission  
4. **HDFS (Hadoop Distributed File System)** → Stores both raw and processed (predicted) data  
5. **Prediction Dashboard** → Displays real-time readmission results to users  

---

## 🧠 Technologies Used

| Component            | Technology     |
| -------------------- | -------------- |
| Programming Language | Python 3.10    |
| Frontend Framework   | Streamlit      |
| Machine Learning     | Scikit-learn   |
| Streaming Platform   | Apache Kafka   |
| Big Data Processing  | Apache Spark   |
| Distributed Storage  | Hadoop HDFS    |
| Data Format          | JSON / Parquet |

---

## 🗂️ Project Structure

```

.
├── app/
│   ├── app2/
│   ├── display_dashboard.py
│   ├── ingestion_consumer.py
│   ├── producer_dashboard.py
│   ├── spark_job.py
│   ├── dashboard.py
│   ├── dashboard_input.py
│   ├── dashboard_predict.py
│   ├── predicted_output.csv
│   ├── spark_streaming.py
│   ├── spark_to_dashboard.py
│   └── streamed_data.csv
│
├── data/
│   ├── IDS_mapping.csv
│   ├── cleaned_patient_data.csv
│   └── diabetic_data.csv
│
├── notebooks/
│   ├── data_preprocessing.ipynb
│   ├── feature_columns.pkl
│   ├── model_evaluation.ipynb
│   ├── model_training.ipynb
│   ├── readmission_model.pkl
│   └── visualisation.ipynb
│
└── src/
├── .env
├── .gitattributes
├── README.md
├── consumer.py
├── patient_kafka_stream.py
├── producer.py
├── read_sample_data.py
├── scaler.pkl
├── spark_kafka_stream.py
├── spark_test_java.py
└── streamed_data.csv

````

---

## ⚙️ Setup Instructions

### 1️⃣ Install Dependencies

Clone the repository and create a virtual environment:

```bash
git clone https://github.com/<your-username>/patient-readmission-prediction-with-bda.git
cd patient-readmission-prediction-with-bda

python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
````

---

### 2️⃣ Start Hadoop

Start Hadoop DFS and YARN:

```bash
start-dfs.cmd
start-yarn.cmd
```

Access Hadoop UI: [http://localhost:9870](http://localhost:9870)

---

### 3️⃣ Start Zookeeper and Kafka

Open two separate terminals and run:

```bash
# Terminal 1
zookeeper-server-start.bat config\zookeeper.properties

# Terminal 2
kafka-server-start.bat config\server.properties
```

---

### 4️⃣ Create Kafka Topics

```bash
kafka-topics.bat --create --topic patient-input --bootstrap-server localhost:9092
kafka-topics.bat --create --topic patient-predictions --bootstrap-server localhost:9092
```

Verify topics:

```bash
kafka-topics.bat --list --bootstrap-server localhost:9092
```

---

### 5️⃣ Run Spark Streaming Job

Start Spark Structured Streaming to consume Kafka messages and store predictions in HDFS:

```bash
python app\spark_kafka_consumer.py
```

Expected logs:

```
✅ Spark session started successfully  
✅ Model loaded successfully  
✅ Connected to Kafka topic: patient-input  
```

---

### 6️⃣ Run Dashboards

#### 🩺 Input Dashboard

This dashboard allows users to input patient details and publish them to Kafka.

```bash
streamlit run app/dashboard_input.py
```

Access at: [http://localhost:8501](http://localhost:8501)

#### 📊 Prediction Dashboard

Displays live readmission predictions streamed from Kafka.

```bash
streamlit run app/dashboard_predict.py
```

Access at: [http://localhost:8502](http://localhost:8502)

---

## 🧩 Example Input Format

| Feature            | Example   |
| ------------------ | --------- |
| race               | Caucasian |
| gender             | Male      |
| age                | 60        |
| time_in_hospital   | 10        |
| num_lab_procedures | 35        |
| num_procedures     | 2         |
| num_medications    | 25        |
| number_outpatient  | 0         |
| number_emergency   | 0         |
| number_inpatient   | 1         |
| number_diagnoses   | 5         |

---

## 🧮 Model Details

* **Algorithm:** Random Forest Classifier
* **Libraries:** Scikit-learn, Pandas, NumPy
* **Input Features:** 11 clinical and demographic variables
* **Output:** Probability of readmission (`Yes` / `No`)

---

## 📁 Data Flow Diagram

```
┌────────────┐        ┌────────────┐       ┌────────────┐
│ Streamlit  │──────▶ │ Apache     │──────▶│ Apache     │
│ Dashboard  │        │ Kafka      │       │ Spark      │
└────────────┘        └────────────┘       │ (Prediction│
                                            │ + HDFS Write)
                                            └────────────┘
                                                    │
                                                    ▼
                                               ┌────────┐
                                               │  HDFS  │
                                               │ Output │
                                               └────────┘
```

---

## 🧾 Output

After each prediction, results are stored in:

```
hdfs://localhost:9870/user/hadoop/patient_predictions/
```

Example output:

```json
{
  "patient_id": "12345",
  "age": 60,
  "gender": "Male",
  "num_medications": 25,
  "num_procedures": 2,
  "prediction": "Readmitted"
}
```

---

## 🧰 Troubleshooting

| Issue                                           | Possible Cause                      | Solution                                                       |
| ----------------------------------------------- | ----------------------------------- | -------------------------------------------------------------- |
| `kafka-topics.bat not recognized`               | Kafka not added to PATH             | Add Kafka `bin/windows` to system PATH                         |
| `ValueError: could not convert string to float` | Missing categorical encoding        | Ensure preprocessing (LabelEncoder / OneHotEncoder) is applied |
| Spark can’t read Kafka topic                    | Kafka not running or topic mismatch | Restart Kafka and verify topic names                           |
| HDFS folder not visible                         | Spark job not yet completed         | Wait until Spark microbatch writes output (check logs)         |

---

## ✨ Future Enhancements

* 🐳 Deploy with **Docker Compose** for one-click setup
* 🔁 Add **model retraining pipeline** using Spark MLlib
* 📈 Integrate **real-time visualization** with Kafka Streams and Plotly Dash

---

## 👨‍💻 Contributor

**Shreyas T H M** – Project Lead & Developer

---

## 📝 License

Licensed under the **MIT License** – feel free to use, modify, and share with attribution.

---

### 🎯 End Goal

An **intelligent, real-time Big Data system** capable of predicting hospital readmissions by combining AI-driven insights with distributed data processing.

---

```

---

```
