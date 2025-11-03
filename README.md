# 🏥 Patient Readmission Prediction using Big Data Technologies

This project predicts **patient readmission probability** using **Machine Learning** integrated with **Apache Kafka**, **Apache Spark**, and **Hadoop (HDFS)** for real-time big data processing.
It includes two dashboards:

1. **Data Input Dashboard** – to send patient details to Kafka.
2. **Prediction Dashboard** – to display predictions streamed from Kafka via Spark ML.

---

## 🚀 Project Overview

### Architecture Workflow

1. **User Input Dashboard** → Publishes patient details to a Kafka topic (`patient-input`).
2. **Apache Kafka** → Acts as a message broker for streaming patient data.
3. **Apache Spark Structured Streaming** → Reads from Kafka, processes and predicts using a trained ML model.
4. **HDFS (Hadoop Distributed File System)** → Stores both raw and processed data.
5. **Prediction Dashboard** → Displays the predicted readmission status to users.

---

## 🧠 Technologies Used

| Component           | Technology     |
| ------------------- | -------------- |
| Programming         | Python 3.10    |
| Frontend            | Streamlit      |
| Machine Learning    | scikit-learn   |
| Streaming           | Apache Kafka   |
| Big Data Processing | Apache Spark   |
| Storage             | Hadoop HDFS    |
| Data Serialization  | JSON / Parquet |

---

## 🗂️ Project Structure

```
patient-readmission-prediction-with-bda/
│
├── app/
│   ├── dashboard_input.py           # Streamlit UI for data entry (publishes to Kafka)
│   ├── dashboard_predict.py         # Streamlit UI for showing predictions
│   ├── kafka_producer.py            # Sends user input to Kafka topic
│   ├── spark_kafka_consumer.py      # Spark job to read from Kafka, predict, and store in HDFS
│   ├── model.pkl                    # Trained RandomForest model
│   └── preprocessing.py             # Handles encoding, feature scaling, etc.
│
├── data/
│   ├── patient_data.csv             # Example dataset used for model training
│   ├── predictions/                 # HDFS target output (can be linked)
│
├── notebooks/
│   └── model_training.ipynb         # ML model training notebook
│
├── requirements.txt
├── README.md
└── .env                             # (Optional) Environment variables for paths, Kafka server, etc.
```

---

## ⚙️ Setup Instructions

### 1️⃣ Install Dependencies

Clone the repo and create a virtual environment:

```bash
git clone https://github.com/<your-username>/patient-readmission-prediction-with-bda.git
cd patient-readmission-prediction-with-bda

python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 2️⃣ Start Hadoop

Make sure Hadoop is running:

```bash
start-dfs.cmd
start-yarn.cmd
```

Access the Hadoop UI at:

```
http://localhost:9870
```

### 3️⃣ Start Zookeeper and Kafka

Open new terminals and run:

```bash
# Terminal 1: Start Zookeeper
zookeeper-server-start.bat config\zookeeper.properties

# Terminal 2: Start Kafka broker
kafka-server-start.bat config\server.properties
```

### 4️⃣ Create Kafka Topics

```bash
kafka-topics.bat --create --topic patient-input --bootstrap-server localhost:9092
kafka-topics.bat --create --topic patient-predictions --bootstrap-server localhost:9092
```

To verify:

```bash
kafka-topics.bat --list --bootstrap-server localhost:9092
```

### 5️⃣ Run Spark Streaming Job

Start the Spark consumer to process Kafka messages and store results in HDFS:

```bash
python app\spark_kafka_consumer.py
```

You should see logs like:

```
✅ Spark session started successfully
✅ Model loaded successfully
✅ Connected to Kafka topic: patient-input
```

### 6️⃣ Run Dashboards

#### 🩺 Input Dashboard

This dashboard allows users to input patient data and send it to Kafka.

```bash
streamlit run app/dashboard_input.py
```

Access it at: [http://localhost:8501](http://localhost:8501)

#### 📊 Prediction Dashboard

Displays predicted readmission results (live updates).

```bash
streamlit run app/dashboard_predict.py
```

Access it at: [http://localhost:8502](http://localhost:8502)

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
* **Libraries:** scikit-learn, pandas, numpy
* **Input Features:** 11 clinical and demographic attributes
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
                                               │ HDFS   │
                                               │ Output │
                                               └────────┘
```

---

## 🧾 Output

After each prediction, results are stored in:

```
hdfs://localhost:9870/user/hadoop/patient_predictions/
```

Each record includes:

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

| Issue                                           | Cause                                    | Fix                                                          |
| ----------------------------------------------- | ---------------------------------------- | ------------------------------------------------------------ |
| `kafka-topics.bat not recognized`               | Kafka not in PATH                        | Add Kafka `bin/windows` to system PATH                       |
| `ValueError: could not convert string to float` | Categorical encoding missing             | Ensure `LabelEncoder`/`OneHotEncoder` used before prediction |
| Spark can’t read Kafka topic                    | Topic name mismatch or Kafka not running | Restart Kafka and re-check topic name                        |
| HDFS folder not visible                         | Spark hasn’t written yet                 | Wait until Spark microbatch writes output (check logs)       |

---

## ✨ Future Enhancements

* Deploy using **Docker Compose** for one-click setup.
* Add **model retraining pipeline** using Spark MLlib.
* Include **real-time visualization** using Kafka Streams dashboard.

---

## 👨‍💻 Contributors

* **Your Name** – Project Lead & Developer
* (Add your teammates if applicable)

---

## 📝 License

This project is licensed under the **MIT License** – feel free to use and modify with credit.

---

**🎯 End Goal:**
An intelligent, real-time big data system capable of predicting hospital readmissions using integrated AI and Big Data pipelines.
