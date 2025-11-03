# 🏥 Patient Readmission Prediction using Big Data Technologies

This project predicts the **probability of patient readmission** using **Machine Learning** integrated with **Apache Kafka**, **Apache Spark**, and **Hadoop (HDFS)** for real-time big data processing.  
It includes two interactive dashboards:

1. **Data Input Dashboard** – for submitting patient details to Kafka.  
2. **Prediction Dashboard** – for viewing predictions streamed from Kafka via Spark ML.

---

## 🚀 Project Overview

### 🔄 Architecture Workflow

1. **User Input Dashboard** → Publishes patient details to a Kafka topic (`patient-input`).  
2. **Apache Kafka** → Acts as a real-time message broker for patient data.  
3. **Apache Spark Structured Streaming** → Reads from Kafka, applies a trained ML model, and predicts readmission.  
4. **HDFS (Hadoop Distributed File System)** → Stores both raw and processed (predicted) data.  
5. **Prediction Dashboard** → Displays real-time readmission results to users.

---

## 🧠 Technologies Used

| Component           | Technology     |
| ------------------- | -------------- |
| Programming Language | Python 3.10    |
| Frontend Framework  | Streamlit      |
| Machine Learning    | Scikit-learn   |
| Streaming Platform  | Apache Kafka   |
| Big Data Processing | Apache Spark   |
| Distributed Storage | Hadoop HDFS    |
| Data Format         | JSON / Parquet |

---

## 🗂️ Project Structure

