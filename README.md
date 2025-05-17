# 📊 Stream Healthcare Analytics Pipeline on GCP

This project demonstrates a real-time healthcare data processing pipeline using **Google Cloud Platform (GCP)** services. It streams data from **Pub/Sub** to **BigQuery** using **Dataflow**, and enables analytics and machine learning using **BigQuery ML** and visualization tools.

---

## 🧱 Project Architecture

1. **Data Source**: A healthcare dataset (CSV) initially uploaded to **Google Cloud Storage (GCS)**.
2. **Streaming Layer**: Data is published to a **Pub/Sub** topic.
3. **Data Processing**: A **Dataflow** streaming pipeline (template) reads the messages from Pub/Sub, parses JSON-formatted data, and writes to:
   - **Raw Table** in BigQuery.
   - **Aggregated Table** in BigQuery (e.g., group by medical condition, billing, etc.).
4. **Analytics**:
   - SQL queries for insights using **BigQuery**.
   - Data exploration via **BigQuery Table Preview** and **Looker Studio (optional)**.
5. **Machine Learning**:
   - ML models built and trained using **BigQuery ML** on the processed healthcare data.

---

## 🚀 Technologies Used

- **Google Cloud Pub/Sub** – Real-time messaging system
- **Google Cloud Dataflow** – Stream processing using Apache Beam templates
- **Google BigQuery** – Data warehousing, analytics, and ML
- **Google Cloud Storage** – Initial dataset storage
- **BigQuery ML** – For model training and predictions on healthcare data

---

## 🛠️ Steps to Reproduce

1. **Upload Data to GCS**:
   Upload your dataset (e.g., `healthcare_dataset.csv`) to a GCS bucket.

2. **Publish to Pub/Sub**:
   Convert each row to JSON and publish it to a Pub/Sub topic.

3. **Deploy Dataflow Streaming Job**:
   Use a **Dataflow template** (Pub/Sub to BigQuery) and modify it to include:
   - JSON parsing logic
   - Separate sinks for raw and aggregated data in BigQuery

4. **Verify Tables in BigQuery**:
   - `health_stream_records` (raw data)
   - `health_aggregates` (grouped/aggregated data)

5. **Run Queries and Build ML Models**:
   Use BigQuery SQL to analyze the data.
   Example: Predict high billing based on age, condition, etc., using `CREATE MODEL`.

---

## 📈 Sample Use Cases

- Track patient admission trends in real-time
- Analyze most common medical conditions
- Predict billing amounts using historical patterns
- Visualize KPIs in real time using Looker Studio


📝 Project Summary
In this project, I implemented both batch and streaming data pipelines on Google Cloud Platform (GCP):

✅ Batch Pipeline: Loaded a healthcare dataset from Google Cloud Storage (GCS) into BigQuery using Dataflow (batch mode). This was fast and efficient due to bulk processing.

🌐 Streaming Pipeline: Streamed the same dataset to Pub/Sub, processed it in real time using a Dataflow (streaming) pipeline, parsed JSON, and loaded both raw and aggregated records into BigQuery.

🔍 Key Observation
The batch pipeline completed much faster, while the streaming pipeline took more time due to:

Continuous event processing

Overheads like windowing, watermarking, and real-time coordination

Small test dataset not leveraging streaming pipeline's full potential

This highlighted the performance and complexity trade-offs between batch and streaming architectures.
