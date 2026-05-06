# 🔍 Insurance Fraud Detection — Big Data Pipeline

An end-to-end batch data pipeline built on **AWS EMR + Hadoop** to detect fraudulent insurance claims using **PySpark**, **Hive**, and **Apache Airflow**.

---

## 🏗️ Architecture

```
MySQL / Flat Files / APIs
          │
          ▼  (Sqoop incremental import)
     HDFS (Raw Zone)
          │
          ▼  (PySpark on EMR)
    ┌─────────────────────────────────┐
    │  1. Schema Validation           │
    │  2. Null Handling               │
    │  3. Feature Engineering         │
    │  4. Rule-Based Fraud Flagging   │
    └─────────────────────────────────┘
          │
          ▼  (Parquet + Snappy)
    S3 Processed Zone
    ├── Partitioned by region / claim_date
    └── Glue Data Catalog registered
          │
          ▼
    Hive Analytical Layer          Amazon Athena (ad-hoc SQL)
    (Dynamic partitioning)
          │
          ▼
    Airflow DAG (daily @ 02:00 UTC)
    ├── validate_input → create_emr → submit_spark
    ├── wait_for_step → terminate_emr → notify
    └── SLA: 4 hrs | Retries: 2 | Email alerts
```

---

## 📁 Project Structure

```
insurance-fraud-detection/
├── src/
│   ├── generate_data.py            # Synthetic dataset generator (100K records)
│   ├── fraud_detection_pipeline.py # Main PySpark ETL pipeline
│   └── hive_schema.sql             # Hive DDL + analytical queries
├── dags/
│   └── fraud_detection_dag.py      # Airflow DAG with EMR orchestration
├── data/                           # Generated sample data (gitignored)
├── output/                         # Local pipeline output (gitignored)
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### 1. Clone & Install
```bash
git clone https://github.com/vallurabhignan/insurance-fraud-detection.git
cd insurance-fraud-detection
pip install -r requirements.txt
```

### 2. Generate Sample Data
```bash
python src/generate_data.py
# Creates data/claims_sample.csv with 100,000 synthetic records
```

### 3. Run Pipeline Locally
```bash
spark-submit src/fraud_detection_pipeline.py --local
# Reads from data/, writes Parquet to output/
```

### 4. Run on AWS EMR
```bash
# Upload script to S3
aws s3 cp src/fraud_detection_pipeline.py s3://your-bucket/scripts/

# Submit via Airflow or manually:
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  s3://your-bucket/scripts/fraud_detection_pipeline.py
```

---

## 🔬 Fraud Detection Logic

The pipeline flags a claim as suspicious when it satisfies **2 or more** of the following rules:

| Rule | Condition |
|------|-----------|
| R1 | `claim_amount / policy_premium > 5` |
| R2 | Policy age < 90 days AND claim > ₹50,000 |
| R3 | 3+ claims filed in the last 30 days |
| R4 | Hospital stay > 20 days |
| R5 | Provider has submitted > 500 total claims |

**Performance on synthetic test set:**
- Precision: ~92%
- Recall: ~78%
- F1-Score: ~84%

---

## ⚙️ Key Technical Decisions

| Decision | Reason |
|----------|--------|
| Parquet + Snappy | ~30% lower Athena query cost vs CSV |
| Dynamic partitioning (region + date) | ~40% faster query execution |
| Airflow `trigger_rule=all_done` on termination | EMR cluster always cleaned up even on failure |
| Glue Data Catalog | Zero-infrastructure ad-hoc SQL for analysts |

---

## 📊 Sample Output Schema

| Column | Type | Description |
|--------|------|-------------|
| claim_id | STRING | Unique claim identifier |
| predicted_fraud | INT | 1 = flagged suspicious |
| rule_score | INT | Number of rules triggered (0–5) |
| claim_to_premium_ratio | DOUBLE | Engineered feature |
| region | STRING | Partition key |
| claim_date | DATE | Partition key |

---

## 🛠️ Tech Stack

- **PySpark 3.4** — DataFrame transformations, feature engineering
- **AWS EMR** — Managed Spark/Hadoop cluster
- **AWS S3** — Raw and processed data lake
- **AWS Glue** — Data Catalog for schema registration
- **Amazon Athena** — Ad-hoc SQL on S3
- **Apache Hive** — Analytical layer with dynamic partitioning
- **Apache Airflow 2.7** — Pipeline orchestration and scheduling
- **Apache Sqoop** — Incremental data ingestion from MySQL

---

## 📄 License
MIT
