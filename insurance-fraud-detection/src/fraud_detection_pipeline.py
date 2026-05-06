"""
fraud_detection_pipeline.py
----------------------------
PySpark batch pipeline for insurance fraud detection.

Steps:
  1. Ingest raw CSV claims from S3 / local path
  2. Schema validation & null handling
  3. Feature engineering
  4. Rule-based anomaly detection (fraud flagging)
  5. Write Parquet output partitioned by region + claim_date
  6. Register in Glue Data Catalog (when running on AWS EMR)

Run locally:
    spark-submit src/fraud_detection_pipeline.py --local

Run on EMR:
    spark-submit --master yarn src/fraud_detection_pipeline.py
"""

import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, DateType
)

# ── LOGGING ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
log = logging.getLogger("FraudPipeline")

# ── SCHEMA ───────────────────────────────────────────────────────────────────
CLAIMS_SCHEMA = StructType([
    StructField("claim_id",               StringType(),  False),
    StructField("policy_holder_id",       StringType(),  True),
    StructField("provider_id",            StringType(),  True),
    StructField("claim_type",             StringType(),  True),
    StructField("claim_amount",           DoubleType(),  True),
    StructField("claim_date",             StringType(),  True),   # cast later
    StructField("region",                 StringType(),  True),
    StructField("num_claims_last_30d",    IntegerType(), True),
    StructField("days_since_policy_start",IntegerType(), True),
    StructField("policy_premium",         DoubleType(),  True),
    StructField("hospital_duration_days", IntegerType(), True),
    StructField("is_fraud",               IntegerType(), True),   # ground truth label
])

# ── CONFIG ────────────────────────────────────────────────────────────────────
S3_INPUT   = "s3://insurance-fraud-pipeline/raw/claims/"
S3_OUTPUT  = "s3://insurance-fraud-pipeline/processed/claims_flagged/"
LOCAL_INPUT  = "data/claims_sample.csv"
LOCAL_OUTPUT = "output/claims_flagged"


def build_spark(local: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("InsuranceFraudDetection")
        .config("spark.sql.shuffle.partitions", "50")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if local:
        builder = builder.master("local[*]")
    return builder.getOrCreate()


# ── STEP 1 : INGEST ──────────────────────────────────────────────────────────
def ingest(spark: SparkSession, path: str):
    log.info(f"Ingesting data from: {path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .schema(CLAIMS_SCHEMA)
        .csv(path)
    )
    log.info(f"  Raw record count : {df.count():,}")
    return df


# ── STEP 2 : VALIDATE & CLEAN ────────────────────────────────────────────────
def validate_and_clean(df):
    log.info("Running schema validation and null handling...")

    initial_count = df.count()

    # Drop rows missing critical fields
    df = df.dropna(subset=["claim_id", "claim_amount", "region", "claim_date"])

    # Fill optional nulls with safe defaults
    df = df.fillna({
        "provider_id":             "UNKNOWN",
        "policy_holder_id":        "UNKNOWN",
        "claim_type":              "OTHER",
        "num_claims_last_30d":     0,
        "days_since_policy_start": 0,
        "hospital_duration_days":  0,
        "policy_premium":          0.0,
    })

    # Cast claim_date to DateType
    df = df.withColumn("claim_date", F.to_date("claim_date", "yyyy-MM-dd"))

    # Remove negative amounts
    df = df.filter(F.col("claim_amount") > 0)

    # Standardise region casing
    df = df.withColumn("region", F.initcap(F.col("region")))

    dropped = initial_count - df.count()
    log.info(f"  Dropped {dropped:,} invalid rows. Clean count: {df.count():,}")
    return df


# ── STEP 3 : FEATURE ENGINEERING ────────────────────────────────────────────
def engineer_features(df):
    log.info("Engineering features...")

    df = df.withColumn(
        "claim_to_premium_ratio",
        F.round(F.col("claim_amount") / (F.col("policy_premium") + 1), 4)
    )

    df = df.withColumn(
        "is_new_policy",
        (F.col("days_since_policy_start") < 90).cast(IntegerType())
    )

    df = df.withColumn(
        "is_high_claim",
        (F.col("claim_amount") > 100_000).cast(IntegerType())
    )

    df = df.withColumn(
        "is_frequent_claimer",
        (F.col("num_claims_last_30d") >= 3).cast(IntegerType())
    )

    # Provider-level claim frequency (potential fraud ring signal)
    provider_stats = (
        df.groupBy("provider_id")
          .agg(F.count("claim_id").alias("provider_claim_count"))
    )
    df = df.join(provider_stats, on="provider_id", how="left")

    df = df.withColumn(
        "is_high_volume_provider",
        (F.col("provider_claim_count") > 500).cast(IntegerType())
    )

    return df


# ── STEP 4 : RULE-BASED FRAUD FLAGGING ──────────────────────────────────────
def apply_fraud_rules(df):
    """
    Flag a claim as suspicious if it meets 2+ of the following conditions:
      R1 — claim_to_premium_ratio > 5
      R2 — is_new_policy AND claim_amount > 50,000
      R3 — num_claims_last_30d >= 3
      R4 — hospital_duration_days > 20
      R5 — is_high_volume_provider
    """
    log.info("Applying rule-based fraud detection...")

    r1 = (F.col("claim_to_premium_ratio") > 5).cast(IntegerType())
    r2 = ((F.col("is_new_policy") == 1) & (F.col("claim_amount") > 50_000)).cast(IntegerType())
    r3 = F.col("is_frequent_claimer")
    r4 = (F.col("hospital_duration_days") > 20).cast(IntegerType())
    r5 = F.col("is_high_volume_provider")

    df = df.withColumn("rule_score", r1 + r2 + r3 + r4 + r5)
    df = df.withColumn(
        "predicted_fraud",
        (F.col("rule_score") >= 2).cast(IntegerType())
    )

    flagged = df.filter(F.col("predicted_fraud") == 1).count()
    total   = df.count()
    log.info(f"  Flagged {flagged:,} / {total:,} claims as suspicious ({flagged/total*100:.2f}%)")
    return df


# ── STEP 5 : EVALUATE (when ground-truth labels available) ──────────────────
def evaluate(df):
    if "is_fraud" not in df.columns:
        return
    log.info("Evaluating against ground-truth labels...")

    tp = df.filter((F.col("predicted_fraud") == 1) & (F.col("is_fraud") == 1)).count()
    fp = df.filter((F.col("predicted_fraud") == 1) & (F.col("is_fraud") == 0)).count()
    fn = df.filter((F.col("predicted_fraud") == 0) & (F.col("is_fraud") == 1)).count()

    precision = tp / (tp + fp + 1e-9)
    recall    = tp / (tp + fn + 1e-9)
    f1        = 2 * precision * recall / (precision + recall + 1e-9)

    log.info(f"  Precision : {precision:.4f}")
    log.info(f"  Recall    : {recall:.4f}")
    log.info(f"  F1-Score  : {f1:.4f}")


# ── STEP 6 : WRITE OUTPUT ────────────────────────────────────────────────────
def write_output(df, output_path: str):
    log.info(f"Writing Parquet output to: {output_path}")
    (
        df.write
          .mode("overwrite")
          .partitionBy("region", "claim_date")
          .parquet(output_path)
    )
    log.info("Write complete.")


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Insurance Fraud Detection Pipeline")
    parser.add_argument("--local", action="store_true", help="Run in local mode")
    parser.add_argument("--input",  default=None, help="Override input path")
    parser.add_argument("--output", default=None, help="Override output path")
    args = parser.parse_args()

    input_path  = args.input  or (LOCAL_INPUT  if args.local else S3_INPUT)
    output_path = args.output or (LOCAL_OUTPUT if args.local else S3_OUTPUT)

    spark = build_spark(args.local)
    spark.sparkContext.setLogLevel("WARN")

    log.info("=" * 60)
    log.info("Insurance Fraud Detection Pipeline — START")
    log.info(f"  Input  : {input_path}")
    log.info(f"  Output : {output_path}")
    log.info("=" * 60)

    df = ingest(spark, input_path)
    df = validate_and_clean(df)
    df = engineer_features(df)
    df = apply_fraud_rules(df)
    evaluate(df)
    write_output(df, output_path)

    log.info("Pipeline completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
