"""
generate_data.py
----------------
Generates synthetic insurance claims dataset and uploads to AWS S3.
Run this once to seed your pipeline with sample data.
"""

import boto3
import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

# ── CONFIG ──────────────────────────────────────────────────────────────────
BUCKET = os.environ.get("S3_BUCKET", "insurance-fraud-pipeline")
PREFIX = "raw/claims/"
NUM_RECORDS = 100_000
RANDOM_SEED = 42

np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

REGIONS = ["North", "South", "East", "West", "Central"]
CLAIM_TYPES = ["Medical", "Accident", "Theft", "Property", "Life"]
PROVIDERS = [f"PROV_{i:04d}" for i in range(1, 500)]
POLICY_HOLDERS = [f"PH_{i:06d}" for i in range(1, 50_000)]


def random_date(start_days_ago=365):
    base = datetime.today()
    delta = timedelta(days=random.randint(0, start_days_ago))
    return (base - delta).strftime("%Y-%m-%d")


def generate_claims(n=NUM_RECORDS):
    fraud_flag = np.random.choice([0, 1], size=n, p=[0.92, 0.08])

    records = []
    for i in range(n):
        is_fraud = fraud_flag[i]
        claim_amount = (
            round(np.random.uniform(50_000, 500_000), 2)
            if is_fraud
            else round(np.random.uniform(500, 50_000), 2)
        )
        num_claims_30d = np.random.randint(3, 10) if is_fraud else np.random.randint(0, 3)
        provider_id = (
            random.choice(PROVIDERS[:20])   # fraud clusters around top-20 providers
            if is_fraud
            else random.choice(PROVIDERS)
        )

        records.append({
            "claim_id": f"CLM_{i+1:07d}",
            "policy_holder_id": random.choice(POLICY_HOLDERS),
            "provider_id": provider_id,
            "claim_type": random.choice(CLAIM_TYPES),
            "claim_amount": claim_amount,
            "claim_date": random_date(),
            "region": random.choice(REGIONS),
            "num_claims_last_30d": num_claims_30d,
            "days_since_policy_start": np.random.randint(1, 3650),
            "policy_premium": round(np.random.uniform(1000, 20000), 2),
            "hospital_duration_days": np.random.randint(0, 30) if is_fraud else np.random.randint(0, 10),
            "is_fraud": is_fraud,
        })

    return pd.DataFrame(records)


def upload_to_s3(df, bucket, prefix):
    local_path = "/tmp/claims_raw.csv"
    df.to_csv(local_path, index=False)

    s3 = boto3.client("s3")
    key = f"{prefix}claims_{datetime.today().strftime('%Y%m%d')}.csv"
    s3.upload_file(local_path, bucket, key)
    print(f"Uploaded {len(df):,} records → s3://{bucket}/{key}")


if __name__ == "__main__":
    print("Generating synthetic insurance claims data...")
    df = generate_claims()
    print(f"  Total records : {len(df):,}")
    print(f"  Fraud records : {df['is_fraud'].sum():,} ({df['is_fraud'].mean()*100:.1f}%)")
    print(f"  Sample:\n{df.head(3).to_string()}")

    # Save locally for testing
    df.to_csv("data/claims_sample.csv", index=False)
    print("Saved locally → data/claims_sample.csv")

    # Uncomment to upload to S3
    # upload_to_s3(df, BUCKET, PREFIX)
