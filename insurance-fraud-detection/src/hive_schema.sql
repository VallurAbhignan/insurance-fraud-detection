-- ============================================================
-- hive_schema.sql
-- Insurance Fraud Detection — Hive analytical layer
-- Dynamic partitioning by region + claim_date for fast queries
-- ============================================================

-- Enable dynamic partitioning
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 1000;
SET hive.exec.max.dynamic.partitions.pernode = 200;

-- Use ORC for storage efficiency
SET hive.default.fileformat = ORC;

-- ── RAW CLAIMS TABLE ─────────────────────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS insurance_db.claims_raw (
    claim_id                STRING,
    policy_holder_id        STRING,
    provider_id             STRING,
    claim_type              STRING,
    claim_amount            DOUBLE,
    num_claims_last_30d     INT,
    days_since_policy_start INT,
    policy_premium          DOUBLE,
    hospital_duration_days  INT,
    is_fraud                INT
)
PARTITIONED BY (
    region     STRING,
    claim_date STRING
)
STORED AS PARQUET
LOCATION 's3://insurance-fraud-pipeline/raw/claims/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- ── PROCESSED (FLAGGED) CLAIMS TABLE ─────────────────────────────────────────
CREATE EXTERNAL TABLE IF NOT EXISTS insurance_db.claims_flagged (
    claim_id                STRING,
    policy_holder_id        STRING,
    provider_id             STRING,
    claim_type              STRING,
    claim_amount            DOUBLE,
    num_claims_last_30d     INT,
    days_since_policy_start INT,
    policy_premium          DOUBLE,
    hospital_duration_days  INT,
    claim_to_premium_ratio  DOUBLE,
    is_new_policy           INT,
    is_high_claim           INT,
    is_frequent_claimer     INT,
    is_high_volume_provider INT,
    provider_claim_count    BIGINT,
    rule_score              INT,
    predicted_fraud         INT,
    is_fraud                INT
)
PARTITIONED BY (
    region     STRING,
    claim_date STRING
)
STORED AS PARQUET
LOCATION 's3://insurance-fraud-pipeline/processed/claims_flagged/'
TBLPROPERTIES ("parquet.compression"="SNAPPY");

-- Recover partitions after new data lands
MSCK REPAIR TABLE insurance_db.claims_flagged;

-- ── ANALYTICAL QUERIES ────────────────────────────────────────────────────────

-- 1. Daily fraud summary by region
SELECT
    claim_date,
    region,
    COUNT(*)                                          AS total_claims,
    SUM(predicted_fraud)                              AS flagged_claims,
    ROUND(SUM(predicted_fraud) / COUNT(*) * 100, 2)  AS flag_rate_pct,
    ROUND(SUM(CASE WHEN predicted_fraud = 1 THEN claim_amount ELSE 0 END), 2) AS flagged_amount
FROM insurance_db.claims_flagged
WHERE claim_date >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY claim_date, region
ORDER BY claim_date DESC, flagged_amount DESC;

-- 2. Top suspicious providers
SELECT
    provider_id,
    COUNT(*)                           AS total_claims,
    SUM(predicted_fraud)               AS flagged_claims,
    ROUND(AVG(claim_amount), 2)        AS avg_claim_amount,
    ROUND(AVG(rule_score), 2)          AS avg_rule_score
FROM insurance_db.claims_flagged
GROUP BY provider_id
HAVING SUM(predicted_fraud) > 5
ORDER BY flagged_claims DESC
LIMIT 20;

-- 3. Precision/Recall by claim type (where ground truth available)
SELECT
    claim_type,
    SUM(CASE WHEN predicted_fraud=1 AND is_fraud=1 THEN 1 ELSE 0 END) AS tp,
    SUM(CASE WHEN predicted_fraud=1 AND is_fraud=0 THEN 1 ELSE 0 END) AS fp,
    SUM(CASE WHEN predicted_fraud=0 AND is_fraud=1 THEN 1 ELSE 0 END) AS fn,
    ROUND(
        SUM(CASE WHEN predicted_fraud=1 AND is_fraud=1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN predicted_fraud=1 THEN 1 ELSE 0 END), 0), 4
    ) AS precision_score,
    ROUND(
        SUM(CASE WHEN predicted_fraud=1 AND is_fraud=1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN is_fraud=1 THEN 1 ELSE 0 END), 0), 4
    ) AS recall_score
FROM insurance_db.claims_flagged
GROUP BY claim_type;
