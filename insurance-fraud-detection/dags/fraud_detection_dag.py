"""
fraud_detection_dag.py
-----------------------
Airflow DAG — orchestrates the Insurance Fraud Detection pipeline.

Schedule : Daily at 02:00 UTC
SLA      : 4 hours from trigger
Retries  : 2 (with 5-minute backoff)
Alerts   : Email on failure / SLA miss
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.dates import days_ago

# ── DEFAULT ARGS ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email":            ["vallurbadri1@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "sla":              timedelta(hours=4),
}

# ── EMR CLUSTER CONFIG ────────────────────────────────────────────────────────
EMR_CLUSTER_CONFIG = {
    "Name": "FraudDetection-EMR",
    "ReleaseLabel": "emr-6.10.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Hive"}, {"Name": "Hadoop"}],
    "Instances": {
        "InstanceGroups": [
            {"Name": "Master", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge",  "InstanceCount": 1},
            {"Name": "Core",   "InstanceRole": "CORE",   "InstanceType": "m5.xlarge",  "InstanceCount": 2},
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "JobFlowRole":     "EMR_EC2_DefaultRole",
    "ServiceRole":     "EMR_DefaultRole",
    "LogUri":          "s3://insurance-fraud-pipeline/emr-logs/",
    "VisibleToAllUsers": True,
}

# ── SPARK STEPS ───────────────────────────────────────────────────────────────
SPARK_STEPS = [
    {
        "Name": "RunFraudDetectionPipeline",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--conf", "spark.sql.shuffle.partitions=100",
                "s3://insurance-fraud-pipeline/scripts/fraud_detection_pipeline.py",
            ],
        },
    }
]

# ── PYTHON CALLBACKS ──────────────────────────────────────────────────────────
def validate_input_data(**context):
    """Quick row-count check before spinning up EMR."""
    import boto3
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(
        Bucket="insurance-fraud-pipeline",
        Prefix="raw/claims/"
    )
    files = response.get("Contents", [])
    if not files:
        raise ValueError("No input files found in S3 raw/claims/ — aborting pipeline.")
    print(f"Found {len(files)} input file(s) in S3.")


def notify_success(**context):
    print(f"[{datetime.now()}] Fraud Detection pipeline completed successfully.")
    print(f"  DAG Run ID : {context['run_id']}")


# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="insurance_fraud_detection",
    default_args=default_args,
    description="Daily batch pipeline to detect insurance fraud using PySpark on EMR",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["data-engineering", "fraud", "pyspark", "emr"],
) as dag:

    # Task 1 — Validate input before wasting EMR spin-up time
    t_validate_input = PythonOperator(
        task_id="validate_input_data",
        python_callable=validate_input_data,
    )

    # Task 2 — Create EMR cluster
    t_create_emr = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMR_CLUSTER_CONFIG,
        aws_conn_id="aws_default",
    )

    # Task 3 — Submit Spark job
    t_submit_spark = EmrAddStepsOperator(
        task_id="submit_spark_job",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
    )

    # Task 4 — Wait for Spark step completion
    t_wait_spark = EmrStepSensor(
        task_id="wait_for_spark_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull('submit_spark_job', key='return_value')[0] }}",
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=14400,   # 4-hour SLA
    )

    # Task 5 — Terminate EMR (always runs to avoid cost leak)
    t_terminate_emr = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        trigger_rule="all_done",   # run even on failure
    )

    # Task 6 — Notify success
    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # ── DAG DEPENDENCIES ──────────────────────────────────────────────────────
    t_validate_input >> t_create_emr >> t_submit_spark >> t_wait_spark >> t_terminate_emr >> t_notify
