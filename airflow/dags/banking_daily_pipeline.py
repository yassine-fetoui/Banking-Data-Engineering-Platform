"""
airflow/dags/banking_daily_pipeline.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Daily orchestration DAG for the banking data platform.

Schedule  : 02:00 AM UTC daily (after CBS nightly batch closes)
SLA       : Must complete by 06:00 AM UTC (4-hour window)
Owner     : data-engineering

Execution order:
  1. [Bronze]  Ingest raw data (transactions, customers, accounts, FX rates)
  2. [Quality] Run Great Expectations checkpoints on Bronze
  3. [Silver]  dbt run silver models (SCD2 + transaction cleansing)
  4. [Silver]  dbt test silver
  5. [Gold]    Spark AML risk scoring
  6. [Gold]    dbt run gold models (360 view, P&L, regulatory)
  7. [Gold]    dbt test gold
  8. [Serve]   Trigger Redshift COPY from Delta Lake → Redshift Serverless
  9. [Monitor] Publish pipeline metrics to CloudWatch
"""
from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from pendulum import datetime


# ── Callbacks ─────────────────────────────────────────────────────────────────

def _on_failure(context: dict) -> None:
    """Alert Slack on any task failure."""
    dag_id  = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id  = context["run_id"]
    SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id="slack_data_alerts",
        message=(
            f":red_circle: *FAILURE* — `{dag_id}.{task_id}`\n"
            f"Run: `{run_id}`\n"
            f"Log: {context['task_instance'].log_url}"
        ),
    ).execute(context)


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="banking_daily_pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner":             "data-engineering",
        "retries":           2,
        "retry_delay":       timedelta(minutes=10),
        "execution_timeout": timedelta(hours=1),
        "on_failure_callback": _on_failure,
        "email_on_failure":  True,
        "email":             ["data-engineering@bank.com"],
        "sla":               timedelta(hours=4),
    },
    tags=["banking", "daily", "bronze", "silver", "gold"],
    doc_md=__doc__,
)
def banking_daily_pipeline() -> None:

    # ── 0. Wait for CBS batch file to land ────────────────────────────────────
    wait_for_cbs_file = S3KeySensor(
        task_id="wait_for_cbs_batch_file",
        bucket_name="banking-landing-{{ var.value.env }}",
        bucket_key="transactions/date={{ ds }}/part-*.csv",
        wildcard_match=True,
        timeout=3600,
        poke_interval=120,
        mode="reschedule",
    )

    # ── 1. Bronze Ingestion ───────────────────────────────────────────────────
    @task_group(group_id="bronze_ingestion")
    def bronze_ingestion() -> None:

        ingest_transactions = GlueJobOperator(
            task_id="ingest_transactions",
            job_name="banking-bronze-transactions",
            script_arguments={
                "--env":        "{{ var.value.env }}",
                "--batch-date": "{{ ds }}",
                "--batch-id":   "{{ run_id }}",
            },
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        ingest_customers = GlueJobOperator(
            task_id="ingest_customers",
            job_name="banking-bronze-customers",
            script_arguments={
                "--env":        "{{ var.value.env }}",
                "--batch-date": "{{ ds }}",
            },
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        ingest_accounts = GlueJobOperator(
            task_id="ingest_accounts",
            job_name="banking-bronze-accounts",
            script_arguments={
                "--env":        "{{ var.value.env }}",
                "--batch-date": "{{ ds }}",
            },
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        ingest_fx_rates = GlueJobOperator(
            task_id="ingest_fx_rates",
            job_name="banking-bronze-fx-rates",
            script_arguments={"--rate-date": "{{ ds }}"},
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        # FX rates must land before Silver (needed for currency conversion)
        [ingest_transactions, ingest_customers, ingest_accounts, ingest_fx_rates]

    # ── 2. Data Quality — Bronze ──────────────────────────────────────────────
    @task_group(group_id="bronze_quality")
    def bronze_quality() -> None:

        ge_transactions = BashOperator(
            task_id="ge_checkpoint_transactions",
            bash_command=(
                "great_expectations checkpoint run bronze_transactions "
                "--batch-request '{\"batch_spec_passthrough\": {\"reader_options\": "
                "{\"partitionFilter\": \"ingestion_date={{ ds }}\"}}}'"
            ),
        )

        ge_customers = BashOperator(
            task_id="ge_checkpoint_customers",
            bash_command="great_expectations checkpoint run bronze_customers",
        )

        [ge_transactions, ge_customers]

    # ── 3. Silver — dbt ───────────────────────────────────────────────────────
    @task_group(group_id="silver_dbt")
    def silver_dbt() -> None:

        dbt_silver_run = BashOperator(
            task_id="dbt_silver_run",
            bash_command=(
                "cd /opt/airflow/dbt && "
                "dbt run --select silver --vars '{run_date: {{ ds }}}' "
                "--profiles-dir . --target {{ var.value.env }}"
            ),
        )

        dbt_silver_snapshot = BashOperator(
            task_id="dbt_silver_snapshot",
            bash_command=(
                "cd /opt/airflow/dbt && "
                "dbt snapshot --profiles-dir . --target {{ var.value.env }}"
            ),
        )

        dbt_silver_test = BashOperator(
            task_id="dbt_silver_test",
            bash_command=(
                "cd /opt/airflow/dbt && "
                "dbt test --select silver --store-failures "
                "--profiles-dir . --target {{ var.value.env }}"
            ),
        )

        dbt_silver_run >> dbt_silver_snapshot >> dbt_silver_test

    # ── 4. Silver — Spark SCD2 ────────────────────────────────────────────────
    spark_scd2 = GlueJobOperator(
        task_id="spark_scd2_customers",
        job_name="banking-silver-scd2-customers",
        script_arguments={
            "--env":        "{{ var.value.env }}",
            "--batch-date": "{{ ds }}",
        },
        aws_conn_id="aws_banking",
        region_name="eu-west-1",
    )

    # ── 5. Gold — AML Scoring (Spark) ─────────────────────────────────────────
    spark_aml = GlueJobOperator(
        task_id="spark_aml_risk_scores",
        job_name="banking-gold-aml-scoring",
        script_arguments={
            "--env":        "{{ var.value.env }}",
            "--batch-date": "{{ ds }}",
        },
        aws_conn_id="aws_banking",
        region_name="eu-west-1",
    )

    # ── 6. Gold — dbt ────────────────────────────────────────────────────────
    @task_group(group_id="gold_dbt")
    def gold_dbt() -> None:

        dbt_gold_run = BashOperator(
            task_id="dbt_gold_run",
            bash_command=(
                "cd /opt/airflow/dbt && "
                "dbt run --select gold --vars '{run_date: {{ ds }}}' "
                "--profiles-dir . --target {{ var.value.env }}"
            ),
        )

        dbt_gold_test = BashOperator(
            task_id="dbt_gold_test",
            bash_command=(
                "cd /opt/airflow/dbt && "
                "dbt test --select gold --store-failures "
                "--profiles-dir . --target {{ var.value.env }}"
            ),
        )

        dbt_gold_run >> dbt_gold_test

    # ── 7. Load Gold → Redshift ───────────────────────────────────────────────
    @task_group(group_id="redshift_load")
    def redshift_load() -> None:

        load_customer_360 = RedshiftSQLOperator(
            task_id="copy_customer_360_to_redshift",
            redshift_conn_id="redshift_banking",
            sql="""
            COPY gold.dim_customer_360
            FROM 's3://banking-data-lake-{{ var.value.env }}/gold/dim_customer_360/'
            IAM_ROLE '{{ var.value.redshift_iam_role }}'
            FORMAT PARQUET;
            """,
        )

        load_aml_scores = RedshiftSQLOperator(
            task_id="copy_aml_scores_to_redshift",
            redshift_conn_id="redshift_banking",
            sql="""
            COPY gold.aml_risk_scores
            FROM 's3://banking-data-lake-{{ var.value.env }}/gold/aml_risk_scores/score_date={{ ds }}/'
            IAM_ROLE '{{ var.value.redshift_iam_role }}'
            FORMAT PARQUET;
            """,
        )

        load_daily_pnl = RedshiftSQLOperator(
            task_id="copy_daily_pnl_to_redshift",
            redshift_conn_id="redshift_banking",
            sql="""
            COPY gold.fct_daily_pnl
            FROM 's3://banking-data-lake-{{ var.value.env }}/gold/fct_daily_pnl/report_date={{ ds }}/'
            IAM_ROLE '{{ var.value.redshift_iam_role }}'
            FORMAT PARQUET;
            """,
        )

        [load_customer_360, load_aml_scores, load_daily_pnl]

    # ── 8. Publish metrics ────────────────────────────────────────────────────
    @task(task_id="publish_pipeline_metrics")
    def publish_metrics(**context: dict) -> None:
        import boto3
        cloudwatch = boto3.client("cloudwatch", region_name="eu-west-1")
        cloudwatch.put_metric_data(
            Namespace="Banking/DataPipeline",
            MetricData=[
                {
                    "MetricName": "PipelineSuccess",
                    "Value":      1,
                    "Unit":       "Count",
                    "Dimensions": [
                        {"Name": "Environment", "Value": "{{ var.value.env }}"},
                        {"Name": "RunDate",     "Value": "{{ ds }}"},
                    ],
                }
            ],
        )

    # ── 9. Success notification ───────────────────────────────────────────────
    slack_success = SlackWebhookOperator(
        task_id="slack_success_notification",
        slack_webhook_conn_id="slack_data_alerts",
        message=(
            ":white_check_mark: *Banking Daily Pipeline — SUCCESS*\n"
            "Date: `{{ ds }}` | Env: `{{ var.value.env }}`\n"
            "Duration: `{{ macros.datetime.utcnow() - dag_run.start_date }}`"
        ),
    )

    # ── Wire up ───────────────────────────────────────────────────────────────
    bronze = bronze_ingestion()
    quality = bronze_quality()
    silver = silver_dbt()
    gold_spark = spark_aml
    gold = gold_dbt()
    redshift = redshift_load()
    metrics = publish_metrics()

    (
        wait_for_cbs_file
        >> bronze
        >> quality
        >> [silver, spark_scd2]
        >> gold_spark
        >> gold
        >> redshift
        >> metrics
        >> slack_success
    )


banking_dag = banking_daily_pipeline()
