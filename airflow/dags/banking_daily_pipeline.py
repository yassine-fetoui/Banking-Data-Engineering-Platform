"""
airflow/dags/banking_daily_pipeline.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Daily orchestration DAG for the banking data platform.

Schedule : 02:00 AM UTC daily (after CBS nightly batch)
SLA      : Must complete by 06:00 AM UTC (4-hour window)
Owner    : data-engineering

Pipeline Flow:
    1. Wait for CBS raw files
    2. Bronze Ingestion (Glue)
    3. Bronze Data Quality (Great Expectations)
    4. Silver Layer
       • dbt models + snapshots
       • Spark SCD2 (customers)
    5. Gold Layer
       • Spark AML Risk Scoring
       • dbt Gold models (360°, P&L, Regulatory)
    6. Serve Layer → Redshift Serverless (COPY)
    7. Observability (CloudWatch + Slack)
"""

from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule
from pendulum import datetime


# ── Callbacks ─────────────────────────────────────────────────────────────────
def on_failure_callback(context: dict) -> None:
    """Send rich Slack alert on task failure."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    execution_date = context["execution_date"]

    SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id="slack_data_alerts",
        message=(
            f":red_circle: *Pipeline Failure*\n"
            f"• DAG: `{dag_id}`\n"
            f"• Task: `{task_id}`\n"
            f"• Run: `{run_id}`\n"
            f"• Date: `{execution_date}`\n"
            f"• Log: {context['task_instance'].log_url}"
        ),
        username="Airflow",
    ).execute(context)


def on_success_callback(context: dict) -> None:
    """Send success notification with duration."""
    duration = context["task_instance"].end_date - context["dag_run"].start_date

    SlackWebhookOperator(
        task_id="slack_success_notification",
        slack_webhook_conn_id="slack_data_alerts",
        message=(
            f":white_check_mark: *Banking Daily Pipeline — SUCCESS*\n"
            f"Date: `{context['ds']}` | Env: `{Variable.get('env')}`\n"
            f"Duration: `{duration}`"
        ),
        username="Airflow",
    ).execute(context)


# ── DAG Definition ────────────────────────────────────────────────────────────
@dag(
    dag_id="banking_daily_pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(hours=1),
        "on_failure_callback": on_failure_callback,
        "email_on_failure": True,
        "email": ["data-engineering@bank.com"],
        "sla": timedelta(hours=4),
    },
    tags=["banking", "daily", "bronze", "silver", "gold", "production"],
    doc_md=__doc__,
    render_template_as_native_obj=True,   # Better Jinja handling
)
def banking_daily_pipeline():

    # ── 0. Wait for CBS files ─────────────────────────────────────────────────
    wait_for_cbs = S3KeySensor(
        task_id="wait_for_cbs_batch_file",
        bucket_name=f"banking-landing-{Variable.get('env')}",
        bucket_key="transactions/date={{ ds }}/part-*.csv",
        wildcard_match=True,
        timeout=7200,           # 2h max wait
        poke_interval=180,
        mode="reschedule",
        soft_fail=False,
    )

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,   # Ensure metrics run even on partial failure
    )

    # ── 1. Bronze Ingestion ───────────────────────────────────────────────────
    @task_group(group_id="bronze_ingestion", tooltip="Ingest raw data from CBS")
    def bronze_ingestion():
        ingest_transactions = GlueJobOperator(
            task_id="ingest_transactions",
            job_name="banking-bronze-transactions",
            script_arguments={
                "--env": "{{ var.value.env }}",
                "--batch-date": "{{ ds }}",
                "--batch-id": "{{ run_id }}",
            },
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        ingest_customers = GlueJobOperator(
            task_id="ingest_customers",
            job_name="banking-bronze-customers",
            script_arguments={
                "--env": "{{ var.value.env }}",
                "--batch-date": "{{ ds }}",
            },
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        ingest_accounts = GlueJobOperator(
            task_id="ingest_accounts",
            job_name="banking-bronze-accounts",
            script_arguments={
                "--env": "{{ var.value.env }}",
                "--batch-date": "{{ ds }}",
            },
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        ingest_fx_rates = GlueJobOperator(
            task_id="ingest_fx_rates",
            job_name="banking-bronze-fx-rates",
            script_arguments={
                "--env": "{{ var.value.env }}",
                "--rate-date": "{{ ds }}",
            },
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        # FX rates needed for Silver currency conversion
        [ingest_transactions, ingest_customers, ingest_accounts] >> ingest_fx_rates

    # ── 2. Bronze Quality ─────────────────────────────────────────────────────
    @task_group(group_id="bronze_quality", tooltip="Great Expectations validation")
    def bronze_quality():
        BashOperator(
            task_id="ge_checkpoint_transactions",
            bash_command=(
                "great_expectations checkpoint run bronze_transactions "
                f"--batch-request '{{\"batch_spec_passthrough\": {{\"reader_options\": "
                "{{\"partitionFilter\": \"ingestion_date={{ ds }}\"}}}}}}}'"
            ),
        )

        BashOperator(
            task_id="ge_checkpoint_customers",
            bash_command="great_expectations checkpoint run bronze_customers",
        )

    # ── 3. Silver Layer ───────────────────────────────────────────────────────
    @task_group(group_id="silver_layer")
    def silver_layer():
        @task_group(group_id="dbt_silver")
        def dbt_silver():
            dbt_run = BashOperator(
                task_id="dbt_run",
                bash_command=(
                    "cd /opt/airflow/dbt && "
                    "dbt run --select silver+ --vars '{run_date: {{ ds }}}' "
                    "--profiles-dir . --target {{ var.value.env }}"
                ),
            )
            dbt_snapshot = BashOperator(
                task_id="dbt_snapshot",
                bash_command=(
                    "cd /opt/airflow/dbt && "
                    "dbt snapshot --profiles-dir . --target {{ var.value.env }}"
                ),
            )
            dbt_test = BashOperator(
                task_id="dbt_test",
                bash_command=(
                    "cd /opt/airflow/dbt && "
                    "dbt test --select silver --store-failures "
                    "--profiles-dir . --target {{ var.value.env }}"
                ),
            )
            dbt_run >> dbt_snapshot >> dbt_test

        spark_scd2 = GlueJobOperator(
            task_id="spark_scd2_customers",
            job_name="banking-silver-scd2-customers",
            script_arguments={
                "--env": "{{ var.value.env }}",
                "--batch-date": "{{ ds }}",
            },
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        dbt_silver() >> spark_scd2

    # ── 4. Gold Layer ─────────────────────────────────────────────────────────
    @task_group(group_id="gold_layer")
    def gold_layer():
        spark_aml = GlueJobOperator(
            task_id="spark_aml_risk_scores",
            job_name="banking-gold-aml-scoring",
            script_arguments={
                "--env": "{{ var.value.env }}",
                "--batch-date": "{{ ds }}",
            },
            aws_conn_id="aws_banking",
            region_name="eu-west-1",
        )

        @task_group(group_id="dbt_gold")
        def dbt_gold():
            dbt_run = BashOperator(
                task_id="dbt_run",
                bash_command=(
                    "cd /opt/airflow/dbt && "
                    "dbt run --select gold+ --vars '{run_date: {{ ds }}}' "
                    "--profiles-dir . --target {{ var.value.env }}"
                ),
            )
            dbt_test = BashOperator(
                task_id="dbt_test",
                bash_command=(
                    "cd /opt/airflow/dbt && "
                    "dbt test --select gold --store-failures "
                    "--profiles-dir . --target {{ var.value.env }}"
                ),
            )
            dbt_run >> dbt_test

        spark_aml >> dbt_gold()

    # ── 5. Serve to Redshift ──────────────────────────────────────────────────
    @task_group(group_id="redshift_load")
    def redshift_load():
        load_customer_360 = RedshiftSQLOperator(
            task_id="copy_customer_360",
            redshift_conn_id="redshift_banking",
            sql="""
                COPY gold.dim_customer_360
                FROM 's3://banking-data-lake-{{ var.value.env }}/gold/dim_customer_360/'
                IAM_ROLE '{{ var.value.redshift_iam_role }}'
                FORMAT PARQUET MANIFEST;
            """,
        )

        load_aml = RedshiftSQLOperator(
            task_id="copy_aml_scores",
            redshift_conn_id="redshift_banking",
            sql="""
                COPY gold.aml_risk_scores
                FROM 's3://banking-data-lake-{{ var.value.env }}/gold/aml_risk_scores/score_date={{ ds }}/'
                IAM_ROLE '{{ var.value.redshift_iam_role }}'
                FORMAT PARQUET;
            """,
        )

        load_pnl = RedshiftSQLOperator(
            task_id="copy_daily_pnl",
            redshift_conn_id="redshift_banking",
            sql="""
                COPY gold.fct_daily_pnl
                FROM 's3://banking-data-lake-{{ var.value.env }}/gold/fct_daily_pnl/report_date={{ ds }}/'
                IAM_ROLE '{{ var.value.redshift_iam_role }}'
                FORMAT PARQUET;
            """,
        )

        [load_customer_360, load_aml, load_pnl]

    # ── 6. Metrics ────────────────────────────────────────────────────────────
    @task(task_id="publish_metrics", retries=1)
    def publish_metrics(**context):
        import boto3
        from datetime import datetime as dt

        cloudwatch = boto3.client("cloudwatch", region_name="eu-west-1")
        duration = (context["task_instance"].end_date - context["dag_run"].start_date).total_seconds()

        cloudwatch.put_metric_data(
            Namespace="Banking/DataPipeline",
            MetricData=[
                {
                    "MetricName": "PipelineSuccess",
                    "Value": 1,
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "Environment", "Value": Variable.get("env")},
                        {"Name": "RunDate", "Value": context["ds"]},
                    ],
                },
                {
                    "MetricName": "PipelineDurationSeconds",
                    "Value": duration,
                    "Unit": "Seconds",
                    "Dimensions": [{"Name": "Environment", "Value": Variable.get("env")}],
                },
            ],
        )

    # ── Wire everything together ──────────────────────────────────────────────
    bronze = bronze_ingestion()
    quality = bronze_quality()
    silver = silver_layer()
    gold = gold_layer()
    redshift = redshift_load()
    metrics = publish_metrics()

    (
        start
        >> wait_for_cbs
        >> bronze
        >> quality
        >> silver
        >> gold
        >> redshift
        >> metrics
        >> end
    )

    # Optional: success notification only on full success
    metrics >> SlackWebhookOperator(
        task_id="slack_success",
        slack_webhook_conn_id="slack_data_alerts",
        message=(
            ":tada: *Banking Daily Pipeline completed successfully*\n"
            f"Date: `{{{{ ds }}}}` | Duration: `{{{{ (execution_date - start_date).total_seconds() }}}}s`"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


banking_dag = banking_daily_pipeline()
