"""
airflow/dags/streaming_consumer_dag.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Manages the Kafka → Delta Lake streaming consumer lifecycle.

This DAG:
  1. Checks if the ECS consumer task is healthy
  2. Restarts it on unhealthy state or configures it with new topic offsets
  3. Runs the DLQ (Dead Letter Queue) reprocessing job every 4 hours

The actual consumer runs as a long-lived ECS Fargate task — not within Airflow.
Airflow manages its lifecycle, not the processing itself.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.sensors.ecs import EcsTaskStateSensor
from pendulum import datetime


@dag(
    dag_id="streaming_consumer_lifecycle",
    schedule="0 */4 * * *",   # Check health every 4 hours
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner":  "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["streaming", "kafka", "delta-lake", "ecs"],
    doc_md=__doc__,
)
def streaming_consumer_dag() -> None:

    @task(task_id="check_consumer_health")
    def check_consumer_health(**context: dict) -> str:
        """Query ECS to check if the consumer task is RUNNING."""
        import boto3
        ecs = boto3.client("ecs", region_name="eu-west-1")
        cluster = "banking-streaming-cluster"

        running = ecs.list_tasks(
            cluster=cluster,
            family="banking-transaction-consumer",
            desiredStatus="RUNNING",
        )["taskArns"]

        status = "RUNNING" if running else "STOPPED"
        context["ti"].xcom_push(key="consumer_status", value=status)
        return status

    @task.branch(task_id="route_by_status")
    def route_by_status(**context: dict) -> str:
        status = context["ti"].xcom_pull(key="consumer_status", task_ids="check_consumer_health")
        return "consumer_healthy" if status == "RUNNING" else "restart_consumer"

    @task(task_id="consumer_healthy")
    def consumer_healthy() -> None:
        import structlog
        structlog.get_logger().info("kafka_consumer_healthy")

    restart_consumer = EcsRunTaskOperator(
        task_id="restart_consumer",
        cluster="banking-streaming-cluster",
        task_definition="banking-transaction-consumer",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [{
                "name": "transaction-consumer",
                "environment": [
                    {"name": "ENV",               "value": "{{ var.value.env }}"},
                    {"name": "KAFKA_BOOTSTRAP",   "value": "{{ var.value.msk_bootstrap }}"},
                    {"name": "SCHEMA_REGISTRY",   "value": "{{ var.value.schema_registry_url }}"},
                ],
            }]
        },
        network_configuration={
            "awsvpcConfiguration": {
                "subnets":        ["{{ var.value.private_subnet_1 }}", "{{ var.value.private_subnet_2 }}"],
                "securityGroups": ["{{ var.value.ecs_security_group }}"],
                "assignPublicIp": "DISABLED",
            }
        },
        awslogs_group="/ecs/banking-transaction-consumer",
        awslogs_region="eu-west-1",
        awslogs_stream_prefix="ecs/transaction-consumer",
        aws_conn_id="aws_banking",
        region_name="eu-west-1",
    )

    # Reprocess DLQ messages every 4 hours
    reprocess_dlq = EcsRunTaskOperator(
        task_id="reprocess_dlq",
        cluster="banking-streaming-cluster",
        task_definition="banking-dlq-reprocessor",
        launch_type="FARGATE",
        overrides={
            "containerOverrides": [{
                "name": "dlq-reprocessor",
                "command": ["python", "-m", "kafka.consumers.dlq_reprocessor",
                            "--env", "{{ var.value.env }}",
                            "--max-records", "1000"],
            }]
        },
        aws_conn_id="aws_banking",
        region_name="eu-west-1",
    )

    # Wire up
    health = check_consumer_health()
    route  = route_by_status()
    health >> route >> [consumer_healthy(), restart_consumer] >> reprocess_dlq


streaming_dag = streaming_consumer_dag()
