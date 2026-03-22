"""
kafka/producers/transaction_producer.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Kafka producer: publishes real-time transactions from the Core Banking System
to the `banking.transactions` topic using Avro serialisation.

Features:
  - Idempotent producer (enable.idempotence=true) → exactly-once delivery
  - Schema Registry integration — schema evolution without breaking consumers
  - PII masking before publishing to Kafka (card numbers hashed)
  - Retry with exponential back-off via tenacity
  - Structured logging for DataDog / CloudWatch
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid
from datetime import datetime
from typing import Any

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from tenacity import retry, stop_after_attempt, wait_exponential

log = structlog.get_logger(__name__)

TOPIC = "banking.transactions"

AVRO_SCHEMA_STR = """
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.banking.events",
  "doc": "Real-time banking transaction event",
  "fields": [
    {"name": "transaction_id",       "type": "string"},
    {"name": "account_id",           "type": "string"},
    {"name": "customer_id",          "type": "string"},
    {"name": "transaction_datetime", "type": "string"},
    {"name": "amount",               "type": "double"},
    {"name": "currency",             "type": "string"},
    {"name": "transaction_type",     "type": "string"},
    {"name": "channel",              "type": ["null", "string"], "default": null},
    {"name": "merchant_id",          "type": ["null", "string"], "default": null},
    {"name": "merchant_category",    "type": ["null", "string"], "default": null},
    {"name": "country_code",         "type": ["null", "string"], "default": null},
    {"name": "status",               "type": "string"},
    {"name": "card_number_hashed",   "type": ["null", "string"], "default": null},
    {"name": "reference_id",         "type": ["null", "string"], "default": null},
    {"name": "event_timestamp",      "type": "long",
     "doc": "Unix millis — set by producer, used for exactly-once idempotency"}
  ]
}
"""


def _mask_card(card_number: str | None) -> str | None:
    if not card_number:
        return None
    return hashlib.sha256(card_number.encode()).hexdigest()


def _delivery_report(err, msg) -> None:
    if err:
        log.error("kafka_delivery_failed", error=str(err), topic=msg.topic())
    else:
        log.debug("kafka_delivered", topic=msg.topic(), partition=msg.partition(), offset=msg.offset())


class TransactionProducer:
    """Avro Kafka producer for real-time transaction events."""

    def __init__(self, bootstrap_servers: str, schema_registry_url: str) -> None:
        schema_registry_conf = {"url": schema_registry_url}
        if "amazonaws" in schema_registry_url:
            # MSK IAM authentication
            schema_registry_conf.update({
                "basic.auth.credentials.source": "SASL_INHERIT",
            })

        self._schema_registry = SchemaRegistryClient(schema_registry_conf)
        self._serializer = AvroSerializer(
            self._schema_registry,
            AVRO_SCHEMA_STR,
            conf={"auto.register.schemas": True},
        )

        producer_conf = {
            "bootstrap.servers":   bootstrap_servers,
            # Exactly-once: idempotent producer
            "enable.idempotence":  True,
            "acks":                "all",
            "retries":             10,
            "max.in.flight.requests.per.connection": 5,
            "compression.type":    "lz4",
            "linger.ms":           5,       # Micro-batching for throughput
            "batch.size":          65536,   # 64 KB batches
        }

        # MSK IAM auth in AWS environments
        if os.getenv("AWS_MSK_IAM_AUTH", "false").lower() == "true":
            producer_conf.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism":    "AWS_MSK_IAM",
                "sasl.jaas.config":  "software.amazon.msk.auth.iam.IAMLoginModule required;",
            })

        self._producer = Producer(producer_conf)
        log.info("producer_initialised", topic=TOPIC, servers=bootstrap_servers)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
    def publish(self, transaction: dict[str, Any]) -> None:
        """Publish a single transaction. Masks card PII, adds event timestamp."""
        payload = {
            **transaction,
            "card_number_hashed": _mask_card(transaction.pop("card_number", None)),
            "status":             transaction.get("status", "PENDING").upper(),
            "currency":           transaction.get("currency", "").upper(),
            "event_timestamp":    int(datetime.utcnow().timestamp() * 1000),
        }

        ctx = SerializationContext(TOPIC, MessageField.VALUE)
        self._producer.produce(
            topic=TOPIC,
            key=payload["transaction_id"].encode(),   # Partition by transaction_id
            value=self._serializer(payload, ctx),
            on_delivery=_delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 30.0) -> int:
        """Flush all buffered messages. Returns number of messages still in queue."""
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            log.warning("kafka_flush_timeout", remaining_messages=remaining)
        return remaining

    def publish_batch(self, transactions: list[dict]) -> None:
        for txn in transactions:
            self.publish(txn)
        self.flush()
        log.info("batch_published", count=len(transactions))
