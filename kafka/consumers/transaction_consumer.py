"""
kafka/consumers/transaction_consumer.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Kafka consumer: reads from `banking.transactions` and writes to Delta Lake
Bronze layer with exactly-once semantics.

Design:
  - Avro deserialisation via Schema Registry
  - Delta Lake MERGE using transaction_id as idempotency key
  - Manual offset commit AFTER successful Delta write
  - Dead-letter queue (DLQ) for poison messages
  - Structured logging for observability
"""
from __future__ import annotations

import json
import os
import signal
import time
from typing import Any

import pandas as pd
import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.utils.spark_session import get_spark, get_s3_path

log = structlog.get_logger(__name__)

TOPIC      = "banking.transactions"
DLQ_TOPIC  = "banking.transactions.dlq"
GROUP_ID   = "banking-bronze-writer"
BATCH_SIZE = 500      # Commit to Delta every N messages
MAX_POLL_S = 30       # Max seconds between Delta commits


class TransactionConsumer:
    """
    Exactly-once Kafka → Delta Lake consumer.

    Pattern:
      1. Poll Kafka messages into an in-memory buffer
      2. Every BATCH_SIZE messages (or MAX_POLL_S seconds), MERGE into Delta
      3. Only THEN commit Kafka offsets
      → If Delta write fails, offsets are NOT committed → no data loss
    """

    def __init__(self, bootstrap_servers: str, schema_registry_url: str, env: str) -> None:
        self._env   = env
        self._spark = get_spark("banking.kafka.consumer", env=env)
        self._delta_path = get_s3_path("bronze", "transactions_streaming", env)
        self._dlq_path   = get_s3_path("bronze", "transactions_dlq", env)

        self._deserializer = AvroDeserializer(
            SchemaRegistryClient({"url": schema_registry_url}),
        )

        self._consumer = Consumer({
            "bootstrap.servers":        bootstrap_servers,
            "group.id":                 GROUP_ID,
            "auto.offset.reset":        "earliest",
            # CRITICAL: disable auto-commit — we commit manually after Delta write
            "enable.auto.commit":       False,
            "max.poll.interval.ms":     300_000,
            "session.timeout.ms":       30_000,
            "fetch.max.bytes":          52_428_800,  # 50 MB
        })
        self._consumer.subscribe([TOPIC])

        self._buffer: list[dict] = []
        self._last_commit = time.monotonic()
        self._running = True

        # Graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT,  self._shutdown)

    def _shutdown(self, *_) -> None:
        log.info("consumer_shutdown_signal_received")
        self._running = False

    def _deserialise(self, msg) -> dict | None:
        """Deserialise Avro message. Returns None on error (→ DLQ)."""
        try:
            ctx = SerializationContext(TOPIC, MessageField.VALUE)
            return self._deserializer(msg.value(), ctx)
        except Exception as exc:
            log.error("deserialisation_failed", error=str(exc), offset=msg.offset())
            self._send_to_dlq(msg)
            return None

    def _send_to_dlq(self, msg) -> None:
        """Write poison messages to Delta DLQ for manual inspection."""
        dlq_row = pd.DataFrame([{
            "topic":     msg.topic(),
            "partition": msg.partition(),
            "offset":    msg.offset(),
            "key":       msg.key().decode() if msg.key() else None,
            "raw_value": msg.value().hex() if msg.value() else None,
            "error_at":  pd.Timestamp.utcnow(),
        }])
        self._spark.createDataFrame(dlq_row) \
            .write.format("delta").mode("append").save(self._dlq_path)

    def _flush_to_delta(self) -> None:
        """MERGE buffer into Delta Lake. Idempotent via transaction_id key."""
        if not self._buffer:
            return

        df = self._spark.createDataFrame(pd.DataFrame(self._buffer)) \
                        .withColumn("_ingested_at",   F.current_timestamp()) \
                        .withColumn("ingestion_date",  F.to_date(F.col("transaction_datetime")))

        if not DeltaTable.isDeltaTable(self._spark, self._delta_path):
            df.write.format("delta").mode("overwrite").partitionBy("ingestion_date").save(self._delta_path)
        else:
            target = DeltaTable.forPath(self._spark, self._delta_path)
            target.alias("tgt").merge(
                df.alias("src"),
                "tgt.transaction_id = src.transaction_id"
            ).whenNotMatchedInsertAll().execute()

        count = len(self._buffer)
        self._buffer.clear()
        self._last_commit = time.monotonic()
        log.info("delta_flush_complete", rows=count)

    def run(self) -> None:
        log.info("consumer_started", topic=TOPIC, group=GROUP_ID, env=self._env)
        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    pass
                elif msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        raise KafkaException(msg.error())
                else:
                    record = self._deserialise(msg)
                    if record:
                        self._buffer.append(record)

                # Flush when buffer full OR time limit reached
                elapsed = time.monotonic() - self._last_commit
                if len(self._buffer) >= BATCH_SIZE or elapsed >= MAX_POLL_S:
                    self._flush_to_delta()
                    # Commit offsets ONLY after successful Delta write
                    self._consumer.commit(asynchronous=False)

        except Exception as exc:
            log.error("consumer_error", error=str(exc))
            raise
        finally:
            # Final flush before shutdown
            self._flush_to_delta()
            self._consumer.commit(asynchronous=False)
            self._consumer.close()
            log.info("consumer_stopped")
