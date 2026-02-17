"""Shared Kafka producer with retry logic and JSON serialization."""

import json

import structlog
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

log = structlog.get_logger()


@retry(
    retry=retry_if_exception_type(NoBrokersAvailable),
    stop=stop_after_attempt(10),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    before_sleep=lambda rs: structlog.get_logger().warning(
        "broker_connect_retry",
        attempt=rs.attempt_number,
    ),
)
def create_producer(broker: str) -> KafkaProducer:
    """Create a KafkaProducer with JSON serialization and gzip compression."""
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        compression_type="gzip",
        acks="all",
    )
    log.info("kafka_producer_connected", broker=broker)
    return producer


def send_message(producer: KafkaProducer, topic: str, value: dict, key: str | None = None):
    """Send a message to a Kafka topic, blocking until acknowledged."""
    try:
        future = producer.send(topic, value=value, key=key)
        future.get(timeout=10)
        log.debug("message_sent", topic=topic, key=key)
    except Exception:
        log.exception("message_send_failed", topic=topic, key=key)
        raise
