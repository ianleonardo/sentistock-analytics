"""Shared Kafka consumer with retry logic and JSON deserialization."""

import json

import structlog
from kafka import KafkaConsumer
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
def create_consumer(broker: str, group_id: str, topics: list[str]) -> KafkaConsumer:
    """Create a KafkaConsumer with JSON deserialization."""
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=broker,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=-1,
    )
    log.info("kafka_consumer_connected", broker=broker, group_id=group_id, topics=topics)
    return consumer
