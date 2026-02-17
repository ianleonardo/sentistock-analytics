"""Database sink: writes processed data to TimescaleDB and Redis cache.

Consumes sentiment-scored, technical-indicators, and correlation-events topics.
Batches writes to TimescaleDB and updates Redis cache with latest values.
"""

import json
import threading
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import redis
import structlog

from config import Settings
from consumer import create_consumer

log = structlog.get_logger()

BATCH_SIZE = 100
BATCH_TIMEOUT_SECONDS = 5

# Redis TTLs
PRICE_TTL = 60
SENTIMENT_TTL = 300
INDICATORS_TTL = 300


class DbSink:
    def __init__(self, settings: Settings):
        self.settings = settings

    def _connect_db(self):
        return psycopg2.connect(self.settings.dsn)

    def _connect_redis(self):
        return redis.Redis(
            host=self.settings.redis_host,
            port=self.settings.redis_port,
            password=self.settings.redis_password or None,
            decode_responses=True,
        )

    def run(self, producer, shutdown_event: threading.Event):
        """Main processing loop with batched writes."""
        consumer = create_consumer(
            self.settings.redpanda_broker,
            self.settings.group_db_sink,
            [
                self.settings.topic_sentiment_scored,
                self.settings.topic_technical_indicators,
                self.settings.topic_correlation_events,
            ],
        )

        db_conn = self._connect_db()
        redis_client = self._connect_redis()

        log.info("db_sink_started")

        # Batch accumulators
        sentiment_batch = []
        technical_batch = []
        correlation_batch = []
        last_flush = time.monotonic()

        try:
            while not shutdown_event.is_set():
                records = consumer.poll(timeout_ms=1000)
                for topic_partition, messages in records.items():
                    topic = topic_partition.topic
                    for msg in messages:
                        data = msg.value
                        if topic == self.settings.topic_sentiment_scored:
                            sentiment_batch.append(data)
                            self._cache_sentiment(redis_client, data)
                        elif topic == self.settings.topic_technical_indicators:
                            technical_batch.append(data)
                            self._cache_indicators(redis_client, data)
                        elif topic == self.settings.topic_correlation_events:
                            correlation_batch.append(data)

                # Check if we should flush
                total = len(sentiment_batch) + len(technical_batch) + len(correlation_batch)
                elapsed = time.monotonic() - last_flush

                if total >= BATCH_SIZE or (total > 0 and elapsed >= BATCH_TIMEOUT_SECONDS):
                    self._flush_batches(db_conn, sentiment_batch, technical_batch, correlation_batch)
                    sentiment_batch.clear()
                    technical_batch.clear()
                    correlation_batch.clear()
                    last_flush = time.monotonic()
        finally:
            # Final flush
            if sentiment_batch or technical_batch or correlation_batch:
                self._flush_batches(db_conn, sentiment_batch, technical_batch, correlation_batch)
            consumer.close()
            db_conn.close()
            redis_client.close()
            log.info("db_sink_stopped")

    def _flush_batches(self, conn, sentiment_batch, technical_batch, correlation_batch):
        """Write all batched data to TimescaleDB."""
        try:
            with conn.cursor() as cur:
                if sentiment_batch:
                    self._insert_sentiment(cur, sentiment_batch)
                if technical_batch:
                    self._insert_technical(cur, technical_batch)
                # correlation_batch: no table to write to (as noted in plan)
                # but we log the signals
                if correlation_batch:
                    log.info("correlation_signals_flushed", count=len(correlation_batch))
            conn.commit()
            log.info(
                "batch_flushed",
                sentiment=len(sentiment_batch),
                technical=len(technical_batch),
                correlation=len(correlation_batch),
            )
        except Exception:
            conn.rollback()
            log.exception("batch_flush_error")

    def _insert_sentiment(self, cur, batch: list[dict]):
        """Batch insert sentiment scores into TimescaleDB."""
        rows = []
        for data in batch:
            rows.append((
                data.get("time", datetime.now(timezone.utc).isoformat()),
                data["ticker"],
                data.get("source", "unknown"),
                data["sentiment_score"],
                data.get("sentiment_label"),
                data.get("sentiment_confidence"),
                1,  # mention_count
                data.get("raw_text", "")[:1000],
            ))
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO sentiment_scores
               (time, ticker, source, sentiment_score, sentiment_label, confidence, mention_count, raw_text)
               VALUES %s""",
            rows,
        )

    def _insert_technical(self, cur, batch: list[dict]):
        """Batch insert technical indicators into TimescaleDB."""
        rows = []
        for data in batch:
            rows.append((
                data.get("time", datetime.now(timezone.utc).isoformat()),
                data["ticker"],
                data.get("rsi"),
                data.get("macd"),
                data.get("macd_signal"),
                data.get("bb_upper"),
                data.get("bb_lower"),
                data.get("sma_20"),
                data.get("ema_50"),
                data.get("volume_sma"),
            ))
        psycopg2.extras.execute_values(
            cur,
            """INSERT INTO technical_indicators
               (time, ticker, rsi, macd, macd_signal, bb_upper, bb_lower, sma_20, ema_50, volume_sma)
               VALUES %s""",
            rows,
        )

    def _cache_sentiment(self, r: redis.Redis, data: dict):
        """Cache latest sentiment for a ticker in Redis."""
        ticker = data.get("ticker")
        if not ticker:
            return
        key = f"sentiment:{ticker}"
        r.setex(key, SENTIMENT_TTL, json.dumps(data, default=str))

    def _cache_indicators(self, r: redis.Redis, data: dict):
        """Cache latest technical indicators for a ticker in Redis."""
        ticker = data.get("ticker")
        if not ticker:
            return
        key = f"indicators:{ticker}"
        r.setex(key, INDICATORS_TTL, json.dumps(data, default=str))
