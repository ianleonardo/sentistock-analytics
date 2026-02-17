"""Technical indicators processor using pandas-ta.

Consumes stock-prices-raw topic, computes technical indicators per ticker
using rolling buffers, and produces to technical-indicators topic.
"""

import threading
from collections import deque
from datetime import datetime, timezone

import pandas as pd
import pandas_ta as ta
import structlog

from config import Settings
from consumer import create_consumer
from producer import send_message

log = structlog.get_logger()

MIN_DATA_POINTS = 50
BUFFER_SIZE = 200


class TechnicalProcessor:
    def __init__(self, settings: Settings):
        self.settings = settings
        # Per-ticker rolling buffer of OHLCV candles
        self.buffers: dict[str, deque] = {}

    def _get_buffer(self, ticker: str) -> deque:
        if ticker not in self.buffers:
            self.buffers[ticker] = deque(maxlen=BUFFER_SIZE)
        return self.buffers[ticker]

    def run(self, producer, shutdown_event: threading.Event):
        """Main processing loop."""
        consumer = create_consumer(
            self.settings.redpanda_broker,
            self.settings.group_technical,
            [self.settings.topic_stock_prices],
        )

        log.info("technical_processor_started")
        try:
            while not shutdown_event.is_set():
                records = consumer.poll(timeout_ms=1000)
                for topic_partition, messages in records.items():
                    for msg in messages:
                        if shutdown_event.is_set():
                            break
                        try:
                            self._process_message(msg.value, producer)
                        except Exception:
                            log.exception("technical_processing_error")
        finally:
            consumer.close()
            log.info("technical_processor_stopped")

    def _process_message(self, data: dict, producer):
        """Process a single stock price message."""
        ticker = data.get("ticker")
        if not ticker:
            return

        candle = {
            "time": data.get("time") or data.get("timestamp"),
            "open": float(data.get("open", 0)),
            "high": float(data.get("high", 0)),
            "low": float(data.get("low", 0)),
            "close": float(data.get("close", 0)),
            "volume": int(data.get("volume", 0)),
        }

        buf = self._get_buffer(ticker)
        buf.append(candle)

        if len(buf) < MIN_DATA_POINTS:
            log.debug(
                "insufficient_data",
                ticker=ticker,
                count=len(buf),
                required=MIN_DATA_POINTS,
            )
            return

        indicators = self._compute_indicators(ticker, buf)
        if indicators is None:
            return

        send_message(
            producer,
            self.settings.topic_technical_indicators,
            indicators,
            key=ticker,
        )
        log.debug("technical_indicators_emitted", ticker=ticker)

    def _compute_indicators(self, ticker: str, buf: deque) -> dict | None:
        """Compute technical indicators from the rolling buffer."""
        df = pd.DataFrame(list(buf))
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

        # RSI(14)
        rsi = ta.rsi(df["close"], length=14)
        # MACD(12, 26, 9)
        macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
        # Bollinger Bands(20, 2)
        bbands = ta.bbands(df["close"], length=20, std=2)
        # SMA(20)
        sma_20 = ta.sma(df["close"], length=20)
        # EMA(50)
        ema_50 = ta.ema(df["close"], length=50)
        # Volume SMA(20)
        vol_sma = ta.sma(df["volume"], length=20)

        # Get latest values (last row)
        idx = len(df) - 1

        def safe_val(series, i=idx):
            if series is None:
                return None
            val = series.iloc[i]
            return round(float(val), 4) if pd.notna(val) else None

        result = {
            "time": buf[-1]["time"] or datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "rsi": safe_val(rsi),
            "macd": safe_val(macd.iloc[:, 0] if macd is not None else None),
            "macd_signal": safe_val(macd.iloc[:, 1] if macd is not None else None),
            "bb_upper": safe_val(bbands.iloc[:, 2] if bbands is not None else None),
            "bb_lower": safe_val(bbands.iloc[:, 0] if bbands is not None else None),
            "sma_20": safe_val(sma_20),
            "ema_50": safe_val(ema_50),
            "volume_sma": safe_val(vol_sma),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

        return result
