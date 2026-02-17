"""Stock price collector using Alpha Vantage."""

import time
from datetime import datetime, timezone

import structlog
from alpha_vantage.timeseries import TimeSeries
from tenacity import retry, stop_after_attempt, wait_exponential

from config import Settings
from producer import send_message

log = structlog.get_logger()

# Alpha Vantage free tier: 5 calls/min → 12s between calls
API_CALL_DELAY = 12


class StockCollector:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.ts = TimeSeries(key=settings.alpha_vantage_key, output_format="json")
        self._last_prices: dict[str, str] = {}  # ticker → last timestamp sent
        log.info("stock_collector_initialized", tickers=len(settings.tickers))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=15, max=120))
    def _get_intraday(self, symbol: str):
        return self.ts.get_intraday(symbol=symbol, interval="5min", outputsize="compact")

    def collect_cycle(self, producer):
        """Fetch latest intraday price for each ticker."""
        total = 0
        for ticker in self.settings.tickers:
            try:
                data, _ = self._get_intraday(ticker)

                if not data:
                    log.warning("no_data_returned", ticker=ticker)
                    continue

                # Get the most recent data point
                latest_ts = sorted(data.keys())[-1]

                # Skip if we already sent this timestamp
                if self._last_prices.get(ticker) == latest_ts:
                    log.debug("price_unchanged", ticker=ticker, timestamp=latest_ts)
                    time.sleep(API_CALL_DELAY)
                    continue

                self._last_prices[ticker] = latest_ts
                point = data[latest_ts]

                message = {
                    "source": "alpha_vantage",
                    "ticker": ticker,
                    "timestamp": latest_ts,
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "open": float(point["1. open"]),
                    "high": float(point["2. high"]),
                    "low": float(point["3. low"]),
                    "close": float(point["4. close"]),
                    "volume": int(float(point["5. volume"])),
                }
                send_message(
                    producer,
                    self.settings.topic_stock_prices,
                    message,
                    key=ticker,
                )
                total += 1

            except Exception:
                log.exception("stock_fetch_failed", ticker=ticker)

            time.sleep(API_CALL_DELAY)

        log.info("stock_cycle_complete", messages_sent=total)

    def collect_daily_history(self, producer, days: int = 30):
        """One-time backfill of daily historical data."""
        log.info("stock_backfill_start", days=days, tickers=len(self.settings.tickers))

        for ticker in self.settings.tickers:
            try:
                data, _ = self.ts.get_daily(symbol=ticker, outputsize="full")

                if not data:
                    continue

                timestamps = sorted(data.keys(), reverse=True)[:days]
                for ts in timestamps:
                    point = data[ts]
                    message = {
                        "source": "alpha_vantage",
                        "ticker": ticker,
                        "timestamp": ts,
                        "collected_at": datetime.now(timezone.utc).isoformat(),
                        "open": float(point["1. open"]),
                        "high": float(point["2. high"]),
                        "low": float(point["3. low"]),
                        "close": float(point["4. close"]),
                        "volume": int(float(point["5. volume"])),
                    }
                    send_message(
                        producer,
                        self.settings.topic_stock_prices,
                        message,
                        key=ticker,
                    )

                log.info("stock_backfill_ticker_done", ticker=ticker, points=len(timestamps))

            except Exception:
                log.exception("stock_backfill_failed", ticker=ticker)

            time.sleep(API_CALL_DELAY)

        log.info("stock_backfill_complete")
