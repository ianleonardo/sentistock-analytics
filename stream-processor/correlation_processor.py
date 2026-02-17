"""Correlation processor: sentiment-price correlation and signal generation.

Polls TimescaleDB on a 60s schedule, reads the sentiment_1h continuous aggregate
and stock_prices table, computes Pearson correlations and lag analysis,
and produces trading signals to correlation-events topic.
"""

import threading
from datetime import datetime, timezone, timedelta

import numpy as np
import psycopg2
import structlog
from scipy import stats

from config import Settings
from producer import send_message

log = structlog.get_logger()

POLL_INTERVAL_SECONDS = 60
LOOKBACK_HOURS = 24
LAG_RANGE = range(-6, 7)  # -6 to +6 hours

# Correlation thresholds
STRONG_THRESHOLD = 0.7
MODERATE_THRESHOLD = 0.4

# Sentiment spike threshold (z-score)
SPIKE_ZSCORE = 2.0


class CorrelationProcessor:
    def __init__(self, settings: Settings):
        self.settings = settings

    def run(self, producer, shutdown_event: threading.Event):
        """Main polling loop."""
        log.info("correlation_processor_started", poll_interval=POLL_INTERVAL_SECONDS)
        while not shutdown_event.is_set():
            try:
                self._run_cycle(producer)
            except Exception:
                log.exception("correlation_cycle_error")
            shutdown_event.wait(timeout=POLL_INTERVAL_SECONDS)
        log.info("correlation_processor_stopped")

    def _run_cycle(self, producer):
        """Run one correlation analysis cycle across all tickers."""
        conn = psycopg2.connect(self.settings.dsn)
        try:
            tickers_with_data = self._get_active_tickers(conn)
            for ticker in tickers_with_data:
                try:
                    signals = self._analyze_ticker(conn, ticker)
                    for signal in signals:
                        send_message(
                            producer,
                            self.settings.topic_correlation_events,
                            signal,
                            key=ticker,
                        )
                        log.info(
                            "correlation_signal",
                            ticker=ticker,
                            signal_type=signal["signal_type"],
                            correlation=signal.get("correlation"),
                        )
                except Exception:
                    log.exception("ticker_correlation_error", ticker=ticker)
        finally:
            conn.close()

    def _get_active_tickers(self, conn) -> list[str]:
        """Get tickers that have both sentiment and price data in the last 24h."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT s.ticker
                FROM sentiment_scores s
                INNER JOIN stock_prices p ON s.ticker = p.ticker
                WHERE s.time >= %s AND p.time >= %s
            """, (cutoff, cutoff))
            return [row[0] for row in cur.fetchall()]

    def _analyze_ticker(self, conn, ticker: str) -> list[dict]:
        """Analyze correlation and generate signals for a single ticker."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        signals = []

        # Fetch hourly sentiment from continuous aggregate
        with conn.cursor() as cur:
            cur.execute("""
                SELECT bucket, avg_sentiment, mention_count
                FROM sentiment_1h
                WHERE ticker = %s AND bucket >= %s
                ORDER BY bucket
            """, (ticker, cutoff))
            sentiment_rows = cur.fetchall()

        # Fetch hourly price changes
        with conn.cursor() as cur:
            cur.execute("""
                SELECT time_bucket('1 hour', time) AS bucket,
                       LAST(close, time) AS close_price,
                       COUNT(*) AS data_points
                FROM stock_prices
                WHERE ticker = %s AND time >= %s
                GROUP BY bucket
                ORDER BY bucket
            """, (ticker, cutoff))
            price_rows = cur.fetchall()

        if len(sentiment_rows) < 4 or len(price_rows) < 4:
            return signals

        # Align sentiment and price data by hour bucket
        sentiment_by_hour = {row[0]: row[1] for row in sentiment_rows}
        price_by_hour = {row[0]: row[1] for row in price_rows}

        common_hours = sorted(set(sentiment_by_hour) & set(price_by_hour))
        if len(common_hours) < 4:
            return signals

        sentiment_arr = np.array([sentiment_by_hour[h] for h in common_hours])
        prices = np.array([price_by_hour[h] for h in common_hours])
        price_returns = np.diff(prices) / prices[:-1]

        # Trim sentiment to match returns length
        sentiment_trimmed = sentiment_arr[1:]

        if len(sentiment_trimmed) < 3 or len(price_returns) < 3:
            return signals

        # Zero-lag correlation
        corr, p_value = stats.pearsonr(sentiment_trimmed, price_returns)

        # Correlation-based signals
        if abs(corr) > STRONG_THRESHOLD and p_value < 0.05:
            signal_type = "strong_buy" if corr > 0 else "strong_sell"
            signals.append(self._make_signal(ticker, signal_type, corr, p_value, lag=0))
        elif abs(corr) > MODERATE_THRESHOLD and p_value < 0.1:
            signal_type = "buy" if corr > 0 else "sell"
            signals.append(self._make_signal(ticker, signal_type, corr, p_value, lag=0))

        # Lag analysis: find optimal lag
        best_lag, best_corr, best_p = self._lag_analysis(sentiment_arr, prices)
        if best_lag != 0 and abs(best_corr) > MODERATE_THRESHOLD and best_p < 0.1:
            signal_type = "buy" if best_corr > 0 else "sell"
            if abs(best_corr) > STRONG_THRESHOLD:
                signal_type = "strong_" + signal_type
            signals.append(self._make_signal(ticker, signal_type, best_corr, best_p, lag=best_lag))

        # Sentiment spike detection
        if len(sentiment_arr) >= 6:
            mean_s = np.mean(sentiment_arr)
            std_s = np.std(sentiment_arr)
            if std_s > 0:
                latest_z = (sentiment_arr[-1] - mean_s) / std_s
                if abs(latest_z) > SPIKE_ZSCORE:
                    signals.append(self._make_signal(
                        ticker, "sentiment_spike", latest_z, None, lag=0,
                        extra={"z_score": round(float(latest_z), 4), "direction": "bullish" if latest_z > 0 else "bearish"},
                    ))

        # Divergence detection: sentiment and price moving in opposite directions
        if len(sentiment_arr) >= 6 and len(prices) >= 6:
            recent_sent_trend = sentiment_arr[-3:].mean() - sentiment_arr[-6:-3].mean()
            recent_price_trend = (prices[-1] - prices[-4]) / prices[-4] if prices[-4] != 0 else 0

            if recent_sent_trend > 0.1 and recent_price_trend < -0.02:
                signals.append(self._make_signal(ticker, "bullish_divergence", None, None, lag=0))
            elif recent_sent_trend < -0.1 and recent_price_trend > 0.02:
                signals.append(self._make_signal(ticker, "bearish_divergence", None, None, lag=0))

        return signals

    def _lag_analysis(self, sentiment: np.ndarray, prices: np.ndarray) -> tuple[int, float, float]:
        """Find the lag with highest absolute correlation."""
        price_returns = np.diff(prices) / prices[:-1]
        best_lag = 0
        best_corr = 0.0
        best_p = 1.0

        for lag in LAG_RANGE:
            if lag == 0:
                continue
            if lag > 0:
                # Sentiment leads price
                s = sentiment[:len(sentiment) - lag]
                p = price_returns[lag - 1:]
            else:
                # Price leads sentiment
                s = sentiment[-lag:]
                p = price_returns[:len(price_returns) + lag]

            # Align lengths
            min_len = min(len(s), len(p))
            if min_len < 3:
                continue
            s = s[:min_len]
            p = p[:min_len]

            try:
                c, pval = stats.pearsonr(s, p)
                if abs(c) > abs(best_corr):
                    best_lag = lag
                    best_corr = c
                    best_p = pval
            except Exception:
                continue

        return best_lag, best_corr, best_p

    def _make_signal(
        self,
        ticker: str,
        signal_type: str,
        correlation: float | None,
        p_value: float | None,
        lag: int,
        extra: dict | None = None,
    ) -> dict:
        signal = {
            "ticker": ticker,
            "signal_type": signal_type,
            "correlation": round(float(correlation), 4) if correlation is not None else None,
            "p_value": round(float(p_value), 4) if p_value is not None else None,
            "lag_hours": lag,
            "lookback_hours": LOOKBACK_HOURS,
            "time": datetime.now(timezone.utc).isoformat(),
        }
        if extra:
            signal.update(extra)
        return signal
