"""Entry point for SentiStock data collectors."""

import argparse
import signal
import sys
import threading
import time

import structlog
from dotenv import load_dotenv

# Configure structlog before any other imports that use it
load_dotenv()
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer() if sys.stderr.isatty() else structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

from config import Settings
from producer import create_producer

log = structlog.get_logger()

shutdown_event = threading.Event()


def run_collector(name, collector, producer, interval):
    """Run a collector in a loop until shutdown is signaled."""
    log.info("collector_started", collector=name, interval=interval)
    while not shutdown_event.is_set():
        try:
            collector.collect_cycle(producer)
        except Exception:
            log.exception("collector_cycle_error", collector=name)
        shutdown_event.wait(timeout=interval)
    log.info("collector_stopped", collector=name)


def main():
    parser = argparse.ArgumentParser(description="SentiStock Data Collectors")
    parser.add_argument("--backfill", action="store_true", help="Run historical stock data backfill")
    args = parser.parse_args()

    settings = Settings()
    log.info("settings_loaded", broker=settings.redpanda_broker)

    producer = create_producer(settings.redpanda_broker)

    collectors: list[tuple[str, object, int]] = []

    # Reddit collector
    if settings.reddit_client_id and settings.reddit_client_secret:
        from reddit_collector import RedditCollector
        valid_tickers = set(settings.tickers)
        reddit = RedditCollector(settings, valid_tickers)
        collectors.append(("reddit", reddit, settings.reddit_poll_interval))
    else:
        log.warning("reddit_collector_disabled", reason="missing REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET")

    # News collector
    if settings.newsapi_key:
        from news_collector import NewsCollector
        news = NewsCollector(settings)
        collectors.append(("news", news, settings.news_poll_interval))
    else:
        log.warning("news_collector_disabled", reason="missing NEWSAPI_KEY")

    # Stock collector
    if settings.alpha_vantage_key:
        from stock_collector import StockCollector
        stock = StockCollector(settings)

        if args.backfill:
            log.info("running_stock_backfill")
            stock.collect_daily_history(producer)
            log.info("stock_backfill_finished")
            if not collectors:
                return

        collectors.append(("stock", stock, settings.stock_poll_interval))
    else:
        log.warning("stock_collector_disabled", reason="missing ALPHA_VANTAGE_KEY")

    if not collectors:
        log.error("no_collectors_configured", hint="Set API keys in .env")
        sys.exit(1)

    # Signal handling for graceful shutdown
    def handle_signal(signum, frame):
        sig_name = signal.Signals(signum).name
        log.info("shutdown_signal_received", signal=sig_name)
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Start collector threads
    threads = []
    for name, collector, interval in collectors:
        t = threading.Thread(
            target=run_collector,
            args=(name, collector, producer, interval),
            daemon=True,
            name=f"collector-{name}",
        )
        t.start()
        threads.append(t)

    log.info("all_collectors_running", count=len(threads))

    # Wait for shutdown
    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(timeout=1)
    except KeyboardInterrupt:
        shutdown_event.set()

    # Wait for threads to finish
    for t in threads:
        t.join(timeout=10)

    producer.close()
    log.info("shutdown_complete")


if __name__ == "__main__":
    main()
