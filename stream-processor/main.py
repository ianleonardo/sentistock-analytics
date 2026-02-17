"""Entry point for SentiStock stream processors."""

import argparse
import signal
import sys
import threading

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


def run_processor(name: str, processor, producer):
    """Run a processor until shutdown is signaled."""
    log.info("processor_started", processor=name)
    try:
        processor.run(producer, shutdown_event)
    except Exception:
        log.exception("processor_crashed", processor=name)


def main():
    parser = argparse.ArgumentParser(description="SentiStock Stream Processors")
    parser.add_argument("--sentiment-only", action="store_true", help="Run only the sentiment processor")
    parser.add_argument("--technical-only", action="store_true", help="Run only the technical processor")
    parser.add_argument("--correlation-only", action="store_true", help="Run only the correlation processor")
    parser.add_argument("--sink-only", action="store_true", help="Run only the DB sink")
    args = parser.parse_args()

    settings = Settings()
    log.info("settings_loaded", broker=settings.redpanda_broker)

    producer = create_producer(settings.redpanda_broker)

    # Determine which processors to run
    run_all = not (args.sentiment_only or args.technical_only or args.correlation_only or args.sink_only)

    processors: list[tuple[str, object]] = []

    if run_all or args.sentiment_only:
        from sentiment_processor import SentimentProcessor
        processors.append(("sentiment", SentimentProcessor(settings)))

    if run_all or args.technical_only:
        from technical_processor import TechnicalProcessor
        processors.append(("technical", TechnicalProcessor(settings)))

    if run_all or args.correlation_only:
        from correlation_processor import CorrelationProcessor
        processors.append(("correlation", CorrelationProcessor(settings)))

    if run_all or args.sink_only:
        from db_sink import DbSink
        processors.append(("db_sink", DbSink(settings)))

    if not processors:
        log.error("no_processors_configured")
        sys.exit(1)

    # Signal handling for graceful shutdown
    def handle_signal(signum, frame):
        sig_name = signal.Signals(signum).name
        log.info("shutdown_signal_received", signal=sig_name)
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Start processor threads
    threads = []
    for name, processor in processors:
        t = threading.Thread(
            target=run_processor,
            args=(name, processor, producer),
            daemon=True,
            name=f"processor-{name}",
        )
        t.start()
        threads.append(t)

    log.info("all_processors_running", count=len(threads))

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
