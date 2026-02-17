"""Sentiment analysis processor using FinBERT.

Consumes social-media-raw and news-raw topics, scores text with FinBERT,
and produces sentiment-scored messages.
"""

import re
import threading
from datetime import datetime, timezone

import structlog
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

from config import Settings
from consumer import create_consumer
from producer import send_message

log = structlog.get_logger()

# Same blacklist as reddit_collector to avoid false ticker matches
TICKER_BLACKLIST = {
    "THE", "FOR", "ALL", "ARE", "BUT", "NOT", "YOU", "HAS", "HER", "HIS",
    "HOW", "ITS", "LET", "MAY", "NEW", "NOW", "OLD", "OUR", "OUT", "OWN",
    "SAY", "SHE", "TOO", "USE", "WAY", "WHO", "BOY", "DID", "GET", "HIM",
    "MAN", "RUN", "GOT", "HIT", "PUT", "TOP", "CEO", "CFO", "COO", "CTO",
    "IPO", "ETF", "GDP", "FDA", "SEC", "FBI", "CIA", "USA", "USD", "EUR",
    "ATH", "EOD", "EPS", "IMO", "LOL", "OMG", "WTF", "YOLO", "FOMO",
    "HODL", "TLDR", "EDIT", "LINK", "POST", "THIS", "THAT", "JUST",
    "LIKE", "WHAT", "WITH", "FROM", "HAVE", "BEEN", "WILL", "THEY",
    "THAN", "WHEN", "THEM", "SOME", "WANT", "VERY", "CALL", "LONG",
    "MAKE", "MUCH", "OVER", "SUCH", "TAKE", "YEAR", "ALSO", "BACK",
    "GOOD", "ONLY", "COME", "COULD", "KNOW", "MOST", "NEED", "SAID",
    "STILL", "GOING", "PUMP", "DUMP", "BEAR", "BULL", "CASH", "DEBT",
    "GAIN", "HOLD", "LOSS", "MOON", "RISK", "SELL", "STAY", "STOP",
    "HUGE", "PAYS", "NEXT", "BEST", "HIGH", "PUTS", "MOVE", "REAL",
    "FREE", "SAFE", "EVER", "DOWN", "RISE", "HELP", "LOOK", "EASY",
    "OPEN", "KEEP", "MORE", "IDEA", "PLAY", "DROP", "DONE", "WEEK",
    "SAME", "SURE",
}

TICKER_PATTERN = re.compile(r"\$?([A-Z]{1,5})\b")

# FinBERT labels in model output order
LABELS = ["negative", "neutral", "positive"]


class SentimentProcessor:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.valid_tickers = set(settings.tickers)
        self._load_model()

    def _load_model(self):
        """Load FinBERT model and tokenizer."""
        model_name = "ProsusAI/finbert"
        log.info("loading_finbert_model", model=model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name)
        self.model.eval()
        log.info("finbert_model_loaded")

    def _score_text(self, text: str) -> tuple[float, str, float]:
        """Score text with FinBERT.

        Returns (score, label, confidence) where:
        - score: positive_prob - negative_prob (range -1.0 to 1.0)
        - label: argmax of [negative, neutral, positive]
        - confidence: max probability
        """
        # Truncate to model max length
        inputs = self.tokenizer(
            text, return_tensors="pt", truncation=True, max_length=512, padding=True
        )
        with torch.no_grad():
            outputs = self.model(**inputs)
        probs = torch.softmax(outputs.logits, dim=1)[0]

        negative_prob = probs[0].item()
        positive_prob = probs[2].item()

        score = positive_prob - negative_prob
        label_idx = probs.argmax().item()
        label = LABELS[label_idx]
        confidence = probs[label_idx].item()

        return score, label, confidence

    def _extract_tickers(self, text: str) -> list[str]:
        """Extract valid stock tickers from text."""
        matches = TICKER_PATTERN.findall(text)
        return list({
            m for m in matches
            if m in self.valid_tickers and m not in TICKER_BLACKLIST
        })

    def run(self, producer, shutdown_event: threading.Event):
        """Main processing loop."""
        consumer = create_consumer(
            self.settings.redpanda_broker,
            self.settings.group_sentiment,
            [self.settings.topic_social_media, self.settings.topic_news],
        )

        log.info("sentiment_processor_started")
        try:
            while not shutdown_event.is_set():
                records = consumer.poll(timeout_ms=1000)
                for topic_partition, messages in records.items():
                    for msg in messages:
                        if shutdown_event.is_set():
                            break
                        try:
                            self._process_message(msg.value, topic_partition.topic, producer)
                        except Exception:
                            log.exception("sentiment_processing_error", topic=topic_partition.topic)
        finally:
            consumer.close()
            log.info("sentiment_processor_stopped")

    def _process_message(self, data: dict, topic: str, producer):
        """Process a single message from social-media-raw or news-raw."""
        source = data.get("source", "unknown")
        full_text = data.get("full_text", "") or data.get("text", "")
        if not full_text:
            return

        # Determine tickers for this message
        if topic == self.settings.topic_social_media:
            # Reddit messages already have tickers extracted
            tickers = data.get("tickers", [])
        else:
            # News messages: extract tickers from text
            tickers = self._extract_tickers(full_text)

        if not tickers:
            return

        # Score the text once
        score, label, confidence = self._score_text(full_text)

        # Emit one message per ticker
        for ticker in tickers:
            output = {
                "source": source,
                "ticker": ticker,
                "time": data.get("created_utc") or data.get("published_at") or datetime.now(timezone.utc).isoformat(),
                "sentiment_score": round(score, 4),
                "sentiment_label": label,
                "sentiment_confidence": round(confidence, 4),
                "raw_text": full_text[:1000],
                "title": data.get("title", ""),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
            send_message(
                producer,
                self.settings.topic_sentiment_scored,
                output,
                key=ticker,
            )
            log.debug(
                "sentiment_scored",
                ticker=ticker,
                score=output["sentiment_score"],
                label=label,
            )
