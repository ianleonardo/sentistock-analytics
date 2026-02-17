"""Centralized configuration via Pydantic BaseSettings."""

from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    # Redpanda / Kafka
    redpanda_broker: str = Field(default="localhost:19092", alias="REDPANDA_BROKER")

    # TimescaleDB
    timescale_host: str = Field(default="localhost", alias="TIMESCALE_HOST")
    timescale_port: int = Field(default=5432, alias="TIMESCALE_PORT")
    postgres_db: str = Field(default="sentistock", alias="POSTGRES_DB")
    postgres_user: str = Field(default="sentistock", alias="POSTGRES_USER")
    postgres_password: str = Field(default="changeme_secure_password", alias="POSTGRES_PASSWORD")

    # Redis
    redis_host: str = Field(default="localhost", alias="REDIS_HOST")
    redis_port: int = Field(default=6379, alias="REDIS_PORT")
    redis_password: str = Field(default="", alias="REDIS_PASSWORD")

    # Consumer group IDs
    group_sentiment: str = "sentiment-processor"
    group_technical: str = "technical-processor"
    group_db_sink: str = "db-sink-processor"

    # Input topics
    topic_social_media: str = "social-media-raw"
    topic_stock_prices: str = "stock-prices-raw"
    topic_news: str = "news-raw"

    # Output topics
    topic_sentiment_scored: str = "sentiment-scored"
    topic_technical_indicators: str = "technical-indicators"
    topic_correlation_events: str = "correlation-events"

    # Alert topic
    topic_alert_triggers: str = "alert-triggers"

    # Tickers to track
    tickers: List[str] = [
        "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA",
        "BRK", "JPM", "JNJ", "V", "PG", "UNH", "HD", "MA", "DIS", "BAC",
        "XOM", "ADBE", "CSCO", "CRM", "CMCSA", "PFE", "NFLX", "TMO", "ABT",
        "NKE", "KO", "PEP", "AVGO", "COST", "WMT", "MRK", "CVX", "LLY",
        "ABBV", "AMD", "ORCL", "ACN", "QCOM", "TXN", "INTC", "MDT", "UPS",
        "MS", "GS", "BLK", "SCHW", "AXP", "LOW", "SBUX", "INTU", "ISRG",
        "GILD", "BKNG", "MDLZ", "ADI", "REGN", "VRTX", "LRCX", "KLAC",
        "PYPL", "SQ", "SNAP", "UBER", "LYFT", "RIVN", "LCID", "PLTR",
        "SOFI", "COIN", "HOOD", "MARA", "RIOT", "GME", "AMC", "BB",
        "NOK", "WISH", "CLOV", "SPCE", "SPY", "QQQ", "IWM",
    ]

    @property
    def dsn(self) -> str:
        return (
            f"host={self.timescale_host} port={self.timescale_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )
