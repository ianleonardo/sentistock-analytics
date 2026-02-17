"""Centralized configuration via Pydantic BaseSettings."""

from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    # Redpanda / Kafka
    redpanda_broker: str = Field(default="localhost:19092", alias="REDPANDA_BROKER")

    # Reddit API
    reddit_client_id: str = Field(default="", alias="REDDIT_CLIENT_ID")
    reddit_client_secret: str = Field(default="", alias="REDDIT_CLIENT_SECRET")
    reddit_user_agent: str = Field(
        default="SentiStock/1.0", alias="REDDIT_USER_AGENT"
    )

    # NewsAPI
    newsapi_key: str = Field(default="", alias="NEWSAPI_KEY")

    # Alpha Vantage
    alpha_vantage_key: str = Field(default="", alias="ALPHA_VANTAGE_KEY")

    # Poll intervals (seconds)
    reddit_poll_interval: int = 60
    news_poll_interval: int = 300
    stock_poll_interval: int = 300

    # Kafka topic names
    topic_social_media: str = "social-media-raw"
    topic_stock_prices: str = "stock-prices-raw"
    topic_news: str = "news-raw"

    # Subreddits to monitor
    subreddits: List[str] = [
        "wallstreetbets",
        "stocks",
        "investing",
        "stockmarket",
        "options",
        "pennystocks",
    ]

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
