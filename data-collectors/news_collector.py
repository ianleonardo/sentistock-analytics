"""News data collector using NewsAPI."""

from datetime import datetime, timedelta, timezone

import structlog
from newsapi import NewsApiClient
from tenacity import retry, stop_after_attempt, wait_exponential

from config import Settings
from producer import send_message

log = structlog.get_logger()

SEARCH_QUERIES = [
    "stock market",
    "earnings report",
    "IPO",
    "merger acquisition",
    "SEC filing",
    "federal reserve",
]

FINANCIAL_SOURCES = (
    "bloomberg,reuters,the-wall-street-journal,financial-times,"
    "cnbc,business-insider,fortune,the-verge,ars-technica"
)


class NewsCollector:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = NewsApiClient(api_key=settings.newsapi_key)
        self.seen_urls: set[str] = set()
        log.info("news_collector_initialized")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=60))
    def _fetch_articles(self, query: str, from_dt: str):
        return self.client.get_everything(
            q=query,
            from_param=from_dt,
            language="en",
            sort_by="publishedAt",
            page_size=20,
        )

    def collect_cycle(self, producer):
        """Run one collection cycle across all search queries."""
        from_dt = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        total = 0

        for query in SEARCH_QUERIES:
            try:
                response = self._fetch_articles(query, from_dt)
                articles = response.get("articles", [])

                for article in articles:
                    url = article.get("url", "")
                    if not url or url in self.seen_urls:
                        continue
                    self.seen_urls.add(url)

                    source_name = article.get("source", {}).get("name", "unknown")
                    title = article.get("title", "")
                    description = article.get("description", "") or ""
                    content = article.get("content", "") or ""

                    message = {
                        "source": "newsapi",
                        "type": "article",
                        "source_name": source_name,
                        "author": article.get("author") or "unknown",
                        "title": title,
                        "description": description,
                        "content": content[:2000],
                        "url": url,
                        "published_at": article.get("publishedAt", ""),
                        "collected_at": datetime.now(timezone.utc).isoformat(),
                        "full_text": f"{title} {description} {content}"[:5000],
                        "query": query,
                    }
                    send_message(
                        producer,
                        self.settings.topic_news,
                        message,
                        key=source_name,
                    )
                    total += 1

            except Exception:
                log.exception("news_query_failed", query=query)

        log.info("news_cycle_complete", messages_sent=total)
