"""Reddit data collector using PRAW."""

import re
import time
from datetime import datetime, timezone

import praw
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import Settings
from producer import send_message

log = structlog.get_logger()

# Words that look like tickers but aren't
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
MAX_SEEN_IDS = 50_000


class RedditCollector:
    def __init__(self, settings: Settings, valid_tickers: set[str]):
        self.settings = settings
        self.valid_tickers = valid_tickers
        self.seen_ids: set[str] = set()
        self.reddit = praw.Reddit(
            client_id=settings.reddit_client_id,
            client_secret=settings.reddit_client_secret,
            user_agent=settings.reddit_user_agent,
        )
        log.info("reddit_collector_initialized", subreddits=settings.subreddits)

    def _extract_tickers(self, text: str) -> list[str]:
        """Extract valid stock tickers from text."""
        matches = TICKER_PATTERN.findall(text)
        return list({
            m for m in matches
            if m in self.valid_tickers and m not in TICKER_BLACKLIST
        })

    def _trim_seen(self):
        if len(self.seen_ids) > MAX_SEEN_IDS:
            # Discard roughly half
            to_remove = list(self.seen_ids)[:MAX_SEEN_IDS // 2]
            self.seen_ids -= set(to_remove)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=60))
    def _fetch_new_posts(self, subreddit_name: str):
        subreddit = self.reddit.subreddit(subreddit_name)
        return list(subreddit.new(limit=50))

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=5, max=60))
    def _fetch_comments(self, subreddit_name: str):
        subreddit = self.reddit.subreddit(subreddit_name)
        return list(subreddit.comments(limit=100))

    def collect_cycle(self, producer):
        """Run one collection cycle across all subreddits."""
        total = 0
        for sub_name in self.settings.subreddits:
            try:
                total += self._collect_subreddit(producer, sub_name)
            except Exception:
                log.exception("subreddit_collection_failed", subreddit=sub_name)
        self._trim_seen()
        log.info("reddit_cycle_complete", messages_sent=total)

    def _collect_subreddit(self, producer, sub_name: str) -> int:
        count = 0

        # Posts
        for post in self._fetch_new_posts(sub_name):
            if post.id in self.seen_ids:
                continue
            self.seen_ids.add(post.id)

            full_text = f"{post.title} {post.selftext}"
            tickers = self._extract_tickers(full_text)
            if not tickers:
                continue

            message = {
                "source": "reddit",
                "type": "post",
                "id": post.id,
                "subreddit": sub_name,
                "author": str(post.author) if post.author else "[deleted]",
                "created_utc": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "title": post.title,
                "text": post.selftext[:1000],
                "full_text": full_text[:5000],
                "score": post.score,
                "num_comments": post.num_comments,
                "upvote_ratio": post.upvote_ratio,
                "tickers": tickers,
                "url": post.url,
                "permalink": f"https://reddit.com{post.permalink}",
            }
            send_message(producer, self.settings.topic_social_media, message, key=sub_name)
            count += 1

        # Comments
        for comment in self._fetch_comments(sub_name):
            if comment.id in self.seen_ids:
                continue
            self.seen_ids.add(comment.id)

            tickers = self._extract_tickers(comment.body)
            if not tickers:
                continue

            message = {
                "source": "reddit",
                "type": "comment",
                "id": comment.id,
                "subreddit": sub_name,
                "author": str(comment.author) if comment.author else "[deleted]",
                "created_utc": datetime.fromtimestamp(comment.created_utc, tz=timezone.utc).isoformat(),
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "title": "",
                "text": comment.body[:1000],
                "full_text": comment.body[:5000],
                "score": comment.score,
                "num_comments": 0,
                "upvote_ratio": 0.0,
                "tickers": tickers,
                "url": "",
                "permalink": f"https://reddit.com{comment.permalink}",
            }
            send_message(producer, self.settings.topic_social_media, message, key=sub_name)
            count += 1

        return count
