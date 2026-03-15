# data/news.py
import logging
import threading
import time
import feedparser
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

FEEDS = [
    # Crypto
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://www.theblock.co/rss.xml",
    "https://decrypt.co/feed",
    # Macro / US stocks
    "https://feeds.reuters.com/reuters/businessNews",
    "https://finance.yahoo.com/news/rssindex",
]

_CACHE_TTL = 1800  # 30 minutes
_MAX_STALENESS = 21600  # 6 hours — refuse to serve cache older than this


class NewsFetcher:
    def __init__(self, feeds: list = None):
        self.feeds = feeds or FEEDS
        self._session = create_session()
        self._cache = None  # None = no cache yet; only set when non-empty
        self._cache_time: float = 0.0
        self._lock = threading.Lock()  # guard cache state across scheduler threads
        self._fetching = False  # prevent duplicate fetches from concurrent threads

    def fetch_feed(self, url: str) -> list:
        # Use requests with per-call timeout instead of global socket.setdefaulttimeout
        # which is unsafe in multi-threaded environments (scheduler runs jobs concurrently)
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
        if feed.bozo and not feed.entries:
            logger.warning("Malformed feed from %s: %s", url,
                           getattr(feed, 'bozo_exception', 'unknown'))
            return []
        articles = []
        for entry in feed.entries[:10]:
            articles.append({
                "title": getattr(entry, "title", ""),
                "summary": getattr(entry, "summary", ""),
                "link": getattr(entry, "link", ""),
                "published": getattr(entry, "published", ""),
            })
        return articles

    def fetch_all(self) -> list:
        # Check cache under lock — fast path
        with self._lock:
            now = time.monotonic()
            if self._cache is not None and (now - self._cache_time) < _CACHE_TTL:
                return self._cache
            # Another thread is already fetching — serve stale cache or empty
            if self._fetching:
                return self._cache if self._cache else []
            self._fetching = True

        # Fetch feeds outside the lock so a slow/stalled feed host (up to 15s each)
        # doesn't block other scheduler threads from reading the cache.
        result = []
        try:
            all_articles = []
            for url in self.feeds:
                try:
                    all_articles.extend(self.fetch_feed(url))
                except Exception as exc:
                    logger.warning("Failed to fetch feed %s: %s", url, exc)
            result = self.deduplicate(all_articles)
        except Exception as exc:
            logger.error("News fetch/dedup failed: %s", exc)
        finally:
            # Clear _fetching and write cache atomically to prevent a second
            # thread from starting a redundant fetch before cache is updated.
            with self._lock:
                self._fetching = False
                now = time.monotonic()
                if not (self._cache is not None and (now - self._cache_time) < _CACHE_TTL):
                    if result:
                        self._cache = result
                        self._cache_time = now

        if result:
            return result
        with self._lock:
            if self._cache and (time.monotonic() - self._cache_time) < _MAX_STALENESS:
                logger.info("All feeds returned empty, serving stale cache (%d articles)", len(self._cache))
                return self._cache
        return []

    def detect_hot_topics(self, min_sources: int = 2) -> list:
        """Detect topics covered by multiple RSS sources — signals a hot story.

        Scans article titles for known crypto coin names and macro keywords.
        Returns topics mentioned by >= min_sources different feeds.
        """
        articles = self.fetch_all()
        if not articles:
            return []

        # Map each feed URL prefix to a source name for counting
        _COIN_KEYWORDS = {
            "bitcoin": "BTC", "btc": "BTC", "ethereum": "ETH", "eth": "ETH",
            "solana": "SOL", "sol": "SOL", "xrp": "XRP", "ripple": "XRP",
            "bnb": "BNB", "binance": "BNB", "dogecoin": "DOGE", "doge": "DOGE",
            "cardano": "ADA", "ada": "ADA", "avalanche": "AVAX", "avax": "AVAX",
        }
        _MACRO_KEYWORDS = {
            "fed": "Fed", "fomc": "FOMC", "inflation": "Inflation",
            "rate cut": "Rate Cut", "rate hike": "Rate Hike",
            "cpi": "CPI", "gdp": "GDP", "tariff": "Tariff",
            "etf": "ETF", "sec": "SEC",
        }

        # topic -> set of source domains
        topic_sources: dict[str, set] = {}
        topic_titles: dict[str, str] = {}  # store latest title per topic

        for article in articles:
            title = (article.get("title") or "").lower()
            link = article.get("link") or ""
            # Extract source domain from link
            source = link.split("/")[2] if link.count("/") >= 2 else link

            for keyword, label in {**_COIN_KEYWORDS, **_MACRO_KEYWORDS}.items():
                if keyword in title:
                    if label not in topic_sources:
                        topic_sources[label] = set()
                        topic_titles[label] = article.get("title", "")
                    topic_sources[label].add(source)

        # Filter to topics covered by enough sources
        hot = []
        for label, sources in topic_sources.items():
            if len(sources) >= min_sources:
                hot.append({
                    "topic": label,
                    "count": len(sources),
                    "sources": sorted(sources),
                    "latest_title": topic_titles.get(label, ""),
                })
        # Sort by source count descending
        hot.sort(key=lambda x: x["count"], reverse=True)
        return hot

    def deduplicate(self, articles: list) -> list:
        seen = set()
        unique = []
        for a in articles:
            link = a.get("link", "").strip()
            if not link:
                continue  # skip articles without a link — can't reference them
            if link not in seen:
                seen.add(link)
                unique.append(a)
        return unique
