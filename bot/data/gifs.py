# data/gifs.py
from __future__ import annotations
import logging
import random
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

GIPHY_SEARCH_URL = "https://api.giphy.com/v1/gifs/search"

# Curated search queries by market mood — crypto culture only
_QUERIES = {
    "bullish": [
        "crypto pump",
        "bitcoin to the moon",
        "crypto bull run",
        "bitcoin pump",
        "crypto green candle",
    ],
    "bearish": [
        "crypto crash",
        "bitcoin dump",
        "crypto rekt",
        "bitcoin bear",
        "crypto red candle",
    ],
    "excitement": [
        "crypto trending",
        "bitcoin hype",
        "crypto breaking news",
        "bitcoin signal",
        "crypto alert",
    ],
}

MAX_GIF_SIZE = 5_000_000  # 5MB — Twitter limit is 15MB but smaller loads faster


class GifFetcher:
    """Fetch GIFs from GIPHY API with curated crypto-culture search queries."""

    def __init__(self, giphy_api_key: str = ""):
        self.api_key = giphy_api_key
        self._session = create_session(retries=1, backoff_factor=0.5)

    def search(self, query: str, limit: int = 8) -> list:
        """Search GIPHY for GIFs. Returns list of GIF URLs."""
        if not self.api_key:
            return []
        try:
            resp = self._session.get(
                GIPHY_SEARCH_URL,
                params={
                    "api_key": self.api_key,
                    "q": query,
                    "limit": limit,
                    "rating": "pg-13",
                    "lang": "en",
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            urls = []
            for gif in data.get("data", []):
                images = gif.get("images", {})
                original = images.get("original", {})
                url = original.get("url", "")
                size = int(original.get("size", 0) or 0)
                if url and size < MAX_GIF_SIZE:
                    urls.append(url)
            return urls
        except Exception as exc:
            logger.warning("GIPHY search failed for %r: %s", query, exc)
            return []

    def fetch(self, mood: str) -> str | None:
        """Get a GIF URL for the given mood. Returns None if unavailable."""
        queries = _QUERIES.get(mood, _QUERIES["excitement"])
        query = random.choice(queries)
        urls = self.search(query)
        if urls:
            return random.choice(urls[:3])
        # Try a second query if first returned nothing
        remaining = [q for q in queries if q != query]
        if remaining:
            urls = self.search(random.choice(remaining))
            if urls:
                return random.choice(urls[:3])
        return None


class DryRunGifFetcher:
    def search(self, query: str, limit: int = 8) -> list:
        logger.info("[DRY RUN] GIF search: %r", query)
        return ["https://media.giphy.com/dry_run_gif.gif"]

    def fetch(self, mood: str) -> str | None:
        logger.info("[DRY RUN] GIF fetch mood=%s", mood)
        return "https://media.giphy.com/dry_run_gif.gif"
