# data/trends.py
from __future__ import annotations

import logging
import threading
import time
import tweepy

logger = logging.getLogger(__name__)

_CACHE_TTL = 900  # 15 minutes — trends update frequently
_MAX_STALENESS = 3600  # 1 hour — refuse to serve cache older than this

# Only keep trends related to crypto or macro economics
CRYPTO_TERMS = {
    "btc", "bitcoin", "eth", "ethereum", "sol", "solana", "bnb", "xrp",
    "crypto", "blockchain", "defi", "nft", "web3", "altcoin", "memecoin",
    "stablecoin", "usdt", "usdc", "doge", "dogecoin", "ada", "cardano",
    "avax", "matic", "polygon", "dot", "polkadot", "link", "chainlink",
    "uni", "uniswap", "aave", "layer2", "l2", "rollup", "zk",
}
MACRO_TERMS = {
    "fed", "fomc", "inflation", "cpi", "gdp", "recession", "rate cut",
    "rate hike", "treasury", "dollar", "dxy", "s&p", "nasdaq", "dow",
    "tariff", "trade war", "employment", "payroll", "pce", "jobs report",
    "stock market", "wall street", "interest rate", "bond", "yield",
}

# WOEID 1 = Worldwide
_WOEID = 1


def _format_summary(trends: list) -> str:
    """Format trends as a one-liner for AI context."""
    if not trends:
        return ""
    parts = []
    for t in trends[:5]:
        vol = f" ({t['volume'] // 1000}K tweets)" if t.get("volume") else ""
        parts.append(f"{t['name']}{vol}")
    return "Currently trending on Twitter: " + " | ".join(parts)


def _extract_hashtags(trends: list) -> list[str]:
    """Extract hashtag-style trends (with #) for AI prompt."""
    return [t["name"] for t in trends if t["name"].startswith("#")][:3]


def _is_relevant(name: str) -> bool:
    """Check if a trend name matches crypto or macro keywords."""
    stripped = name.lower().lstrip("#")
    for term in CRYPTO_TERMS:
        if term in stripped:
            return True
    for term in MACRO_TERMS:
        if term in stripped:
            return True
    return False


class TrendsFetcher:
    def __init__(self, api_key: str, api_secret: str,
                 access_token: str, access_token_secret: str,
                 woeid: int = _WOEID):
        auth = tweepy.OAuth1UserHandler(
            api_key, api_secret, access_token, access_token_secret
        )
        self._api = tweepy.API(auth)
        self._woeid = woeid
        self._cache: list | None = None
        self._cache_time: float = 0.0
        self._last_fetch_time: float = 0.0  # tracks last successful API call (incl. empty results)
        self._lock = threading.Lock()
        self._fetching = False
        self._prev_snapshot: dict[str, int] = {}  # trend_name -> volume from last check

    def _fetch_raw(self) -> list | None:
        """Call Twitter v1.1 trends/place endpoint. Returns None on error."""
        try:
            results = self._api.get_place_trends(self._woeid)
        except Exception as exc:
            logger.warning("Twitter trends fetch failed: %s", exc)
            return None
        if not results or not results[0].get("trends"):
            return []
        return results[0]["trends"]

    def fetch_all(self) -> list:
        """Fetch, filter, and cache trending topics. Returns list of dicts."""
        with self._lock:
            now = time.monotonic()
            if self._cache is not None and (now - self._cache_time) < _CACHE_TTL:
                return self._cache
            # Also honour negative-cache: API succeeded but nothing relevant
            if self._last_fetch_time > 0 and (now - self._last_fetch_time) < _CACHE_TTL:
                return self._cache if self._cache else []
            if self._fetching:
                return self._cache if self._cache else []
            self._fetching = True

        fetch_ok = False
        filtered = []
        try:
            raw = self._fetch_raw()
            fetch_ok = raw is not None  # None = API error, [] = empty but successful
            for t in (raw or []):
                name = t.get("name", "")
                if not _is_relevant(name):
                    continue
                volume = t.get("tweet_volume") or 0
                filtered.append({
                    "name": name,
                    "volume": volume,
                    "url": t.get("url", ""),
                })
            # Sort by volume descending (trends with no volume data go last)
            filtered.sort(key=lambda x: x["volume"], reverse=True)
        finally:
            # Clear _fetching and update cache atomically to prevent a second
            # thread from starting a redundant fetch.
            with self._lock:
                self._fetching = False
                now = time.monotonic()
                if filtered:
                    self._cache = filtered
                    self._cache_time = now
                if fetch_ok:
                    self._last_fetch_time = now

        # Return logic outside finally — exceptions propagate correctly
        if filtered:
            return filtered
        with self._lock:
            if self._cache and (time.monotonic() - self._cache_time) < _MAX_STALENESS:
                logger.info("Trends fetch empty, serving stale cache (%d items)", len(self._cache))
                return self._cache
        return []

    def format_summary(self, trends: list | None = None) -> str:
        if trends is None:
            trends = self.fetch_all()
        return _format_summary(trends)

    def get_trending_hashtags(self, trends: list | None = None) -> list[str]:
        if trends is None:
            trends = self.fetch_all()
        return _extract_hashtags(trends)

    def _fetch_fresh(self) -> list | None:
        """Fetch and filter trends directly from API, bypassing cache.

        Does NOT update the shared cache — avoids disrupting other jobs that
        rely on the normal fetch_all() cache lifecycle.
        Returns None on API failure (vs empty list for "API succeeded, nothing relevant").
        """
        raw = self._fetch_raw()
        if raw is None:
            return None  # API error — distinguish from "no relevant trends"
        if not raw:
            return []
        filtered = []
        for t in raw:
            name = t.get("name", "")
            if not _is_relevant(name):
                continue
            volume = t.get("tweet_volume") or 0
            filtered.append({
                "name": name,
                "volume": volume,
                "url": t.get("url", ""),
            })
        filtered.sort(key=lambda x: x["volume"], reverse=True)
        return filtered

    def detect_new_trends(self, volume_threshold: int = 30_000) -> list:
        """Detect trends that are new or surging (volume >2x) since last check.

        Returns list of dicts: [{"name": ..., "volume": ..., "reason": "new"|"surge"}]
        First call after startup seeds the snapshot and returns empty (no false alerts).
        Uses a dedicated API call to avoid disrupting the shared cache.
        """
        current = self._fetch_fresh()
        if current is None:
            logger.warning("Trend detection: API fetch failed, skipping this cycle")
            return []
        if not current:
            return []

        current_map = {t["name"]: t["volume"] for t in current}

        with self._lock:
            # First call: seed snapshot, don't fire alerts for everything already trending
            if not self._prev_snapshot:
                self._prev_snapshot = current_map
                logger.info("Trend detection: seeded initial snapshot with %d trends", len(current_map))
                return []

            prev = self._prev_snapshot
            self._prev_snapshot = current_map

        alerts = []
        for t in current:
            name = t["name"]
            volume = t["volume"]
            if volume < volume_threshold:
                continue
            prev_vol = prev.get(name)
            if prev_vol is None:
                # Brand new trend
                alerts.append({**t, "reason": "new"})
            elif prev_vol > 0 and volume >= prev_vol * 2:
                # Volume surged >2x
                alerts.append({**t, "reason": "surge", "prev_volume": prev_vol})

        return alerts


class DryRunTrendsFetcher:
    """Fake trends for --dry-run mode."""

    def fetch_all(self) -> list:
        logger.info("[DRY RUN] Returning fake trending topics")
        return [
            {"name": "#Bitcoin", "volume": 150000, "url": ""},
            {"name": "#CryptoMarket", "volume": 50000, "url": ""},
            {"name": "Fed rate", "volume": 80000, "url": ""},
        ]

    def format_summary(self, trends: list | None = None) -> str:
        if trends is None:
            trends = self.fetch_all()
        return _format_summary(trends)

    def get_trending_hashtags(self, trends: list | None = None) -> list[str]:
        if trends is None:
            trends = self.fetch_all()
        return _extract_hashtags(trends)

    _dry_run_seeded = False

    def detect_new_trends(self, volume_threshold: int = 30_000) -> list:
        # Mimic real behavior: first call seeds, second call returns alerts
        if not self._dry_run_seeded:
            self._dry_run_seeded = True
            logger.info("[DRY RUN] Trend detection: seeded initial snapshot")
            return []
        logger.info("[DRY RUN] Returning fake new trend alerts")
        return [
            {"name": "#Bitcoin", "volume": 150000, "url": "", "reason": "surge", "prev_volume": 60000},
            {"name": "FOMC", "volume": 80000, "url": "", "reason": "new"},
        ]
