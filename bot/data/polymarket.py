# data/polymarket.py
"""Polymarket prediction market data fetcher.

Uses the Gamma API (public, no auth required) to fetch trending
prediction markets — crypto, macro, geopolitics. Gives the AI
unique context that few other crypto KOLs have.
"""
import logging
import threading
import time as _time
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"
_CACHE_TTL = 600  # 10 minutes — market odds don't move that fast

# Tags that are relevant to our audience (macro + geopolitics + politics)
_RELEVANT_TAGS = {
    "finance", "federal reserve", "interest rates", "economy",
    "gdp", "tariffs", "trade", "geopolitics", "military",
    "politics", "elections", "regulation",
}


class PolymarketFetcher:
    def __init__(self):
        self._session = create_session()
        self._cache: dict = {}
        self._lock = threading.Lock()

    def _get_cached(self, key: str):
        with self._lock:
            entry = self._cache.get(key)
            if entry and (_time.monotonic() - entry[1]) < _CACHE_TTL:
                return entry[0]
        return None

    def _set_cached(self, key: str, data):
        with self._lock:
            self._cache[key] = (data, _time.monotonic())

    def get_trending_markets(self, limit: int = 100) -> list:
        """Fetch top markets by 24h volume. Returns list of market dicts."""
        cached = self._get_cached("trending")
        if cached is not None:
            return cached
        try:
            resp = self._session.get(
                f"{GAMMA_API}/markets",
                params={
                    "limit": limit,
                    "active": "true",
                    "closed": "false",
                    "order": "volume24hr",
                    "ascending": "false",
                },
                timeout=10,
            )
            resp.raise_for_status()
            markets = resp.json()
            if not isinstance(markets, list):
                return []
            result = self._filter_relevant(markets)
            if result:
                self._set_cached("trending", result)
            return result
        except Exception as exc:
            logger.warning("Polymarket trending fetch failed: %s", exc)
            return []

    # Keywords in question text that signal relevance to our audience
    # Polymarket's unique value = macro/geopolitics/politics odds, NOT crypto prices
    # Crypto price brackets ("BTC above $70K?") are boring — macro is the differentiator
    _RELEVANT_KEYWORDS = [
        # Macro / Central banks
        "fed", "interest rate", "tariff", "inflation", "recession", "gdp",
        "treasury", "bond", "yield", "dollar", "rate cut", "rate hike",
        "central bank", "monetary policy", "employment", "jobs", "cpi", "pce",
        # Geopolitics
        "trump", "china", "trade war", "war", "iran", "russia", "ukraine",
        "sanctions", "nato", "middle east", "israel", "north korea",
        # Politics / Regulation
        "sec", "regulation", "ban", "congress", "senate", "election",
        "president", "supreme court", "executive order", "legislation",
        # Markets / Risk sentiment
        "oil", "gold", "commodit", "s&p", "nasdaq", "stock market",
        "recession", "default", "debt ceiling", "shutdown",
        # Tech / AI (macro-relevant)
        "ai ", "artificial intelligence", "nvidia", "apple", "google",
        "antitrust", "monopoly",
    ]

    def _filter_relevant(self, markets: list) -> list:
        """Keep only markets relevant to our crypto/macro audience."""
        relevant = []
        for m in markets:
            # Tags can be list of dicts (from events) or list of strings or None
            raw_tags = m.get("tags") or []
            tags = set()
            for t in raw_tags:
                if isinstance(t, str):
                    tags.add(t.lower())
                elif isinstance(t, dict) and t.get("label"):
                    tags.add(t["label"].lower())
            question = (m.get("question") or "").lower()
            is_relevant = (
                bool(tags & _RELEVANT_TAGS)
                or any(kw in question for kw in self._RELEVANT_KEYWORDS)
            )
            if is_relevant:
                norm = self._normalize(m)
                # Skip boring markets where outcome is near-certain
                prob = norm.get("yes_prob")
                if prob is not None and (prob < 0.05 or prob > 0.95):
                    continue
                relevant.append(norm)
        return relevant

    def _normalize(self, m: dict) -> dict:
        """Extract the fields we care about into a clean dict."""
        import json as _json
        raw_prices = m.get("outcomePrices") or []
        # API returns outcomePrices as a JSON-encoded string, not a list
        if isinstance(raw_prices, str):
            try:
                raw_prices = _json.loads(raw_prices)
            except (ValueError, TypeError):
                raw_prices = []
        yes_prob = None
        if raw_prices and len(raw_prices) >= 1:
            try:
                yes_prob = float(raw_prices[0])
            except (ValueError, TypeError):
                pass
        vol_24h = 0
        try:
            vol_24h = float(m.get("volume24hr") or 0)
        except (ValueError, TypeError):
            pass
        return {
            "question": m.get("question", ""),
            "yes_prob": yes_prob,
            "volume_24h": vol_24h,
            "end_date": m.get("endDate", ""),
        }

    def get_polymarket_snapshot(self) -> dict:
        """Get a snapshot of trending prediction markets for AI context."""
        markets = self.get_trending_markets()
        if not markets:
            return {}
        return {"markets": markets[:8]}  # top 8 most relevant by volume

    def format_summary(self, data: dict) -> str:
        """Format prediction market data for AI context."""
        if not data:
            return ""
        markets = data.get("markets", [])
        if not markets:
            return ""
        lines = []
        for m in markets[:5]:  # top 5 in context to save tokens
            q = m["question"]
            prob = m.get("yes_prob")
            vol = m.get("volume_24h", 0)
            if prob is not None:
                prob_str = f"{prob * 100:.0f}%"
            else:
                prob_str = "?"
            if vol >= 1e6:
                vol_str = f"${vol / 1e6:.1f}M"
            elif vol >= 1e3:
                vol_str = f"${vol / 1e3:.0f}K"
            else:
                vol_str = f"${vol:.0f}"
            lines.append(f'"{q}" → {prob_str} YES ({vol_str} 24h vol)')
        return "Polymarket: " + " | ".join(lines)


class DryRunPolymarketFetcher:
    """Fake prediction market data for --dry-run mode."""

    def get_trending_markets(self, limit: int = 20) -> list:
        logger.info("[DRY RUN] Returning fake Polymarket data")
        return [
            {"question": "Will Bitcoin reach $150,000 in March?",
             "yes_prob": 0.12, "volume_24h": 2_500_000, "end_date": "2026-03-31"},
            {"question": "Will the Fed decrease rates in March 2026?",
             "yes_prob": 0.35, "volume_24h": 1_800_000, "end_date": "2026-03-19"},
            {"question": "Will Ethereum flip Bitcoin by market cap in 2026?",
             "yes_prob": 0.03, "volume_24h": 800_000, "end_date": "2026-12-31"},
        ]

    def get_polymarket_snapshot(self) -> dict:
        return {"markets": self.get_trending_markets()}

    def format_summary(self, data: dict) -> str:
        if not data:
            return ""
        return ('Polymarket: "Will Bitcoin reach $150K in March?" → 12% YES ($2.5M 24h vol) | '
                '"Will the Fed decrease rates in March 2026?" → 35% YES ($1.8M 24h vol)')
