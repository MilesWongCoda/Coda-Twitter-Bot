# data/prices.py
from __future__ import annotations

import logging
import threading
import time
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

CRYPTO_IDS = ["bitcoin", "ethereum", "solana", "binancecoin", "ripple"]
FEAR_GREED_IMAGE_URL = "https://alternative.me/crypto/fear-and-greed-index.png"

_CACHE_TTL = 120  # 2 minutes — CoinGecko free tier: 50 calls/min
_TRENDING_CACHE_TTL = 7200  # 2 hours — trending list changes slowly


class PriceFetcher:
    def __init__(self, coingecko_api_key: str):
        self.api_key = coingecko_api_key
        self.base_url = "https://api.coingecko.com/api/v3"
        self._session = create_session()
        self._cache: dict = {}       # key → (data, timestamp)
        self._lock = threading.Lock()

    def _get_cached(self, key: str, ttl: int = None):
        with self._lock:
            entry = self._cache.get(key)
            if entry and (time.monotonic() - entry[1]) < (ttl or _CACHE_TTL):
                return entry[0]
        return None

    def _set_cached(self, key: str, data):
        with self._lock:
            self._cache[key] = (data, time.monotonic())

    def get_crypto_prices(self, coin_ids: list = None) -> dict:
        ids = coin_ids or CRYPTO_IDS
        cache_key = f"prices:{','.join(sorted(ids))}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        url = f"{self.base_url}/simple/price"
        params = {
            "ids": ",".join(ids),
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        }
        headers = {"x-cg-demo-api-key": self.api_key} if self.api_key else {}
        try:
            resp = self._session.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data:
                self._set_cached(cache_key, data)
            return data
        except Exception as exc:
            logger.warning("CoinGecko prices fetch failed: %s", exc)
            return {}

    def format_crypto_summary(self, prices: dict) -> str:
        SYMBOLS = {
            "bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL",
            "binancecoin": "BNB", "ripple": "XRP",
        }
        lines = []
        for coin_id, data in prices.items():
            symbol = SYMBOLS.get(coin_id, coin_id.upper())
            try:
                usd = data.get("usd")
                change_raw = data.get("usd_24h_change")
                if usd is None:
                    lines.append(f"{symbol}: N/A")
                    continue
                price = float(usd)
                change = float(change_raw) if change_raw is not None else 0.0
                if price >= 100:
                    price_str = f"${price:,.0f}"
                elif price >= 1:
                    price_str = f"${price:,.2f}"
                else:
                    price_str = f"${price:.4f}"
                sign = "+" if change >= 0 else ""
                lines.append(f"{symbol}: {price_str} ({sign}{change:.1f}%)")
            except (TypeError, ValueError):
                lines.append(f"{symbol}: N/A")
        return " | ".join(lines)

    def get_fear_greed(self) -> dict:
        cached = self._get_cached("fear_greed")
        if cached is not None:
            return cached
        try:
            resp = self._session.get(
                "https://api.alternative.me/fng/?limit=1", timeout=10
            )
            resp.raise_for_status()
            data_list = resp.json().get("data", [])
            if not data_list:
                logger.warning("Fear & Greed API returned empty data list")
                return {}
            entry = data_list[0]
            value_raw = entry.get("value")
            if value_raw is None:
                return {}
            data = {
                "value": int(value_raw),
                "label": entry.get("value_classification", ""),
            }
            self._set_cached("fear_greed", data)
            return data
        except Exception as exc:
            logger.warning("Fear & Greed fetch failed: %s", exc)
            return {}

    def get_ohlc(self, coin_id: str = "bitcoin", days: int = 7) -> list | None:
        """Fetch OHLC data from CoinGecko. Returns [[timestamp_ms, open, high, low, close], ...]."""
        cache_key = f"ohlc:{coin_id}:{days}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        url = f"{self.base_url}/coins/{coin_id}/ohlc"
        params = {"vs_currency": "usd", "days": days}
        headers = {"x-cg-demo-api-key": self.api_key} if self.api_key else {}
        try:
            resp = self._session.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or len(data) < 2:
                logger.warning("CoinGecko OHLC returned insufficient data for %s", coin_id)
                return None
            self._set_cached(cache_key, data)
            return data
        except Exception as exc:
            logger.warning("CoinGecko OHLC fetch failed for %s: %s", coin_id, exc)
            return None

    def get_prices_with_1h_change(self, coin_ids: list = None) -> list:
        """Fetch prices with 1h and 24h change via /coins/markets. Returns list of dicts."""
        ids = coin_ids or CRYPTO_IDS
        cache_key = f"markets_1h:{','.join(sorted(ids))}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        url = f"{self.base_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": ",".join(ids),
            "price_change_percentage": "1h,24h",
            "per_page": len(ids),
            "page": 1,
        }
        headers = {"x-cg-demo-api-key": self.api_key} if self.api_key else {}
        try:
            resp = self._session.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data:
                self._set_cached(cache_key, data)
            return data
        except Exception as exc:
            logger.warning("CoinGecko markets fetch failed: %s", exc)
            return []

    def get_weekly_changes(self, coin_ids: list = None) -> list:
        """Fetch 7-day price changes via /coins/markets. Returns list of dicts."""
        ids = coin_ids or CRYPTO_IDS
        cache_key = f"markets_7d:{','.join(sorted(ids))}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        url = f"{self.base_url}/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": ",".join(ids),
            "price_change_percentage": "7d",
            "per_page": len(ids),
            "page": 1,
        }
        headers = {"x-cg-demo-api-key": self.api_key} if self.api_key else {}
        try:
            resp = self._session.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data:
                self._set_cached(cache_key, data)
            return data
        except Exception as exc:
            logger.warning("CoinGecko weekly changes fetch failed: %s", exc)
            return []

    def get_trending_coins(self) -> list:
        """CoinGecko /search/trending — top coins by user search volume (updates every 10 min)."""
        cached = self._get_cached("trending_coins", ttl=_TRENDING_CACHE_TTL)
        if cached is not None:
            return cached
        url = f"{self.base_url}/search/trending"
        headers = {"x-cg-demo-api-key": self.api_key} if self.api_key else {}
        try:
            resp = self._session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            raw = resp.json()
            coins = []
            for item in (raw.get("coins") or []):
                coin = item.get("item", {})
                coins.append({
                    "name": coin.get("name", ""),
                    "symbol": (coin.get("symbol") or "").upper(),
                    "score": coin.get("score", 0),
                    "market_cap_rank": coin.get("market_cap_rank"),
                })
            if coins:
                self._set_cached("trending_coins", coins)
            return coins
        except Exception as exc:
            logger.warning("CoinGecko trending fetch failed: %s", exc)
            return []

    def format_trending_summary(self, coins: list = None) -> str:
        """Format trending coins into a cashtag-ready context string."""
        if coins is None:
            coins = self.get_trending_coins()
        if not coins:
            return ""
        parts = []
        for c in coins[:10]:
            sym = c.get("symbol", "")
            name = c.get("name", "")
            rank = c.get("market_cap_rank")
            rank_str = f" (rank #{rank})" if rank else ""
            if sym:
                parts.append(f"${sym} {name}{rank_str}")
        return "Today's trending coins on CoinGecko: " + ", ".join(parts) if parts else ""

    def format_fear_greed(self, data: dict) -> str:
        if not data:
            return ""
        value = data.get("value")
        label = data.get("label")
        if value is None or label is None:
            return ""
        return f"Fear & Greed: {value}/100 ({label})"
