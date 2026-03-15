# data/derivatives.py
import logging
import threading
import time as _time
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

BASE_URL = "https://open-api-v4.coinglass.com"
_CACHE_TTL = 300  # 5 minutes


class DerivativesFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"CG-API-KEY": api_key}
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

    def get_funding_rate(self, symbol: str = "BTC") -> dict:
        """Current average funding rate across exchanges for a given symbol."""
        cache_key = f"funding_rate_{symbol}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        try:
            resp = self._session.get(
                f"{BASE_URL}/api/futures/fundingRate/exchange-list",
                headers=self.headers,
                params={"symbol": symbol},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if str(data.get("code")) == "0" and data.get("data"):
                rates = []
                for item in data["data"]:
                    try:
                        if item.get("rate") is not None:
                            rates.append(float(item["rate"]))
                    except (ValueError, TypeError):
                        continue
                if rates:
                    result = {"avg_rate": sum(rates) / len(rates)}
                    self._set_cached(cache_key, result)
                    return result
        except Exception as exc:
            logger.warning("CoinGlass funding rate failed for %s: %s", symbol, exc)
        return {}

    def get_btc_funding_rate(self) -> dict:
        """Backward-compatible alias for get_funding_rate('BTC')."""
        return self.get_funding_rate("BTC")

    def get_open_interest(self, symbol: str = "BTC") -> dict:
        """Current total open interest across exchanges (USD) for a given symbol."""
        cache_key = f"open_interest_{symbol}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        try:
            resp = self._session.get(
                f"{BASE_URL}/api/futures/openInterest/exchange-list",
                headers=self.headers,
                params={"symbol": symbol},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if str(data.get("code")) == "0" and data.get("data"):
                total = 0
                for item in data["data"]:
                    try:
                        total += float(item.get("openInterestUsd", 0) or 0)
                    except (ValueError, TypeError):
                        continue
                result = {"total_oi_usd": total}
                if total > 0:  # Don't cache clearly-invalid zero OI
                    self._set_cached(cache_key, result)
                return result
        except Exception as exc:
            logger.warning("CoinGlass open interest failed for %s: %s", symbol, exc)
        return {}

    def get_btc_open_interest(self) -> dict:
        """Backward-compatible alias for get_open_interest('BTC')."""
        return self.get_open_interest("BTC")

    def get_btc_snapshot(self) -> dict:
        result = {}
        result.update(self.get_btc_funding_rate())
        result.update(self.get_btc_open_interest())
        return result

    def _get_symbol_snapshot(self, symbol: str) -> dict:
        """Get derivatives snapshot for any symbol."""
        result = {}
        result.update(self.get_funding_rate(symbol))
        result.update(self.get_open_interest(symbol))
        return result

    # Coins with active perp futures on major exchanges
    _MULTI_SYMBOLS = [("btc", "BTC"), ("eth", "ETH"), ("sol", "SOL")]

    def get_multi_snapshot(self) -> dict:
        """Return combined derivatives snapshot for BTC, ETH, SOL."""
        return {key: self._get_symbol_snapshot(sym) for key, sym in self._MULTI_SYMBOLS}

    def format_summary(self, data: dict) -> str:
        parts = []
        if "avg_rate" in data:
            rate_pct = data["avg_rate"] * 100
            sign = "+" if rate_pct >= 0 else ""
            if rate_pct > 0.05:
                sentiment = "longs paying premium"
            elif rate_pct < -0.01:
                sentiment = "shorts paying premium"
            else:
                sentiment = "neutral"
            parts.append(f"Funding: {sign}{rate_pct:.3f}% ({sentiment})")
        if "total_oi_usd" in data:
            oi_b = data["total_oi_usd"] / 1e9
            parts.append(f"OI: ${oi_b:.1f}B")
        return " | ".join(parts) if parts else ""

    def _format_symbol_summary(self, symbol: str, data: dict) -> str:
        """Format derivatives data for a single symbol."""
        parts = []
        if "avg_rate" in data:
            rate_pct = data["avg_rate"] * 100
            sign = "+" if rate_pct >= 0 else ""
            if rate_pct > 0.05:
                sentiment = "longs paying premium"
            elif rate_pct < -0.01:
                sentiment = "shorts paying premium"
            else:
                sentiment = "neutral"
            parts.append(f"Funding: {sign}{rate_pct:.3f}% ({sentiment})")
        if "total_oi_usd" in data:
            oi_b = data["total_oi_usd"] / 1e9
            parts.append(f"OI: ${oi_b:.1f}B")
        if parts:
            return f"{symbol}: " + " | ".join(parts)
        return ""

    def format_multi_summary(self, data: dict) -> str:
        """Format multi-coin derivatives data for AI context."""
        if not data:
            return ""
        sections = []
        for key, label in self._MULTI_SYMBOLS:
            s = self._format_symbol_summary(label, data.get(key, {}))
            if s:
                sections.append(s)
        return " | ".join(sections) if sections else ""
