# data/macro.py
"""Yahoo Finance macro data fetcher: DXY, S&P 500, VIX, US 10Y."""
import logging
import threading
import time
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

_CACHE_TTL = 300  # 5 minutes

# Yahoo Finance chart endpoint (public JSON, no API key)
_YF_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"

_SYMBOLS = {
    "dxy": "DX-Y.NYB",
    "sp500": "^GSPC",
    "vix": "^VIX",
    "us10y": "^TNX",
}

_LABELS = {
    "dxy": "DXY",
    "sp500": "S&P 500",
    "vix": "VIX",
    "us10y": "10Y",
}


class MacroFetcher:
    def __init__(self):
        self._session = create_session()
        self._cache: dict = {}
        self._lock = threading.Lock()

    def _get_cached(self, key: str):
        with self._lock:
            entry = self._cache.get(key)
            if entry and (time.monotonic() - entry[1]) < _CACHE_TTL:
                return entry[0]
        return None

    def _set_cached(self, key: str, data):
        with self._lock:
            self._cache[key] = (data, time.monotonic())

    def _fetch_quote(self, yf_symbol: str) -> dict:
        """Fetch current price + change % from Yahoo Finance chart endpoint."""
        cached = self._get_cached(yf_symbol)
        if cached is not None:
            return cached
        try:
            resp = self._session.get(
                _YF_URL.format(symbol=yf_symbol),
                params={"interval": "1d", "range": "2d"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            result = data["chart"]["result"][0]
            meta = result["meta"]
            price = meta["regularMarketPrice"]
            prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
            change_pct = ((price - prev_close) / prev_close * 100) if prev_close else 0
            quote = {"price": round(price, 2), "change_pct": round(change_pct, 2)}
            self._set_cached(yf_symbol, quote)
            return quote
        except Exception as exc:
            logger.warning("MacroFetcher: failed to fetch %s: %s", yf_symbol, exc)
            return {}

    def get_macro_snapshot(self) -> dict:
        """Return macro data: {dxy: {price, change_pct}, sp500: {...}, ...}."""
        snapshot = {}
        for key, symbol in _SYMBOLS.items():
            quote = self._fetch_quote(symbol)
            if quote:
                snapshot[key] = quote
        return snapshot

    @staticmethod
    def format_summary(data: dict) -> str:
        """Format macro snapshot into one-line summary."""
        if not data:
            return ""
        parts = []
        for key in ("dxy", "sp500", "vix", "us10y"):
            q = data.get(key)
            if not q:
                continue
            label = _LABELS[key]
            price = q["price"]
            change = q.get("change_pct", 0)
            if key == "sp500":
                price_str = f"{price:,.0f}"
            elif key in ("vix", "us10y"):
                price_str = f"{price:.2f}"
            else:
                price_str = f"{price:.1f}"
            sign = "+" if change >= 0 else ""
            if key == "us10y":
                parts.append(f"{label}: {price_str}%")
            else:
                parts.append(f"{label}: {price_str} ({sign}{change:.1f}%)")
        return " | ".join(parts)


class DryRunMacroFetcher:
    """Fake macro data for --dry-run mode."""

    def get_macro_snapshot(self) -> dict:
        return {
            "dxy": {"price": 104.2, "change_pct": -0.3},
            "sp500": {"price": 5234, "change_pct": 0.5},
            "vix": {"price": 18.2, "change_pct": 2.1},
            "us10y": {"price": 4.35, "change_pct": -0.5},
        }

    @staticmethod
    def format_summary(data: dict) -> str:
        return MacroFetcher.format_summary(data)
