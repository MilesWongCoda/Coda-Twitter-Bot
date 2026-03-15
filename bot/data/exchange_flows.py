# data/exchange_flows.py
import logging
import threading
import time
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

BASE_URL = "https://open-api-v4.coinglass.com"
_CACHE_TTL = 900        # 15 minutes — exchange flows update frequently
_MAX_STALENESS = 3600   # 1 hour


class ExchangeFlowFetcher:
    """Fetch BTC/ETH exchange balance data from CoinGlass."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"CG-API-KEY": api_key}
        self._session = create_session()
        # Legacy single-symbol cache (BTC)
        self._cache = None
        self._cache_time: float = 0.0
        self._lock = threading.Lock()
        self._fetching = False
        # Per-symbol caches for multi-coin support
        self._symbol_cache: dict = {}  # {symbol: (data, timestamp)}
        self._symbol_fetching: dict = {}  # {symbol: bool}

    def get_exchange_balance(self) -> dict:
        """Get BTC exchange balance data. Returns raw API data."""
        with self._lock:
            now = time.monotonic()
            if self._cache is not None and (now - self._cache_time) < _CACHE_TTL:
                return self._cache
            if self._fetching:
                return self._cache if self._cache else {}
            self._fetching = True
            stale = self._cache if self._cache and (now - self._cache_time) < _MAX_STALENESS else None
        try:
            resp = self._session.get(
                f"{BASE_URL}/api/exchange/balance/list",
                headers=self.headers,
                params={"symbol": "BTC"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if str(data.get("code")) == "0" and data.get("data") is not None:
                result = data["data"]
                if result:  # don't cache empty list/dict
                    with self._lock:
                        self._cache = result
                        self._cache_time = time.monotonic()
                    return result
                # Empty but valid response — fall through to stale fallback
        except Exception as exc:
            logger.warning("CoinGlass exchange balance fetch failed: %s", exc)
        finally:
            try:
                with self._lock:
                    self._fetching = False
            except BaseException:
                self._fetching = False
        if stale is not None:
            return stale
        logger.warning("Exchange flow: fetch failed and no usable stale cache available")
        return {}

    def get_exchange_snapshot(self) -> dict:
        """Combined snapshot of exchange flow data."""
        balance = self.get_exchange_balance()
        if not balance:
            return {}
        return {"balance": balance}

    def format_summary(self, data: dict) -> str:
        """Format exchange flow data for AI context."""
        if not data:
            return ""
        balance = data.get("balance")
        if not balance:
            return ""
        parts = []
        # If balance is a list (per-exchange), aggregate
        if isinstance(balance, list):
            total = 0
            # change24h represents net change (positive = net inflow to exchange)
            change_24h = 0
            for item in balance:
                try:
                    total += float(item.get("balance", 0) or 0)
                    change_24h += float(item.get("change24h", 0) or 0)
                except (ValueError, TypeError):
                    continue
            if total > 0:
                parts.append(f"Exchange BTC balance: {total:,.0f} BTC")
            if change_24h != 0:
                direction = "inflow" if change_24h > 0 else "outflow"
                signal = "bearish" if change_24h > 0 else "bullish"
                parts.append(f"24h net {direction}: {abs(change_24h):,.0f} BTC ({signal})")
        # If balance is a dict (aggregated), try direct fields
        elif isinstance(balance, dict):
            total = balance.get("totalBalance")
            if total is None:
                total = balance.get("total")
            if total is not None:
                try:
                    parts.append(f"Exchange BTC balance: {float(total):,.0f} BTC")
                except (ValueError, TypeError):
                    logger.warning("Exchange format_summary: non-numeric totalBalance: %r", total)
            change = balance.get("change24h")
            if change is None:
                change = balance.get("netflow24h")
            if change is not None:
                try:
                    change_val = float(change)
                    direction = "inflow" if change_val > 0 else "outflow"
                    signal = "bearish" if change_val > 0 else "bullish"
                    parts.append(f"24h net {direction}: {abs(change_val):,.0f} BTC ({signal})")
                except (ValueError, TypeError):
                    logger.warning("Exchange format_summary: non-numeric change: %r", change)
        return " | ".join(parts) if parts else ""

    # ── Multi-coin (BTC + ETH) ────────────────────────────────────────

    def _get_exchange_balance_for(self, symbol: str) -> dict:
        """Get exchange balance for a given symbol. Uses per-symbol cache."""
        with self._lock:
            now = time.monotonic()
            entry = self._symbol_cache.get(symbol)
            if entry is not None and (now - entry[1]) < _CACHE_TTL:
                return entry[0]
            if self._symbol_fetching.get(symbol):
                return entry[0] if entry else {}
            self._symbol_fetching[symbol] = True
            stale = entry[0] if entry and (now - entry[1]) < _MAX_STALENESS else None
        try:
            resp = self._session.get(
                f"{BASE_URL}/api/exchange/balance/list",
                headers=self.headers,
                params={"symbol": symbol},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if str(data.get("code")) == "0" and data.get("data") is not None:
                result = data["data"]
                if result:
                    with self._lock:
                        self._symbol_cache[symbol] = (result, time.monotonic())
                    return result
        except Exception as exc:
            logger.warning("CoinGlass exchange balance fetch failed for %s: %s", symbol, exc)
        finally:
            try:
                with self._lock:
                    self._symbol_fetching[symbol] = False
            except BaseException:
                self._symbol_fetching[symbol] = False
        if stale is not None:
            return stale
        logger.warning("Exchange flow (%s): fetch failed and no usable stale cache", symbol)
        return {}

    # Coins to fetch for multi-coin snapshots
    _MULTI_SYMBOLS = [("btc", "BTC"), ("eth", "ETH"), ("sol", "SOL"), ("xrp", "XRP")]

    def get_exchange_balance_multi(self) -> dict:
        """Fetch exchange balances for BTC, ETH, SOL, XRP."""
        return {key: self._get_exchange_balance_for(sym) for key, sym in self._MULTI_SYMBOLS}

    def get_exchange_snapshot_multi(self) -> dict:
        """Combined snapshot of multi-coin exchange flow data."""
        multi = self.get_exchange_balance_multi()
        return {key: {"balance": multi[key]} for key in multi if multi[key]}

    def _format_balance_for_symbol(self, symbol: str, balance) -> list:
        """Format a single symbol's balance data into parts list."""
        parts = []
        if isinstance(balance, list):
            total = 0
            change_24h = 0
            for item in balance:
                try:
                    total += float(item.get("balance", 0) or 0)
                    change_24h += float(item.get("change24h", 0) or 0)
                except (ValueError, TypeError):
                    continue
            if total > 0:
                parts.append(f"Exchange {symbol} balance: {total:,.0f} {symbol}")
            if change_24h != 0:
                direction = "inflow" if change_24h > 0 else "outflow"
                signal = "bearish" if change_24h > 0 else "bullish"
                parts.append(f"24h net {direction}: {abs(change_24h):,.0f} {symbol} ({signal})")
        elif isinstance(balance, dict):
            total = balance.get("totalBalance")
            if total is None:
                total = balance.get("total")
            if total is not None:
                try:
                    parts.append(f"Exchange {symbol} balance: {float(total):,.0f} {symbol}")
                except (ValueError, TypeError):
                    pass
            change = balance.get("change24h")
            if change is None:
                change = balance.get("netflow24h")
            if change is not None:
                try:
                    change_val = float(change)
                    direction = "inflow" if change_val > 0 else "outflow"
                    signal = "bearish" if change_val > 0 else "bullish"
                    parts.append(f"24h net {direction}: {abs(change_val):,.0f} {symbol} ({signal})")
                except (ValueError, TypeError):
                    pass
        return parts

    def format_multi_summary(self, data: dict) -> str:
        """Format multi-coin exchange flow data for AI context."""
        if not data:
            return ""
        all_parts = []
        for symbol_key, label in self._MULTI_SYMBOLS:
            coin_data = data.get(symbol_key, {})
            balance = coin_data.get("balance") if coin_data else None
            if balance:
                all_parts.extend(self._format_balance_for_symbol(label, balance))
        return " | ".join(all_parts) if all_parts else ""


class DryRunExchangeFlowFetcher:
    """Fake exchange flow data for --dry-run mode."""

    def get_exchange_balance(self) -> dict:
        logger.info("[DRY RUN] Returning fake exchange balance")
        return {"totalBalance": 2_100_000, "change24h": -5200}

    def get_exchange_snapshot(self) -> dict:
        return {"balance": self.get_exchange_balance()}

    def format_summary(self, data: dict) -> str:
        if not data:
            return ""
        return "Exchange BTC balance: 2,100,000 BTC | 24h net outflow: 5,200 BTC (bullish)"

    def get_exchange_balance_multi(self) -> dict:
        logger.info("[DRY RUN] Returning fake multi exchange balance")
        return {
            "btc": {"totalBalance": 2_100_000, "change24h": -5200},
            "eth": {"totalBalance": 18_500_000, "change24h": 12000},
            "sol": {"totalBalance": 45_000_000, "change24h": -150000},
            "xrp": {"totalBalance": 3_200_000_000, "change24h": 25000000},
        }

    def get_exchange_snapshot_multi(self) -> dict:
        multi = self.get_exchange_balance_multi()
        return {k: {"balance": v} for k, v in multi.items()}

    def format_multi_summary(self, data: dict) -> str:
        if not data:
            return ""
        return ("Exchange BTC balance: 2,100,000 BTC | 24h net outflow: 5,200 BTC (bullish) | "
                "Exchange ETH balance: 18,500,000 ETH | 24h net inflow: 12,000 ETH (bearish) | "
                "Exchange SOL balance: 45,000,000 SOL | 24h net outflow: 150,000 SOL (bullish) | "
                "Exchange XRP balance: 3,200,000,000 XRP | 24h net inflow: 25,000,000 XRP (bearish)")
