# data/whale_alerts.py
import logging
import threading
import time
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

BASE_URL = "https://api.whale-alert.io/v1"
_CACHE_TTL = 300         # 5 minutes — near-realtime data
_MAX_STALENESS = 1800    # 30 minutes
_DEFAULT_MIN_VALUE = 500_000  # $500K (free tier minimum)
_LOOKBACK_SECONDS = 3600      # 1 hour lookback


class WhaleAlertFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._session = create_session()
        self._cache = None
        self._cache_time: float = 0.0
        self._lock = threading.Lock()
        self._fetching = False

    def get_recent_transactions(self, min_value: int = _DEFAULT_MIN_VALUE,
                                lookback: int = _LOOKBACK_SECONDS) -> list:
        """Fetch recent large transactions."""
        with self._lock:
            now = time.monotonic()
            if self._cache is not None and (now - self._cache_time) < _CACHE_TTL:
                return self._cache
            if self._fetching:
                return self._cache if self._cache else []
            self._fetching = True
            stale = self._cache if self._cache and (now - self._cache_time) < _MAX_STALENESS else None
        try:
            start = int(time.time()) - lookback
            resp = self._session.get(
                f"{BASE_URL}/transactions",
                params={
                    "api_key": self.api_key,
                    "min_value": min_value,
                    "start": start,
                    "currency": "btc,eth,sol,xrp",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("result") == "success":
                txs = data.get("transactions", [])
                if txs:  # don't overwrite valid cache with empty result
                    with self._lock:
                        self._cache = txs
                        self._cache_time = time.monotonic()
                return txs
        except Exception as exc:
            logger.warning("Whale Alert fetch failed: %s", exc)
        finally:
            try:
                with self._lock:
                    self._fetching = False
            except BaseException:
                self._fetching = False
        if stale is not None:
            return stale
        return []

    def get_whale_snapshot(self) -> dict:
        """Get a summary of recent whale activity."""
        txs = self.get_recent_transactions()
        if not txs:
            return {}
        total_usd = 0
        exchange_inflows = 0
        exchange_outflows = 0
        count = len(txs)
        for tx in txs:
            amount_usd = float(tx.get("amount_usd", 0) or 0)
            total_usd += amount_usd
            to_owner = tx.get("to") or {}
            from_owner = tx.get("from") or {}
            if to_owner.get("owner_type") == "exchange":
                exchange_inflows += amount_usd
            if from_owner.get("owner_type") == "exchange":
                exchange_outflows += amount_usd
        return {
            "count": count,
            "total_usd": total_usd,
            "exchange_inflows_usd": exchange_inflows,
            "exchange_outflows_usd": exchange_outflows,
        }

    def format_summary(self, data: dict) -> str:
        """Format whale alert data for AI context."""
        if not data:
            return ""
        parts = []
        count = data.get("count", 0)
        total = data.get("total_usd", 0)
        if count and total:
            if total >= 1e9:
                parts.append(f"Whale alerts (1h): {count} txs, ${total / 1e9:.1f}B moved")
            else:
                parts.append(f"Whale alerts (1h): {count} txs, ${total / 1e6:.0f}M moved")
        inflows = data.get("exchange_inflows_usd", 0)
        outflows = data.get("exchange_outflows_usd", 0)
        if inflows or outflows:
            if inflows > outflows:
                net = inflows - outflows
                parts.append(f"Net ${net / 1e6:.0f}M to exchanges (bearish)")
            elif outflows > inflows:
                net = outflows - inflows
                parts.append(f"Net ${net / 1e6:.0f}M from exchanges (bullish)")
        return " | ".join(parts) if parts else ""


class DryRunWhaleAlertFetcher:
    """Fake whale alert data for --dry-run mode."""

    def get_recent_transactions(self, **kwargs) -> list:
        logger.info("[DRY RUN] Returning fake whale transactions")
        return [
            {"blockchain": "bitcoin", "symbol": "btc", "amount": 500,
             "amount_usd": 47_500_000,
             "from": {"owner": "unknown", "owner_type": "unknown"},
             "to": {"owner": "Coinbase", "owner_type": "exchange"}},
            {"blockchain": "bitcoin", "symbol": "btc", "amount": 300,
             "amount_usd": 28_500_000,
             "from": {"owner": "Binance", "owner_type": "exchange"},
             "to": {"owner": "unknown", "owner_type": "unknown"}},
        ]

    def get_whale_snapshot(self) -> dict:
        return {
            "count": 3,
            "total_usd": 125_000_000,
            "exchange_inflows_usd": 80_000_000,
            "exchange_outflows_usd": 45_000_000,
        }

    def format_summary(self, data: dict) -> str:
        if not data:
            return ""
        return "Whale alerts (1h): 3 txs, $125M moved | Net $35M to exchanges (bearish)"
