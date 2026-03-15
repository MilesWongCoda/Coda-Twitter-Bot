# data/etf_flows.py
import logging
import threading
import time
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

BASE_URL = "https://open-api-v4.coinglass.com"
_CACHE_TTL = 3600       # 1 hour — ETF data updates daily after market close
_MAX_STALENESS = 7200   # 2 hours

_FLOW_KEYS = ("totalFlow", "net_inflow_usd", "netFlow", "flow_usd", "total")


class ETFFlowFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"CG-API-KEY": api_key}
        self._session = create_session()
        self._cache = None
        self._cache_time: float = 0.0
        self._lock = threading.Lock()
        self._fetching = False

    def get_btc_etf_flows(self) -> list:
        """Fetch BTC ETF daily flow history. Returns list of daily flow dicts."""
        with self._lock:
            now = time.monotonic()
            if self._cache is not None and (now - self._cache_time) < _CACHE_TTL:
                return self._cache
            if self._fetching:
                return self._cache if self._cache else []
            self._fetching = True
            stale = self._cache if self._cache and (now - self._cache_time) < _MAX_STALENESS else None
        try:
            resp = self._session.get(
                f"{BASE_URL}/api/etf/bitcoin/flow-history",
                headers=self.headers,
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            if str(data.get("code")) == "0" and data.get("data") is not None:
                result = data["data"]
                if isinstance(result, list) and result:
                    with self._lock:
                        self._cache = result
                        self._cache_time = time.monotonic()
                    return result
                logger.warning("CoinGlass ETF flow returned unexpected data type")
        except Exception as exc:
            logger.warning("CoinGlass ETF flow fetch failed: %s", exc)
        finally:
            try:
                with self._lock:
                    self._fetching = False
            except BaseException:
                self._fetching = False
        if stale is not None:
            return stale
        logger.warning("ETF flow: fetch failed and no usable stale cache available")
        return []

    def get_etf_snapshot(self) -> dict:
        """Get the most recent day's ETF flow data."""
        flows = self.get_btc_etf_flows()
        if not flows:
            return {}
        # Sort defensively — API order not guaranteed
        date_key = next((k for k in ("date", "timestamp", "time") if k in flows[0]), None)
        if date_key:
            flows = sorted(flows, key=lambda x: str(x.get(date_key, "")))
        return flows[-1]

    def format_summary(self, data: dict) -> str:
        """Format ETF flow data for AI context."""
        if not data:
            return ""
        net = None
        for key in _FLOW_KEYS:
            if key in data and data[key] is not None:
                try:
                    net = float(data[key])
                except (ValueError, TypeError):
                    logger.warning("ETF format_summary: non-numeric value for %s: %r", key, data[key])
                    continue
                break
        if net is None:
            logger.warning("ETF format_summary: no recognized flow key. Keys: %s",
                           list(data.keys())[:10])
            return ""
        sign = "+" if net >= 0 else "-"
        abs_net = abs(net)
        if abs_net >= 1e9:
            return f"BTC ETF net flow: {sign}${abs_net / 1e9:.2f}B"
        return f"BTC ETF net flow: {sign}${abs_net / 1e6:.1f}M"


class DryRunETFFlowFetcher:
    """Fake ETF flow data for --dry-run mode."""

    def get_btc_etf_flows(self) -> list:
        logger.info("[DRY RUN] Returning fake ETF flow data")
        return [{"date": "2025-02-22", "totalFlow": 150_000_000}]

    def get_etf_snapshot(self) -> dict:
        return self.get_btc_etf_flows()[0]

    def format_summary(self, data: dict) -> str:
        if not data:
            return ""
        return "BTC ETF net flow: +$150.0M"
