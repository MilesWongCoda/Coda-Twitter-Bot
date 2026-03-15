# data/onchain.py
import logging
import threading
import time as _time
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

_CACHE_TTL = 300  # 5 minutes


class OnChainFetcher:
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

    def get_blockchair_stats(self) -> dict:
        cached = self._get_cached("blockchair")
        if cached is not None:
            return cached
        try:
            resp = self._session.get("https://api.blockchair.com/bitcoin/stats", timeout=15)
            resp.raise_for_status()
            data = resp.json().get("data", {})
            if data:
                self._set_cached("blockchair", data)
            return data
        except Exception as exc:
            logger.warning("Blockchair stats fetch failed: %s", exc)
            return {}

    def get_mempool_fees(self) -> dict:
        cached = self._get_cached("mempool_fees")
        if cached is not None:
            return cached
        try:
            resp = self._session.get(
                "https://mempool.space/api/v1/fees/recommended", timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            if data:
                self._set_cached("mempool_fees", data)
            return data
        except Exception as exc:
            logger.warning("Mempool fees fetch failed: %s", exc)
            return {}

    @staticmethod
    def _safe_int(val):
        try:
            return int(val)
        except (ValueError, TypeError):
            logger.debug("_safe_int: cannot convert %r", val)
            return None

    @staticmethod
    def _safe_float(val):
        try:
            return float(val)
        except (ValueError, TypeError):
            logger.debug("_safe_float: cannot convert %r", val)
            return None

    def get_btc_snapshot(self) -> dict:
        result = {}
        stats = self.get_blockchair_stats()
        v = self._safe_int(stats.get("transactions_24h"))
        if v is not None:
            result["transactions_24h"] = v
        v = self._safe_float(stats.get("volume_24h"))
        if v is not None:
            result["transaction_volume_btc"] = v / 1e8  # Blockchair returns satoshis
        v = self._safe_int(stats.get("mempool_transactions"))
        if v is not None:
            result["mempool_tx"] = v
        v = self._safe_float(stats.get("hashrate_24h"))
        if v is not None:
            result["hashrate"] = v
        fees = self.get_mempool_fees()
        if fees:
            if fees.get("fastestFee") is not None:
                result["fastest_fee"] = fees["fastestFee"]
            if fees.get("hourFee") is not None:
                result["hour_fee"] = fees["hourFee"]
        return result

    def format_summary(self, data: dict) -> str:
        parts = []
        if "transactions_24h" in data:
            parts.append(f"24h TXs: {data['transactions_24h']:,}")
        if "transaction_volume_btc" in data:
            val = data["transaction_volume_btc"]
            parts.append(f"TX volume: {val:,.0f} BTC")
        if "hashrate" in data:
            parts.append(f"Hashrate: {data['hashrate'] / 1e18:.1f} EH/s")
        if "fastest_fee" in data:
            parts.append(f"Fees: {data['fastest_fee']} sat/vB")
        return " | ".join(parts) if parts else "On-chain data unavailable"

    # ── ETH on-chain data ──────────────────────────────────────────────

    def get_eth_stats(self) -> dict:
        """Fetch Ethereum blockchain stats from Blockchair."""
        cached = self._get_cached("blockchair_eth")
        if cached is not None:
            return cached
        try:
            resp = self._session.get(
                "https://api.blockchair.com/ethereum/stats", timeout=15
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            if data:
                self._set_cached("blockchair_eth", data)
            return data
        except Exception as exc:
            logger.warning("Blockchair ETH stats fetch failed: %s", exc)
            return {}

    def get_eth_snapshot(self) -> dict:
        """Return formatted ETH on-chain data dict."""
        result = {}
        stats = self.get_eth_stats()
        v = self._safe_int(stats.get("transactions_24h"))
        if v is not None:
            result["transactions_24h"] = v
        v = self._safe_int(stats.get("blocks_24h"))
        if v is not None:
            result["blocks_24h"] = v
        gwei_options = stats.get("suggested_transaction_fee_gwei_options")
        if isinstance(gwei_options, dict):
            v = self._safe_float(gwei_options.get("normal") or gwei_options.get("fast"))
            if v is not None:
                result["gas_gwei"] = v
        return result

    # ── SOL on-chain data ─────────────────────────────────────────────

    def get_sol_stats(self) -> dict:
        """Fetch Solana blockchain stats from Blockchair."""
        cached = self._get_cached("blockchair_sol")
        if cached is not None:
            return cached
        try:
            resp = self._session.get(
                "https://api.blockchair.com/solana/stats", timeout=15
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            if data:
                self._set_cached("blockchair_sol", data)
            return data
        except Exception as exc:
            logger.warning("Blockchair SOL stats fetch failed: %s", exc)
            return {}

    def get_sol_snapshot(self) -> dict:
        """Return formatted SOL on-chain data dict."""
        result = {}
        stats = self.get_sol_stats()
        v = self._safe_int(stats.get("transactions_24h"))
        if v is not None:
            result["transactions_24h"] = v
        v = self._safe_float(stats.get("tps"))
        if v is not None:
            result["tps"] = v
        return result

    # ── Multi-chain combined ──────────────────────────────────────────

    def get_multi_chain_snapshot(self) -> dict:
        """Return combined BTC + ETH + SOL on-chain snapshot."""
        return {
            "btc": self.get_btc_snapshot(),
            "eth": self.get_eth_snapshot(),
            "sol": self.get_sol_snapshot(),
        }

    def format_multi_summary(self, data: dict) -> str:
        """Format multi-chain on-chain data for AI context."""
        sections = []
        btc = data.get("btc", {})
        if btc:
            parts = []
            if "transactions_24h" in btc:
                parts.append(f"24h TXs: {btc['transactions_24h']:,}")
            if "hashrate" in btc:
                parts.append(f"Hashrate: {btc['hashrate'] / 1e18:.1f} EH/s")
            if "fastest_fee" in btc:
                parts.append(f"Fees: {btc['fastest_fee']} sat/vB")
            if parts:
                sections.append("BTC: " + " | ".join(parts))

        eth = data.get("eth", {})
        if eth:
            parts = []
            if "transactions_24h" in eth:
                parts.append(f"24h TXs: {eth['transactions_24h']:,}")
            if "gas_gwei" in eth:
                parts.append(f"Gas: {eth['gas_gwei']:.0f} gwei")
            if parts:
                sections.append("ETH: " + " | ".join(parts))

        sol = data.get("sol", {})
        if sol:
            parts = []
            if "transactions_24h" in sol:
                parts.append(f"24h TXs: {sol['transactions_24h']:,}")
            if "tps" in sol:
                parts.append(f"TPS: {sol['tps']:.0f}")
            if parts:
                sections.append("SOL: " + " | ".join(parts))

        return " | ".join(sections) if sections else "On-chain data unavailable"
