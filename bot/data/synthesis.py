# data/synthesis.py
"""Cross-source data synthesis: correlate price, flows, derivatives, macro."""
import logging
import re

logger = logging.getLogger(__name__)

_NUM_RE = re.compile(r'[-+]?\d[\d,]*\.?\d*')


class DataSynthesizer:
    """Pure logic layer — consumes fetcher outputs, produces analysis text."""

    # ── Private helpers to extract key numbers from formatted summaries ──

    @staticmethod
    def _extract_btc_change(price_data) -> float:
        """Extract BTC 24h change % from price data dict or formatted string."""
        if isinstance(price_data, dict):
            btc = price_data.get("bitcoin", {})
            return btc.get("usd_24h_change", 0) if isinstance(btc, dict) else 0
        if isinstance(price_data, str):
            m = re.search(r'BTC.*?([+-]?\d+\.?\d*)%', price_data)
            return float(m.group(1)) if m else 0
        return 0

    @staticmethod
    def _extract_etf_flow(etf_data) -> float:
        """Extract total ETF flow in USD millions from data dict or string."""
        if isinstance(etf_data, dict):
            return etf_data.get("total_flow_usd", 0)
        if isinstance(etf_data, str):
            m = re.search(r'([+-]?\$?[\d,]+\.?\d*)\s*[Mm]', etf_data)
            if m:
                val = m.group(1).replace('$', '').replace(',', '')
                try:
                    return float(val)
                except ValueError:
                    pass
        return 0

    @staticmethod
    def _extract_exchange_netflow(exchange_data) -> float:
        """Extract BTC exchange netflow from data dict or string."""
        if isinstance(exchange_data, dict):
            return exchange_data.get("btc_netflow", 0)
        if isinstance(exchange_data, str):
            m = re.search(r'([+-]?\d[\d,]*\.?\d*)\s*BTC', exchange_data)
            if m:
                val = m.group(1).replace(',', '')
                try:
                    return float(val)
                except ValueError:
                    pass
        return 0

    @staticmethod
    def _extract_funding(derivatives_data) -> float:
        """Extract BTC funding rate from data dict or string."""
        if isinstance(derivatives_data, dict):
            return derivatives_data.get("funding_rate", 0)
        if isinstance(derivatives_data, str):
            m = re.search(r'[Ff]unding.*?([+-]?\d+\.?\d+)%', derivatives_data)
            return float(m.group(1)) if m else 0
        return 0

    # ── Core analysis methods ──

    def diagnose_move(self, price_change_pct: float, etf_flow_usd: float = 0,
                      exchange_netflow_btc: float = 0, whale_txns: int = 0) -> str:
        """Diagnose what's driving a price move.

        Returns a one-line diagnosis like "ETF-driven rally" or "Liquidation cascade".
        """
        drivers = []

        if abs(etf_flow_usd) > 100:
            direction = "inflow" if etf_flow_usd > 0 else "outflow"
            drivers.append(f"ETF {direction} (${abs(etf_flow_usd):.0f}M)")

        if abs(exchange_netflow_btc) > 1000:
            if exchange_netflow_btc < 0:
                drivers.append("exchange outflows (accumulation)")
            else:
                drivers.append("exchange inflows (sell pressure)")

        if whale_txns > 5:
            drivers.append(f"whale activity ({whale_txns} large txns)")

        if not drivers:
            if abs(price_change_pct) > 3:
                return "Liquidation cascade likely — no clear fundamental driver"
            return "Mixed signals — no dominant driver"

        if price_change_pct > 0:
            return f"Rally driven by: {', '.join(drivers)}"
        else:
            return f"Selloff driven by: {', '.join(drivers)}"

    def detect_regime(self, fear_greed: int = 50, funding_rate: float = 0,
                      price_7d_change: float = 0, dxy_change: float = 0) -> str:
        """Detect current market regime.

        Returns regime label + one-line description.
        """
        # Euphoria
        if fear_greed > 80 and funding_rate > 0.03 and price_7d_change > 5:
            return "Euphoria — extreme greed + positive funding + strong rally. Distribution risk high."

        # Capitulation
        if fear_greed < 25 and price_7d_change < -10:
            return "Capitulation — extreme fear + sharp decline. Historical bottom signals."

        # Distribution
        if fear_greed > 60 and funding_rate > 0.01 and price_7d_change < -2:
            return "Distribution — greed but price falling. Smart money likely exiting."

        # Risk-on
        if fear_greed > 50 and price_7d_change > 2:
            regime = "Risk-on — positive momentum + sentiment"
            if dxy_change < -0.3:
                regime += " + dollar weakness (tailwind)"
            return regime

        # Dollar headwind/tailwind
        if abs(dxy_change) > 0.5:
            if dxy_change > 0:
                return f"Dollar headwind — DXY up {dxy_change:.1f}%, crypto faces pressure."
            return f"Dollar tailwind — DXY down {abs(dxy_change):.1f}%, risk assets benefit."

        return "Range-bound — no clear regime signal."

    def find_divergences(self, price_change: float = 0, funding_rate: float = 0,
                         exchange_netflow: float = 0, etf_flow: float = 0,
                         fear_greed: int = 50) -> list:
        """Find contradictory signals that suggest inflection points.

        Returns list of divergence descriptions.
        """
        divergences = []

        # Price up but funding negative = shorts paying longs during rally
        if price_change > 2 and funding_rate < -0.005:
            divergences.append(
                "Price rising but funding negative — shorts paying longs. "
                "Rally not driven by leverage. Bullish divergence."
            )

        # Price down but exchange outflows = accumulation during dip
        if price_change < -2 and exchange_netflow < -500:
            divergences.append(
                "Price dropping but coins leaving exchanges. "
                "Smart money accumulating the dip."
            )

        # Extreme greed but exchange outflows = conviction buying
        if fear_greed > 75 and exchange_netflow < -1000:
            divergences.append(
                "Extreme greed + exchange outflows. "
                "High conviction accumulation despite euphoria."
            )

        # ETF inflow but exchange inflow = institution vs retail divergence
        if etf_flow > 100 and exchange_netflow > 1000:
            divergences.append(
                "ETF inflows but exchange deposits rising. "
                "Institutional buying vs retail selling."
            )

        # Price up but ETF outflow
        if price_change > 3 and etf_flow < -50:
            divergences.append(
                "Rally despite ETF outflows. "
                "Crypto-native demand overpowering institutional exits."
            )

        return divergences

    def synthesize(self, price_data=None, etf_data=None, exchange_data=None,
                   derivatives_data=None, fear_greed=None, macro_data=None) -> str:
        """Main entry point: cross-source synthesis analysis.

        All parameters are optional (graceful degradation). Accepts both raw
        dicts and formatted summary strings.

        Returns formatted analysis block for prepending to AI context.
        """
        sections = []

        # Extract key numbers
        btc_change = self._extract_btc_change(price_data) if price_data else 0
        etf_flow = self._extract_etf_flow(etf_data) if etf_data else 0
        exch_netflow = self._extract_exchange_netflow(exchange_data) if exchange_data else 0
        funding = self._extract_funding(derivatives_data) if derivatives_data else 0
        fg_val = fear_greed if isinstance(fear_greed, (int, float)) else 50
        if isinstance(fear_greed, dict):
            fg_val = fear_greed.get("value", 50)
        dxy_change = 0
        if isinstance(macro_data, dict) and "dxy" in macro_data:
            dxy_change = macro_data["dxy"].get("change_pct", 0)

        # Move diagnosis (only if meaningful move)
        if abs(btc_change) > 1.5:
            diagnosis = self.diagnose_move(btc_change, etf_flow, exch_netflow)
            sections.append(f"Move diagnosis: {diagnosis}")

        # Regime detection
        regime = self.detect_regime(fg_val, funding, btc_change * 2, dxy_change)
        if regime:
            sections.append(f"Market regime: {regime}")

        # Divergences
        divergences = self.find_divergences(btc_change, funding, exch_netflow, etf_flow, fg_val)
        if divergences:
            sections.append("Divergences:\n" + "\n".join(f"- {d}" for d in divergences))

        if not sections:
            return ""

        return "=== Cross-Source Analysis ===\n" + "\n".join(sections) + "\n==="
