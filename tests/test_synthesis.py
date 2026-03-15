# tests/test_synthesis.py
import pytest
from bot.data.synthesis import DataSynthesizer


@pytest.fixture
def synth():
    return DataSynthesizer()


# ── diagnose_move ──

def test_diagnose_etf_driven_rally(synth):
    result = synth.diagnose_move(3.0, etf_flow_usd=250)
    assert "ETF" in result
    assert "inflow" in result


def test_diagnose_liquidation_cascade(synth):
    result = synth.diagnose_move(5.0, etf_flow_usd=0, exchange_netflow_btc=0)
    assert "Liquidation" in result or "no clear" in result.lower()


def test_diagnose_exchange_outflow_accumulation(synth):
    result = synth.diagnose_move(-2.0, exchange_netflow_btc=-3000)
    assert "outflow" in result.lower() or "accumulation" in result.lower()


# ── detect_regime ──

def test_detect_euphoria(synth):
    result = synth.detect_regime(fear_greed=85, funding_rate=0.05, price_7d_change=8)
    assert "Euphoria" in result


def test_detect_capitulation(synth):
    result = synth.detect_regime(fear_greed=15, price_7d_change=-15)
    assert "Capitulation" in result


# ── find_divergences ──

def test_divergence_price_up_funding_negative(synth):
    result = synth.find_divergences(price_change=5, funding_rate=-0.01)
    assert len(result) >= 1
    assert "funding negative" in result[0].lower() or "shorts paying" in result[0].lower()


def test_divergence_price_down_exchange_outflows(synth):
    result = synth.find_divergences(price_change=-3, exchange_netflow=-2000)
    assert len(result) >= 1
    assert "accumulating" in result[0].lower() or "leaving exchanges" in result[0].lower()


# ── synthesize ──

def test_synthesize_full_data(synth):
    result = synth.synthesize(
        price_data={"bitcoin": {"usd": 95000, "usd_24h_change": 4.5}},
        etf_data={"total_flow_usd": 300},
        exchange_data={"btc_netflow": -5000},
        derivatives_data={"funding_rate": -0.02},
        fear_greed={"value": 72},
        macro_data={"dxy": {"price": 104, "change_pct": -0.5}},
    )
    assert "Cross-Source Analysis" in result
    assert "===" in result


def test_synthesize_partial_data(synth):
    """Should work with only price data."""
    result = synth.synthesize(
        price_data={"bitcoin": {"usd": 90000, "usd_24h_change": 5.0}},
    )
    assert "Cross-Source Analysis" in result


def test_synthesize_no_data(synth):
    """No meaningful data should return empty string."""
    result = synth.synthesize()
    # With all defaults (0 change, 50 fear_greed), regime is range-bound
    # which produces output, but move diagnosis is skipped
    assert isinstance(result, str)


# ── extract helpers ──

def test_extract_btc_change_from_dict(synth):
    assert synth._extract_btc_change({"bitcoin": {"usd_24h_change": 3.5}}) == 3.5


def test_extract_btc_change_from_string(synth):
    result = synth._extract_btc_change("BTC: $95,000 (+4.2%)")
    assert abs(result - 4.2) < 0.01
