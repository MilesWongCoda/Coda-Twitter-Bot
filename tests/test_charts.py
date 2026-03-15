# tests/test_charts.py
import os
import pytest
from PIL import Image
from bot.data.charts import generate_market_card, generate_onchain_card, COLUMN_NAMES


SAMPLE_PRICES = {
    "bitcoin": {"usd": 95432, "usd_24h_change": -2.3},
    "ethereum": {"usd": 2680, "usd_24h_change": 1.5},
    "solana": {"usd": 145, "usd_24h_change": 4.2},
    "binancecoin": {"usd": 612, "usd_24h_change": -0.8},
    "ripple": {"usd": 0.52, "usd_24h_change": 3.1},
}

SAMPLE_FG = {"value": 25, "label": "Extreme Fear"}

SAMPLE_ONCHAIN = {
    "transactions_24h": 450000,
    "transaction_volume_btc": 250000,
    "hashrate": 520000000000000000000,
    "mempool_tx": 12000,
    "fastest_fee": 45,
    "hour_fee": 12,
}

SAMPLE_DERIV = {"avg_rate": 0.00025, "total_oi_usd": 18500000000}


# ── Market card ──────────────────────────────────────────────────────────────

def test_generate_market_card_returns_valid_png():
    path = generate_market_card(SAMPLE_PRICES, SAMPLE_FG, "morning_brief")
    assert path is not None
    assert os.path.exists(path)
    img = Image.open(path)
    assert img.size == (1200, 675)
    assert img.mode == "RGB"
    os.unlink(path)


def test_generate_market_card_evening():
    path = generate_market_card(SAMPLE_PRICES, SAMPLE_FG, "evening_wrap")
    assert path is not None
    os.unlink(path)


def test_generate_market_card_empty_prices():
    path = generate_market_card({}, SAMPLE_FG, "morning_brief")
    assert path is not None
    os.unlink(path)


def test_generate_market_card_empty_fg():
    path = generate_market_card(SAMPLE_PRICES, {}, "morning_brief")
    assert path is not None
    os.unlink(path)


def test_generate_market_card_none_fg():
    path = generate_market_card(SAMPLE_PRICES, None, "morning_brief")
    assert path is not None
    os.unlink(path)


def test_generate_market_card_partial_prices():
    prices = {"bitcoin": {"usd": 95000, "usd_24h_change": 1.0}}
    path = generate_market_card(prices, SAMPLE_FG, "morning_brief")
    assert path is not None
    os.unlink(path)


# ── Onchain card ─────────────────────────────────────────────────────────────

def test_generate_onchain_card_returns_valid_png():
    path = generate_onchain_card(SAMPLE_ONCHAIN, SAMPLE_DERIV, "onchain_signal")
    assert path is not None
    assert os.path.exists(path)
    img = Image.open(path)
    assert img.size == (1200, 675)
    os.unlink(path)


def test_generate_onchain_card_no_derivatives():
    path = generate_onchain_card(SAMPLE_ONCHAIN, None, "onchain_signal")
    assert path is not None
    os.unlink(path)


def test_generate_onchain_card_empty_data():
    path = generate_onchain_card({}, None, "onchain_signal")
    assert path is not None
    os.unlink(path)


# ── Candlestick chart ───────────────────────────────────────────────────────

def test_generate_candlestick_chart_returns_valid_png():
    from bot.data.charts import generate_candlestick_chart
    # Simulate 30 OHLC data points
    import time
    base_ts = int(time.time() * 1000)
    ohlc_data = []
    for i in range(30):
        ts = base_ts + i * 3600000
        o = 95000 + i * 100
        h = o + 500
        l = o - 300
        c = o + 200
        ohlc_data.append([ts, o, h, l, c])
    path = generate_candlestick_chart(ohlc_data, "bitcoin", 7)
    assert path is not None
    assert os.path.exists(path)
    img = Image.open(path)
    assert img.mode in ("RGB", "RGBA")
    os.unlink(path)


def test_generate_candlestick_chart_returns_none_on_empty_data():
    from bot.data.charts import generate_candlestick_chart
    assert generate_candlestick_chart([], "bitcoin", 7) is None
    assert generate_candlestick_chart(None, "bitcoin", 7) is None


def test_generate_candlestick_chart_returns_none_on_insufficient_data():
    from bot.data.charts import generate_candlestick_chart
    result = generate_candlestick_chart([[1, 2, 3, 4, 5], [2, 3, 4, 5, 6]], "bitcoin", 7)
    assert result is None


# ── Weekly scorecard ────────────────────────────────────────────────────────

def test_generate_weekly_scorecard_returns_valid_png():
    from bot.data.charts import generate_weekly_scorecard
    weekly_data = [
        {"symbol": "btc", "current_price": 95000, "price_change_percentage_7d_in_currency": 5.2},
        {"symbol": "eth", "current_price": 2700, "price_change_percentage_7d_in_currency": -1.3},
        {"symbol": "sol", "current_price": 145, "price_change_percentage_7d_in_currency": 8.1},
        {"symbol": "bnb", "current_price": 612, "price_change_percentage_7d_in_currency": 0.5},
        {"symbol": "xrp", "current_price": 0.52, "price_change_percentage_7d_in_currency": -3.2},
    ]
    path = generate_weekly_scorecard(weekly_data)
    assert path is not None
    assert os.path.exists(path)
    img = Image.open(path)
    assert img.size == (1200, 675)
    os.unlink(path)


def test_generate_weekly_scorecard_returns_none_on_empty():
    from bot.data.charts import generate_weekly_scorecard
    assert generate_weekly_scorecard([]) is None
    assert generate_weekly_scorecard(None) is None


# ── Column names ─────────────────────────────────────────────────────────────

def test_column_names_all_defined():
    expected = {"morning_brief", "hot_take", "onchain_signal", "us_open", "evening_wrap", "alpha_thread"}
    assert expected == set(COLUMN_NAMES.keys())
