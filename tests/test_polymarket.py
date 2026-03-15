# tests/test_polymarket.py
import pytest
from unittest.mock import MagicMock, patch
from bot.data.polymarket import PolymarketFetcher, DryRunPolymarketFetcher


def _make_market(question, tags=None, outcome_prices='["0.45", "0.55"]',
                 volume24hr=1000000):
    return {
        "question": question,
        "tags": tags or [],
        "outcomePrices": outcome_prices,
        "volume24hr": volume24hr,
        "endDate": "2026-04-01",
    }


@pytest.fixture
def fetcher():
    with patch("bot.data.polymarket.create_session") as mock_session:
        f = PolymarketFetcher()
        f._session = mock_session.return_value
        return f


# ── filtering ────────────────────────────────────────────────────────────────

def test_filter_keeps_macro_fed_keyword(fetcher):
    markets = [_make_market("Will the Fed cut rates in March?")]
    result = fetcher._filter_relevant(markets)
    assert len(result) == 1
    assert "Fed" in result[0]["question"]


def test_filter_keeps_macro_keyword(fetcher):
    markets = [_make_market("Will the Fed cut interest rates?")]
    result = fetcher._filter_relevant(markets)
    assert len(result) == 1


def test_filter_keeps_geopolitics_keyword(fetcher):
    markets = [_make_market("Will Trump impose new tariffs on China?")]
    result = fetcher._filter_relevant(markets)
    assert len(result) == 1


def test_filter_removes_irrelevant(fetcher):
    markets = [_make_market("Will the Pacers win the NBA Finals?")]
    result = fetcher._filter_relevant(markets)
    assert len(result) == 0


def test_filter_keeps_tag_match(fetcher):
    markets = [_make_market("Some question", tags=["geopolitics"])]
    result = fetcher._filter_relevant(markets)
    assert len(result) == 1


def test_filter_removes_extreme_probability_low(fetcher):
    """Markets with < 5% YES probability are boring, should be filtered."""
    markets = [_make_market("Will BTC hit $1M tomorrow?",
                            outcome_prices='["0.01", "0.99"]')]
    result = fetcher._filter_relevant(markets)
    assert len(result) == 0


def test_filter_removes_extreme_probability_high(fetcher):
    """Markets with > 95% YES probability are boring, should be filtered."""
    markets = [_make_market("Will Bitcoin exist in 2027?",
                            outcome_prices='["0.99", "0.01"]')]
    result = fetcher._filter_relevant(markets)
    assert len(result) == 0


def test_filter_keeps_interesting_probability(fetcher):
    """Markets between 5%-95% are interesting."""
    markets = [_make_market("Will Iran agree to a ceasefire?",
                            outcome_prices='["0.35", "0.65"]')]
    result = fetcher._filter_relevant(markets)
    assert len(result) == 1
    assert result[0]["yes_prob"] == pytest.approx(0.35)


# ── normalize ────────────────────────────────────────────────────────────────

def test_normalize_parses_json_prices(fetcher):
    m = _make_market("Q?", outcome_prices='["0.72", "0.28"]')
    result = fetcher._normalize(m)
    assert result["yes_prob"] == pytest.approx(0.72)


def test_normalize_handles_bad_prices(fetcher):
    m = _make_market("Q?", outcome_prices="not json")
    result = fetcher._normalize(m)
    assert result["yes_prob"] is None


def test_normalize_extracts_volume(fetcher):
    m = _make_market("Q?", volume24hr=2500000)
    result = fetcher._normalize(m)
    assert result["volume_24h"] == 2500000


# ── get_polymarket_snapshot ──────────────────────────────────────────────────

def test_snapshot_returns_empty_on_no_markets(fetcher):
    fetcher._session.get.return_value = MagicMock(
        status_code=200, json=lambda: [], raise_for_status=lambda: None)
    result = fetcher.get_polymarket_snapshot()
    assert result == {}


def test_snapshot_limits_to_8(fetcher):
    markets = [_make_market(f"Will tariff event {i} happen?") for i in range(15)]
    resp = MagicMock()
    resp.json.return_value = markets
    resp.raise_for_status = MagicMock()
    fetcher._session.get.return_value = resp
    result = fetcher.get_polymarket_snapshot()
    assert len(result["markets"]) <= 8


def test_snapshot_returns_empty_on_api_error(fetcher):
    fetcher._session.get.side_effect = Exception("API down")
    result = fetcher.get_polymarket_snapshot()
    assert result == {}


# ── format_summary ───────────────────────────────────────────────────────────

def test_format_summary_with_data(fetcher):
    data = {"markets": [
        {"question": "Will BTC hit $100K?", "yes_prob": 0.65, "volume_24h": 2500000},
    ]}
    summary = fetcher.format_summary(data)
    assert "Polymarket:" in summary
    assert "65%" in summary
    assert "$2.5M" in summary


def test_format_summary_empty(fetcher):
    assert fetcher.format_summary({}) == ""
    assert fetcher.format_summary(None) == ""


# ── caching ──────────────────────────────────────────────────────────────────

def test_caching_avoids_second_call(fetcher):
    markets = [_make_market("Will the Fed cut rates in March?")]
    resp = MagicMock()
    resp.json.return_value = markets
    resp.raise_for_status = MagicMock()
    fetcher._session.get.return_value = resp
    fetcher.get_trending_markets()
    fetcher.get_trending_markets()
    assert fetcher._session.get.call_count == 1


# ── DryRunPolymarketFetcher ─────────────────────────────────────────────────

def test_dry_run_returns_data():
    f = DryRunPolymarketFetcher()
    data = f.get_polymarket_snapshot()
    assert "markets" in data
    assert len(data["markets"]) >= 2


def test_dry_run_format_summary():
    f = DryRunPolymarketFetcher()
    data = f.get_polymarket_snapshot()
    summary = f.format_summary(data)
    assert "Polymarket:" in summary
