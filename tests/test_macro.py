# tests/test_macro.py
import pytest
from unittest.mock import patch, MagicMock
from bot.data.macro import MacroFetcher, DryRunMacroFetcher


@pytest.fixture
def fetcher():
    return MacroFetcher()


def test_format_summary():
    """format_summary should produce a readable one-liner."""
    data = {
        "dxy": {"price": 104.2, "change_pct": -0.3},
        "sp500": {"price": 5234, "change_pct": 0.5},
        "vix": {"price": 18.2, "change_pct": 2.1},
        "us10y": {"price": 4.35, "change_pct": -0.5},
    }
    result = MacroFetcher.format_summary(data)
    assert "DXY: 104.2" in result
    assert "S&P 500: 5,234" in result
    assert "VIX:" in result
    assert "10Y: 4.35%" in result
    assert "|" in result


def test_format_summary_partial_data():
    """Should handle partial data gracefully."""
    data = {"dxy": {"price": 104.2, "change_pct": -0.3}}
    result = MacroFetcher.format_summary(data)
    assert "DXY: 104.2" in result
    assert "S&P 500" not in result


def test_format_summary_empty():
    """Empty data should return empty string."""
    assert MacroFetcher.format_summary({}) == ""
    assert MacroFetcher.format_summary(None) == ""


def test_fetch_quote_api_failure(fetcher):
    """API failure should return empty dict, not raise."""
    mock_session = MagicMock()
    mock_session.get.side_effect = Exception("network error")
    fetcher._session = mock_session
    result = fetcher._fetch_quote("DX-Y.NYB")
    assert result == {}


def test_fetch_quote_caching(fetcher):
    """Second call within TTL should use cache, not hit API."""
    mock_session = MagicMock()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "chart": {"result": [{"meta": {
            "regularMarketPrice": 104.2,
            "chartPreviousClose": 104.5,
        }}]}
    }
    mock_session.get.return_value = mock_resp
    fetcher._session = mock_session

    fetcher._fetch_quote("DX-Y.NYB")
    fetcher._fetch_quote("DX-Y.NYB")
    assert mock_session.get.call_count == 1  # second call hits cache


def test_dry_run_macro_fetcher():
    """DryRunMacroFetcher should return complete fake data."""
    dry = DryRunMacroFetcher()
    data = dry.get_macro_snapshot()
    assert "dxy" in data
    assert "sp500" in data
    assert "vix" in data
    assert "us10y" in data
    summary = dry.format_summary(data)
    assert "DXY" in summary
    assert "S&P 500" in summary
