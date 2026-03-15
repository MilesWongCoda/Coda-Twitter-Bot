# tests/test_derivatives.py
import pytest
from unittest.mock import patch, MagicMock
from bot.data.derivatives import DerivativesFetcher


@pytest.fixture
def fetcher():
    with patch("bot.data.derivatives.create_session") as mock_cs:
        mock_cs.return_value = MagicMock()
        f = DerivativesFetcher(api_key="test_key")
    return f


def test_get_btc_funding_rate(fetcher):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "code": "0",
        "data": [
            {"rate": "0.0001"},
            {"rate": "0.0003"},
            {"rate": None},          # should be skipped
        ],
    }
    mock_resp.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_resp
    result = fetcher.get_btc_funding_rate()
    assert "avg_rate" in result
    assert abs(result["avg_rate"] - 0.0002) < 1e-9


def test_get_btc_funding_rate_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_btc_funding_rate()
    assert result == {}


def test_get_btc_open_interest(fetcher):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "code": "0",
        "data": [
            {"openInterestUsd": "10000000000"},
            {"openInterestUsd": "8000000000"},
        ],
    }
    mock_resp.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_resp
    result = fetcher.get_btc_open_interest()
    assert result["total_oi_usd"] == pytest.approx(18_000_000_000)


def test_get_btc_open_interest_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_btc_open_interest()
    assert result == {}


def test_format_summary_positive_funding(fetcher):
    data = {"avg_rate": 0.0006, "total_oi_usd": 18_500_000_000}
    summary = fetcher.format_summary(data)
    assert "+0.060%" in summary
    assert "longs paying premium" in summary
    assert "$18.5B" in summary


def test_format_summary_negative_funding(fetcher):
    data = {"avg_rate": -0.0002, "total_oi_usd": 15_000_000_000}
    summary = fetcher.format_summary(data)
    assert "-0.020%" in summary
    assert "shorts paying premium" in summary


def test_format_summary_neutral_funding(fetcher):
    data = {"avg_rate": 0.0001}
    summary = fetcher.format_summary(data)
    assert "neutral" in summary


def test_format_summary_empty(fetcher):
    assert fetcher.format_summary({}) == ""


def test_get_btc_snapshot_combines_data(fetcher):
    with patch.object(fetcher, "get_btc_funding_rate", return_value={"avg_rate": 0.0003}):
        with patch.object(fetcher, "get_btc_open_interest", return_value={"total_oi_usd": 20e9}):
            result = fetcher.get_btc_snapshot()
    assert result["avg_rate"] == 0.0003
    assert result["total_oi_usd"] == 20e9
