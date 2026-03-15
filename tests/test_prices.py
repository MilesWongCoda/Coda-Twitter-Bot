# tests/test_prices.py
import pytest
from unittest.mock import patch, MagicMock
from bot.data.prices import PriceFetcher


@pytest.fixture
def fetcher():
    with patch("bot.data.prices.create_session") as mock_cs:
        mock_cs.return_value = MagicMock()
        f = PriceFetcher(coingecko_api_key="test_key")
    return f


def test_get_crypto_prices(fetcher):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "bitcoin": {"usd": 95000, "usd_24h_change": 2.5},
        "ethereum": {"usd": 3200, "usd_24h_change": -1.2},
    }
    mock_response.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_response
    prices = fetcher.get_crypto_prices(["bitcoin", "ethereum"])
    assert prices["bitcoin"]["usd"] == 95000
    assert prices["ethereum"]["usd_24h_change"] == -1.2


def test_format_crypto_summary(fetcher):
    prices = {
        "bitcoin": {"usd": 95000, "usd_24h_change": 2.5},
        "ethereum": {"usd": 3200, "usd_24h_change": -1.2},
    }
    summary = fetcher.format_crypto_summary(prices)
    assert "BTC" in summary
    assert "95,000" in summary
    assert "+2.5%" in summary


def test_get_fear_greed(fetcher):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": [{"value": "75", "value_classification": "Greed"}]
    }
    mock_response.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_response
    result = fetcher.get_fear_greed()
    assert result["value"] == 75
    assert result["label"] == "Greed"


def test_fear_greed_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_fear_greed()
    assert result == {}


def test_format_fear_greed(fetcher):
    data = {"value": 82, "label": "Extreme Greed"}
    result = fetcher.format_fear_greed(data)
    assert "82" in result
    assert "Extreme Greed" in result


def test_format_fear_greed_empty(fetcher):
    assert fetcher.format_fear_greed({}) == ""


def test_format_fear_greed_missing_keys(fetcher):
    assert fetcher.format_fear_greed({"value": 50}) == ""
    assert fetcher.format_fear_greed({"label": "Neutral"}) == ""


def test_get_crypto_prices_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_crypto_prices()
    assert result == {}


def test_format_crypto_summary_handles_missing_usd(fetcher):
    prices = {"bitcoin": {"usd_24h_change": 2.5}}
    summary = fetcher.format_crypto_summary(prices)
    assert "BTC: N/A" in summary


def test_format_crypto_summary_handles_empty_dict(fetcher):
    summary = fetcher.format_crypto_summary({})
    assert summary == ""


# ── V2: OHLC, 1h changes, weekly changes ────────────────────────────────────

def test_get_ohlc(fetcher):
    mock_response = MagicMock()
    mock_response.json.return_value = [
        [1700000000000, 95000, 96000, 94500, 95500],
        [1700003600000, 95500, 96500, 95000, 96000],
        [1700007200000, 96000, 97000, 95500, 96500],
    ]
    mock_response.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_response
    result = fetcher.get_ohlc("bitcoin", days=7)
    assert result is not None
    assert len(result) == 3


def test_get_ohlc_returns_none_on_insufficient_data(fetcher):
    mock_response = MagicMock()
    mock_response.json.return_value = [[1700000000000, 95000, 96000, 94500, 95500]]
    mock_response.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_response
    result = fetcher.get_ohlc("bitcoin", days=7)
    assert result is None


def test_get_ohlc_returns_none_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_ohlc("bitcoin", days=7)
    assert result is None


def test_get_prices_with_1h_change(fetcher):
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 95000,
         "price_change_percentage_1h_in_currency": 2.5,
         "price_change_percentage_24h_in_currency": -1.0,
         "total_volume": 500000000},
    ]
    mock_response.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_response
    result = fetcher.get_prices_with_1h_change()
    assert len(result) == 1
    assert result[0]["id"] == "bitcoin"


def test_get_prices_with_1h_change_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_prices_with_1h_change()
    assert result == []


def test_get_weekly_changes(fetcher):
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 95000,
         "price_change_percentage_7d_in_currency": 5.2},
    ]
    mock_response.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_response
    result = fetcher.get_weekly_changes()
    assert len(result) == 1
    assert result[0]["price_change_percentage_7d_in_currency"] == 5.2


def test_get_weekly_changes_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_weekly_changes()
    assert result == []
