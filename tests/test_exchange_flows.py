# tests/test_exchange_flows.py
import pytest
from unittest.mock import patch, MagicMock
from bot.data.exchange_flows import ExchangeFlowFetcher, DryRunExchangeFlowFetcher


@pytest.fixture
def fetcher():
    with patch("bot.data.exchange_flows.create_session") as mock_cs:
        mock_cs.return_value = MagicMock()
        f = ExchangeFlowFetcher(api_key="test_key")
    return f


def _mock_response(data, code="0"):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"code": code, "data": data}
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def test_get_exchange_balance_success(fetcher):
    balance = [
        {"exchangeName": "Binance", "balance": 500000, "change24h": -1200},
        {"exchangeName": "Coinbase", "balance": 400000, "change24h": -800},
    ]
    fetcher._session.get.return_value = _mock_response(balance)
    result = fetcher.get_exchange_balance()
    assert isinstance(result, list)
    assert len(result) == 2


def test_get_exchange_balance_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_exchange_balance()
    assert result == {}


def test_get_exchange_balance_uses_cache(fetcher):
    balance = [{"exchangeName": "Binance", "balance": 500000}]
    fetcher._session.get.return_value = _mock_response(balance)
    fetcher.get_exchange_balance()
    fetcher.get_exchange_balance()
    assert fetcher._session.get.call_count == 1


def test_get_exchange_balance_serves_stale(fetcher):
    balance = [{"exchangeName": "Binance", "balance": 500000}]
    fetcher._session.get.return_value = _mock_response(balance)
    fetcher.get_exchange_balance()
    fetcher._cache_time -= 1200  # > TTL (900) but < MAX_STALENESS (3600)
    fetcher._session.get.side_effect = Exception("fail")
    result = fetcher.get_exchange_balance()
    assert isinstance(result, list)


def test_get_exchange_snapshot(fetcher):
    balance = [{"exchangeName": "Binance", "balance": 500000}]
    fetcher._session.get.return_value = _mock_response(balance)
    result = fetcher.get_exchange_snapshot()
    assert "balance" in result


def test_get_exchange_snapshot_empty(fetcher):
    fetcher._session.get.side_effect = Exception("fail")
    result = fetcher.get_exchange_snapshot()
    assert result == {}


def test_format_summary_list_outflow(fetcher):
    data = {"balance": [
        {"exchangeName": "Binance", "balance": 500000, "change24h": -3000},
        {"exchangeName": "Coinbase", "balance": 400000, "change24h": -2000},
    ]}
    summary = fetcher.format_summary(data)
    assert "900,000 BTC" in summary
    assert "outflow" in summary
    assert "bullish" in summary


def test_format_summary_list_inflow(fetcher):
    data = {"balance": [
        {"exchangeName": "Binance", "balance": 500000, "change24h": 5000},
    ]}
    summary = fetcher.format_summary(data)
    assert "inflow" in summary
    assert "bearish" in summary


def test_format_summary_dict(fetcher):
    data = {"balance": {"totalBalance": 2_100_000, "change24h": -5200}}
    summary = fetcher.format_summary(data)
    assert "2,100,000 BTC" in summary
    assert "outflow" in summary


def test_format_summary_empty(fetcher):
    assert fetcher.format_summary({}) == ""
    assert fetcher.format_summary({"balance": None}) == ""
    assert fetcher.format_summary({"balance": {}}) == ""


def test_dry_run_returns_data():
    dry = DryRunExchangeFlowFetcher()
    bal = dry.get_exchange_balance()
    assert bal
    snap = dry.get_exchange_snapshot()
    assert "balance" in snap
    summary = dry.format_summary(snap)
    assert "BTC" in summary
    assert dry.format_summary({}) == ""
