# tests/test_whale_alerts.py
import pytest
from unittest.mock import patch, MagicMock
from bot.data.whale_alerts import WhaleAlertFetcher, DryRunWhaleAlertFetcher


@pytest.fixture
def fetcher():
    with patch("bot.data.whale_alerts.create_session") as mock_cs:
        mock_cs.return_value = MagicMock()
        f = WhaleAlertFetcher(api_key="test_key")
    return f


SAMPLE_TXS = [
    {
        "blockchain": "bitcoin",
        "symbol": "btc",
        "amount": 500,
        "amount_usd": 47_500_000,
        "from": {"owner": "unknown", "owner_type": "unknown"},
        "to": {"owner": "Coinbase", "owner_type": "exchange"},
    },
    {
        "blockchain": "bitcoin",
        "symbol": "btc",
        "amount": 300,
        "amount_usd": 28_500_000,
        "from": {"owner": "Binance", "owner_type": "exchange"},
        "to": {"owner": "unknown", "owner_type": "unknown"},
    },
    {
        "blockchain": "ethereum",
        "symbol": "eth",
        "amount": 10000,
        "amount_usd": 32_000_000,
        "from": {"owner": "unknown", "owner_type": "unknown"},
        "to": {"owner": "unknown", "owner_type": "unknown"},
    },
]


def _mock_response(txs):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"result": "success", "transactions": txs}
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def test_get_recent_transactions_success(fetcher):
    fetcher._session.get.return_value = _mock_response(SAMPLE_TXS)
    result = fetcher.get_recent_transactions()
    assert len(result) == 3


def test_get_recent_transactions_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_recent_transactions()
    assert result == []


def test_get_recent_transactions_returns_empty_on_failure_result(fetcher):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"result": "error", "message": "bad key"}
    mock_resp.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_resp
    result = fetcher.get_recent_transactions()
    assert result == []


def test_get_recent_transactions_uses_cache(fetcher):
    fetcher._session.get.return_value = _mock_response(SAMPLE_TXS)
    fetcher.get_recent_transactions()
    fetcher.get_recent_transactions()
    assert fetcher._session.get.call_count == 1


def test_get_recent_transactions_serves_stale(fetcher):
    fetcher._session.get.return_value = _mock_response(SAMPLE_TXS)
    fetcher.get_recent_transactions()
    fetcher._cache_time -= 400  # > TTL (300) but < MAX_STALENESS (1800)
    fetcher._session.get.side_effect = Exception("fail")
    result = fetcher.get_recent_transactions()
    assert len(result) == 3


def test_get_whale_snapshot_aggregates(fetcher):
    fetcher._session.get.return_value = _mock_response(SAMPLE_TXS)
    result = fetcher.get_whale_snapshot()
    assert result["count"] == 3
    assert result["total_usd"] == pytest.approx(108_000_000)
    # TX 1: unknown → exchange (inflow $47.5M)
    assert result["exchange_inflows_usd"] == pytest.approx(47_500_000)
    # TX 2: exchange → unknown (outflow $28.5M)
    assert result["exchange_outflows_usd"] == pytest.approx(28_500_000)


def test_get_whale_snapshot_empty(fetcher):
    fetcher._session.get.side_effect = Exception("fail")
    result = fetcher.get_whale_snapshot()
    assert result == {}


def test_format_summary_with_data(fetcher):
    data = {
        "count": 3,
        "total_usd": 108_000_000,
        "exchange_inflows_usd": 47_500_000,
        "exchange_outflows_usd": 28_500_000,
    }
    summary = fetcher.format_summary(data)
    assert "3 txs" in summary
    assert "$108M" in summary
    assert "to exchanges" in summary
    assert "bearish" in summary


def test_format_summary_outflow_dominant(fetcher):
    data = {
        "count": 2,
        "total_usd": 80_000_000,
        "exchange_inflows_usd": 10_000_000,
        "exchange_outflows_usd": 70_000_000,
    }
    summary = fetcher.format_summary(data)
    assert "from exchanges" in summary
    assert "bullish" in summary


def test_format_summary_empty(fetcher):
    assert fetcher.format_summary({}) == ""
    assert fetcher.format_summary({"count": 0, "total_usd": 0}) == ""


def test_dry_run_returns_data():
    dry = DryRunWhaleAlertFetcher()
    txs = dry.get_recent_transactions()
    assert len(txs) >= 1
    snap = dry.get_whale_snapshot()
    assert snap["count"] == 3
    summary = dry.format_summary(snap)
    assert "Whale" in summary
    assert dry.format_summary({}) == ""
