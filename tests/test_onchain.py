# tests/test_onchain.py
import pytest
from unittest.mock import patch, MagicMock
from bot.data.onchain import OnChainFetcher


@pytest.fixture
def fetcher():
    with patch("bot.data.onchain.create_session") as mock_cs:
        mock_cs.return_value = MagicMock()
        f = OnChainFetcher()
    return f


def test_get_blockchair_stats(fetcher):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"data": {"transactions_24h": 500000, "volume_24h": 30_000_000_000}}
    mock_resp.raise_for_status = MagicMock()
    fetcher._session.get.return_value = mock_resp
    result = fetcher.get_blockchair_stats()
    assert result["transactions_24h"] == 500000


def test_format_onchain_summary(fetcher):
    data = {
        "transactions_24h": 500000,
        "transaction_volume_btc": 250000,
        "fastest_fee": 42,
    }
    summary = fetcher.format_summary(data)
    assert "500,000" in summary
    assert "250,000 BTC" in summary
    assert "42" in summary


def test_get_blockchair_stats_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_blockchair_stats()
    assert result == {}


def test_get_btc_snapshot_returns_empty_when_no_data(fetcher):
    with patch.object(fetcher, "get_blockchair_stats", return_value={}):
        with patch.object(fetcher, "get_mempool_fees", return_value={}):
            snapshot = fetcher.get_btc_snapshot()
    assert snapshot == {}


def test_mempool_fees_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_mempool_fees()
    assert result == {}


def test_format_hashrate_in_eh_s(fetcher):
    data = {"hashrate": 750_000_000_000_000_000_000}  # 750 EH/s
    summary = fetcher.format_summary(data)
    assert "750.0 EH/s" in summary


def test_format_hashrate_small_value(fetcher):
    data = {"hashrate": 5e17}  # 0.5 EH/s
    summary = fetcher.format_summary(data)
    assert "0.5 EH/s" in summary


def test_get_btc_snapshot_includes_fees(fetcher):
    with patch.object(fetcher, "get_blockchair_stats", return_value={}):
        with patch.object(fetcher, "get_mempool_fees", return_value={"fastestFee": 15, "hourFee": 8}):
            snapshot = fetcher.get_btc_snapshot()
    assert snapshot["fastest_fee"] == 15
    assert snapshot["hour_fee"] == 8


def test_format_summary_returns_sentinel_on_empty_data(fetcher):
    assert fetcher.format_summary({}) == "On-chain data unavailable"


def test_get_btc_snapshot_partial_blockchair_only(fetcher):
    with patch.object(fetcher, "get_blockchair_stats",
                      return_value={"transactions_24h": 400000}):
        with patch.object(fetcher, "get_mempool_fees", return_value={}):
            snapshot = fetcher.get_btc_snapshot()
    assert snapshot["transactions_24h"] == 400000
    assert "fastest_fee" not in snapshot
