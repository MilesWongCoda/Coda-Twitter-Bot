# tests/test_etf_flows.py
import pytest
from unittest.mock import patch, MagicMock
from bot.data.etf_flows import ETFFlowFetcher, DryRunETFFlowFetcher


@pytest.fixture
def fetcher():
    with patch("bot.data.etf_flows.create_session") as mock_cs:
        mock_cs.return_value = MagicMock()
        f = ETFFlowFetcher(api_key="test_key")
    return f


def _mock_response(data, code="0"):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"code": code, "data": data}
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def test_get_btc_etf_flows_success(fetcher):
    flows = [
        {"date": "2025-02-20", "totalFlow": 100_000_000},
        {"date": "2025-02-21", "totalFlow": 150_000_000},
    ]
    fetcher._session.get.return_value = _mock_response(flows)
    result = fetcher.get_btc_etf_flows()
    assert len(result) == 2
    assert result[-1]["totalFlow"] == 150_000_000


def test_get_btc_etf_flows_returns_empty_on_error(fetcher):
    fetcher._session.get.side_effect = Exception("timeout")
    result = fetcher.get_btc_etf_flows()
    assert result == []


def test_get_btc_etf_flows_returns_empty_on_bad_code(fetcher):
    fetcher._session.get.return_value = _mock_response([], code="1")
    result = fetcher.get_btc_etf_flows()
    assert result == []


def test_get_btc_etf_flows_uses_cache(fetcher):
    flows = [{"date": "2025-02-21", "totalFlow": 150_000_000}]
    fetcher._session.get.return_value = _mock_response(flows)
    fetcher.get_btc_etf_flows()
    fetcher.get_btc_etf_flows()  # should hit cache
    assert fetcher._session.get.call_count == 1


def test_get_btc_etf_flows_serves_stale_on_failure(fetcher):
    flows = [{"date": "2025-02-21", "totalFlow": 150_000_000}]
    fetcher._session.get.return_value = _mock_response(flows)
    fetcher.get_btc_etf_flows()  # populate cache
    # Expire TTL but keep within staleness
    fetcher._cache_time -= 4000  # > TTL (3600) but < MAX_STALENESS (7200)
    fetcher._session.get.side_effect = Exception("fail")
    result = fetcher.get_btc_etf_flows()
    assert len(result) == 1


def test_get_etf_snapshot(fetcher):
    flows = [
        {"date": "2025-02-20", "totalFlow": 100_000_000},
        {"date": "2025-02-21", "totalFlow": 150_000_000},
    ]
    fetcher._session.get.return_value = _mock_response(flows)
    result = fetcher.get_etf_snapshot()
    assert result["totalFlow"] == 150_000_000


def test_get_etf_snapshot_empty(fetcher):
    assert fetcher.get_etf_snapshot() == {}


def test_format_summary_positive_flow(fetcher):
    data = {"totalFlow": 150_000_000}
    summary = fetcher.format_summary(data)
    assert "+$150.0M" in summary
    assert "BTC ETF net flow" in summary


def test_format_summary_negative_flow(fetcher):
    data = {"totalFlow": -80_500_000}
    summary = fetcher.format_summary(data)
    assert "-$80.5M" in summary


def test_format_summary_large_flow(fetcher):
    data = {"totalFlow": 1_200_000_000}
    summary = fetcher.format_summary(data)
    assert "+$1.20B" in summary


def test_format_summary_empty(fetcher):
    assert fetcher.format_summary({}) == ""


def test_format_summary_none_value(fetcher):
    assert fetcher.format_summary({"totalFlow": None}) == ""


def test_dry_run_returns_data():
    dry = DryRunETFFlowFetcher()
    flows = dry.get_btc_etf_flows()
    assert len(flows) >= 1
    snapshot = dry.get_etf_snapshot()
    assert snapshot
    summary = dry.format_summary(snapshot)
    assert "ETF" in summary
    assert dry.format_summary({}) == ""
