# tests/test_trends.py
import time
import pytest
from unittest.mock import MagicMock, patch
from bot.data.trends import (
    TrendsFetcher, DryRunTrendsFetcher, _is_relevant,
    _CACHE_TTL, _MAX_STALENESS, CRYPTO_TERMS, MACRO_TERMS,
)


# ── _is_relevant filter ─────────────────────────────────────────────────────

def test_is_relevant_matches_crypto_hashtag():
    assert _is_relevant("#Bitcoin") is True


def test_is_relevant_matches_crypto_plain():
    assert _is_relevant("Ethereum") is True


def test_is_relevant_matches_macro_keyword():
    assert _is_relevant("Fed rate") is True


def test_is_relevant_matches_macro_hashtag():
    assert _is_relevant("#FOMC") is True


def test_is_relevant_matches_defi():
    assert _is_relevant("#DeFi") is True


def test_is_relevant_ignores_irrelevant():
    assert _is_relevant("#Oscars2026") is False
    assert _is_relevant("Taylor Swift") is False
    assert _is_relevant("#SuperBowl") is False


def test_is_relevant_case_insensitive():
    assert _is_relevant("#BITCOIN") is True
    assert _is_relevant("#bitcoin") is True
    assert _is_relevant("INFLATION") is True


# ── TrendsFetcher.fetch_all ──────────────────────────────────────────────────

@pytest.fixture
def fetcher():
    with patch("bot.data.trends.tweepy.OAuth1UserHandler"):
        with patch("bot.data.trends.tweepy.API") as mock_api_cls:
            f = TrendsFetcher("key", "secret", "token", "token_secret")
            f._api = mock_api_cls.return_value
            return f


def _make_raw_trends(items):
    """Helper: build raw Twitter v1.1 trends response."""
    return [{"trends": [
        {"name": name, "tweet_volume": vol, "url": f"https://twitter.com/search?q={name}"}
        for name, vol in items
    ]}]


def test_fetch_all_filters_and_sorts(fetcher):
    fetcher._api.get_place_trends.return_value = _make_raw_trends([
        ("#Bitcoin", 150000),
        ("#Oscars2026", 300000),  # irrelevant — filtered out
        ("Fed rate", 80000),
        ("#CryptoMarket", None),  # no volume → goes last
    ])
    result = fetcher.fetch_all()
    assert len(result) == 3
    assert result[0]["name"] == "#Bitcoin"
    assert result[0]["volume"] == 150000
    assert result[1]["name"] == "Fed rate"
    assert result[2]["name"] == "#CryptoMarket"
    assert result[2]["volume"] == 0  # None → 0


def test_fetch_all_caches_results(fetcher):
    fetcher._api.get_place_trends.return_value = _make_raw_trends([
        ("#Bitcoin", 150000),
    ])
    r1 = fetcher.fetch_all()
    r2 = fetcher.fetch_all()  # should come from cache
    assert r1 == r2
    assert fetcher._api.get_place_trends.call_count == 1


def test_fetch_all_refreshes_after_ttl(fetcher):
    fetcher._api.get_place_trends.return_value = _make_raw_trends([
        ("#Bitcoin", 150000),
    ])
    fetcher.fetch_all()
    # Simulate cache expiry (both cache time and last-fetch time)
    fetcher._cache_time -= _CACHE_TTL + 1
    fetcher._last_fetch_time -= _CACHE_TTL + 1
    fetcher._api.get_place_trends.return_value = _make_raw_trends([
        ("#Ethereum", 90000),
    ])
    result = fetcher.fetch_all()
    assert result[0]["name"] == "#Ethereum"
    assert fetcher._api.get_place_trends.call_count == 2


def test_fetch_all_serves_stale_on_failure(fetcher):
    # First call succeeds
    fetcher._api.get_place_trends.return_value = _make_raw_trends([
        ("#Bitcoin", 150000),
    ])
    fetcher.fetch_all()
    # Expire cache (both timestamps)
    fetcher._cache_time -= _CACHE_TTL + 1
    fetcher._last_fetch_time -= _CACHE_TTL + 1
    # Second call fails
    fetcher._api.get_place_trends.side_effect = Exception("API down")
    result = fetcher.fetch_all()
    assert len(result) == 1
    assert result[0]["name"] == "#Bitcoin"


def test_fetch_all_returns_empty_when_stale_beyond_max(fetcher):
    fetcher._api.get_place_trends.return_value = _make_raw_trends([
        ("#Bitcoin", 150000),
    ])
    fetcher.fetch_all()
    # Expire well beyond max staleness (both timestamps)
    fetcher._cache_time -= _MAX_STALENESS + 1
    fetcher._last_fetch_time -= _MAX_STALENESS + 1
    fetcher._api.get_place_trends.side_effect = Exception("API down")
    result = fetcher.fetch_all()
    assert result == []


def test_fetch_all_returns_empty_on_first_call_failure(fetcher):
    fetcher._api.get_place_trends.side_effect = Exception("API down")
    result = fetcher.fetch_all()
    assert result == []


def test_fetch_all_handles_empty_api_response(fetcher):
    fetcher._api.get_place_trends.return_value = [{"trends": []}]
    result = fetcher.fetch_all()
    assert result == []


# ── format_summary ───────────────────────────────────────────────────────────

def test_format_summary_with_volume(fetcher):
    trends = [
        {"name": "#Bitcoin", "volume": 150000},
        {"name": "Fed rate", "volume": 80000},
    ]
    summary = fetcher.format_summary(trends)
    assert "Currently trending on Twitter:" in summary
    assert "#Bitcoin (150K tweets)" in summary
    assert "Fed rate (80K tweets)" in summary


def test_format_summary_no_volume(fetcher):
    trends = [{"name": "#Crypto", "volume": 0}]
    summary = fetcher.format_summary(trends)
    assert "#Crypto" in summary
    assert "tweets" not in summary


def test_format_summary_empty(fetcher):
    assert fetcher.format_summary([]) == ""


def test_format_summary_limits_to_5(fetcher):
    trends = [{"name": f"#{i}", "volume": i * 1000} for i in range(10)]
    summary = fetcher.format_summary(trends)
    assert summary.count("|") == 4  # 5 items → 4 separators


# ── get_trending_hashtags ────────────────────────────────────────────────────

def test_get_trending_hashtags_extracts_hash_tags(fetcher):
    trends = [
        {"name": "#Bitcoin", "volume": 150000},
        {"name": "Fed rate", "volume": 80000},  # no # → excluded
        {"name": "#CryptoMarket", "volume": 50000},
        {"name": "#ETH", "volume": 30000},
        {"name": "#Solana", "volume": 10000},  # 4th → over limit of 3
    ]
    tags = fetcher.get_trending_hashtags(trends)
    assert tags == ["#Bitcoin", "#CryptoMarket", "#ETH"]


def test_get_trending_hashtags_empty():
    fetcher = DryRunTrendsFetcher()
    tags = fetcher.get_trending_hashtags([])
    assert tags == []


# ── DryRunTrendsFetcher ──────────────────────────────────────────────────────

def test_dry_run_returns_fake_data():
    fetcher = DryRunTrendsFetcher()
    data = fetcher.fetch_all()
    assert len(data) == 3
    assert data[0]["name"] == "#Bitcoin"


def test_dry_run_format_summary():
    fetcher = DryRunTrendsFetcher()
    summary = fetcher.format_summary()
    assert "Currently trending" in summary
    assert "#Bitcoin" in summary


def test_dry_run_get_trending_hashtags():
    fetcher = DryRunTrendsFetcher()
    tags = fetcher.get_trending_hashtags()
    assert "#Bitcoin" in tags
    assert "#CryptoMarket" in tags
    # "Fed rate" has no # so should not be in hashtags
    assert "Fed rate" not in tags
