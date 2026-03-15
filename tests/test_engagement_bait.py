# tests/bot/test_engagement_bait.py
import pytest
from unittest.mock import MagicMock, patch
from bot.jobs.engagement_bait import EngagementBaitJob


@pytest.fixture
def store():
    m = MagicMock()
    m.get_recent_content.return_value = []
    m.get_top_tweets.return_value = []
    m.get_bottom_tweets.return_value = []
    m.get_performance_patterns.return_value = {}
    m.get_recent_data_topics.return_value = set()
    return m


@pytest.fixture
def generator():
    m = MagicMock()
    m.generate_tweet.return_value = "$BTC hits $120k by June. Change my mind."
    m.last_variant = "tone_default"
    return m


@pytest.fixture
def poster():
    m = MagicMock()
    m.post_tweet.return_value = "tweet_id_bait_1"
    return m


@pytest.fixture
def prices():
    m = MagicMock()
    m.get_crypto_prices.return_value = {"bitcoin": {"usd": 95000}}
    m.format_crypto_summary.return_value = "BTC: $95,000 (+2.5%)"
    return m


def test_engagement_bait_posts_tweet(store, generator, poster, prices):
    job = EngagementBaitJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result == "tweet_id_bait_1"
    poster.post_tweet.assert_called_once()
    store.mark_posted.assert_called_once()


def test_engagement_bait_returns_none_on_empty_generation(store, generator, poster, prices):
    generator.generate_tweet.return_value = ""
    job = EngagementBaitJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()


def test_engagement_bait_context_includes_price_data(store, generator, poster, prices):
    job = EngagementBaitJob(store=store, generator=generator, poster=poster, prices=prices)
    job.execute()
    # Check the context passed to generate_tweet contains price info
    call_args = generator.generate_tweet.call_args
    context = call_args[0][0]
    assert "BTC" in context or "Market" in context
