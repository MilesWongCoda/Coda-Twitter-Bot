# tests/test_engager.py
import pytest
from unittest.mock import MagicMock
from bot.twitter.engager import Engager, DryRunEngager


@pytest.fixture
def engager():
    mock_client = MagicMock()
    mock_store = MagicMock()
    mock_store.is_posted.return_value = False
    return Engager(client=mock_client, store=mock_store)


def test_get_recent_tweets_from_user(engager):
    t1 = MagicMock(id="t1", text="BTC looking bullish")
    t1.public_metrics = {"like_count": 10, "retweet_count": 5, "reply_count": 2}
    t2 = MagicMock(id="t2", text="ETH merge complete")
    t2.public_metrics = {"like_count": 20, "retweet_count": 10, "reply_count": 3}
    engager.client.get_users_tweets.return_value = MagicMock(data=[t1, t2])
    tweets = engager.get_recent_tweets_from_user("user_id_123", max_results=5)
    assert len(tweets) == 2
    assert tweets[0]["id"] == "t1"
    assert "engagement" in tweets[0]


def test_skip_already_replied(engager):
    engager.store.is_posted.return_value = True
    engager.client.get_users_tweets.return_value = MagicMock(
        data=[MagicMock(id="t1", text="Some tweet")]
    )
    tweets = engager.get_unresponded_tweets("user_id_123")
    assert len(tweets) == 0


def test_filter_promotional_tweets(engager):
    tweets = [
        {"id": "1", "text": "BTC analysis thread here"},
        {"id": "2", "text": "SPONSORED: Buy crypto now at discount!"},
        {"id": "3", "text": "Fed raising rates again, watch out"},
    ]
    filtered = engager.filter_tweets(tweets)
    assert len(filtered) == 2
    assert all("SPONSORED" not in t["text"] for t in filtered)


def test_get_recent_tweets_handles_empty_response(engager):
    engager.client.get_users_tweets.return_value = MagicMock(data=None)
    tweets = engager.get_recent_tweets_from_user("user_id_123")
    assert tweets == []


def test_get_recent_tweets_handles_api_exception(engager):
    import tweepy
    engager.client.get_users_tweets.side_effect = tweepy.errors.TweepyException("rate limited")
    tweets = engager.get_recent_tweets_from_user("user_id_123")
    assert tweets == []


# ── DryRunEngager ──────────────────────────────────────────────────────────────

def test_sort_by_engagement(engager):
    tweets = [
        {"id": "1", "text": "Low", "engagement": 10},
        {"id": "2", "text": "High", "engagement": 500},
        {"id": "3", "text": "Mid", "engagement": 100},
    ]
    sorted_tweets = engager.sort_by_engagement(tweets)
    assert sorted_tweets[0]["engagement"] == 500
    assert sorted_tweets[1]["engagement"] == 100
    assert sorted_tweets[2]["engagement"] == 10


def test_sort_by_engagement_handles_missing_key(engager):
    tweets = [{"id": "1", "text": "No engagement key"}, {"id": "2", "text": "Has it", "engagement": 50}]
    sorted_tweets = engager.sort_by_engagement(tweets)
    assert sorted_tweets[0]["engagement"] == 50


# ── DryRunEngager ──────────────────────────────────────────────────────────────

def test_dry_run_engager_returns_fake_user_id():
    e = DryRunEngager()
    uid = e.get_user_id("elonmusk")
    assert uid is not None
    assert "elonmusk" in uid


def test_dry_run_engager_returns_fake_tweet():
    e = DryRunEngager()
    tweets = e.get_unresponded_tweets("dry_uid_elonmusk")
    assert len(tweets) == 2
    assert "id" in tweets[0] and "text" in tweets[0]
    assert "engagement" in tweets[0]


def test_dry_run_engager_filter_passthrough():
    e = DryRunEngager()
    tweets = [{"id": "1", "text": "BTC analysis"}, {"id": "2", "text": "buy now AIRDROP"}]
    assert e.filter_tweets(tweets) == tweets  # DryRunEngager passes everything through


# ── get_own_user_id ──────────────────────────────────────────────────────────

def test_get_own_user_id(engager):
    mock_me = MagicMock()
    mock_me.data = MagicMock(id=12345)
    engager.client.get_me.return_value = mock_me
    uid = engager.get_own_user_id()
    assert uid == "12345"
    # Second call should use cache, not call API again
    uid2 = engager.get_own_user_id()
    assert uid2 == "12345"
    engager.client.get_me.assert_called_once()


def test_get_own_user_id_returns_none_on_failure(engager):
    engager.client.get_me.side_effect = Exception("auth error")
    uid = engager.get_own_user_id()
    assert uid is None


# ── get_mentions ─────────────────────────────────────────────────────────────

def test_get_mentions(engager):
    t1 = MagicMock(id="m1", text="@bot great analysis", author_id="u1",
                   in_reply_to_user_id="bot_123", conversation_id="conv1")
    t1.public_metrics = {"like_count": 5, "retweet_count": 2, "reply_count": 3}
    engager.client.get_users_mentions.return_value = MagicMock(data=[t1])
    mentions = engager.get_mentions("bot_123")
    assert len(mentions) == 1
    assert mentions[0]["id"] == "m1"
    assert mentions[0]["in_reply_to_user_id"] == "bot_123"
    assert mentions[0]["engagement"] == 31  # 5*3 + 2*5 + 3*2


def test_get_mentions_handles_empty_response(engager):
    engager.client.get_users_mentions.return_value = MagicMock(data=None)
    mentions = engager.get_mentions("bot_123")
    assert mentions == []


def test_get_mentions_handles_api_exception(engager):
    engager.client.get_users_mentions.side_effect = Exception("rate limited")
    mentions = engager.get_mentions("bot_123")
    assert mentions == []


# ── DryRunEngager new methods ────────────────────────────────────────────────

def test_dry_run_get_own_user_id():
    e = DryRunEngager()
    uid = e.get_own_user_id()
    assert uid is not None
    assert isinstance(uid, str)


def test_dry_run_get_mentions():
    e = DryRunEngager()
    mentions = e.get_mentions("dry_bot_uid")
    assert len(mentions) == 2
    assert "id" in mentions[0]
    assert "in_reply_to_user_id" in mentions[0]


# ── search_recent_tweets ───────────────────────────────────────────────────

def test_search_recent_tweets_returns_results(engager):
    t1 = MagicMock(id="s1", text="$BTC whale accumulation detected", author_id="a1")
    t1.public_metrics = {"like_count": 50, "retweet_count": 10, "reply_count": 5}
    t1.reply_settings = "everyone"
    user1 = MagicMock(id="a1", username="whale_watcher")
    resp = MagicMock(data=[t1], includes={"users": [user1]})
    engager.client.search_recent_tweets.return_value = resp
    results = engager.search_recent_tweets("$BTC whale")
    assert len(results) == 1
    assert results[0]["username"] == "whale_watcher"
    assert results[0]["engagement"] > 0


def test_search_recent_tweets_returns_empty_on_api_failure(engager):
    engager.client.search_recent_tweets.side_effect = Exception("API error")
    results = engager.search_recent_tweets("$BTC")
    assert results == []


def test_search_recent_tweets_filters_restricted_replies(engager):
    t1 = MagicMock(id="s1", text="BTC analysis", author_id="a1")
    t1.public_metrics = {"like_count": 10, "retweet_count": 5, "reply_count": 2}
    t1.reply_settings = "mentionedUsers"  # restricted
    t2 = MagicMock(id="s2", text="Bitcoin ETF inflows", author_id="a2")
    t2.public_metrics = {"like_count": 20, "retweet_count": 8, "reply_count": 3}
    t2.reply_settings = "everyone"
    resp = MagicMock(data=[t1, t2], includes={"users": []})
    engager.client.search_recent_tweets.return_value = resp
    results = engager.search_recent_tweets("Bitcoin ETF")
    assert len(results) == 1
    assert results[0]["id"] == "s2"


# ── DryRunEngager search ─────────────────────────────────────────────────────

# ── like_tweet ────────────────────────────────────────────────────────────────

def test_like_tweet_success(engager):
    engager.client.like.return_value = MagicMock()
    assert engager.like_tweet("t123") is True
    engager.client.like.assert_called_once_with("t123")


def test_like_tweet_handles_exception(engager):
    engager.client.like.side_effect = Exception("rate limited")
    assert engager.like_tweet("t123") is False


def test_dry_run_like_tweet():
    e = DryRunEngager()
    assert e.like_tweet("t123") is True


def test_dry_run_search_recent_tweets():
    e = DryRunEngager()
    results = e.search_recent_tweets("$BTC whale")
    assert len(results) == 2
    assert "id" in results[0]
    assert results[0]["reply_settings"] == "everyone"
