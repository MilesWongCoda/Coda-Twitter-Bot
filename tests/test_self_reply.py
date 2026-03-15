# tests/test_self_reply.py
import pytest
from unittest.mock import MagicMock, patch
from bot.jobs.self_reply import SelfReplyJob


@pytest.fixture
def store():
    m = MagicMock()
    m.is_posted.return_value = False
    m.count_posts_since_midnight.return_value = 0
    m.get_recent_content.return_value = []
    return m


@pytest.fixture
def generator():
    m = MagicMock()
    m.generate_tweet.return_value = "Great point — ETH correlation is at 0.87, highest since March."
    return m


@pytest.fixture
def poster():
    m = MagicMock()
    m.post_reply.return_value = "reply_id_789"
    return m


def _make_engager(bot_uid="bot_123", mentions=None):
    e = MagicMock()
    e.get_own_user_id.return_value = bot_uid
    e.get_mentions.return_value = mentions or []
    e.sort_by_engagement.side_effect = lambda tw: sorted(
        tw, key=lambda t: t.get("engagement", 0), reverse=True
    )
    return e


def _quality_mention(mention_id="m1", text="@bot Great analysis on BTC! What about ETH?",
                     in_reply_to="bot_123", engagement=15, conversation_id=None):
    return {
        "id": mention_id, "text": text,
        "author_id": "commenter_1",
        "in_reply_to_user_id": in_reply_to,
        "conversation_id": conversation_id,
        "engagement": engagement,
    }


# ── Happy path ───────────────────────────────────────────────────────────────

@patch("bot.jobs.self_reply.DAILY_SELF_REPLY_CAP", 5)
def test_self_reply_happy_path(store, generator, poster):
    mention = _quality_mention()
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == ["self_reply_m1"]
    poster.post_reply.assert_called_once()
    store.mark_posted.assert_called_once_with("self_reply_m1", "self_reply",
                                               generator.generate_tweet.return_value)


# ── Filters ──────────────────────────────────────────────────────────────────

def test_self_reply_skips_non_reply_mentions(store, generator, poster):
    """Mentions that aren't replies to bot's own tweets should be skipped."""
    mention = _quality_mention(in_reply_to="someone_else")
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []
    poster.post_reply.assert_not_called()


def test_self_reply_skips_already_replied(store, generator, poster):
    store.is_posted.return_value = True
    mention = _quality_mention()
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []


def test_self_reply_skips_short_comments(store, generator, poster):
    mention = _quality_mention(text="@bot yes")  # too short after stripping @mention
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []


def test_self_reply_skips_promo_comments(store, generator, poster):
    mention = _quality_mention(text="@bot check this airdrop and free nft giveaway here")
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []


# ── Caps and edge cases ──────────────────────────────────────────────────────

@patch("bot.jobs.self_reply.DAILY_SELF_REPLY_CAP", 5)
def test_self_reply_respects_daily_cap(store, generator, poster):
    store.count_posts_since_midnight.return_value = 5  # at cap
    mention = _quality_mention()
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []
    poster.post_reply.assert_not_called()


def test_self_reply_returns_empty_when_no_bot_uid(store, generator, poster):
    engager = _make_engager(bot_uid=None)
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []


def test_self_reply_returns_empty_when_no_mentions(store, generator, poster):
    engager = _make_engager(mentions=[])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []


def test_self_reply_skips_when_generator_returns_empty(store, generator, poster):
    generator.generate_tweet.return_value = ""
    mention = _quality_mention()
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []
    poster.post_reply.assert_not_called()


def test_self_reply_skips_when_poster_fails(store, generator, poster):
    poster.post_reply.return_value = None
    mention = _quality_mention()
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []
    store.mark_posted.assert_not_called()


def test_self_reply_skips_bot_own_replies(store, generator, poster):
    """Bot's own replies appearing in mentions should be skipped (anti-loop)."""
    mention = _quality_mention(mention_id="m1", text="@someone Great analysis on BTC correlation data!")
    mention["author_id"] = "bot_123"  # bot replying to itself
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []
    poster.post_reply.assert_not_called()


@patch("bot.jobs.self_reply.DAILY_SELF_REPLY_CAP", 5)
def test_self_reply_includes_original_tweet_context(store, generator, poster):
    """When conversation_id maps to a stored tweet, the context should include our original tweet."""
    store.get_content_by_id.return_value = "BTC exchange outflows hit 6-month high. Bullish signal."
    mention = _quality_mention(conversation_id="orig_tweet_123")
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == ["self_reply_m1"]
    # Verify the context passed to generate_tweet includes our original tweet
    context_arg = generator.generate_tweet.call_args[0][0]
    assert "Your tweet:" in context_arg
    assert "BTC exchange outflows" in context_arg


@patch("bot.jobs.self_reply.DAILY_SELF_REPLY_CAP", 5)
def test_self_reply_strips_multiple_leading_mentions(store, generator, poster):
    """Mentions like '@bot @alice great analysis...' should strip all leading @handles."""
    mention = _quality_mention(text="@bot @alice Great analysis on BTC correlation data!")
    engager = _make_engager(mentions=[mention])
    job = SelfReplyJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == ["self_reply_m1"]  # passes quality check after stripping both handles
