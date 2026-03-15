# tests/test_jobs.py
import pytest
from unittest.mock import MagicMock, patch
from bot.jobs.morning_brief import MorningBriefJob
from bot.jobs.hot_take import HotTakeJob
from bot.jobs.onchain_signal import OnChainSignalJob
from bot.jobs.alpha_thread import AlphaThreadJob
from bot.jobs.us_open import USOpenJob
from bot.jobs.evening_wrap import EveningWrapJob
from bot.jobs.engagement import EngagementJob
from bot.jobs.price_alert import PriceAlertJob
from bot.jobs.weekly_poll import WeeklyPollJob
from bot.jobs.weekly_recap import WeeklyRecapJob
from bot.jobs.metrics_collector import MetricsCollectorJob


@pytest.fixture
def store():
    m = MagicMock()
    m.topic_on_cooldown.return_value = False
    m.count_posts_since_midnight.return_value = 0
    m.get_recent_content.return_value = []
    m.is_user_restricted.return_value = False
    m.prune_restricted_users.return_value = 0
    return m


@pytest.fixture
def generator():
    m = MagicMock()
    m.generate_tweet.return_value = "BTC shows strength above key resistance."
    m.generate_thread.return_value = ["1/ Thread tweet one", "2/ Thread tweet two"]
    return m


@pytest.fixture
def poster():
    m = MagicMock()
    m.post_tweet.return_value = "tweet_id_123"
    m.post_thread.return_value = ["t1", "t2"]
    m.post_reply.return_value = "reply_id_456"
    m.upload_image_from_url.return_value = "media_id_789"
    m.upload_image_from_file.return_value = "media_id_file_789"
    return m


@pytest.fixture
def news():
    m = MagicMock()
    m.fetch_all.return_value = [
        {"title": "BTC surges", "summary": "Bitcoin rose 5%", "link": "http://a.com/1"},
    ]
    return m


@pytest.fixture
def prices():
    m = MagicMock()
    m.get_crypto_prices.return_value = {"bitcoin": {"usd": 95000, "usd_24h_change": 2.5}}
    m.format_crypto_summary.return_value = "BTC: $95,000 (+2.5%)"
    m.get_fear_greed.return_value = {"value": 72, "label": "Greed"}
    m.format_fear_greed.return_value = "Fear & Greed: 72/100 (Greed)"
    return m


@pytest.fixture
def summarizer():
    m = MagicMock()
    m.summarize.return_value = "• BTC surged 5%\n• Fed holds rates"
    return m


@pytest.fixture
def onchain():
    m = MagicMock()
    btc_snap = {"active_addresses": 450000, "fastest_fee": 42}
    m.get_btc_snapshot.return_value = btc_snap
    m.format_summary.return_value = "Active addresses: 450,000 | Fees: 42 sat/vB"
    m.get_multi_chain_snapshot.return_value = {"btc": btc_snap, "eth": {}, "sol": {}}
    m.format_multi_summary.return_value = "BTC: Active addresses: 450,000 | Fees: 42 sat/vB"
    return m


# ── MorningBriefJob ──────────────────────────────────────────────────────────

def test_morning_brief_posts_tweet(store, generator, poster, news, prices, summarizer):
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices)
    result = job.execute()
    assert result == "tweet_id_123"
    poster.post_tweet.assert_called_once()
    store.mark_posted.assert_called_once()


def test_morning_brief_skips_on_cooldown(store, generator, poster, news, prices, summarizer):
    store.topic_on_cooldown.return_value = True
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()


def test_morning_brief_skips_when_no_news(store, generator, poster, news, prices, summarizer):
    news.fetch_all.return_value = []
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices)
    result = job.execute()
    assert result is None


def test_morning_brief_raises_when_poster_fails(store, generator, poster, news, prices, summarizer):
    """Twitter API failure (poster returns None) must propagate as an exception
    so BaseJob.run() fires notify_failure — not silently swallowed."""
    poster.post_tweet.return_value = None
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices)
    with pytest.raises(RuntimeError):
        job.execute()
    store.mark_posted.assert_not_called()


def test_morning_brief_generates_chart(store, generator, poster, news, prices, summarizer):
    """Morning brief should generate a branded market card and upload it."""
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices)
    job.execute()
    poster.upload_image_from_file.assert_called_once()
    call_kwargs = poster.post_tweet.call_args
    assert call_kwargs.kwargs.get("media_ids") == ["media_id_file_789"]


# ── HotTakeJob ───────────────────────────────────────────────────────────────

def _make_polymarket_mock():
    poly = MagicMock()
    poly.get_trending_markets.return_value = [
        {"question": "Will the Fed cut rates in March?", "yes_prob": 0.35, "volume_24h": 1_800_000},
        {"question": "Will Bitcoin reach $150K?", "yes_prob": 0.12, "volume_24h": 2_500_000},
    ]
    return poly


def test_hot_take_posts_polymarket_question_text_day(store, generator, poster):
    """Non-poll day (Mon/Wed/Fri/Sun) → plain tweet."""
    poly = _make_polymarket_mock()
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    with patch("bot.jobs.hot_take.datetime") as mock_dt:
        mock_dt.datetime.now.return_value.weekday.return_value = 0  # Monday
        result = job.execute()
    assert result == "tweet_id_123"
    posted_text = poster.post_tweet.call_args[0][0]
    assert "Fed cut rates" in posted_text
    assert posted_text.endswith("?")
    poster.post_poll.assert_not_called()


def test_hot_take_posts_polymarket_poll_day(store, generator, poster):
    """Poll day (Tue/Thu/Sat) → Twitter poll with Yes/No."""
    poly = _make_polymarket_mock()
    poster.post_poll.return_value = "poll_id_456"
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    with patch("bot.jobs.hot_take.datetime") as mock_dt:
        mock_dt.datetime.now.return_value.weekday.return_value = 1  # Tuesday
        result = job.execute()
    assert result == "poll_id_456"
    poster.post_poll.assert_called_once()
    call_args = poster.post_poll.call_args
    assert "Fed cut rates" in call_args[0][0]
    assert call_args[0][1] == ["Yes", "No"]
    assert call_args[1]["duration_minutes"] == 1440
    poster.post_tweet.assert_not_called()


def test_hot_take_raises_when_poster_fails(store, generator, poster):
    poster.post_tweet.return_value = None
    poly = _make_polymarket_mock()
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    with patch("bot.jobs.hot_take.datetime") as mock_dt:
        mock_dt.datetime.now.return_value.weekday.return_value = 0  # text day
        with pytest.raises(RuntimeError):
            job.execute()


def test_hot_take_raises_when_poll_fails(store, generator, poster):
    poster.post_poll.return_value = None
    poly = _make_polymarket_mock()
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    with patch("bot.jobs.hot_take.datetime") as mock_dt:
        mock_dt.datetime.now.return_value.weekday.return_value = 3  # Thursday = poll day
        with pytest.raises(RuntimeError):
            job.execute()


def test_hot_take_skips_all_on_cooldown(store, generator, poster):
    store.topic_on_cooldown.return_value = True
    poly = _make_polymarket_mock()
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()
    poster.post_poll.assert_not_called()


def test_hot_take_skips_when_no_markets(store, generator, poster):
    poly = MagicMock()
    poly.get_trending_markets.return_value = []
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    result = job.execute()
    assert result is None


def test_hot_take_skips_when_no_polymarket(store, generator, poster):
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=None)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()


def test_hot_take_no_image(store, generator, poster):
    """Hot Take posts text only — no media in either mode."""
    poly = _make_polymarket_mock()
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    with patch("bot.jobs.hot_take.datetime") as mock_dt:
        mock_dt.datetime.now.return_value.weekday.return_value = 0  # text day
        job.execute()
    assert poster.post_tweet.call_args.kwargs.get("media_ids") is None


# ── USOpenJob ────────────────────────────────────────────────────────────────

def test_us_open_posts_tweet(store, generator, poster, news, prices, summarizer):
    job = USOpenJob(store=store, generator=generator, poster=poster,
                    summarizer=summarizer, news=news, prices=prices)
    result = job.execute()
    assert result == "tweet_id_123"
    poster.post_tweet.assert_called_once()
    store.mark_posted.assert_called_once()


def test_us_open_skips_on_cooldown(store, generator, poster, news, prices, summarizer):
    store.topic_on_cooldown.return_value = True
    job = USOpenJob(store=store, generator=generator, poster=poster,
                    summarizer=summarizer, news=news, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()


def test_us_open_raises_when_poster_fails(store, generator, poster, news, prices, summarizer):
    poster.post_tweet.return_value = None
    job = USOpenJob(store=store, generator=generator, poster=poster,
                    summarizer=summarizer, news=news, prices=prices)
    with pytest.raises(RuntimeError):
        job.execute()


def test_us_open_returns_none_when_generator_empty(store, generator, poster, news, prices, summarizer):
    generator.generate_tweet.return_value = ""
    job = USOpenJob(store=store, generator=generator, poster=poster,
                    summarizer=summarizer, news=news, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()


def test_us_open_generates_chart(store, generator, poster, news, prices, summarizer):
    """US Open now generates a candlestick chart."""
    prices.get_ohlc.return_value = [[1700000000000 + i * 3600000, 95000, 96000, 94500, 95500] for i in range(10)]
    job = USOpenJob(store=store, generator=generator, poster=poster,
                    summarizer=summarizer, news=news, prices=prices)
    job.execute()
    poster.upload_image_from_file.assert_called_once()


# ── EveningWrapJob ───────────────────────────────────────────────────────────

def test_evening_wrap_posts_tweet(store, generator, poster, news, prices, summarizer):
    job = EveningWrapJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, prices=prices)
    result = job.execute()
    assert result == "tweet_id_123"
    poster.post_tweet.assert_called_once()


def test_evening_wrap_skips_on_cooldown(store, generator, poster, news, prices, summarizer):
    store.topic_on_cooldown.return_value = True
    job = EveningWrapJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, prices=prices)
    result = job.execute()
    assert result is None


def test_evening_wrap_raises_when_poster_fails(store, generator, poster, news, prices, summarizer):
    poster.post_tweet.return_value = None
    job = EveningWrapJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, prices=prices)
    with pytest.raises(RuntimeError):
        job.execute()


def test_evening_wrap_returns_none_when_generator_empty(store, generator, poster, news, prices, summarizer):
    generator.generate_tweet.return_value = ""
    job = EveningWrapJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()


def test_evening_wrap_generates_chart(store, generator, poster, news, prices, summarizer):
    """Evening wrap should generate a branded market card and upload it."""
    job = EveningWrapJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, prices=prices)
    job.execute()
    poster.upload_image_from_file.assert_called_once()


# ── OnChainSignalJob ──────────────────────────────────────────────────────────

def test_onchain_signal_posts_tweet(store, generator, poster, onchain):
    job = OnChainSignalJob(store=store, generator=generator, poster=poster, onchain=onchain)
    result = job.execute()
    assert result == "tweet_id_123"
    poster.post_tweet.assert_called_once()


def test_onchain_signal_skips_on_cooldown(store, generator, poster, onchain):
    store.topic_on_cooldown.return_value = True
    job = OnChainSignalJob(store=store, generator=generator, poster=poster, onchain=onchain)
    result = job.execute()
    assert result is None
    onchain.get_btc_snapshot.assert_not_called()  # cooldown checked BEFORE API call


def test_onchain_signal_skips_when_no_data(store, generator, poster, onchain):
    onchain.get_multi_chain_snapshot.return_value = {"btc": {}, "eth": {}, "sol": {}}
    job = OnChainSignalJob(store=store, generator=generator, poster=poster, onchain=onchain)
    result = job.execute()
    assert result is None


def test_onchain_signal_includes_derivatives_when_present(store, generator, poster, onchain):
    derivatives = MagicMock()
    derivatives.get_multi_snapshot.return_value = {"btc": {"avg_rate": 0.0003}, "eth": {}}
    derivatives.format_multi_summary.return_value = "BTC Funding: +0.030% (longs paying premium)"
    job = OnChainSignalJob(store=store, generator=generator, poster=poster,
                           onchain=onchain, derivatives=derivatives)
    job.execute()
    call_context = generator.generate_tweet.call_args[0][0]
    assert "Funding" in call_context


def test_onchain_signal_works_without_derivatives(store, generator, poster, onchain):
    job = OnChainSignalJob(store=store, generator=generator, poster=poster,
                           onchain=onchain, derivatives=None)
    result = job.execute()
    assert result == "tweet_id_123"


# ── AlphaThreadJob ────────────────────────────────────────────────────────────

def test_alpha_thread_posts_thread(store, generator, poster, news, prices, summarizer, onchain):
    generator.generate_thread.return_value = ["1/ t1", "2/ t2", "3/ t3", "4/ t4", "5/ t5"]
    poster.post_thread.return_value = ["t1", "t2", "t3", "t4", "t5"]
    job = AlphaThreadJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, onchain=onchain, prices=prices)
    result = job.execute()
    assert result == ["t1", "t2", "t3", "t4", "t5"]
    poster.post_thread.assert_called_once()
    assert store.mark_posted.call_count == 5


def test_alpha_thread_raises_when_all_posts_fail(store, generator, poster, news,
                                                   prices, summarizer, onchain):
    generator.generate_thread.return_value = ["1/ t1", "2/ t2", "3/ t3", "4/ t4", "5/ t5"]
    poster.post_thread.return_value = []
    job = AlphaThreadJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, onchain=onchain, prices=prices)
    with pytest.raises(RuntimeError):
        job.execute()
    store.mark_posted.assert_not_called()


def test_alpha_thread_returns_none_when_generate_thread_is_incomplete(
        store, generator, poster, news, prices, summarizer, onchain):
    # LLM returns only 2 tweets instead of 5 — should abort, not post broken thread
    generator.generate_thread.return_value = ["1/ Only intro tweet", "2/ Second tweet"]
    job = AlphaThreadJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, onchain=onchain, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_thread.assert_not_called()
    store.mark_posted.assert_not_called()


def test_alpha_thread_partial_post_stores_and_raises(
        store, generator, poster, news, prices, summarizer, onchain):
    # 3 of 5 tweets post — continuation also fails, raises so operator is notified
    generator.generate_thread.return_value = ["1/ t1", "2/ t2", "3/ t3", "4/ t4", "5/ t5"]
    poster.post_thread.return_value = ["id1", "id2", "id3"]  # only 3 of 5 succeeded
    # Continuation attempts fail (create_tweet returns no data)
    poster.client.create_tweet.return_value = MagicMock(data=None)
    job = AlphaThreadJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, onchain=onchain, prices=prices)
    with pytest.raises(RuntimeError, match="partially posted"):
        job.execute()
    # 3 original + continuation attempts stored
    assert store.mark_posted.call_count == 3


# ── V3: flexible thread length ───────────────────────────────────────────────

def test_alpha_thread_accepts_4_tweets(store, generator, poster, news,
                                       prices, summarizer, onchain):
    """AI generating 4 tweets instead of 5 should still post (min = num_tweets - 1)."""
    generator.generate_thread.return_value = ["1/ t1", "2/ t2", "3/ t3", "4/ t4"]
    poster.post_thread.return_value = ["id1", "id2", "id3", "id4"]
    job = AlphaThreadJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, onchain=onchain, prices=prices)
    result = job.execute()
    assert result == ["id1", "id2", "id3", "id4"]
    poster.post_thread.assert_called_once()
    assert len(poster.post_thread.call_args[0][0]) == 4  # exactly 4 tweets posted


def test_alpha_thread_trims_6_tweets_to_5(store, generator, poster, news,
                                           prices, summarizer, onchain):
    """AI generating 6 tweets should be trimmed to 5."""
    generator.generate_thread.return_value = ["1/ t1", "2/ t2", "3/ t3", "4/ t4", "5/ t5", "6/ t6"]
    poster.post_thread.return_value = ["id1", "id2", "id3", "id4", "id5"]
    job = AlphaThreadJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, onchain=onchain, prices=prices)
    result = job.execute()
    assert result == ["id1", "id2", "id3", "id4", "id5"]
    # Should have passed 5 tweets to post_thread
    posted_tweets = poster.post_thread.call_args[0][0]
    assert len(posted_tweets) == 5


def test_alpha_thread_skips_when_no_news(store, generator, poster, news,
                                          prices, summarizer, onchain):
    news.fetch_all.return_value = []
    job = AlphaThreadJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, onchain=onchain, prices=prices)
    result = job.execute()
    assert result is None


def test_alpha_thread_returns_none_when_generate_thread_empty(
        store, generator, poster, news, prices, summarizer, onchain):
    generator.generate_thread.return_value = []
    job = AlphaThreadJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, onchain=onchain, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_thread.assert_not_called()


def test_onchain_signal_returns_none_when_generator_empty(store, generator, poster, onchain):
    generator.generate_tweet.return_value = ""
    job = OnChainSignalJob(store=store, generator=generator, poster=poster, onchain=onchain)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()


# ── EngagementJob ─────────────────────────────────────────────────────────────

@pytest.mark.skip(reason="Reply/QT disabled — API 403, moving to browser engagement")
@patch("bot.jobs.engagement.time.sleep")
def test_engagement_replies_to_watchlist(_sleep, store, generator, poster):
    from bot.twitter.watchlist import TIER1_USERNAMES
    engager = MagicMock()
    engager.get_user_id.side_effect = lambda u: "user_123" if u == TIER1_USERNAMES[0] else None
    # engagement=30 keeps it below QT threshold (50) so it takes the reply path
    engager.get_unresponded_tweets.return_value = [{"id": "t1", "text": "Great BTC analysis", "engagement": 30, "username": TIER1_USERNAMES[0], "tier": "tier1"}]
    engager.filter_tweets.side_effect = lambda tweets: tweets
    engager.sort_by_engagement.side_effect = lambda tweets: tweets

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    job.execute()
    poster.post_reply.assert_called()
    store.mark_posted.assert_called_once()


@patch("bot.jobs.engagement.time.sleep")
def test_engagement_skips_reply_when_generate_tweet_returns_empty(_sleep, store, generator, poster):
    from bot.twitter.watchlist import TIER1_USERNAMES
    generator.generate_tweet.return_value = ""  # AI returns nothing
    engager = MagicMock()
    engager.get_user_id.side_effect = lambda u: "user_123" if u == TIER1_USERNAMES[0] else None
    engager.get_unresponded_tweets.return_value = [{"id": "t1", "text": "Great BTC analysis", "engagement": 50}]
    engager.filter_tweets.side_effect = lambda tweets: tweets
    engager.sort_by_engagement.side_effect = lambda tweets: tweets

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    poster.post_reply.assert_not_called()
    store.mark_posted.assert_not_called()
    assert result == []


@patch("bot.jobs.engagement.time.sleep")
def test_engagement_stops_at_daily_cap(_sleep, store, generator, poster):
    # Both reply and quote caps reached
    def mock_count(job_type):
        if job_type == "reply":
            return 20
        if job_type == "quote_tweet":
            return 10
        return 0
    store.count_posts_since_midnight.side_effect = mock_count
    engager = MagicMock()
    engager.get_user_id.return_value = "user_123"
    engager.get_unresponded_tweets.return_value = [{"id": "t1", "text": "test", "engagement": 10}]
    engager.filter_tweets.side_effect = lambda tweets: tweets
    engager.sort_by_engagement.side_effect = lambda tweets: tweets

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    poster.post_reply.assert_not_called()
    poster.post_quote_tweet.assert_not_called()


def test_engagement_skips_user_when_id_not_found(store, generator, poster):
    engager = MagicMock()
    engager.get_user_id.return_value = None
    engager.sort_by_engagement.side_effect = lambda tweets: tweets

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    job.execute()
    engager.get_unresponded_tweets.assert_not_called()


@pytest.mark.skip(reason="Reply/QT disabled — API 403, moving to browser engagement")
@patch("bot.jobs.engagement.time.sleep")
def test_engagement_returns_list_of_posted_keys(_sleep, store, generator, poster):
    from bot.twitter.watchlist import TIER1_USERNAMES
    engager = MagicMock()
    engager.get_user_id.side_effect = lambda u: "user_123" if u == TIER1_USERNAMES[0] else None
    # engagement=30 keeps below QT threshold so reply path is taken
    engager.get_unresponded_tweets.return_value = [{"id": "t99", "text": "Great analysis", "engagement": 30}]
    engager.filter_tweets.side_effect = lambda tweets: tweets
    engager.sort_by_engagement.side_effect = lambda tweets: tweets

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert isinstance(result, list)
    assert result == ["reply_t99"]


@pytest.mark.skip(reason="Reply/QT disabled — API 403, moving to browser engagement")
@patch("bot.jobs.engagement.time.sleep")
def test_engagement_quote_tweet_hot_topic(_sleep, store, generator, poster):
    """Hot topic + high engagement should trigger quote tweet."""
    from bot.twitter.watchlist import TIER1_USERNAMES
    engager = MagicMock()
    engager.get_own_user_id.return_value = "bot_uid"
    engager.get_mentions.return_value = []
    first_user = TIER1_USERNAMES[0]
    engager.get_user_id.side_effect = lambda u: "user_123" if u == first_user else None
    # Tweet text contains trending coin keywords that _is_hot_topic will match
    engager.get_unresponded_tweets.return_value = [
        {"id": "t1", "text": "Polymarket prediction market odds shifting on rate cut", "engagement": 500}
    ]
    engager.filter_tweets.side_effect = lambda tw: tw
    engager.search_recent_tweets.return_value = []
    store.is_posted.return_value = False

    poster.post_reply.return_value = "reply_id"
    poster.post_quote_tweet.return_value = "qt_id"

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    # Should QT because it's Polymarket-related with high engagement
    poster.post_quote_tweet.assert_called()
    assert any("quote_" in key for key in result)


@pytest.mark.skip(reason="Reply/QT disabled — API 403, moving to browser engagement")
@patch("bot.jobs.engagement.time.sleep")
def test_engagement_reply_counter_only_increments_on_success(_sleep, store, generator, poster):
    """Counter must NOT increment when post fails — otherwise cadence drifts."""
    from bot.twitter.watchlist import TIER1_USERNAMES
    engager = MagicMock()
    engager.get_own_user_id.return_value = None  # skip conversation phase
    first_user = TIER1_USERNAMES[0]
    engager.get_user_id.side_effect = lambda u: "user_123" if u == first_user else None
    # Use question tweets so _should_quote_tweet returns False → forces reply path
    def make_tweets(user_id, max_results=10):
        return [
            {"id": f"t{i}", "text": f"What about BTC target {i}?", "engagement": 50}
            for i in range(3)
        ]
    engager.get_unresponded_tweets.side_effect = make_tweets
    engager.filter_tweets.side_effect = lambda tw: tw
    engager.search_recent_tweets.return_value = []

    # First reply succeeds, second fails, third succeeds
    poster.post_reply.side_effect = ["r1", None, "r3"]

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    # Only successful posts should be in result (failed one skipped)
    assert len(result) == 2


def test_engagement_returns_empty_list_when_no_tweets(store, generator, poster):
    engager = MagicMock()
    engager.get_user_id.return_value = "user_123"
    engager.get_unresponded_tweets.return_value = []
    engager.filter_tweets.side_effect = lambda tweets: tweets
    engager.sort_by_engagement.side_effect = lambda tweets: tweets

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert result == []


@pytest.mark.skip(reason="Reply/QT disabled — API 403, moving to browser engagement")
@patch("bot.jobs.engagement.time.sleep")
def test_engagement_search_candidates_collected(_sleep, store, generator, poster):
    """Search candidates should be collected and included."""
    engager = MagicMock()
    engager.get_user_id.return_value = None  # No watchlist users resolve
    engager.get_unresponded_tweets.return_value = []
    engager.filter_tweets.side_effect = lambda tweets: tweets
    engager.search_recent_tweets.return_value = [
        {"id": "search_t1", "text": "$BTC whale accumulation", "author_id": "ext_1",
         "username": "whale_finder", "engagement": 200, "reply_settings": "everyone"}
    ]

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    engager.search_recent_tweets.assert_called()
    poster.post_reply.assert_called()


@patch("bot.jobs.engagement.time.sleep")
def test_engagement_search_excludes_watchlist_users(_sleep, store, generator, poster):
    """Search results from users already in watchlist should be excluded."""
    engager = MagicMock()
    # Simulate a watchlist user resolving to user_id "known_123"
    engager.get_user_id.side_effect = lambda u: "known_123" if u == "VitalikButerin" else None
    engager.get_unresponded_tweets.return_value = []
    engager.filter_tweets.side_effect = lambda tweets: tweets
    # Search returns a tweet from the same known user
    engager.search_recent_tweets.return_value = [
        {"id": "search_dup", "text": "BTC ETF data", "author_id": "known_123",
         "username": "VitalikButerin", "engagement": 500, "reply_settings": "everyone"}
    ]

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    # The search tweet from known_123 should be excluded, so no replies
    poster.post_reply.assert_not_called()


@pytest.mark.skip(reason="Reply/QT disabled — API 403, moving to browser engagement")
@patch("bot.jobs.engagement.time.sleep")
def test_engagement_search_sort_score_is_1_5x(_sleep, store, generator, poster):
    """Search candidates should have 1.5x engagement sort score."""
    engager = MagicMock()
    engager.get_user_id.return_value = None
    engager.get_unresponded_tweets.return_value = []
    engager.filter_tweets.side_effect = lambda tweets: tweets
    engager.search_recent_tweets.return_value = [
        {"id": "search_t1", "text": "$BTC whale move", "author_id": "ext_1",
         "username": "user1", "engagement": 100, "reply_settings": "everyone"}
    ]

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    job.execute()
    # Verify the generator was called (tweet passed through)
    # The 1.5x score is internal sorting — we test that the tweet was processed
    generator.generate_tweet.assert_called()


@patch("bot.jobs.engagement.time.sleep")
def test_engagement_tier3_has_accounts(_sleep, store, generator, poster):
    """Tier3 should have mid-range accounts for open-reply engagement."""
    from bot.twitter.watchlist import TIER3_USERNAMES
    assert len(TIER3_USERNAMES) > 0


# ── Dynamic Search Queries ───────────────────────────────────────────────────

def test_engagement_dynamic_queries_polymarket_and_trending(store, generator, poster):
    """Search queries should include Polymarket + trending coins (no static crypto/trends)."""
    engager = MagicMock()
    polymarket = MagicMock()
    polymarket.get_trending_markets.return_value = [
        {"question": "Will the Fed cut rates in March?", "yes_prob": 0.35, "volume_24h": 1_800_000},
    ]
    prices = MagicMock()
    prices.get_trending_coins.return_value = [
        {"symbol": "PEPE", "name": "Pepe", "market_cap_rank": 40},
    ]
    job = EngagementJob(store=store, generator=generator, poster=poster,
                        engager=engager, polymarket=polymarket, prices=prices)
    queries = job._build_search_queries()
    assert len(queries) <= 12
    combined = " ".join(queries).lower()
    # Should have Polymarket-derived query
    assert "fed" in combined or "rate" in combined
    # Should have trending coin
    assert "$pepe" in combined


def test_engagement_dynamic_queries_fallback_on_trends_failure(store, generator, poster):
    """If trends fail, should fall back to static SEARCH_QUERIES."""
    from bot.jobs.engagement import SEARCH_QUERIES
    engager = MagicMock()
    trends = MagicMock()
    trends.fetch_all.side_effect = Exception("API error")
    job = EngagementJob(store=store, generator=generator, poster=poster,
                        engager=engager, trends=trends)
    queries = job._build_search_queries()
    # Should still have queries (from static list)
    assert len(queries) > 0
    assert len(queries) <= 12


# ── Engagement Effectiveness Logging ─────────────────────────────────────────

@pytest.mark.skip(reason="Reply/QT disabled — API 403, moving to browser engagement")
@patch("bot.jobs.engagement.time.sleep")
def test_engagement_logs_effectiveness_on_reply(_sleep, store, generator, poster):
    """After a successful reply, store.log_engagement should be called."""
    from bot.twitter.watchlist import TIER1_USERNAMES
    engager = MagicMock()
    engager.get_own_user_id.return_value = "bot_uid"
    engager.get_mentions.return_value = []
    engager.get_user_id.side_effect = lambda u: "user_123" if u == TIER1_USERNAMES[0] else None
    # engagement=30 keeps below QT threshold to test the reply path
    engager.get_unresponded_tweets.return_value = [
        {"id": "t1", "text": "BTC analysis", "engagement": 30, "username": TIER1_USERNAMES[0], "tier": "tier1"}
    ]
    engager.filter_tweets.side_effect = lambda tw: tw
    engager.search_recent_tweets.return_value = []
    store.is_posted.return_value = False

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    job.execute()
    store.log_engagement.assert_called()
    call_args = store.log_engagement.call_args[0]
    assert call_args[2] == "reply"  # action_type


# ── Smart QT vs Reply ────────────────────────────────────────────────────────

def test_should_quote_tweet_hot_topic_high_engagement():
    """Polymarket-related tweets should trigger QT at lower thresholds."""
    from bot.jobs.engagement import EngagementJob
    job = EngagementJob.__new__(EngagementJob)
    job.polymarket = None
    job.prices = None
    # Polymarket-related with decent engagement → QT
    tweet = {"text": "Polymarket prediction odds just shifted", "tier": "tier1", "engagement": 30}
    assert job._should_quote_tweet(tweet) is True
    # Polymarket with engagement=5 → now QT (lowered threshold from 20 to 5)
    tweet2 = {"text": "Polymarket data", "tier": "tier1", "engagement": 5}
    assert job._should_quote_tweet(tweet2) is True
    # Polymarket with engagement < 5 → no QT
    tweet3 = {"text": "Polymarket data", "tier": "tier1", "engagement": 2}
    assert job._should_quote_tweet(tweet3) is False


def test_should_quote_tweet_question_gets_reply():
    """Tweet with question mark should get a reply, not QT."""
    from bot.jobs.engagement import EngagementJob
    job = EngagementJob.__new__(EngagementJob)
    tweet = {"text": "What's your BTC target for this cycle?", "tier": "tier1", "engagement": 500}
    assert job._should_quote_tweet(tweet) is False


# ── Conversation Continuation ────────────────────────────────────────────────

@patch("bot.jobs.engagement.time.sleep")
def test_engagement_conversation_candidates_replied(_sleep, store, generator, poster):
    """Mentions that are replies to our tweets should get conversation replies."""
    engager = MagicMock()
    engager.get_own_user_id.return_value = "bot_uid"
    engager.get_mentions.return_value = [
        {"id": "m1", "text": "@bot great analysis!", "author_id": "u1",
         "in_reply_to_user_id": "bot_uid", "engagement": 20}
    ]
    engager.get_user_id.return_value = None
    engager.get_unresponded_tweets.return_value = []
    engager.filter_tweets.side_effect = lambda tw: tw
    engager.search_recent_tweets.return_value = []
    store.is_posted.return_value = False

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert "convo_m1" in result
    poster.post_reply.assert_called()


@patch("bot.jobs.engagement.time.sleep")
def test_engagement_conversation_skips_already_handled(_sleep, store, generator, poster):
    """Already-replied conversation mentions should not be re-replied."""
    engager = MagicMock()
    engager.get_own_user_id.return_value = "bot_uid"
    engager.get_mentions.return_value = [
        {"id": "m1", "text": "@bot great!", "author_id": "u1",
         "in_reply_to_user_id": "bot_uid", "engagement": 20}
    ]
    engager.get_user_id.return_value = None
    engager.get_unresponded_tweets.return_value = []
    engager.filter_tweets.side_effect = lambda tw: tw
    engager.search_recent_tweets.return_value = []
    # convo_m1 already posted
    store.is_posted.side_effect = lambda key: key == "convo_m1"

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    assert "convo_m1" not in result


@patch("bot.jobs.engagement.time.sleep")
def test_engagement_conversation_cap_respected(_sleep, store, generator, poster):
    """At most CONVO_PER_RUN_CAP conversation replies per run."""
    from bot.jobs.engagement import CONVO_PER_RUN_CAP
    engager = MagicMock()
    engager.get_own_user_id.return_value = "bot_uid"
    # 10 mentions — more than cap
    engager.get_mentions.return_value = [
        {"id": f"m{i}", "text": f"@bot reply {i}", "author_id": f"u{i}",
         "in_reply_to_user_id": "bot_uid", "engagement": 10}
        for i in range(10)
    ]
    engager.get_user_id.return_value = None
    engager.get_unresponded_tweets.return_value = []
    engager.filter_tweets.side_effect = lambda tw: tw
    engager.search_recent_tweets.return_value = []
    store.is_posted.return_value = False

    job = EngagementJob(store=store, generator=generator, poster=poster, engager=engager)
    result = job.execute()
    convo_keys = [k for k in result if k.startswith("convo_")]
    assert len(convo_keys) <= CONVO_PER_RUN_CAP


# ── BaseJob.run() notifier integration ───────────────────────────────────────

def test_run_calls_notifier_on_success(store, generator, poster, summarizer, news, prices):
    notifier = MagicMock()
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices, notifier=notifier)
    job.run()
    notifier.notify_success.assert_called_once()
    args = notifier.notify_success.call_args[0]
    assert args[0] == "MorningBrief"
    assert args[1] == "tweet_id_123"  # result from poster.post_tweet


def test_run_calls_notifier_on_exception(store, generator, poster, summarizer, news, prices):
    notifier = MagicMock()
    generator.generate_tweet.side_effect = RuntimeError("Anthropic down")
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices, notifier=notifier)
    job.run()
    notifier.notify_failure.assert_called_once()
    args = notifier.notify_failure.call_args[0]
    assert "MorningBrief" in args[0]


def test_run_notifier_not_called_when_job_skipped(store, generator, poster,
                                                   summarizer, news, prices):
    notifier = MagicMock()
    store.topic_on_cooldown.return_value = True  # job will return None (skipped)
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices, notifier=notifier)
    job.run()
    notifier.notify_success.assert_not_called()
    notifier.notify_failure.assert_not_called()


def test_alpha_thread_notifier_gets_tweet_ids(
        store, generator, poster, news, prices, summarizer, onchain):
    notifier = MagicMock()
    generator.generate_thread.return_value = ["1/ t1", "2/ t2", "3/ t3", "4/ t4", "5/ t5"]
    poster.post_thread.return_value = ["id1", "id2", "id3", "id4", "id5"]
    job = AlphaThreadJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, onchain=onchain,
                         prices=prices, notifier=notifier)
    job.run()
    notifier.notify_success.assert_called_once()
    args = notifier.notify_success.call_args[0]
    assert args[0] == "AlphaThread"
    assert args[1] == ["id1", "id2", "id3", "id4", "id5"]


# ── PriceAlertJob ────────────────────────────────────────────────────────────

def test_price_alert_triggers_on_big_move(store, generator, poster, prices):
    prices.get_prices_with_1h_change.return_value = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 95000,
         "price_change_percentage_1h_in_currency": 7.5,
         "price_change_percentage_24h_in_currency": 3.0,
         "total_volume": 500000000},
    ]
    prices.get_ohlc.return_value = None  # skip chart
    job = PriceAlertJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is not None
    assert len(result) == 1
    poster.post_tweet.assert_called_once()
    store.mark_posted.assert_called_once()


def test_price_alert_skips_small_move(store, generator, poster, prices):
    prices.get_prices_with_1h_change.return_value = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 95000,
         "price_change_percentage_1h_in_currency": 1.9,
         "price_change_percentage_24h_in_currency": 1.0,
         "total_volume": 500000000},
    ]
    job = PriceAlertJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()


def test_price_alert_skips_low_volume(store, generator, poster, prices):
    prices.get_prices_with_1h_change.return_value = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 95000,
         "price_change_percentage_1h_in_currency": 8.0,
         "price_change_percentage_24h_in_currency": 3.0,
         "total_volume": 50000},  # low volume
    ]
    job = PriceAlertJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None


def test_price_alert_respects_cooldown(store, generator, poster, prices):
    prices.get_prices_with_1h_change.return_value = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 95000,
         "price_change_percentage_1h_in_currency": 7.5,
         "price_change_percentage_24h_in_currency": 3.0,
         "total_volume": 500000000},
    ]
    # Cooldown active for this coin
    store.topic_on_cooldown.side_effect = lambda topic, cooldown_hours=4: topic == "price_alert_bitcoin"
    job = PriceAlertJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_tweet.assert_not_called()


def test_price_alert_returns_none_on_empty_markets(store, generator, poster, prices):
    prices.get_prices_with_1h_change.return_value = []
    job = PriceAlertJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None


def test_price_alert_respects_daily_cap(store, generator, poster, prices):
    """Price alert should stop when daily cap is reached."""
    from bot.jobs.price_alert import MAX_DAILY_ALERTS
    store.count_posts_since_midnight.return_value = MAX_DAILY_ALERTS
    prices.get_prices_with_1h_change.return_value = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 100000,
         "price_change_percentage_1h_in_currency": 8.0,
         "price_change_percentage_24h_in_currency": 10.0,
         "total_volume": 500_000_000},
    ]
    job = PriceAlertJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None
    generator.generate_tweet.assert_not_called()


def test_price_alert_includes_exchange_and_whale_flows(store, generator, poster, prices):
    """PriceAlertJob should include exchange flow and whale data in context."""
    prices.get_prices_with_1h_change.return_value = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 95000,
         "price_change_percentage_1h_in_currency": 7.5,
         "price_change_percentage_24h_in_currency": 3.0,
         "total_volume": 500_000_000},
    ]
    prices.get_ohlc.return_value = None
    exchange_flows = MagicMock()
    exchange_flows.get_exchange_snapshot.return_value = {"balance": []}
    exchange_flows.format_summary.return_value = "24h net outflow: 3,000 BTC (bullish)"
    whale_alerts = MagicMock()
    whale_alerts.get_whale_snapshot.return_value = {"count": 2}
    whale_alerts.format_summary.return_value = "Whale alerts (1h): 2 txs, $80M moved"
    job = PriceAlertJob(store=store, generator=generator, poster=poster,
                        prices=prices, exchange_flows=exchange_flows, whale_alerts=whale_alerts)
    job.execute()
    ctx = generator.generate_tweet.call_args[0][0]
    assert "Exchange flows" in ctx
    assert "Whale" in ctx


def test_price_alert_works_without_flow_data(store, generator, poster, prices):
    """PriceAlertJob should work fine when exchange_flows and whale_alerts are None."""
    prices.get_prices_with_1h_change.return_value = [
        {"id": "bitcoin", "symbol": "btc", "current_price": 95000,
         "price_change_percentage_1h_in_currency": 7.5,
         "price_change_percentage_24h_in_currency": 3.0,
         "total_volume": 500_000_000},
    ]
    prices.get_ohlc.return_value = None
    job = PriceAlertJob(store=store, generator=generator, poster=poster,
                        prices=prices, exchange_flows=None, whale_alerts=None)
    result = job.execute()
    assert result is not None


# ── WeeklyPollJob ────────────────────────────────────────────────────────────

def test_weekly_poll_posts_poll(store, generator, poster, prices):
    prices.get_weekly_changes.return_value = [
        {"price_change_percentage_7d_in_currency": 3.5},
    ]
    generator.generate_tweet.return_value = (
        "Where is $BTC heading next week?\n"
        "A: Moon above $100k\n"
        "B: Crab at $95k\n"
        "C: Dip to $90k\n"
        "D: Crash below $85k"
    )
    poster.post_poll.return_value = "poll_id_1"
    job = WeeklyPollJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result == "poll_id_1"
    poster.post_poll.assert_called_once()
    call_args = poster.post_poll.call_args
    assert len(call_args[0][1]) == 4  # 4 options


def test_weekly_poll_skips_on_cooldown(store, generator, poster, prices):
    store.topic_on_cooldown.return_value = True
    job = WeeklyPollJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None
    poster.post_poll.assert_not_called()


def test_weekly_poll_handles_bad_ai_output(store, generator, poster, prices):
    prices.get_weekly_changes.return_value = []
    generator.generate_tweet.return_value = "Just a plain tweet with no options"
    job = WeeklyPollJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None  # failed to parse poll format


# ── WeeklyRecapJob ───────────────────────────────────────────────────────────

def test_weekly_recap_posts_tweet(store, generator, poster, prices):
    prices.get_weekly_changes.return_value = [
        {"symbol": "btc", "current_price": 95000, "price_change_percentage_7d_in_currency": 5.2},
        {"symbol": "eth", "current_price": 2700, "price_change_percentage_7d_in_currency": -1.3},
    ]
    job = WeeklyRecapJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result == "tweet_id_123"
    poster.post_tweet.assert_called_once()


def test_weekly_recap_skips_on_cooldown(store, generator, poster, prices):
    store.topic_on_cooldown.return_value = True
    job = WeeklyRecapJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None


def test_weekly_recap_skips_when_no_data(store, generator, poster, prices):
    prices.get_weekly_changes.return_value = []
    job = WeeklyRecapJob(store=store, generator=generator, poster=poster, prices=prices)
    result = job.execute()
    assert result is None


# ── MetricsCollectorJob ──────────────────────────────────────────────────────

def test_metrics_collector_updates_metrics():
    store = MagicMock()
    engager = MagicMock()
    store.get_recent_original_tweet_ids.return_value = [
        {"tweet_id": "t1", "job_type": "morning_brief"},
        {"tweet_id": "t2", "job_type": "hot_take"},
    ]
    engager.get_tweet_metrics.return_value = {"impressions": 100, "likes": 5}

    job = MetricsCollectorJob(store=store, engager=engager)
    job.execute()
    assert engager.get_tweet_metrics.call_count == 2
    assert store.upsert_tweet_metrics.call_count == 2


def test_metrics_collector_skips_dry_run_ids():
    store = MagicMock()
    engager = MagicMock()
    store.get_recent_original_tweet_ids.return_value = [
        {"tweet_id": "dry_12345", "job_type": "morning_brief"},
        {"tweet_id": "real_t1", "job_type": "hot_take"},
        {"tweet_id": "real_t2", "job_type": "self_followup"},
    ]
    engager.get_tweet_metrics.return_value = {"impressions": 100}

    job = MetricsCollectorJob(store=store, engager=engager)
    job.execute()
    assert engager.get_tweet_metrics.call_count == 2  # real_t1 + real_t2, skips dry_
    engager.get_tweet_metrics.assert_any_call("real_t1")
    engager.get_tweet_metrics.assert_any_call("real_t2")


def test_metrics_collector_handles_no_recent_tweets():
    store = MagicMock()
    engager = MagicMock()
    store.get_recent_original_tweet_ids.return_value = []

    job = MetricsCollectorJob(store=store, engager=engager)
    job.execute()
    engager.get_tweet_metrics.assert_not_called()


# ── V3: MetricsCollector per-tweet error handling ────────────────────────────

def test_metrics_collector_continues_on_per_tweet_error():
    """One tweet's metric fetch failing should not prevent others from being collected."""
    store = MagicMock()
    engager = MagicMock()
    store.get_recent_original_tweet_ids.return_value = [
        {"tweet_id": "t1", "job_type": "morning_brief"},
        {"tweet_id": "t2", "job_type": "hot_take"},
        {"tweet_id": "t3", "job_type": "evening_wrap"},
    ]
    # t1 succeeds, t2 throws, t3 succeeds
    engager.get_tweet_metrics.side_effect = [
        {"impressions": 100, "likes": 5, "retweets": 1, "replies": 0, "quotes": 0, "bookmarks": 0},
        Exception("Twitter 500"),
        {"impressions": 200, "likes": 10, "retweets": 2, "replies": 1, "quotes": 0, "bookmarks": 1},
    ]

    job = MetricsCollectorJob(store=store, engager=engager)
    job.execute()
    assert engager.get_tweet_metrics.call_count == 3  # all three attempted
    assert store.upsert_tweet_metrics.call_count == 2  # t1 + t3 upserted


# ── V3: Trends integration in content jobs ────────────────────────────────────

def test_hot_take_picks_second_market_when_first_on_cooldown(store, generator, poster):
    """Should skip cooldown markets and pick the next one."""
    poly = _make_polymarket_mock()
    # First question on cooldown, second not
    store.topic_on_cooldown.side_effect = lambda key, **kw: "Fed cut" in key
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    with patch("bot.jobs.hot_take.datetime") as mock_dt:
        mock_dt.datetime.now.return_value.weekday.return_value = 0  # text day
        result = job.execute()
    assert result == "tweet_id_123"
    posted_text = poster.post_tweet.call_args[0][0]
    assert "Bitcoin" in posted_text or "$150K" in posted_text


def test_morning_brief_includes_trends(store, generator, poster, news, prices, summarizer):
    trends = MagicMock()
    trends.fetch_all.return_value = [{"name": "#Ethereum", "volume": 90000}]
    trends.format_summary.return_value = "Currently trending on Twitter: #Ethereum (90K tweets)"
    trends.get_trending_hashtags.return_value = ["#Ethereum"]

    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices, trends=trends)
    result = job.execute()
    assert result == "tweet_id_123"
    call_context = generator.generate_tweet.call_args[0][0]
    assert "#Ethereum" in call_context


def test_us_open_includes_trends(store, generator, poster, news, prices, summarizer):
    trends = MagicMock()
    trends.fetch_all.return_value = [{"name": "Fed rate", "volume": 80000}]
    trends.format_summary.return_value = "Currently trending on Twitter: Fed rate (80K tweets)"
    trends.get_trending_hashtags.return_value = []

    job = USOpenJob(store=store, generator=generator, poster=poster,
                    summarizer=summarizer, news=news, prices=prices, trends=trends)
    result = job.execute()
    assert result == "tweet_id_123"
    call_context = generator.generate_tweet.call_args[0][0]
    assert "Fed rate" in call_context


def test_evening_wrap_includes_trends(store, generator, poster, news, prices, summarizer):
    trends = MagicMock()
    trends.fetch_all.return_value = [{"name": "#Bitcoin", "volume": 150000}]
    trends.format_summary.return_value = "Currently trending on Twitter: #Bitcoin (150K tweets)"
    trends.get_trending_hashtags.return_value = ["#Bitcoin"]

    job = EveningWrapJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices, trends=trends)
    result = job.execute()
    assert result == "tweet_id_123"
    call_kwargs = generator.generate_tweet.call_args[1]
    assert call_kwargs["trending_tags"] == ["#Bitcoin"]


def test_hot_take_does_not_use_ai_generator(store, generator, poster):
    """Hot Take posts Polymarket question directly — no AI generation needed."""
    poly = _make_polymarket_mock()
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    with patch("bot.jobs.hot_take.datetime") as mock_dt:
        mock_dt.datetime.now.return_value.weekday.return_value = 0  # text day
        job.execute()
    # generator.generate_tweet should NOT be called — we post the raw question
    generator.generate_tweet.assert_not_called()


# ── Flow Data Integration Tests ───────────────────────────────────────────────


def test_morning_brief_includes_etf_flows(store, generator, poster, news, prices, summarizer):
    etf_flows = MagicMock()
    etf_flows.get_etf_snapshot.return_value = {"totalFlow": 150_000_000}
    etf_flows.format_summary.return_value = "BTC ETF net flow: +$150.0M"
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices,
                          etf_flows=etf_flows)
    job.execute()
    call_context = generator.generate_tweet.call_args[0][0]
    assert "ETF flows" in call_context
    assert "+$150.0M" in call_context


def test_morning_brief_includes_exchange_flows(store, generator, poster, news, prices, summarizer):
    exchange_flows = MagicMock()
    exchange_flows.get_exchange_snapshot_multi.return_value = {"btc": {"balance": {"totalBalance": 2_100_000}}, "eth": {}}
    exchange_flows.format_multi_summary.return_value = "Exchange BTC balance: 2,100,000 BTC"
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices,
                          exchange_flows=exchange_flows)
    job.execute()
    call_context = generator.generate_tweet.call_args[0][0]
    assert "Exchange flows" in call_context


def test_morning_brief_works_without_flow_data(store, generator, poster, news, prices, summarizer):
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices,
                          etf_flows=None, exchange_flows=None)
    result = job.execute()
    assert result == "tweet_id_123"


def test_hot_take_question_ends_with_question_mark(store, generator, poster):
    """Ensure posted text always ends with ?"""
    poly = MagicMock()
    poly.get_trending_markets.return_value = [
        {"question": "Bitcoin above 200K by December", "yes_prob": 0.08, "volume_24h": 500_000},
    ]
    job = HotTakeJob(store=store, generator=generator, poster=poster, polymarket=poly)
    with patch("bot.jobs.hot_take.datetime") as mock_dt:
        mock_dt.datetime.now.return_value.weekday.return_value = 0  # text day
        job.execute()
    posted_text = poster.post_tweet.call_args[0][0]
    assert posted_text.endswith("?")


def test_evening_wrap_includes_etf_flows(store, generator, poster, news, prices, summarizer):
    etf_flows = MagicMock()
    etf_flows.get_etf_snapshot.return_value = {"totalFlow": -80_000_000}
    etf_flows.format_summary.return_value = "BTC ETF net flow: -$80.0M"
    job = EveningWrapJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, prices=prices,
                         etf_flows=etf_flows)
    job.execute()
    call_context = generator.generate_tweet.call_args[0][0]
    assert "Daily ETF scorecard" in call_context
    assert "-$80.0M" in call_context


def test_evening_wrap_works_without_etf_flows(store, generator, poster, news, prices, summarizer):
    job = EveningWrapJob(store=store, generator=generator, poster=poster,
                         summarizer=summarizer, news=news, prices=prices,
                         etf_flows=None)
    result = job.execute()
    assert result == "tweet_id_123"


def test_onchain_includes_exchange_flows(store, generator, poster, onchain):
    exchange_flows = MagicMock()
    exchange_flows.get_exchange_snapshot_multi.return_value = {"btc": {"balance": []}, "eth": {"balance": []}}
    exchange_flows.format_multi_summary.return_value = "24h net outflow: 3,000 BTC (bullish)"
    job = OnChainSignalJob(store=store, generator=generator, poster=poster,
                           onchain=onchain, exchange_flows=exchange_flows)
    job.execute()
    call_context = generator.generate_tweet.call_args[0][0]
    assert "Exchange flows" in call_context


def test_onchain_works_without_exchange_flows(store, generator, poster, onchain):
    job = OnChainSignalJob(store=store, generator=generator, poster=poster,
                           onchain=onchain, exchange_flows=None)
    result = job.execute()
    assert result == "tweet_id_123"


# ── V4: mini-thread mode (Mon/Wed/Fri) ──────────────────────────────────────

@patch("bot.jobs.base.BaseJob._check_topic_overlap", return_value=[])
@patch("bot.jobs.base.BaseJob._mark_posted_with_topics")
def test_morning_brief_thread_on_monday(mock_mark, mock_overlap, store, generator, poster, news, prices, summarizer):
    """On Monday (weekday=0), morning_brief should try mini-thread."""
    generator.generate_mini_thread = MagicMock(
        return_value=["1/ Headline", "2/ Detail", "3/ Take"]
    )
    poster.post_thread.return_value = ["t1", "t2", "t3"]
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices)
    with patch.object(job, "_should_use_thread", return_value=True):
        result = job.execute()
    assert isinstance(result, list)
    assert len(result) == 3
    poster.post_thread.assert_called_once()


@patch("bot.jobs.base.BaseJob._check_topic_overlap", return_value=[])
@patch("bot.jobs.base.BaseJob._mark_posted_with_topics")
def test_morning_brief_single_on_tuesday(mock_mark, mock_overlap, store, generator, poster, news, prices, summarizer):
    """On Tuesday (weekday=1), morning_brief should use single tweet."""
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices)
    with patch.object(job, "_should_use_thread", return_value=False):
        result = job.execute()
    assert result == "tweet_id_123"
    poster.post_tweet.assert_called_once()


@patch("bot.jobs.base.BaseJob._check_topic_overlap", return_value=[])
@patch("bot.jobs.base.BaseJob._mark_posted_with_topics")
def test_morning_brief_thread_fallback_on_failure(mock_mark, mock_overlap, store, generator, poster, news, prices, summarizer):
    """If mini-thread generation fails, should fall back to single tweet."""
    generator.generate_mini_thread = MagicMock(side_effect=Exception("API error"))
    job = MorningBriefJob(store=store, generator=generator, poster=poster,
                          summarizer=summarizer, news=news, prices=prices)
    with patch.object(job, "_should_use_thread", return_value=True):
        result = job.execute()
    assert result == "tweet_id_123"
    poster.post_tweet.assert_called_once()
