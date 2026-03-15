# tests/test_telegram.py
import pytest
from unittest.mock import patch, MagicMock
from bot.notifications.telegram import TelegramNotifier


@pytest.fixture
def store():
    """Mock store that the notifier uses for previews and progress."""
    s = MagicMock()
    s.get_content_by_id.return_value = None
    s.count_posts_since_midnight_batch.return_value = {}
    return s


@pytest.fixture
def notifier(store):
    return TelegramNotifier(bot_token="test_token", chat_id="123456", store=store)


@pytest.fixture
def notifier_no_store():
    return TelegramNotifier(bot_token="test_token", chat_id="123456")


def test_send_success(notifier):
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    with patch("bot.notifications.telegram._get_session") as mock_get:
        mock_session = MagicMock()
        mock_session.post.return_value = mock_resp
        mock_get.return_value = mock_session
        result = notifier.send("Hello")
    assert result is True
    mock_session.post.assert_called_once()
    call_kwargs = mock_session.post.call_args.kwargs
    assert call_kwargs["json"]["chat_id"] == "123456"
    assert call_kwargs["json"]["text"] == "Hello"


def test_send_returns_false_on_network_error(notifier):
    with patch("bot.notifications.telegram._get_session") as mock_get:
        mock_session = MagicMock()
        mock_session.post.side_effect = Exception("timeout")
        mock_get.return_value = mock_session
        result = notifier.send("Hello")
    assert result is False


def test_notify_content_success(notifier, store):
    """Content job shows column name, preview from store, and tweet link."""
    store.get_content_by_id.return_value = "BTC tests $97k as ETH stagnates"
    store.count_posts_since_midnight_batch.return_value = {"morning_brief": 1}
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("MorningBrief", "12345")
    mock_send.assert_called_once()
    msg = mock_send.call_args[0][0]
    assert "The Open" in msg
    assert "BTC tests" in msg
    assert "x.com/i/status/12345" in msg


def test_notify_content_with_progress(notifier, store):
    """Content progress counter shows X/N (dynamic denominator)."""
    store.count_posts_since_midnight_batch.return_value = {
        "morning_brief": 1, "hot_take": 1
    }
    with patch.object(notifier, "_content_progress", return_value="2/6"):
        with patch.object(notifier, "send") as mock_send:
            notifier.notify_success("HotTake", "222")
    msg = mock_send.call_args[0][0]
    assert "Signal Flare" in msg
    assert "[2/6]" in msg


def test_notify_thread_success(notifier, store):
    """Thread notification shows thread length and link."""
    store.get_content_by_id.return_value = "1/ First tweet of the thread"
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("AlphaThread", ["t1", "t2", "t3"])
    msg = mock_send.call_args[0][0]
    assert "Deep Dive" in msg
    assert "3 tweets" in msg
    assert "x.com/i/status/t1" in msg


def test_notify_engagement_success(notifier, store):
    """Engagement notification shows reply/quote counts."""
    store.count_posts_since_midnight_batch.return_value = {
        "reply": 8, "quote_tweet": 3
    }
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("Engagement", ["reply_111", "reply_222", "quote_333"])
    msg = mock_send.call_args[0][0]
    assert "Engagement" in msg
    assert "+2" in msg  # 2 replies this run
    assert "+1" in msg  # 1 quote this run
    assert "8/20" in msg  # total replies today
    assert "3/10" in msg  # total quotes today


def test_notify_engagement_silent_when_empty(notifier):
    """Engagement with no posted IDs stays silent."""
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("Engagement", [])
    mock_send.assert_not_called()


def test_notify_self_reply_success(notifier):
    """Self reply notification shows count."""
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("SelfReply", ["sr_1", "sr_2"])
    msg = mock_send.call_args[0][0]
    assert "Self Reply" in msg
    assert "2 comments" in msg


def test_notify_failure_sends_error(notifier):
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_failure("HotTake", ConnectionError("timeout"))
    mock_send.assert_called_once()
    msg = mock_send.call_args[0][0]
    assert "Signal Flare" in msg
    assert "❌" in msg


def test_notify_content_no_store(notifier_no_store):
    """Without store, still shows column name and link."""
    with patch.object(notifier_no_store, "send") as mock_send:
        notifier_no_store.notify_success("MorningBrief", "12345")
    msg = mock_send.call_args[0][0]
    assert "The Open" in msg
    assert "x.com/i/status/12345" in msg


def test_notify_dryrun_skips_link(notifier):
    """Dry-run IDs don't generate tweet links."""
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("MorningBrief", "dry_run_12345")
    msg = mock_send.call_args[0][0]
    assert "x.com" not in msg


def test_daily_report(notifier):
    """Daily report shows schedule comparison."""
    counts = {
        "morning_brief": 1,
        "hot_take": 2,
        "us_open": 0,
        "evening_wrap": 1,
        "reply": 10,
        "quote_tweet": 3,
        "self_reply": 2,
        "self_followup": 3,
        "trend_alert": 2,
        "price_alert": 1,
    }
    with patch.object(notifier, "send") as mock_send:
        notifier.send_daily_report(counts)
    msg = mock_send.call_args[0][0]
    assert "Daily Report" in msg
    assert "✅ The Open" in msg
    assert "❌ Wall St. Cross" in msg
    assert "10/20" in msg
    assert "3/10" in msg
    assert "📈 Trend Alert ×2" in msg
    assert "🚨 Price Alert ×1" in msg


def test_daily_report_with_best_tweet(notifier):
    """Daily report includes best tweet section."""
    counts = {"morning_brief": 1, "hot_take": 1, "reply": 5}
    best = {
        "impressions": 2100,
        "likes": 45,
        "retweets": 8,
        "content": "BTC funding rate flips negative for the first time in 3 months"
    }
    with patch.object(notifier, "send") as mock_send:
        notifier.send_daily_report(counts, best_tweet=best)
    msg = mock_send.call_args[0][0]
    assert "Best tweet" in msg
    assert "2,100" in msg
    assert "45 likes" in msg
    assert "BTC funding" in msg


def test_notify_price_alert(notifier):
    """Price alert notification shows count and links."""
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("PriceAlert", ["alert_1", "alert_2"])
    msg = mock_send.call_args[0][0]
    assert "Price Alert" in msg
    assert "2 alerts" in msg
    assert "x.com/i/status/alert_1" in msg


def test_notify_price_alert_silent_when_empty(notifier):
    """Price alert with no IDs stays silent."""
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("PriceAlert", [])
    mock_send.assert_not_called()


def test_notify_weekly_poll(notifier, store):
    """Weekly poll goes through content notification path."""
    store.get_content_by_id.return_value = "Where is $BTC heading?"
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("WeeklyPoll", "poll_123")
    msg = mock_send.call_args[0][0]
    assert "Weekly Poll" in msg


def test_notify_weekly_recap(notifier, store):
    """Weekly recap goes through content notification path."""
    store.get_content_by_id.return_value = "Weekly scorecard: $BTC +5%"
    with patch.object(notifier, "send") as mock_send:
        notifier.notify_success("WeeklyRecap", "recap_123")
    msg = mock_send.call_args[0][0]
    assert "Weekly Recap" in msg


# ── V2.5: enhanced daily report with performance data ─────────────────────────

def test_daily_report_with_performance_data(notifier, store):
    """Daily report should include per-type performance section when data available."""
    counts = {"morning_brief": 1, "hot_take": 2, "reply": 10, "quote_tweet": 3}
    perf = {
        "morning_brief": {"count": 1, "avg_impressions": 2500, "avg_likes": 80, "avg_eng_score": 350},
        "hot_take": {"count": 2, "avg_impressions": 1800, "avg_likes": 50, "avg_eng_score": 220},
    }
    with patch.object(notifier, "send") as mock_send:
        notifier.send_daily_report(counts, perf_by_type=perf)
    msg = mock_send.call_args[0][0]
    assert "Performance" in msg
    assert "The Open" in msg  # morning_brief column name
    assert "views" in msg
    assert "eng" in msg
