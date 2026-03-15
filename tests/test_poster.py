# tests/test_poster.py
import pytest
from unittest.mock import MagicMock
from bot.twitter.poster import Poster, DryRunPoster


def _tweet(tweet_id: str):
    """Return a mock that mimics a tweepy create_tweet Response: data is a dict."""
    return MagicMock(data={"id": tweet_id, "text": "mock"})


@pytest.fixture
def poster():
    mock_client = MagicMock()
    return Poster(client=mock_client)


def test_post_tweet_returns_id(poster):
    poster.client.create_tweet.return_value = _tweet("12345")
    tweet_id = poster.post_tweet("BTC is looking strong here.")
    assert tweet_id == "12345"
    poster.client.create_tweet.assert_called_once_with(text="BTC is looking strong here.")


def test_post_tweet_with_media_ids(poster):
    poster.client.create_tweet.return_value = _tweet("12345")
    tweet_id = poster.post_tweet("BTC chart", media_ids=["media_001"])
    assert tweet_id == "12345"
    poster.client.create_tweet.assert_called_once_with(
        text="BTC chart", media_ids=["media_001"]
    )


def test_post_tweet_without_media_ids_omits_param(poster):
    poster.client.create_tweet.return_value = _tweet("12345")
    poster.post_tweet("No image")
    poster.client.create_tweet.assert_called_once_with(text="No image")


def test_post_thread_chains_replies(poster):
    poster.client.create_tweet.side_effect = [_tweet("100"), _tweet("101"), _tweet("102")]
    ids = poster.post_thread(["Tweet 1/", "Tweet 2/", "Tweet 3/"])
    assert ids == ["100", "101", "102"]
    calls = poster.client.create_tweet.call_args_list
    assert calls[1].kwargs.get("in_reply_to_tweet_id") == "100"  # tweet 2 chains to tweet 1
    assert calls[2].kwargs.get("in_reply_to_tweet_id") == "101"  # tweet 3 chains to tweet 2


def test_post_quote_tweet(poster):
    poster.client.create_tweet.return_value = _tweet("999")
    tweet_id = poster.post_quote_tweet("My commentary", "original_tweet_id_123")
    assert tweet_id == "999"
    poster.client.create_tweet.assert_called_once_with(
        text="My commentary",
        quote_tweet_id="original_tweet_id_123"
    )


def test_post_reply(poster):
    poster.client.create_tweet.return_value = _tweet("777")
    tweet_id = poster.post_reply("Sharp response", "target_tweet_id")
    assert tweet_id == "777"
    poster.client.create_tweet.assert_called_once_with(
        text="Sharp response",
        in_reply_to_tweet_id="target_tweet_id"
    )


def test_post_tweet_raises_on_api_error(poster):
    import tweepy
    poster.client.create_tweet.side_effect = tweepy.errors.TweepyException("rate limit")
    with pytest.raises(tweepy.errors.TweepyException):
        poster.post_tweet("test tweet")


def test_post_thread_partial_failure_returns_partial_ids(poster):
    import tweepy
    poster.client.create_tweet.side_effect = [
        _tweet("100"),
        _tweet("101"),
        tweepy.errors.TweepyException("forbidden"),
    ]
    ids = poster.post_thread(["Tweet 1/", "Tweet 2/", "Tweet 3/"])
    assert ids == ["100", "101"]


def test_post_reply_raises_on_api_error(poster):
    import tweepy
    poster.client.create_tweet.side_effect = tweepy.errors.TweepyException("unauthorized")
    with pytest.raises(tweepy.errors.TweepyException):
        poster.post_reply("reply text", "tweet_id_123")


# ── upload_image_from_url ─────────────────────────────────────────────────────

def test_post_tweet_returns_none_when_resp_data_empty(poster):
    poster.client.create_tweet.return_value = MagicMock(data=None)
    result = poster.post_tweet("some tweet")
    assert result is None


def test_post_quote_tweet_raises_on_api_error(poster):
    import tweepy
    poster.client.create_tweet.side_effect = tweepy.errors.TweepyException("forbidden")
    with pytest.raises(tweepy.errors.TweepyException):
        poster.post_quote_tweet("commentary", "orig_id")


def test_post_thread_stops_when_first_tweet_returns_no_data(poster):
    poster.client.create_tweet.return_value = MagicMock(data=None)
    ids = poster.post_thread(["Tweet 1/", "Tweet 2/"])
    assert ids == []


# ── upload_image_from_url ─────────────────────────────────────────────────────

def test_upload_image_from_url_success(poster, mocker):
    poster._api = MagicMock()
    mock_media = MagicMock()
    mock_media.media_id = 1234567890
    poster._api.media_upload.return_value = mock_media
    mock_resp = MagicMock()
    mock_resp.content = b"fake png bytes"
    mock_resp.raise_for_status = MagicMock()
    mocker.patch("bot.twitter.poster.requests.get", return_value=mock_resp)
    result = poster.upload_image_from_url("https://example.com/image.png")
    assert result == "1234567890"
    poster._api.media_upload.assert_called_once()


def test_upload_image_from_url_no_api(poster):
    poster._api = None
    result = poster.upload_image_from_url("https://example.com/image.png")
    assert result is None


def test_upload_image_from_url_download_fails(poster, mocker):
    poster._api = MagicMock()
    mocker.patch("bot.twitter.poster.requests.get", side_effect=Exception("timeout"))
    result = poster.upload_image_from_url("https://example.com/image.png")
    assert result is None


# ── DryRunPoster ──────────────────────────────────────────────────────────────

def test_dry_run_poster_upload_image_returns_fake_id():
    p = DryRunPoster()
    result = p.upload_image_from_url("https://example.com/image.png")
    assert result == "dry_media_123"


def test_dry_run_poster_post_tweet_with_media():
    p = DryRunPoster()
    result = p.post_tweet("BTC chart", media_ids=["dry_media_123"])
    assert result is not None
    assert result.startswith("dry_")


def test_dry_run_poster_post_tweet_returns_fake_id():
    p = DryRunPoster()
    result = p.post_tweet("BTC looking strong")
    assert result is not None
    assert result.startswith("dry_")


def test_dry_run_poster_post_thread_returns_correct_count():
    p = DryRunPoster()
    ids = p.post_thread(["Tweet 1/3", "Tweet 2/3", "Tweet 3/3"])
    assert len(ids) == 3


def test_dry_run_poster_post_reply_returns_fake_id():
    p = DryRunPoster()
    result = p.post_reply("Great analysis!", "tweet_orig_123")
    assert result is not None
    assert result.startswith("dry_reply_")


# ── post_poll ───────────────────────────────────────────────────────────────

def test_post_poll_success(poster):
    poster.client.create_tweet.return_value = _tweet("poll_123")
    result = poster.post_poll("What's next for $BTC?", ["Moon", "Dump", "Crab", "Uncertain"])
    assert result == "poll_123"
    poster.client.create_tweet.assert_called_once_with(
        text="What's next for $BTC?",
        poll_options=["Moon", "Dump", "Crab", "Uncertain"],
        poll_duration_minutes=1440,
    )


def test_post_poll_custom_duration(poster):
    poster.client.create_tweet.return_value = _tweet("poll_456")
    result = poster.post_poll("Question?", ["Yes", "No"], duration_minutes=60)
    assert result == "poll_456"
    call_kwargs = poster.client.create_tweet.call_args.kwargs
    assert call_kwargs["poll_duration_minutes"] == 60


def test_post_poll_rejects_invalid_options(poster):
    assert poster.post_poll("Q?", []) is None
    assert poster.post_poll("Q?", ["Only one"]) is None
    assert poster.post_poll("Q?", ["A", "B", "C", "D", "E"]) is None


def test_post_poll_raises_on_api_error(poster):
    import tweepy
    poster.client.create_tweet.side_effect = tweepy.errors.TweepyException("forbidden")
    with pytest.raises(tweepy.errors.TweepyException):
        poster.post_poll("Q?", ["A", "B"])


def test_dry_run_poster_post_poll():
    p = DryRunPoster()
    result = p.post_poll("What's next?", ["Up", "Down", "Sideways"])
    assert result is not None
    assert result.startswith("dry_poll_")


# ── V2.5: retry on transient errors ──────────────────────────────────────────

def test_post_tweet_retries_on_server_error(poster, mocker):
    """Transient 5xx should be retried, then succeed."""
    mocker.patch("bot.twitter.poster.time.sleep")
    import tweepy
    poster.client.create_tweet.side_effect = [
        tweepy.errors.TwitterServerError(MagicMock(status_code=503)),
        _tweet("retry_ok"),
    ]
    result = poster.post_tweet("retry test")
    assert result == "retry_ok"
    assert poster.client.create_tweet.call_count == 2


def test_post_tweet_raises_after_max_retries(poster, mocker):
    """After max retries exhausted, should raise."""
    mocker.patch("bot.twitter.poster.time.sleep")
    import tweepy
    poster.client.create_tweet.side_effect = tweepy.errors.TwitterServerError(
        MagicMock(status_code=503)
    )
    with pytest.raises(tweepy.errors.TwitterServerError):
        poster.post_tweet("will fail")
    assert poster.client.create_tweet.call_count == 3  # 1 initial + 2 retries
