# tests/test_generate.py
import pytest
import anthropic
from unittest.mock import patch, MagicMock
from bot.ai.generate import TweetGenerator, SYSTEM_PROMPT, _twitter_len

PERSONA = SYSTEM_PROMPT


@pytest.fixture
def generator():
    return TweetGenerator(api_key="test_key", persona=PERSONA)


def test_generate_single_tweet(generator):
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="BTC exchange outflows hit 6-month high. Smart money leaving CEXs. Bullish signal.")]
    )
    with patch.object(generator, "client", mock_client):
        tweet = generator.generate_tweet(
            context="BTC exchange outflows at 6-month high",
            tweet_type="hot_take",
            recent_tweets=[]
        )
    assert len(tweet) <= 280
    assert len(tweet) > 0


def test_generate_thread(generator):
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="1/ BTC on-chain signal thread\n2/ Exchange outflows rising\n3/ Historically bullish\n4/ Watch $95k resistance")]
    )
    with patch.object(generator, "client", mock_client):
        tweets = generator.generate_thread(
            context="Weekly on-chain analysis",
            num_tweets=4
        )
    assert isinstance(tweets, list)
    assert len(tweets) >= 1


def test_tweet_not_over_280_chars(generator):
    mock_client = MagicMock()
    long_text = "A" * 300
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text=long_text)]
    )
    with patch.object(generator, "client", mock_client):
        tweet = generator.generate_tweet("context", "hot_take", [])
    assert _twitter_len(tweet) <= 280


def test_unknown_tweet_type_uses_default(generator):
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="Some tweet content here")]
    )
    with patch.object(generator, "client", mock_client):
        tweet = generator.generate_tweet("context", "unknown_type", [])
    assert len(tweet) > 0


def test_reply_truncated_to_240_chars(generator):
    mock_client = MagicMock()
    long_text = "A " * 200  # 400 chars
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text=long_text)]
    )
    with patch.object(generator, "client", mock_client):
        tweet = generator.generate_tweet("context", "reply", [])
    assert _twitter_len(tweet) <= 240


def test_generate_thread_returns_empty_on_no_numbered_lines(generator):
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="Some unnumbered content\nwithout slashes")]
    )
    with patch.object(generator, "client", mock_client):
        tweets = generator.generate_thread("context", num_tweets=5)
    assert tweets == []


def test_generate_tweet_returns_empty_on_empty_content(generator):
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(content=[])
    with patch.object(generator, "client", mock_client):
        tweet = generator.generate_tweet("context", "hot_take", [])
    assert tweet == ""


def test_generate_thread_captures_multiline_tweet(generator):
    """Tweets that span two lines (e.g. LLM adds a line break mid-tweet) must be
    captured as one chunk, not silently truncated to the first line only."""
    multi_line_raw = (
        "1/ BTC exchange outflows hit 6-month high.\n"
        "Smart money is clearly rotating off CEXs.\n"
        "2/ ETH staking yield compressing — not bullish.\n"
        "3/ Fed dot-plot unchanged; macro headwinds persist.\n"
        "4/ On-chain: miner reserves stable, no capitulation.\n"
        "5/ Net thesis: range-bound Q1, accumulate dips."
    )
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text=multi_line_raw)]
    )
    with patch.object(generator, "client", mock_client):
        tweets = generator.generate_thread("context", num_tweets=5)
    assert len(tweets) == 5
    # Tweet 1 should contain BOTH lines joined
    assert "Smart money" in tweets[0]


def test_generate_tweet_raises_ai_rate_limit_on_rate_limit(generator):
    """RateLimitError should raise AIRateLimitError so BaseJob.run() can notify."""
    from bot.ai.generate import AIRateLimitError
    mock_client = MagicMock()
    mock_client.messages.create.side_effect = anthropic.RateLimitError(
        message="rate limited",
        response=MagicMock(status_code=429, headers={}),
        body=None,
    )
    with patch.object(generator, "client", mock_client):
        with pytest.raises(AIRateLimitError):
            generator.generate_tweet("context", "hot_take", [])


def test_generate_tweet_raises_on_unexpected_error(generator):
    """Non-transient errors should propagate so BaseJob.run() can notify."""
    mock_client = MagicMock()
    mock_client.messages.create.side_effect = Exception("connection error")
    with patch.object(generator, "client", mock_client):
        with pytest.raises(Exception, match="connection error"):
            generator.generate_tweet("context", "hot_take", [])


def test_generate_thread_raises_ai_rate_limit_on_rate_limit(generator):
    """RateLimitError should raise AIRateLimitError so BaseJob.run() can notify."""
    from bot.ai.generate import AIRateLimitError
    mock_client = MagicMock()
    mock_client.messages.create.side_effect = anthropic.RateLimitError(
        message="rate limited",
        response=MagicMock(status_code=429, headers={}),
        body=None,
    )
    with patch.object(generator, "client", mock_client):
        with pytest.raises(AIRateLimitError):
            generator.generate_thread("context", num_tweets=5)


def test_generate_thread_raises_on_unexpected_error(generator):
    """Non-transient errors should propagate."""
    mock_client = MagicMock()
    mock_client.messages.create.side_effect = Exception("timeout")
    with patch.object(generator, "client", mock_client):
        with pytest.raises(Exception, match="timeout"):
            generator.generate_thread("context", num_tweets=5)


# ── V3: negative feedback injection ──────────────────────────────────────────

def test_generate_tweet_includes_negative_feedback(generator):
    """Bottom performers should appear in the prompt as 'avoid this style'."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="Fresh take on $BTC at $97k")]
    )
    bottom = [
        {"impressions": 3000, "likes": 2, "retweets": 0, "content": "Generic boring tweet"},
        {"impressions": 2000, "likes": 1, "retweets": 0, "content": "Another bad one"},
    ]
    with patch.object(generator, "client", mock_client):
        tweet = generator.generate_tweet("context", "hot_take", [], bottom_performers=bottom)
    assert len(tweet) > 0
    # Verify the prompt contained negative feedback
    prompt = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "worst performers" in prompt
    assert "Generic boring" in prompt


# ── V3: trending tags injection ───────────────────────────────────────────────

def test_generate_tweet_includes_trending_tags_in_prompt(generator):
    """Trending tags should appear in the prompt when provided."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="$BTC bouncing off support #Bitcoin")]
    )
    with patch.object(generator, "client", mock_client):
        tweet = generator.generate_tweet(
            "BTC at $95k", "hot_take", [],
            trending_tags=["#Bitcoin", "#CryptoMarket"]
        )
    assert len(tweet) > 0
    prompt = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "Currently trending hashtags:" in prompt
    assert "#Bitcoin" in prompt
    assert "#CryptoMarket" in prompt
    assert "weave one in naturally" in prompt


# ── V4: performance patterns injection ────────────────────────────────────────

# ── V4: mini-thread generation ────────────────────────────────────────────────

def test_generate_mini_thread_basic(generator):
    """generate_mini_thread should return a list of tweets."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="1/ BTC overnight action was wild.\n2/ ETF inflows hit $300M.\n3/ Shorts getting squeezed above 97k.")]
    )
    with patch.object(generator, "client", mock_client):
        tweets = generator.generate_mini_thread("context", "morning_brief", num_tweets=3)
    assert isinstance(tweets, list)
    assert len(tweets) == 3


def test_generate_mini_thread_with_feedback(generator):
    """Feedback (patterns, top/bottom) should appear in the prompt."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="1/ Thread tweet one\n2/ Thread tweet two\n3/ Thread tweet three")]
    )
    patterns = {"insights": ["Short tweets work better"]}
    top = [{"impressions": 5000, "likes": 100, "content": "Great tweet"}]
    with patch.object(generator, "client", mock_client):
        tweets = generator.generate_mini_thread(
            "context", "morning_brief",
            performance_patterns=patterns, top_performers=top,
        )
    assert len(tweets) == 3
    prompt = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "Short tweets work better" in prompt
    assert "Great tweet" in prompt


def test_generate_mini_thread_truncates_long_tweets(generator):
    """Each tweet in the thread should be truncated to 280 chars."""
    mock_client = MagicMock()
    long_tweet = "1/ " + "A" * 300 + "\n2/ Short tweet\n3/ Another"
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text=long_tweet)]
    )
    with patch.object(generator, "client", mock_client):
        tweets = generator.generate_mini_thread("context", "morning_brief")
    for t in tweets:
        assert len(t) <= 280


def test_generate_mini_thread_sets_variant(generator):
    """Mini-thread should set _last_variant with 'thread_' prefix."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="1/ One\n2/ Two\n3/ Three")]
    )
    with patch.object(generator, "client", mock_client):
        generator.generate_mini_thread("context", "morning_brief")
    assert generator.last_variant.startswith("thread_")


def test_generate_tweet_tracks_last_variant(generator):
    """After generating, last_variant should reflect the tone used."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="$BTC looks bullish")]
    )
    with patch.object(generator, "client", mock_client):
        generator.generate_tweet("context", "hot_take", [])
    assert generator.last_variant.startswith("style_")


def test_generate_tweet_includes_performance_patterns(generator):
    """Performance patterns should appear in the prompt as data-driven insights."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="$BTC sharp move today")]
    )
    patterns = {
        "top_patterns": {"avg_length": 140},
        "bottom_patterns": {"avg_length": 230},
        "insights": ["Shorter tweets perform better (top avg 140 chars vs bottom avg 230 chars)"],
    }
    with patch.object(generator, "client", mock_client):
        tweet = generator.generate_tweet("context", "hot_take", [],
                                          performance_patterns=patterns)
    assert len(tweet) > 0
    prompt = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "What works for your audience (data-driven):" in prompt
    assert "Shorter tweets perform better" in prompt


def test_tone_weighted_selection_uses_random_choices(generator):
    """Tone selection should use weighted random.choices, not uniform random.choice."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="$BTC breaking out")]
    )
    with patch.object(generator, "client", mock_client), \
         patch("bot.ai.generate.random.choices", return_value=["TONE: Ask a question. State the data, then pose a genuine question to your audience about what it means. Make them think."]) as mock_choices:
        generator.generate_tweet("context", "hot_take", [])
    mock_choices.assert_called_once()
    # Verify weights were passed
    _, kwargs = mock_choices.call_args
    assert "weights" in kwargs
    assert len(kwargs["weights"]) > 0


def test_contrarian_style_exists_in_content_modes():
    """A contrarian style mode should exist in content style pool."""
    from bot.ai.generate import _TONE_MODES
    content_styles = [t for t, _ in _TONE_MODES["content"]]
    assert any("contrarian" in t.lower() for t in content_styles)


def test_generate_tweet_no_trending_tags_no_block(generator):
    """When trending_tags is None or empty, no trending block in prompt."""
    mock_client = MagicMock()
    mock_client.messages.create.return_value = MagicMock(
        content=[MagicMock(text="$BTC looks strong")]
    )
    with patch.object(generator, "client", mock_client):
        generator.generate_tweet("context", "hot_take", [], trending_tags=None)
    prompt = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "Currently trending hashtags:" not in prompt

    with patch.object(generator, "client", mock_client):
        generator.generate_tweet("context", "hot_take", [], trending_tags=[])
    prompt = mock_client.messages.create.call_args.kwargs["messages"][0]["content"]
    assert "Currently trending hashtags:" not in prompt
