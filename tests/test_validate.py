# tests/test_validate.py
import pytest
from bot.ai.validate import extract_numbers, validate_tweet_numbers


# ── extract_numbers ──────────────────────────────────────────────────────────

def test_extract_dollar_millions():
    nums = extract_numbers("BTC ETF net flow: +$150.0M")
    vals = {v for _, v, _ in nums}
    assert 150_000_000 in vals


def test_extract_billions():
    nums = extract_numbers("$1.5B inflow today")
    vals = {v for _, v, _ in nums}
    assert 1_500_000_000 in vals


def test_extract_percentages():
    nums = extract_numbers("BTC up +2.3% today, ETH down -1.5%")
    vals = {v for _, v, _ in nums}
    assert 2.3 in vals
    assert -1.5 in vals


def test_extract_btc_amounts():
    nums = extract_numbers("Exchange BTC balance: 2,100,000 BTC | 24h net outflow: 5,200 BTC")
    vals = {v for _, v, _ in nums}
    assert 2_100_000 in vals
    assert 5_200 in vals


def test_extract_k_suffix():
    nums = extract_numbers("$95K resistance")
    vals = {v for _, v, _ in nums}
    assert 95_000 in vals


def test_extract_skips_trivial_numbers():
    """Thread numbering like '1/', '2/' should be ignored."""
    nums = extract_numbers("1/ BTC up 2/ ETH down 3/ SOL flat")
    vals = {v for _, v, _ in nums}
    assert 1 not in vals
    assert 2 not in vals
    assert 3 not in vals


def test_extract_keeps_percentages_below_threshold():
    """Percentages like 2.3% should be kept even though 2.3 < 10."""
    nums = extract_numbers("+2.3%")
    assert len(nums) > 0
    assert nums[0][2] is True  # is_percentage flag


def test_extract_keeps_meaningful_numbers():
    """Numbers >= 10 should be kept even without suffix."""
    nums = extract_numbers("hashrate: 750 EH/s, 24h volume: 350K TXs")
    vals = {v for _, v, _ in nums}
    assert 750 in vals
    assert 350_000 in vals


# ── validate_tweet_numbers ───────────────────────────────────────────────────

def test_validate_all_traced():
    ctx = "BTC: $95,000 (+2.3%). ETF net flow: +$150.0M"
    tweet = "$BTC at $95K with +$150M ETF inflow, up 2.3%"
    result = validate_tweet_numbers(ctx, tweet)
    assert result["valid"] is True
    assert len(result["untraced"]) == 0


def test_validate_untraced_number():
    ctx = "BTC: $95,000 (+2.3%)"
    tweet = "$BTC at $95K with $500M whale transfer"
    result = validate_tweet_numbers(ctx, tweet)
    assert result["valid"] is False
    assert len(result["untraced"]) > 0


def test_validate_format_conversion_billions():
    """$1,500,000,000 in context should match $1.5B in tweet."""
    ctx = "ETF inflow: $1,500,000,000"
    tweet = "$1.5B BTC ETF inflow — largest since March"
    result = validate_tweet_numbers(ctx, tweet)
    assert result["valid"] is True


def test_validate_empty_tweet():
    ctx = "BTC: $95,000"
    tweet = "Interesting market dynamics today"
    result = validate_tweet_numbers(ctx, tweet)
    assert result["valid"] is True
    assert len(result["tweet_numbers"]) == 0


def test_validate_negative_numbers():
    ctx = "ETF outflow: -$80,500,000"
    tweet = "-$80.5M ETF outflow — bearish signal"
    result = validate_tweet_numbers(ctx, tweet)
    assert result["valid"] is True


def test_validate_percentage_match():
    ctx = "BTC 24h change: +5.7%"
    tweet = "$BTC surged 5.7% in 24h"
    result = validate_tweet_numbers(ctx, tweet)
    assert result["valid"] is True


def test_validate_partial_number_in_context():
    """2.1M in tweet should match 2,100,000 in context via numeric comparison."""
    ctx = "Exchange BTC balance: 2,100,000 BTC"
    tweet = "2.1M BTC sitting on exchanges"
    result = validate_tweet_numbers(ctx, tweet)
    assert result["valid"] is True


def test_validate_multiple_untraced():
    """Multiple fabricated numbers should all appear in untraced set."""
    ctx = "BTC: $95,000"
    tweet = "$BTC at $95K, $200M inflows, $50M whale move"
    result = validate_tweet_numbers(ctx, tweet)
    assert result["valid"] is False
    assert len(result["untraced"]) >= 2
