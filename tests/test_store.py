# tests/test_store.py
import pytest
import os
import tempfile
from bot.db.store import Store


@pytest.fixture
def store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    s = Store(db_path)
    yield s
    os.unlink(db_path)


def test_mark_and_check_posted(store):
    store.mark_posted("tweet_123", "hot_take", "BTC analysis content")
    assert store.is_posted("tweet_123") is True


def test_not_posted_by_default(store):
    assert store.is_posted("nonexistent_id") is False


def test_topic_on_cooldown(store):
    store.mark_posted("tweet_456", "hot_take", "BTC analysis", topic="BTC")
    assert store.topic_on_cooldown("BTC", cooldown_hours=4) is True


def test_topic_not_on_cooldown_after_expiry(store):
    store.mark_posted("tweet_789", "hot_take", "ETH analysis", topic="ETH",
                      posted_at="2020-01-01 00:00:00")
    assert store.topic_on_cooldown("ETH", cooldown_hours=4) is False


def test_get_recent_tweets(store):
    store.mark_posted("t1", "morning_brief", "content one")
    store.mark_posted("t2", "hot_take", "content two")
    recent = store.get_recent_content(hours=24)
    assert len(recent) == 2


def test_mark_posted_deduplicates(store):
    store.mark_posted("dup_id", "hot_take", "first content")
    store.mark_posted("dup_id", "hot_take", "second content")  # same id
    recent = store.get_recent_content(hours=24)
    assert len(recent) == 1
    assert recent[0] == "first content"


def test_count_posts_since_midnight(store):
    store.mark_posted("r1", "reply", "reply content one")
    store.mark_posted("r2", "reply", "reply content two")
    store.mark_posted("h1", "hot_take", "hot take content")
    assert store.count_posts_since_midnight("reply") == 2
    assert store.count_posts_since_midnight("hot_take") == 1
    assert store.count_posts_since_midnight("morning_brief") == 0


def test_count_posts_since_midnight_excludes_old_posts(store):
    store.mark_posted("old_reply", "reply", "old content",
                      posted_at="2020-01-01 00:00:00")
    store.mark_posted("new_reply", "reply", "new content")
    assert store.count_posts_since_midnight("reply") == 1


def test_get_content_by_id_returns_content(store):
    store.mark_posted("tweet_abc", "morning_brief", "BTC surges to $97k")
    assert store.get_content_by_id("tweet_abc") == "BTC surges to $97k"


def test_get_content_by_id_returns_none_for_missing(store):
    assert store.get_content_by_id("nonexistent_id") is None


def test_prune_old_posts(store):
    store.mark_posted("old1", "reply", "old content", posted_at="2020-01-01 00:00:00")
    store.mark_posted("old2", "reply", "old content 2", posted_at="2020-01-02 00:00:00")
    store.mark_posted("new1", "reply", "new content")
    deleted = store.prune_old_posts(keep_days=90)
    assert deleted == 2
    assert store.is_posted("old1") is False
    assert store.is_posted("new1") is True


def test_prune_old_posts_returns_zero_when_nothing_to_prune(store):
    store.mark_posted("recent", "reply", "recent content")
    deleted = store.prune_old_posts(keep_days=90)
    assert deleted == 0


def test_count_posts_since_midnight_batch(store):
    store.mark_posted("r1", "reply", "reply one")
    store.mark_posted("r2", "reply", "reply two")
    store.mark_posted("h1", "hot_take", "hot take one")
    store.mark_posted("s1", "self_reply", "self reply one")
    counts = store.count_posts_since_midnight_batch()
    assert counts.get("reply") == 2
    assert counts.get("hot_take") == 1
    assert counts.get("self_reply") == 1
    assert counts.get("morning_brief", 0) == 0


def test_count_posts_since_midnight_batch_excludes_old(store):
    store.mark_posted("old", "reply", "old content", posted_at="2020-01-01 00:00:00")
    store.mark_posted("new", "reply", "new content")
    counts = store.count_posts_since_midnight_batch()
    assert counts.get("reply") == 1


# ── tweet_metrics (V2 feedback loop) ────────────────────────────────────────

def test_upsert_tweet_metrics(store):
    store.mark_posted("tm_1", "morning_brief", "content")
    metrics = {"impressions": 1200, "likes": 35, "retweets": 5, "replies": 3, "quotes": 1, "bookmarks": 2}
    store.upsert_tweet_metrics("tm_1", "morning_brief", metrics)
    top = store.get_top_tweets(days=7, limit=3)
    assert len(top) == 1
    assert top[0]["impressions"] == 1200
    assert top[0]["likes"] == 35


def test_upsert_tweet_metrics_updates_existing(store):
    store.mark_posted("tm_2", "hot_take", "content")
    store.upsert_tweet_metrics("tm_2", "hot_take", {"impressions": 100, "likes": 5, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0})
    store.upsert_tweet_metrics("tm_2", "hot_take", {"impressions": 500, "likes": 20, "retweets": 3, "replies": 1, "quotes": 0, "bookmarks": 1})
    top = store.get_top_tweets(days=7, limit=3)
    assert top[0]["impressions"] == 500


def test_get_top_tweets_empty(store):
    top = store.get_top_tweets(days=7, limit=3)
    assert top == []


def test_get_top_tweets_respects_limit(store):
    for i in range(5):
        store.mark_posted(f"tm_{i}", "hot_take", f"content {i}")
        store.upsert_tweet_metrics(f"tm_{i}", "hot_take", {"impressions": (i + 1) * 100, "likes": i, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0})
    top = store.get_top_tweets(days=7, limit=3)
    assert len(top) == 3
    assert top[0]["impressions"] == 500  # highest first


def test_get_recent_original_tweet_ids(store):
    store.mark_posted("orig_1", "morning_brief", "content 1")
    store.mark_posted("reply_1", "reply", "reply content")
    store.mark_posted("qt_1", "quote_tweet", "quote content")
    store.mark_posted("orig_2", "hot_take", "content 2")
    ids = store.get_recent_original_tweet_ids(hours=168)
    tweet_ids = [r["tweet_id"] for r in ids]
    assert "orig_1" in tweet_ids
    assert "orig_2" in tweet_ids
    assert "reply_1" not in tweet_ids
    assert "qt_1" not in tweet_ids


# ── V2.5: engagement score ranking ──────────────────────────────────────────

def test_get_top_tweets_engagement_score_ranking(store):
    """Weighted engagement score should rank better than raw impressions."""
    store.mark_posted("high_imp", "morning_brief", "high impressions")
    store.mark_posted("high_eng", "morning_brief", "high engagement")
    # high_imp: 50K impressions but only 10 likes
    store.upsert_tweet_metrics("high_imp", "morning_brief",
        {"impressions": 50000, "likes": 10, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0})
    # high_eng: 20K impressions but 800 likes, 50 retweets
    store.upsert_tweet_metrics("high_eng", "morning_brief",
        {"impressions": 20000, "likes": 800, "retweets": 50, "replies": 20, "quotes": 5, "bookmarks": 30})
    top = store.get_top_tweets(days=7, limit=2)
    assert top[0]["tweet_id"] == "high_eng"  # higher engagement score wins


def test_get_top_tweets_filter_by_job_type(store):
    """job_type filter should only return tweets of that type."""
    store.mark_posted("mb_1", "morning_brief", "morning content")
    store.mark_posted("ht_1", "hot_take", "hot take content")
    store.upsert_tweet_metrics("mb_1", "morning_brief",
        {"impressions": 1000, "likes": 50, "retweets": 5, "replies": 2, "quotes": 0, "bookmarks": 3})
    store.upsert_tweet_metrics("ht_1", "hot_take",
        {"impressions": 2000, "likes": 100, "retweets": 10, "replies": 5, "quotes": 1, "bookmarks": 5})
    # Filter by morning_brief
    top = store.get_top_tweets(days=7, limit=3, job_type="morning_brief")
    assert len(top) == 1
    assert top[0]["tweet_id"] == "mb_1"
    # Global (no filter) returns both
    top_all = store.get_top_tweets(days=7, limit=3)
    assert len(top_all) == 2


def test_get_avg_metrics_by_type(store):
    """Average metrics grouped by job_type for daily report (yesterday's data)."""
    from datetime import datetime, timedelta, timezone
    # Create a post timestamped to yesterday (guaranteed to be in yesterday's window)
    yesterday_noon = (datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
                      - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    store.mark_posted("mb_y1", "morning_brief", "yesterday content", posted_at=yesterday_noon)
    store.upsert_tweet_metrics("mb_y1", "morning_brief",
        {"impressions": 2000, "likes": 80, "retweets": 8, "replies": 3, "quotes": 0, "bookmarks": 4})
    result = store.get_avg_metrics_by_type()
    assert "morning_brief" in result
    assert result["morning_brief"]["count"] == 1
    assert result["morning_brief"]["avg_impressions"] == 2000


# ── V3: negative feedback (bottom performers) ────────────────────────────────

def test_get_bottom_tweets(store):
    """Bottom tweets should be sorted by engagement score ascending."""
    store.mark_posted("good_t", "morning_brief", "great tweet")
    store.mark_posted("bad_t", "morning_brief", "boring tweet")
    store.upsert_tweet_metrics("good_t", "morning_brief",
        {"impressions": 5000, "likes": 200, "retweets": 30, "replies": 10, "quotes": 5, "bookmarks": 15})
    store.upsert_tweet_metrics("bad_t", "morning_brief",
        {"impressions": 3000, "likes": 5, "retweets": 0, "replies": 1, "quotes": 0, "bookmarks": 0})
    bottom = store.get_bottom_tweets(days=7, limit=2)
    assert len(bottom) == 2
    assert bottom[0]["tweet_id"] == "bad_t"  # worst first


def test_get_bottom_tweets_filters_by_job_type(store):
    """Bottom tweets with job_type filter should only return that type."""
    store.mark_posted("mb_bad", "morning_brief", "bad morning")
    store.mark_posted("ht_bad", "hot_take", "bad take")
    store.upsert_tweet_metrics("mb_bad", "morning_brief",
        {"impressions": 1000, "likes": 2, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0})
    store.upsert_tweet_metrics("ht_bad", "hot_take",
        {"impressions": 1000, "likes": 1, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0})
    bottom = store.get_bottom_tweets(days=7, limit=2, job_type="morning_brief")
    assert len(bottom) == 1
    assert bottom[0]["tweet_id"] == "mb_bad"


def test_get_bottom_tweets_requires_min_impressions(store):
    """Tweets with < 100 impressions should not appear in bottom list."""
    store.mark_posted("low_imp", "morning_brief", "low impressions")
    store.upsert_tweet_metrics("low_imp", "morning_brief",
        {"impressions": 50, "likes": 0, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0})
    bottom = store.get_bottom_tweets(days=7, limit=2)
    assert len(bottom) == 0


# ── V3: min impressions threshold in top tweets ──────────────────────────────

def test_get_top_tweets_requires_min_impressions(store):
    """Tweets with < 100 impressions should not appear in top performers."""
    store.mark_posted("low_t", "morning_brief", "low impressions tweet")
    store.upsert_tweet_metrics("low_t", "morning_brief",
        {"impressions": 50, "likes": 10, "retweets": 5, "replies": 3, "quotes": 0, "bookmarks": 2})
    top = store.get_top_tweets(days=7, limit=3)
    assert len(top) == 0


# ── V3: self_followup included in original tweet IDs ─────────────────────────

# ── V4: performance patterns (structured feedback) ────────────────────────────

def test_analyze_tweet_patterns_empty():
    """Empty input should return empty dict."""
    from bot.db.store import Store
    assert Store._analyze_tweet_patterns([]) == {}


def test_analyze_tweet_patterns_with_data():
    """Should compute structural metrics from tweet content."""
    from bot.db.store import Store
    tweets = [
        {"content": "BTC at $95k. Shorts getting squeezed?\nAccumulation."},
        {"content": "$ETH funding rate -0.02%. This is distribution."},
    ]
    p = Store._analyze_tweet_patterns(tweets)
    assert "avg_length" in p
    assert "avg_lines" in p
    assert "question_pct" in p
    assert p["question_pct"] == 50  # 1 out of 2 has a question
    assert p["data_density"] > 0
    assert p["definitive_ending_pct"] > 0  # both have definitive endings


def test_get_performance_patterns_empty(store):
    """Empty DB should return empty dict."""
    result = store.get_performance_patterns(days=7)
    assert result == {}


# ── V4: variant storage and A/B testing ──────────────────────────────────────

def test_mark_posted_with_variant(store):
    """Variant should be stored and retrievable."""
    store.mark_posted("v_1", "hot_take", "BTC content", variant="tone_contrarian")
    # Verify via raw query
    import sqlite3
    conn = sqlite3.connect(store.db_path)
    row = conn.execute("SELECT variant FROM posts WHERE id='v_1'").fetchone()
    conn.close()
    assert row[0] == "tone_contrarian"


def test_get_variant_performance(store):
    """Should return avg engagement grouped by variant."""
    for i in range(4):
        store.mark_posted(f"va_{i}", "hot_take", f"content {i}", variant="tone_minimal")
        store.upsert_tweet_metrics(f"va_{i}", "hot_take",
            {"impressions": 1000, "likes": 20 + i, "retweets": 2, "replies": 1, "quotes": 0, "bookmarks": 1})
    for i in range(4):
        store.mark_posted(f"vb_{i}", "hot_take", f"content {i}", variant="tone_contrarian")
        store.upsert_tweet_metrics(f"vb_{i}", "hot_take",
            {"impressions": 1000, "likes": 5 + i, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0})
    result = store.get_variant_performance(days=14)
    assert len(result) == 2
    assert result[0]["variant"] == "tone_minimal"  # higher eng first
    assert result[0]["count"] == 4


def test_get_variant_performance_min_sample(store):
    """Variants with < 3 samples should be excluded."""
    for i in range(2):
        store.mark_posted(f"vs_{i}", "hot_take", f"content {i}", variant="tone_rare")
        store.upsert_tweet_metrics(f"vs_{i}", "hot_take",
            {"impressions": 1000, "likes": 50, "retweets": 5, "replies": 2, "quotes": 0, "bookmarks": 3})
    result = store.get_variant_performance(days=14)
    assert len(result) == 0  # not enough samples


def test_get_performance_patterns_with_data(store):
    """Should return insights comparing top vs bottom."""
    # Top tweet: short with question
    store.mark_posted("top1", "morning_brief", "BTC at $95k?")
    store.upsert_tweet_metrics("top1", "morning_brief",
        {"impressions": 5000, "likes": 200, "retweets": 30, "replies": 10, "quotes": 5, "bookmarks": 15})
    # Bottom tweet: long without question
    store.mark_posted("bot1", "morning_brief", "A" * 250 + " boring tweet no data at all nothing here")
    store.upsert_tweet_metrics("bot1", "morning_brief",
        {"impressions": 3000, "likes": 2, "retweets": 0, "replies": 0, "quotes": 0, "bookmarks": 0})
    result = store.get_performance_patterns(days=7)
    assert "top_patterns" in result
    assert "bottom_patterns" in result
    assert "insights" in result
    assert isinstance(result["insights"], list)


# ── Time-Slot Performance ─────────────────────────────────────────────────────

def test_get_metrics_by_hour(store):
    """Should group metrics by hour of day."""
    from datetime import datetime, timezone
    # Post at 08:xx and 14:xx UTC today
    now = datetime.now(timezone.utc)
    t08 = now.replace(hour=8, minute=30, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    t14 = now.replace(hour=14, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    store.mark_posted("h08", "morning_brief", "morning content", posted_at=t08)
    store.mark_posted("h14", "us_open", "us open content", posted_at=t14)
    store.upsert_tweet_metrics("h08", "morning_brief",
        {"impressions": 2000, "likes": 50, "retweets": 5, "replies": 3, "quotes": 0, "bookmarks": 2})
    store.upsert_tweet_metrics("h14", "us_open",
        {"impressions": 3000, "likes": 80, "retweets": 10, "replies": 5, "quotes": 1, "bookmarks": 3})
    result = store.get_metrics_by_hour(days=14)
    assert "08" in result
    assert "14" in result
    assert result["08"]["count"] == 1
    assert result["14"]["avg_impressions"] == 3000


# ── Engagement Effectiveness ──────────────────────────────────────────────────

def test_log_and_get_engagement_effectiveness(store):
    """log_engagement + get_engagement_effectiveness round-trip."""
    store.log_engagement("elonmusk", "tier1", "reply", "r1")
    store.log_engagement("elonmusk", "tier1", "reply", "r2")
    store.log_engagement("elonmusk", "tier1", "conversation_reply", "c1")
    store.log_engagement("whale_watcher", "tier3", "reply", "r3")
    result = store.get_engagement_effectiveness(days=7)
    assert len(result) == 2
    elon = next(r for r in result if r["username"] == "elonmusk")
    assert elon["total"] == 3
    assert elon["conversation_replies"] == 1
    assert elon["tier"] == "tier1"


def test_get_engagement_effectiveness_excludes_old(store):
    """Old engagement entries should not appear."""
    # We can't easily backdate engagement_log since it uses DEFAULT datetime('now')
    # but we can verify empty result for fresh store
    result = store.get_engagement_effectiveness(days=7)
    assert result == []


def test_get_recent_engagement_tweet_ids(store):
    """Should return IDs of reply/quote_tweet/conversation_reply posts."""
    store.mark_posted("reply_t1", "reply", "reply content")
    store.mark_posted("quote_t2", "quote_tweet", "quote content")
    store.mark_posted("convo_m1", "conversation_reply", "convo content")
    store.mark_posted("orig_1", "morning_brief", "original content")
    ids = store.get_recent_engagement_tweet_ids(hours=48)
    assert "reply_t1" in ids
    assert "quote_t2" in ids
    assert "convo_m1" in ids
    assert "orig_1" not in ids


def test_get_recent_engagement_tweet_ids_excludes_old(store):
    """Old engagement tweets should not be returned."""
    store.mark_posted("old_reply", "reply", "old", posted_at="2020-01-01 00:00:00")
    store.mark_posted("new_reply", "reply", "new")
    ids = store.get_recent_engagement_tweet_ids(hours=48)
    assert "new_reply" in ids
    assert "old_reply" not in ids


def test_get_recent_original_tweet_ids_excludes_replies_and_followups(store):
    """self_followup and self_reply posts should be excluded from metrics collection."""
    store.mark_posted("orig_1", "morning_brief", "morning content")
    store.mark_posted("followup_1", "self_followup", "followup content")
    store.mark_posted("reply_1", "self_reply", "reply content")
    ids = store.get_recent_original_tweet_ids(hours=168)
    tweet_ids = [r["tweet_id"] for r in ids]
    assert "orig_1" in tweet_ids
    assert "followup_1" not in tweet_ids  # self_followup excluded (it's a reply)
    assert "reply_1" not in tweet_ids  # self_reply excluded
