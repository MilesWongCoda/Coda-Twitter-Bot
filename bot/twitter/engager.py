# twitter/engager.py
from __future__ import annotations

import re
import logging
from datetime import datetime, timedelta, timezone
import tweepy
from bot.db.store import Store

logger = logging.getLogger(__name__)

PROMO_KEYWORDS = [
    "sponsored", "ad:", "giveaway", "airdrop", "100x", "buy now", "discount",
    "limited offer", "presale", "mint now", "whitelist", "alpha call",
    "10x", "free nft", "claim now", "dm me", "join now",
]

# Relevance filter aligned to brand: Bitcoin · Rates · Flows · Alpha
# Only engage with tweets about BTC, macro rates/flows, or on-chain data
_RELEVANCE_TERMS = {
    # Bitcoin (primary asset)
    "btc", "bitcoin", "satoshi", "halving", "miner",
    # Macro / Rates
    "fed", "fomc", "inflation", "cpi", "gdp", "recession", "rate cut",
    "rate hike", "treasury", "dxy", "yield", "interest rate", "bond",
    "pce", "payroll", "employment", "jobs report",
    # Flows / Liquidity
    "etf", "inflow", "outflow", "capital flow", "fund flow", "liquidity",
    "grayscale", "blackrock", "fidelity", "spot etf", "reserve",
    # Markets (macro context)
    "nasdaq", "s&p", "dow", "tariff", "trade war", "wall street",
    "stock market", "risk-on", "risk-off", "dollar",
    # On-chain
    "on-chain", "onchain", "whale", "exchange flow", "utxo", "mempool",
    "hashrate", "difficulty",
    # Crypto macro (not altcoin-specific)
    "crypto", "blockchain", "stablecoin",
    "liquidat", "leverage", "short squeeze", "funding rate",
    "open interest", "derivatives",
    # Prediction markets (differentiator track)
    "polymarket", "prediction market", "betting odds", "kalshi",
    "event contract", "binary option",
    # DeFi data (part of our brand)
    "defi", "tvl", "dex", "lending", "borrow", "aave", "uniswap",
    "curve", "maker", "lido", "staking", "restaking",
    # ETH (core asset alongside BTC)
    "eth", "ethereum", "layer2", "l2", "rollup",
    # Major altcoins — relevant when discussed in macro/flow context
    "sol", "solana", "bnb", "xrp", "doge", "ada", "cardano",
    "avax", "matic", "polygon", "altcoin", "altseason",
}


# Short terms prone to substring false positives need word-boundary matching
# e.g. "eth" matching "Bethlehem", "sol" matching "solution", "ada" matching "Canada"
_SHORT_TERMS = {t for t in _RELEVANCE_TERMS if len(t) <= 4}
_LONG_TERMS = _RELEVANCE_TERMS - _SHORT_TERMS
_SHORT_PATTERN = re.compile(
    r'\b(' + '|'.join(re.escape(t) for t in sorted(_SHORT_TERMS)) + r')\b',
    re.IGNORECASE
)


def _is_on_topic(text: str) -> bool:
    """Check if tweet is about Bitcoin, rates, flows, or macro — our brand pillars."""
    lower = text.lower()
    # Cashtags are strong signal
    if "$btc" in lower or "$eth" in lower or "$sol" in lower or "$xrp" in lower:
        return True
    # Long terms (5+ chars) — substring match is safe
    for term in _LONG_TERMS:
        if term in lower:
            return True
    # Also check hashtag-style: #RateHike, #OpenInterest, #FundingRate
    no_spaces = lower.replace(" ", "").replace("-", "").replace("_", "")
    for term in _LONG_TERMS:
        if " " in term and term.replace(" ", "") in no_spaces:
            return True
    # Short terms (<=4 chars) — require word boundary to avoid false positives
    if _SHORT_PATTERN.search(text):
        return True
    return False


class Engager:
    def __init__(self, client: tweepy.Client, store: Store):
        self.client = client
        self.store = store
        self._own_user_id = None
        self.last_error = None  # last API error for upstream diagnostics

    def get_recent_tweets_from_user(self, user_id: str, max_results: int = 5,
                                    max_age_hours: int = 48) -> list:
        max_results = max(5, min(max_results, 100))  # Twitter API constraint: 5–100
        start_time = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            resp = self.client.get_users_tweets(
                id=user_id,
                max_results=max_results,
                tweet_fields=["id", "text", "created_at", "public_metrics", "reply_settings"],
                exclude=["retweets", "replies"],
                start_time=start_time,
            )
        except Exception as exc:
            self.last_error = f"get_tweets(uid={user_id}): {exc}"
            logger.warning("get_users_tweets failed for user_id=%s: %s", user_id, exc)
            return []
        if not resp.data:
            return []
        tweets = []
        for t in resp.data:
            metrics = t.public_metrics or {}
            reply_settings = getattr(t, "reply_settings", None) or "everyone"
            tweets.append({
                "id": str(t.id),
                "text": t.text,
                "likes": metrics.get("like_count", 0),
                "retweets": metrics.get("retweet_count", 0),
                "replies": metrics.get("reply_count", 0),
                "engagement": metrics.get("like_count", 0) * 3 + metrics.get("retweet_count", 0) * 5 + metrics.get("reply_count", 0) * 2,
                "reply_settings": reply_settings,
            })
        return tweets

    def get_unresponded_tweets(self, user_id: str, max_results: int = 5) -> list:
        tweets = self.get_recent_tweets_from_user(user_id, max_results)
        return [t for t in tweets
                if not self.store.is_posted(f"reply_{t['id']}")
                and not self.store.is_posted(f"quote_{t['id']}")]

    def sort_by_engagement(self, tweets: list) -> list:
        return sorted(tweets, key=lambda t: t.get("engagement", 0), reverse=True)

    def filter_tweets(self, tweets: list) -> list:
        """Filter out promos and off-topic tweets (e.g. KOL posting about sports)."""
        def is_promo(text: str) -> bool:
            lower = text.lower()
            return any(kw in lower for kw in PROMO_KEYWORDS)
        return [t for t in tweets
                if not is_promo(t["text"]) and _is_on_topic(t["text"])]

    def get_user_id(self, username: str):
        try:
            resp = self.client.get_user(username=username)
            return str(resp.data.id) if resp.data else None
        except Exception as exc:
            self.last_error = f"get_user(@{username}): {exc}"
            logger.warning("get_user_id failed for @%s: %s", username, exc)
            return None

    def get_own_user_id(self):
        if self._own_user_id is not None:
            return self._own_user_id
        try:
            resp = self.client.get_me()
            if resp.data:
                self._own_user_id = str(resp.data.id)
        except Exception as exc:
            logger.warning("get_me failed: %s", exc)
        return self._own_user_id

    def search_recent_tweets(self, query: str, max_results: int = 10,
                             max_age_hours: int = 6) -> list:
        """Search recent tweets by keyword for engagement opportunities."""
        max_results = max(10, min(max_results, 100))
        start_time = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Filter out retweets, replies, and non-English tweets
        full_query = f"{query} -is:retweet -is:reply lang:en"
        try:
            resp = self.client.search_recent_tweets(
                query=full_query,
                max_results=max_results,
                tweet_fields=["id", "text", "created_at", "public_metrics", "reply_settings"],
                expansions=["author_id"],
                user_fields=["username"],
                start_time=start_time,
            )
        except Exception as exc:
            self.last_error = f"search({query[:30]}): {exc}"
            logger.warning("search_recent_tweets failed for query=%r: %s", query, exc)
            return []
        if not resp.data:
            return []
        # Build author_id → username lookup from includes
        user_map = {}
        if hasattr(resp, "includes") and resp.includes and "users" in resp.includes:
            for u in resp.includes["users"]:
                user_map[str(u.id)] = u.username
        tweets = []
        for t in resp.data:
            reply_settings = getattr(t, "reply_settings", None) or "everyone"
            if reply_settings != "everyone":
                continue
            metrics = t.public_metrics or {}
            author_id = str(t.author_id) if hasattr(t, "author_id") else ""
            tweets.append({
                "id": str(t.id),
                "text": t.text,
                "author_id": author_id,
                "username": user_map.get(author_id, ""),
                "likes": metrics.get("like_count", 0),
                "retweets": metrics.get("retweet_count", 0),
                "replies": metrics.get("reply_count", 0),
                "engagement": metrics.get("like_count", 0) * 3 + metrics.get("retweet_count", 0) * 5 + metrics.get("reply_count", 0) * 2,
                "reply_settings": reply_settings,
            })
        return tweets

    def get_tweet_metrics(self, tweet_id: str) -> dict | None:
        """Fetch public + non-public metrics for one of our own tweets."""
        try:
            resp = self.client.get_tweet(
                tweet_id,
                tweet_fields=["public_metrics", "non_public_metrics"],
                user_auth=True,
            )
        except Exception as exc:
            logger.warning("get_tweet_metrics failed for %s: %s", tweet_id, exc)
            return None
        if not resp.data:
            return None
        pub = resp.data.public_metrics or {}
        # non_public_metrics may not be available on free tier
        non_pub = getattr(resp.data, "non_public_metrics", None) or {}
        return {
            "impressions": non_pub.get("impression_count", 0),
            "likes": pub.get("like_count", 0),
            "retweets": pub.get("retweet_count", 0),
            "replies": pub.get("reply_count", 0),
            "quotes": pub.get("quote_count", 0),
            "bookmarks": pub.get("bookmark_count", 0),
        }

    def like_tweet(self, tweet_id: str) -> bool:
        """Like a tweet. Returns True on success."""
        try:
            self.client.like(tweet_id)
            return True
        except Exception as exc:
            logger.debug("like_tweet failed for %s: %s", tweet_id, exc)
            return False

    def get_mentions(self, user_id: str, max_results: int = 20,
                     max_age_hours: int = 48) -> list:
        max_results = max(5, min(max_results, 100))
        start_time = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            resp = self.client.get_users_mentions(
                id=user_id,
                max_results=max_results,
                start_time=start_time,
                tweet_fields=["created_at", "author_id", "public_metrics",
                              "conversation_id", "in_reply_to_user_id"],
                expansions=["author_id"],
                user_fields=["username"],
            )
        except Exception as exc:
            logger.warning("get_users_mentions failed for user_id=%s: %s", user_id, exc)
            return []
        if not resp.data:
            return []
        # Build author_id → username lookup from includes
        user_map = {}
        if hasattr(resp, "includes") and resp.includes and "users" in resp.includes:
            for u in resp.includes["users"]:
                user_map[str(u.id)] = u.username
        own_uid = self.get_own_user_id()
        mentions = []
        for t in resp.data:
            # Skip our own outgoing tweets that happen to mention ourselves
            if own_uid and str(t.author_id) == own_uid:
                continue
            metrics = t.public_metrics or {}
            author_id = str(t.author_id)
            mentions.append({
                "id": str(t.id),
                "text": t.text,
                "author_id": author_id,
                "username": user_map.get(author_id, author_id),
                "in_reply_to_user_id": str(t.in_reply_to_user_id) if t.in_reply_to_user_id else None,
                "conversation_id": str(t.conversation_id) if t.conversation_id else None,
                "engagement": metrics.get("like_count", 0) * 3 + metrics.get("retweet_count", 0) * 5 + metrics.get("reply_count", 0) * 2,
            })
        return mentions


class DryRunEngager:
    def search_recent_tweets(self, query: str, max_results: int = 10,
                             max_age_hours: int = 6) -> list:
        logger.info("[DRY RUN] search_recent_tweets query=%r", query)
        return [
            {"id": "dry_search_1", "text": f"$BTC analysis related to: {query[:30]}",
             "author_id": "search_author_1", "username": "search_user_1",
             "engagement": 200, "likes": 40, "retweets": 16, "replies": 10,
             "reply_settings": "everyone"},
            {"id": "dry_search_2", "text": f"Bitcoin ETF data point about: {query[:30]}",
             "author_id": "search_author_2", "username": "search_user_2",
             "engagement": 100, "likes": 20, "retweets": 8, "replies": 5,
             "reply_settings": "everyone"},
        ]

    def get_tweet_metrics(self, tweet_id: str) -> dict | None:
        logger.info("[DRY RUN] get_tweet_metrics for %s", tweet_id)
        return {"impressions": 1200, "likes": 35, "retweets": 8, "replies": 5, "quotes": 2, "bookmarks": 10}

    def get_user_id(self, username: str) -> str:
        logger.info("[DRY RUN] get_user_id @%s → fake_uid", username)
        return f"dry_uid_{username}"

    def get_unresponded_tweets(self, user_id: str, max_results: int = 5) -> list:
        logger.info("[DRY RUN] get_unresponded_tweets for user_id=%s", user_id)
        return [
            {"id": f"dry_tweet_{user_id}_1", "text": "BTC just broke $100k resistance. This is the cycle.", "engagement": 1500, "likes": 300, "retweets": 100, "replies": 50},  # 300*3+100*5+50*2
            {"id": f"dry_tweet_{user_id}_2", "text": "Macro conditions shifting. Fed pivot incoming.", "engagement": 600, "likes": 120, "retweets": 40, "replies": 20},  # 120*3+40*5+20*2
        ]

    def sort_by_engagement(self, tweets: list) -> list:
        return sorted(tweets, key=lambda t: t.get("engagement", 0), reverse=True)

    def filter_tweets(self, tweets: list) -> list:
        return tweets

    def get_own_user_id(self) -> str:
        logger.info("[DRY RUN] get_own_user_id → dry_bot_uid")
        return "dry_bot_uid"

    def like_tweet(self, tweet_id: str) -> bool:
        logger.info("[DRY RUN] like_tweet %s", tweet_id)
        return True

    def get_mentions(self, user_id: str, max_results: int = 20,
                     max_age_hours: int = 48) -> list:
        logger.info("[DRY RUN] get_mentions for user_id=%s", user_id)
        return [
            {"id": "dry_mention_1", "text": "@bot Great BTC analysis! What about ETH correlation?",
             "author_id": "commenter_1", "username": "dry_commenter_1",
             "in_reply_to_user_id": user_id,
             "conversation_id": "dry_conv_1", "engagement": 15},
            {"id": "dry_mention_2", "text": "@bot buy now airdrop",
             "author_id": "commenter_2", "username": "dry_commenter_2",
             "in_reply_to_user_id": user_id,
             "conversation_id": "dry_conv_2", "engagement": 1},
        ]
