# jobs/engagement.py
import logging
import random
import time
from bot.jobs.base import BaseJob
from bot.twitter.engager import Engager
from bot.twitter.watchlist import TIER1_USERNAMES, TIER2_USERNAMES, TIER3_USERNAMES, POLYMARKET_USERNAMES

logger = logging.getLogger(__name__)

DAILY_REPLY_CAP = 20
DAILY_QUOTE_CAP = 10
DAILY_LIKE_CAP = 100  # safe daily ceiling — well below Twitter's detection threshold
PER_RUN_LIKE_CAP = 15  # ~15 per run × 6 runs = ~90/day
PER_RUN_CAP = 5  # max interactions per run — spread activity across runs
CONVO_PER_RUN_CAP = 3  # max conversation continuations per run
MIN_ENGAGEMENT = 3  # at least 1 like — Tier3 accounts have low engagement, that's fine
REPLY_DELAY_RANGE = (15, 60)  # seconds between replies — look human
LIKE_DELAY_RANGE = (3, 8)  # seconds between likes — faster than replies but still human
_FAILED_ID_TTL = 7200  # 2 hours before retrying a failed user_id lookup

# AI bot accounts — never engage, never reply, never like
# Two AIs talking to each other is a waste of rate limit and looks bad
BLOCKED_USERNAMES = {
    "gork",          # Grok AI bot on X
}
BLOCKED_USER_IDS = {
    "1909390269533011968",  # bot that loop-replies (username unresolved)
}

# Broadened static queries — mix of macro, on-chain, and prediction market
SEARCH_QUERIES = [
    "Polymarket -giveaway",
    "$BTC analysis -giveaway -airdrop",
    "bitcoin ETF inflow -giveaway",
    "crypto macro -giveaway -airdrop",
    "funding rate liquidation -giveaway",
    "whale alert crypto -giveaway",
]


_POLYMARKET_KEYWORDS = {
    "polymarket", "prediction market", "betting odds", "kalshi",
    "% yes", "% no", "event contract", "binary option",
}


class EngagementJob(BaseJob):
    def __init__(self, store, generator, poster, engager: Engager,
                 prices=None, exchange_flows=None, polymarket=None,
                 trends=None, notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.engager = engager
        self.prices = prices
        self.exchange_flows = exchange_flows
        self.polymarket = polymarket
        self.trends = trends
        self._user_id_cache: dict = {}

    def _get_cached_user_id(self, username: str):
        entry = self._user_id_cache.get(username)
        if entry is not None:
            uid, cached_at = entry
            if uid is not None:
                return uid
            # Failed lookup — retry after TTL
            if time.time() - cached_at < _FAILED_ID_TTL:
                return None
        uid = self.engager.get_user_id(username)
        self._user_id_cache[username] = (uid, time.time())
        return uid

    def _replies_today(self) -> int:
        return self.store.count_posts_since_midnight("reply")

    def _quotes_today(self) -> int:
        return self.store.count_posts_since_midnight("quote_tweet")

    def _likes_today(self) -> int:
        return self.store.count_posts_since_midnight("like")

    def _collect_candidates(self, usernames, tier_label):
        candidates = []
        for username in usernames:
            user_id = self._get_cached_user_id(username)
            if not user_id:
                continue
            tweets = self.engager.get_unresponded_tweets(user_id, max_results=10)
            tweets = self.engager.filter_tweets(tweets)
            for t in tweets:
                t["username"] = username
                t["tier"] = tier_label
            candidates.extend(tweets)
        return candidates

    def _collect_search_candidates(self, queries):
        """Search Twitter for keyword-based engagement opportunities."""
        # Collect all known watchlist user_ids so we can skip duplicates
        known_user_ids = {uid for uid, _ts in self._user_id_cache.values() if uid}
        candidates = []
        for query in queries:
            try:
                tweets = self.engager.search_recent_tweets(query, max_results=10)
            except Exception as exc:
                logger.warning("Search query %r failed: %s", query, exc)
                continue
            tweets = self.engager.filter_tweets(tweets)
            for t in tweets:
                # Skip authors already in watchlist
                if t.get("author_id") in known_user_ids:
                    continue
                # Skip AI bot accounts
                if (t.get("username", "").lower() in BLOCKED_USERNAMES
                        or t.get("author_id", "") in BLOCKED_USER_IDS):
                    continue
                t["tier"] = "search"
                if not t.get("username"):
                    t["username"] = t.get("author_id", "unknown")
            candidates.extend([t for t in tweets if t.get("tier") == "search"])
        return candidates

    def _build_search_queries(self) -> list:
        """Build search queries from Polymarket + trending coins only (low competition)."""
        queries = list(SEARCH_QUERIES)
        # Dynamic Polymarket queries — find people discussing the hottest prediction markets
        if self.polymarket:
            try:
                poly_queries = self._extract_polymarket_queries()
                queries.extend(poly_queries)
            except Exception as exc:
                logger.warning("Polymarket search query extraction failed: %s", exc)
        # Dynamic trending coin queries — small-cap cashtags have less competition
        if self.prices:
            try:
                trending_coins = self.prices.get_trending_coins()
                for c in trending_coins[:3]:
                    sym = c.get("symbol", "")
                    if sym and sym not in ("BTC", "ETH", "SOL", "BNB", "XRP"):
                        queries.append(f"${sym} -giveaway -airdrop")
            except Exception as exc:
                logger.warning("Trending coins search query failed: %s", exc)
        random.shuffle(queries)
        return queries[:12]

    def _extract_polymarket_queries(self) -> list:
        """Extract Twitter search queries from trending Polymarket market questions."""
        import re
        markets = self.polymarket.get_trending_markets()
        if not markets:
            return []
        queries = []
        _STRIP_WORDS = re.compile(
            r'^(will|is|does|has|are|can|should|do|would|could)\s+',
            re.IGNORECASE,
        )
        _DATE_TAIL = re.compile(
            r'\s+(in|by|before|after|during)\s+'
            r'(january|february|march|april|may|june|july|august|september|october|november|december'
            r'|\d{4}).*$',
            re.IGNORECASE,
        )
        for m in markets[:5]:
            q = m.get("question", "")
            if not q:
                continue
            q = _STRIP_WORDS.sub('', q).rstrip('?').strip()
            q = _DATE_TAIL.sub('', q).strip()
            # Remove "the" at start
            if q.lower().startswith('the '):
                q = q[4:]
            # Truncate to ~50 chars at word boundary
            if len(q) > 50:
                q = q[:50].rsplit(' ', 1)[0]
            if len(q) >= 10:
                queries.append(f"{q} -giveaway")
        return queries[:3]

    def _collect_conversation_candidates(self):
        """Find mentions that are replies to our engagement tweets — continue the conversation."""
        try:
            bot_uid = self.engager.get_own_user_id()
            if not bot_uid:
                return []
            mentions = self.engager.get_mentions(bot_uid)
            if not mentions or not isinstance(mentions, list):
                return []
        except Exception as exc:
            logger.warning("Failed to collect conversation candidates: %s", exc)
            return []
        # Only keep mentions that are direct replies to us and not already handled
        candidates = []
        for m in mentions:
            if m.get("in_reply_to_user_id") != bot_uid:
                continue
            if str(m.get("author_id", "")) == bot_uid:
                continue
            # Skip AI bot accounts — two AIs chatting = waste
            if (m.get("username", "").lower() in BLOCKED_USERNAMES
                    or m.get("author_id", "") in BLOCKED_USER_IDS):
                logger.debug("Skipping blocked bot @%s in conversation", m.get("username"))
                continue
            convo_key = f"convo_{m['id']}"
            if self.store.is_posted(convo_key):
                continue
            if not m.get("username"):
                m["username"] = m.get("author_id", "unknown")
            m["tier"] = "conversation"
            m["_sort_score"] = m.get("engagement", 0) * 3
            candidates.append(m)
        return candidates

    @staticmethod
    def _sanitize(text: str) -> str:
        import re
        return re.sub(r'[\r\n\t]', ' ', text).strip()[:500]

    def _get_flow_context(self) -> str:
        """Build a short flow data snippet for QT/reply context."""
        parts = []
        try:
            if self.prices:
                price_data = self.prices.get_crypto_prices()
                summary = self.prices.format_crypto_summary(price_data)
                if summary:
                    parts.append(summary)
            if self.exchange_flows:
                exch_summary = self._safe_format(self.exchange_flows, self.exchange_flows.get_exchange_snapshot())
                if exch_summary:
                    parts.append(exch_summary)
            if self.polymarket:
                poly_data = self.polymarket.get_polymarket_snapshot()
                poly_summary = self._safe_format(self.polymarket, poly_data)
                if poly_summary:
                    parts.append(poly_summary)
        except Exception as exc:
            logger.warning("Engagement flow context failed: %s", exc)
        return " | ".join(parts) if parts else ""

    @staticmethod
    def _is_polymarket_related(text: str) -> bool:
        lower = text.lower()
        return any(kw in lower for kw in _POLYMARKET_KEYWORDS)

    def _get_hot_keywords(self) -> list:
        """Get today's hot topic keywords from Polymarket + trending coins."""
        if hasattr(self, '_hot_keywords_cache'):
            return self._hot_keywords_cache
        keywords = []
        if self.polymarket:
            try:
                markets = self.polymarket.get_trending_markets()
                for m in markets[:3]:
                    q = m.get("question", "").lower()
                    # Extract key phrases from questions
                    for word in q.replace("?", "").split():
                        if len(word) > 3 and word not in ("will", "does", "have", "been", "this", "that", "with", "from", "they", "their", "about", "would", "could", "should", "before", "after", "above", "below", "between"):
                            keywords.append(word)
            except Exception:
                pass
        if self.prices:
            try:
                coins = self.prices.get_trending_coins()
                for c in coins[:5]:
                    sym = c.get("symbol", "")
                    name = c.get("name", "")
                    if sym:
                        keywords.append(sym.lower())
                    if name:
                        keywords.append(name.lower())
            except Exception:
                pass
        self._hot_keywords_cache = keywords
        return keywords

    def _is_hot_topic(self, text: str) -> bool:
        """Check if tweet text matches today's hot topics."""
        lower = text.lower()
        keywords = self._get_hot_keywords()
        matches = sum(1 for kw in keywords if kw in lower)
        return matches >= 2  # at least 2 keyword hits = relevant to hot topic

    def _should_quote_tweet(self, tweet) -> bool:
        """Topic-based QT: QT tweets about hot topics or with decent engagement."""
        text = tweet.get("text", "")
        eng = tweet.get("engagement", 0)
        # Tweet asks a question → reply (answer it)
        if "?" in text:
            return False
        # Core logic: tweet is about a hot topic AND has some engagement
        if self._is_hot_topic(text) and eng >= 10:
            return True
        # Polymarket-related → QT (our differentiator)
        if self._is_polymarket_related(text) and eng >= 5:
            return True
        return False

    def _build_context(self, tweet, flow_ctx: str, poly_ctx: str = "") -> str:
        safe = self._sanitize(tweet['text'])
        ctx = f"Original tweet by @{tweet['username']}: {safe}\n"
        is_poly = self._is_polymarket_related(tweet['text'])
        if is_poly and poly_ctx:
            # Polymarket-specific: reference prediction odds
            ctx += f"Polymarket prediction data: {poly_ctx}\n"
            ctx += ("Reference Polymarket odds in your reply — cite specific % or volume. "
                    "Only use numbers from the original tweet or the data above. Do not invent statistics.")
        elif flow_ctx:
            ctx += f"Current flow data: {flow_ctx}\n"
            ctx += ("Use the flow data above. Lead with a number, then give your read. "
                    "Only use numbers from the original tweet or the data above. Do not invent statistics.")
        else:
            ctx += ("Add your trader's perspective. "
                    "Only reference numbers from the original tweet. Do not invent statistics.")
        return ctx

    def _try_quote_tweet(self, tweet, recent, flow_ctx: str = "", poly_ctx: str = ""):
        """Attempt a quote tweet. Returns (db_key, None) on success, (None, error_str) on failure."""
        context = self._build_context(tweet, flow_ctx, poly_ctx)
        qt_text = self.generator.generate_tweet(context, "quote_tweet", recent)
        if not qt_text:
            return None, "empty_generation"
        self._validate_output(context, qt_text, "quote_tweet")
        # Hard reject fabricated numbers — engagement content represents us in others' replies
        from bot.ai.validate import validate_tweet_numbers
        vr = validate_tweet_numbers(context, qt_text)
        if not vr["valid"]:
            logger.warning("Rejecting QT with untraced numbers: %s", vr["untraced"])
            return None, "untraced_numbers"
        try:
            qt_id = self.poster.post_quote_tweet(qt_text, tweet["id"])
        except Exception as exc:
            logger.error("Quote tweet failed for @%s: %s", tweet["username"], exc)
            self._last_raw_error = str(exc)
            return None, self._classify_error(exc)
        if qt_id:
            db_key = f"quote_{tweet['id']}"
            self.store.mark_posted(db_key, "quote_tweet", qt_text)
            self.store.log_engagement(tweet.get("username", ""), tweet.get("tier", ""), "quote_tweet", str(qt_id))
            self.engager.like_tweet(tweet["id"])
            logger.info("Quote tweeted @%s (engagement=%s): %s",
                        tweet["username"], tweet.get("engagement"), tweet["id"])
            return db_key, None
        return None, "no_data_returned"

    def _try_reply(self, tweet, recent, flow_ctx: str = "", poly_ctx: str = ""):
        """Attempt a reply. Returns (db_key, None) on success, (None, error_str) on failure."""
        context = self._build_context(tweet, flow_ctx, poly_ctx)
        reply_text = self.generator.generate_tweet(context, "reply", recent)
        if not reply_text:
            return None, "empty_generation"
        self._validate_output(context, reply_text, "reply")
        from bot.ai.validate import validate_tweet_numbers
        vr = validate_tweet_numbers(context, reply_text)
        if not vr["valid"]:
            logger.warning("Rejecting reply with untraced numbers: %s", vr["untraced"])
            return None, "untraced_numbers"
        try:
            reply_id = self.poster.post_reply(reply_text, tweet["id"])
        except Exception as exc:
            logger.error("Reply failed for @%s: %s", tweet["username"], exc)
            self._last_raw_error = str(exc)
            return None, self._classify_error(exc)
        if reply_id:
            db_key = f"reply_{tweet['id']}"
            self.store.mark_posted(db_key, "reply", reply_text)
            self.store.log_engagement(tweet.get("username", ""), tweet.get("tier", ""), "reply", str(reply_id))
            self.engager.like_tweet(tweet["id"])
            logger.info("Replied to @%s (engagement=%s): %s",
                        tweet["username"], tweet.get("engagement"), tweet["id"])
            return db_key, None
        return None, "no_data_returned"

    @staticmethod
    def _classify_error(exc: Exception) -> str:
        """Extract a short error label from an exception for diagnostics."""
        exc_str = str(exc)
        if "403" in exc_str:
            return "403_forbidden"
        if "404" in exc_str:
            return "404_not_found"
        if "429" in exc_str:
            return "429_rate_limit"
        if "duplicate" in exc_str.lower():
            return "duplicate"
        return type(exc).__name__[:30]

    def _try_conversation_reply(self, mention, recent, flow_ctx: str = "", poly_ctx: str = ""):
        """Reply to a mention continuing a conversation. Returns (db_key, None) on success."""
        safe = self._sanitize(mention['text'])
        ctx = f"Someone replied to your tweet: {safe}\n"
        ctx += ("Continue the conversation naturally. Be brief, add value, ask a follow-up or agree/push back. "
                "Only reference numbers from the conversation. Do not invent statistics.")
        reply_text = self.generator.generate_tweet(ctx, "reply", recent)
        if not reply_text:
            return None, "empty_generation"
        self._validate_output(ctx, reply_text, "reply")
        from bot.ai.validate import validate_tweet_numbers
        vr = validate_tweet_numbers(ctx, reply_text)
        if not vr["valid"]:
            logger.warning("Rejecting convo reply with untraced numbers: %s", vr["untraced"])
            return None, "untraced_numbers"
        try:
            reply_id = self.poster.post_reply(reply_text, mention["id"])
        except Exception as exc:
            logger.error("Conversation reply failed for mention %s: %s", mention["id"], exc)
            return None, self._classify_error(exc)
        if reply_id:
            db_key = f"convo_{mention['id']}"
            self.store.mark_posted(db_key, "conversation_reply", reply_text)
            self.store.log_engagement(mention.get("username", ""), "conversation", "conversation_reply", str(reply_id))
            self.engager.like_tweet(mention["id"])
            logger.info("Conversation reply to mention %s (engagement=%s)",
                        mention["id"], mention.get("engagement"))
            return db_key, None
        return None, "no_data_returned"

    def _like_batch(self, candidates: list):
        """Like a batch of tweets we didn't reply/QT to — pure visibility play.

        Priority: 1) replies to our tweets (reward engagement)
                  2) KOL watchlist tweets  3) search results
        """
        like_count = self._likes_today()
        if like_count >= DAILY_LIKE_CAP:
            return 0

        budget = min(PER_RUN_LIKE_CAP, DAILY_LIKE_CAP - like_count)
        liked = 0

        for tweet in candidates:
            if liked >= budget:
                break
            if (tweet.get("username", "").lower() in BLOCKED_USERNAMES
                    or tweet.get("author_id", "") in BLOCKED_USER_IDS):
                continue
            like_key = f"like_{tweet['id']}"
            if self.store.is_posted(like_key):
                continue
            if liked > 0:
                time.sleep(random.uniform(*LIKE_DELAY_RANGE))
            if self.engager.like_tweet(tweet["id"]):
                self.store.mark_posted(like_key, "like", "", topic="like")
                liked += 1

        if liked:
            logger.info("Batch liked %d tweets (daily total: %d/%d)",
                        liked, like_count + liked, DAILY_LIKE_CAP)
        return liked

    def execute(self):
        posted_ids = []
        self.engager.last_error = None  # reset so diagnostic only shows THIS run's errors
        self._last_raw_error = None

        # Housekeeping: prune expired 403 entries
        try:
            pruned = self.store.prune_restricted_users(ttl_hours=48)
            if pruned:
                logger.info("Pruned %d expired restricted users", pruned)
        except Exception as exc:
            logger.warning("prune_restricted_users failed: %s", exc)

        # ── Phase 0: Conversation continuations (highest priority) ──
        logger.info("Engagement run starting — reply cap: %d/%d, quote cap: %d/%d",
                     self._replies_today(), DAILY_REPLY_CAP, self._quotes_today(), DAILY_QUOTE_CAP)
        convo_candidates = self._collect_conversation_candidates()
        if convo_candidates:
            recent = self.get_recent_tweets()
            flow_ctx = self._get_flow_context()
            convo_count = 0
            reply_count = self._replies_today()
            for mention in convo_candidates[:CONVO_PER_RUN_CAP]:
                if reply_count >= DAILY_REPLY_CAP:
                    break
                if convo_count > 0:
                    delay = random.uniform(*REPLY_DELAY_RANGE)
                    logger.debug("Convo delay: %.0fs", delay)
                    time.sleep(delay)
                db_key, _err = self._try_conversation_reply(mention, recent, flow_ctx)
                if db_key:
                    posted_ids.append(db_key)
                    convo_count += 1
                    reply_count += 1
            if convo_count:
                logger.info("Conversation continuations: %d/%d", convo_count, len(convo_candidates))

        # ── Phase 1: Collect candidates from all tiers ──
        t1 = self._collect_candidates(TIER1_USERNAMES, "tier1")
        t2 = self._collect_candidates(TIER2_USERNAMES, "tier2")
        t3 = self._collect_candidates(TIER3_USERNAMES, "tier3")
        pm = self._collect_candidates(POLYMARKET_USERNAMES, "polymarket")
        search = self._collect_search_candidates(self._build_search_queries())
        all_raw = t1 + t2 + t3 + pm + search
        candidates = [t for t in all_raw if t.get("engagement", 0) >= MIN_ENGAGEMENT]
        # Filter out users on the persistent 403 blocklist
        candidates = [t for t in candidates if not self.store.is_user_restricted(t.get("username", ""))]
        logger.info("Engagement candidates: t1=%d t2=%d t3=%d pm=%d search=%d → after filter: %d/%d",
                     len(t1), len(t2), len(t3), len(pm), len(search), len(candidates), len(all_raw))

        # ── Reply/QT DISABLED — API 403 on both for most accounts ──
        # Engagement moving to browser-based approach.
        # Only batch-like remains active for free visibility.
        logger.info("Reply/QT disabled (API 403). Like-only mode.")

        # ── Batch like — pure visibility, no content creation ──
        # Priority: conversation replies > KOL tweets > search results
        like_pool = convo_candidates + candidates
        self._like_batch(like_pool)

        return posted_ids
