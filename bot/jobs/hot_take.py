# jobs/hot_take.py
"""Hot Take — posts the hottest Polymarket question.

Two modes:
  - Poll days (Tue/Thu/Sat): post as a Twitter poll with Yes/No options
  - Text days (Mon/Wed/Fri/Sun): post as a bare question tweet

No odds, no commentary, no context. Just the question itself.
Short, human, naturally invites replies and votes.
"""
import datetime
import logging
from bot.jobs.base import BaseJob
from bot.data.polymarket import PolymarketFetcher

logger = logging.getLogger(__name__)

# Days to post as a Twitter poll (weekday index: 0=Mon, 1=Tue, ... 6=Sun)
_POLL_DAYS = {1, 3, 5}  # Tue, Thu, Sat

# Poll duration: 24 hours
_POLL_DURATION_MINUTES = 1440


class HotTakeJob(BaseJob):
    def __init__(self, store, generator, poster, polymarket: PolymarketFetcher = None,
                 notifier=None, **kwargs):
        super().__init__(store, generator, poster, notifier=notifier)
        self.polymarket = polymarket

    def _is_poll_day(self) -> bool:
        return datetime.datetime.now().weekday() in _POLL_DAYS

    def execute(self):
        if not self.polymarket:
            logger.warning("HotTakeJob: no polymarket fetcher configured")
            return None

        markets = self.polymarket.get_trending_markets()
        if not markets:
            return None

        # Find first market not on cooldown (24h per question)
        chosen = None
        topic_key = None
        for m in markets:
            q = m.get("question", "").strip()
            if not q:
                continue
            key = f"hot_take_{q[:80]}"
            if not self.store.topic_on_cooldown(key, cooldown_hours=24):
                chosen = m
                topic_key = key
                break
        if not chosen:
            return None

        # Just post the question — nothing else
        tweet = chosen["question"].strip()

        # Ensure it ends with ?
        if not tweet.endswith("?"):
            tweet += "?"

        # Safety: must fit in a tweet
        if len(tweet) > 280:
            tweet = tweet[:277] + "...?"

        # Poll days → Twitter poll; text days → plain tweet
        use_poll = self._is_poll_day()
        if use_poll:
            tweet_id = self.poster.post_poll(tweet, ["Yes", "No"],
                                             duration_minutes=_POLL_DURATION_MINUTES)
        else:
            tweet_id = self.poster.post_tweet(tweet)

        if not tweet_id:
            mode = "post_poll" if use_poll else "post_tweet"
            raise RuntimeError(f"{mode} returned None — Twitter API rejected or unavailable")
        self.store.mark_posted(topic_key, "hot_take", tweet, topic=topic_key)
        self._mark_posted_with_topics(tweet_id, "hot_take", tweet, topic="hot_take")
        self._schedule_followup(tweet_id, tweet, "hot_take", tweet_text=tweet)
        logger.info("Hot take posted (%s): %s", "poll" if use_poll else "text", tweet[:80])
        return tweet_id
