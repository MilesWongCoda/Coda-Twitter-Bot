# jobs/base.py
import logging
import random
import threading
from html import escape as _html_escape
from bot.db.store import Store
from bot.ai.generate import TweetGenerator
from bot.twitter.poster import Poster

logger = logging.getLogger(__name__)

# Tweet types that should get an automatic follow-up reply
_FOLLOWUP_TYPES = {"morning_brief", "hot_take", "onchain_signal", "us_open", "evening_wrap", "trend_alert"}
_FOLLOWUP_DELAY_MIN = 300   # 5 minutes
_FOLLOWUP_DELAY_MAX = 900   # 15 minutes
_FOLLOWUP_PROBABILITY = 0.0  # 0% — 0-follower phase, self-replies are invisible; re-enable at 100+ followers


class BaseJob:
    def __init__(self, store: Store, generator: TweetGenerator, poster: Poster,
                 notifier=None):
        self.store = store
        self.generator = generator
        self.poster = poster
        self.notifier = notifier  # optional TelegramNotifier

    def get_recent_tweets(self) -> list:
        return self.store.get_recent_content(hours=24)

    def get_top_performers(self, job_type: str = None) -> list:
        try:
            if job_type:
                results = self.store.get_top_tweets(days=7, limit=3, job_type=job_type)
                if len(results) >= 2:
                    return results
            # Fallback to global top if not enough per-type data
            return self.store.get_top_tweets(days=7, limit=3)
        except Exception as exc:
            logger.warning("get_top_performers failed: %s", exc)
            return []

    def get_bottom_performers(self, job_type: str = None) -> list:
        try:
            if job_type:
                results = self.store.get_bottom_tweets(days=7, limit=2, job_type=job_type)
                if len(results) >= 2:
                    return results
            return self.store.get_bottom_tweets(days=7, limit=2)
        except Exception as exc:
            logger.warning("get_bottom_performers failed: %s", exc)
            return []

    def get_performance_patterns(self) -> dict:
        try:
            return self.store.get_performance_patterns(days=7)
        except Exception as exc:
            logger.warning("get_performance_patterns failed: %s", exc)
            return {}

    @staticmethod
    def _safe_format(fetcher, data) -> str:
        """Call fetcher.format_summary(data) safely, returning '' on any error."""
        if not data:
            return ""
        try:
            return fetcher.format_summary(data) or ""
        except Exception as exc:
            logger.warning("format_summary failed for %s: %s",
                           type(fetcher).__name__, exc)
            return ""

    def _enrich_with_trends(self, context: str) -> tuple:
        """Append trending topics to context and extract hashtags. Fails silently."""
        if not getattr(self, 'trends', None):
            return context, []
        try:
            trends_data = self.trends.fetch_all()
            trend_ctx = self.trends.format_summary(trends_data)
            trending_tags = self.trends.get_trending_hashtags(trends_data)
            if trend_ctx:
                context = f"{context}\n\n{trend_ctx}"
            return context, trending_tags
        except Exception as exc:
            logger.warning("Trends enrichment failed for %s, continuing without: %s",
                           self._job_name(), exc)
            return context, []

    def _validate_output(self, context: str, tweet: str, tweet_type: str) -> str:
        """Validate AI output numbers trace back to context. Monitor mode: never blocks."""
        try:
            from bot.ai.validate import validate_tweet_numbers
            result = validate_tweet_numbers(context, tweet)
            if not result["valid"]:
                logger.warning(
                    "%s: untraced numbers in AI output: %s (tweet_type=%s)",
                    self._job_name(), result["untraced"], tweet_type
                )
                # Telegram notification suppressed — monitor mode doesn't block posting,
                # so notifications are noise. Log warning above is sufficient.
        except Exception as exc:
            logger.debug("Output validation error (non-fatal): %s", exc)
        return tweet

    def _warn_chart_failure(self, exc: Exception):
        """Log and optionally notify about chart generation failure."""
        logger.warning("Chart generation failed for %s: %s", self._job_name(), exc)
        if self.notifier:
            try:
                self.notifier.send(f"⚠️ <b>{self._job_name()}</b> chart failed\n<code>{_html_escape(str(exc)[:150])}</code>")
            except Exception as notify_exc:
                logger.debug("Notifier send failed in _warn_chart_failure: %s", notify_exc)

    def _check_topic_overlap(self, tweet_text: str, cooldown_hours: int = 6) -> list:
        """Check if the generated tweet overlaps with recently posted data topics.

        Returns list of overlapping topic tags, empty if clean.
        """
        from bot.ai.topic_extractor import extract_data_topics
        new_topics = extract_data_topics(tweet_text)
        if not new_topics:
            return []
        recent_topics = self.store.get_recent_data_topics(hours=cooldown_hours)
        overlap = new_topics & recent_topics
        return list(overlap)

    def _generate_with_dedup(self, context, tweet_type, recent, top=None, bottom=None,
                              trending_tags=None, performance_patterns=None,
                              max_retries=2, cooldown_hours=6):
        """Generate a tweet with topic-level dedup. Retries if overlap detected."""
        overlap = []
        for attempt in range(max_retries):
            tweet = self.generator.generate_tweet(
                context, tweet_type, recent,
                top_performers=top, bottom_performers=bottom,
                trending_tags=trending_tags,
                performance_patterns=performance_patterns,
            )
            if not tweet:
                return None
            overlap = self._check_topic_overlap(tweet, cooldown_hours)
            if not overlap:
                return tweet
            logger.warning("%s: topic overlap detected (attempt %d/%d): %s — regenerating",
                           self._job_name(), attempt + 1, max_retries, overlap)
            # Add blacklist to context for retry
            blacklist = ", ".join(overlap)
            context = f"{context}\n\nBLACKLIST — do NOT write about these topics (already posted recently): {blacklist}"
        # Last attempt exceeded, accept whatever we got
        logger.warning("%s: accepting tweet after %d dedup retries (overlaps: %s)",
                       self._job_name(), max_retries, overlap)
        return tweet

    def _mark_posted_with_topics(self, tweet_id, job_type, content, topic=None):
        """Mark posted and extract+store data topics from content."""
        from bot.ai.topic_extractor import extract_data_topics
        data_topics = extract_data_topics(content)
        variant = getattr(self.generator, 'last_variant', None) or ""
        self.store.mark_posted(tweet_id, job_type, content, topic=topic,
                               data_topics=data_topics, variant=variant)

    @staticmethod
    def _should_use_thread(days=(0,)) -> bool:
        """Return True if today (UTC weekday) is in *days*. 0=Mon, 4=Fri."""
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).weekday() in days

    def _job_name(self) -> str:
        return self.__class__.__name__.replace("Job", "")

    # Class-level dedup guard — shared intentionally so no job instance can
    # double-schedule the same tweet_id.  Lock protects the set.
    _pending_followups: set = set()
    _followup_lock = threading.Lock()

    def _schedule_followup(self, tweet_id: str, context: str, tweet_type: str,
                           tweet_text: str = ""):
        """Schedule an auto-reply to our own tweet after a random delay.

        Only fires ~60% of the time to avoid robotic patterns.
        Delay is randomized between 5-15 minutes to look natural.
        """
        if tweet_type not in _FOLLOWUP_TYPES:
            return
        # Roll the dice — not every tweet needs a followup
        if random.random() > _FOLLOWUP_PROBABILITY:
            logger.info("Skipping followup for %s (random skip, p=%.0f%%)",
                        tweet_id, (1 - _FOLLOWUP_PROBABILITY) * 100)
            return
        with BaseJob._followup_lock:
            if tweet_id in BaseJob._pending_followups:
                logger.debug("Followup already scheduled for %s, skipping", tweet_id)
                return
            BaseJob._pending_followups.add(tweet_id)

        def _do_followup():
            try:
                # DB-based dedup: survives process restarts
                dedup_key = f"followup_for_{tweet_id}"
                if self.store.is_posted(dedup_key):
                    logger.debug("Followup already posted for %s, skipping", tweet_id)
                    return
                recent = self.get_recent_tweets()
                tweet_ref = f'Your tweet that was posted: "{tweet_text[:280]}"\n' if tweet_text else ""
                followup_context = (
                    f'{tweet_ref}'
                    f'Data context: "{context[:300]}"\n'
                    f"Reply to YOUR OWN tweet above with one more angle on the SAME specific topic. "
                    f"Do NOT restate what the tweet already said. Add a 'what to watch' or one comparison. "
                    f"Keep it to 1-2 sentences. Do NOT introduce new data points not in the context."
                )
                reply_text = self.generator.generate_tweet(followup_context, "self_followup", recent)
                if not reply_text:
                    return
                reply_id = self.poster.post_reply(reply_text, tweet_id)
                if reply_id:
                    self.store.mark_posted(dedup_key, "self_followup", "")
                    self.store.mark_posted(reply_id, "self_followup", reply_text)
                    logger.info("Auto followup posted to %s → %s", tweet_id, reply_id)
            except Exception as exc:
                logger.warning("Auto followup failed for %s: %s", tweet_id, exc)
                if self.notifier:
                    try:
                        self.notifier.send(f"⚠️ Followup for {tweet_id} failed: {str(exc)[:100]}")
                    except Exception:
                        pass
            finally:
                with BaseJob._followup_lock:
                    BaseJob._pending_followups.discard(tweet_id)

        delay = random.randint(_FOLLOWUP_DELAY_MIN, _FOLLOWUP_DELAY_MAX)
        timer = threading.Timer(delay, _do_followup)
        timer.daemon = True
        timer.start()
        logger.info("Scheduled auto followup for %s in %ds", tweet_id, delay)

    def run(self):
        try:
            result = self.execute()
            if result and self.notifier:
                self.notifier.notify_success(self._job_name(), result)
                # Syndicate to public Telegram channel
                self._publish_to_channel(result)
            return result
        except Exception as e:
            logger.exception("Job %s failed: %s", self.__class__.__name__, e)
            if self.notifier:
                self.notifier.notify_failure(self._job_name(), e)
            return None

    def _publish_to_channel(self, result):
        """Syndicate posted content to the public Telegram channel."""
        if not self.notifier or not getattr(self.notifier, 'channel_id', ''):
            return
        job_name = self._job_name()
        try:
            if isinstance(result, list):
                # Thread (alpha_thread) or multi-tweet — get content from store
                contents = []
                for tid in result:
                    c = self.store.get_content_by_id(tid) if self.store else None
                    if c:
                        contents.append(c)
                if contents:
                    self.notifier.publish_thread_to_channel(job_name, contents, result)
            elif isinstance(result, str):
                # Single tweet
                content = self.store.get_content_by_id(result) if self.store else None
                if content:
                    self.notifier.publish_to_channel(job_name, content, result)
        except Exception as exc:
            logger.warning("Channel publish failed for %s: %s", job_name, exc)

    def execute(self):
        raise NotImplementedError
