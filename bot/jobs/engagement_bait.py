# jobs/engagement_bait.py
import logging
from bot.jobs.base import BaseJob

logger = logging.getLogger(__name__)


class EngagementBaitJob(BaseJob):
    def __init__(self, store, generator, poster, prices=None,
                 exchange_flows=None, polymarket=None, notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.prices = prices
        self.exchange_flows = exchange_flows
        self.polymarket = polymarket

    def execute(self):
        context_parts = []
        if self.prices:
            try:
                price_data = self.prices.get_crypto_prices()
                s = self.prices.format_crypto_summary(price_data)
                if s:
                    context_parts.append(f"Market: {s}")
            except Exception as exc:
                logger.debug("EngagementBait prices failed: %s", exc)
        if self.exchange_flows:
            try:
                snap = self.exchange_flows.get_exchange_snapshot()
                s = self._safe_format(self.exchange_flows, snap)
                if s:
                    context_parts.append(f"Exchange flows: {s}")
            except Exception as exc:
                logger.debug("EngagementBait exchange flows failed: %s", exc)
        if self.polymarket:
            try:
                poly_data = self.polymarket.get_polymarket_snapshot()
                s = self._safe_format(self.polymarket, poly_data)
                if s:
                    context_parts.append(s)
            except Exception as exc:
                logger.debug("EngagementBait polymarket failed: %s", exc)

        context = "\n".join(context_parts) if context_parts else "Current crypto market data"

        recent = self.get_recent_tweets()
        top = self.get_top_performers(job_type="engagement_bait")
        bottom = self.get_bottom_performers(job_type="engagement_bait")
        patterns = self.get_performance_patterns()
        tweet = self._generate_with_dedup(
            context, "engagement_bait", recent,
            top=top, bottom=bottom, performance_patterns=patterns,
        )
        if not tweet:
            return None
        self._validate_output(context, tweet, "engagement_bait")
        tweet_id = self.poster.post_tweet(tweet)
        if not tweet_id:
            raise RuntimeError("post_tweet returned None — Twitter API rejected or unavailable")
        self._mark_posted_with_topics(tweet_id, "engagement_bait", tweet)
        return tweet_id
