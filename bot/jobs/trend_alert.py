# jobs/trend_alert.py
import logging
from datetime import datetime, timezone
from bot.jobs.base import BaseJob
from bot.data.prices import PriceFetcher

logger = logging.getLogger(__name__)

GIF_COOLDOWN_HOURS = 20  # max 1 GIF per day

# Cooldown per coin — prevents tweeting the same coin at both 09:00 and 18:00
ALERT_COOLDOWN_HOURS = 12


class TrendAlertJob(BaseJob):
    """Fixed-schedule trend alert: pick the top trending coin and tweet about it.

    Runs 2x/day (09:00 + 18:00 UTC). No delta detection — just "what's hot right now".
    Value comes from $CASHTAG searchability, not real-time breaking.
    """

    def __init__(self, store, generator, poster, prices: PriceFetcher,
                 news=None, exchange_flows=None, whale_alerts=None,
                 etf_flows=None, gif_fetcher=None, notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.prices = prices
        self.exchange_flows = exchange_flows
        self.whale_alerts = whale_alerts
        self.etf_flows = etf_flows
        self.gif_fetcher = gif_fetcher

    def _pick_trending_coin(self) -> dict | None:
        """Pick the top CoinGecko trending coin not on cooldown."""
        trending = self.prices.get_trending_coins()
        if not trending:
            return None
        for coin in trending:
            sym = coin.get("symbol", "")
            if not sym:
                continue
            topic = f"trend_alert_{sym.lower()}"
            if self.store.topic_on_cooldown(topic, cooldown_hours=ALERT_COOLDOWN_HOURS):
                continue
            return coin
        return None

    def execute(self):
        coin = self._pick_trending_coin()
        if not coin:
            logger.info("Trend alert: no eligible trending coin (all on cooldown or empty)")
            return None

        sym = coin["symbol"]
        name = coin["name"]
        rank = coin.get("market_cap_rank")
        rank_str = f" (rank #{rank})" if rank else ""

        # Build context
        context = f"{name} ({sym}) is trending on CoinGecko{rank_str}"

        recent = self.get_recent_tweets()
        top = self.get_top_performers(job_type="trend_alert")
        bottom = self.get_bottom_performers(job_type="trend_alert")
        patterns = self.get_performance_patterns()

        price_data = self.prices.get_crypto_prices()
        price_summary = self.prices.format_crypto_summary(price_data) if price_data else ""
        etf_summary = self._safe_format(self.etf_flows, self.etf_flows.get_etf_snapshot()) if self.etf_flows else ""
        flow_summary = self._safe_format(self.exchange_flows, self.exchange_flows.get_exchange_snapshot()) if self.exchange_flows else ""
        whale_summary = self._safe_format(self.whale_alerts, self.whale_alerts.get_whale_snapshot()) if self.whale_alerts else ""

        if price_summary:
            context += f"\n\nMarket snapshot: {price_summary}"
        if etf_summary:
            context += f"\n\nETF flows: {etf_summary}"
        if flow_summary:
            context += f"\n\nExchange flows: {flow_summary}"
        if whale_summary:
            context += f"\n\n{whale_summary}"

        tweet = self._generate_with_dedup(
            context, "trend_alert", recent,
            top=top, bottom=bottom,
            trending_tags=[f"${sym}"],
            performance_patterns=patterns,
        )
        if not tweet:
            return None
        self._validate_output(context, tweet, "trend_alert")

        # Attach GIF if quota available (1/day)
        media_ids = None
        if self.gif_fetcher and not self.store.topic_on_cooldown("gif_daily", GIF_COOLDOWN_HOURS):
            gif_url = self.gif_fetcher.fetch("excitement")
            if gif_url:
                mid = self.poster.upload_image_from_url(gif_url)
                if mid:
                    media_ids = [mid]
                    logger.info("GIF attached to trend alert for $%s", sym)

        try:
            tweet_id = self.poster.post_tweet(tweet, media_ids=media_ids)
        except Exception as exc:
            logger.error("Trend alert post failed for $%s: %s", sym, exc)
            return None
        if not tweet_id:
            return None

        topic = f"trend_alert_{sym.lower()}"
        self._mark_posted_with_topics(tweet_id, "trend_alert", tweet, topic=topic)
        self._schedule_followup(tweet_id, context, "trend_alert", tweet_text=tweet)
        # Mark GIF used for the day
        if media_ids:
            date_key = datetime.now(timezone.utc).strftime("gif_daily_%Y%m%d")
            self.store.mark_posted(date_key, "gif_marker", "", topic="gif_daily")
        logger.info("Trend alert posted for $%s", sym)
        return tweet_id
