# jobs/price_alert.py
import logging
import os
from bot.jobs.base import BaseJob
from bot.data.prices import PriceFetcher

logger = logging.getLogger(__name__)

# 1h price change thresholds per coin — large caps need smaller moves to be newsworthy
ALERT_THRESHOLDS = {
    "bitcoin": 2.5,
    "ethereum": 2.5,
    "solana": 3.5,
    "binancecoin": 3.5,
    "ripple": 3.5,
}
DEFAULT_THRESHOLD = 3.5
# Minimum 24h volume to filter out low-cap noise
MIN_VOLUME = 100_000_000
# Cooldown per coin (hours) — avoid spamming on volatile swings
ALERT_COOLDOWN_HOURS = 2
# Maximum alerts per day to prevent flooding
MAX_DAILY_ALERTS = 6


class PriceAlertJob(BaseJob):
    def __init__(self, store, generator, poster, prices: PriceFetcher,
                 exchange_flows=None, whale_alerts=None, notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.prices = prices
        self.exchange_flows = exchange_flows
        self.whale_alerts = whale_alerts

    def execute(self):
        today_count = self.store.count_posts_since_midnight("price_alert")
        if today_count >= MAX_DAILY_ALERTS:
            logger.debug("Price alert daily cap reached (%d/%d)", today_count, MAX_DAILY_ALERTS)
            return None

        markets = self.prices.get_prices_with_1h_change()
        if not markets:
            return None

        posted_ids = []
        for coin in markets:
            if today_count + len(posted_ids) >= MAX_DAILY_ALERTS:
                break
            coin_id = coin.get("id", "")
            symbol = (coin.get("symbol") or "").upper()
            price = coin.get("current_price", 0) or 0
            change_1h = coin.get("price_change_percentage_1h_in_currency") or 0
            change_24h = coin.get("price_change_percentage_24h_in_currency") or 0
            volume = coin.get("total_volume", 0) or 0

            threshold = ALERT_THRESHOLDS.get(coin_id, DEFAULT_THRESHOLD)
            if abs(change_1h) < threshold:
                continue
            if volume < MIN_VOLUME:
                continue

            # Per-coin cooldown using topic_on_cooldown
            topic = f"price_alert_{coin_id}"
            if self.store.topic_on_cooldown(topic, cooldown_hours=ALERT_COOLDOWN_HOURS):
                logger.debug("Price alert for %s on cooldown, skipping", coin_id)
                continue

            direction = "surged" if change_1h > 0 else "dropped"
            context = (
                f"${symbol} just {direction} {abs(change_1h):.1f}% in the last hour. "
                f"Current price: ${price:,.2f}. 24h change: {change_24h:+.1f}%. "
                f"24h volume: ${volume:,.0f}."
            )
            # Enrich with flow data so AI can connect the move to capital flows
            if self.exchange_flows:
                s = self._safe_format(self.exchange_flows, self.exchange_flows.get_exchange_snapshot())
                if s:
                    context += f" Exchange flows: {s}."
            if self.whale_alerts:
                s = self._safe_format(self.whale_alerts, self.whale_alerts.get_whale_snapshot())
                if s:
                    context += f" {s}."

            recent = self.get_recent_tweets()
            tweet = self.generator.generate_tweet(context, "price_alert", recent)
            if not tweet:
                continue
            self._validate_output(context, tweet, "price_alert")

            # Try to attach a 1-day candlestick chart
            media_ids = None
            try:
                from bot.data.charts import generate_candlestick_chart
                ohlc = self.prices.get_ohlc(coin_id, days=1)
                chart_path = generate_candlestick_chart(ohlc, coin_id, 1) if ohlc else None
                if chart_path:
                    try:
                        mid = self.poster.upload_image_from_file(chart_path)
                        if mid:
                            media_ids = [mid]
                    finally:
                        os.unlink(chart_path)
            except Exception as exc:
                self._warn_chart_failure(exc)

            try:
                tweet_id = self.poster.post_tweet(tweet, media_ids=media_ids)
            except Exception as exc:
                logger.error("Price alert post failed for %s: %s", symbol, exc)
                continue
            if tweet_id:
                self.store.mark_posted(tweet_id, "price_alert", tweet, topic=topic)
                posted_ids.append(tweet_id)
                logger.info("Price alert posted for %s: %+.1f%% 1h", symbol, change_1h)

        return posted_ids if posted_ids else None
