# jobs/weekly_recap.py
import logging
import os
from bot.jobs.base import BaseJob
from bot.data.prices import PriceFetcher

logger = logging.getLogger(__name__)


class WeeklyRecapJob(BaseJob):
    def __init__(self, store, generator, poster, prices: PriceFetcher,
                 etf_flows=None, exchange_flows=None, whale_alerts=None,
                 notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.prices = prices
        self.etf_flows = etf_flows
        self.exchange_flows = exchange_flows
        self.whale_alerts = whale_alerts

    def execute(self):
        if self.store.topic_on_cooldown("weekly_recap", cooldown_hours=144):  # ~6 days
            return None

        weekly = self.prices.get_weekly_changes()
        if not weekly:
            return None

        # Build context from weekly data
        parts = []
        best, worst = None, None
        best_ch, worst_ch = float('-inf'), float('inf')
        for coin in weekly:
            symbol = (coin.get("symbol") or "").upper()
            price = coin.get("current_price", 0) or 0
            ch_raw = coin.get("price_change_percentage_7d_in_currency")
            if ch_raw is None:
                continue  # skip coins with no 7d data
            ch = float(ch_raw)
            if price >= 100:
                price_str = f"${price:,.0f}"
            elif price >= 1:
                price_str = f"${price:,.2f}"
            else:
                price_str = f"${price:.4f}"
            parts.append(f"${symbol}: {price_str} ({ch:+.1f}% 7d)")
            if ch > best_ch:
                best_ch, best = ch, symbol
            if ch < worst_ch:
                worst_ch, worst = ch, symbol

        context = f"Weekly performance: {' | '.join(parts)}"
        if best and worst:
            context += f"\nBest: ${best} ({best_ch:+.1f}%), Worst: ${worst} ({worst_ch:+.1f}%)"

        if self.etf_flows:
            s = self._safe_format(self.etf_flows, self.etf_flows.get_etf_snapshot())
            if s:
                context += f"\nETF flows: {s}"
        if self.exchange_flows:
            s = self._safe_format(self.exchange_flows, self.exchange_flows.get_exchange_snapshot())
            if s:
                context += f"\nExchange flows: {s}"
        if self.whale_alerts:
            s = self._safe_format(self.whale_alerts, self.whale_alerts.get_whale_snapshot())
            if s:
                context += f"\nWhale activity: {s}"

        recent = self.get_recent_tweets()
        top = self.get_top_performers(job_type="weekly_recap")
        bottom = self.get_bottom_performers(job_type="weekly_recap")
        tweet = self.generator.generate_tweet(context, "weekly_recap", recent, top_performers=top, bottom_performers=bottom)
        if not tweet:
            return None
        self._validate_output(context, tweet, "weekly_recap")

        # Generate weekly scorecard image (after tweet generation to avoid orphaned media)
        media_ids = None
        try:
            from bot.data.charts import generate_weekly_scorecard
            chart_path = generate_weekly_scorecard(weekly)
            if chart_path:
                try:
                    mid = self.poster.upload_image_from_file(chart_path)
                    if mid:
                        media_ids = [mid]
                finally:
                    os.unlink(chart_path)
        except Exception as exc:
            self._warn_chart_failure(exc)

        tweet_id = self.poster.post_tweet(tweet, media_ids=media_ids)
        if not tweet_id:
            raise RuntimeError("post_tweet returned None")
        self.store.mark_posted(tweet_id, "weekly_recap", tweet, topic="weekly_recap")
        return tweet_id
