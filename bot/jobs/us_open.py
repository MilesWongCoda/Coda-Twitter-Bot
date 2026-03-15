# jobs/us_open.py
import logging
import os
from bot.jobs.base import BaseJob
from bot.data.news import NewsFetcher
from bot.data.prices import PriceFetcher
from bot.ai.summarize import Summarizer

logger = logging.getLogger(__name__)


class USOpenJob(BaseJob):
    def __init__(self, store, generator, poster, summarizer: Summarizer,
                 news: NewsFetcher, prices: PriceFetcher, trends=None,
                 etf_flows=None, exchange_flows=None, polymarket=None,
                 macro=None, notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.summarizer = summarizer
        self.news = news
        self.prices = prices
        self.trends = trends
        self.etf_flows = etf_flows
        self.exchange_flows = exchange_flows
        self.polymarket = polymarket
        self.macro = macro

    def execute(self):
        if self.store.topic_on_cooldown("us_open", cooldown_hours=20):
            return None
        articles = self.news.fetch_all()
        if not articles:
            return None
        price_data = self.prices.get_crypto_prices()
        price_summary = self.prices.format_crypto_summary(price_data) or "Price data unavailable"

        news_summary = self.summarizer.summarize(articles[:3])
        parts = [f"US market about to open. Crypto: {price_summary}"]
        parts.append(f"Macro news: {news_summary}")

        if self.etf_flows:
            s = self._safe_format(self.etf_flows, self.etf_flows.get_etf_snapshot())
            if s:
                parts.append(f"ETF pre-market: {s}")
        if self.exchange_flows:
            exch_data = self.exchange_flows.get_exchange_snapshot_multi()
            s = self.exchange_flows.format_multi_summary(exch_data)
            if s:
                parts.append(f"Exchange flows: {s}")

        if self.polymarket:
            s = self._safe_format(self.polymarket, self.polymarket.get_polymarket_snapshot())
            if s:
                parts.append(s)

        if self.macro:
            try:
                macro_data = self.macro.get_macro_snapshot()
                macro_summary = self.macro.format_summary(macro_data)
                if macro_summary:
                    parts.append(f"Macro: {macro_summary}")
            except Exception as exc:
                logger.warning("Macro data fetch failed: %s", exc)

        # Trending coins — cashtag diversity
        try:
            trending_summary = self.prices.format_trending_summary()
            if trending_summary and isinstance(trending_summary, str):
                parts.append(trending_summary)
        except Exception:
            pass

        context = "\n\n".join(parts)
        context, trending_tags = self._enrich_with_trends(context)

        recent = self.get_recent_tweets()
        top = self.get_top_performers(job_type="us_open")
        bottom = self.get_bottom_performers(job_type="us_open")
        patterns = self.get_performance_patterns()
        tweet = self._generate_with_dedup(context, "us_open", recent, top=top, bottom=bottom, trending_tags=trending_tags, performance_patterns=patterns)
        if not tweet:
            return None
        self._validate_output(context, tweet, "us_open")

        # Try BTC 1-day candlestick chart (after tweet gen to avoid orphaned media)
        media_ids = None
        try:
            from bot.data.charts import generate_candlestick_chart
            ohlc = self.prices.get_ohlc("bitcoin", days=1)
            chart_path = generate_candlestick_chart(ohlc, "bitcoin", 1) if ohlc else None
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
            raise RuntimeError("post_tweet returned None — Twitter API rejected or unavailable")
        self._mark_posted_with_topics(tweet_id, "us_open", tweet, topic="us_open")
        self._schedule_followup(tweet_id, context, "us_open", tweet_text=tweet)
        return tweet_id
