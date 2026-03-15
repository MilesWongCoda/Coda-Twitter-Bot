# jobs/morning_brief.py
import logging
import os
from bot.jobs.base import BaseJob
from bot.data.news import NewsFetcher
from bot.data.prices import PriceFetcher
from bot.ai.summarize import Summarizer

logger = logging.getLogger(__name__)


class MorningBriefJob(BaseJob):
    def __init__(self, store, generator, poster, summarizer: Summarizer,
                 news: NewsFetcher, prices: PriceFetcher, trends=None,
                 etf_flows=None, exchange_flows=None, macro=None,
                 synthesizer_data=None, polymarket=None, notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.summarizer = summarizer
        self.news = news
        self.prices = prices
        self.trends = trends
        self.etf_flows = etf_flows
        self.exchange_flows = exchange_flows
        self.macro = macro
        self.synthesizer_data = synthesizer_data
        self.polymarket = polymarket

    def execute(self):
        if self.store.topic_on_cooldown("morning_brief", cooldown_hours=20):
            return None
        articles = self.news.fetch_all()
        if not articles:
            return None
        price_data = self.prices.get_crypto_prices()
        price_summary = self.prices.format_crypto_summary(price_data) or "Price data unavailable"
        fg_raw = self.prices.get_fear_greed()
        fg = self.prices.format_fear_greed(fg_raw)

        news_summary = self.summarizer.summarize(articles[:5])
        parts = [f"Morning briefing. Prices: {price_summary}"]
        if fg:
            parts.append(fg)
        parts.append(f"Key headlines: {news_summary}")

        if self.etf_flows:
            s = self._safe_format(self.etf_flows, self.etf_flows.get_etf_snapshot())
            if s:
                parts.append(f"ETF flows: {s}")
        if self.exchange_flows:
            exch_data = self.exchange_flows.get_exchange_snapshot_multi()
            s = self.exchange_flows.format_multi_summary(exch_data)
            if s:
                parts.append(f"Exchange flows: {s}")

        if self.polymarket:
            poly_summary = self._safe_format(self.polymarket, self.polymarket.get_polymarket_snapshot())
            if poly_summary:
                parts.append(f"Prediction markets: {poly_summary}")

        macro_data = None
        if self.macro:
            try:
                macro_data = self.macro.get_macro_snapshot()
                macro_summary = self.macro.format_summary(macro_data)
                if macro_summary:
                    parts.append(f"Macro: {macro_summary}")
            except Exception as exc:
                logger.warning("Macro data fetch failed: %s", exc)

        # Cross-source synthesis
        if self.synthesizer_data:
            try:
                synthesis = self.synthesizer_data.synthesize(
                    price_data=price_data, fear_greed=fg_raw,
                    macro_data=macro_data,
                )
                if synthesis:
                    parts.insert(0, synthesis)
            except Exception as exc:
                logger.warning("Data synthesis failed: %s", exc)

        # Trending coins — cashtag diversity
        try:
            trending_summary = self.prices.format_trending_summary()
            if trending_summary and isinstance(trending_summary, str):
                parts.append(trending_summary)
        except Exception:
            pass

        context = "\n".join(parts)
        context, trending_tags = self._enrich_with_trends(context)

        recent = self.get_recent_tweets()
        top = self.get_top_performers(job_type="morning_brief")
        bottom = self.get_bottom_performers(job_type="morning_brief")
        patterns = self.get_performance_patterns()

        # Chart generation (both paths may use it)
        media_ids = None
        try:
            from bot.data.charts import generate_market_card
            chart_path = generate_market_card(price_data, fg_raw, "morning_brief")
            if chart_path:
                try:
                    mid = self.poster.upload_image_from_file(chart_path)
                    if mid:
                        media_ids = [mid]
                finally:
                    os.unlink(chart_path)
        except Exception as exc:
            self._warn_chart_failure(exc)

        # Monday: mini-thread mode (week open)
        if self._should_use_thread(days=(0,)):
            try:
                tweets = self.generator.generate_mini_thread(
                    context, "morning_brief", num_tweets=3,
                    recent_tweets=recent, top_performers=top, bottom_performers=bottom,
                    trending_tags=trending_tags, performance_patterns=patterns,
                )
                if tweets and len(tweets) >= 2:
                    for t in tweets:
                        self._validate_output(context, t, "morning_brief")
                    tweet_ids = self.poster.post_thread(tweets, first_tweet_media_ids=media_ids)
                    if tweet_ids:
                        for tid, txt in zip(tweet_ids, tweets):
                            self._mark_posted_with_topics(tid, "morning_brief", txt, topic="morning_brief")
                        self._schedule_followup(tweet_ids[0], context, "morning_brief", tweet_text=tweets[0])
                        return tweet_ids
            except Exception as exc:
                logger.warning("Mini-thread failed for morning_brief, falling back to single: %s", exc)

        # Single tweet (default or fallback)
        tweet = self._generate_with_dedup(context, "morning_brief", recent, top=top, bottom=bottom, trending_tags=trending_tags, performance_patterns=patterns)
        if not tweet:
            return None
        self._validate_output(context, tweet, "morning_brief")
        tweet_id = self.poster.post_tweet(tweet, media_ids=media_ids)
        if not tweet_id:
            raise RuntimeError("post_tweet returned None — Twitter API rejected or unavailable")
        self._mark_posted_with_topics(tweet_id, "morning_brief", tweet, topic="morning_brief")
        self._schedule_followup(tweet_id, context, "morning_brief", tweet_text=tweet)
        return tweet_id
