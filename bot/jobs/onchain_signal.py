# jobs/onchain_signal.py
import logging
import os
from bot.jobs.base import BaseJob
from bot.data.onchain import OnChainFetcher

logger = logging.getLogger(__name__)


class OnChainSignalJob(BaseJob):
    def __init__(self, store, generator, poster, onchain: OnChainFetcher,
                 derivatives=None, exchange_flows=None, whale_alerts=None,
                 synthesizer_data=None, notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.onchain = onchain
        self.derivatives = derivatives
        self.exchange_flows = exchange_flows
        self.whale_alerts = whale_alerts
        self.synthesizer_data = synthesizer_data

    def execute(self):
        if self.store.topic_on_cooldown("onchain", cooldown_hours=6):
            return None
        multi_chain = self.onchain.get_multi_chain_snapshot()
        data = multi_chain.get("btc", {})  # BTC snapshot for chart generation
        if not data and not multi_chain.get("eth"):
            return None

        deriv_data = None
        deriv_summary = ""
        exch_summary = ""
        context = self.onchain.format_multi_summary(multi_chain)
        if self.derivatives:
            deriv_multi = self.derivatives.get_multi_snapshot()
            deriv_data = deriv_multi.get("btc", {})  # for chart generation
            deriv_summary = self.derivatives.format_multi_summary(deriv_multi)
            if deriv_summary:
                context += f"\nDerivatives: {deriv_summary}"

        if self.exchange_flows:
            exch_data = self.exchange_flows.get_exchange_snapshot_multi()
            exch_summary = self.exchange_flows.format_multi_summary(exch_data)
            if exch_summary:
                context += f"\nExchange flows: {exch_summary}"

        if self.whale_alerts:
            whale_data = self.whale_alerts.get_whale_snapshot()
            whale_summary = self._safe_format(self.whale_alerts, whale_data)
            if whale_summary:
                context += f"\nWhale activity: {whale_summary}"

        # Cross-source synthesis
        if self.synthesizer_data:
            try:
                synthesis = self.synthesizer_data.synthesize(
                    exchange_data=exch_summary if self.exchange_flows else "",
                    derivatives_data=deriv_summary if self.derivatives else "",
                )
                if synthesis:
                    context = f"{synthesis}\n\n{context}"
            except Exception as exc:
                logger.warning("Data synthesis failed: %s", exc)

        recent = self.get_recent_tweets()
        top = self.get_top_performers(job_type="onchain_signal")
        bottom = self.get_bottom_performers(job_type="onchain_signal")
        patterns = self.get_performance_patterns()

        # Chart generation (both paths may use it)
        media_ids = None
        try:
            from bot.data.charts import generate_onchain_card
            chart_path = generate_onchain_card(data, deriv_data, "onchain_signal")
            if chart_path:
                try:
                    mid = self.poster.upload_image_from_file(chart_path)
                    if mid:
                        media_ids = [mid]
                finally:
                    os.unlink(chart_path)
        except Exception as exc:
            self._warn_chart_failure(exc)

        # Mon/Wed/Fri: mini-thread mode
        if self._should_use_thread():
            try:
                tweets = self.generator.generate_mini_thread(
                    context, "onchain_signal", num_tweets=3,
                    recent_tweets=recent, top_performers=top, bottom_performers=bottom,
                    performance_patterns=patterns,
                )
                if tweets and len(tweets) >= 2:
                    for t in tweets:
                        self._validate_output(context, t, "onchain_signal")
                    tweet_ids = self.poster.post_thread(tweets, first_tweet_media_ids=media_ids)
                    if tweet_ids:
                        for tid, txt in zip(tweet_ids, tweets):
                            self._mark_posted_with_topics(tid, "onchain_signal", txt, topic="onchain")
                        self._schedule_followup(tweet_ids[0], context, "onchain_signal", tweet_text=tweets[0])
                        return tweet_ids
            except Exception as exc:
                logger.warning("Mini-thread failed for onchain_signal, falling back to single: %s", exc)

        # Single tweet (default or fallback)
        tweet = self._generate_with_dedup(context, "onchain_signal", recent, top=top, bottom=bottom, performance_patterns=patterns)
        if not tweet:
            return None
        self._validate_output(context, tweet, "onchain_signal")
        tweet_id = self.poster.post_tweet(tweet, media_ids=media_ids)
        if not tweet_id:
            raise RuntimeError("post_tweet returned None — Twitter API rejected or unavailable")
        self._mark_posted_with_topics(tweet_id, "onchain_signal", tweet, topic="onchain")
        self._schedule_followup(tweet_id, context, "onchain_signal", tweet_text=tweet)
        return tweet_id
