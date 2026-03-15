# jobs/alpha_thread.py
import logging
import os
from bot.ai.summarize import Summarizer
from bot.data.news import NewsFetcher
from bot.data.onchain import OnChainFetcher
from bot.data.prices import PriceFetcher
from bot.jobs.base import BaseJob

logger = logging.getLogger(__name__)


class AlphaThreadJob(BaseJob):
    def __init__(self, store, generator, poster, summarizer: Summarizer,
                 news: NewsFetcher, onchain: OnChainFetcher, prices: PriceFetcher,
                 derivatives=None, etf_flows=None, exchange_flows=None,
                 whale_alerts=None, polymarket=None, synthesizer_data=None,
                 notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.summarizer = summarizer
        self.news = news
        self.onchain = onchain
        self.prices = prices
        self.derivatives = derivatives
        self.etf_flows = etf_flows
        self.exchange_flows = exchange_flows
        self.whale_alerts = whale_alerts
        self.polymarket = polymarket
        self.synthesizer_data = synthesizer_data

    def execute(self):
        # 36-hour cooldown: Mon/Wed/Fri spaced 48h apart, prevents double-post on restart
        if self.store.topic_on_cooldown("alpha_thread", cooldown_hours=36):
            return None
        articles = self.news.fetch_all()
        if not articles:
            return None
        onchain_data = self.onchain.get_btc_snapshot()
        price_data = self.prices.get_crypto_prices()
        onchain_summary = self.onchain.format_summary(onchain_data) if onchain_data else "On-chain data unavailable"
        price_summary = self.prices.format_crypto_summary(price_data) or "Price data unavailable"
        news_summary = self.summarizer.summarize(articles[:5])
        deriv_summary = ""
        if self.derivatives:
            deriv_data = self.derivatives.get_btc_snapshot()
            deriv_summary = self._safe_format(self.derivatives, deriv_data)
        deriv_line = f"\nDerivatives: {deriv_summary}" if deriv_summary else ""

        # Fund flow data
        flow_parts = []
        if self.etf_flows:
            etf_data = self.etf_flows.get_etf_snapshot()
            s = self._safe_format(self.etf_flows, etf_data)
            if s:
                flow_parts.append(s)
        if self.exchange_flows:
            exch_data = self.exchange_flows.get_exchange_snapshot()
            s = self._safe_format(self.exchange_flows, exch_data)
            if s:
                flow_parts.append(s)
        if self.whale_alerts:
            whale_data = self.whale_alerts.get_whale_snapshot()
            s = self._safe_format(self.whale_alerts, whale_data)
            if s:
                flow_parts.append(s)
        flow_line = f"\nFund flows: {' | '.join(flow_parts)}" if flow_parts else ""

        poly_line = ""
        if self.polymarket:
            s = self._safe_format(self.polymarket, self.polymarket.get_polymarket_snapshot())
            if s:
                poly_line = f"\n{s}"

        # Cross-source synthesis
        synthesis_block = ""
        if self.synthesizer_data:
            try:
                synthesis = self.synthesizer_data.synthesize(
                    price_data=price_data, fear_greed=self.prices.get_fear_greed(),
                )
                if synthesis:
                    synthesis_block = f"\n{synthesis}"
            except Exception as exc:
                logger.warning("Data synthesis failed: %s", exc)

        context = f"""Capital flows thread context:{synthesis_block}
Prices: {price_summary}
On-chain: {onchain_summary}{deriv_line}{flow_line}{poly_line}
News (extract any flow-related signals — ETF filings, fund allocations, institutional moves): {news_summary}
Write a 5-tweet capital flows thread showing where smart money is moving."""
        num_tweets = 5
        recent = self.get_recent_tweets()
        tweets = self.generator.generate_thread(context, num_tweets=num_tweets, recent_tweets=recent)
        if not tweets:
            return None
        for t in tweets:
            self._validate_output(context, t, "alpha_thread")
        min_tweets = num_tweets - 1  # allow one fewer tweet than requested
        if len(tweets) < min_tweets:
            logger.warning("generate_thread returned %d tweets (need >=%d), aborting",
                           len(tweets), min_tweets)
            return None
        # Trim to requested length if AI generated extra
        tweets = tweets[:num_tweets]
        # Generate branded card for first tweet
        first_media = None
        try:
            from bot.data.charts import generate_market_card
            fg_raw = self.prices.get_fear_greed()
            chart_path = generate_market_card(price_data, fg_raw, "alpha_thread")
            if chart_path:
                try:
                    mid = self.poster.upload_image_from_file(chart_path)
                    if mid:
                        first_media = [mid]
                finally:
                    os.unlink(chart_path)
        except Exception as exc:
            self._warn_chart_failure(exc)

        tweet_ids = self.poster.post_thread(tweets, first_tweet_media_ids=first_media)
        if not tweet_ids:
            raise RuntimeError("post_thread returned no IDs — all tweets failed to post")
        if len(tweet_ids) < len(tweets):
            # Partial failure: try to complete the thread from where we left off
            logger.warning("Alpha thread partially posted (%d/%d), attempting to continue",
                           len(tweet_ids), len(tweets))
            remaining = tweets[len(tweet_ids):]
            last_id = tweet_ids[-1]
            for remaining_tweet in remaining:
                try:
                    from bot.twitter.poster import _retry
                    resp = _retry(lambda t=remaining_tweet, rid=last_id:
                                  self.poster.client.create_tweet(text=t, in_reply_to_tweet_id=rid))
                    if resp and resp.data:
                        new_id = str(resp.data["id"])
                        tweet_ids.append(new_id)
                        last_id = new_id
                    else:
                        break
                except Exception as exc:
                    logger.error("Thread continuation failed at tweet %d: %s", len(tweet_ids) + 1, exc)
                    break
            # Store whatever we managed to post (with topic so cooldown fires)
            for tweet_id, tweet_text in zip(tweet_ids, tweets):
                self.store.mark_posted(tweet_id, "alpha_thread", tweet_text, topic="alpha_thread")
            if len(tweet_ids) < len(tweets):
                # Still incomplete — cooldown already set via topic, prevent orphan retry
                raise RuntimeError(
                    f"Alpha thread partially posted ({len(tweet_ids)}/{len(tweets)}) — "
                    f"stored posted tweets, cooldown set to prevent orphan retry"
                )
            return tweet_ids  # recovered partial — skip the duplicate loop below
        for tweet_id, tweet_text in zip(tweet_ids, tweets):
            self.store.mark_posted(tweet_id, "alpha_thread", tweet_text, topic="alpha_thread")
        return tweet_ids
