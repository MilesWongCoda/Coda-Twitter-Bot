# jobs/weekly_poll.py
import logging
import re
from bot.jobs.base import BaseJob
from bot.data.prices import PriceFetcher

logger = logging.getLogger(__name__)


class WeeklyPollJob(BaseJob):
    def __init__(self, store, generator, poster, prices: PriceFetcher,
                 etf_flows=None, exchange_flows=None, notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)
        self.prices = prices
        self.etf_flows = etf_flows
        self.exchange_flows = exchange_flows

    def execute(self):
        if self.store.topic_on_cooldown("weekly_poll", cooldown_hours=144):  # ~6 days
            return None

        # Gather context: BTC weekly change + flow data for poll framing
        parts = []
        weekly = self.prices.get_weekly_changes(["bitcoin"])
        if weekly and isinstance(weekly[0], dict):
            ch = weekly[0].get("price_change_percentage_7d_in_currency")
            if ch is not None:
                parts.append(f"$BTC is {ch:+.1f}% this week.")

        if self.etf_flows:
            s = self._safe_format(self.etf_flows, self.etf_flows.get_etf_snapshot())
            if s:
                parts.append(f"ETF flows: {s}")
        if self.exchange_flows:
            s = self._safe_format(self.exchange_flows, self.exchange_flows.get_exchange_snapshot())
            if s:
                parts.append(f"Exchange flows: {s}")

        data_ctx = " ".join(parts) + " " if parts else ""
        context = f"{data_ctx}Create a flow-themed market poll about where capital will move next week."
        recent = self.get_recent_tweets()
        top = self.get_top_performers(job_type="weekly_poll")
        bottom = self.get_bottom_performers(job_type="weekly_poll")
        raw = self.generator.generate_tweet(context, "weekly_poll", recent, top_performers=top, bottom_performers=bottom)
        if not raw:
            return None

        # Parse: first line = question, lines starting with A:/B:/C:/D: = options
        lines = [l.strip() for l in raw.strip().split("\n") if l.strip()]
        question = None
        options = []
        for line in lines:
            match = re.match(r'^[A-D]:\s*(.+)', line)
            if match:
                opt = match.group(1).strip()[:25]  # Twitter poll max 25 chars
                options.append(opt)
            elif question is None:
                question = line[:240]  # leave headroom for Twitter poll metadata

        if not question or len(options) < 2:
            logger.warning("WeeklyPoll: failed to parse poll from AI output: %s", raw[:200])
            if self.notifier:
                self.notifier.send(f"⚠️ <b>WeeklyPoll</b> parse failed — AI output malformed")
            return None

        # Ensure exactly 2-4 options
        options = options[:4]

        tweet_id = self.poster.post_poll(question, options, duration_minutes=1440)
        if not tweet_id:
            raise RuntimeError("post_poll returned None")
        self.store.mark_posted(tweet_id, "weekly_poll", f"{question}\n" + "\n".join(options), topic="weekly_poll")
        return tweet_id
