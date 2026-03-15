# jobs/pinned_thread.py
"""One-shot job: post "Why I track flows" pinned thread with live data."""
import logging

from bot.config import BOT_HANDLE

logger = logging.getLogger(__name__)


class PinnedThreadJob:
    def __init__(self, poster, prices, etf_flows, exchange_flows, notifier=None):
        self.poster = poster
        self.prices = prices
        self.etf_flows = etf_flows
        self.exchange_flows = exchange_flows
        self.notifier = notifier

    def _get_etf_line(self) -> str:
        try:
            snap = self.etf_flows.get_etf_snapshot() if self.etf_flows else {}
            summary = self.etf_flows.format_summary(snap) if snap else ""
        except Exception as exc:
            logger.warning("PinnedThread: ETF data fetch failed: %s", exc)
            summary = ""
        if summary:
            return f"Latest data point — {summary}.\n\n"
        return ""

    def _get_exchange_line(self) -> str:
        try:
            snap = self.exchange_flows.get_exchange_snapshot() if self.exchange_flows else {}
            summary = self.exchange_flows.format_summary(snap) if snap else ""
        except Exception as exc:
            logger.warning("PinnedThread: Exchange data fetch failed: %s", exc)
            summary = ""
        if summary:
            return f"Right now: {summary}.\n\n"
        return ""

    def _get_fear_greed_line(self) -> str:
        try:
            fg = self.prices.get_fear_greed() if self.prices else {}
            summary = self.prices.format_fear_greed(fg) if fg else ""
        except Exception as exc:
            logger.warning("PinnedThread: Fear & Greed fetch failed: %s", exc)
            summary = ""
        if summary:
            return f"Current sentiment: {summary}.\n\n"
        return ""

    def _build_tweets(self) -> list:
        etf_line = self._get_etf_line()
        exchange_line = self._get_exchange_line()
        fg_line = self._get_fear_greed_line()

        tweets = [
            # 1/7 — Hook
            (
                "Most traders watch price. Smart money watches flows.\n\n"
                "Price tells you what happened. Flows tell you what's about to happen.\n\n"
                "Here's the framework I use to track capital movement across BTC, ETH & macro \U0001f9f5"
            ),
            # 2/7 — ETF flows
            (
                "ETF inflows are the clearest institutional signal we have.\n\n"
                f"{etf_line}"
                "I track daily net flows across all spot BTC ETFs. "
                "When total net flow flips positive after a drawdown, I pay attention."
            ),
            # 3/7 — Exchange flows
            (
                "Exchange netflow = coins moving onto exchanges minus coins moving off.\n\n"
                f"{exchange_line}"
                "Sustained outflow = holders moving to cold storage. They're not selling.\n"
                "Sustained inflow = potential sell pressure building.\n\n"
                "This signal front-ran every major correction in 2024-25."
            ),
            # 4/7 — Whale wallets
            (
                "Whale wallets (1,000+ BTC) are the hardest to track but most telling.\n\n"
                "When whales accumulate during fear, the bottom is usually in.\n"
                "When whales distribute during euphoria, the top is usually in.\n\n"
                "Not always — but the hit rate is far better than sentiment polls."
            ),
            # 5/7 — Macro / rates
            (
                "None of this happens in a vacuum.\n\n"
                f"{fg_line}"
                "Fed rate decisions move liquidity. Liquidity moves risk assets.\n\n"
                "CPI prints, FOMC dots, treasury yields — these set the macro tide. "
                "Flows tell you who's positioning ahead of it."
            ),
            # 6/7 — Synthesis
            (
                "My daily process:\n\n"
                "\u2192 Check ETF net flows (institutional appetite)\n"
                "\u2192 Check exchange netflow (sell pressure or accumulation)\n"
                "\u2192 Check whale wallet movement (smart money positioning)\n"
                "\u2192 Overlay macro calendar (rate decisions, CPI, jobs)\n\n"
                "When 3+ signals align, conviction goes up."
            ),
            # 7/7 — CTA
            (
                "I post these signals daily — ETF flows, whale moves, exchange data, rate signals.\n\n"
                "Data first, opinions second.\n\n"
                f"Follow @{BOT_HANDLE} if you want the flow data before the price moves."
            ),
        ]
        return tweets

    def run(self):
        logger.info("PinnedThreadJob: building thread with live data")
        tweets = self._build_tweets()
        for i, t in enumerate(tweets, 1):
            logger.info("Thread %d/7 (%d chars): %s", i, len(t), t[:80])

        ids = self.poster.post_thread(tweets)
        if not ids:
            logger.error("PinnedThreadJob: post_thread returned no IDs")
            if self.notifier:
                self.notifier.send("\u274c Pinned thread failed — no tweets posted")
            return []

        first_url = f"https://x.com/{BOT_HANDLE}/status/{ids[0]}"
        logger.info("PinnedThreadJob: posted %d/%d tweets. First: %s", len(ids), len(tweets), first_url)
        if self.notifier:
            self.notifier.send(
                f"\U0001f9f5 <b>Pinned thread posted!</b> ({len(ids)}/{len(tweets)} tweets)\n\n"
                f"Go pin it: {first_url}\n\n"
                f"Profile \u2192 first tweet \u2192 \u22ee \u2192 Pin to your profile"
            )
        return ids
