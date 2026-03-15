# main.py
import argparse
import html as _html
import logging
import os
import random
import signal
import tweepy
from apscheduler.schedulers.blocking import BlockingScheduler
from bot.config import Config, BOT_HANDLE
from bot.db.store import Store
from bot.data.news import NewsFetcher
from bot.data.prices import PriceFetcher
from bot.data.onchain import OnChainFetcher
from bot.data.trends import TrendsFetcher, DryRunTrendsFetcher
from bot.data.derivatives import DerivativesFetcher
from bot.data.etf_flows import ETFFlowFetcher, DryRunETFFlowFetcher
from bot.data.exchange_flows import ExchangeFlowFetcher, DryRunExchangeFlowFetcher
from bot.data.whale_alerts import WhaleAlertFetcher, DryRunWhaleAlertFetcher
from bot.data.polymarket import PolymarketFetcher, DryRunPolymarketFetcher
from bot.data.macro import MacroFetcher, DryRunMacroFetcher
from bot.data.synthesis import DataSynthesizer
from bot.ai.summarize import Summarizer
from bot.ai.generate import TweetGenerator
from bot.twitter.poster import Poster, DryRunPoster
from bot.twitter.engager import Engager, DryRunEngager
from bot.jobs.morning_brief import MorningBriefJob
from bot.jobs.hot_take import HotTakeJob
from bot.jobs.onchain_signal import OnChainSignalJob
from bot.jobs.us_open import USOpenJob
from bot.jobs.evening_wrap import EveningWrapJob
from bot.jobs.alpha_thread import AlphaThreadJob
from bot.jobs.engagement import EngagementJob
from bot.jobs.self_reply import SelfReplyJob
from bot.jobs.metrics_collector import MetricsCollectorJob
from bot.jobs.price_alert import PriceAlertJob
from bot.jobs.weekly_poll import WeeklyPollJob
from bot.jobs.weekly_recap import WeeklyRecapJob
from bot.jobs.trend_alert import TrendAlertJob
from bot.jobs.engagement_bait import EngagementBaitJob
from bot.jobs.pinned_thread import PinnedThreadJob
from bot.jobs.fng import FearGreedJob
from bot.data.gifs import GifFetcher, DryRunGifFetcher
from bot.notifications.telegram import TelegramNotifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

# Absolute path to the DB so it's found regardless of the working directory
_HERE = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DB = os.path.join(_HERE, "db", "kol.db")


def build_components(cfg: Config, dry_run: bool = False) -> dict:
    twitter_client = tweepy.Client(
        bearer_token=cfg.twitter_bearer_token,
        consumer_key=cfg.twitter_api_key,
        consumer_secret=cfg.twitter_api_secret,
        access_token=cfg.twitter_access_token,
        access_token_secret=cfg.twitter_access_token_secret,
        wait_on_rate_limit=True,
    )
    store = Store(":memory:") if dry_run else Store(_DEFAULT_DB)
    news = NewsFetcher()
    prices = PriceFetcher(cfg.coingecko_api_key)
    onchain = OnChainFetcher()
    derivatives = DerivativesFetcher(cfg.coinglass_api_key) if cfg.coinglass_api_key else None
    summarizer = Summarizer(cfg.openai_api_key)
    generator = TweetGenerator(cfg.anthropic_api_key)
    poster = DryRunPoster() if dry_run else Poster(
        client=twitter_client,
        api_key=cfg.twitter_api_key,
        api_secret=cfg.twitter_api_secret,
        access_token=cfg.twitter_access_token,
        access_token_secret=cfg.twitter_access_token_secret,
    )
    engager = DryRunEngager() if dry_run else Engager(client=twitter_client, store=store)
    trends = DryRunTrendsFetcher() if dry_run else TrendsFetcher(
        cfg.twitter_api_key, cfg.twitter_api_secret,
        cfg.twitter_access_token, cfg.twitter_access_token_secret,
    )
    macro = DryRunMacroFetcher() if dry_run else MacroFetcher()
    synthesizer = DataSynthesizer()
    if dry_run:
        etf_flows = DryRunETFFlowFetcher()
        exchange_flows = DryRunExchangeFlowFetcher()
        whale_alerts = DryRunWhaleAlertFetcher()
        polymarket = DryRunPolymarketFetcher()
    else:
        etf_flows = ETFFlowFetcher(cfg.coinglass_api_key) if cfg.coinglass_api_key else None
        exchange_flows = ExchangeFlowFetcher(cfg.coinglass_api_key) if cfg.coinglass_api_key else None
        whale_alerts = WhaleAlertFetcher(cfg.whale_alert_api_key) if cfg.whale_alert_api_key else None
        polymarket = PolymarketFetcher()
    gif_fetcher = DryRunGifFetcher() if dry_run else GifFetcher(cfg.giphy_api_key)
    return dict(
        store=store, news=news, prices=prices, onchain=onchain, derivatives=derivatives,
        summarizer=summarizer, generator=generator, poster=poster, engager=engager,
        trends=trends, etf_flows=etf_flows, exchange_flows=exchange_flows,
        whale_alerts=whale_alerts, polymarket=polymarket, macro=macro,
        synthesizer_data=synthesizer, gif_fetcher=gif_fetcher,
    )


def main():
    parser = argparse.ArgumentParser(description=f"@{BOT_HANDLE} Flow Tracker Bot")
    parser.add_argument("--dry-run", action="store_true",
                        help="Log tweets instead of posting; use in-memory DB (smoke test)")
    args = parser.parse_args()

    cfg = Config()
    if args.dry_run:
        logger.warning("=== DRY RUN MODE — no tweets will be posted, DB is in-memory ===")
    c = build_components(cfg, dry_run=args.dry_run)
    scheduler = BlockingScheduler(timezone="UTC", job_defaults={"jitter": 120})  # 0-2min random delay on all jobs

    notifier = TelegramNotifier(
        cfg.telegram_bot_token, cfg.telegram_chat_id,
        store=c["store"], channel_id=cfg.telegram_channel_id,
        openai_api_key=cfg.openai_api_key,
    ) if cfg.telegram_bot_token and cfg.telegram_chat_id else None
    if notifier:
        logger.info("Telegram notifications enabled")
        notifier.send(f"🤖 @{BOT_HANDLE} started — Flow Tracker scheduler running")
    else:
        logger.info("Telegram notifications disabled (TELEGRAM_BOT_TOKEN not set)")

    def _job(cls, keys):
        return cls(**{k: c[k] for k in keys}, notifier=notifier)

    morning    = _job(MorningBriefJob,  ["store", "generator", "poster", "summarizer", "news", "prices", "trends", "etf_flows", "exchange_flows", "macro", "synthesizer_data", "polymarket"])
    hot_take   = _job(HotTakeJob,       ["store", "generator", "poster", "polymarket"])
    onchain    = _job(OnChainSignalJob, ["store", "generator", "poster", "onchain", "derivatives", "exchange_flows", "whale_alerts", "synthesizer_data"])
    us_open    = _job(USOpenJob,        ["store", "generator", "poster", "summarizer", "news", "prices", "trends", "etf_flows", "exchange_flows", "polymarket", "macro"])
    wrap       = _job(EveningWrapJob,   ["store", "generator", "poster", "summarizer", "news", "prices", "trends", "etf_flows", "exchange_flows", "whale_alerts", "polymarket", "macro", "synthesizer_data"])
    thread     = _job(AlphaThreadJob,   ["store", "generator", "poster", "summarizer", "news", "onchain", "prices", "derivatives", "etf_flows", "exchange_flows", "whale_alerts", "polymarket", "synthesizer_data"])
    engagement = _job(EngagementJob,    ["store", "generator", "poster", "engager", "prices", "exchange_flows", "polymarket", "trends"])
    self_reply = _job(SelfReplyJob,    ["store", "generator", "poster", "engager", "prices", "exchange_flows", "polymarket"])
    metrics    = MetricsCollectorJob(store=c["store"], engager=c["engager"], notifier=notifier)
    price_alert = _job(PriceAlertJob, ["store", "generator", "poster", "prices", "exchange_flows", "whale_alerts"])
    weekly_poll = _job(WeeklyPollJob, ["store", "generator", "poster", "prices", "etf_flows", "exchange_flows"])
    weekly_recap = _job(WeeklyRecapJob, ["store", "generator", "poster", "prices", "etf_flows", "exchange_flows", "whale_alerts"])
    trend_alert = _job(TrendAlertJob, ["store", "generator", "poster", "prices", "exchange_flows", "whale_alerts", "etf_flows", "gif_fetcher"])
    engagement_bait = _job(EngagementBaitJob, ["store", "generator", "poster", "prices", "exchange_flows", "polymarket"])
    fng = _job(FearGreedJob, ["store", "generator", "poster"])

    # Daily heartbeat at 06:50 UTC — confirms scheduler is still alive and reports
    # yesterday's post count so operator can tell at a glance if posting is working.
    def _heartbeat():
        try:
            c["store"].prune_old_posts(keep_days=90)
            if notifier:
                counts = c["store"].count_posts_yesterday_batch()
                best = c["store"].get_best_tweet_yesterday()
                perf = c["store"].get_avg_metrics_by_type()
                variant_perf = c["store"].get_variant_performance(days=14)
                effectiveness = c["store"].get_engagement_effectiveness(days=7)
                hour_perf = c["store"].get_metrics_by_hour(days=14)
                notifier.send_daily_report(counts, best_tweet=best, perf_by_type=perf,
                                           variant_perf=variant_perf,
                                           engagement_effectiveness=effectiveness,
                                           hour_perf=hour_perf)
        except Exception as exc:
            logger.exception("Heartbeat failed: %s", exc)
            if notifier:
                notifier.send(f"❌ <b>Heartbeat failed</b>\n<code>{_html.escape(str(exc)[:200])}</code>")

    scheduler.add_job(_heartbeat, "cron", hour=6, minute=50)

    # Daily schedule (UTC) — minutes staggered + jitter=120s to look human
    scheduler.add_job(morning.run,  "cron", hour=8,  minute=12, max_instances=1)  # ~08:12 Morning Brief
    scheduler.add_job(hot_take.run, "cron", hour=12, minute=37, max_instances=1)  # ~12:37 Hot Take (Polymarket question)
    # On-Chain Signal: PAUSED — 9 views / 0% eng, data already in Morning Brief & Evening Wrap
    # scheduler.add_job(onchain.run, "cron", hour=12, minute=0, max_instances=1)
    # 14:25 UTC covers both EST and EDT pre-market window
    scheduler.add_job(us_open.run,  "cron", hour=14, minute=25, max_instances=1)  # ~14:25 US pre-market
    # Engagement Bait: PAUSED — no audience to engage with at 0-follower phase
    # scheduler.add_job(engagement_bait.run, "cron", hour=18, minute=0, max_instances=1)
    scheduler.add_job(wrap.run,     "cron", hour=22, minute=48, max_instances=1)  # ~22:48 Evening Wrap

    # Alpha thread: PAUSED — 0-follower phase, no audience for long threads
    # scheduler.add_job(thread.run, "cron", day_of_week="mon,wed,fri", hour=21, minute=0, max_instances=1)

    # Engagement: REPLY/QT DISABLED — API returns 403 on both reply and QT for most accounts.
    # Keeping like-only mode for free visibility. Reply/QT moving to browser-based approach.
    scheduler.add_job(engagement.run, "cron", hour="2",  minute=17, max_instances=1)  # ~02:17 Asia afternoon
    scheduler.add_job(engagement.run, "cron", hour="6",  minute=43, max_instances=1)  # ~06:43 EU morning
    scheduler.add_job(engagement.run, "cron", hour="9",  minute=8,  max_instances=1)  # ~09:08 Asia evening
    scheduler.add_job(engagement.run, "cron", hour="11", minute=52, max_instances=1)  # ~11:52 EU afternoon
    scheduler.add_job(engagement.run, "cron", hour="16", minute=21, max_instances=1)  # ~16:21 US morning
    scheduler.add_job(engagement.run, "cron", hour="20", minute=33, max_instances=1)  # ~20:33 US evening

    # Self-reply: respond to people who reply to our tweets
    scheduler.add_job(self_reply.run, "cron", hour="10", minute=44, max_instances=1)  # ~10:44
    scheduler.add_job(self_reply.run, "cron", hour="17", minute=9,  max_instances=1)  # ~17:09
    scheduler.add_job(self_reply.run, "cron", hour="23", minute=16, max_instances=1)  # ~23:16

    # Metrics collector: every 6h — background data for feedback loop
    scheduler.add_job(metrics.run, "cron", hour="0,6,12,18", minute=15, max_instances=1)

    # Price alerts: every 5 minutes — event-driven tweets on >5% 1h moves
    # jitter=0: price alerts need consistent timing, not the global 120s jitter
    scheduler.add_job(price_alert.run, "interval", minutes=5, max_instances=1, jitter=0)

    # Weekly poll: Sunday 20:00 UTC
    scheduler.add_job(weekly_poll.run, "cron", day_of_week="sun", hour=20, minute=0, max_instances=1)

    # Weekly recap: PAUSED — 0-follower phase, no audience for recap content
    # scheduler.add_job(weekly_recap.run, "cron", day_of_week="sun", hour=12, minute=0, max_instances=1)

    # Fear & Greed Index: 1x/day with gauge image — $CASHTAG searchable
    scheduler.add_job(fng.run, "cron", hour=10, minute=0, max_instances=1)

    # Trend alerts: 2x/day fixed schedule — pick top trending coin and tweet about it
    # Value is $CASHTAG searchability, not real-time breaking
    scheduler.add_job(trend_alert.run, "cron", hour=9,  minute=5,  max_instances=1)  # ~09:05 Asia evening
    scheduler.add_job(trend_alert.run, "cron", hour=18, minute=15, max_instances=1)  # ~18:15 US afternoon

    # One-shot: pinned thread with live data — only if not already posted
    if not c["store"].is_posted("pinned_thread"):
        pinned = PinnedThreadJob(
            poster=c["poster"], prices=c["prices"],
            etf_flows=c["etf_flows"], exchange_flows=c["exchange_flows"],
            notifier=notifier,
        )
        from datetime import datetime, timedelta, timezone
        run_at = (datetime.now(timezone.utc) + timedelta(days=1)).replace(hour=14, minute=0, second=0, microsecond=0)
        scheduler.add_job(pinned.run, "date", run_date=run_at, id="pinned_thread_once")
        logger.info("Pinned thread scheduled for %s UTC", run_at.strftime("%Y-%m-%d %H:%M"))
    else:
        logger.info("Pinned thread already posted, skipping")

    def _shutdown(signum, frame):
        logger.info("Received signal %d — shutting down scheduler", signum)
        scheduler.shutdown(wait=False)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    logger.info("Scheduler started. Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        logger.info("Scheduler stopped")


if __name__ == "__main__":
    main()
