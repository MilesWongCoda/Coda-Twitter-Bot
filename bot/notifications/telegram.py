# notifications/telegram.py
import html
import logging
import threading
from datetime import datetime, timezone, timedelta

from bot.config import BOT_HANDLE
from bot.data.http_retry import create_session

logger = logging.getLogger(__name__)

# Thread-local sessions: requests.Session is not thread-safe, and telegram
# notifications can be sent from both the scheduler thread and daemon timer threads.
_tg_local = threading.local()


def _get_session():
    if not hasattr(_tg_local, 'session'):
        _tg_local.session = create_session(retries=2, backoff_factor=0.5)
    return _tg_local.session

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
TELEGRAM_PHOTO_API = "https://api.telegram.org/bot{token}/sendPhoto"

# Map job class names to branded column names
_COLUMN_NAMES = {
    "MorningBrief": "The Open",
    "HotTake": "Signal Flare",
    "USOpen": "Wall St. Cross",
    "EveningWrap": "The Close",
    "AlphaThread": "Deep Dive",
    "Engagement": "Engagement",
    "SelfReply": "Self Reply",
    "PriceAlert": "🚨 Price Alert",
    "TrendAlert": "📈 Trend Alert",
    "WeeklyPoll": "Weekly Poll",
    "WeeklyRecap": "Weekly Recap",
}

# Content job names (single tweet with a link)
_CONTENT_JOBS = {"MorningBrief", "HotTake", "USOpen", "EveningWrap"}

# Fixed daily content schedule: (job_type, column_name, expected_count)
_DAILY_SCHEDULE = [
    ("morning_brief", "The Open", 1),
    ("trend_alert", "📈 Trend Alert", 2),
    ("hot_take", "Signal Flare", 1),
    ("us_open", "Wall St. Cross", 1),
    ("evening_wrap", "The Close", 1),
]

# Floating jobs — triggered by market conditions, no fixed expected count
_FLOATING_JOBS = [
    ("price_alert", "🚨 Price Alert"),
]

# Content job_types for progress counting (fixed only)
_CONTENT_TYPES = {"morning_brief", "hot_take", "us_open", "evening_wrap", "trend_alert"}

TWEET_URL = "https://x.com/i/status/{}"


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, store=None, channel_id: str = "",
                 openai_api_key: str = ""):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.store = store
        self.channel_id = channel_id  # Public channel for content syndication
        self._openai_key = openai_api_key

    def send(self, text: str, parse_html: bool = True) -> bool:
        """Send a message. Returns True on success, False on failure (never raises)."""
        try:
            payload = {"chat_id": self.chat_id, "text": text}
            if parse_html:
                payload["parse_mode"] = "HTML"
                payload["disable_web_page_preview"] = True
            resp = _get_session().post(
                TELEGRAM_API.format(token=self.bot_token),
                json=payload,
                timeout=10,
            )
            resp.raise_for_status()
            return True
        except Exception as exc:
            msg = str(exc)
            if self.bot_token:
                msg = msg.replace(self.bot_token, "***")
            logger.warning("Telegram notification failed: %s", msg)
            return False

    # Content types worth syndicating to the public channel.
    # Trend/price alerts are Twitter-search plays — no value on TG.
    _CHANNEL_ALLOWED = {
        "MorningBrief", "HotTake", "OnChainSignal", "USOpen", "EveningWrap",
        "AlphaThread", "WeeklyPoll", "WeeklyRecap", "PinnedThread",
    }

    def publish_to_channel(self, job_name: str, tweet_content: str,
                           tweet_id: str = None) -> bool:
        """Publish tweet content to the public Telegram channel.

        Only publishes curated content types (skips trend/price alerts,
        engagement, self-replies). Formats with branded header + tweet link.
        """
        if not self.channel_id:
            return False

        # Filter: only high-value content goes to the channel
        if job_name not in self._CHANNEL_ALLOWED:
            logger.debug("Channel skip: %s not in allowed types", job_name)
            return False

        if not tweet_content:
            return False

        column = _COLUMN_NAMES.get(job_name, job_name)
        lines = []

        # Header with branded column name
        lines.append(f"<b>{html.escape(column)}</b>")
        lines.append("")

        # Content — use full text, not truncated
        safe_content = html.escape(tweet_content)
        lines.append(safe_content)

        # Tweet link
        if tweet_id and not tweet_id.startswith("dry_"):
            lines.append("")
            lines.append(f'<a href="{TWEET_URL.format(tweet_id)}">View on X</a>')

        text = "\n".join(lines)

        try:
            payload = {
                "chat_id": self.channel_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            resp = _get_session().post(
                TELEGRAM_API.format(token=self.bot_token),
                json=payload,
                timeout=10,
            )
            resp.raise_for_status()
            logger.info("Published to channel %s: %s", self.channel_id, job_name)
            return True
        except Exception as exc:
            msg = str(exc)
            if self.bot_token:
                msg = msg.replace(self.bot_token, "***")
            logger.warning("Channel publish failed: %s", msg)
            return False

    def publish_thread_to_channel(self, job_name: str, tweet_contents: list,
                                  tweet_ids: list = None) -> bool:
        """Publish a thread as a single combined message to the channel."""
        if not self.channel_id or not tweet_contents:
            return False

        if job_name not in self._CHANNEL_ALLOWED:
            logger.debug("Channel thread skip: %s not in allowed types", job_name)
            return False

        column = _COLUMN_NAMES.get(job_name, job_name)
        lines = [f"<b>{html.escape(column)}</b>  ·  🧵 Thread", ""]

        for i, content in enumerate(tweet_contents, 1):
            lines.append(f"<b>{i}/</b> {html.escape(content)}")
            lines.append("")

        # Link to first tweet
        if tweet_ids and tweet_ids[0] and not tweet_ids[0].startswith("dry_"):
            lines.append(f'<a href="{TWEET_URL.format(tweet_ids[0])}">View on X</a>')

        text = "\n".join(lines)
        # Telegram max is 4096 chars
        if len(text) > 4096:
            text = text[:4090] + "\n…"

        try:
            payload = {
                "chat_id": self.channel_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            resp = _get_session().post(
                TELEGRAM_API.format(token=self.bot_token),
                json=payload,
                timeout=10,
            )
            resp.raise_for_status()
            logger.info("Published thread to channel %s: %s", self.channel_id, job_name)
            return True
        except Exception as exc:
            msg = str(exc)
            if self.bot_token:
                msg = msg.replace(self.bot_token, "***")
            logger.warning("Channel thread publish failed: %s", msg)
            return False

    def _format_time(self) -> str:
        return datetime.now(timezone.utc).strftime("%H:%M UTC")

    def _content_progress(self) -> str:
        """Return today's content progress like '3/4'."""
        if not self.store:
            return ""
        try:
            counts = self.store.count_posts_since_midnight_batch()
            done = sum(counts.get(t, 0) for t in _CONTENT_TYPES)
            expected = sum(exp for _, _, exp in _DAILY_SCHEDULE)
            return f"{done}/{expected}"
        except Exception:
            return ""

    def _tweet_link(self, tweet_id: str) -> str:
        """Build tweet link, skip for dry-run fake IDs."""
        if not tweet_id or tweet_id.startswith("dry_"):
            return ""
        return TWEET_URL.format(tweet_id)

    def notify_success(self, job_name: str, result):
        """Smart notification based on job type.

        Args:
            job_name: class name without "Job" suffix (e.g. "MorningBrief")
            result: tweet_id (str) for content, list[str] for threads/engagement
        """
        column = _COLUMN_NAMES.get(job_name, job_name)
        time_str = self._format_time()

        if isinstance(result, list) and job_name in _CONTENT_JOBS:
            self._notify_thread(column, time_str, result)
        elif job_name in _CONTENT_JOBS:
            self._notify_content(column, time_str, result)
        elif job_name == "AlphaThread":
            self._notify_thread(column, time_str, result)
        elif job_name == "Engagement":
            self._notify_engagement(column, time_str, result)
        elif job_name == "SelfReply":
            self._notify_self_reply(column, time_str, result)
        elif job_name == "PriceAlert":
            self._notify_price_alert(column, time_str, result)
        elif job_name == "TrendAlert":
            self._notify_content(column, time_str, result)
        elif job_name in ("WeeklyPoll", "WeeklyRecap"):
            self._notify_content(column, time_str, result)
        else:
            # Fallback
            self.send(f"✅ <b>{html.escape(column)}</b>  ·  {time_str}")

    def _notify_content(self, column: str, time_str: str, tweet_id: str):
        """Single content tweet: column + progress + preview + link."""
        lines = [f"✅ <b>{html.escape(column)}</b>  ·  {time_str}"]

        progress = self._content_progress()
        if progress:
            lines[0] += f"  [{progress}]"

        # Preview
        if self.store and tweet_id:
            try:
                content = self.store.get_content_by_id(tweet_id)
                if content:
                    snippet = html.escape(content[:200]).replace("\n", " ")
                    if len(content) > 200:
                        snippet += "…"
                    lines.append("")
                    lines.append(f"<i>{snippet}</i>")
            except Exception:
                pass

        # Tweet link
        link = self._tweet_link(tweet_id)
        if link:
            lines.append("")
            lines.append(f"🔗 {link}")

        self.send("\n".join(lines))

    def _notify_thread(self, column: str, time_str: str, tweet_ids: list):
        """Thread: column + thread length + first tweet preview + link."""
        n = len(tweet_ids) if isinstance(tweet_ids, list) else 0
        lines = [f"✅ <b>{html.escape(column)}</b>  ·  {time_str}"]
        lines.append(f"🧵 Thread: {n} tweets")

        progress = self._content_progress()
        if progress:
            lines[0] += f"  [{progress}]"

        # Preview first tweet
        first_id = tweet_ids[0] if tweet_ids else None
        if self.store and first_id:
            try:
                content = self.store.get_content_by_id(first_id)
                if content:
                    snippet = html.escape(content[:200]).replace("\n", " ")
                    if len(content) > 200:
                        snippet += "…"
                    lines.append("")
                    lines.append(f"<i>{snippet}</i>")
            except Exception:
                pass

        link = self._tweet_link(first_id) if first_id else ""
        if link:
            lines.append("")
            lines.append(f"🔗 {link}")

        self.send("\n".join(lines))

    def _notify_engagement(self, column: str, time_str: str, posted_ids: list):
        """Engagement: count of replies/quotes/convos + daily progress."""
        if not posted_ids:
            return  # nothing posted, stay silent

        replies = sum(1 for k in posted_ids if k.startswith("reply_"))
        quotes = sum(1 for k in posted_ids if k.startswith("quote_"))
        convos = sum(1 for k in posted_ids if k.startswith("convo_"))

        lines = [f"✅ <b>{html.escape(column)}</b>  ·  {time_str}"]

        # Show today's running totals
        if self.store:
            try:
                counts = self.store.count_posts_since_midnight_batch()
                r_total = counts.get("reply", 0)
                q_total = counts.get("quote_tweet", 0)
                lines.append(f"  Replies: +{replies} → {r_total}/20")
                lines.append(f"  Quotes:  +{quotes} → {q_total}/10")
                if convos:
                    lines.append(f"  Convos:  +{convos}")
            except Exception:
                lines.append(f"  Replies: +{replies}  ·  Quotes: +{quotes}")
                if convos:
                    lines.append(f"  Convos: +{convos}")
        else:
            lines.append(f"  Replies: +{replies}  ·  Quotes: +{quotes}")

        self.send("\n".join(lines))

    def _notify_self_reply(self, column: str, time_str: str, posted_ids: list):
        """Self reply: count replied."""
        if not posted_ids:
            return
        n = len(posted_ids)
        lines = [f"✅ <b>{html.escape(column)}</b>  ·  {time_str}"]
        lines.append(f"  Replied to {n} comment{'s' if n > 1 else ''}")
        self.send("\n".join(lines))

    def _notify_price_alert(self, column: str, time_str: str, posted_ids: list):
        """Price alert: list of tweet IDs for each alerted coin."""
        if not posted_ids:
            return
        n = len(posted_ids)
        lines = [f"✅ <b>{html.escape(column)}</b>  ·  {time_str}"]
        lines.append(f"  {n} alert{'s' if n > 1 else ''} triggered")
        for tid in posted_ids[:3]:
            link = self._tweet_link(tid)
            if link:
                lines.append(f"  🔗 {link}")
        self.send("\n".join(lines))

    def notify_failure(self, job_name: str, error):
        """Formatted failure notification."""
        column = _COLUMN_NAMES.get(job_name, job_name)
        time_str = self._format_time()
        safe_error = html.escape(str(error))[:300]
        lines = [
            f"❌ <b>{html.escape(column)}</b>  ·  {time_str}",
            "",
            f"<code>{safe_error}</code>",
        ]
        self.send("\n".join(lines))

    def notify_skipped(self, job_name: str, reason: str):
        """Job skipped — silent by default, useful for debugging."""
        logger.debug("Job %s skipped: %s", job_name, reason)

    def _format_metric_short(self, n: float) -> str:
        """Format 1200 → '1.2K', 15000 → '15K'."""
        if n >= 1_000_000:
            return f"{n / 1_000_000:.1f}M"
        if n >= 1_000:
            return f"{n / 1_000:.1f}K"
        return f"{n:.0f}"

    def _generate_daily_insight(self, counts: dict, perf_by_type: dict = None,
                                best_tweet: dict = None,
                                engagement_effectiveness: list = None,
                                hour_perf: dict = None) -> str:
        """Use GPT-4o-mini to generate a brief daily summary + recommendation."""
        if not self._openai_key:
            return ""
        try:
            from openai import OpenAI
            # Build a data snapshot for the AI
            parts = []
            # Content completion
            fixed_done = sum(1 for jt, _, _ in _DAILY_SCHEDULE if counts.get(jt, 0) > 0)
            fixed_total = len(_DAILY_SCHEDULE)
            parts.append(f"Fixed content: {fixed_done}/{fixed_total} completed")
            for jt, col, _ in _DAILY_SCHEDULE:
                if counts.get(jt, 0) == 0:
                    parts.append(f"  MISSED: {col}")
            # Floating
            for jt, col in _FLOATING_JOBS:
                n = counts.get(jt, 0)
                if n > 0:
                    parts.append(f"Floating: {col} ×{n}")
            # Engagement
            parts.append(f"Replies: {counts.get('reply', 0)}/20 cap")
            parts.append(f"Quote tweets: {counts.get('quote_tweet', 0)}/10 cap")
            parts.append(f"Likes given: {counts.get('like', 0)}/100 cap")
            # Performance by type
            if perf_by_type:
                parts.append("Performance by type:")
                for jt, data in sorted(perf_by_type.items(),
                                        key=lambda x: x[1].get("avg_impressions", 0), reverse=True):
                    avg_imp = data.get("avg_impressions", 0)
                    avg_likes = data.get("avg_likes", 0)
                    eng_rate = (avg_likes / avg_imp * 100) if avg_imp > 0 else 0
                    parts.append(f"  {jt}: {avg_imp:.0f} avg views, {eng_rate:.1f}% eng rate")
            # Best tweet
            if best_tweet and best_tweet.get("impressions", 0) > 0:
                parts.append(f"Best tweet: {best_tweet['impressions']} views, "
                             f"{best_tweet.get('likes', 0)} likes — "
                             f"\"{(best_tweet.get('content') or '')[:80]}\"")
            # Engagement effectiveness
            if engagement_effectiveness:
                replied_back = [e for e in engagement_effectiveness if e.get("conversation_replies", 0) > 0]
                parts.append(f"KOL engagement: {len(engagement_effectiveness)} targets, "
                             f"{len(replied_back)} replied back")
            # Best hours
            if hour_perf:
                sorted_h = sorted(hour_perf.items(),
                                  key=lambda x: x[1].get("avg_engagement", 0), reverse=True)
                top3 = [f"{h}:00 UTC ({d.get('avg_engagement', 0):.0f} eng)" for h, d in sorted_h[:3]]
                parts.append(f"Best hours: {', '.join(top3)}")

            data_text = "\n".join(parts)
            client = OpenAI(api_key=self._openai_key)
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": (
                        f"You are an analytics assistant for a crypto Twitter bot (@{BOT_HANDLE}). "
                        "Given yesterday's performance data, write a concise insight in 2-3 sentences: "
                        "1) What went well or poorly 2) One specific actionable recommendation. "
                        "Be direct, no fluff. Write in Chinese."
                    )},
                    {"role": "user", "content": data_text},
                ],
                max_tokens=200,
                temperature=0.3,
            )
            if resp.choices:
                return (resp.choices[0].message.content or "").strip()
        except Exception as exc:
            logger.warning("Daily insight generation failed: %s", exc)
        return ""

    def send_daily_report(self, counts: dict, best_tweet: dict = None,
                          perf_by_type: dict = None,
                          variant_perf: list = None,
                          engagement_effectiveness: list = None,
                          hour_perf: dict = None):
        """Daily progress report comparing actual posts vs expected schedule."""
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = yesterday.strftime("%b %d")
        weekday = yesterday.weekday()

        lines = [f"📊 <b>Daily Report</b>  ·  {date_str}", ""]

        # ── Fixed content schedule ──
        lines.append("<b>Content (fixed)</b>")
        for job_type, column, expected in _DAILY_SCHEDULE:
            actual = counts.get(job_type, 0)
            if actual >= expected:
                icon = "✅"
            elif actual > 0:
                icon = "⚠️"
            else:
                icon = "❌"
            count_str = f" ×{actual}" if expected > 1 or actual > 1 else ""
            target_str = f"/{expected}" if expected > 1 else ""
            lines.append(f"  {icon} {column}{count_str}{target_str}")

        # ── Floating jobs (market-triggered) ──
        floating_total = 0
        for job_type, column in _FLOATING_JOBS:
            actual = counts.get(job_type, 0)
            floating_total += actual
            if actual > 0:
                lines.append(f"  📍 {column} ×{actual}")
        if floating_total == 0:
            lines.append("  ⏭ No alerts triggered")

        # ── Performance by type (if metrics available) ──
        if perf_by_type:
            lines.append("")
            lines.append("<b>Performance</b>")
            type_to_col = {jt: col for jt, col, _ in _DAILY_SCHEDULE}
            for jt, col in _FLOATING_JOBS:
                type_to_col[jt] = col
            for jt, data in sorted(perf_by_type.items(), key=lambda x: x[1].get("avg_impressions", 0), reverse=True):
                col_name = type_to_col.get(jt, jt)
                avg_imp = data.get("avg_impressions", 0)
                avg_likes = data.get("avg_likes", 0)
                eng_rate = (avg_likes / avg_imp * 100) if avg_imp > 0 else 0
                lines.append(f"  {col_name}: {self._format_metric_short(avg_imp)} views · {eng_rate:.1f}% eng")

        # ── Engagement stats ──
        replies = counts.get("reply", 0)
        quotes = counts.get("quote_tweet", 0)
        likes = counts.get("like", 0)
        self_replies = counts.get("self_reply", 0)
        followups = counts.get("self_followup", 0)

        lines.append("")
        lines.append("<b>Engagement</b>")
        lines.append(f"  Replies:    {replies}/20")
        lines.append(f"  Quotes:     {quotes}/10")
        lines.append(f"  Likes:      {likes}/100")
        lines.append(f"  Self-reply: {self_replies}")
        lines.append(f"  Followup:   {followups}")

        # ── A/B Test Results ──
        if variant_perf and len(variant_perf) >= 2:
            lines.append("")
            lines.append("<b>A/B Test</b>")
            for vp in variant_perf[:5]:
                v_name = html.escape(vp["variant"])
                lines.append(f"  {v_name}: {vp['avg_engagement']:.0f} eng (n={vp['count']})")

        # ── Engagement Effectiveness ──
        if engagement_effectiveness:
            lines.append("")
            lines.append("<b>Engagement Targets</b>")
            for eff in engagement_effectiveness[:5]:
                u = html.escape(eff["username"])
                convo = eff.get("conversation_replies", 0)
                reply_back = f" ({convo} replied back)" if convo else ""
                lines.append(f"  @{u}: {eff['total']} interactions{reply_back}")

        # ── Performance by Hour ──
        if hour_perf:
            lines.append("")
            lines.append("<b>Best Hours</b>")
            # Show top 3 hours by avg engagement
            sorted_hours = sorted(hour_perf.items(),
                                  key=lambda x: x[1].get("avg_engagement", 0), reverse=True)
            for hour_str, data in sorted_hours[:3]:
                avg_eng = data.get("avg_engagement", 0)
                count = data.get("count", 0)
                lines.append(f"  {hour_str}:00 UTC: {avg_eng:.0f} avg eng (n={count})")

        # ── Total ──
        total = sum(counts.values())
        lines.append("")
        lines.append(f"<b>Total: {total} posts</b>")

        # ── Best tweet ──
        if best_tweet and best_tweet.get("impressions", 0) > 0:
            imp = best_tweet["impressions"]
            lk = best_tweet.get("likes", 0)
            rt = best_tweet.get("retweets", 0)
            content_raw = (best_tweet.get("content", "") or "")
            snippet = html.escape(content_raw[:100]).replace("\n", " ")
            suffix = "…" if len(content_raw) > 100 else ""
            lines.append("")
            lines.append(f"🏆 <b>Best tweet</b>")
            lines.append(f"  {imp:,} views · {lk} likes · {rt} RT")
            lines.append(f"  <i>{snippet}{suffix}</i>")

        # ── AI Summary & Recommendations ──
        insight = self._generate_daily_insight(
            counts, perf_by_type=perf_by_type, best_tweet=best_tweet,
            engagement_effectiveness=engagement_effectiveness, hour_perf=hour_perf,
        )
        if insight:
            lines.append("")
            lines.append(f"💡 <b>Insight</b>")
            lines.append(html.escape(insight))

        self.send("\n".join(lines))
