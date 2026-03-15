# ai/generate.py
import random
import re
import time
import logging
from typing import Optional
import anthropic
from bot.config import ANTHROPIC_MODEL, BOT_HANDLE

logger = logging.getLogger(__name__)

# Retry config for transient errors (429, 529 overloaded, 5xx)
_MAX_RETRIES = 3
_BASE_DELAY = 5  # seconds


class AIRateLimitError(Exception):
    """Raised when AI provider rate-limits us — surfaces to Telegram via BaseJob."""
    pass

SYSTEM_PROMPT = f"""You are @{BOT_HANDLE} — ex-institutional flow desk trader turned independent.
You trade your own book and tweet what you see. One insight per tweet. Use $CASHTAG format always.

Study these examples. Each one sounds DIFFERENT — that's the point:

"📊 $BTC CME OI up 18% this week. Not hedging."

"$ETH funding deeply negative for 3 straight days while price holds. Someone knows something."

"Polymarket puts rate cut at 71% ($4.2M vol)

Meanwhile $BTC sitting at the exact same level it was pre-FOMC. Market doesn't believe the Fed either."

"lmao $SOL went from 'Solana is dead' to ATH in 4 months. Crypto attention spans are genuinely broken"

"📈 $BTC exchange reserves just hit a 5-year low. 2.1M BTC left on exchanges.

Where did it all go? Cold storage. OTC desks. ETFs. Anywhere but exchanges."

"$BTC 86k and nobody's talking about it. Sounds about right for this cycle."

"🐋 220M USDT just moved to Coinbase Prime. Last time that wallet moved was Nov 2024."

"$DOGE up 12% because a government agency has the same acronym. This market is unserious."

"hot take: $ETH at 0.03 ratio is either generational value or a slow death. no in-between."

"📉 Fear & Greed at 22. Same crowd calling for 200k three weeks ago."

Rules (only the ones that matter):
- Always include at least one $CASHTAG — it's how people find us in search
- Observations, not advice. Never say buy/sell/long/short
- Never use: GM, WAGMI, NFA, DYOR, 🚀🌙💎🙌
- English only
- When Polymarket data is available, use the specific odds + volume — that's our edge
- 0-2 emojis max. 📊🔥📈📉⚡🐋💰 only.

Everything else — length, structure, tone, humor — VARY IT. If your last 3 tweets had the same structure, break the pattern. Real people are inconsistent. Be inconsistent.

The examples above are for STYLE only. Never reuse their specific numbers or data — only use data from the context you're given.
"""

TWEET_INSTRUCTIONS = {
    "morning_brief": "One thing from overnight. What does it set up today?",
    "hot_take": "React to this data. Bold enough to get quote-tweeted.",
    "onchain_signal": "One on-chain signal. The number. Your read.",
    "us_open": "US markets opening. One thing crypto traders should care about.",
    "evening_wrap": "End of day. One number, one verdict.",
    "quote_tweet": "Add one thing the original missed. 1-2 sentences max.",
    "reply": "Add one thing. 1 sentence. You're in their mentions.",
    "self_reply": (
        "Reply like you're texting a trader friend. "
        "One sentence. If they're wrong, push back. If right, say so. Max 180 chars."
    ),
    "self_followup": (
        "Quick follow-up to your own tweet. 'Oh and one more thing' energy. "
        "Don't restate. Max 150 chars."
    ),
    "price_alert": "A coin just moved >5% in 1h. Start with 📈 or 📉 matching direction. Snap read: liquidation cascade, real buying, or fakeout?",
    "weekly_poll": (
        "Create a market poll. "
        "Output format: first line is a question (max 280 chars), then exactly 4 lines "
        "starting with 'A:' 'B:' 'C:' 'D:' for options (each max 25 chars)."
    ),
    "weekly_recap": "One number that defined this week. What's it mean for next week?",
    "trend_alert": "This coin is trending. Why should traders care?",
    "engagement_bait": "Make people want to reply. Controversial take, bold prediction, or binary choice.",
}

# Tone modes — randomly prepended to content tweet instructions to vary structure.
# Each mode steers the AI toward a different tweet archetype.
_TONE_MODES = {
    "content": [
        # Style archetypes — each produces a structurally different tweet
        ("STYLE: Text message to a trader friend. Fragments. Under 100 chars.", 12),
        ("STYLE: Just the number. One sentence max.", 10),
        ("STYLE: Ask a question. Make people want to reply.", 12),
        ("STYLE: Sarcastic/dry humor. The market is doing something absurd — point it out.", 10),
        ("STYLE: Bold contrarian take. Say what everyone's afraid to say.", 12),
        ("STYLE: Tell a tiny story in 2-3 lines. Setup → punchline.", 10),
        ("STYLE: Start with the conclusion. No buildup.", 10),
        ("STYLE: Compare two things that shouldn't be compared. Make it land.", 8),
        ("", 16),  # no style — let the data and mood dictate
    ],
    "reply": [
        ("STYLE: 3-8 words max. React, don't explain.", 4),
        ("STYLE: One fragment. Agree or disagree.", 3),
        ("STYLE: One sentence with a number they missed.", 3),
    ],
}


# Replies/QTs need shorter limits: Twitter prepends "@username " which eats into the limit
_TWEET_CHAR_LIMITS = {
    "reply": 200,
    "quote_tweet": 200,
    "self_reply": 180,
    "self_followup": 150,
    "weekly_poll": 800,  # structured output: question + A:/B:/C:/D: options, not a single tweet
}


_URL_RE = re.compile(r'https?://\S+')


def _format_metric(n: int) -> str:
    """Format 1200 → '1.2K', 15000 → '15K'."""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _twitter_len(text: str) -> int:
    """Twitter-weighted length: every codepoint = 1, URLs normalized to 23."""
    text_no_urls = _URL_RE.sub('X' * 23, text)
    return len(text_no_urls)


def _safe_truncate(text: str, limit: int = 280) -> str:
    if _twitter_len(text) <= limit:
        return text
    # Binary search for the longest prefix that fits
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _twitter_len(text[:mid]) <= limit:
            lo = mid
        else:
            hi = mid - 1
    text = text[:lo]
    truncated = text.rsplit(" ", 1)[0].strip()
    return truncated if truncated else text.strip()


def _is_retryable(exc: Exception) -> bool:
    """Check if an Anthropic error is transient and worth retrying."""
    if isinstance(exc, (anthropic.RateLimitError, anthropic.APIConnectionError, anthropic.APITimeoutError)):
        return True
    if isinstance(exc, anthropic.APIStatusError) and exc.status_code in (429, 500, 502, 503, 529):
        return True
    return False


def _call_with_retry(fn, tweet_type: str = "unknown"):
    """Call fn() with exponential backoff on transient Anthropic errors."""
    for attempt in range(_MAX_RETRIES):
        try:
            return fn()
        except anthropic.AuthenticationError:
            raise  # permanent, no retry
        except (anthropic.RateLimitError, anthropic.APIStatusError,
                anthropic.APIConnectionError, anthropic.APITimeoutError) as exc:
            if not _is_retryable(exc):
                raise
            if attempt == _MAX_RETRIES - 1:
                logger.error("All %d retries exhausted for %s: %s", _MAX_RETRIES, tweet_type, exc)
                raise AIRateLimitError(f"AI unavailable for {tweet_type} after {_MAX_RETRIES} retries") from exc
            delay = _BASE_DELAY * (2 ** attempt)
            logger.warning("Retryable error for %s (attempt %d/%d), waiting %ds: %s",
                           tweet_type, attempt + 1, _MAX_RETRIES, delay, exc)
            time.sleep(delay)


class TweetGenerator:
    def __init__(self, api_key: str, persona: str = SYSTEM_PROMPT):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.persona = persona
        self._last_variant = ""

    @property
    def last_variant(self) -> str:
        return self._last_variant

    def generate_tweet(self, context: str, tweet_type: str,
                       recent_tweets: Optional[list] = None,
                       top_performers: Optional[list] = None,
                       bottom_performers: Optional[list] = None,
                       trending_tags: Optional[list] = None,
                       performance_patterns: Optional[dict] = None) -> str:
        instruction = TWEET_INSTRUCTIONS.get(tweet_type, "One data point. Your read.")

        # Randomize tone for content tweets to avoid formulaic output
        if tweet_type in ("reply", "self_reply", "quote_tweet"):
            tone_pool = _TONE_MODES.get("reply", [("", 1)])
        elif tweet_type in ("self_followup", "weekly_poll", "weekly_recap", "price_alert"):
            tone_pool = [("", 1)]  # no tone variation for structured/specialized types
        else:
            tone_pool = _TONE_MODES.get("content", [("", 1)])
        tone = random.choices(
            [t for t, _ in tone_pool],
            weights=[w for _, w in tone_pool],
            k=1,
        )[0]
        # Track variant for A/B testing
        if tone:
            # Extract short label from "STYLE: Xyz. ..." → "style_xyz"
            label = tone.split(".")[0].replace("STYLE: ", "").strip().lower()
            label = "_".join(label.split()[:3])  # first 3 words
            self._last_variant = f"style_{label}" if label else "style_default"
            instruction = f"{tone}\n\n{instruction}"
        else:
            self._last_variant = "style_default"

        recent = "\n".join((recent_tweets or [])[-10:]) or "None"

        # Inject top performer data for feedback loop
        perf_block = ""
        if top_performers:
            lines = []
            for i, tp in enumerate(top_performers[:3], 1):
                views = tp.get("impressions", 0)
                likes = tp.get("likes", 0)
                snippet = (tp.get("content", "") or "")[:80]
                if views > 0:
                    lines.append(f'{i}. [{_format_metric(views)} views, {likes} likes] "{snippet}"')
            if lines:
                perf_block = "\nYour recent top performers (write content similar in style):\n" + "\n".join(lines)

        # Negative feedback: worst performers to avoid
        neg_block = ""
        if bottom_performers:
            neg_lines = []
            for i, bp in enumerate(bottom_performers[:2], 1):
                views = bp.get("impressions", 0)
                likes = bp.get("likes", 0)
                snippet = (bp.get("content", "") or "")[:80]
                if views > 0:
                    neg_lines.append(f'{i}. [{_format_metric(views)} views, {likes} likes] "{snippet}"')
            if neg_lines:
                neg_block = "\nYour recent worst performers (avoid this style/topic):\n" + "\n".join(neg_lines)

        # Data-driven patterns from structural analysis
        patterns_block = ""
        if performance_patterns and performance_patterns.get("insights"):
            pattern_lines = "\n".join(f"- {i}" for i in performance_patterns["insights"])
            patterns_block = f"\nWhat works for your audience (data-driven):\n{pattern_lines}\n"

        # Build feedback section
        feedback = patterns_block + perf_block + neg_block

        # Trending hashtags — suggest but don't force
        tag_block = ""
        if trending_tags:
            # Sanitize: strip control chars to prevent prompt injection via trend names
            safe_tags = [re.sub(r'[\r\n\t]', ' ', t).strip()[:50]
                         for t in trending_tags[:3] if t and t.strip()]
            if safe_tags:
                tags_str = ", ".join(safe_tags)
                tag_block = (
                    f"\nCurrently trending hashtags: {tags_str}\n"
                    "If any of these are relevant to your tweet, weave one in naturally "
                    "(mid-sentence or end, never as the first word). Don't force it.\n"
                )

        user_prompt = f"""Recent tweets (don't repeat these):
{recent}
{feedback}{tag_block}

Data:
{context}

{instruction}

Don't repeat any data point from your recent tweets above. Output ONLY the tweet. No quotes, no labels."""

        max_tokens = 600 if tweet_type == "weekly_poll" else 300
        resp = _call_with_retry(
            lambda: self.client.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=max_tokens,
                system=self.persona,
                messages=[{"role": "user", "content": user_prompt}]
            ),
            tweet_type=tweet_type,
        )
        if resp is None or not resp.content:
            logger.warning("Anthropic returned empty content for tweet_type=%s", tweet_type)
            return ""
        tweet = resp.content[0].text.strip()
        if tweet_type == "weekly_poll":
            return tweet  # structured multi-line output; truncation corrupts it
        # Enforce $CASHTAG rule — every content tweet must be searchable
        _CASHTAG_EXEMPT = {"reply", "quote_tweet", "self_followup"}
        if tweet_type not in _CASHTAG_EXEMPT and not re.search(r'\$[A-Z]{2,}', tweet):
            logger.warning("CASHTAG missing in %s tweet, injecting $BTC", tweet_type)
            tweet = f"$BTC {tweet}" if len(tweet) + 5 <= 280 else tweet
        limit = _TWEET_CHAR_LIMITS.get(tweet_type, 280)
        return _safe_truncate(tweet, limit)

    def generate_thread(self, context: str, num_tweets: int = 5,
                        recent_tweets: Optional[list] = None) -> list:
        recent = "\n".join((recent_tweets or [])[-10:]) or "None"
        user_prompt = f"""Recent tweets (do not repeat these topics):
{recent}

Write a {num_tweets}-tweet capital flows thread. You're breaking down where the money moved and what it means.
Tweet 1: Lead with the biggest flow number and your headline take.
Tweet 2-{num_tweets-1}: Walk through key data — ETFs, exchange flows, on-chain, derivatives. Add historical comparisons where they fit.
Tweet {num_tweets}: Your positioning read — what setup you're watching and what confirms or invalidates it.
Each tweet MUST start with its number followed by a slash (e.g. "1/", "2/").
Each tweet on its own line. No blank lines between tweets.
Context: {context}
Output ONLY the numbered tweets, one per line."""

        resp = _call_with_retry(
            lambda: self.client.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=2500,
                system=self.persona,
                messages=[{"role": "user", "content": user_prompt}]
            ),
            tweet_type="alpha_thread",
        )
        if resp is None or not resp.content:
            logger.warning("Anthropic returned empty content for generate_thread")
            return []
        raw = resp.content[0].text.strip()
        chunks = re.split(r'(?m)^(?=\d+/\s*)', raw)
        tweets = [chunk.strip() for chunk in chunks if re.match(r'^\d+/\s*', chunk.strip())]
        result = [st for st in (_safe_truncate(t, 280) for t in tweets) if st and _twitter_len(st) > 5]
        if len(result) < num_tweets:
            logger.warning("generate_thread: expected %d tweets, got %d", num_tweets, len(result))
        return result

    def generate_mini_thread(self, context: str, tweet_type: str,
                             num_tweets: int = 3,
                             recent_tweets: Optional[list] = None,
                             top_performers: Optional[list] = None,
                             bottom_performers: Optional[list] = None,
                             trending_tags: Optional[list] = None,
                             performance_patterns: Optional[dict] = None) -> list:
        """Generate a short thread (default 3 tweets) for content jobs.

        Structure: Tweet 1 = headline take, Tweet 2-3 = deeper data/angles.
        Uses same feedback loop as generate_tweet.
        """
        instruction = TWEET_INSTRUCTIONS.get(tweet_type, "")

        # Tone selection (same as generate_tweet)
        tone_pool = _TONE_MODES.get("content", [("", 1)])
        tone = random.choices(
            [t for t, _ in tone_pool],
            weights=[w for _, w in tone_pool],
            k=1,
        )[0]
        if tone:
            label = tone.split(".")[0].replace("STYLE: ", "").strip().lower()
            label = "_".join(label.split()[:3])
            self._last_variant = f"thread_{label}" if label else "thread_default"
        else:
            self._last_variant = "thread_default"

        recent = "\n".join((recent_tweets or [])[-10:]) or "None"

        # Feedback blocks (reuse generate_tweet logic)
        perf_block = ""
        if top_performers:
            lines = []
            for i, tp in enumerate(top_performers[:3], 1):
                views = tp.get("impressions", 0)
                likes = tp.get("likes", 0)
                snippet = (tp.get("content", "") or "")[:80]
                if views > 0:
                    lines.append(f'{i}. [{_format_metric(views)} views, {likes} likes] "{snippet}"')
            if lines:
                perf_block = "\nYour recent top performers (write content similar in style):\n" + "\n".join(lines)

        neg_block = ""
        if bottom_performers:
            neg_lines = []
            for i, bp in enumerate(bottom_performers[:2], 1):
                views = bp.get("impressions", 0)
                likes = bp.get("likes", 0)
                snippet = (bp.get("content", "") or "")[:80]
                if views > 0:
                    neg_lines.append(f'{i}. [{_format_metric(views)} views, {likes} likes] "{snippet}"')
            if neg_lines:
                neg_block = "\nYour recent worst performers (avoid this style/topic):\n" + "\n".join(neg_lines)

        patterns_block = ""
        if performance_patterns and performance_patterns.get("insights"):
            pattern_lines = "\n".join(f"- {i}" for i in performance_patterns["insights"])
            patterns_block = f"\nWhat works for your audience (data-driven):\n{pattern_lines}\n"

        feedback = patterns_block + perf_block + neg_block

        tag_block = ""
        if trending_tags:
            safe_tags = [re.sub(r'[\r\n\t]', ' ', t).strip()[:50]
                         for t in trending_tags[:3] if t and t.strip()]
            if safe_tags:
                tags_str = ", ".join(safe_tags)
                tag_block = (
                    f"\nCurrently trending hashtags: {tags_str}\n"
                    "If any of these are relevant, weave one in naturally.\n"
                )

        tone_line = f"{tone}\n\n" if tone else ""

        user_prompt = f"""Recent tweets (do NOT repeat):
{recent}
{feedback}{tag_block}

Context:
{context}

{tone_line}Write a {num_tweets}-tweet mini-thread for {tweet_type}.
Tweet 1: Your headline take — the ONE number or signal that matters. Make it punchy enough to stand alone.
Tweet 2-{num_tweets}: Go deeper — different angle, historical comparison, or positioning read. Each tweet must add something new.

Rules:
- Each tweet MUST start with its number followed by a slash (e.g. "1/", "2/").
- Each tweet max 280 chars. Keep them tight.
- {instruction}

Output ONLY the numbered tweets, one per line."""

        resp = _call_with_retry(
            lambda: self.client.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=1200,
                system=self.persona,
                messages=[{"role": "user", "content": user_prompt}]
            ),
            tweet_type=f"mini_thread_{tweet_type}",
        )
        if resp is None or not resp.content:
            logger.warning("Anthropic returned empty content for mini_thread_%s", tweet_type)
            return []
        raw = resp.content[0].text.strip()
        chunks = re.split(r'(?m)^(?=\d+/\s*)', raw)
        tweets = [chunk.strip() for chunk in chunks if re.match(r'^\d+/\s*', chunk.strip())]
        # Strip "1/ ", "2/ " numbering — used for parsing only, not for posting
        tweets = [re.sub(r'^\d+/\s*', '', t).strip() for t in tweets]
        result = [st for st in (_safe_truncate(t, 280) for t in tweets) if st and _twitter_len(st) > 5]
        if len(result) < num_tweets:
            logger.warning("generate_mini_thread: expected %d tweets, got %d", num_tweets, len(result))
        return result
