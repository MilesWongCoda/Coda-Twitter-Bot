# twitter/poster.py
from __future__ import annotations

import logging
import os
import tempfile
import tweepy
import time
import requests

logger = logging.getLogger(__name__)

# Transient errors worth retrying
_RETRYABLE = (tweepy.errors.TwitterServerError, requests.ConnectionError, requests.Timeout)


def _retry(fn, max_retries: int = 2, backoff: int = 3):
    """Retry on transient Twitter errors (5xx, connection, timeout)."""
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except _RETRYABLE as exc:
            if attempt == max_retries:
                raise
            wait = backoff * (attempt + 1)
            logger.warning("Transient error (attempt %d/%d), retrying in %ds: %s",
                           attempt + 1, max_retries + 1, wait, exc)
            time.sleep(wait)
    return None


class Poster:
    def __init__(self, client: tweepy.Client = None, api_key: str = None,
                 api_secret: str = None, access_token: str = None,
                 access_token_secret: str = None):
        if client:
            self.client = client
        else:
            self.client = tweepy.Client(
                consumer_key=api_key,
                consumer_secret=api_secret,
                access_token=access_token,
                access_token_secret=access_token_secret,
                wait_on_rate_limit=True,
            )
        # v1.1 API for media uploads
        if api_key and api_secret and access_token and access_token_secret:
            auth = tweepy.OAuth1UserHandler(
                api_key, api_secret, access_token, access_token_secret
            )
            self._api = tweepy.API(auth)
        else:
            self._api = None

    def upload_image_from_url(self, image_url: str) -> str | None:
        """Download image from URL and upload to Twitter. Returns media_id string."""
        if not self._api:
            logger.warning("v1.1 API not available, cannot upload media")
            return None
        try:
            resp = requests.get(image_url, timeout=15)
            resp.raise_for_status()
            # Detect file type from content-type header or URL
            content_type = resp.headers.get("content-type", "")
            if "gif" in content_type or image_url.lower().endswith(".gif"):
                suffix = ".gif"
            elif "png" in content_type or image_url.lower().endswith(".png"):
                suffix = ".png"
            else:
                suffix = ".jpg"
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
                f.write(resp.content)
                tmp_path = f.name
            try:
                media = self._api.media_upload(filename=tmp_path)
                return str(media.media_id)
            finally:
                os.unlink(tmp_path)
        except Exception as exc:
            logger.warning("upload_image_from_url failed: %s", exc)
            return None

    def upload_image_from_file(self, file_path: str) -> str | None:
        """Upload a local image file to Twitter. Returns media_id string."""
        if not self._api:
            logger.warning("v1.1 API not available, cannot upload media")
            return None
        try:
            media = self._api.media_upload(filename=file_path)
            return str(media.media_id)
        except Exception as exc:
            logger.warning("upload_image_from_file failed: %s", exc)
            return None

    def post_tweet(self, text: str, media_ids: list = None):
        try:
            def _call():
                kwargs = {"text": text}
                if media_ids:
                    kwargs["media_ids"] = media_ids
                return self.client.create_tweet(**kwargs)
            resp = _retry(_call)
        except (tweepy.errors.TweepyException, requests.ConnectionError, requests.Timeout) as exc:
            logger.error("create_tweet failed: %s", exc)
            raise
        if resp and resp.data:
            return str(resp.data["id"])
        logger.warning("create_tweet returned no data")
        return None

    def post_thread(self, tweets: list, first_tweet_media_ids: list = None) -> list:
        ids = []
        reply_to = None
        for tweet in tweets:
            try:
                kwargs = {"text": tweet}
                if reply_to:
                    kwargs["in_reply_to_tweet_id"] = reply_to
                elif first_tweet_media_ids:
                    kwargs["media_ids"] = first_tweet_media_ids
                resp = _retry(lambda kw=kwargs: self.client.create_tweet(**kw))
            except (tweepy.errors.TweepyException, requests.ConnectionError, requests.Timeout) as exc:
                logger.error("post_thread create_tweet failed at tweet %d: %s", len(ids) + 1, exc)
                break
            if not resp or not resp.data:
                logger.warning("Thread tweet returned no data, stopping thread")
                break
            tweet_id = str(resp.data["id"])
            ids.append(tweet_id)
            reply_to = tweet_id
            time.sleep(1)
        return ids

    def post_quote_tweet(self, text: str, original_tweet_id: str):
        try:
            resp = _retry(lambda: self.client.create_tweet(text=text, quote_tweet_id=original_tweet_id))
        except (tweepy.errors.TweepyException, requests.ConnectionError, requests.Timeout) as exc:
            logger.error("post_quote_tweet failed: %s", exc)
            raise
        if resp and resp.data:
            return str(resp.data["id"])
        logger.warning("post_quote_tweet returned no data")
        return None

    def post_reply(self, text: str, reply_to_tweet_id: str):
        try:
            resp = _retry(lambda: self.client.create_tweet(text=text, in_reply_to_tweet_id=reply_to_tweet_id))
        except (tweepy.errors.TweepyException, requests.ConnectionError, requests.Timeout) as exc:
            logger.error("post_reply failed: %s", exc)
            raise
        if resp and resp.data:
            return str(resp.data["id"])
        logger.warning("post_reply returned no data")
        return None

    def post_poll(self, question: str, options: list, duration_minutes: int = 1440):
        """Post a tweet with a poll. Note: polls and media are mutually exclusive."""
        if not options or len(options) < 2 or len(options) > 4:
            logger.error("post_poll: need 2-4 options, got %d", len(options) if options else 0)
            return None
        if not (5 <= duration_minutes <= 10080):
            logger.error("post_poll: duration_minutes must be 5-10080, got %d", duration_minutes)
            return None
        try:
            def _call():
                return self.client.create_tweet(
                    text=question,
                    poll_options=options,
                    poll_duration_minutes=duration_minutes,
                )
            resp = _retry(_call)
        except (tweepy.errors.TweepyException, requests.ConnectionError, requests.Timeout) as exc:
            logger.error("post_poll failed: %s", exc)
            raise
        if resp and resp.data:
            return str(resp.data["id"])
        logger.warning("post_poll returned no data")
        return None


class DryRunPoster:
    """Logs instead of posting. Use with --dry-run to smoke-test before live deploy."""

    def upload_image_from_url(self, image_url: str) -> str | None:
        logger.info("[DRY RUN] Would upload image from %s", image_url)
        return "dry_media_123"

    def upload_image_from_file(self, file_path: str) -> str | None:
        logger.info("[DRY RUN] Would upload image from file %s", file_path)
        return "dry_media_file_123"

    def post_tweet(self, text: str, media_ids: list = None):
        from bot.ai.generate import _twitter_len
        media_note = f" with {len(media_ids)} image(s)" if media_ids else ""
        logger.info("[DRY RUN] Would tweet (%d twitter-chars%s): %s",
                    _twitter_len(text), media_note, text[:120])
        return f"dry_{abs(hash(text)) % 10 ** 8}"

    def post_thread(self, tweets: list, first_tweet_media_ids: list = None) -> list:
        for i, t in enumerate(tweets, 1):
            media_note = " (with image)" if i == 1 and first_tweet_media_ids else ""
            logger.info("[DRY RUN] Thread %d/%d%s: %s", i, len(tweets), media_note, t[:80])
        return [f"dry_thread_{i}" for i in range(len(tweets))]

    def post_reply(self, text: str, reply_to_tweet_id: str):
        logger.info("[DRY RUN] Would reply to %s: %s", reply_to_tweet_id, text[:80])
        return f"dry_reply_{abs(hash(text)) % 10 ** 8}"

    def post_quote_tweet(self, text: str, original_tweet_id: str):
        logger.info("[DRY RUN] Would quote %s: %s", original_tweet_id, text[:80])
        return f"dry_quote_{abs(hash(text)) % 10 ** 8}"

    def post_poll(self, question: str, options: list, duration_minutes: int = 1440):
        logger.info("[DRY RUN] Would post poll: %s | Options: %s | Duration: %dm",
                    question[:80], options, duration_minutes)
        return f"dry_poll_{abs(hash(question)) % 10 ** 8}"
