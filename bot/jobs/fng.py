# jobs/fng.py
"""Daily Fear & Greed Index tweet with gauge image."""

import os
import logging
from datetime import datetime, timezone
from bot.jobs.base import BaseJob
from bot.data.fng import fetch_fng, generate_gauge_image

logger = logging.getLogger(__name__)


class FearGreedJob(BaseJob):
    def __init__(self, store, generator, poster, notifier=None):
        super().__init__(store, generator, poster, notifier=notifier)

    def execute(self):
        dedup_key = f"fng_{datetime.now(timezone.utc).strftime('%Y-%m-%d')}"
        if self.store.is_posted(dedup_key):
            logger.info("FNG already posted today, skipping")
            return []

        data = fetch_fng()
        if not data:
            logger.warning("FNG: failed to fetch data")
            return []

        value = data["value"]
        label = data["label"]
        color = data["color"]

        date_str = datetime.now(timezone.utc).strftime("%d %B %Y")
        img_path = generate_gauge_image(value, date_str)

        try:
            # Upload image
            media_id = self.poster.upload_image_from_file(img_path)
            if not media_id:
                logger.error("FNG: image upload failed")
                return []

            # Tweet text — short, with $BTC cashtag for search discoverability
            tweet = f"$BTC Fear & Greed Index: {value} — {label}"
            tweet_id = self.poster.post_tweet(tweet, media_ids=[media_id])

            if tweet_id:
                self.store.mark_posted(dedup_key, "fng", tweet)
                self.store.mark_posted(tweet_id, "fng", tweet)
                logger.info("FNG posted: %s (%s)", value, label)
                return [dedup_key]
        finally:
            # Clean up temp image
            try:
                os.unlink(img_path)
            except OSError:
                pass

        return []
