# jobs/metrics_collector.py
import logging
import threading
import time

logger = logging.getLogger(__name__)


class MetricsCollectorJob:
    """Periodically fetch performance metrics for our own tweets (feedback loop)."""

    def __init__(self, store, engager, notifier=None):
        self.store = store
        self.engager = engager
        self.notifier = notifier

    def run(self):
        # Run in a background thread to avoid blocking the scheduler for 50-100+ seconds
        thread = threading.Thread(target=self._run_bg, daemon=True)
        thread.start()

    def _run_bg(self):
        try:
            self.execute()
        except Exception as exc:
            logger.exception("MetricsCollectorJob failed: %s", exc)
            if self.notifier:
                self.notifier.notify_failure("MetricsCollector", exc)

    def execute(self):
        tweet_rows = self.store.get_recent_original_tweet_ids(hours=168)
        if not tweet_rows:
            logger.debug("MetricsCollector: no recent tweets to check")
            return

        updated = 0
        errors = 0
        for row in tweet_rows:
            tweet_id = row["tweet_id"]
            # Skip dry-run fake IDs
            if tweet_id.startswith("dry_") or tweet_id.startswith("followup_for_"):
                continue
            try:
                metrics = self.engager.get_tweet_metrics(tweet_id)
                if metrics:
                    self.store.upsert_tweet_metrics(tweet_id, row["job_type"], metrics)
                    updated += 1
            except Exception as exc:
                errors += 1
                logger.warning("MetricsCollector: failed to fetch %s: %s", tweet_id, exc)
            time.sleep(1)  # respect rate limits

        if updated or errors:
            logger.info("MetricsCollector: updated %d, errors %d, total %d", updated, errors, len(tweet_rows))
