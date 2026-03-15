# db/store.py
import os
import re
import sqlite3
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)


class Store:
    def __init__(self, db_path: str = "db/kol.db"):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        try:
            yield conn
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                logger.warning("Rollback also failed", exc_info=True)
            raise
        finally:
            conn.close()

    def _init_db(self):
        db_dir = os.path.dirname(self.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        with self._conn() as conn:
            result = conn.execute("PRAGMA journal_mode=WAL").fetchone()
            if self.db_path != ":memory:" and result and result[0].lower() != "wal":
                logger.warning("WAL mode not enabled, got: %s", result[0])
            conn.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id TEXT PRIMARY KEY,
                    job_type TEXT,
                    content TEXT,
                    topic TEXT,
                    posted_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_topic ON posts(topic, posted_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_posted_at ON posts(posted_at)")
            # Migration: add data_topics column for cross-job topic-level dedup
            try:
                conn.execute("ALTER TABLE posts ADD COLUMN data_topics TEXT DEFAULT ''")
            except Exception:
                pass  # column already exists
            # Migration: add variant column for A/B testing
            try:
                conn.execute("ALTER TABLE posts ADD COLUMN variant TEXT DEFAULT ''")
            except Exception:
                pass  # column already exists
            # Tweet performance metrics (feedback loop)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tweet_metrics (
                    tweet_id TEXT PRIMARY KEY,
                    job_type TEXT,
                    impressions INTEGER DEFAULT 0,
                    likes INTEGER DEFAULT 0,
                    retweets INTEGER DEFAULT 0,
                    replies INTEGER DEFAULT 0,
                    quotes INTEGER DEFAULT 0,
                    bookmarks INTEGER DEFAULT 0,
                    checked_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_metrics_job_type ON tweet_metrics(job_type)")
            # Engagement effectiveness tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS engagement_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_username TEXT,
                    target_tier TEXT,
                    action_type TEXT,
                    our_tweet_id TEXT,
                    engaged_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_englog_username ON engagement_log(target_username)")
            # Persistent 403 blocklist — users whose tweets block our replies
            conn.execute("""
                CREATE TABLE IF NOT EXISTS restricted_users (
                    username TEXT PRIMARY KEY,
                    reason TEXT DEFAULT '403_forbidden',
                    blocked_at TEXT DEFAULT (datetime('now'))
                )
            """)

    def mark_posted(self, tweet_id: str, job_type: str, content: str,
                    topic: Optional[str] = None, posted_at: Optional[str] = None,
                    data_topics: Optional[set] = None, variant: Optional[str] = None):
        dt_str = ",".join(sorted(data_topics)) if data_topics else ""
        var_str = variant or ""
        with self._conn() as conn:
            if posted_at:
                conn.execute(
                    "INSERT OR IGNORE INTO posts (id, job_type, content, topic, posted_at, data_topics, variant) VALUES (?,?,?,?,?,?,?)",
                    (tweet_id, job_type, content, topic, posted_at, dt_str, var_str)
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO posts (id, job_type, content, topic, data_topics, variant) VALUES (?,?,?,?,?,?)",
                    (tweet_id, job_type, content, topic, dt_str, var_str)
                )

    def is_posted(self, tweet_id: str) -> bool:
        with self._conn() as conn:
            row = conn.execute("SELECT 1 FROM posts WHERE id=?", (tweet_id,)).fetchone()
            return row is not None

    def topic_on_cooldown(self, topic: str, cooldown_hours: int = 4) -> bool:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=cooldown_hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM posts WHERE topic=? AND posted_at > ?",
                (topic, cutoff)
            ).fetchone()
            return row is not None

    def get_recent_data_topics(self, hours: int = 6) -> set:
        """Return all data_topics from posts in the last N hours.

        Used for cross-job topic-level dedup: if any job already tweeted about
        'fear_greed', other jobs should avoid repeating it within the cooldown.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT data_topics FROM posts WHERE posted_at > ? AND data_topics != ''",
                (cutoff,)
            ).fetchall()
            topics = set()
            for (dt_str,) in rows:
                if dt_str:
                    topics.update(t.strip() for t in dt_str.split(",") if t.strip())
            return topics

    def get_recent_content(self, hours: int = 24) -> list:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT content FROM posts WHERE posted_at > ? ORDER BY posted_at DESC",
                (cutoff,)
            ).fetchall()
            return [r[0] for r in rows]

    def get_content_by_id(self, tweet_id: str) -> Optional[str]:
        with self._conn() as conn:
            row = conn.execute("SELECT content FROM posts WHERE id=?", (tweet_id,)).fetchone()
            return row[0] if row else None

    def prune_old_posts(self, keep_days: int = 90) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            # Clean up truly orphaned metrics (parent post already deleted in a prior run)
            conn.execute("""
                DELETE FROM tweet_metrics WHERE tweet_id NOT IN (SELECT id FROM posts)
            """)
            # Delete metrics for posts about to be pruned
            conn.execute("""
                DELETE FROM tweet_metrics WHERE tweet_id IN (
                    SELECT id FROM posts WHERE posted_at < ?
                )
            """, (cutoff,))
            # Prune old engagement log entries
            conn.execute("DELETE FROM engagement_log WHERE engaged_at < ?", (cutoff,))
            cursor = conn.execute("DELETE FROM posts WHERE posted_at < ?", (cutoff,))
            deleted = cursor.rowcount
            if deleted:
                logger.info("Pruned %d posts older than %d days", deleted, keep_days)
            return deleted

    def count_posts_since_midnight_batch(self) -> dict:
        """Count all job types since midnight in a single query."""
        midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT job_type, COUNT(*) FROM posts WHERE posted_at >= ? GROUP BY job_type",
                (midnight,)
            ).fetchall()
            return {row[0]: row[1] for row in rows}

    def count_posts_yesterday_batch(self) -> dict:
        """Count all job types for the previous UTC calendar day."""
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        today_str = today.strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT job_type, COUNT(*) FROM posts WHERE posted_at >= ? AND posted_at < ? GROUP BY job_type",
                (yesterday, today_str)
            ).fetchall()
            return {row[0]: row[1] for row in rows}

    def upsert_tweet_metrics(self, tweet_id: str, job_type: str, metrics: dict):
        """Insert or update tweet metrics."""
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO tweet_metrics (tweet_id, job_type, impressions, likes, retweets, replies, quotes, bookmarks, checked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(tweet_id) DO UPDATE SET
                    impressions=excluded.impressions, likes=excluded.likes,
                    retweets=excluded.retweets, replies=excluded.replies,
                    quotes=excluded.quotes, bookmarks=excluded.bookmarks,
                    checked_at=excluded.checked_at
            """, (
                tweet_id, job_type,
                metrics.get("impressions", 0), metrics.get("likes", 0),
                metrics.get("retweets", 0), metrics.get("replies", 0),
                metrics.get("quotes", 0), metrics.get("bookmarks", 0),
            ))

    def get_top_tweets(self, days: int = 7, limit: int = 3, job_type: Optional[str] = None) -> list:
        """Return top-performing tweets by weighted engagement score in the last N days.

        Score = likes*3 + retweets*5 + replies*2 + bookmarks*4.
        Optionally filter by job_type for per-column feedback.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        sql = """
            SELECT m.tweet_id, m.impressions, m.likes, m.retweets, p.content,
                   (m.likes * 3 + m.retweets * 5 + m.replies * 2 + m.bookmarks * 4) AS eng_score
            FROM tweet_metrics m
            JOIN posts p ON m.tweet_id = p.id
            WHERE p.posted_at > ? AND m.impressions >= 100
        """
        params: list = [cutoff]
        if job_type:
            sql += " AND m.job_type = ?"
            params.append(job_type)
        sql += " ORDER BY eng_score DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [
                {"tweet_id": r[0], "impressions": r[1], "likes": r[2], "retweets": r[3], "content": r[4]}
                for r in rows
            ]

    def get_recent_original_tweet_ids(self, hours: int = 72) -> list:
        """Return tweet IDs of original content posts (not replies/quotes) from the last N hours."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        exclude_types = ("reply", "quote_tweet", "self_reply", "self_followup")
        placeholders = ",".join("?" for _ in exclude_types)
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT id, job_type FROM posts WHERE posted_at > ? AND job_type NOT IN ({placeholders}) ORDER BY posted_at DESC",
                (cutoff, *exclude_types)
            ).fetchall()
            return [{"tweet_id": r[0], "job_type": r[1]} for r in rows]

    def get_best_tweet_yesterday(self) -> Optional[dict]:
        """Return the best tweet from yesterday by impressions (for daily report)."""
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        today_str = today.strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            row = conn.execute("""
                SELECT m.tweet_id, m.impressions, m.likes, m.retweets, p.content
                FROM tweet_metrics m
                JOIN posts p ON m.tweet_id = p.id
                WHERE p.posted_at >= ? AND p.posted_at < ? AND m.impressions > 0
                ORDER BY m.impressions DESC
                LIMIT 1
            """, (yesterday, today_str)).fetchone()
            if row:
                return {"tweet_id": row[0], "impressions": row[1], "likes": row[2], "retweets": row[3], "content": row[4]}
            return None

    def get_avg_metrics_by_type(self) -> dict:
        """Return yesterday's average metrics per job_type for the daily report.

        Returns: {job_type: {avg_impressions, avg_likes, avg_eng_score, count}}
        """
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = (today - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        today_str = today.strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT m.job_type,
                       COUNT(*) AS cnt,
                       AVG(m.impressions) AS avg_imp,
                       AVG(m.likes) AS avg_likes,
                       AVG(m.likes * 3 + m.retweets * 5 + m.replies * 2 + m.bookmarks * 4) AS avg_eng
                FROM tweet_metrics m
                JOIN posts p ON m.tweet_id = p.id
                WHERE p.posted_at >= ? AND p.posted_at < ? AND m.impressions > 0
                GROUP BY m.job_type
            """, (yesterday, today_str)).fetchall()
            return {
                r[0]: {"count": r[1], "avg_impressions": r[2] or 0, "avg_likes": r[3] or 0, "avg_eng_score": r[4] or 0}
                for r in rows
            }

    def get_bottom_tweets(self, days: int = 7, limit: int = 2, job_type: Optional[str] = None) -> list:
        """Return worst-performing tweets by engagement score in the last N days.

        Only includes tweets with >= 100 impressions (enough data to judge).
        Used as negative feedback: 'avoid writing like these'.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        sql = """
            SELECT m.tweet_id, m.impressions, m.likes, m.retweets, p.content,
                   (m.likes * 3 + m.retweets * 5 + m.replies * 2 + m.bookmarks * 4) AS eng_score
            FROM tweet_metrics m
            JOIN posts p ON m.tweet_id = p.id
            WHERE p.posted_at > ? AND m.impressions >= 100
        """
        params: list = [cutoff]
        if job_type:
            sql += " AND m.job_type = ?"
            params.append(job_type)
        sql += " ORDER BY eng_score ASC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
            return [
                {"tweet_id": r[0], "impressions": r[1], "likes": r[2], "retweets": r[3], "content": r[4]}
                for r in rows
            ]

    @staticmethod
    def _analyze_tweet_patterns(tweets: list) -> dict:
        """Analyze structural features of a list of tweet dicts (with 'content' key).

        Returns metrics: avg_length, avg_lines, question_pct, data_density,
        definitive_ending_pct.
        """
        if not tweets:
            return {}
        lengths = []
        line_counts = []
        questions = 0
        data_points = 0
        definitive_endings = 0
        _NUM_RE = re.compile(r'[\d,]+\.?\d*[%KMBkmb]?')
        _DEFINITIVE_RE = re.compile(
            r'(This is|That\'s|Shorts are|Longs are|Accumulation|Distribution|Bullish|Bearish|'
            r'Not sustainable|Game over|Pain trade|Squeez)',
            re.IGNORECASE,
        )
        for t in tweets:
            content = t.get("content", "") or ""
            if not content:
                continue
            lengths.append(len(content))
            lines = [ln for ln in content.split("\n") if ln.strip()]
            line_counts.append(len(lines))
            if "?" in content:
                questions += 1
            data_points += len(_NUM_RE.findall(content))
            # Check last line for definitive ending
            last_line = lines[-1] if lines else ""
            if _DEFINITIVE_RE.search(last_line):
                definitive_endings += 1

        n = len(lengths) or 1
        return {
            "avg_length": round(sum(lengths) / n),
            "avg_lines": round(sum(line_counts) / n, 1),
            "question_pct": round(questions / n * 100),
            "data_density": round(data_points / n, 1),
            "definitive_ending_pct": round(definitive_endings / n * 100),
        }

    def get_performance_patterns(self, days: int = 7) -> dict:
        """Compare structural patterns of top vs bottom tweets and generate insights.

        Returns: {top_patterns: dict, bottom_patterns: dict, insights: list[str]}
        """
        top = self.get_top_tweets(days=days, limit=10)
        bottom = self.get_bottom_tweets(days=days, limit=10)
        if not top and not bottom:
            return {}
        top_p = self._analyze_tweet_patterns(top)
        bot_p = self._analyze_tweet_patterns(bottom)
        if not top_p or not bot_p:
            return {"top_patterns": top_p, "bottom_patterns": bot_p, "insights": []}

        insights = []
        # Length comparison
        if top_p["avg_length"] < bot_p["avg_length"] * 0.85:
            insights.append(
                f"Shorter tweets perform better (top avg {top_p['avg_length']} chars "
                f"vs bottom avg {bot_p['avg_length']} chars)"
            )
        elif top_p["avg_length"] > bot_p["avg_length"] * 1.15:
            insights.append(
                f"Longer tweets perform better (top avg {top_p['avg_length']} chars "
                f"vs bottom avg {bot_p['avg_length']} chars)"
            )
        # Question usage
        if top_p["question_pct"] > bot_p["question_pct"] + 15:
            insights.append(
                f"Questions drive more engagement ({top_p['question_pct']}% of top tweets vs "
                f"{bot_p['question_pct']}% of bottom)"
            )
        # Data density
        if top_p["data_density"] > bot_p["data_density"] + 0.5:
            insights.append(
                f"Data-heavy tweets perform better (top avg {top_p['data_density']} numbers/tweet "
                f"vs bottom {bot_p['data_density']})"
            )
        elif top_p["data_density"] < bot_p["data_density"] - 0.5:
            insights.append(
                f"Less data, more narrative performs better (top avg {top_p['data_density']} numbers "
                f"vs bottom {bot_p['data_density']})"
            )
        # Definitive endings
        if top_p["definitive_ending_pct"] > bot_p["definitive_ending_pct"] + 15:
            insights.append(
                f"Definitive endings drive engagement ({top_p['definitive_ending_pct']}% of top tweets)"
            )
        # Line count
        if top_p["avg_lines"] < bot_p["avg_lines"] - 0.5:
            insights.append(
                f"Fewer lines work better (top avg {top_p['avg_lines']} lines "
                f"vs bottom {bot_p['avg_lines']})"
            )

        return {"top_patterns": top_p, "bottom_patterns": bot_p, "insights": insights}

    def get_variant_performance(self, days: int = 14) -> list:
        """Return average engagement per variant over the last N days.

        Returns list of dicts: [{variant, count, avg_engagement}], sorted by avg desc.
        Only includes variants with >= 3 data points.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT p.variant,
                       COUNT(*) AS cnt,
                       AVG(m.likes * 3 + m.retweets * 5 + m.replies * 2 + m.bookmarks * 4) AS avg_eng
                FROM posts p
                JOIN tweet_metrics m ON p.id = m.tweet_id
                WHERE p.posted_at > ? AND p.variant != '' AND m.impressions >= 100
                GROUP BY p.variant
                HAVING cnt >= 3
                ORDER BY avg_eng DESC
            """, (cutoff,)).fetchall()
            return [
                {"variant": r[0], "count": r[1], "avg_engagement": round(r[2] or 0, 1)}
                for r in rows
            ]

    def get_metrics_by_hour(self, days: int = 14) -> dict:
        """Return average performance metrics grouped by hour of day.

        Returns: {hour_str: {count, avg_impressions, avg_engagement}}
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT strftime('%H', p.posted_at) AS hour,
                       COUNT(*) AS cnt,
                       AVG(m.impressions) AS avg_imp,
                       AVG(m.likes * 3 + m.retweets * 5 + m.replies * 2 + m.bookmarks * 4) AS avg_eng
                FROM posts p
                JOIN tweet_metrics m ON p.id = m.tweet_id
                WHERE p.posted_at > ? AND m.impressions >= 100
                GROUP BY hour
                ORDER BY hour
            """, (cutoff,)).fetchall()
            return {
                r[0]: {"count": r[1], "avg_impressions": round(r[2] or 0), "avg_engagement": round(r[3] or 0, 1)}
                for r in rows
            }

    def get_recent_engagement_tweet_ids(self, hours: int = 48) -> list:
        """Return tweet IDs of recent reply/quote_tweet posts for conversation tracking."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT id FROM posts WHERE job_type IN ('reply', 'quote_tweet', 'conversation_reply') AND posted_at > ?",
                (cutoff,)
            ).fetchall()
            return [r[0] for r in rows]

    def log_engagement(self, username: str, tier: str, action_type: str, our_tweet_id: str = ""):
        """Log an engagement action for effectiveness tracking."""
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO engagement_log (target_username, target_tier, action_type, our_tweet_id) VALUES (?,?,?,?)",
                (username, tier, action_type, our_tweet_id)
            )

    def get_engagement_effectiveness(self, days: int = 7) -> list:
        """Return engagement stats per target username over the last N days.

        Returns list of dicts: [{username, tier, total, conversation_replies}]
        sorted by total desc.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT target_username, target_tier,
                       COUNT(*) AS total,
                       SUM(CASE WHEN action_type = 'conversation_reply' THEN 1 ELSE 0 END) AS convo_replies
                FROM engagement_log
                WHERE engaged_at > ?
                GROUP BY target_username
                ORDER BY total DESC
            """, (cutoff,)).fetchall()
            return [
                {"username": r[0], "tier": r[1], "total": r[2], "conversation_replies": r[3]}
                for r in rows
            ]

    def mark_user_restricted(self, username: str, reason: str = "403_forbidden"):
        """Mark a user as restricted (403). TTL enforced on read side."""
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO restricted_users (username, reason, blocked_at) VALUES (?, ?, datetime('now'))",
                (username, reason)
            )

    def is_user_restricted(self, username: str, ttl_hours: int = 48) -> bool:
        """Check if a user is on the 403 blocklist and still within TTL."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=ttl_hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM restricted_users WHERE username=? AND blocked_at > ?",
                (username, cutoff)
            ).fetchone()
            return row is not None

    def prune_restricted_users(self, ttl_hours: int = 48) -> int:
        """Remove expired entries from the restricted users table."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=ttl_hours)).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            cursor = conn.execute("DELETE FROM restricted_users WHERE blocked_at <= ?", (cutoff,))
            return cursor.rowcount

    def count_posts_since_midnight(self, job_type: str) -> int:
        midnight = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM posts WHERE job_type=? AND posted_at >= ?",
                (job_type, midnight)
            ).fetchone()
            return row[0]
