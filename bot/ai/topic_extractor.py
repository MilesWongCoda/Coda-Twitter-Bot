# ai/topic_extractor.py
"""Lightweight keyword-based topic extractor for cross-job dedup.

Pure regex/keyword matching — no AI calls. Extracts data-level topic tags
from tweet text so we can detect when multiple jobs tweet about the same
underlying data point (e.g., Fear & Greed, CME OI, hashrate).
"""
import re

# Each entry: (topic_tag, compiled_regex)
# Patterns are case-insensitive. Order doesn't matter — all are checked.
_TOPIC_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("fear_greed", re.compile(
        r"f\s*&\s*g|fear\s*[&/]\s*greed|fear\s+and\s+greed", re.IGNORECASE
    )),
    ("hashrate", re.compile(
        r"hash\s*rate|EH/s|mining\s+difficulty", re.IGNORECASE
    )),
    ("cme_oi", re.compile(
        r"\bCME\b|\bopen\s+interest\b|\bOI\b", re.IGNORECASE
    )),
    ("exchange_flow", re.compile(
        r"exchange\s+(out|in)flow|exchange\s+balance|net\s+(in|out)flow",
        re.IGNORECASE
    )),
    ("etf_flow", re.compile(
        r"ETF\s+(flow|inflow|outflow)|spot\s+ETF", re.IGNORECASE
    )),
    ("funding_rate", re.compile(
        r"funding\s+rate|funding|perp\s+basis", re.IGNORECASE
    )),
    ("whale_alert", re.compile(
        r"\bwhale\b|large\s+transfer", re.IGNORECASE
    )),
    ("btc_price", re.compile(
        r"\$BTC\b.{0,30}(move[ds]?|pump|dump|rall(y|ied)|drop|crash|surge[ds]?|dip|bounce[ds]?)",
        re.IGNORECASE
    )),
    ("eth_price", re.compile(
        r"\$ETH\b.{0,30}(move[ds]?|pump|dump|rall(y|ied)|drop|crash|surge[ds]?|dip|bounce[ds]?)",
        re.IGNORECASE
    )),
    ("sol_price", re.compile(
        r"\$SOL\b.{0,30}(move[ds]?|pump|dump|rall(y|ied)|drop|crash|surge[ds]?|dip|bounce[ds]?)",
        re.IGNORECASE
    )),
    ("xrp_price", re.compile(
        r"\$XRP\b.{0,30}(move[ds]?|pump|dump|rall(y|ied)|drop|crash|surge[ds]?|dip|bounce[ds]?)",
        re.IGNORECASE
    )),
    ("miner", re.compile(
        r"\bminer\b|\bmining\b|miner\s+(outflow|capitulation)", re.IGNORECASE
    )),
    ("liquidation", re.compile(
        r"liquidat(ion|ed)|squeeze[ds]?", re.IGNORECASE
    )),
    ("polymarket", re.compile(
        r"polymarket|prediction\s+market|betting\s+odds|%\s+YES\b|%\s+NO\b", re.IGNORECASE
    )),
]


def extract_data_topics(text: str) -> set[str]:
    """Extract data-level topic tags from tweet text.

    Returns a set of topic tags like {"fear_greed", "hashrate", "cme_oi"}.
    These are used for cross-job dedup cooldown — if a topic was tweeted
    recently by any job, other jobs should avoid repeating it.

    Pure keyword/regex matching, no AI calls.
    """
    if not text:
        return set()
    topics = set()
    for tag, pattern in _TOPIC_PATTERNS:
        if pattern.search(text):
            topics.add(tag)
    return topics
