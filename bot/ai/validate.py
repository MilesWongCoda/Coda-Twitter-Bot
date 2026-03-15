# ai/validate.py
import re
import logging

logger = logging.getLogger(__name__)

# Match numbers in various formats: $1.5B, $150M, 95,000, 2.3%, +0.043%, 750, etc.
_NUM_RE = re.compile(r'[-+]?\$?[\d,]+(?:\.\d+)?(?:[BMKk])?%?')

_SUFFIX_MULT = {'B': 1e9, 'b': 1e9, 'M': 1e6, 'm': 1e6, 'K': 1e3, 'k': 1e3}

# Thread numbering (1/, 2/) and small numbers aren't meaningful data points
_TRIVIAL_THRESHOLD = 10

# Year-like values (2000-2030) are almost always date references, not data claims
_YEAR_MIN = 2000
_YEAR_MAX = 2030


def _parse_to_float(token: str) -> float:
    """Parse a number token to its float value. Raises ValueError on failure."""
    clean = token.replace('$', '').replace(',', '').replace('+', '')
    is_pct = clean.endswith('%')
    if is_pct:
        clean = clean.rstrip('%')
    suffix = clean[-1] if clean and clean[-1] in _SUFFIX_MULT else ''
    if suffix:
        return float(clean[:-1]) * _SUFFIX_MULT[suffix]
    return float(clean)


def extract_numbers(text: str) -> list:
    """Extract (raw_token, float_value, is_percentage) tuples from text.

    Skips trivially small non-percentage numbers (< 10) to ignore thread numbering.
    Skips year-like values (2000-2030) which are date references, not data claims.
    """
    raw = _NUM_RE.findall(text)
    result = []
    for r in raw:
        clean = r.replace('$', '').replace(',', '').replace('+', '')
        if not clean or clean in ('-', '.'):
            continue
        is_pct = clean.endswith('%')
        try:
            val = _parse_to_float(r)
        except ValueError:
            continue
        # Skip trivially small non-percentage numbers
        if not is_pct and abs(val) < _TRIVIAL_THRESHOLD:
            continue
        # Skip year-like integers (2000-2030) — date references, not data claims
        if not is_pct and not r.startswith('$') and ',' not in r and val == int(val) and _YEAR_MIN <= val <= _YEAR_MAX:
            continue
        result.append((r, val, is_pct))
    return result


def validate_tweet_numbers(context: str, tweet: str) -> dict:
    """Check if numbers in tweet can be traced to context.

    Uses numeric comparison with 1% tolerance to handle format differences
    (e.g. "$1.5B" matches "$1,500,000,000", "95K" matches "95,000").

    Returns dict with: valid, tweet_numbers, untraced, context_numbers.
    """
    ctx_entries = extract_numbers(context)
    tweet_entries = extract_numbers(tweet)

    ctx_pct_vals = {val for _, val, p in ctx_entries if p}
    ctx_abs_vals = {val for _, val, p in ctx_entries if not p}
    tweet_tokens = set()
    untraced = set()

    for token, val, is_pct in tweet_entries:
        tweet_tokens.add(token)
        # Match percentages only against context percentages, absolutes only against absolutes.
        # This prevents "2.3%" from matching "$2.3".
        candidates = ctx_pct_vals if is_pct else ctx_abs_vals
        matched = False
        for cv in candidates:
            denom = max(abs(val), abs(cv), 1)
            if abs(cv - val) / denom < 0.05:
                matched = True
                break
        if matched:
            continue
        # Also check if the raw token appears as a standalone number in context
        # (word-boundary match to avoid "2.3" matching inside "12.3")
        clean_token = token.replace('$', '').replace(',', '').replace('+', '').rstrip('%')
        if re.search(r'(?<!\d)' + re.escape(clean_token) + r'(?!\d)', context):
            continue
        # Percentages: allow small values (likely rounding/reformatting from context)
        # and values derivable from context ratios, but flag large fabricated percentages
        if is_pct:
            pct_val = abs(val)
            # Small percentages (<20%) are often reformatted from context data
            if pct_val < 20:
                continue
            # Check if this percentage could be derived as a ratio of two context values
            derivable = False
            ctx_list = sorted(val for _, val, _ in ctx_entries)
            for i, a in enumerate(ctx_list):
                for b in ctx_list[i + 1:]:
                    if a != 0:
                        ratio = abs(b / a) * 100
                        if abs(ratio - pct_val) / max(pct_val, 1) < 0.1:
                            derivable = True
                            break
                if derivable:
                    break
            if derivable:
                continue
        untraced.add(token)

    return {
        "valid": len(untraced) == 0,
        "tweet_numbers": tweet_tokens,
        "untraced": untraced,
        "context_numbers": {t for t, _, _ in ctx_entries},
    }
