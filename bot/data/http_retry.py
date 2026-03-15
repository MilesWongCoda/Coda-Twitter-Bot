# data/http_retry.py
"""Shared retry-enabled HTTP session for all data fetchers."""
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


def create_session(retries: int = 3, backoff_factor: float = 1.0,
                   status_forcelist=(429, 500, 502, 503), timeout: float = 15) -> requests.Session:
    """Create a requests.Session with automatic retry on transient errors.

    Default: 3 retries with 1s/2s/4s backoff on 429/500/502/503.
    After all retries exhausted, the final response is returned (not raised).
    Callers use resp.raise_for_status() to detect the final failure.
    """
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    # Note: requests.Session does not support session-level timeout.
    # All callers MUST pass timeout= explicitly to .get()/.post().
    return session
