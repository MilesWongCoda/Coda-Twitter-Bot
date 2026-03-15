# tests/test_news.py
import pytest
from unittest.mock import patch, MagicMock
from bot.data.news import NewsFetcher


@pytest.fixture
def fetcher():
    with patch("bot.data.news.create_session") as mock_cs:
        mock_cs.return_value = MagicMock()
        f = NewsFetcher()
    return f


def _mock_requests_and_feedparser():
    """Return a mock response for session.get."""
    mock_resp = MagicMock()
    mock_resp.content = b"<rss>fake</rss>"
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def test_parse_rss_returns_articles(fetcher):
    mock_resp = _mock_requests_and_feedparser()
    mock_feed = MagicMock()
    mock_feed.entries = [
        MagicMock(title="BTC hits 100k", summary="Bitcoin reached...", link="http://a.com/1",
                  published="Mon, 22 Feb 2026 08:00:00 +0000"),
        MagicMock(title="Fed holds rates", summary="Federal Reserve...", link="http://a.com/2",
                  published="Mon, 22 Feb 2026 07:00:00 +0000"),
    ]
    fetcher._session.get.return_value = mock_resp
    with patch("feedparser.parse", return_value=mock_feed):
        articles = fetcher.fetch_feed("http://fake-rss.com/feed")
    assert len(articles) == 2
    assert articles[0]["title"] == "BTC hits 100k"
    assert "link" in articles[0]


def test_fetch_all_returns_combined(fetcher):
    mock_resp = _mock_requests_and_feedparser()
    mock_feed = MagicMock()
    mock_feed.entries = [MagicMock(title="News", summary="Summary", link="http://x.com",
                                    published="Mon, 22 Feb 2026 08:00:00 +0000")]
    fetcher._session.get.return_value = mock_resp
    with patch("feedparser.parse", return_value=mock_feed):
        articles = fetcher.fetch_all()
    assert len(articles) > 0


def test_deduplicate_by_link(fetcher):
    articles = [
        {"title": "A", "link": "http://same.com", "summary": "x"},
        {"title": "A dup", "link": "http://same.com", "summary": "x"},
        {"title": "B", "link": "http://other.com", "summary": "y"},
    ]
    unique = fetcher.deduplicate(articles)
    assert len(unique) == 2


def test_deduplicate_skips_empty_links(fetcher):
    articles = [
        {"title": "No link 1", "link": "", "summary": "x"},
        {"title": "No link 2", "link": "", "summary": "y"},
        {"title": "Has link", "link": "http://a.com", "summary": "z"},
    ]
    unique = fetcher.deduplicate(articles)
    assert len(unique) == 1
    assert unique[0]["title"] == "Has link"


def test_fetch_all_uses_cache_on_second_call(fetcher):
    mock_resp = _mock_requests_and_feedparser()
    mock_feed = MagicMock()
    mock_feed.entries = [MagicMock(title="News", summary="Sum", link="http://x.com",
                                    published="Mon, 22 Feb 2026 08:00:00 +0000")]
    fetcher._session.get.return_value = mock_resp
    with patch("feedparser.parse", return_value=mock_feed):
        fetcher.fetch_all()
        fetcher.fetch_all()  # second call within TTL
    assert fetcher._session.get.call_count == len(fetcher.feeds)  # only called once per feed, not twice


def test_fetch_all_empty_result_not_cached(fetcher):
    """If all feeds return nothing, the result must NOT be cached.
    Next call should re-attempt fetching instead of serving a stale empty list."""
    mock_resp = _mock_requests_and_feedparser()
    mock_feed = MagicMock()
    mock_feed.entries = []
    fetcher._session.get.return_value = mock_resp
    with patch("feedparser.parse", return_value=mock_feed):
        result1 = fetcher.fetch_all()
        result2 = fetcher.fetch_all()
    assert result1 == []
    assert result2 == []
    # fetch_feed is called twice per feed set (not served from cache since empty)
    assert fetcher._session.get.call_count == 2 * len(fetcher.feeds)


def test_fetch_feed_handles_request_error(fetcher):
    """Network error in session.get should propagate so fetch_all catches it."""
    fetcher._session.get.side_effect = Exception("Connection refused")
    with pytest.raises(Exception, match="Connection refused"):
        fetcher.fetch_feed("http://dead-feed.com/rss")
