# tests/test_summarize.py
import pytest
from unittest.mock import patch, MagicMock
from bot.ai.summarize import Summarizer


@pytest.fixture
def summarizer():
    return Summarizer(api_key="test_key")


def test_summarize_articles(summarizer):
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content="BTC up, Fed hawkish, ETH consolidating"))]
    )
    with patch.object(summarizer, "client", mock_client):
        result = summarizer.summarize([
            {"title": "BTC surges", "summary": "Bitcoin rose 5% as..."},
            {"title": "Fed holds", "summary": "Federal Reserve kept rates..."},
        ])
    assert "BTC" in result
    assert len(result) > 10


def test_summarize_empty_list(summarizer):
    result = summarizer.summarize([])
    assert result == ""


def test_summarize_caps_at_max_articles(summarizer):
    mock_client = MagicMock()
    mock_client.chat.completions.create.return_value = MagicMock(
        choices=[MagicMock(message=MagicMock(content="summary"))]
    )
    articles = [{"title": f"Article {i}", "summary": "text"} for i in range(20)]
    with patch.object(summarizer, "client", mock_client):
        summarizer.summarize(articles, max_articles=5)
    call_args = mock_client.chat.completions.create.call_args
    user_content = call_args.kwargs["messages"][1]["content"]
    assert user_content.count("Article") == 5
