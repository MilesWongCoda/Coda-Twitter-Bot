# tests/test_base_job.py
import pytest
from unittest.mock import MagicMock
from bot.jobs.base import BaseJob


def test_base_job_run_calls_execute():
    class TestJob(BaseJob):
        def execute(self):
            return "executed"

    store = MagicMock()
    store.get_recent_content.return_value = []
    job = TestJob(store=store, generator=MagicMock(), poster=MagicMock())
    result = job.run()
    assert result == "executed"


def test_base_job_run_catches_exceptions():
    class BrokenJob(BaseJob):
        def execute(self):
            raise ValueError("something went wrong")

    store = MagicMock()
    job = BrokenJob(store=store, generator=MagicMock(), poster=MagicMock())
    result = job.run()
    assert result is None


def test_get_recent_tweets_delegates_to_store():
    class TestJob(BaseJob):
        def execute(self):
            pass

    store = MagicMock()
    store.get_recent_content.return_value = ["tweet 1", "tweet 2"]
    job = TestJob(store=store, generator=MagicMock(), poster=MagicMock())
    recent = job.get_recent_tweets()
    assert recent == ["tweet 1", "tweet 2"]
    store.get_recent_content.assert_called_once_with(hours=24)
