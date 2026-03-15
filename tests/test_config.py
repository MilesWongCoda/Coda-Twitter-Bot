# tests/test_config.py
import pytest
import os


def test_config_loads_env_vars(monkeypatch):
    monkeypatch.setenv("TWITTER_API_KEY", "test_key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test_anthropic")
    monkeypatch.setenv("OPENAI_API_KEY", "test_openai")
    monkeypatch.setenv("TWITTER_API_SECRET", "s")
    monkeypatch.setenv("TWITTER_ACCESS_TOKEN", "t")
    monkeypatch.setenv("TWITTER_ACCESS_TOKEN_SECRET", "ts")
    monkeypatch.setenv("TWITTER_BEARER_TOKEN", "bt")
    monkeypatch.delenv("COINGECKO_API_KEY", raising=False)  # optional key absent

    import importlib
    import bot.config as config_module
    importlib.reload(config_module)
    cfg = config_module.Config()
    assert cfg.twitter_api_key == "test_key"
    assert cfg.anthropic_api_key == "test_anthropic"
    assert cfg.openai_api_key == "test_openai"
    assert cfg.coingecko_api_key == ""  # empty when not set


def test_config_loads_with_optional_coingecko_key(monkeypatch):
    for key in ["TWITTER_API_KEY", "TWITTER_API_SECRET", "TWITTER_ACCESS_TOKEN",
                "TWITTER_ACCESS_TOKEN_SECRET", "TWITTER_BEARER_TOKEN",
                "ANTHROPIC_API_KEY", "OPENAI_API_KEY"]:
        monkeypatch.setenv(key, "x")
    monkeypatch.setenv("COINGECKO_API_KEY", "cg_demo_key")

    import importlib
    import bot.config as config_module
    importlib.reload(config_module)
    cfg = config_module.Config()
    assert cfg.coingecko_api_key == "cg_demo_key"


def test_config_raises_on_missing_env_var(monkeypatch):
    for key in ["TWITTER_API_SECRET", "TWITTER_ACCESS_TOKEN",
                "TWITTER_ACCESS_TOKEN_SECRET", "TWITTER_BEARER_TOKEN",
                "ANTHROPIC_API_KEY", "OPENAI_API_KEY"]:
        monkeypatch.setenv(key, "x")
    monkeypatch.delenv("TWITTER_API_KEY", raising=False)

    import importlib
    import bot.config as config_module
    importlib.reload(config_module)
    with pytest.raises(EnvironmentError):
        config_module.Config()
