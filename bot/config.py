# config.py
import os
from dotenv import load_dotenv

load_dotenv()

REQUIRED_KEYS = [
    "TWITTER_API_KEY", "TWITTER_API_SECRET", "TWITTER_ACCESS_TOKEN",
    "TWITTER_ACCESS_TOKEN_SECRET", "TWITTER_BEARER_TOKEN",
    "ANTHROPIC_API_KEY",
    # OPENAI_API_KEY is optional: only used by Summarizer. Without it, news
    # summarization will fail but all other jobs will work fine.
    # COINGECKO_API_KEY is optional: the demo endpoint works without a key,
    # but a key raises the rate limit. Leave blank to use anonymous access.
]


ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o").strip()
BOT_HANDLE = os.environ.get("BOT_HANDLE", "YourBotHandle").strip()


class Config:
    def __init__(self):
        missing = [k for k in REQUIRED_KEYS if not os.environ.get(k, "").strip()]
        if missing:
            raise EnvironmentError(f"Missing required environment variables: {missing}")
        self.twitter_api_key = os.environ["TWITTER_API_KEY"].strip()
        self.twitter_api_secret = os.environ["TWITTER_API_SECRET"].strip()
        self.twitter_access_token = os.environ["TWITTER_ACCESS_TOKEN"].strip()
        self.twitter_access_token_secret = os.environ["TWITTER_ACCESS_TOKEN_SECRET"].strip()
        self.twitter_bearer_token = os.environ["TWITTER_BEARER_TOKEN"].strip()
        self.anthropic_api_key = os.environ["ANTHROPIC_API_KEY"].strip()
        self.openai_api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        self.coingecko_api_key = os.environ.get("COINGECKO_API_KEY", "").strip()
        # Optional — derivatives + ETF/exchange flow features disabled if not set
        self.coinglass_api_key = os.environ.get("COINGLASS_API_KEY", "").strip()
        # Optional — whale alert features disabled if not set
        self.whale_alert_api_key = os.environ.get("WHALE_ALERT_API_KEY", "").strip()
        # Optional — Telegram alerts disabled if not set
        self.telegram_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        self.telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
        # Optional — public Telegram channel for content syndication
        self.telegram_channel_id = os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
        # Optional — GIPHY API key (1 GIF/day on trend alerts)
        self.giphy_api_key = os.environ.get("GIPHY_API_KEY", "").strip()
