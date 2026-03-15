# Crypto Flow Tracker Bot

AI-powered crypto Twitter bot that posts market analysis, tracks on-chain data, and engages with the crypto community.

## What it does

- **Scheduled content**: Morning brief, hot take, US open, evening wrap — each with real market data
- **Data-driven tweets**: Price alerts (>5% moves), trending coins, Fear & Greed Index, whale alerts
- **AI generation**: Claude generates tweets from live data with tone variation and feedback loops
- **Engagement**: Automated likes on KOL tweets for organic discovery
- **Telegram notifications**: Daily reports, error alerts, performance metrics
- **Feedback loop**: Tracks tweet performance and feeds top/bottom performers back into AI prompts

## Architecture

```
bot/
  ai/           # Tweet generation (Claude) + news summarization (GPT-4o) + validation
  data/         # Market data fetchers (CoinGecko, CoinGlass, Polymarket, whale alerts, etc.)
  db/           # SQLite store for tweets, metrics, engagement tracking
  jobs/         # APScheduler jobs — each job = one content type
  twitter/      # Tweepy wrapper: posting, engagement, watchlist
  notifications/# Telegram alerts + daily reports
  config.py     # Environment-based configuration
  main.py       # Entry point — scheduler setup
```

## Setup

```bash
# Clone and create virtual environment
git clone https://github.com/YOUR_USERNAME/chainmacrolab.git
cd chainmacrolab
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your API keys (see below)

# Run
python -m bot.main

# Dry run (no tweets posted, in-memory DB)
python -m bot.main --dry-run
```

## Required API Keys

| Key | Source | Required |
|-----|--------|----------|
| `TWITTER_API_KEY` / `SECRET` | [Twitter Developer Portal](https://developer.twitter.com) | Yes |
| `TWITTER_ACCESS_TOKEN` / `SECRET` | Twitter Developer Portal | Yes |
| `TWITTER_BEARER_TOKEN` | Twitter Developer Portal | Yes |
| `ANTHROPIC_API_KEY` | [Anthropic Console](https://console.anthropic.com) | Yes |
| `OPENAI_API_KEY` | [OpenAI Platform](https://platform.openai.com) | Optional (news summarization) |
| `COINGECKO_API_KEY` | [CoinGecko](https://www.coingecko.com/en/api) | Optional (raises rate limit) |
| `COINGLASS_API_KEY` | [CoinGlass](https://www.coinglass.com/pricing) | Optional (derivatives/ETF/exchange flows) |
| `WHALE_ALERT_API_KEY` | [Whale Alert](https://whale-alert.io) | Optional (whale transfer alerts) |
| `GIPHY_API_KEY` | [GIPHY Developers](https://developers.giphy.com) | Optional (GIFs on trend alerts) |
| `TELEGRAM_BOT_TOKEN` | [@BotFather](https://t.me/BotFather) | Optional (notifications) |

## Content Schedule (UTC)

| Time | Job | Description |
|------|-----|-------------|
| 08:12 | Morning Brief | Overnight macro + crypto summary |
| 09:05 | Trend Alert | Top trending coin with $CASHTAG |
| 10:00 | Fear & Greed | FNG index with gauge image |
| 12:37 | Hot Take | Polymarket-driven opinion |
| 14:25 | US Open | Pre-market analysis |
| 18:15 | Trend Alert | Afternoon trending coin |
| 22:48 | Evening Wrap | End-of-day summary |
| Every 5m | Price Alert | Event-driven on >5% 1h moves |
| 6x/day | Engagement | Batch likes on KOL tweets |
| 3x/day | Self Reply | Respond to mentions |
| Sunday 20:00 | Weekly Poll | Community poll |

## Testing

```bash
pytest tests/ -v
```

## Deployment

```bash
# Set your VPS connection
export DEPLOY_VPS="user@your-server-ip"
export DEPLOY_PATH="/path/to/bot"

./deploy.sh
```

## Key Design Decisions

- **$CASHTAG in every tweet**: Programmatically enforced — tweets without $CASHTAG are unsearchable on Twitter
- **Tone variation**: Weighted random style selection prevents formulaic AI output
- **Feedback loop**: Top/bottom performing tweets fed back into prompts
- **Graceful degradation**: Optional API keys — missing keys disable features, don't crash the bot
- **Rate limit awareness**: Exponential backoff on AI calls, circuit breaker on Twitter 403s

## License

MIT
