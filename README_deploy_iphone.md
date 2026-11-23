# BTC Safe Trader V2 — iPhone-friendly cloud deployment

This bundle runs the bot as a Render background worker. It performs automatic scanning,
Telegram alerts, and PAPER trades only. It does not place real-money orders.

## Before you deploy
- Keep your Telegram bot token private. Never commit it to GitHub.
- In Telegram, open your bot and press Start.
- Get your numeric chat ID from a trusted Telegram ID bot, or from your own bot's
  getUpdates response after sending it a message.
- Render background workers require paid compute. The Starter worker is currently
  listed from $7/month; a 1 GB persistent disk adds storage cost. Check current pricing
  before creating the service.
- Do not enable live trading. This version has no live-order execution.

## Deploy from an iPhone
1. Sign in to GitHub in Safari and create a PRIVATE repository named `btc-safe-trader-v2`.
2. Upload these four files from this bundle's extracted folder:
   - `btc_poly_bot_v2.py`
   - `requirements.txt`
   - `render.yaml`
   - `README.md`
3. Sign in to Render in Safari and choose New > Blueprint.
4. Connect GitHub and select your private `btc-safe-trader-v2` repository.
5. Review the Blueprint. It should create a Background Worker named `btc-safe-trader-v2`
   with a persistent disk mounted at `/var/data`.
6. When prompted, enter `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` as secret environment
   values. Do not put either secret in GitHub files or screenshots.
7. Create/apply the Blueprint. Check the worker's Logs for the startup message.
8. Open your Telegram bot chat and confirm you receive the startup message.

## Important limitations
- Free Render services do not include background workers. This worker uses paid compute.
- The local SQLite database is stored on the attached persistent disk.
- The strategy's confidence score is an uncalibrated heuristic, not a true win probability.
  Run paper trading and review resolved outcomes before considering any real-money system.
- Binance BTCUSDT is only a momentum proxy; Polymarket resolution uses its stated
  settlement source. Check each market's rules.
