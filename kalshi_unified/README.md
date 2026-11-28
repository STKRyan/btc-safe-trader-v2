# Unified Kalshi BTC / ETH / Gold paper bot

This bot is isolated on branch `kalshi-gold-eth-btc-unified`. It does not edit, import, or deploy the BTC Live bot, Crypto Radar, or the existing Polymarket bot.

## Current implementation

- Reads public Kalshi market metadata and binary quotes from the official Trade API host.
- Requires an unambiguous BTC, ETH, or Gold title, explicit above/below threshold language, primary settlement rules, ticker, and close metadata. Ambiguous contracts are skipped.
- Uses YES/NO contract-price momentum, executable bid/ask, spread, price-band, stale-data and independent confirmation gates. **UP/DOWN on the card refers to the Kalshi contract's traded price/YES-vs-NO side, not an independently verified spot-price forecast for BTC/ETH/gold.**
- Paper-only entries, scalp/snipe targets, stops, max hold, signal flip exits, and official settlement reconciliation. No authenticated account, order placement, or real trading endpoint exists in this bot.
- SQLite ledger separates the first initial call's official settlement result from every managed paper trade. A later flip cannot rewrite the original call.
- Append-only managed trade rows; per-market Discord cards are edited in place where possible. Cards for open paper positions continue updating even when a new signal becomes WAIT.
- Overall settled initial-call win/loss counts and streak metrics. Heuristic strength/flip-risk values are not win probabilities.

## Safety / limitations

- Paper results are simulations and do not include all fees, queue position, partial fills, latency or market impact. Treat them as optimistic research estimates.
- The “Second Brain” currently means a separate deterministic confirmation gate (momentum threshold plus quote/spread check), **not** a separate AI model or independent external prediction.
- This initial strategy is a contract-price momentum prototype. It does not yet validate underlying spot-price feeds or produce evidence-based 60% predictive accuracy. It must not be described as proven or high-confidence.
- Market discovery is intentionally conservative and may miss some listings. Verify each market's official rules before relying on its signal.
- Do not add real trading credentials or enable live orders.

## Isolated validation

GitHub Actions runs syntax checks and unit tests for this branch. After CI is green, run locally or in a new, separate Render worker:

```bash
pip install -r requirements-kalshi.txt
python -m unittest discover -s tests -p 'test_kalshi_unified.py'
python kalshi_unified_bot.py
```

Set `KALSHI_UNIFIED_DISCORD_WEBHOOK_URL` only to a **new dedicated Discord webhook**. Optional:
- `KALSHI_UNIFIED_DB`: SQLite path (default `kalshi_unified.sqlite3`)
- `KALSHI_SCAN_SECONDS`: scan interval (default 5 seconds)
- `KALSHI_CARD_UPDATE_SECONDS`: minimum same-state card update interval (default 30 seconds)
- `KALSHI_MAX_CARDS`: maximum qualified cards (default 4)

For Render, create a completely new Background Worker with start command `python kalshi_unified_bot.py` and a separate persistent disk mounted where the SQLite file lives. Do not modify existing Render services, environment variables, or the existing `render.yaml`. Deployment is not performed by this branch.
