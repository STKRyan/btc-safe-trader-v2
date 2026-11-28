# Unified Kalshi Gold / ETH / BTC Bot

This is a separate paper-trading bot. It does not modify or reuse the running entrypoints for BTC Live, Crypto Radar, or the Polymarket multi-asset scanner.

## Scope

- Kalshi markets for BTC, ETH, and gold, discovered from verified market metadata and settlement rules.
- One active state/card per market contract and signal; edit/update the existing Discord message where supported, otherwise send only on state transitions or throttled meaningful updates.
- Fast market-data polling with bounded concurrency, connection reuse, API rate-limit handling, and configurable polling intervals.
- Signal types: SCALP UP/DOWN, SNIPE UP/DOWN, HOLD UP/DOWN, WAIT, SELL/EXIT, and FLIP/REASSESS.
- Independent Second Brain veto/check, entry quality, hold strength, flip risk, contract-price/spread/liquidity checks, and cross-market ranking.
- Paper ledger with append-only initial calls, managed entries/exits, flips, settlement, and audit trail.
- Statistics overall and by asset, contract, trade type, timeframe, and initial/managed outcome; current/best streaks.
- Persistent SQLite state on a dedicated Render disk.

## Result accounting (separate, never blended)

1. **Initial-call settlement result:** WIN or LOSS based on the original direction and official settlement. A changed direction does not erase the original-call result.
2. **Managed paper result:** realized paper P&L from simulated entry and exit prices, including flip saves and losses.
3. **Streak policy:** streaks are computed from explicitly selected result streams (initial-call and managed-trade). A flip save may extend the managed streak but cannot turn a wrong initial call into a correct initial call.
4. **No artificial loss suppression:** if the original call is wrong at settlement, record it as an initial-call loss. If a trade exits before settlement, record its managed outcome and still settle the initial call later.

## Safety and honest reporting

- Paper-only: no order placement or private trading credentials.
- Do not fabricate confidence or claim a guaranteed win rate. Show heuristic strength separately from empirical, sample-qualified historical success.
- If Second Brain disagrees, spread/price/liquidity is poor, flip risk is high, market data is stale, or settlement rules cannot be verified: WAIT / NO ENTRY.
- Do not chase a streak or force an entry. A streak objective is subordinate to capital preservation and valid data.
- Self-correction means updating the active paper signal/exit plan when conditions change, then learning from outcomes; it does not rewrite past calls or settled records.

## Build and deployment sequence

1. Verify current official Kalshi market-data endpoints and contract/settlement fields.
2. Implement isolated modules for market discovery, data feed, signal engine, Second Brain, paper ledger, message updater, and stats.
3. Add deterministic unit tests for contract mapping, flip/settlement accounting, duplicate-message suppression, and streak calculations.
4. Add a separate Render worker and separate persistent disk only after local validation.
5. Deploy paper-only, verify logs and message updates, then observe enough settled samples before enabling any empirical performance gate.

The bot is not ready for deployment until tests and live paper runtime checks pass.
