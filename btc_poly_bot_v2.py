
#!/usr/bin/env python3
"""
BTC Polymarket Paper Bot v3
Multi-timeframe signals, order-book pressure, paper-only execution,
Telegram alerts, and prediction tracking for later calibration.

IMPORTANT:
- No live orders are submitted.
- Model probability and edge are UNCALIBRATED heuristic estimates.
- Do not treat them as true odds until enough resolved observations have
  been collected and out-of-sample calibration has been checked.
"""

import argparse
import json
import math
import os
import sqlite3
import time
import traceback
from datetime import datetime, timezone

import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
TELEGRAM = "https://api.telegram.org"

# ----- Risk and scan settings -----
TIMEFRAMES = (5, 15)
SCAN_INTERVAL_SECONDS = 15
PAPER_BANKROLL_USD = 100.00
PAPER_STAKE_USD = 2.00
MAX_DAILY_PAPER_LOSS_USD = 10.00
MAX_DAILY_ENTRIES = 5
MAX_OPEN_PAPER_TRADES = 2

MIN_VOTES = {5: 3, 15: 4}
MIN_BOOK_DEPTH_USD = 1000.00
MAX_SPREAD = 0.04
MIN_SECONDS_REMAINING = 45
MAX_SECONDS_REMAINING_FRACTION = 0.80
MAX_ENTRY_ASK = 0.85
MIN_EDGE_PROXY = 0.06  # heuristic only; not a calibrated probability
HTTP_TIMEOUT = 12
DB_PATH = os.getenv("BTC_BOT_DB", "btc_poly_v2.sqlite3")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "BTC-Polymarket-PaperBot/3.0"})


def now_utc():
    return datetime.now(timezone.utc)


def iso_now():
    return now_utc().isoformat(timespec="seconds")


def get_json(url, params=None):
    response = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.json()


def send_telegram(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("TELEGRAM ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is missing")
        return False
    try:
        response = SESSION.post(
            f"{TELEGRAM}/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": message,
                  "disable_web_page_preview": True},
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        print("Telegram message sent")
        return True
    except Exception as exc:
        print(f"Telegram send failed: {type(exc).__name__}: {exc}")
        return False


def db():
    return sqlite3.connect(DB_PATH)


def init_db():
    with db() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS paper_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_slug TEXT NOT NULL,
                timeframe INTEGER NOT NULL,
                side TEXT NOT NULL,
                token_id TEXT NOT NULL,
                entry_price REAL NOT NULL,
                stake_usd REAL NOT NULL,
                shares REAL NOT NULL,
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                exit_value REAL,
                pnl_usd REAL,
                resolved_at TEXT,
                UNIQUE(market_slug, side)
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS signal_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_slug TEXT NOT NULL,
                timeframe INTEGER NOT NULL,
                side TEXT NOT NULL,
                model_probability REAL NOT NULL,
                market_probability REAL,
                edge_proxy REAL,
                created_at TEXT NOT NULL,
                resolved_side TEXT,
                UNIQUE(market_slug, timeframe, side)
            )
        """)
        con.commit()


def scalar(query, params=()):
    with db() as con:
        row = con.execute(query, params).fetchone()
        return row[0] if row else None


def execute(query, params=()):
    with db() as con:
        con.execute(query, params)
        con.commit()


def ema(values, period):
    if len(values) < period:
        return None
    alpha = 2.0 / (period + 1.0)
    value = sum(values[:period]) / period
    for x in values[period:]:
        value = alpha * x + (1.0 - alpha) * value
    return value


def rsi(values, period=14):
    if len(values) <= period:
        return None
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(x, 0.0) for x in changes[-period:]]
    losses = [max(-x, 0.0) for x in changes[-period:]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - 100.0 / (1.0 + rs)


def fetch_candles():
    # Coinbase candles are [time, low, high, open, close, volume].
    rows = get_json(
        "https://api.exchange.coinbase.com/products/BTC-USD/candles",
        {"granularity": 60},
    )
    if not isinstance(rows, list) or len(rows) < 80:
        raise RuntimeError("Coinbase returned fewer than 80 one-minute candles")
    rows = sorted(rows, key=lambda row: int(row[0]))
    return [{
        "time": int(x[0]),
        "open": float(x[3]),
        "high": float(x[2]),
        "low": float(x[1]),
        "close": float(x[4]),
        "volume": float(x[5]),
    } for x in rows]


def technical_signal(candles, minutes):
    # Different horizons for 5m and 15m; all inputs are closed 1m candles.
    closed = candles[:-1]
    closes = [x["close"] for x in closed]
    volumes = [x["volume"] for x in closed]
    if len(closes) < 60:
        raise RuntimeError("Not enough closed candles for indicators")

    e9, e21, e50 = ema(closes, 9), ema(closes, 21), ema(closes, 50)
    rsi_now = rsi(closes, 14)
    macd_now = ema(closes, 12) - ema(closes, 26)
    macd_prev = ema(closes[:-1], 12) - ema(closes[:-1], 26)

    if minutes == 5:
        fast_n, slow_n = 3, 8
        rsi_up, rsi_down = (51, 69), (31, 49)
        move_threshold = 0.025
    else:
        fast_n, slow_n = 5, 15
        rsi_up, rsi_down = (52, 68), (32, 48)
        move_threshold = 0.040

    fast_move = (closes[-1] / closes[-(fast_n + 1)] - 1) * 100
    slow_move = (closes[-1] / closes[-(slow_n + 1)] - 1) * 100
    avg_vol = sum(volumes[-21:-1]) / 20
    vol_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 0.0

    votes = [
        1 if e9 > e21 > e50 else -1 if e9 < e21 < e50 else 0,
        1 if rsi_now is not None and rsi_up[0] <= rsi_now <= rsi_up[1]
        else -1 if rsi_now is not None and rsi_down[0] <= rsi_now <= rsi_down[1]
        else 0,
        1 if macd_now > macd_prev and macd_now > 0
        else -1 if macd_now < macd_prev and macd_now < 0 else 0,
        1 if fast_move > move_threshold and slow_move > 0
        else -1 if fast_move < -move_threshold and slow_move < 0 else 0,
        1 if vol_ratio >= 1.10 and fast_move > 0
        else -1 if vol_ratio >= 1.10 and fast_move < 0 else 0,
    ]
    score = sum(votes)
    side = "UP" if score > 0 else "DOWN" if score < 0 else "NEUTRAL"

    # Volatility guard: standard deviation of recent 1m returns, in percent.
    returns = [
        (closes[i] / closes[i - 1] - 1) * 100
        for i in range(len(closes) - 20, len(closes))
    ]
    mean_ret = sum(returns) / len(returns)
    volatility = math.sqrt(sum((x - mean_ret) ** 2 for x in returns) / len(returns))

    return {
        "side": side, "score": score, "rsi": rsi_now,
        "fast_move": fast_move, "slow_move": slow_move,
        "volume_ratio": vol_ratio, "volatility": volatility,
    }


def get_market(minutes):
    duration = minutes * 60
    start = int(time.time()) // duration * duration
    slug = f"btc-updown-{minutes}m-{start}"
    event = get_json(f"{GAMMA}/events/slug/{slug}")
    if isinstance(event, list):
        event = event[0] if event else None
    if not event:
        raise RuntimeError(f"Current Polymarket event not found: {slug}")

    markets = event.get("markets") or []
    market = next((m for m in markets if m.get("clobTokenIds")), None)
    if not market:
        raise RuntimeError(f"No CLOB token IDs in event {slug}")

    token_ids = market["clobTokenIds"]
    outcomes = market.get("outcomes", ["Up", "Down"])
    if isinstance(token_ids, str):
        token_ids = json.loads(token_ids)
    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    mapping = {str(o).strip().lower(): token
               for o, token in zip(outcomes, token_ids)}
    if not mapping.get("up") or not mapping.get("down"):
        raise RuntimeError(f"Could not map UP/DOWN tokens for {slug}")

    return {
        "slug": slug, "start": start, "end": start + duration,
        "up_token": mapping["up"], "down_token": mapping["down"],
    }


def order_book(token_id):
    return get_json(f"{CLOB}/book", {"token_id": token_id})


def book_stats(book):
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    if not bids or not asks:
        return None

    bids = sorted(bids, key=lambda x: float(x["price"]), reverse=True)[:10]
    asks = sorted(asks, key=lambda x: float(x["price"]))[:10]
    bid = float(bids[0]["price"])
    ask = float(asks[0]["price"])
    bid_depth = sum(float(x["price"]) * float(x["size"]) for x in bids)
    ask_depth = sum(float(x["price"]) * float(x["size"]) for x in asks)
    total_depth = bid_depth + ask_depth
    imbalance = ((bid_depth - ask_depth) / total_depth) if total_depth else 0.0
    return {
        "bid": bid, "ask": ask, "mid": (bid + ask) / 2,
        "spread": ask - bid, "bid_depth_usd": bid_depth,
        "ask_depth_usd": ask_depth, "imbalance": imbalance,
    }


def model_probability(tech, book, side):
    # This is a transparent, UNCALIBRATED score. It is not a true probability.
    directional_score = abs(tech["score"]) / 5.0
    pressure = book["imbalance"] if side == "UP" else -book["imbalance"]
    volatility_penalty = min(0.08, tech["volatility"] * 0.04)
    raw = 0.50 + 0.20 * directional_score + 0.08 * pressure - volatility_penalty
    return max(0.50, min(0.78, raw))


def daily_stats():
    today = now_utc().date().isoformat()
    with db() as con:
        rows = con.execute(
            "SELECT status,pnl_usd,opened_at FROM paper_trades"
        ).fetchall()
    todays = [r for r in rows if (r[2] or "").startswith(today)]
    entries = len(todays)
    realized_losses = sum(
        float(r[1] or 0) for r in todays
        if r[0] == "RESOLVED" and float(r[1] or 0) < 0
    )
    open_count = scalar(
        "SELECT COUNT(*) FROM paper_trades WHERE status='OPEN'"
    ) or 0
    return entries, realized_losses, open_count


def create_paper_trade(market, minutes, side, token_id, ask):
    entries, losses, open_count = daily_stats()
    if entries >= MAX_DAILY_ENTRIES:
        return "BLOCKED: daily entry limit reached"
    if losses <= -MAX_DAILY_PAPER_LOSS_USD:
        return "BLOCKED: daily loss limit reached"
    if open_count >= MAX_OPEN_PAPER_TRADES:
        return "BLOCKED: maximum open paper trades reached"
    if scalar("SELECT COUNT(*) FROM paper_trades WHERE market_slug=?",
              (market["slug"],)):
        return "BLOCKED: already traded this market"

    fill = min(0.99, ask + 0.005)
    shares = PAPER_STAKE_USD / fill
    execute("""
        INSERT OR IGNORE INTO paper_trades
        (market_slug,timeframe,side,token_id,entry_price,stake_usd,shares,opened_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, (market["slug"], minutes, side, token_id, fill,
          PAPER_STAKE_USD, shares, iso_now()))
    return f"PAPER ENTRY RECORDED at ${fill:.3f}"


def record_observation(market, minutes, side, model_p, market_p, edge):
    execute("""
        INSERT OR IGNORE INTO signal_observations
        (market_slug,timeframe,side,model_probability,market_probability,edge_proxy,created_at)
        VALUES (?,?,?,?,?,?,?)
    """, (market, minutes, side, model_p, market_p, edge, iso_now()))


def resolve_observations():
    # Resolve historical signal observations so calibration can be evaluated later.
    with db() as con:
        rows = con.execute("""
            SELECT DISTINCT market_slug FROM signal_observations
            WHERE resolved_side IS NULL
        """).fetchall()

    for (slug,) in rows:
        try:
            event = get_json(f"{GAMMA}/events/slug/{slug}")
            if isinstance(event, list):
                event = event[0] if event else None
            if not event:
                continue
            markets = event.get("markets") or []
            market = markets[0] if markets else None
            if not market or not market.get("closed"):
                continue
            outcomes = market.get("outcomes", ["Up", "Down"])
            prices = market.get("outcomePrices")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(prices, str):
                prices = json.loads(prices)
            if not prices or len(prices) != len(outcomes):
                continue
            price_map = {str(o).lower(): float(p)
                         for o, p in zip(outcomes, prices)}
            winner = "UP" if price_map.get("up", 0) >= 0.99 else (
                "DOWN" if price_map.get("down", 0) >= 0.99 else None
            )
            if winner:
                execute(
                    "UPDATE signal_observations SET resolved_side=? WHERE market_slug=?",
                    (winner, slug),
                )
        except Exception as exc:
            print(f"Observation resolution error for {slug}: {exc}")


def calibration_summary():
    with db() as con:
        rows = con.execute("""
            SELECT model_probability,
                   CASE WHEN side=resolved_side THEN 1.0 ELSE 0.0 END
            FROM signal_observations
            WHERE resolved_side IS NOT NULL
        """).fetchall()
    if not rows:
        return "Calibration: collecting resolved examples; no score yet"
    brier = sum((p - outcome) ** 2 for p, outcome in rows) / len(rows)
    return (
        f"Resolved signal samples: {len(rows)} | "
        f"Brier score: {brier:.3f} (lower is better; "
        "small samples are not reliable)"
    )


def scan_timeframe(minutes, candles):
    market = get_market(minutes)
    remaining = max(0, market["end"] - int(time.time()))
    tech = technical_signal(candles, minutes)

    up_book = book_stats(order_book(market["up_token"]))
    down_book = book_stats(order_book(market["down_token"]))
    side = tech["side"]
    chosen = up_book if side == "UP" else down_book if side == "DOWN" else None
    token = market["up_token"] if side == "UP" else (
        market["down_token"] if side == "DOWN" else None
    )
    reasons = []

    if chosen is None:
        reasons.append("No directional agreement")
        model_p = market_p = edge = None
    else:
        model_p = model_probability(tech, chosen, side)
        market_p = chosen["mid"]
        edge = model_p - market_p

        if abs(tech["score"]) < MIN_VOTES[minutes]:
            reasons.append(f"Need {MIN_VOTES[minutes]}/5 directional votes")
        if chosen["ask_depth_usd"] < MIN_BOOK_DEPTH_USD:
            reasons.append("Insufficient ask-side liquidity")
        if chosen["spread"] > MAX_SPREAD:
            reasons.append("Spread too wide")
        if chosen["ask"] > MAX_ENTRY_ASK:
            reasons.append("Entry ask too expensive")
        if remaining < MIN_SECONDS_REMAINING:
            reasons.append("Too close to resolution")
        if remaining > minutes * 60 * MAX_SECONDS_REMAINING_FRACTION:
            reasons.append("Market too early")
        if edge < MIN_EDGE_PROXY:
            reasons.append("Heuristic edge proxy below threshold")

        record_observation(
            market["slug"], minutes, side, model_p, market_p, edge
        )

    paper_result = "NO ENTRY"
    if chosen and not reasons:
        paper_result = create_paper_trade(
            market, minutes, side, token, chosen["ask"]
        )

    return {
        "minutes": minutes, "market": market, "remaining": remaining,
        "tech": tech, "side": side, "book": chosen,
        "model_p": model_p, "market_p": market_p, "edge": edge,
        "reasons": reasons, "paper_result": paper_result,
    }


def format_report(result):
    t = result["tech"]
    lines = [
        f"BTC {result['minutes']}M | {result['market']['slug']}",
        f"Signal: {result['side']} | votes {t['score']}/5",
        f"RSI: {t['rsi']:.1f} | fast move: {t['fast_move']:+.3f}%",
        f"Slow move: {t['slow_move']:+.3f}% | volume: {t['volume_ratio']:.2f}x",
        f"1m volatility: {t['volatility']:.4f}%",
        f"Time remaining: {result['remaining']}s",
    ]
    if result["book"]:
        b = result["book"]
        lines.extend([
            f"Bid/ask: ${b['bid']:.3f}/${b['ask']:.3f} | spread: {b['spread']:.3f}",
            f"Ask depth: ${b['ask_depth_usd']:.0f} | book imbalance: {b['imbalance']:+.2f}",
            f"Raw model estimate: {result['model_p'] * 100:.1f}% (UNCALIBRATED)",
            f"Market midpoint: {result['market_p'] * 100:.1f}%",
            f"Raw edge proxy: {result['edge'] * 100:+.1f} percentage points",
        ])
    lines.append(f"Paper action: {result['paper_result']}")
    if result["reasons"]:
        lines.append("Entry blocked: " + "; ".join(result["reasons"]))
    lines.append("PAPER ONLY â no real orders")
    return "\n".join(lines)


def notify_signal(result, message):
    side = result["side"]
    if side not in ("UP", "DOWN"):
        print(f"BTC {result['minutes']}m neutral; no directional alert")
        return

    key = f"alert:{result['minutes']}:{result['market']['slug']}:{side}"
    if scalar("SELECT value FROM bot_state WHERE key=?", (key,)):
        print(f"Already alerted: {key}")
        return

    emoji = "ð¢" if side == "UP" else "ð´"
    if send_telegram(f"{emoji} BTC {result['minutes']}M {side} SIGNAL\n\n{message}"):
        execute("INSERT OR REPLACE INTO bot_state(key,value) VALUES (?,?)",
                (key, iso_now()))

    if result["paper_result"].startswith("PAPER ENTRY RECORDED"):
        send_telegram("ð§¾ " + result["paper_result"] + "\n" + message)


def resolve_paper_trades():
    with db() as con:
        trades = con.execute("""
            SELECT id,market_slug,side,stake_usd,shares
            FROM paper_trades WHERE status='OPEN'
        """).fetchall()

    for trade_id, slug, side, stake, shares in trades:
        try:
            event = get_json(f"{GAMMA}/events/slug/{slug}")
            if isinstance(event, list):
                event = event[0] if event else None
            if not event:
                continue
            markets = event.get("markets") or []
            market = markets[0] if markets else None
            if not market or not market.get("closed"):
                continue
            outcomes = market.get("outcomes", ["Up", "Down"])
            prices = market.get("outcomePrices")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(prices, str):
                prices = json.loads(prices)
            if not prices or len(prices) != len(outcomes):
                continue
            values = {str(o).lower(): float(p)
                      for o, p in zip(outcomes, prices)}
            winner = "UP" if values.get("up", 0) >= 0.99 else (
                "DOWN" if values.get("down", 0) >= 0.99 else None
            )
            if not winner:
                continue
            payout = shares if side == winner else 0.0
            pnl = payout - stake
            execute("""
                UPDATE paper_trades SET status='RESOLVED',
                exit_value=?,pnl_usd=?,resolved_at=?
                WHERE id=? AND status='OPEN'
            """, (payout, pnl, iso_now(), trade_id))
            send_telegram(
                f"ð PAPER TRADE RESOLVED\n{slug}\n"
                f"Position: {side} | Result: {winner}\n"
                f"Stake: ${stake:.2f} | P/L: ${pnl:+.2f}"
            )
        except Exception as exc:
            print(f"Paper resolution error for {slug}: {exc}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--duration", type=int, default=0,
                        help="Run repeated scans for this many seconds")
    args = parser.parse_args()

    init_db()
    print("BTC PAPER BOT v3 started; live trading is disabled.")
    print(f"Timeframes: {TIMEFRAMES}; stake: ${PAPER_STAKE_USD:.2f}")

    # Startup is printed to the Actions log only.
    # Do not send a Telegram startup message on every scheduled run.

    started = time.time()
    while True:
        try:
            resolve_paper_trades()
            resolve_observations()
            candles = fetch_candles()

            for minutes in TIMEFRAMES:
                try:
                    result = scan_timeframe(minutes, candles)
                    message = format_report(result)
                    print("\n" + "-" * 60 + "\n" + message)
                    notify_signal(result, message)
                except Exception as exc:
                    error = (
                        f"BTC {minutes}M SCAN ERROR\n"
                        f"{type(exc).__name__}: {exc}\n"
                        "No real order was placed."
                    )
                    print(error)
                    send_telegram(error)

            print(calibration_summary())

        except Exception as exc:
            traceback.print_exc()
            send_telegram(
                f"â ï¸ BTC BOT ERROR: {type(exc).__name__}: {exc}"
            )

        if args.once:
            break
        if args.duration and time.time() - started >= args.duration:
            break
        time.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

