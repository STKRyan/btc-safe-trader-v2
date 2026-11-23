
#!/usr/bin/env python3
"""
BTC Polymarket Paper Trading Bot
Telegram UP/DOWN alerts
PAPER TRADING ONLY - NO LIVE ORDERS
"""

import argparse
import json
import os
import sqlite3
import time
import traceback
from datetime import datetime, timezone

import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
TELEGRAM = "https://api.telegram.org"

# -------- SETTINGS --------

SCAN_INTERVAL_SECONDS = 15
TIMEFRAMES = (5, 15)

PAPER_BANKROLL_USD = 100.00
PAPER_STAKE_USD = 2.00
MAX_DAILY_PAPER_LOSS_USD = 10.00
MAX_DAILY_ENTRIES = 5
MAX_OPEN_PAPER_TRADES = 2

MIN_DIRECTIONAL_VOTES_5M = 3
MIN_DIRECTIONAL_VOTES_15M = 4

MIN_BOOK_DEPTH_USD = 1000.00
MAX_SPREAD = 0.04
MIN_SECONDS_REMAINING = 45
MAX_SECONDS_REMAINING_FRACTION = 0.80
MAX_ENTRY_ASK = 0.85
MIN_MODEL_EDGE = 0.05

COOLDOWN_SECONDS = 60
DB_PATH = os.getenv("BTC_BOT_DB", "btc_poly_v2.sqlite3")
HTTP_TIMEOUT = 12

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "BTC-Polymarket-PaperBot/3.0"
})


def utc_now():
    return datetime.now(timezone.utc)


def iso_now():
    return utc_now().isoformat(timespec="seconds")


def get_json(url, params=None):
    response = SESSION.get(
        url, params=params, timeout=HTTP_TIMEOUT
    )
    response.raise_for_status()
    return response.json()


def send_telegram(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        print("ERROR: Telegram credentials missing")
        return False

    try:
        response = SESSION.post(
            f"{TELEGRAM}/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": message,
                "disable_web_page_preview": True
            },
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        print("Telegram alert sent successfully")
        return True
    except Exception as exc:
        print(f"Telegram error: {exc}")
        return False


def init_db():
    with sqlite3.connect(DB_PATH) as con:
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
        con.commit()


def db_scalar(query, params=()):
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute(query, params).fetchone()
        return row[0] if row else None


def db_execute(query, params=()):
    with sqlite3.connect(DB_PATH) as con:
        con.execute(query, params)
        con.commit()


def ema(values, period):
    if len(values) < period:
        return None

    alpha = 2.0 / (period + 1.0)
    result = sum(values[:period]) / period

    for value in values[period:]:
        result = alpha * value + (1 - alpha) * result

    return result


def rsi(values, period=14):
    if len(values) <= period:
        return None

    changes = [
        values[i] - values[i - 1]
        for i in range(1, len(values))
    ]

    gains = [max(x, 0) for x in changes[-period:]]
    losses = [max(-x, 0) for x in changes[-period:]]

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def fetch_candles():
    rows = get_json(
        "https://api.exchange.coinbase.com/products/BTC-USD/candles",
        {"granularity": 60}
    )

    if not isinstance(rows, list) or len(rows) < 60:
        raise RuntimeError("Insufficient Coinbase candles")

    rows = sorted(rows, key=lambda x: int(x[0]))

    return [{
        "open_time": int(x[0]) * 1000,
        "open": float(x[3]),
        "high": float(x[2]),
        "low": float(x[1]),
        "close": float(x[4]),
        "volume": float(x[5]),
    } for x in rows]


def technical_signal(candles):
    closed = candles[:-1]

    closes = [x["close"] for x in closed]
    volumes = [x["volume"] for x in closed]

    if len(closes) < 60:
        raise RuntimeError("Not enough closed candles")

    e9 = ema(closes, 9)
    e21 = ema(closes, 21)
    e50 = ema(closes, 50)

    current_rsi = rsi(closes)

    macd_now = ema(closes, 12) - ema(closes, 26)
    macd_prev = (
        ema(closes[:-1], 12) -
        ema(closes[:-1], 26)
    )

    move_3m = (closes[-1] / closes[-4] - 1) * 100
    move_10m = (closes[-1] / closes[-11] - 1) * 100

    avg_vol = sum(volumes[-21:-1]) / 20
    vol_ratio = volumes[-1] / avg_vol if avg_vol > 0 else 0

    votes = [
        1 if e9 > e21 > e50 else
        -1 if e9 < e21 < e50 else 0,

        1 if current_rsi is not None and 52 <= current_rsi <= 68 else
        -1 if current_rsi is not None and 32 <= current_rsi <= 48 else 0,

        1 if macd_now > macd_prev and macd_now > 0 else
        -1 if macd_now < macd_prev and macd_now < 0 else 0,

        1 if move_3m > 0.035 and move_10m > 0 else
        -1 if move_3m < -0.035 and move_10m < 0 else 0,

        1 if vol_ratio >= 1.10 and move_3m > 0 else
        -1 if vol_ratio >= 1.10 and move_3m < 0 else 0,
    ]

    score = sum(votes)

    direction = (
        "UP" if score > 0 else
        "DOWN" if score < 0 else
        "NEUTRAL"
    )

    confidence = min(0.75, 0.50 + 0.05 * abs(score))

    return {
        "direction": direction,
        "score": score,
        "rsi": current_rsi,
        "move_3m_pct": move_3m,
        "move_10m_pct": move_10m,
        "volume_ratio": vol_ratio,
        "confidence_proxy": confidence,
    }


def get_market(minutes):
    duration = minutes * 60
    start = int(time.time()) // duration * duration

    slug = f"btc-updown-{minutes}m-{start}"

    event = get_json(f"{GAMMA}/events/slug/{slug}")

    if isinstance(event, list):
        event = event[0] if event else None

    if not event:
        raise RuntimeError(f"Market not found: {slug}")

    markets = event.get("markets") or []

    market = next(
        (m for m in markets if m.get("clobTokenIds")),
        None
    )

    if not market:
        raise RuntimeError("No market token IDs found")

    tokens = market["clobTokenIds"]
    outcomes = market.get("outcomes", ["Up", "Down"])

    if isinstance(tokens, str):
        tokens = json.loads(tokens)

    if isinstance(outcomes, str):
        outcomes = json.loads(outcomes)

    mapping = {
        str(o).strip().lower(): t
        for o, t in zip(outcomes, tokens)
    }

    up_id = mapping.get("up")
    down_id = mapping.get("down")

    if not up_id or not down_id:
        raise RuntimeError("Could not identify UP/DOWN tokens")

    return {
        "slug": slug,
        "start": start,
        "end": start + duration,
        "question": market.get("question") or slug,
        "up_token": up_id,
        "down_token": down_id,
    }


def order_book(token_id):
    return get_json(
        f"{CLOB}/book",
        {"token_id": token_id}
    )


def book_stats(book):
    bids = book.get("bids") or []
    asks = book.get("asks") or []

    if not bids or not asks:
        return None

    best_bid = max(float(x["price"]) for x in bids)
    best_ask = min(float(x["price"]) for x in asks)

    levels = sorted(
        asks, key=lambda x: float(x["price"])
    )[:10]

    depth = sum(
        float(x["price"]) * float(x["size"])
        for x in levels
    )

    return {
        "bid": best_bid,
        "ask": best_ask,
        "spread": best_ask - best_bid,
        "ask_depth_usd": depth,
    }


def daily_stats():
    today = utc_now().date().isoformat()

    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute(
            "SELECT status,pnl_usd,opened_at FROM paper_trades"
        ).fetchall()

    today_rows = [
        r for r in rows
        if (r[2] or "").startswith(today)
    ]

    entries = len(today_rows)

    losses = sum(
        float(r[1] or 0)
        for r in today_rows
        if r[0] == "RESOLVED" and (r[1] or 0) < 0
    )

    open_count = db_scalar(
        "SELECT COUNT(*) FROM paper_trades WHERE status='OPEN'"
    ) or 0

    return entries, losses, open_count


def create_paper_trade(market, minutes, side, token_id, ask):
    entries, losses, open_count = daily_stats()

    if entries >= MAX_DAILY_ENTRIES:
        return "daily entry limit reached"

    if losses <= -MAX_DAILY_PAPER_LOSS_USD:
        return "daily paper loss limit reached"

    if open_count >= MAX_OPEN_PAPER_TRADES:
        return "maximum open paper trades reached"

    if db_scalar(
        "SELECT COUNT(*) FROM paper_trades WHERE market_slug=?",
        (market["slug"],)
    ):
        return "already traded this market"

    simulated_price = min(0.99, ask + 0.005)
    shares = PAPER_STAKE_USD / simulated_price

    db_execute("""
        INSERT OR IGNORE INTO paper_trades
        (market_slug,timeframe,side,token_id,entry_price,
         stake_usd,shares,opened_at)
        VALUES (?,?,?,?,?,?,?,?)
    """, (
        market["slug"], minutes, side, token_id,
        simulated_price, PAPER_STAKE_USD,
        shares, iso_now()
    ))

    return f"paper entry created at ${simulated_price:.3f}"


def scan_timeframe(minutes, candles, tech):
    market = get_market(minutes)

    remaining = max(
        0, market["end"] - int(time.time())
    )

    up = book_stats(order_book(market["up_token"]))
    down = book_stats(order_book(market["down_token"]))

    side = tech["direction"]

    chosen = (
        up if side == "UP" else
        down if side == "DOWN" else None
    )

    token = (
        market["up_token"] if side == "UP" else
        market["down_token"] if side == "DOWN" else None
    )

    reasons = []

    required_votes = (
        MIN_DIRECTIONAL_VOTES_5M
        if minutes == 5
        else MIN_DIRECTIONAL_VOTES_15M
    )

    if chosen is None:
        reasons.append("No directional signal")
    else:
        if abs(tech["score"]) < required_votes:
            reasons.append(
                f"Votes below required {required_votes}/5"
            )

        if chosen["ask_depth_usd"] < MIN_BOOK_DEPTH_USD:
            reasons.append("Order book too thin")

        if chosen["spread"] > MAX_SPREAD:
            reasons.append("Spread too wide")

        if chosen["ask"] > MAX_ENTRY_ASK:
            reasons.append("Entry price too expensive")

        if remaining < MIN_SECONDS_REMAINING:
            reasons.append("Too close to market resolution")

        if remaining > minutes * 60 * MAX_SECONDS_REMAINING_FRACTION:
            reasons.append("Market too early")

    edge_proxy = (
        tech["confidence_proxy"] - chosen["ask"]
        if chosen else None
    )

    if chosen and edge_proxy < MIN_MODEL_EDGE:
        reasons.append("Heuristic edge below minimum")

    result = {
        "minutes": minutes,
        "market": market,
        "remaining": remaining,
        "up": up,
        "down": down,
        "tech": tech,
        "side": side,
        "chosen": chosen,
        "token": token,
        "edge_proxy": edge_proxy,
        "reasons": reasons,
    }

    if chosen and not reasons:
        result["paper_result"] = create_paper_trade(
            market, minutes, side, token, chosen["ask"]
        )
    else:
        result["paper_result"] = "no entry"

    return result


def report(result):
    tech = result["tech"]
    market = result["market"]

    lines = [
        f"BTC {result['minutes']}M MARKET",
        f"Direction: {result['side']}",
        f"Votes: {tech['score']}/5",
        f"RSI: {tech['rsi']:.1f}",
        f"3m move: {tech['move_3m_pct']:+.3f}%",
        f"10m move: {tech['move_10m_pct']:+.3f}%",
        f"Volume: {tech['volume_ratio']:.2f}x",
        f"Time remaining: {result['remaining']} seconds",
        f"Market: {market['slug']}",
    ]

    if result["chosen"]:
        book = result["chosen"]

        lines.append(
            f"Ask: ${book['ask']:.3f} | "
            f"Spread: {book['spread']:.3f}"
        )

        lines.append(
            f"Depth: ${book['ask_depth_usd']:.0f}"
        )

        lines.append(
            f"Edge proxy: {result['edge_proxy'] * 100:+.1f}pp "
            "(uncalibrated)"
        )

    lines.append(
        f"Paper action: {result['paper_result']}"
    )

    if result["reasons"]:
        lines.append(
            "Blocked by: " + "; ".join(result["reasons"])
        )
    else:
        lines.append("Entry checks passed")

    return "\n".join(lines)


def notify_signal(result, message):
    side = result["side"]

    if side not in ("UP", "DOWN"):
        print("Neutral signal - no directional alert")
        return

    market = result["market"]["slug"]
    minutes = result["minutes"]

    alert_key = f"signal:{minutes}:{market}:{side}"

    already_sent = db_scalar(
        "SELECT value FROM bot_state WHERE key=?",
        (alert_key,)
    )

    if already_sent:
        print(f"Duplicate alert skipped: {alert_key}")
        return

    emoji = "🟢" if side == "UP" else "🔴"

    alert = (
        f"{emoji} BTC {minutes}M {side} SIGNAL\n\n"
        f"{message}\n\n"
        "PAPER TRADING ONLY"
    )

    if send_telegram(alert):
        db_execute(
            "INSERT OR REPLACE INTO bot_state(key,value) VALUES (?,?)",
            (alert_key, iso_now())
        )


def resolve_open_trades():
    with sqlite3.connect(DB_PATH) as con:
        rows = con.execute("""
            SELECT id,market_slug,side,entry_price,stake_usd,shares
            FROM paper_trades WHERE status='OPEN'
        """).fetchall()

    for trade in rows:
        trade_id, slug, side, entry, stake, shares = trade

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

            price_map = {
                str(o).lower(): float(p)
                for o, p in zip(outcomes, prices)
            }

            winning_side = None

            if price_map.get("up", 0) >= 0.99:
                winning_side = "UP"
            elif price_map.get("down", 0) >= 0.99:
                winning_side = "DOWN"

            if not winning_side:
                continue

            payout = shares if side == winning_side else 0
            pnl = payout - stake

            db_execute("""
                UPDATE paper_trades
                SET status='RESOLVED',exit_value=?,
                    pnl_usd=?,resolved_at=?
                WHERE id=? AND status='OPEN'
            """, (payout, pnl, iso_now(), trade_id))

            send_telegram(
                f"📊 PAPER TRADE RESOLVED\n"
                f"Market: {slug}\n"
                f"Position: {side}\n"
                f"Result: {winning_side}\n"
                f"Stake: ${stake:.2f}\n"
                f"P/L: ${pnl:+.2f}"
            )

        except Exception as exc:
            print(f"Resolution error: {exc}")


def performance_summary():
    with sqlite3.connect(DB_PATH) as con:
        row = con.execute("""
            SELECT COUNT(*),
                SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END),
                SUM(COALESCE(pnl_usd,0)),
                SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END)
            FROM paper_trades
        """).fetchone()

    total, wins, losses, pnl, opened = row

    wins = wins or 0
    losses = losses or 0
    resolved = wins + losses

    return {
        "total": total or 0,
        "resolved": resolved,
        "wins": wins,
        "losses": losses,
        "pnl": pnl or 0,
        "open": opened or 0,
        "win_rate": wins / resolved * 100 if resolved else 0,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    init_db()

    print("BTC PAPER BOT STARTED")
    print("Real-money trading: DISABLED")
    print(f"Timeframes: {TIMEFRAMES}")
    print(f"Paper stake: ${PAPER_STAKE_USD:.2f}")

    while True:
        try:
            resolve_open_trades()

            candles = fetch_candles()
            tech = technical_signal(candles)

            for minutes in TIMEFRAMES:
                try:
                    result = scan_timeframe(
                        minutes, candles, tech
                    )

                    message = report(result)

                    print("\n" + "=" * 50)
                    print(message)

                    # Send UP/DOWN even when entry is blocked.
                    notify_signal(result, message)

                except Exception as exc:
                    error = (
                        f"BTC {minutes}M SCAN ERROR\n"
                        f"{type(exc).__name__}: {exc}\n"
                        "No trade was placed."
                    )

                    print(error)
                    send_telegram(error)

            stats = performance_summary()

            print(
                f"PAPER STATS | "
                f"Resolved: {stats['resolved']} | "
                f"Wins: {stats['wins']} | "
                f"Losses: {stats['losses']} | "
                f"Win rate: {stats['win_rate']:.1f}% | "
                f"P/L: ${stats['pnl']:+.2f} | "
                f"Open: {stats['open']}"
            )

        except Exception as exc:
            print(f"MAIN LOOP ERROR: {exc}")
            traceback.print_exc()
            send_telegram(
                f"⚠️ BTC BOT ERROR\n{type(exc).__name__}: {exc}"
            )

        if args.once:
            break

        time.sleep(SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()

