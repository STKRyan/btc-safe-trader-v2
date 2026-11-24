#!/usr/bin/env python3
"""Polymarket BTC conservative paper scanner v2.

Paper trading only. This program never submits orders to the Polymarket CLOB.
It reads public market data, sends Telegram alerts, and records simulated trades
in SQLite. Model scores and hold/flip gauges are heuristic, not calibrated odds.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
COINBASE = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
TIMEFRAMES = (5, 15)
MIN_VOTES = {5: 3, 15: 4}  # 5m is more flexible; 15m stays strict.
SCAN_INTERVAL_SECONDS = 15
PAPER_BANKROLL_USD = 100.00
PAPER_STAKE_USD = 2.00
MAX_DAILY_PAPER_LOSS_USD = 10.00
MAX_DAILY_ENTRIES = 5
MAX_OPEN_PAPER_TRADES = 2
MIN_BOOK_DEPTH_USD = 1000.00
MAX_SPREAD = 0.04
MIN_SECONDS_REMAINING = 45
MAX_SECONDS_REMAINING_FRACTION = 0.80
MAX_ENTRY_ASK = 0.85
MIN_EDGE_PROXY = 0.05
COOLDOWN_SECONDS = 60
HTTP_TIMEOUT = 12
DB_PATH = os.getenv("BTC_BOT_DB", "btc_poly_v2.sqlite3")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "BTC-Conservative-Paper-Bot/2.0"})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def telegram(message: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("Telegram credentials missing; message not sent:", message[:200])
        return
    try:
        response = SESSION.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": message, "disable_web_page_preview": True},
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        if not payload.get("ok"):
            print("Telegram API returned not-ok:", payload)
    except Exception as exc:
        print(f"Telegram send failed: {exc}")


def http_json(url: str, params: Optional[dict] = None) -> Any:
    response = SESSION.get(url, params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    return response.json()


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_slug TEXT NOT NULL,
            timeframe INTEGER NOT NULL,
            side TEXT NOT NULL,
            model_p REAL NOT NULL,
            market_p REAL NOT NULL,
            edge REAL NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(market_slug, timeframe, side)
        );
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_slug TEXT NOT NULL,
            timeframe INTEGER NOT NULL,
            side TEXT NOT NULL,
            token_id TEXT NOT NULL,
            entry_price REAL NOT NULL,
            quantity REAL NOT NULL,
            remaining_quantity REAL NOT NULL DEFAULT 0,
            realized_pnl REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'OPEN',
            partial_taken INTEGER NOT NULL DEFAULT 0,
            last_action TEXT NOT NULL DEFAULT '',
            opened_at TEXT NOT NULL,
            closed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS alerts (
            alert_key TEXT PRIMARY KEY,
            sent_at REAL NOT NULL
        );
        """
    )

    # Migrate databases created by earlier bot versions. CREATE TABLE IF NOT
    # EXISTS does not add columns to an existing SQLite table, so add any fields
    # required by this version before the scanner queries them.
    trade_columns = {row[1] for row in conn.execute("PRAGMA table_info(paper_trades)")}
    migrations = {
        "timeframe": "INTEGER NOT NULL DEFAULT 5",
        "side": "TEXT NOT NULL DEFAULT 'UP'",
        "token_id": "TEXT NOT NULL DEFAULT ''",
        "entry_price": "REAL NOT NULL DEFAULT 0",
        "quantity": "REAL NOT NULL DEFAULT 0",
        "remaining_quantity": "REAL NOT NULL DEFAULT 0",
        "realized_pnl": "REAL NOT NULL DEFAULT 0",
        "status": "TEXT NOT NULL DEFAULT 'OPEN'",
        "partial_taken": "INTEGER NOT NULL DEFAULT 0",
        "last_action": "TEXT NOT NULL DEFAULT ''",
        "opened_at": "TEXT NOT NULL DEFAULT ''",
        "closed_at": "TEXT",
    }
    for column, declaration in migrations.items():
        if column not in trade_columns:
            conn.execute(f"ALTER TABLE paper_trades ADD COLUMN {column} {declaration}")

    # Preserve old open positions by treating their original quantity as the
    # remaining quantity. Carry forward legacy pnl_usd values when present.
    trade_columns = {row[1] for row in conn.execute("PRAGMA table_info(paper_trades)")}
    conn.execute(
        "UPDATE paper_trades SET remaining_quantity=quantity "
        "WHERE status='OPEN' AND COALESCE(remaining_quantity,0)=0 AND quantity>0"
    )
    if "pnl_usd" in trade_columns:
        conn.execute(
            "UPDATE paper_trades SET realized_pnl=COALESCE(pnl_usd,0) "
            "WHERE COALESCE(realized_pnl,0)=0 AND COALESCE(pnl_usd,0)<>0"
        )
    conn.commit()
    return conn


def alert_once(conn: sqlite3.Connection, key: str, message: str) -> None:
    row = conn.execute("SELECT sent_at FROM alerts WHERE alert_key=?", (key,)).fetchone()
    if row and time.time() - row["sent_at"] < 3600:
        return
    telegram(message)
    conn.execute(
        "INSERT INTO alerts(alert_key,sent_at) VALUES(?,?) "
        "ON CONFLICT(alert_key) DO UPDATE SET sent_at=excluded.sent_at",
        (key, time.time()),
    )
    conn.commit()


def market_slug(minutes: int, now_epoch: Optional[int] = None) -> str:
    now_epoch = int(now_epoch or time.time())
    period = minutes * 60
    start = now_epoch - (now_epoch % period)
    return f"btc-updown-{minutes}m-{start}"


def get_market(minutes: int) -> Optional[dict]:
    slug = market_slug(minutes)
    try:
        data = http_json(f"{GAMMA}/markets", {"slug": slug})
        if isinstance(data, list) and data:
            return data[0]
        if isinstance(data, dict) and data:
            return data
    except Exception as exc:
        print(f"Gamma lookup failed for {slug}: {exc}")
    return None


def parse_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def outcome_tokens(market: dict) -> dict[str, str]:
    outcomes = parse_list(market.get("outcomes"))
    tokens = parse_list(market.get("clobTokenIds"))
    mapping: dict[str, str] = {}
    for outcome, token in zip(outcomes, tokens):
        name = str(outcome).strip().lower()
        if name in ("up", "yes"):
            mapping["UP"] = str(token)
        elif name in ("down", "no"):
            mapping["DOWN"] = str(token)
    return mapping


def get_book(token_id: str) -> Optional[dict]:
    try:
        return http_json(f"{CLOB}/book", {"token_id": token_id})
    except Exception as exc:
        print(f"Order book lookup failed: {exc}")
        return None


def book_stats(book: dict) -> Optional[dict]:
    bids = sorted(book.get("bids", []), key=lambda x: float(x.get("price", 0)), reverse=True)
    asks = sorted(book.get("asks", []), key=lambda x: float(x.get("price", 1)))
    if not bids or not asks:
        return None
    bid = float(bids[0]["price"])
    ask = float(asks[0]["price"])
    bid_depth = sum(float(x.get("price", 0)) * float(x.get("size", 0)) for x in bids[:10])
    ask_depth = sum(float(x.get("price", 0)) * float(x.get("size", 0)) for x in asks[:10])
    return {
        "bid": bid,
        "ask": ask,
        "spread": max(0.0, ask - bid),
        "depth": min(bid_depth, ask_depth),
        "imbalance": (bid_depth - ask_depth) / max(1.0, bid_depth + ask_depth),
    }


def get_candles() -> list[list[float]]:
    end = utc_now()
    start = end.timestamp() - 120 * 60
    params = {
        "granularity": 60,
        "start": datetime.fromtimestamp(start, timezone.utc).isoformat(),
        "end": end.isoformat(),
    }
    data = http_json(COINBASE, params)
    # Coinbase candle order: [time, low, high, open, close, volume], newest first.
    return sorted(data, key=lambda row: int(row[0]))


def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (period + 1.0)
    result = [values[0]]
    for value in values[1:]:
        result.append(alpha * value + (1.0 - alpha) * result[-1])
    return result


def rsi(values: list[float], period: int = 14) -> Optional[float]:
    if len(values) <= period:
        return None
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(x, 0.0) for x in changes[-period:]]
    losses = [max(-x, 0.0) for x in changes[-period:]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def technical_signal(candles: list[list[float]]) -> dict:
    closes = [float(row[4]) for row in candles]
    volumes = [float(row[5]) for row in candles]
    if len(closes) < 55:
        raise RuntimeError(f"Not enough Coinbase candles ({len(closes)}); need at least 55")
    e9, e21, e50 = ema(closes, 9), ema(closes, 21), ema(closes, 50)
    current_rsi = rsi(closes)
    macd_fast, macd_slow = ema(closes, 12), ema(closes, 26)
    macd_now = macd_fast[-1] - macd_slow[-1]
    macd_prev = macd_fast[-2] - macd_slow[-2]
    move3 = (closes[-1] / closes[-4] - 1.0) * 100.0
    move10 = (closes[-1] / closes[-11] - 1.0) * 100.0
    vol_avg = sum(volumes[-21:-1]) / max(1, len(volumes[-21:-1]))
    vol_ratio = volumes[-1] / vol_avg if vol_avg > 0 else 0.0
    votes = 0
    votes += 1 if e9[-1] > e21[-1] > e50[-1] else (-1 if e9[-1] < e21[-1] < e50[-1] else 0)
    votes += 1 if current_rsi is not None and 52 <= current_rsi <= 68 else (-1 if current_rsi is not None and 32 <= current_rsi <= 48 else 0)
    votes += 1 if macd_now > macd_prev else (-1 if macd_now < macd_prev else 0)
    votes += 1 if move3 >= 0.035 and move10 > 0 else (-1 if move3 <= -0.035 and move10 < 0 else 0)
    votes += 1 if vol_ratio >= 1.10 and move3 > 0 else (-1 if vol_ratio >= 1.10 and move3 < 0 else 0)
    side = "UP" if votes > 0 else "DOWN" if votes < 0 else "NEUTRAL"
    return {
        "score": votes,
        "side": side,
        "rsi": current_rsi if current_rsi is not None else 50.0,
        "move3": move3,
        "move10": move10,
        "vol_ratio": vol_ratio,
        "last_price": closes[-1],
    }


def seconds_remaining(minutes: int, now_epoch: Optional[float] = None) -> int:
    now_epoch = now_epoch or time.time()
    period = minutes * 60
    return int(period - (now_epoch % period))


def model_probability(score: int, side: str) -> float:
    # A deliberately rough score proxy, not a statistically calibrated probability.
    directional = abs(score)
    return min(0.78, 0.50 + 0.055 * directional)


def daily_closed_pnl(conn: sqlite3.Connection) -> float:
    today = utc_now().date().isoformat()
    row = conn.execute(
        "SELECT COALESCE(SUM(realized_pnl),0) AS pnl FROM paper_trades "
        "WHERE substr(COALESCE(closed_at,opened_at),1,10)=?",
        (today,),
    ).fetchone()
    return float(row["pnl"] or 0.0)


def open_trade_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) AS n FROM paper_trades WHERE status='OPEN'").fetchone()
    return int(row["n"])


def entries_today(conn: sqlite3.Connection) -> int:
    today = utc_now().date().isoformat()
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM paper_trades WHERE substr(opened_at,1,10)=?", (today,)
    ).fetchone()
    return int(row["n"])


def record_observation(conn: sqlite3.Connection, slug: str, minutes: int, side: str,
                       model_p: float, market_p: float, edge: float) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO signals(market_slug,timeframe,side,model_p,market_p,edge,created_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (slug, minutes, side, model_p, market_p, edge, utc_now().isoformat()),
    )
    conn.commit()


def create_paper_trade(conn: sqlite3.Connection, market: dict, minutes: int, side: str,
                       token_id: str, ask: float) -> bool:
    if ask <= 0 or ask >= 1:
        return False
    quantity = PAPER_STAKE_USD / ask
    conn.execute(
        "INSERT INTO paper_trades(market_slug,timeframe,side,token_id,entry_price,quantity,remaining_quantity,opened_at) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (market.get("slug", ""), minutes, side, token_id, ask, quantity, quantity, utc_now().isoformat()),
    )
    conn.commit()
    telegram(
        f"🧪 PAPER ENTRY — BTC {minutes}m {side}\n"
        f"Entry: ${ask:.3f} | Stake: ${PAPER_STAKE_USD:.2f}\n"
        f"Quantity: {quantity:.3f} shares\n"
        "Real-money orders: DISABLED"
    )
    return True


def hold_and_flip(roi: float, score: int, position_side: str, imbalance: float) -> tuple[int, int]:
    aligned = score if position_side == "UP" else -score
    hold = max(0, min(100, 50 + aligned * 8 + int(imbalance * 15) + (8 if roi > 0 else -5 if roi < -0.04 else 0)))
    flip = max(0, min(100, 50 - aligned * 8 - int(imbalance * 15) + (15 if aligned < 0 else 0)))
    return hold, flip


# BTC Polymarket Telegram call-sign upgrade
# Paste this over the EXISTING manage_open_trades() function in btc_poly_bot_v2.py.
# It uses the bot's existing data, hold_and_flip(), SQLite fields and Telegram sender.
# No new package, API key, or extra service is required.

def manage_open_trades(conn: sqlite3.Connection, tech: dict) -> None:
    rows = conn.execute("SELECT * FROM paper_trades WHERE status='OPEN'").fetchall()

    for trade in rows:
        book = get_book(trade["token_id"])
        stats = book_stats(book) if book else None
        if not stats:
            continue

        bid = stats["bid"]                  # Best current Polymarket sell bid
        entry = float(trade["entry_price"])
        roi = (bid / entry - 1.0) if entry else 0.0
        hold, flip = hold_and_flip(
            roi, int(tech["score"]), trade["side"], stats["imbalance"]
        )

        qty = float(trade["remaining_quantity"])
        partial_taken = int(trade["partial_taken"])
        realized = float(trade["realized_pnl"])

        # Plain-English market call, relative to the currently held UP/DOWN token.
        market_side = tech.get("side", "NEUTRAL")
        position_side = trade["side"]
        aligned_score = int(tech["score"]) if position_side == "UP" else -int(tech["score"])

        if market_side == "NEUTRAL" or int(tech["score"]) == 0:
            call = "⚪ WAIT — NO CLEAR DIRECTION"
        elif market_side != position_side and abs(int(tech["score"])) >= 2:
            call = f"🔄 FLIP RISK — MARKET CALL IS {market_side}"
        elif market_side == "UP":
            call = "🟢 UP CALL — MARKET LEANING UP"
        else:
            call = "🔴 DOWN CALL — MARKET LEANING DOWN"

        # Keep the bot's existing paper-management thresholds unchanged.
        partial_target = entry * 1.05
        full_target = entry * 1.10
        protective_stop = entry * 0.92
        next_target = full_target if partial_taken else partial_target

        action = "HOLD"
        close_qty = 0.0

        if roi <= -0.08:
            action = "🛑 STOP / EXIT 100%"
            close_qty = qty
        elif roi >= 0.10:
            action = "🎯 TAKE PROFIT / EXIT 100%"
            close_qty = qty
        elif flip >= 80 or hold < 35:
            action = "🔴 HIGH FLIP RISK / EXIT 100%"
            close_qty = qty
        elif roi >= 0.05 and not partial_taken:
            action = "🟠 TAKE PROFIT / SELL 50%"
            close_qty = qty / 2.0
        elif flip >= 65:
            if not partial_taken:
                action = "🟠 FLIP RISK / SELL 50%"
                close_qty = qty / 2.0
            else:
                action = "🟡 FLIP RISK / WATCH"
        elif hold < 55:
            action = "🟡 WATCH CLOSELY"

        # Record simulated fills exactly as the original paper bot did.
        if close_qty > 0:
            realized += (bid - entry) * close_qty
            qty -= close_qty
            if qty <= 0.000001:
                status = "CLOSED"
                closed_at = utc_now().isoformat()
                qty = 0.0
            else:
                status = "OPEN"
                closed_at = None

            if "SELL 50%" in action:
                partial_taken = 1

            conn.execute(
                "UPDATE paper_trades SET remaining_quantity=?,realized_pnl=?,"
                "status=?,partial_taken=?,last_action=?,closed_at=? WHERE id=?",
                (qty, realized, status, partial_taken, action, closed_at, trade["id"]),
            )
            conn.commit()
            sell_line = f"SELL PRICE NOW: ${bid:.3f} (current best bid)"
        else:
            status = "OPEN"
            closed_at = None
            sell_line = f"NEXT SELL TARGET: ${next_target:.3f}"
            if action != trade["last_action"]:
                conn.execute(
                    "UPDATE paper_trades SET last_action=? WHERE id=?",
                    (action, trade["id"]),
                )
                conn.commit()

        # Send a readable alert when the action changes; risk/exit actions remain visible.
        if action != trade["last_action"] or "EXIT" in action or "SELL 50%" in action:
            telegram(
                f"📣 BTC {trade['timeframe']}m TRADE CALL\n"
                f"POSITION: {position_side}\n"
                f"CALL: {call}\n\n"
                f"🟢 HOLD STRENGTH: {hold}%\n"
                f"🟠 FLIP RISK: {flip}%\n"
                f"ACTION: {action}\n\n"
                f"ENTRY: ${entry:.3f}\n"
                f"CURRENT SELL BID: ${bid:.3f}\n"
                f"{sell_line}\n"
                f"PARTIAL PROFIT LEVEL (+5%): ${partial_target:.3f}\n"
                f"FULL PROFIT LEVEL (+10%): ${full_target:.3f}\n"
                f"PROTECTIVE STOP (-8%): ${protective_stop:.3f}\n"
                f"POSITION RETURN: {roi * 100:+.1f}%\n"
                f"RSI: {float(tech.get('rsi', 50)):.1f} | "
                f"BTC 3m: {float(tech.get('move3', 0)):+.3f}% | "
                f"BTC 10m: {float(tech.get('move10', 0)):+.3f}%\n\n"
                "PAPER MODE ONLY — no real orders are sent. "
                "Hold/flip values are heuristic scores, not win probabilities. "
                "A target is not a guaranteed fill; the displayed bid is the current "
                "top bid and may change before an order could execute."
            )
