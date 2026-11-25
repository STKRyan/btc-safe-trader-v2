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
# Scalp signals are deliberately paper-only and use tighter liquidity/momentum filters.
SCALP_MIN_VOTES = 3
SCALP_MIN_SECONDS_LEFT = 90
SCALP_MAX_SECONDS_LEFT = 240
SCALP_MIN_ASK = 0.25
SCALP_MAX_ASK = 0.75
SCALP_MAX_SPREAD = 0.02
SCALP_MIN_DEPTH_USD = 500.00
SCALP_TAKE_PROFIT = 0.04       # +4% contract-price return target
SCALP_STOP_LOSS = 0.03         # -3% contract-price protective stop
SCALP_MAX_HOLD_SECONDS = 120
HTTP_TIMEOUT = 12
DB_PATH = os.getenv("BTC_BOT_DB", "btc_poly_v2.sqlite3")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "BTC-Conservative-Paper-Bot/2.0"})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def telegram_chat_ids() -> list[str]:
    """Return all configured destinations; supports a private chat and groups."""
    raw = os.getenv("TELEGRAM_CHAT_IDS", "")
    ids = [part.strip() for part in raw.replace(";", ",").split(",") if part.strip()]
    legacy = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if legacy:
        ids.append(legacy)
    # Preserve order while removing duplicates.
    return list(dict.fromkeys(ids))


def telegram(message: str, reply_markup: Optional[dict] = None) -> bool:
    """Send a compact HTML-formatted Telegram card to every configured destination."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_ids = telegram_chat_ids()
    if not token or not chat_ids:
        print("Telegram credentials or destination IDs missing; message not sent:", message[:200])
        return False
    sent = False
    for chat_id in chat_ids:
        data = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if reply_markup:
            data["reply_markup"] = json.dumps(reply_markup)
        try:
            response = SESSION.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=data,
                timeout=HTTP_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("ok"):
                sent = True
            else:
                print(f"Telegram API rejected destination {chat_id}: {payload}")
        except Exception as exc:
            print(f"Telegram send failed for destination {chat_id}: {exc}")
    return sent


def telegram_api(method: str, data: dict) -> Any:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        return None
    response = SESSION.post(
        f"https://api.telegram.org/bot{token}/{method}", data=data, timeout=HTTP_TIMEOUT
    )
    response.raise_for_status()
    payload = response.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram {method} failed: {payload}")
    return payload.get("result")


def menu_markup() -> dict:
    return {"inline_keyboard": [
        [{"text": "📊 Dashboard", "callback_data": "dashboard"},
         {"text": "⚡ Scalp setups", "callback_data": "scalps"}],
        [{"text": "📂 Open positions", "callback_data": "positions"}],
    ]}


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
        CREATE TABLE IF NOT EXISTS scalp_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_slug TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            target_price REAL NOT NULL,
            stop_price REAL NOT NULL,
            setup_score INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(market_slug, side)
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
        "trade_type": "TEXT NOT NULL DEFAULT 'STANDARD'",
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
    if not telegram(message, menu_markup()):
        return
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
        "move1": (closes[-1] / closes[-2] - 1.0) * 100.0,
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


def gauge_bar(value: int | float, width: int = 10) -> str:
    """Compact visual for heuristic scores; not a probability."""
    pct = max(0, min(100, int(round(float(value)))))
    filled = round(pct / 100 * width)
    return "▰" * filled + "▱" * (width - filled)


def gauge_line(label: str, value: int | float, icon: str = "") -> str:
    pct = max(0, min(100, int(round(float(value)))))
    return f"{icon} <b>{label} {pct}%</b>  <code>{gauge_bar(pct)}</code>"


def create_paper_trade(conn: sqlite3.Connection, market: dict, minutes: int, side: str,
                       token_id: str, ask: float, trade_type: str = "STANDARD") -> bool:
    if ask <= 0 or ask >= 1:
        return False
    quantity = PAPER_STAKE_USD / ask
    conn.execute(
        "INSERT INTO paper_trades(market_slug,timeframe,side,token_id,entry_price,quantity,remaining_quantity,opened_at,trade_type) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        (market.get("slug", ""), minutes, side, token_id, ask, quantity, quantity,
         utc_now().isoformat(), trade_type),
    )
    conn.commit()
    label = "⚡ SCALP PAPER ENTRY" if trade_type == "SCALP" else "🧪 PAPER ENTRY"
    target = ask * (1.0 + (SCALP_TAKE_PROFIT if trade_type == "SCALP" else 0.10))
    stop = ask * (1.0 - (SCALP_STOP_LOSS if trade_type == "SCALP" else 0.08))
    telegram(
        f"<b>{label} · BTC {minutes}m {side}</b>\n"
        f"🟢 IN <code>${ask:.3f}</code> · 💵 ${PAPER_STAKE_USD:.2f}\n"
        f"🎯 TP ${target:.3f} · 🛑 SL ${stop:.3f}\n"
        f"📦 {quantity:.3f} shares\n"
        "<i>Paper only · simulated fills</i>", menu_markup()
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
        bid = stats["bid"]
        entry = float(trade["entry_price"])
        if entry <= 0:
            continue
        roi = bid / entry - 1.0
        hold, flip = hold_and_flip(roi, int(tech["score"]), trade["side"], stats["imbalance"])
        qty = float(trade["remaining_quantity"])
        partial_taken = int(trade["partial_taken"])
        realized = float(trade["realized_pnl"])
        trade_type = str(trade["trade_type"] or "STANDARD").upper()
        position_side = trade["side"]
        market_side = tech.get("side", "NEUTRAL")

        if market_side == "NEUTRAL" or int(tech["score"]) == 0:
            call = "⚪ WAIT"
        elif market_side != position_side and abs(int(tech["score"])) >= 2:
            call = f"🔄 FLIP RISK — {market_side}"
        else:
            call = "🟢 UP LEAN" if market_side == "UP" else "🔴 DOWN LEAN"

        if trade_type == "SCALP":
            target = entry * (1.0 + SCALP_TAKE_PROFIT)
            stop = entry * (1.0 - SCALP_STOP_LOSS)
            opened = datetime.fromisoformat(str(trade["opened_at"]).replace("Z", "+00:00"))
            age_seconds = max(0, (utc_now() - opened).total_seconds())
            action = "HOLD SCALP"
            if roi >= SCALP_TAKE_PROFIT:
                action = "🎯 SCALP TARGET — EXIT 100%"
            elif roi <= -SCALP_STOP_LOSS:
                action = "🛑 SCALP STOP — EXIT 100%"
            elif flip >= 75 or (market_side not in (position_side, "NEUTRAL") and abs(int(tech["score"])) >= 2):
                action = "🔄 SCALP FLIP WARNING — EXIT 100%"
            elif age_seconds >= SCALP_MAX_HOLD_SECONDS:
                action = "⏱ SCALP TIME EXIT — EXIT 100%"
            close_qty = qty if "EXIT 100%" in action else 0.0
            partial_target = target
            full_target = target
            protective_stop = stop
            sell_line = f"<b>Current sell bid:</b> ${bid:.3f}"
            action_label = action
        else:
            partial_target = entry * 1.05
            full_target = entry * 1.10
            protective_stop = entry * 0.92
            next_target = full_target if partial_taken else partial_target
            action = "HOLD"
            close_qty = 0.0
            if roi <= -0.08:
                action, close_qty = "🛑 STOP / EXIT 100%", qty
            elif roi >= 0.10:
                action, close_qty = "🎯 TAKE PROFIT / EXIT 100%", qty
            elif flip >= 80 or hold < 35:
                action, close_qty = "🔴 HIGH FLIP RISK / EXIT 100%", qty
            elif roi >= 0.05 and not partial_taken:
                action, close_qty = "🟠 TAKE PROFIT / SELL 50%", qty / 2.0
            elif flip >= 65:
                if not partial_taken:
                    action, close_qty = "🟠 FLIP RISK / SELL 50%", qty / 2.0
                else:
                    action = "🟡 FLIP RISK / WATCH"
            elif hold < 55:
                action = "🟡 WATCH CLOSELY"
            sell_line = f"<b>Next sell target:</b> ${full_target if partial_taken else partial_target:.3f}"
            action_label = action

        if close_qty > 0:
            realized += (bid - entry) * close_qty
            qty -= close_qty
            closed = qty <= 0.000001
            if closed:
                qty, status, closed_at = 0.0, "CLOSED", utc_now().isoformat()
            else:
                status, closed_at = "OPEN", None
            if "SELL 50%" in action:
                partial_taken = 1
            conn.execute(
                "UPDATE paper_trades SET remaining_quantity=?,realized_pnl=?,status=?,partial_taken=?,last_action=?,closed_at=? WHERE id=?",
                (qty, realized, status, partial_taken, action, closed_at, trade["id"]),
            )
            conn.commit()
            sell_line = f"<b>Simulated exit bid:</b> ${bid:.3f}"

        should_alert = action != trade["last_action"] or (close_qty > 0)
        if should_alert:
            label = "⚡ SCALP MANAGEMENT" if trade_type == "SCALP" else "📣 BTC POSITION UPDATE"
            telegram(
                f"<b>{label} · {trade['timeframe']}m {position_side}</b>\n"
                f"<b>{action_label}</b> · {call}\n\n"
                f"{gauge_line('HOLD', hold, '🟢')}\n"
                f"{gauge_line('FLIP RISK', flip, '🟠')}\n\n"
                f"🎟 IN <code>${entry:.3f}</code> · 📍 BID <code>${bid:.3f}</code>\n"
                f"{sell_line}\n"
                f"🎯 TP <code>${full_target:.3f}</code> · 🛑 SL <code>${protective_stop:.3f}</code>\n"
                f"📈 P/L <b>{roi*100:+.2f}%</b>\n"
                f"₿ RSI {float(tech.get('rsi',50)):.0f} · 1m {float(tech.get('move1',0)):+.2f}% · 3m {float(tech.get('move3',0)):+.2f}%\n"
                "<i>Paper only · gauges are heuristic, not calibrated odds.</i>",
                menu_markup(),
            )
        if action != trade["last_action"]:
            conn.execute("UPDATE paper_trades SET last_action=? WHERE id=?", (action, trade["id"]))
            conn.commit()


def scan_scalp(conn: sqlite3.Connection, tech: dict) -> None:
    """Find selective short-horizon 5m contract scalps; paper trade only."""
    market = get_market(5)
    if not market:
        return
    slug = str(market.get("slug") or market_slug(5))
    side = str(tech.get("side", "NEUTRAL"))
    score = int(tech.get("score", 0))
    if side not in ("UP", "DOWN") or abs(score) < SCALP_MIN_VOTES:
        return
    move1 = float(tech.get("move1", 0.0))
    move3 = float(tech.get("move3", 0.0))
    rsi_value = float(tech.get("rsi", 50.0))
    momentum_ok = (move1 > 0 and move3 > 0.035) if side == "UP" else (move1 < 0 and move3 < -0.035)
    rsi_ok = (48 <= rsi_value <= 70) if side == "UP" else (30 <= rsi_value <= 52)
    remaining = seconds_remaining(5)
    if not (SCALP_MIN_SECONDS_LEFT <= remaining <= SCALP_MAX_SECONDS_LEFT and momentum_ok and rsi_ok):
        return
    tokens = outcome_tokens(market)
    token_id = tokens.get(side)
    if not token_id:
        return
    book = get_book(token_id)
    stats = book_stats(book) if book else None
    if not stats:
        return
    ask, bid = stats["ask"], stats["bid"]
    filters_ok = (
        SCALP_MIN_ASK <= ask <= SCALP_MAX_ASK
        and stats["spread"] <= SCALP_MAX_SPREAD
        and stats["depth"] >= SCALP_MIN_DEPTH_USD
    )
    if not filters_ok:
        return
    # A setup score ranks agreement of independent filters; it is not a win probability.
    setup_score = min(95, 45 + abs(score)*7 + min(12, abs(move3)*100) + (8 if stats["imbalance"] * (1 if side == "UP" else -1) > 0 else 0))
    target = ask * (1.0 + SCALP_TAKE_PROFIT)
    stop = ask * (1.0 - SCALP_STOP_LOSS)
    conn.execute(
        "INSERT OR IGNORE INTO scalp_signals(market_slug,side,entry_price,target_price,stop_price,setup_score,created_at) VALUES(?,?,?,?,?,?,?)",
        (slug, side, ask, target, stop, setup_score, utc_now().isoformat()),
    )
    conn.commit()
    key = f"scalp:{slug}:{side}"
    alert_once(
        conn, key,
        f"<b>⚡ BTC SCALP SETUP · 5m {side}</b>\n"
        f"<b>Setup score:</b> {setup_score}/100 <i>(ranking score, not win probability)</i>\n"
        f"<b>Entry zone:</b> ${ask:.3f}–${min(0.99, ask+stats['spread']):.3f}\n"
        f"<b>Quick target (+{SCALP_TAKE_PROFIT*100:.1f}%):</b> ${target:.3f}\n"
        f"<b>Protective stop (-{SCALP_STOP_LOSS*100:.1f}%):</b> ${stop:.3f}\n"
        f"<b>Current bid:</b> ${bid:.3f} · <b>Spread:</b> {stats['spread']:.3f}\n"
        f"<b>BTC:</b> 1m {move1:+.3f}% · 3m {move3:+.3f}% · RSI {rsi_value:.1f}\n"
        f"<b>Time left:</b> {remaining}s\n"
        "<i>Strict filters passed. Paper-only entry candidate; fees, spread and slippage can erase small gains.</i>",
    )
    # Reuse the existing conservative daily caps and stake. Avoid duplicate entries in the same 5m market.
    already = conn.execute(
        "SELECT 1 FROM paper_trades WHERE market_slug=? AND side=? AND trade_type='SCALP' LIMIT 1",
        (slug, side),
    ).fetchone()
    if already:
        return
    if daily_closed_pnl(conn) <= -MAX_DAILY_PAPER_LOSS_USD or entries_today(conn) >= MAX_DAILY_ENTRIES:
        return
    if open_trade_count(conn) >= MAX_OPEN_PAPER_TRADES:
        return
    create_paper_trade(conn, market, 5, side, token_id, ask, "SCALP")


def dashboard_text(conn: sqlite3.Connection) -> str:
    open_count = open_trade_count(conn)
    pnl = daily_closed_pnl(conn)
    last = conn.execute("SELECT side,setup_score,entry_price,target_price FROM scalp_signals ORDER BY id DESC LIMIT 1").fetchone()
    if last:
        score = int(last["setup_score"] or 0)
        scalp = (f"⚡ LAST SCALP: <b>{last['side']}</b> · {score}/100\n"
                 f"<code>{gauge_bar(score)}</code>\nIN ${last['entry_price']:.3f} → TP ${last['target_price']:.3f}")
    else:
        scalp = "⚡ SCALP: <b>NO QUALIFIED SETUP</b>"
    pnl_icon = "🟢" if pnl >= 0 else "🔴"
    return ("<b>₿ BTC SIGNAL DESK</b>\n"
            f"🧪 PAPER · 📂 OPEN {open_count} · {pnl_icon} P/L ${pnl:+.2f}\n\n"
            f"{scalp}\n\n"
            "<i>Scores are heuristic gauges, not win probabilities.</i>")


def scalps_text(conn: sqlite3.Connection) -> str:
    rows = conn.execute("SELECT * FROM scalp_signals ORDER BY id DESC LIMIT 5").fetchall()
    if not rows:
        body = "<b>NO QUALIFIED SETUPS</b>\nWaiting for filters to align."
    else:
        cards = []
        for r in rows:
            score = int(r["setup_score"] or 0)
            icon = "🟢" if r["side"] == "UP" else "🔴"
            cards.append(f"<b>{icon} {r['side']} · {score}/100</b>\n"
                         f"<code>{gauge_bar(score)}</code>\n"
                         f"IN ${r['entry_price']:.3f} · TP ${r['target_price']:.3f} · SL ${r['stop_price']:.3f}")
        body = "\n\n".join(cards)
    return "<b>⚡ SCALP RADAR</b>\n" + body + "\n\n<i>Setup score ≠ win probability.</i>"


def positions_text(conn: sqlite3.Connection) -> str:
    rows = conn.execute("SELECT * FROM paper_trades WHERE status='OPEN' ORDER BY id DESC LIMIT 10").fetchall()
    if not rows:
        body = "<b>NO OPEN POSITIONS</b>\nStanding by."
    else:
        cards = []
        for r in rows:
            kind = "⚡ SCALP" if r["trade_type"] == "SCALP" else "📈 STANDARD"
            cards.append(f"<b>{kind} · {r['timeframe']}m {r['side']}</b>\n"
                         f"IN ${r['entry_price']:.3f} · LEFT {r['remaining_quantity']:.3f} shares")
        body = "\n\n".join(cards)
    return "<b>📂 OPEN POSITIONS</b>\n" + body


def process_telegram_updates(conn: sqlite3.Connection, offset: int) -> int:
    """Handle menu taps and slash commands while the GitHub Actions job is running."""
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    allowed_chats = set(telegram_chat_ids())
    if not token:
        return offset
    try:
        updates = telegram_api("getUpdates", {"offset": offset, "timeout": 0, "allowed_updates": json.dumps(["message", "callback_query"])}) or []
        for update in updates:
            offset = max(offset, int(update["update_id"]) + 1)
            callback = update.get("callback_query")
            message = callback.get("message", {}) if callback else update.get("message", {})
            chat = message.get("chat", {})
            incoming_chat_id = str(chat.get("id", ""))
            if not incoming_chat_id:
                continue
            if callback:
                action = callback.get("data", "dashboard")
                try:
                    telegram_api("answerCallbackQuery", {"callback_query_id": callback["id"]})
                except Exception as callback_exc:
                    # A queued tap can outlive Telegram's short callback-answer window;
                    # still try to refresh the panel itself.
                    print(f"Callback acknowledgement skipped: {callback_exc}")
            else:
                action = str(message.get("text", "")).split()[0].lower().split("@")[0]
            # Let an administrator discover the group ID after adding the bot.
            # This command is intentionally handled before destination filtering.
            if action in ("/chatid", "/id"):
                telegram_api("sendMessage", {
                    "chat_id": incoming_chat_id,
                    "text": f"This Telegram chat ID is: {incoming_chat_id}\nAdd it to the TELEGRAM_CHAT_IDS GitHub secret to receive scanner alerts here.",
                    "disable_web_page_preview": True,
                })
                continue
            if incoming_chat_id not in allowed_chats:
                continue
            if action in ("dashboard", "/start", "/status", "/dashboard"):
                card = dashboard_text(conn)
            elif action in ("scalps", "/scalps"):
                card = scalps_text(conn)
            elif action in ("positions", "/positions"):
                card = positions_text(conn)
            elif action in ("/help", "/menu"):
                card = "<b>BTC SIGNAL DESK</b>\nUse /dashboard, /scalps, or /positions. Or tap a panel button below."
            else:
                continue
            if callback:
                telegram_api("editMessageText", {
                    "chat_id": incoming_chat_id,
                    "message_id": message["message_id"],
                    "text": card,
                    "parse_mode": "HTML",
                    "reply_markup": json.dumps(menu_markup()),
                    "disable_web_page_preview": True,
                })
            else:
                telegram(card, menu_markup())
    except Exception as exc:
        print(f"Telegram menu polling error: {type(exc).__name__}: {exc}")
    return offset


def scan_one(conn: sqlite3.Connection, minutes: int, tech: dict) -> None:
    market = get_market(minutes)
    if not market:
        slug_hint = market_slug(minutes)
        print(f"No active Gamma market found for BTC {minutes}m ({slug_hint})")
        alert_once(
            conn, f"status_v1:{slug_hint}:{minutes}:NO_MARKET",
            f"<b>₿ BTC {minutes}m · 🟡 WAIT</b>\n⛔ <b>NO ENTRY</b> · Market data unavailable\n<i>Scanner is running; waiting for a valid market.</i>"
        )
        return
    slug = str(market.get("slug") or market_slug(minutes))
    remaining = seconds_remaining(minutes)
    tokens = outcome_tokens(market)
    if not tokens:
        print(f"Could not map UP/DOWN token IDs for {slug}; outcomes={market.get('outcomes')}")
        alert_once(
            conn, f"status_v1:{slug}:{minutes}:NO_TOKENS",
            f"<b>₿ BTC {minutes}m · 🟡 WAIT</b>\n⛔ <b>NO ENTRY</b> · Market outcomes unavailable\n<i>Safety filter active; no trade will be considered.</i>"
        )
        return
    side = tech["side"]
    if side not in ("UP", "DOWN"):
        print(f"BTC {minutes}m neutral signal; no entry")
        wait_key = f"signal_v3:{slug}:{minutes}:WAIT"
        wait_message = (
            f"<b>₿ BTC {minutes}m · ⚪ WAIT</b>\n"
            f"VOTES {tech['score']}/5 · RSI {tech['rsi']:.0f}\n"
            f"3m {tech['move3']:+.2f}% · 10m {tech['move10']:+.2f}%\n"
            "⏸ <b>NO ENTRY</b> · No clear edge\n"
            "<i>Paper only · heuristic signal.</i>"
        )
        alert_once(conn, wait_key, wait_message)
        return
    token_id = tokens.get(side)
    if not token_id:
        print(f"Missing {side} token for {slug}")
        alert_once(
            conn, f"status_v1:{slug}:{minutes}:MISSING_TOKEN",
            f"<b>₿ BTC {minutes}m · {side}</b>\n⛔ <b>NO ENTRY</b> · Outcome token unavailable\n<i>Safety filter active.</i>"
        )
        return
    book = get_book(token_id)
    stats = book_stats(book) if book else None
    if not stats:
        print(f"No usable order book for {slug} {side}")
        hold, flip = hold_and_flip(0.0, int(tech["score"]), side, 0.0)
        alert_once(
            conn, f"status_v1:{slug}:{minutes}:NO_BOOK:{side}",
            f"<b>₿ BTC {minutes}m · {'🟢 UP' if side == 'UP' else '🔴 DOWN'} WATCH</b>\n"
            f"{gauge_line('STRENGTH', hold, '🟢')}\n{gauge_line('FLIP RISK', flip, '🟠')}\n\n"
            f"VOTES {abs(int(tech['score']))}/5 · RSI {tech['rsi']:.0f}\n"
            f"3m {tech['move3']:+.2f}% · 10m {tech['move10']:+.2f}%\n"
            "⛔ <b>NO ENTRY</b> · Order book unavailable\n"
            "<i>Signal only · no trade opened.</i>"
        )
        return
    ask = stats["ask"]
    market_p = ask
    model_p = model_probability(int(tech["score"]), side)
    edge = model_p - market_p
    record_observation(conn, slug, minutes, side, model_p, market_p, edge)
    reasons = []
    if abs(int(tech["score"])) < MIN_VOTES[minutes]:
        reasons.append(f"directional votes {abs(int(tech['score']))}/5 below {MIN_VOTES[minutes]}")
    if stats["depth"] < MIN_BOOK_DEPTH_USD:
        reasons.append("book depth too low")
    if stats["spread"] > MAX_SPREAD:
        reasons.append("spread too wide")
    if ask > MAX_ENTRY_ASK:
        reasons.append("entry ask too expensive")
    if remaining < MIN_SECONDS_REMAINING:
        reasons.append("too little time remaining")
    if remaining / (minutes * 60) > MAX_SECONDS_REMAINING_FRACTION:
        reasons.append("market is too early")
    if edge < MIN_EDGE_PROXY:
        reasons.append("edge proxy below threshold")
    print(
        f"BTC {minutes}m {side} | votes {tech['score']}/5 | RSI {tech['rsi']:.1f} "
        f"| 3m {tech['move3']:+.3f}% | 10m {tech['move10']:+.3f}% "
        f"| ask {ask:.3f} bid {stats['bid']:.3f} depth ${stats['depth']:.0f} "
        f"| edge proxy {edge*100:+.1f}pp | {remaining}s left"
    )
    # Always send a useful directional call, even when entry filters block a trade.
    # This is a signal gauge, not an open-position HOLD calculation.
    signal_hold, signal_flip = hold_and_flip(0.0, int(tech["score"]), side, stats["imbalance"])
    call_sign = "🟢 UP" if side == "UP" else "🔴 DOWN"
    signal_key = f"signal_v3:{slug}:{minutes}:{side}"
    message = (
        f"<b>₿ BTC {minutes}m CALL · {call_sign}</b>\n\n"
        f"{gauge_line('STRENGTH', signal_hold, '🟢')}\n"
        f"{gauge_line('FLIP RISK', signal_flip, '🟠')}\n\n"
        f"VOTES {abs(int(tech['score']))}/5 · RSI {tech['rsi']:.0f}\n"
        f"3m {tech['move3']:+.2f}% · 10m {tech['move10']:+.2f}%\n"
        f"ASK ${ask:.3f} · BID ${stats['bid']:.3f} · DEPTH ${stats['depth']:.0f}\n"
        f"EDGE {edge*100:+.1f}pp\n\n"
        + ("⛔ <b>NO ENTRY</b> · " + "; ".join(reasons) if reasons else "✅ <b>FILTERS PASS</b> · paper entry considered")
        + "\n<i>Paper only · bars are heuristic gauges, not calibrated probabilities.</i>"
    )
    # Versioned key avoids the previous version's one-hour alert record suppressing this upgrade.
    alert_once(conn, signal_key, message)
    if reasons:
        return
    if daily_closed_pnl(conn) <= -MAX_DAILY_PAPER_LOSS_USD:
        print("Daily paper loss limit reached; entries paused")
        return
    if entries_today(conn) >= MAX_DAILY_ENTRIES:
        print("Daily paper entry cap reached")
        return
    if open_trade_count(conn) >= MAX_OPEN_PAPER_TRADES:
        print("Maximum open paper trades reached")
        return
    create_paper_trade(conn, market, minutes, side, token_id, ask)


def print_summary(conn: sqlite3.Connection) -> None:
    open_count = open_trade_count(conn)
    pnl = daily_closed_pnl(conn)
    total = conn.execute("SELECT COUNT(*) AS n FROM signals").fetchone()["n"]
    print(f"Summary: open paper trades={open_count}, today's realized PnL=${pnl:.2f}, saved signal observations={total}")
    print("Paper bankroll reference: $100.00; this is a simulation, not a live balance.")


def main() -> int:
    parser = argparse.ArgumentParser(description="BTC Polymarket paper scanner")
    parser.add_argument("--duration", type=int, default=0, help="Run for this many seconds; 0 means one scan")
    args = parser.parse_args()
    if args.duration < 0:
        parser.error("--duration must be zero or a positive number")
    conn = db_connect()
    alert_once(
        conn,
        "btc_signal_desk_startup_v3",
        "<b>🤖 BTC SIGNAL DESK IS ONLINE</b>\n"
        "Paper trading only · Real-money orders disabled\n"
        f"Stake ${PAPER_STAKE_USD:.2f} · Daily entry cap {MAX_DAILY_ENTRIES}\n"
        "5m/15m directional scanner + selective 5m scalp finder\n"
        "Tap a panel below for a compact dashboard.",
    )
    started = time.time()
    telegram_offset = 0
    try:
        try:
            telegram_api("setMyCommands", {"commands": json.dumps([
                {"command": "dashboard", "description": "BTC bot dashboard"},
                {"command": "scalps", "description": "Recent scalp setups"},
                {"command": "positions", "description": "Open paper positions"},
                {"command": "help", "description": "Bot menu help"},
            ])})
        except Exception as exc:
            print(f"Telegram command setup skipped: {exc}")
        while True:
            try:
                candles = get_candles()
                tech = technical_signal(candles)
                manage_open_trades(conn, tech)
                scan_scalp(conn, tech)
                for minutes in TIMEFRAMES:
                    try:
                        scan_one(conn, minutes, tech)
                    except Exception as exc:
                        print(f"BTC {minutes}m scan error: {type(exc).__name__}: {exc}")
                print_summary(conn)
            except Exception as exc:
                print(f"Scan cycle error: {type(exc).__name__}: {exc}")
                telegram(f"<b>⚠️ Scanner data error</b>\n<code>{type(exc).__name__}: {str(exc)[:300]}</code>", menu_markup())
            telegram_offset = process_telegram_updates(conn, telegram_offset)
            # Render worker mode: duration 0 means keep running continuously.
            # GitHub Actions can still pass --duration 180 for a bounded scan job.
            if args.duration > 0 and time.time() - started >= args.duration:
                break
            if args.duration > 0:
                remaining_runtime = max(1, args.duration - (time.time() - started))
                time.sleep(min(SCAN_INTERVAL_SECONDS, remaining_runtime))
            else:
                time.sleep(SCAN_INTERVAL_SECONDS)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
