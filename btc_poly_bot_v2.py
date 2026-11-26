#!/usr/bin/env python3
"""Polymarket BTC conservative paper scanner v2.

Paper trading only. This program never submits orders to the Polymarket CLOB.
It reads public market data, sends Discord webhook alerts, and records simulated trades
in SQLite. Model scores and hold/flip gauges are heuristic, not calibrated odds.
"""

from __future__ import annotations

import argparse
import json
import html
import re
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests
import urllib.parse

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
COINBASE = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
TIMEFRAMES = (5, 15)
MIN_VOTES = {5: 3, 15: 4}  # 5m is more flexible; 15m stays strict.
SCAN_INTERVAL_SECONDS = 1
DISCORD_DASHBOARD_UPDATE_SECONDS = 5  # keep Discord message edits within rate limits
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


def discord_send(message: str, reply_markup: Optional[dict] = None) -> bool:
    """Send an alert to the configured Discord channel using a private webhook."""
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook:
        print("DISCORD_WEBHOOK_URL is missing; Discord alert was not sent.")
        return False
    if not (webhook.startswith("https://discord.com/api/webhooks/") or
            webhook.startswith("https://discordapp.com/api/webhooks/")):
        print("DISCORD_WEBHOOK_URL does not look like a Discord webhook URL.")
        return False

    # Convert the scanner's existing HTML-style into readable Discord Markdown.
    text = message
    text = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*b\s*>", "**", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*/\s*b\s*>", "**", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*i\s*>", "*", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*/\s*i\s*>", "*", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*code\s*>", "`", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*/\s*code\s*>", "`", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text).strip()
    if len(text) > 1990:
        text = text[:1980] + "\n… (message shortened)"

    try:
        response = SESSION.post(
            webhook,
            json={"content": text, "allowed_mentions": {"parse": []}},
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        return True
    except Exception as exc:
        # Never print the webhook URL; it is a credential.
        print(f"Discord send failed: {type(exc).__name__}: {exc}")
        return False


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
        CREATE TABLE IF NOT EXISTS bot_state (
            state_key TEXT PRIMARY KEY,
            state_value TEXT NOT NULL
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

    # Repair older bot_state tables without deleting saved dashboard state.
    state_columns = {row[1] for row in conn.execute("PRAGMA table_info(bot_state)")}
    if "state_value" not in state_columns:
        conn.execute(
            "ALTER TABLE bot_state ADD COLUMN state_value TEXT NOT NULL DEFAULT ''"
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
    """Send each new directional paper signal once; keep WAIT/status noise dashboard-only."""
    if key.endswith(":WAIT") or any(tag in key for tag in (":NO_MARKET", ":NO_TOKENS", ":MISSING_TOKEN")):
        return
    # Directional signal messages and order-book WATCH calls are useful; generic
    # WAIT/status updates stay on the dashboard to avoid Discord spam.
    if not (key.startswith("signal_v4:") or ":NO_BOOK:" in key):
        return
    row = conn.execute("SELECT 1 FROM alerts WHERE alert_key=?", (key,)).fetchone()
    if row:
        return
    if discord_send(message):
        conn.execute("INSERT OR IGNORE INTO alerts(alert_key,sent_at) VALUES(?,?)", (key, time.time()))
        conn.commit()
        print(f"Discord BTC paper signal sent: {key}")

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
    highs = [float(row[2]) for row in candles]
    lows = [float(row[1]) for row in candles]
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
    recent_high=max(highs[-20:]); recent_low=min(lows[-20:])
    range_position=100.0*(closes[-1]-recent_low)/max(1e-12,recent_high-recent_low)
    range_pct=100.0*(recent_high-recent_low)/max(1e-12,closes[-1])
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
        "range_high": recent_high,
        "range_low": recent_low,
        "range_position": range_position,
        "range_pct": range_pct,
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


def direction_split(score: int) -> tuple[int, int]:
    """Translate the signed indicator vote into a bounded directional-bias display.

    These are visualization scores only, not calibrated market probabilities.
    """
    up = max(10, min(90, 50 + int(score) * 8))
    return up, 100 - up


def shared_learning_data() -> dict:
    try:
        with open("data/shared_learning.json", "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return {}

def update_shared_learning(conn: sqlite3.Connection, bot: str = "btc") -> None:
    rows=conn.execute("SELECT realized_pnl FROM paper_trades WHERE status='CLOSED' ORDER BY COALESCE(closed_at,opened_at) DESC,id DESC LIMIT 50").fetchall()
    count=len(rows)
    wins=sum(float(row["realized_pnl"] or 0)>0 for row in rows)
    losses=sum(float(row["realized_pnl"] or 0)<0 for row in rows)
    data=shared_learning_data()
    data[bot]={"sample":count,"wins":wins,"losses":losses,"win_rate":round(100*wins/count,1) if count else None,"updated_at":utc_now().isoformat()}
    data["updated_at"]=utc_now().isoformat()
    os.makedirs("data",exist_ok=True)
    with open("data/shared_learning.json","w",encoding="utf-8") as handle:
        json.dump(data,handle,indent=2)

def recent_paper_performance(conn: sqlite3.Connection, limit: int = 20) -> dict:
    """Summarize the latest fully closed simulated trades for conservative feedback."""
    rows = conn.execute(
        "SELECT realized_pnl FROM paper_trades WHERE status='CLOSED' "
        "ORDER BY COALESCE(closed_at,opened_at) DESC, id DESC LIMIT ?", (limit,)
    ).fetchall()
    count = len(rows)
    wins = sum(1 for row in rows if float(row["realized_pnl"] or 0.0) > 0)
    losses = sum(1 for row in rows if float(row["realized_pnl"] or 0.0) < 0)
    return {"count": count, "wins": wins, "losses": losses,
            "win_rate": (100.0 * wins / count) if count else None,
            "loss_rate": (100.0 * losses / count) if count else None}


def suggested_hold_window(minutes: int, score: int, flip: int) -> str:
    """A cautious review window, never a promise to hold until a target."""
    strength = abs(int(score))
    if strength < MIN_VOTES[minutes] or flip >= 60:
        return "WAIT · recheck in 30s"
    if minutes == 5:
        return "1–2 min · reassess every scan" if strength >= 4 and flip < 40 else "30–60s · reassess"
    return "3–5 min · reassess every scan" if strength >= 4 and flip < 40 else "1–2 min · reassess"


def scalp_fit_score(score: int, tech: dict, stats: dict) -> int:
    """Rank scalp conditions from indicators and liquidity; this is not a win rate."""
    side = "UP" if score > 0 else "DOWN"
    move1 = float(tech.get("move1", 0.0))
    move3 = float(tech.get("move3", 0.0))
    aligned = (move1 > 0 and move3 > 0) if side == "UP" else (move1 < 0 and move3 < 0)
    score_value = 35 + abs(int(score)) * 8
    score_value += 12 if aligned else -8
    score_value += 10 if float(stats.get("depth", 0)) >= 2000 else 0
    score_value += 8 if float(stats.get("spread", 1)) <= 0.015 else 0
    return max(0, min(95, int(score_value)))


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
    # No separate Discord entry post. The persistent dashboard displays the position.
    return True


def hold_and_flip(roi: float, score: int, position_side: str, imbalance: float) -> tuple[int, int]:
    aligned = score if position_side == "UP" else -score
    hold = max(0, min(100, 50 + aligned * 8 + int(imbalance * 15) + (8 if roi > 0 else -5 if roi < -0.04 else 0)))
    flip = max(0, min(100, 50 - aligned * 8 - int(imbalance * 15) + (15 if aligned < 0 else 0)))
    return hold, flip


# BTC Polymarket call-sign upgrade
# Paste this over the EXISTING manage_open_trades() function in btc_poly_bot_v2.py.
# It uses the bot's existing data, hold_and_flip(), SQLite fields and Discord sender.
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
        range_position=tech.get("range_position")
        if range_position is not None and ((trade["side"]=="UP" and range_position>=90) or (trade["side"]=="DOWN" and range_position<=10)):
            flip=min(100,flip+10); hold=max(0,hold-10)
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
    if not second_opinion(tech, stats)["agrees"]:
        return
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
    # Scalp setups are reflected in the dashboard, not sent as separate posts.
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




def second_opinion(tech: dict, stats: Optional[dict] = None) -> dict:
    """Independent conservative confirmation using short momentum and book pressure.

    This is a rule-based cross-check, not a trained model or a probability.
    It deliberately returns WAIT when the available evidence is mixed.
    """
    move1 = float(tech.get("move1", 0.0))
    move3 = float(tech.get("move3", 0.0))
    move10 = float(tech.get("move10", 0.0))
    score = 0
    score += 1 if move1 > 0.015 else -1 if move1 < -0.015 else 0
    score += 1 if move3 > 0.025 else -1 if move3 < -0.025 else 0
    score += 1 if move10 > 0.04 else -1 if move10 < -0.04 else 0
    if stats:
        imbalance = float(stats.get("imbalance", 0.0))
        score += 1 if imbalance > 0.12 else -1 if imbalance < -0.12 else 0
    range_position=tech.get("range_position")
    if range_position is not None:
        if range_position>=90: score-=1
        elif range_position<=10: score+=1
    side = "UP" if score >= 2 else "DOWN" if score <= -2 else "WAIT"
    primary = str(tech.get("side", "NEUTRAL"))
    agrees = side == primary and primary in ("UP", "DOWN")
    return {"side": side, "score": score, "agrees": agrees,
            "label": "CONFIRMED" if agrees else "DISAGREEMENT / WAIT"}


def _dashboard_embed(conn: sqlite3.Connection, tech: dict, opinions: dict) -> dict:
    """Build the branded BTC SIGNAL TERMINAL Discord dashboard."""
    open_rows = conn.execute(
        "SELECT timeframe,side,entry_price,remaining_quantity,trade_type,last_action "
        "FROM paper_trades WHERE status='OPEN' ORDER BY id DESC LIMIT 5"
    ).fetchall()
    pnl = daily_closed_pnl(conn)
    perf = recent_paper_performance(conn, 20)
    side = str(tech.get("side", "NEUTRAL"))
    second = str(opinions.get("side", "WAIT"))
    confirmed = bool(opinions.get("agrees"))
    safe_call = side if confirmed else "WAIT"
    score = int(tech.get("score", 0))
    up_bias, down_bias = direction_split(score)
    # Live, rule-based gauges respond to the current indicator score and momentum.
    # These are signal-strength/risk scores, never calibrated win probabilities.
    move1 = float(tech.get("move1", 0.0))
    move3 = float(tech.get("move3", 0.0))
    rsi_now = float(tech.get("rsi", 50.0))
    hold = max(5, min(95, 50 + abs(score) * 8 + (6 if move1 * score > 0 else -8)))
    flip = max(5, min(95, 100 - hold + (12 if rsi_now >= 72 or rsi_now <= 28 else 0)
                       + (10 if move1 * score < 0 else 0)))
    flip = min(95, flip)
    scalp_live = max(0, min(95, 35 + abs(score) * 8
                            + (12 if move1 * move3 > 0 and abs(move3) >= 0.035 else -5)
                            + (8 if abs(move1) >= 0.025 else 0)))
    scalp_call = ("SCALP CALL " + side) if side in ("UP", "DOWN") and scalp_live >= 65 and confirmed else "SCALP STANDBY"
    price = float(tech.get("last_price", 0.0))
    rsi_value = float(tech.get("rsi", 50.0))
    color = 0x19C37D if safe_call == "UP" else 0xEF4444 if safe_call == "DOWN" else 0xF59E0B
    call_icon = "🟢" if safe_call == "UP" else "🔴" if safe_call == "DOWN" else "🟡"
    action_label = (("UP CALL" if safe_call == "UP" else "DOWN CALL") + f" · {abs(score)*9+50}% strength") if confirmed else "WAIT · SIGNALS NOT ALIGNED"

    # Clock-based progress bar for elapsed time in the current 5-minute BTC
    # market window. Indicator gauges below remain separate heuristic scores.
    market_period_seconds = 5 * 60
    elapsed_seconds = int(time.time() % market_period_seconds)
    market_progress = int(elapsed_seconds * 100 / market_period_seconds)
    seconds_to_close = market_period_seconds - elapsed_seconds
    progress_bar = gauge_bar(market_progress, 20)
    countdown = f"{seconds_to_close // 60:02d}:{seconds_to_close % 60:02d}"
    if not confirmed:
        action_now = "WAIT — confirmation disagrees; no new paper entry"
    elif open_rows:
        action_now = str(open_rows[0]["last_action"] or "HOLD / MONITOR")
    else:
        action_now = f"{safe_call} BIAS — WAIT for entry filters; no forced entry"
    # Discord embeds support real color accents, an author line, thumbnail,
    # field columns, and a footer. Keep gauges as indicator scores, not odds.
    description = (
        f"### {call_icon}  {safe_call}  ·  {action_label}\n"
        f"**ACTION NOW:** {action_now}\n"
        f"**5M MARKET WINDOW**  {market_progress}%  `{progress_bar}`  ·  closes in **{countdown}**\n"
        f"**BTC / USD**  `${price:,.2f}`\n"
        f"`PRIMARY  {side}`  ·  `CHECK  {second}`  ·  "
        f"{'🟢 MATCH' if confirmed else '🟠 MISMATCH'}\n"
        f"*Last refresh <t:{int(time.time())}:R>*"
    )
    fields = [        {
            "name": "〢 MARKET BIAS",
            "value": (
                f"🟩 **UP  {up_bias:>2}%**  `{gauge_bar(up_bias, 18)}`\n"
                f"🟥 **DOWN {down_bias:>2}%**  `{gauge_bar(down_bias, 18)}`\n"
                "`Relative indicator vote · not win probability`"
            ),
            "inline": False,
        },
        {
            "name": "〢 MOMENTUM",
            "value": (
                f"**RSI** `{rsi_value:.1f}`\n"
                f"**1m** `{float(tech.get('move1',0)):+.3f}%`  "
                f"**3m** `{float(tech.get('move3',0)):+.3f}%`\n"
                f"**10m** `{float(tech.get('move10',0)):+.3f}%`  "
                f"**20-candle range** `{float(tech.get('range_position',50)):.0f}% from low`\n"
                f"**Votes** `{score:+d}/5`"
            ),
            "inline": True,
        },
        {
            "name": "〢 HOLD STRENGTH",
            "value": f"**{hold}%**\n`{gauge_bar(hold, 13)}`",
            "inline": True,
        },
        {
            "name": "〢 REVERSAL / FLIP RISK",
            "value": f"**{flip}%**\n`{gauge_bar(flip, 13)}`",
            "inline": True,
        },
    ]

    perf_text = (
        f"**{perf['wins']}W · {perf['losses']}L** 　`Win {perf['win_rate']:.0f}% · Loss {perf['loss_rate']:.0f}%`\n"
        if perf['count'] else "`Collecting completed paper trades`\n"
    )
    perf_text += f"Today P/L  **${pnl:+.2f}**\nOpen positions  **{len(open_rows)}**"
    fields.append({"name": "〢 PAPER PERFORMANCE", "value": perf_text, "inline": True})
    fields.append({
        "name": "〢 SCALP CALL",
        "value": f"**{scalp_call}** · **{scalp_live}%**\n`{gauge_bar(scalp_live, 13)}`\n`Live setup-strength score · not win odds`",
        "inline": True,
    })

    if open_rows:
        lines = []
        for row in open_rows:
            kind = "⚡ SCALP" if str(row["trade_type"]).upper() == "SCALP" else "📈 HOLD"
            lines.append(
                f"`{kind}` **{row['timeframe']}m {row['side']}**  ·  "
                f"Entry `${float(row['entry_price']):.3f}`  ·  "
                f"Qty `{float(row['remaining_quantity']):.3f}`\n"
                f"↳ {row['last_action'] or 'Monitoring position'}"
            )
        positions = "\n".join(lines)
    else:
        positions = "`NO OPEN POSITIONS`\nWatching for the next qualified setup."
    sell_call = "NO OPEN POSITION"
    if open_rows:
        latest_action = str(open_rows[0]["last_action"] or "").upper()
        if any(word in latest_action for word in ("SELL", "EXIT", "STOP", "FLIP")) or flip >= 75:
            sell_call = f"SELL / REDUCE · {max(flip, 75)}% sell-risk score"
        else:
            sell_call = f"HOLD / WATCH · {flip}% flip risk"
        positions = f"**SELL CALL: {sell_call}**\n" + positions
    if open_rows:
        entry_exit_text = (
            f"**POSITION:** {open_rows[0]['timeframe']}m {open_rows[0]['side']} · "
            f"paper entry ${float(open_rows[0]['entry_price']):.3f}\n"
            f"**NOW:** {open_rows[0]['last_action'] or 'HOLD / MONITOR'}\n"
            "**SELL / HOLD:** follow the live target, stop, and flip-risk action shown above."
        )
    else:
        entry_exit_text = (
            f"**ENTRY:** WAIT · current {safe_call} bias is not an entry by itself.\n"
            "**BUY-IN:** paper entry only when market, timing, edge, spread, depth, and confirmation filters pass.\n"
            "**SELL / HOLD:** no open paper position; wait for a qualified setup."
        )
    fields.append({"name": "〢 ENTRY / SELL / HOLD NOW", "value": entry_exit_text[:1024], "inline": False})
    fields.append({"name": "〢 ACTIVE PAPER POSITIONS", "value": positions[:1024], "inline": False})

    latest_scalp = conn.execute(
        "SELECT side,setup_score,entry_price,target_price,stop_price "
        "FROM scalp_signals ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if latest_scalp:
        scalp_score = max(0, min(100, int(latest_scalp["setup_score"] or 0)))
        scalp_text = (
            f"**{latest_scalp['side']}**  ·  Setup score **{scalp_score}/100**\n"
            f"`{gauge_bar(scalp_score, 18)}`\n"
            f"Entry `${float(latest_scalp['entry_price']):.3f}`  ·  "
            f"Target `${float(latest_scalp['target_price']):.3f}`  ·  "
            f"Stop `${float(latest_scalp['stop_price']):.3f}`"
        )
    else:
        scalp_text = "`SCALP RADAR: STANDBY`\nNo setup currently meets the momentum and liquidity filters."
    fields.append({"name": "〢 SCALP RADAR", "value": scalp_text, "inline": False})
    fields.append({
        "name": "〢 EXECUTION GUARDRAILS",
        "value": (
            "🛡️ Two-check disagreement = **WAIT**  ·  Simulated positions only\n"
            "📡 Coinbase 1-minute candles · 1-second pause between scan cycles; API time adds to cycle\n"
            "*Bars show rule-based indicator scores, not guaranteed outcomes.*"
        ),
        "inline": False,
    })
    return {
        "author": {
            "name": "BTC SIGNAL TERMINAL  /  MARKET INTELLIGENCE",
            "icon_url": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png",
        },
        "title": "━━━━━━━━  BTC / USD  ━━━━━━━━",
        "description": description,
        "color": color,
        "thumbnail": {"url": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png"},
        "fields": fields,
        "footer": {"text": "BTC SIGNAL TERMINAL  •  PAPER MODE  •  NO LIVE ORDERS"},
        "timestamp": utc_now().isoformat(),
    }

def write_btc_site_data(conn: sqlite3.Connection, tech: dict, opinions: dict) -> None:
    """Publish BTC bot snapshot separately from the multi-crypto scanner data."""
    perf=recent_paper_performance(conn,50)
    scalp=conn.execute("SELECT side,setup_score,entry_price,target_price,stop_price FROM scalp_signals ORDER BY id DESC LIMIT 1").fetchone()
    side=str(tech.get("side","WAIT")); second=str(opinions.get("side","WAIT"))
    safe_side=side if opinions.get("agrees") and side in ("UP","DOWN") else "WAIT"
    score=int(tech.get("score",0)); hold=max(0,min(100,50+abs(score)*8)); flip=100-hold
    markets=[]
    for tf in TIMEFRAMES:
        markets.append({"asset":"BTC","timeframe":tf,"side":safe_side,"primary_side":side,"second_brain":second,
            "action":"CALL "+safe_side if safe_side in ("UP","DOWN") else "SIT OUT / WAIT",
            "score":score,"risk":flip,"risk_label":"HIGH" if flip>=70 else "MODERATE" if flip>=45 else "LOW",
            "rsi":float(tech.get("rsi",50)),"move1_pct":float(tech.get("move1",0)),
            "move3_pct":float(tech.get("move3",0)),"move10_pct":float(tech.get("move10",0)),
            "range_position":float(tech.get("range_position",50)),"scalp":"See BTC bot Discord dashboard for active scalp details."})
    data={"updated_at":utc_now().isoformat(),"paper_only":True,
          "summary":{"resolved_sample":perf["count"],"wins":perf["wins"],"losses":perf["losses"],
          "win_rate":perf["win_rate"],"loss_rate":perf["loss_rate"],
          "realized_pnl":daily_closed_pnl(conn),"open_positions":conn.execute("SELECT COUNT(*) FROM paper_trades WHERE status='OPEN'").fetchone()[0]},
          "markets":markets,"scalp":dict(scalp) if scalp else None,
          "disclaimer":"BTC bot signal is rule-based paper guidance, not guaranteed prediction."}
    os.makedirs("docs",exist_ok=True)
    with open("docs/data_btc.json","w",encoding="utf-8") as handle: json.dump(data,handle,indent=2)

def update_dashboard(conn: sqlite3.Connection, tech: dict, opinions: dict) -> None:
    """Create once, then edit the same Discord webhook message on a safe cadence."""
    write_btc_site_data(conn, tech, opinions)
    last_update_row = conn.execute(
        "SELECT state_value FROM bot_state WHERE state_key='dashboard_last_update'"
    ).fetchone()
    try:
        last_update = float(last_update_row["state_value"]) if last_update_row else 0.0
    except (TypeError, ValueError):
        last_update = 0.0
    if time.time() - last_update < DISCORD_DASHBOARD_UPDATE_SECONDS:
        return
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook:
        print("Discord dashboard skipped: DISCORD_WEBHOOK_URL secret is missing.")
        return
    if not (webhook.startswith("https://discord.com/api/webhooks/") or
            webhook.startswith("https://discordapp.com/api/webhooks/")):
        print("Discord dashboard skipped: webhook secret is not a valid Discord webhook URL.")
        return
    row = conn.execute("SELECT state_value FROM bot_state WHERE state_key='dashboard_message_id'").fetchone()
    message_id = row["state_value"] if row else ""
    payload = {"embeds":[_dashboard_embed(conn, tech, opinions)],"allowed_mentions":{"parse":[]}}
    # Record the attempt time too, so a Discord rate-limit/error cannot cause
    # repeated requests on every 1-second scanner cycle.
    conn.execute(
        "INSERT INTO bot_state(state_key,state_value) VALUES('dashboard_last_update',?) "
        "ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value",
        (str(time.time()),),
    )
    conn.commit()
    try:
        if message_id:
            url = webhook.rstrip("/") + "/messages/" + message_id
            response = SESSION.patch(url, json=payload, timeout=HTTP_TIMEOUT)
            if response.status_code == 404:
                print("Discord dashboard message was not found; creating a replacement.")
                message_id = ""
            else:
                response.raise_for_status()
                conn.execute(
                    "INSERT INTO bot_state(state_key,state_value) VALUES('dashboard_last_update',?) "
                    "ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value",
                    (str(time.time()),),
                )
                conn.commit()
                print("Discord dashboard updated successfully.")
                return
        response = SESSION.post(webhook.rstrip("/") + "?wait=true", json=payload, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        result = response.json()
        new_id = str(result.get("id", ""))
        if new_id:
            conn.execute("INSERT INTO bot_state(state_key,state_value) VALUES('dashboard_message_id',?) ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value", (new_id,))
            conn.execute(
                "INSERT INTO bot_state(state_key,state_value) VALUES('dashboard_last_update',?) "
                "ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value",
                (str(time.time()),),
            )
            conn.commit()
            print("Discord dashboard posted successfully; message ID saved.")
        else:
            print("Discord dashboard POST succeeded, but Discord returned no message ID.")
    except Exception as exc:
        print(f"Discord dashboard update failed: {type(exc).__name__}: {exc}")


def update_btc_channel_message(conn: sqlite3.Connection, webhook_env: str, state_prefix: str, embed: dict, interval: int) -> None:
    """Create a channel message once, then edit it at a controlled cadence."""
    webhook = os.getenv(webhook_env, "").strip()
    if not webhook:
        return
    if not (webhook.startswith("https://discord.com/api/webhooks/") or
            webhook.startswith("https://discordapp.com/api/webhooks/")):
        print(f"{state_prefix} skipped: invalid Discord webhook URL.")
        return
    last_key = state_prefix + "_last_update"
    message_key = state_prefix + "_message_id"
    row = conn.execute("SELECT state_value FROM bot_state WHERE state_key=?", (last_key,)).fetchone()
    try:
        last_update = float(row["state_value"]) if row else 0.0
    except (TypeError, ValueError):
        last_update = 0.0
    if time.time() - last_update < interval:
        return
    row = conn.execute("SELECT state_value FROM bot_state WHERE state_key=?", (message_key,)).fetchone()
    message_id = row["state_value"] if row else ""
    payload = {"embeds": [embed], "allowed_mentions": {"parse": []}}
    conn.execute(
        "INSERT INTO bot_state(state_key,state_value) VALUES(?,?) "
        "ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value",
        (last_key, str(time.time())),
    )
    conn.commit()
    try:
        if message_id:
            response = SESSION.patch(webhook.rstrip("/") + "/messages/" + message_id,
                                    json=payload, timeout=HTTP_TIMEOUT)
            if response.status_code != 404:
                response.raise_for_status()
                print(f"{state_prefix} Discord message updated.")
                return
        response = SESSION.post(webhook.rstrip("/") + "?wait=true", json=payload, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        new_id = str(response.json().get("id", ""))
        if new_id:
            conn.execute(
                "INSERT INTO bot_state(state_key,state_value) VALUES(?,?) "
                "ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value",
                (message_key, new_id),
            )
            conn.commit()
            print(f"{state_prefix} Discord message created; ID saved.")
    except Exception as exc:
        print(f"{state_prefix} Discord update failed: {type(exc).__name__}: {exc}")


def update_btc_live_channel(conn: sqlite3.Connection, tech: dict, opinions: dict) -> None:
    """Mirror the live BTC terminal into a dedicated Discord channel."""
    embed = _dashboard_embed(conn, tech, opinions)
    update_btc_channel_message(conn, "DISCORD_BTC_LIVE_WEBHOOK_URL",
                               "btc_live_dashboard", embed, DISCORD_DASHBOARD_UPDATE_SECONDS)


def update_btc_chart_channel(conn: sqlite3.Connection, candles: list[list[float]], tech: dict) -> None:
    """Update one Discord message with a rolling 60-minute chart image."""
    if not candles:
        return
    recent = candles[-60:]
    closes = [round(float(c[4]), 2) for c in recent]
    times = [datetime.fromtimestamp(float(c[0]), timezone.utc).strftime("%H:%M") for c in recent]
    latest = closes[-1]
    change = ((latest / closes[0]) - 1.0) * 100.0 if closes[0] else 0.0
    config = {
        "type": "line",
        "data": {"labels": times, "datasets": [{
            "label": "BTC/USD", "data": closes,
            "borderColor": "#2ecc71" if change >= 0 else "#e74c3c",
            "borderWidth": 2, "pointRadius": 0, "fill": False, "tension": 0.15
        }]},
        "options": {
            "plugins": {"legend": {"display": False}},
            "scales": {"x": {"ticks": {"maxTicksLimit": 8}},
                       "y": {"title": {"display": True, "text": "BTC price (USD)"}}}
        }
    }
    chart_url = "https://quickchart.io/chart?width=900&height=420&format=png&backgroundColor=white&c=" + urllib.parse.quote(json.dumps(config), safe="")
    embed = {
        "title": "BTC LIVE PRICE GRAPH",
        "description": (
            f"BTC {latest:,.2f} USD · Rolling 60m: {change:+.2f}%\n"
            f"Scanner: {tech.get('side', 'WAIT')} · RSI {float(tech.get('rsi', 50)):.1f}\n"
            "1-minute candles · refreshes about every 30 seconds"
        ),
        "color": 0x19C37D if change >= 0 else 0xEF4444,
        "image": {"url": chart_url},
        "footer": {"text": "BTC price visualization · paper trading only"},
        "timestamp": utc_now().isoformat()
    }
    update_btc_channel_message(conn, "DISCORD_BTC_CHART_WEBHOOK_URL",
                               "btc_live_chart", embed, 30)


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
    opinions = second_opinion(tech, stats)
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
    if not opinions["agrees"]:
        reasons.append(f"second opinion {opinions['side']} does not confirm primary {side}")
    # Learn conservatively from completed paper trades: poor recent results can
    # tighten entries, but strong results never loosen the original safety rules.
    performance = recent_paper_performance(conn, 20)
    if performance["count"] >= 10 and performance["win_rate"] < 60.0:
        reasons.append("recent paper results below 60%; adaptive safety pause")
    peer=shared_learning_data().get("multi",{})
    if peer.get("sample",0)>=10 and peer.get("win_rate") is not None and peer.get("win_rate")<45:
        reasons.append("shared learning caution: multi-asset bot recent win rate below 45%")
    if daily_closed_pnl(conn) <= -MAX_DAILY_PAPER_LOSS_USD:
        reasons.append("daily paper loss limit reached")
    if entries_today(conn) >= MAX_DAILY_ENTRIES:
        reasons.append("daily paper entry cap reached")
    if open_trade_count(conn) >= MAX_OPEN_PAPER_TRADES:
        reasons.append("maximum open paper positions reached")
    print(
        f"BTC {minutes}m {side} | votes {tech['score']}/5 | RSI {tech['rsi']:.1f} "
        f"| 3m {tech['move3']:+.3f}% | 10m {tech['move10']:+.3f}% "
        f"| ask {ask:.3f} bid {stats['bid']:.3f} depth ${stats['depth']:.0f} "
        f"| edge proxy {edge*100:+.1f}pp | {remaining}s left"
    )
    # Compact visual call: directional bias, hold/flip gauges, scalp fit and hold window.
    signal_hold, signal_flip = hold_and_flip(0.0, int(tech["score"]), side, stats["imbalance"])
    range_position=tech.get("range_position")
    if range_position is not None and ((side=="UP" and range_position>=90) or (side=="DOWN" and range_position<=10)):
        signal_flip=min(100,signal_flip+10); signal_hold=max(0,signal_hold-10)
    up_bias, down_bias = direction_split(int(tech["score"]))
    scalp_fit = scalp_fit_score(int(tech["score"]), tech, stats) if minutes == 5 else 0
    hold_window = suggested_hold_window(minutes, int(tech["score"]), signal_flip)
    performance_line = (
        f"📚 LAST {performance['count']} PAPER: {performance['wins']}W/{performance['losses']}L · "
        f"Win {performance['win_rate']:.0f}% · Loss {performance['loss_rate']:.0f}%"
        if performance["count"] else "📚 PAPER TRACK RECORD: building sample"
    )
    if reasons:
        action = "⏸ WAIT" if side in ("UP", "DOWN") else "⚪ NO TRADE"
        entry_line = "🚫 <b>NO ENTRY</b> · " + "; ".join(reasons)
    else:
        action = "🟢 BUY PAPER" if side == "UP" else "🔴 BUY PAPER"
        entry_line = "✅ <b>ENTRY FILTERS PASS</b> · paper entry eligible"
    signal_key = f"signal_v4:{slug}:{minutes}:{side}"
    message = (
        f"<b>₿ BTC {minutes}m  |  {action}</b>\n"
        f"<b>{'🟢 UP' if side == 'UP' else '🔴 DOWN'} BIAS</b>  {up_bias}% / {down_bias}%\n"
        f"<code>UP   {gauge_bar(up_bias, 10)}\nDOWN {gauge_bar(down_bias, 10)}</code>\n\n"
        f"{gauge_line('HOLD', signal_hold, '🟢')}\n"
        f"{gauge_line('FLIP RISK', signal_flip, '🟠')}\n"
        f"⚡ SCALP FIT: <b>{scalp_fit}/100</b>  <code>{gauge_bar(scalp_fit)}</code>\n"
        f"⏱ HOLD WINDOW: <b>{hold_window}</b>\n\n"
        f"VOTES {abs(int(tech['score']))}/5 · RSI {tech['rsi']:.0f}\n"
        f"3m {tech['move3']:+.2f}% · 10m {tech['move10']:+.2f}% · 20-candle range {float(tech.get('range_position',50)):.0f}% from low\n"
        f"ASK ${ask:.3f} · BID ${stats['bid']:.3f} · DEPTH ${stats['depth']:.0f}\n"
        f"EDGE {edge*100:+.1f}pp\n"
        f"SECOND BRAIN: {opinions['side']} · {opinions['label']}\n"
        f"{entry_line}\n"
        f"{performance_line}\n"
        "<i>Paper only · bars are indicator scores, not win probabilities. Hold window is a review suggestion.</i>"
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
    parser.add_argument("--duration", type=int, default=0, help="Run for this many seconds; 0 means continuous scanning")
    args = parser.parse_args()
    if args.duration < 0:
        parser.error("--duration must be zero or a positive number")
    conn = db_connect()
    print("BTC signal desk starting in paper-only dashboard mode.")
    started = time.time()
    try:
        while True:
            try:
                candles = get_candles()
                tech = technical_signal(candles)
                manage_open_trades(conn, tech)
                scan_scalp(conn, tech)
                dashboard_opinion = second_opinion(tech)
                for minutes in TIMEFRAMES:
                    try:
                        scan_one(conn, minutes, tech)
                    except Exception as exc:
                        print(f"BTC {minutes}m scan error: {type(exc).__name__}: {exc}")
                update_dashboard(conn, tech, dashboard_opinion)
                update_btc_live_channel(conn, tech, dashboard_opinion)
                update_btc_chart_channel(conn, candles, tech)
                update_shared_learning(conn)
                print_summary(conn)
            except Exception as exc:
                print(f"Scan cycle error: {type(exc).__name__}: {exc}")
                print("Scan error logged; scanner will retry signal delivery on the next cycle.")
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
