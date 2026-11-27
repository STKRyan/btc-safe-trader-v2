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
# RISK-FIRST MODE: fewer, stricter paper calls; protection beats activity.
MIN_VOTES = {5: 3, 15: 3}  # majority of five directional checks; second brain/risk gates still apply
SCAN_INTERVAL_SECONDS = 1
DISCORD_DASHBOARD_UPDATE_SECONDS = 5  # keep Discord message edits within rate limits
SIGNAL_REPEAT_SECONDS = 30  # keep active paper calls updating without a long quiet period
PAPER_BANKROLL_USD = 100.00
PAPER_STAKE_USD = 1.00
MAX_DAILY_PAPER_LOSS_USD = 4.00
MAX_DAILY_ENTRIES = 3
MAX_OPEN_PAPER_TRADES = 1
MIN_BOOK_DEPTH_USD = 1500.00
MAX_SPREAD = 0.025
MIN_SECONDS_REMAINING = 45
MAX_SECONDS_REMAINING_FRACTION = 0.98  # permit high-quality entries about 5–45s after a new 5m/15m window opens
MAX_ENTRY_ASK = 0.75
MIN_EDGE_PROXY = 0.08
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
    """Send a compact, color-accented BTC signal card to Discord."""
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
    if not webhook:
        print("DISCORD_WEBHOOK_URL is missing; Discord alert was not sent.")
        return False
    if not (webhook.startswith("https://discord.com/api/webhooks/") or
            webhook.startswith("https://discordapp.com/api/webhooks/")):
        print("DISCORD_WEBHOOK_URL does not look like a Discord webhook URL.")
        return False
    text = message
    text = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*b\s*>", "**", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*/\s*b\s*>", "**", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*i\s*>", "*", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*/\s*i\s*>", "*", text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*code\s*>", chr(96), text, flags=re.IGNORECASE)
    text = re.sub(r"<\s*/\s*code\s*>", chr(96), text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text).strip()
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    headline = re.sub(r"\*\*", "", lines[0] if lines else "BTC SIGNAL UPDATE").strip()[:220]
    upper = text.upper()
    status_text = " ".join(lines[:2]).upper()
    if any(word in status_text for word in ("SELL / REDUCE","PROTECT / EXIT","EXIT RISK","STOP LOSS")):
        color,badge=0xEF4444,"🔴 EXIT / PROTECT"
    elif any(word in status_text for word in ("SELL 50%","TAKE PROFIT","TAKE-PROFIT")):
        color,badge=0xF59E0B,"🟠 TAKE PROFIT"
    elif re.search(r"\bFLIP(?:PED)?\b", status_text):
        color,badge=0xF59E0B,"🟠 FLIP"
    elif "WAIT" in status_text or "NO ENTRY" in status_text or "NO MARKET" in status_text:
        color,badge=0x8794A8,"⚪ WAIT"
    elif "HOLD" in status_text:
        color,badge=0x14B8A6,"🩵 HOLD"
    elif re.search(r"\bDOWN\b",status_text):
        color,badge=0xEF4444,"🔴 DOWN"
    elif re.search(r"\bUP\b",status_text):
        color,badge=0x19C37D,"🟢 UP"
    else:
        color,badge=0x4BD9FF,"📡 UPDATE"

    def pick(terms, limit=1):
        found=[]
        for line in lines[1:]:
            clean=line.replace("**","").strip()
            if any(term in clean.upper() for term in terms):
                if clean not in found:
                    found.append(clean)
                if len(found)>=limit: break
        return found
    bias=pick(["BIAS"])
    meters=pick(["HOLD ","FLIP RISK","SCALP FIT"],3)
    market=pick(["ASK $","EDGE "],2)
    confirm=pick(["SECOND BRAIN:","ENTRY FILTERS","NO ENTRY"],2)
    fields=[]
    if bias: fields.append({"name":"DIRECTION","value":bias[0][:250],"inline":False})
    if meters: fields.append({"name":"LIVE METERS","value":"\n".join(meters)[:700],"inline":False})
    if market: fields.append({"name":"MARKET","value":"\n".join(market)[:500],"inline":True})
    if confirm: fields.append({"name":"CHECK","value":"\n".join(confirm)[:500],"inline":True})
    payload={
        "embeds":[{
            "title":f"{badge} · {headline or 'BTC SIGNAL'}",
            "description":"**LIVE PAPER SIGNAL**\nUpdated with the latest scan. See the persistent dashboard for the full meters.",
            "color":color,
            "fields":fields[:4],
            "footer":{"text":"BTC signal desk · paper only · no live orders"},
            "timestamp":utc_now().isoformat()
        }],
        "allowed_mentions":{"parse":[]}
    }
    try:
        response = SESSION.post(webhook, json=payload, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return True
    except Exception as exc:
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
            closed_at TEXT,
            entry_features TEXT NOT NULL DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS alerts (
            alert_key TEXT PRIMARY KEY,
            sent_at REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS btc_snipe_ticks (
            market_slug TEXT NOT NULL,
            timeframe INTEGER NOT NULL,
            side TEXT NOT NULL,
            ask REAL NOT NULL,
            ts REAL NOT NULL,
            PRIMARY KEY(market_slug,timeframe,side)
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
        CREATE TABLE IF NOT EXISTS btc_snipe_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id TEXT NOT NULL,
            market_slug TEXT NOT NULL,
            timeframe INTEGER NOT NULL,
            side TEXT NOT NULL,
            entry_price REAL NOT NULL,
            features TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'OPEN',
            outcome TEXT,
            winning_side TEXT,
            created_at TEXT NOT NULL,
            last_check REAL NOT NULL DEFAULT 0,
            resolved_at TEXT,
            UNIQUE(market_id,timeframe,side)
        );
        CREATE TABLE IF NOT EXISTS btc_loss_reviews (
            trade_id INTEGER PRIMARY KEY,
            timeframe INTEGER NOT NULL,
            trade_type TEXT NOT NULL,
            tags TEXT NOT NULL DEFAULT '[]',
            reviewed_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS btc_forecasts (
            market_id TEXT NOT NULL,
            market_slug TEXT NOT NULL,
            timeframe INTEGER NOT NULL,
            initial_side TEXT NOT NULL,
            current_side TEXT NOT NULL,
            pending_side TEXT NOT NULL DEFAULT '',
            pending_since REAL NOT NULL DEFAULT 0,
            flip_count INTEGER NOT NULL DEFAULT 0,
            features TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'OPEN',
            outcome TEXT,
            winning_side TEXT,
            created_at TEXT NOT NULL,
            last_seen REAL NOT NULL,
            last_check REAL NOT NULL DEFAULT 0,
            resolved_at TEXT,
            PRIMARY KEY(market_id, timeframe)
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
        "entry_features": "TEXT NOT NULL DEFAULT '{}'",
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
    """Send a signal alert once per market/side; the live dashboard handles frequent updates."""
    # Keep the channel clean: the persistent dashboard is edited in place every
    # few seconds. Alerts are only posted once for each unique market/timeframe/
    # side (or WAIT/order-book state), rather than repeating on a timer.
    if not (key.startswith("signal_v4:") or
            (key.startswith("signal_v3:") and key.endswith(":WAIT")) or
            ":NO_BOOK:" in key):
        return
    row = conn.execute("SELECT sent_at FROM alerts WHERE alert_key=?", (key,)).fetchone()
    if row:
        return
    if discord_send(message):
        conn.execute(
            "INSERT INTO alerts(alert_key,sent_at) VALUES(?,?) "
            "ON CONFLICT(alert_key) DO UPDATE SET sent_at=excluded.sent_at",
            (key, time.time()),
        )
        conn.commit()
        print(f"Discord BTC paper signal sent once: {key}")

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


def daily_closed_pnl(conn: sqlite3.Connection, timeframe: Optional[int] = None) -> float:
    today = utc_now().date().isoformat()
    query = (
        "SELECT COALESCE(SUM(realized_pnl),0) AS pnl FROM paper_trades "
        "WHERE substr(COALESCE(closed_at,opened_at),1,10)=?"
    )
    params: list[Any] = [today]
    if timeframe is not None:
        query += " AND timeframe=?"
        params.append(int(timeframe))
    row = conn.execute(query, params).fetchone()
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

def btc_trade_tracker(conn: sqlite3.Connection, timeframe: Optional[int] = None,
                       trade_type: Optional[str] = None, limit: int = 100) -> dict:
    """Realized paper-trade W/L and streak stats; no forecast-only outcomes."""
    query = "SELECT realized_pnl FROM paper_trades WHERE status='CLOSED'"
    params: list[Any] = []
    if timeframe is not None:
        query += " AND timeframe=?"
        params.append(int(timeframe))
    if trade_type is not None:
        query += " AND UPPER(trade_type)=?"
        params.append(str(trade_type).upper())
    query += " ORDER BY COALESCE(closed_at,opened_at) DESC,id DESC LIMIT ?"
    params.append(max(1, int(limit)))
    rows = conn.execute(query, params).fetchall()
    pnls = [float(row["realized_pnl"] or 0.0) for row in rows]
    wins = sum(p > 0 for p in pnls)
    losses = sum(p < 0 for p in pnls)
    flats = len(pnls) - wins - losses
    decisive = wins + losses
    current_wins = current_losses = 0
    for pnl in pnls:
        if pnl > 0 and current_losses == 0:
            current_wins += 1
        elif pnl < 0 and current_wins == 0:
            current_losses += 1
        else:
            break
    best_wins = run = 0
    # Compute the best streak across all closed trades in chronological order.
    all_query = "SELECT realized_pnl FROM paper_trades WHERE status='CLOSED'"
    all_params: list[Any] = []
    if timeframe is not None:
        all_query += " AND timeframe=?"
        all_params.append(int(timeframe))
    if trade_type is not None:
        all_query += " AND UPPER(trade_type)=?"
        all_params.append(str(trade_type).upper())
    all_query += " ORDER BY COALESCE(closed_at,opened_at),id"
    for row in conn.execute(all_query, all_params).fetchall():
        if float(row["realized_pnl"] or 0.0) > 0:
            run += 1
            best_wins = max(best_wins, run)
        else:
            run = 0
    return {"sample":len(pnls),"wins":wins,"losses":losses,"flats":flats,
            "win_rate":100.0*wins/decisive if decisive else None,
            "loss_rate":100.0*losses/decisive if decisive else None,
            "current_win_streak":current_wins,"current_loss_streak":current_losses,
            "best_win_streak":best_wins,"net_pnl":sum(pnls)}


def btc_streak_guard(conn: sqlite3.Connection, timeframe: int,
                      trade_type: Optional[str] = None) -> dict:
    """Protect paper entry streaks; forecast recovery is independent of trade entry."""
    trades = btc_trade_tracker(conn, timeframe=timeframe, trade_type=trade_type, limit=100)
    forecasts = conn.execute(
        "SELECT outcome FROM btc_forecasts WHERE timeframe=? AND status='RESOLVED' "
        "ORDER BY resolved_at DESC LIMIT 12", (int(timeframe),)
    ).fetchall()
    forecast_wins = 0
    for row in forecasts:
        if row["outcome"] in ("WIN", "FLIP"):
            forecast_wins += 1
        else:
            break
    return {"trades":trades,"forecast_recovery_wins":forecast_wins}


def btc_loss_diagnosis(conn: sqlite3.Connection, timeframe: Optional[int] = None,
                         trade_type: Optional[str] = None, limit: int = 100) -> str:
    """Explain recent realized paper losses using entry snapshots and exit labels."""
    query = "SELECT realized_pnl,last_action,entry_features FROM paper_trades WHERE status='CLOSED' AND realized_pnl<0"
    params: list[Any] = []
    if timeframe is not None:
        query += " AND timeframe=?"
        params.append(int(timeframe))
    if trade_type is not None:
        query += " AND UPPER(trade_type)=?"
        params.append(str(trade_type).upper())
    query += " ORDER BY COALESCE(closed_at,opened_at) DESC,id DESC LIMIT ?"
    params.append(max(1, int(limit)))
    rows = conn.execute(query, params).fetchall()
    if not rows:
        return "Loss diagnosis: no closed losing paper trades yet"
    exits: dict[str,int] = {}
    tags: dict[str,int] = {}
    recorded = 0
    for row in rows:
        action = str(row["last_action"] or "").upper()
        exit_tag = "STOP" if "STOP" in action else "FLIP" if "FLIP" in action else "TIME" if "TIME" in action else "OTHER"
        exits[exit_tag] = exits.get(exit_tag,0)+1
        try:
            features = json.loads(row["entry_features"] or "{}")
            if not isinstance(features,dict) or not features:
                continue
            recorded += 1
        except Exception:
            continue
        score = abs(int(features.get("score",0) or 0))
        flip = int(features.get("flip_risk",0) or 0)
        spread = float(features.get("spread",0) or 0)
        depth = float(features.get("depth",0) or 0)
        ask = float(features.get("ask",0) or 0)
        move1 = float(features.get("move1",0) or 0)
        move3 = float(features.get("move3",0) or 0)
        range_pos = float(features.get("range_position",50) or 50)
        checks = [
            ("weak votes <4",score<4),
            ("high flip risk ≥65",flip>=65),
            ("wide spread >1.5¢",spread>0.015),
            ("thin depth <$2k",depth<2000),
            ("expensive entry >$0.65",ask>0.65),
            ("1m/3m momentum conflict",move1*move3<=0),
            ("near range extreme",range_pos>=85 or range_pos<=15),
            ("second brain disagreed",features.get("second_brain_agrees") is False),
        ]
        for label,matched in checks:
            if matched:
                tags[label]=tags.get(label,0)+1
    exit_text=" · ".join(f"{key} {value}" for key,value in sorted(exits.items(),key=lambda kv:-kv[1])[:3])
    if recorded:
        tag_text=" · ".join(f"{key} {value}/{recorded}" for key,value in sorted(tags.items(),key=lambda kv:-kv[1])[:3])
        return f"Losses n={len(rows)} · exits: {exit_text}\nEntry patterns (may overlap, n={recorded}): {tag_text or 'no repeated filter pattern'}"
    return f"Losses n={len(rows)} · exits: {exit_text}\nEntry details: older trades have no saved feature snapshot; new trades will be analyzed."


def btc_forecast_score_text(conn: sqlite3.Connection) -> str:
    rows=conn.execute("SELECT outcome FROM btc_forecasts WHERE status='RESOLVED' AND outcome IN ('WIN','FLIP','LOSS') ORDER BY resolved_at ASC,market_id ASC,timeframe ASC").fetchall()
    wins=sum(r["outcome"]=="WIN" for r in rows); flips=sum(r["outcome"]=="FLIP" for r in rows); losses=sum(r["outcome"]=="LOSS" for r in rows)
    current=best=0
    for r in rows:
        if r["outcome"] in ("WIN","FLIP"): current+=1; best=max(best,current)
        else: current=0
    saved=conn.execute("SELECT state_value FROM bot_state WHERE state_key='btc_forecast_best_streak'").fetchone()
    best=max(best,int(saved["state_value"]) if saved else 0)
    conn.execute("INSERT INTO bot_state(state_key,state_value) VALUES('btc_forecast_best_streak',?) ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value",(str(best),)); conn.commit()
    n=len(rows); rate=100*(wins+flips)/n if n else None
    return (f"FORECAST W/F/L **{wins}/{flips}/{losses}** · correct {rate:.1f}% · n={n}\n🔥 Current **{current}** · 🏆 Record **{best}**") if n else "Forecast W/F/L · waiting for first settled market"


def btc_snipe_score_text(conn: sqlite3.Connection) -> str:
    rows=conn.execute("SELECT outcome FROM btc_snipe_journal WHERE status='RESOLVED' AND outcome IN ('WIN','FLIP','LOSS') ORDER BY resolved_at ASC,id ASC").fetchall()
    wins=sum(r["outcome"]=="WIN" for r in rows); flips=sum(r["outcome"]=="FLIP" for r in rows); losses=sum(r["outcome"]=="LOSS" for r in rows)
    current=best=0
    for r in rows:
        if r["outcome"] in ("WIN","FLIP"): current+=1; best=max(best,current)
        else: current=0
    saved=conn.execute("SELECT state_value FROM bot_state WHERE state_key='btc_snipe_best_streak'").fetchone()
    best=max(best,int(saved["state_value"]) if saved else 0)
    conn.execute("INSERT INTO bot_state(state_key,state_value) VALUES('btc_snipe_best_streak',?) ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value",(str(best),)); conn.commit()
    n=len(rows); rate=100*(wins+flips)/n if n else None
    return (f"SNIPE W/F/L **{wins}/{flips}/{losses}** · correct {rate:.1f}% · n={n}\n🎯 Current **{current}** · 🏆 Record **{best}**") if n else "Snipe W/F/L · waiting for first settled snipe"


def btc_tracker_text(conn: sqlite3.Connection) -> str:
    five = btc_trade_tracker(conn, timeframe=5, limit=1000000)
    fifteen = btc_trade_tracker(conn, timeframe=15, limit=1000000)
    snipes = btc_trade_tracker(conn, timeframe=5, trade_type="SCALP", limit=1000000)
    def rate(stats: dict) -> str:
        return f"{stats['win_rate']:.0f}%W/{stats['loss_rate']:.0f}%L" if stats["win_rate"] is not None else "building sample"
    def record(stats: dict) -> str:
        return f"{stats['wins']}W/{stats['losses']}L · {rate(stats)} · n={stats['sample']}"
    return (
        f"BTC 5M: {record(five)}\nBTC 15M: {record(fifteen)}\n"
        f"⚡ SNIPE/SCALP 5M: {record(snipes)}\n"
        f"🔥 STREAK 5M: {five['current_win_streak']}W/{five['current_loss_streak']}L · best {five['best_win_streak']}\n"
        f"STREAK 15M: {fifteen['current_win_streak']}W/{fifteen['current_loss_streak']}L · best {fifteen['best_win_streak']}\n"
        f"SNIPE STREAK: {snipes['current_win_streak']}W/{snipes['current_loss_streak']}L · best {snipes['best_win_streak']}\n"
        f"{btc_forecast_score_text(conn)}\n{btc_snipe_score_text(conn)}"
    )


def btc_saved_loss_review_text(conn: sqlite3.Connection, minutes: int) -> str:
    rows=conn.execute("SELECT tags FROM btc_loss_reviews WHERE timeframe=? ORDER BY reviewed_at DESC LIMIT 20",(int(minutes),)).fetchall()
    tally={}
    for row in rows:
        try: tags=json.loads(row["tags"] or "[]")
        except Exception: tags=[]
        for tag in set(tags): tally[tag]=tally.get(tag,0)+1
    top=sorted(tally.items(),key=lambda kv:-kv[1])[:3]
    return f"n={len(rows)} · "+" · ".join(f"{tag} {count}/20" for tag,count in top) if rows else "building sample"


def btc_loss_report_text(conn: sqlite3.Connection) -> str:
    return (
        f"BTC 5M · {btc_loss_diagnosis(conn,5)[:280]}\n"
        f"BTC 15M · {btc_loss_diagnosis(conn,15)[:220]}\n"
        f"⚡ SNIPE 5M · {btc_loss_diagnosis(conn,5,'SCALP')[:280]}\n"
        f"🧠 SAVED LOSS PATTERNS · 5M {btc_saved_loss_review_text(conn,5)}"
    )


def recent_paper_performance(conn: sqlite3.Connection, limit: int = 20,
                             timeframe: Optional[int] = None) -> dict:
    """Summarize recent closed paper trades, optionally for one timeframe."""
    query = "SELECT realized_pnl FROM paper_trades WHERE status='CLOSED'"
    params: list[Any] = []
    if timeframe is not None:
        query += " AND timeframe=?"
        params.append(int(timeframe))
    query += " ORDER BY COALESCE(closed_at,opened_at) DESC, id DESC LIMIT ?"
    params.append(int(limit))
    rows = conn.execute(query, params).fetchall()
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
                       token_id: str, ask: float, trade_type: str = "STANDARD",
                       entry_features: Optional[dict] = None) -> bool:
    if ask <= 0 or ask >= 1:
        return False
    quantity = PAPER_STAKE_USD / ask
    conn.execute(
        "INSERT INTO paper_trades(market_slug,timeframe,side,token_id,entry_price,quantity,remaining_quantity,opened_at,trade_type,entry_features) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (market.get("slug", ""), minutes, side, token_id, ask, quantity, quantity,
         utc_now().isoformat(), trade_type,
         json.dumps(entry_features or {}, separators=(",",":"), default=str)),
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



def record_btc_snipe(conn: sqlite3.Connection, market: dict, minutes: int,
                       side: str, entry: float, features: dict) -> None:
    """Save every qualified snipe once, whether or not a paper position opens."""
    market_id=str(market.get("id") or "")
    if not market_id or side not in ("UP","DOWN") or not 0<float(entry)<1: return
    conn.execute("INSERT OR IGNORE INTO btc_snipe_journal(market_id,market_slug,timeframe,side,entry_price,features,created_at) VALUES(?,?,?,?,?,?,?)",
        (market_id,str(market.get("slug") or market_slug(minutes)),int(minutes),side,float(entry),
         json.dumps(features,separators=(",",":"),default=str),utc_now().isoformat()))
    conn.commit()


def resolve_btc_snipes(conn: sqlite3.Connection) -> None:
    """Resolve saved snipes and credit a FLIP only when the final forecast corrected direction."""
    stamp=time.time()
    rows=conn.execute("SELECT * FROM btc_snipe_journal WHERE status='OPEN' ORDER BY id").fetchall()
    for row in rows:
        try:
            if stamp-float(row["last_check"] or 0)<180: continue
            conn.execute("UPDATE btc_snipe_journal SET last_check=? WHERE id=?",(stamp,row["id"])); conn.commit()
            data=http_json(f"{GAMMA}/markets/{row['market_id']}")
            if not isinstance(data,dict) or not data.get("closed"): continue
            outs=parse_list(data.get("outcomes")); prices=parse_list(data.get("outcomePrices"))
            mapping={str(name).lower():float(price) for name,price in zip(outs,prices)}
            winning="UP" if mapping.get("up",mapping.get("yes",0))>=0.99 else ("DOWN" if mapping.get("down",mapping.get("no",0))>=0.99 else None)
            if not winning: continue
            forecast=conn.execute("SELECT current_side,flip_count FROM btc_forecasts WHERE market_id=? AND timeframe=?",(row["market_id"],row["timeframe"])).fetchone()
            final_side=str(forecast["current_side"]) if forecast else str(row["side"])
            flips=int(forecast["flip_count"] or 0) if forecast else 0
            outcome="WIN" if str(row["side"])==winning else ("FLIP" if final_side==winning and flips>0 else "LOSS")
            if outcome=="LOSS":
                try: sf=json.loads(row["features"] or "{}")
                except Exception: sf={}
                tags=[]
                if abs(int(sf.get("score",0) or 0))<4: tags.append("weak votes <4")
                if int(sf.get("flip_risk",0) or 0)>=65: tags.append("high flip risk >=65")
                if float(sf.get("spread",0) or 0)>0.015: tags.append("wide spread >1.5c")
                if float(sf.get("depth",0) or 0)<2000: tags.append("thin depth <$2k")
                if float(sf.get("ask",0) or 0)>0.65: tags.append("expensive entry >$0.65")
                if float(sf.get("move1",0) or 0)*float(sf.get("move3",0) or 0)<=0: tags.append("momentum conflict")
                rp=float(sf.get("range_position",50) or 50)
                if rp>=85 or rp<=15: tags.append("near range extreme")
                conn.execute("INSERT OR IGNORE INTO btc_loss_reviews(trade_id,timeframe,trade_type,tags,reviewed_at) VALUES(?,?,?,?,?)",
                    (-int(row["id"]),int(row["timeframe"]),"SNIPE",json.dumps(tags),utc_now().isoformat()))
            conn.execute("UPDATE btc_snipe_journal SET status='RESOLVED',outcome=?,winning_side=?,resolved_at=? WHERE id=?",
                (outcome,winning,utc_now().isoformat(),row["id"])); conn.commit()
            print(f"BTC snipe settled: {row['timeframe']}m {outcome} · {row['side']} -> {winning}",flush=True)
        except Exception as exc: print(f"BTC snipe resolve error: {type(exc).__name__}: {exc}",flush=True)


def btc_snipe_streak_guard(conn: sqlite3.Connection, tech: dict, brain: dict, ask: float) -> dict:
    """Tighten snipe selection after losses and protect winning streaks."""
    rows=conn.execute("SELECT outcome FROM btc_snipe_journal WHERE status='RESOLVED' AND outcome IN ('WIN','FLIP','LOSS') ORDER BY resolved_at DESC,id DESC LIMIT 20").fetchall()
    wins=sum(r["outcome"] in ("WIN","FLIP") for r in rows); losses=sum(r["outcome"]=="LOSS" for r in rows)
    current_loss=current_win=0
    for r in rows:
        if r["outcome"]=="LOSS" and current_win==0: current_loss+=1
        elif r["outcome"] in ("WIN","FLIP") and current_loss==0: current_win+=1
        else: break
    recovery=btc_streak_guard(conn,5,"SCALP")["forecast_recovery_wins"]
    score=abs(int(tech.get("score",0) or 0)); risk=int(brain.get("flip_risk",95)); reasons=[]
    if current_loss>=2 and recovery<2: reasons.append(f"{current_loss} snipe losses; wait for 2 forecast wins ({recovery}/2)")
    elif current_loss==1 and (score<4 or risk>50): reasons.append("after a snipe loss require 4/5 votes and flip risk <=50")
    if current_win>=3 and (score<4 or risk>45 or ask>0.65): reasons.append("protecting 3+ snipe wins; skip weak/late setup")
    if len(rows)>=12 and wins/(wins+losses)<0.50 and recovery<2: reasons.append("recent snipe results below 50%; learning pause")
    return {"sample":len(rows),"wins":wins,"losses":losses,"current_win_streak":current_win,"current_loss_streak":current_loss,"blocked":bool(reasons),"reasons":reasons}


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
    scalp_opinion = second_opinion(tech, stats, conn=conn, minutes=5, slug=slug)
    if not scalp_opinion["agrees"]:
        return
    scalp_streak = btc_streak_guard(conn, 5, "SCALP")
    scalp_stats = scalp_streak["trades"]
    scalp_losses = scalp_stats["current_loss_streak"]
    scalp_flip = int(scalp_opinion.get("flip_risk",100))
    if scalp_losses >= 2 and scalp_streak["forecast_recovery_wins"] < 2:
        print(f"BTC 5m scalp streak guard: {scalp_losses} snipe losses; waiting for 2 forecast wins.")
        return
    if scalp_losses == 1 and (abs(score) < 4 or scalp_flip > 50):
        print("BTC 5m scalp streak guard: after one snipe loss require 4/5 votes and flip risk <=50.")
        return
    if scalp_stats["current_win_streak"] >= 3 and (abs(score) < 4 or scalp_flip > 45 or ask > 0.65):
        print("BTC 5m scalp streak protection: preserving 3+ wins; skip weak/late setup.")
        return
    filters_ok = (
        SCALP_MIN_ASK <= ask <= SCALP_MAX_ASK
        and stats["spread"] <= SCALP_MAX_SPREAD
        and stats["depth"] >= SCALP_MIN_DEPTH_USD
    )
    if not filters_ok:
        return
    snipe_guard=btc_snipe_streak_guard(conn,tech,scalp_opinion,ask)
    if snipe_guard["blocked"]:
        print("BTC snipe streak protection: "+"; ".join(snipe_guard["reasons"]),flush=True)
        return
    record_btc_snipe(conn,market,5,side,ask,{
        "score":score,"rsi":rsi_value,"move1":move1,"move3":move3,
        "range_position":tech.get("range_position"),"ask":ask,"bid":bid,
        "spread":stats["spread"],"depth":stats["depth"],"imbalance":stats["imbalance"],
        "second_brain_agrees":bool(scalp_opinion["agrees"]),"second_brain_side":scalp_opinion["side"],
        "flip_risk":scalp_flip,"remaining_seconds":remaining
    })
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
    scalp_features = {
        "score":score,"rsi":rsi_value,"move1":move1,"move3":move3,
        "range_position":tech.get("range_position"),"ask":ask,"bid":bid,
        "spread":stats["spread"],"depth":stats["depth"],
        "imbalance":stats["imbalance"],"second_brain_agrees":bool(scalp_opinion["agrees"]),
        "second_brain_side":scalp_opinion["side"],
        "flip_risk":int(scalp_opinion.get("flip_risk",0)),
        "setup_score":setup_score,"remaining_seconds":remaining
    }
    create_paper_trade(conn, market, 5, side, token_id, ask, "SCALP", scalp_features)


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




def record_btc_forecast(conn: sqlite3.Connection, market: dict, minutes: int, tech: dict) -> None:
    """Record a market-window forecast and debounce rapid direction noise."""
    market_id = str(market.get("id") or "")
    side = str(tech.get("side", "NEUTRAL"))
    if not market_id or side not in ("UP", "DOWN"):
        return
    stamp = time.time()
    slug = str(market.get("slug") or market_slug(minutes))
    features = json.dumps({key: tech.get(key) for key in
                           ("score", "rsi", "move1", "move3", "move10",
                            "vol_ratio", "range_position", "last_price")})
    row = conn.execute(
        "SELECT * FROM btc_forecasts WHERE market_id=? AND timeframe=?",
        (market_id, minutes),
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT OR IGNORE INTO btc_forecasts(market_id,market_slug,timeframe,initial_side,current_side,features,created_at,last_seen) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (market_id, slug, minutes, side, side, features, utc_now().isoformat(), stamp),
        )
    else:
        current = str(row["current_side"])
        pending = str(row["pending_side"] or "")
        pending_since = float(row["pending_since"] or 0)
        flips = int(row["flip_count"] or 0)
        if side == current:
            pending, pending_since = "", 0.0
        elif pending != side:
            pending, pending_since = side, stamp
        elif stamp - pending_since >= 9.0:
            current = side
            flips += 1
            pending, pending_since = "", 0.0
            print(f"BTC forecast flip confirmed: {minutes}m {row['current_side']} -> {side} · flips={flips}", flush=True)
        conn.execute(
            "UPDATE btc_forecasts SET current_side=?,pending_side=?,pending_since=?,flip_count=?,features=?,last_seen=? "
            "WHERE market_id=? AND timeframe=?",
            (current, pending, pending_since, flips, features, stamp, market_id, minutes),
        )
    conn.commit()


def resolve_btc_forecasts(conn: sqlite3.Connection) -> None:
    """Resolve expired BTC forecasts from Polymarket's final market outcome."""
    stamp = time.time()
    rows = conn.execute(
        "SELECT * FROM btc_forecasts WHERE status='OPEN' ORDER BY last_seen DESC LIMIT 80"
    ).fetchall()
    for row in rows:
        try:
            try:
                market_start = int(str(row["market_slug"]).rsplit("-", 1)[1])
            except Exception:
                continue
            if stamp < market_start + int(row["timeframe"]) * 60 + 12:
                continue
            if stamp - float(row["last_check"] or 0) < 120:
                continue
            conn.execute(
                "UPDATE btc_forecasts SET last_check=? WHERE market_id=? AND timeframe=?",
                (stamp, row["market_id"], row["timeframe"]),
            )
            conn.commit()
            data = http_json(f"{GAMMA}/markets/{row['market_id']}")
            if not isinstance(data, dict) or not data.get("closed"):
                continue
            outcomes = parse_list(data.get("outcomes"))
            prices = parse_list(data.get("outcomePrices"))
            mapping = {str(name).lower(): float(price) for name, price in zip(outcomes, prices)}
            winning = "UP" if mapping.get("up", mapping.get("yes", 0)) >= 0.99 else (
                "DOWN" if mapping.get("down", mapping.get("no", 0)) >= 0.99 else None
            )
            if not winning:
                continue
            correct = str(row["current_side"]) == winning
            outcome = "FLIP" if correct and int(row["flip_count"] or 0) > 0 else (
                "WIN" if correct else "LOSS"
            )
            conn.execute(
                "UPDATE btc_forecasts SET status='RESOLVED',outcome=?,winning_side=?,resolved_at=? "
                "WHERE market_id=? AND timeframe=?",
                (outcome, winning, utc_now().isoformat(), row["market_id"], row["timeframe"]),
            )
            conn.commit()
            print(f"BTC forecast resolved: {row['timeframe']}m {outcome} · flips={row['flip_count']}", flush=True)
        except Exception as exc:
            print(f"BTC forecast resolve error: {type(exc).__name__}: {exc}", flush=True)
    resolve_btc_snipes(conn)


def btc_forecast_learning(conn: Optional[sqlite3.Connection], minutes: int) -> dict:
    """Summarize resolved forecast accuracy and market-window churn."""
    if conn is None:
        return {"sample": 0, "wins": 0, "losses": 0, "flips": 0,
                "win_rate": None, "flip_rate": 0.0, "caution": False, "pause": False}
    rows = conn.execute(
        "SELECT outcome,flip_count FROM btc_forecasts WHERE timeframe=? AND status='RESOLVED' "
        "ORDER BY resolved_at DESC LIMIT 24", (minutes,)
    ).fetchall()
    sample = len(rows)
    wins = sum(row["outcome"] == "WIN" for row in rows)
    losses = sum(row["outcome"] == "LOSS" for row in rows)
    flips = sum(row["outcome"] == "FLIP" for row in rows)
    decisive = wins + flips + losses
    win_rate = (100.0 * (wins + flips) / decisive) if decisive else None
    flip_rate = flips / sample if sample else 0.0
    caution = sample >= 8 and ((win_rate is not None and win_rate < 50.0) or flip_rate >= 0.35)
    pause = sample >= 12 and ((win_rate is not None and win_rate < 40.0) or flip_rate >= 0.50)
    return {"sample": sample, "wins": wins, "losses": losses, "flips": flips,
            "win_rate": win_rate, "flip_rate": flip_rate,
            "caution": caution, "pause": pause}


def record_btc_loss_reviews(conn: sqlite3.Connection) -> None:
    """Persist a post-loss review so repeated mistakes tighten future calls."""
    rows=conn.execute(
        "SELECT id,timeframe,trade_type,last_action,entry_features FROM paper_trades "
        "WHERE status='CLOSED' AND realized_pnl<0 AND id NOT IN "
        "(SELECT trade_id FROM btc_loss_reviews) ORDER BY id"
    ).fetchall()
    for row in rows:
        try:
            f=json.loads(row["entry_features"] or "{}")
            if not isinstance(f,dict): f={}
        except Exception: f={}
        tags=[]
        if abs(int(f.get("score",0) or 0))<4: tags.append("weak votes <4")
        if int(f.get("flip_risk",0) or 0)>=65: tags.append("high flip risk >=65")
        if float(f.get("spread",0) or 0)>0.015: tags.append("wide spread >1.5c")
        if float(f.get("depth",0) or 0)<2000: tags.append("thin depth <$2k")
        if float(f.get("ask",0) or 0)>0.65: tags.append("expensive entry >$0.65")
        if float(f.get("move1",0) or 0)*float(f.get("move3",0) or 0)<=0: tags.append("momentum conflict")
        rp=float(f.get("range_position",50) or 50)
        if rp>=85 or rp<=15: tags.append("near range extreme")
        if f.get("second_brain_agrees") is False: tags.append("second brain disagreed")
        if "FLIP" in str(row["last_action"] or "").upper(): tags.append("exit after reversal")
        conn.execute("INSERT OR IGNORE INTO btc_loss_reviews(trade_id,timeframe,trade_type,tags,reviewed_at) VALUES(?,?,?,?,?)",
            (row["id"],int(row["timeframe"]),str(row["trade_type"] or "STANDARD"),json.dumps(tags),utc_now().isoformat()))
    conn.commit()


def btc_loss_pattern_guard(conn: Optional[sqlite3.Connection], minutes: int,
                           tech: dict, stats: Optional[dict], current_flip_risk: int) -> dict:
    """Block a repeated condition only after it appears in multiple saved losing reviews."""
    if conn is None: return {"sample":0,"active":[],"block":False}
    rows=conn.execute("SELECT tags FROM btc_loss_reviews WHERE timeframe=? ORDER BY reviewed_at DESC LIMIT 20",(int(minutes),)).fetchall()
    tally={}
    for row in rows:
        try: tags=json.loads(row["tags"] or "[]")
        except Exception: tags=[]
        for tag in set(tags): tally[tag]=tally.get(tag,0)+1
    score=abs(int(tech.get("score",0) or 0))
    move1=float(tech.get("move1",0) or 0); move3=float(tech.get("move3",0) or 0)
    rp=float(tech.get("range_position",50) or 50)
    current={
        "weak votes <4":score<4,
        "high flip risk >=65":int(current_flip_risk)>=65,
        "wide spread >1.5c":bool(stats and float(stats.get("spread",0) or 0)>0.015),
        "thin depth <$2k":bool(stats and float(stats.get("depth",0) or 0)<2000),
        "expensive entry >$0.65":bool(stats and float(stats.get("ask",0) or 0)>0.65),
        "momentum conflict":move1*move3<=0,
        "near range extreme":rp>=85 or rp<=15,
    }
    active=[tag for tag,matched in current.items() if matched and tally.get(tag,0)>=3]
    block=len(rows)>=5 and any(tally.get(tag,0)>=4 for tag in active)
    return {"sample":len(rows),"active":active,"counts":{tag:tally.get(tag,0) for tag in active},"block":block}


def second_opinion(tech: dict, stats: Optional[dict] = None,
                   conn: Optional[sqlite3.Connection] = None, minutes: int = 5,
                   slug: Optional[str] = None) -> dict:
    """Second-brain challenge: momentum, book pressure, exhaustion and resolved memory."""
    move1 = float(tech.get("move1", 0.0))
    move3 = float(tech.get("move3", 0.0))
    move10 = float(tech.get("move10", 0.0))
    score = 0
    score += 1 if move1 > 0.015 else -1 if move1 < -0.015 else 0
    score += 1 if move3 > 0.025 else -1 if move3 < -0.025 else 0
    score += 1 if move10 > 0.04 else -1 if move10 < -0.04 else 0
    imbalance = float(stats.get("imbalance", 0.0)) if stats else 0.0
    if stats:
        score += 1 if imbalance > 0.12 else -1 if imbalance < -0.12 else 0
    range_position = tech.get("range_position")
    if range_position is not None:
        if range_position >= 90:
            score -= 1
        elif range_position <= 10:
            score += 1
    side = "UP" if score >= 2 else "DOWN" if score <= -2 else "WAIT"
    primary = str(tech.get("side", "NEUTRAL"))
    agrees = side == primary and primary in ("UP", "DOWN")
    memory = btc_forecast_learning(conn, minutes)
    market_flips = 0
    if conn is not None and slug:
        row = conn.execute(
            "SELECT flip_count FROM btc_forecasts WHERE market_slug=? AND timeframe=? AND status='OPEN'",
            (slug, minutes),
        ).fetchone()
        market_flips = int(row["flip_count"] or 0) if row else 0
    risk = 30
    if side != primary:
        risk += 22
    if move1 * move3 < 0:
        risk += 12
    if (primary == "UP" and float(tech.get("rsi", 50)) >= 72) or (
        primary == "DOWN" and float(tech.get("rsi", 50)) <= 28
    ):
        risk += 12
    if range_position is not None and ((primary == "UP" and range_position >= 90) or
                                      (primary == "DOWN" and range_position <= 10)):
        risk += 10
    if stats and imbalance * (1 if primary == "UP" else -1) < -0.10:
        risk += 12
    risk += min(20, market_flips * 7)
    risk += min(15, int(memory["flip_rate"] * 30))
    risk = max(5, min(95, risk))
    loss_guard=btc_loss_pattern_guard(conn,minutes,tech,stats,risk)
    risk=max(5,min(95,risk+min(24,8*len(loss_guard["active"]))))
    if market_flips >= 2: agrees = False
    if memory["pause"] or loss_guard["block"]: agrees = False
    if memory["caution"] and score < 3: agrees = False
    label = "CONFIRMED" if agrees else (
        "CHOPPY WINDOW · WAIT" if market_flips >= 2 else
        "LOSS-PATTERN PAUSE · WAIT" if loss_guard["block"] else
        "LEARNING PAUSE · WAIT" if memory["pause"] else "DISAGREEMENT / WAIT"
    )
    loss_text=(" · repeated loss patterns: "+", ".join(loss_guard["active"])) if loss_guard["active"] else ""
    debate = (
        f"PRIMARY {primary} / CHECK {side} ({score:+d}) · "
        f"window flips {market_flips} · resolved {memory['sample']} "
        f"with {memory['flip_rate']*100:.0f}% flip history{loss_text}"
    )
    return {"side": side, "score": score, "agrees": agrees, "label": label,
            "flip_risk": risk, "market_flips": market_flips, "memory": memory,
            "loss_guard":loss_guard,"debate": debate}

def _live_market_odds(minutes: int) -> dict:
    """Cached current Polymarket UP/DOWN best-ask prices for dashboard meters."""
    now=time.time()
    cache=getattr(_live_market_odds,"_cache",{})
    cached=cache.get(int(minutes))
    if cached and now-cached[0]<5:
        return cached[1]
    result={"up":None,"down":None,"raw":{}}
    try:
        market=get_market(minutes)
        if market:
            tokens=outcome_tokens(market)
            for side in ("UP","DOWN"):
                token=tokens.get(side)
                book=get_book(token) if token else None
                stats=book_stats(book) if book else None
                if stats and 0<float(stats["ask"])<1:
                    ask=float(stats["ask"])
                    result[side.lower()]=int(round(ask*100))
                    result["raw"][side.lower()]=ask
    except Exception as exc:
        print(f"BTC {minutes}m live odds refresh failed: {type(exc).__name__}: {exc}")
    cache[int(minutes)]=(now,result)
    _live_market_odds._cache=cache
    return result

def color_meter(value: int, fill: str="🟩", width: int=10) -> str:
    value=max(0,min(100,int(value)))
    filled=round(value*width/100)
    return fill*filled+"⬜"*(width-filled)

def btc_snipe_meter(conn: sqlite3.Connection, minutes: int, side: str,
                      live_odds: dict, confirmed: bool, flip_risk: int) -> dict:
    """Track selected Polymarket contract movement for a readable SNIPE meter."""
    ask=live_odds.get("raw",{}).get(str(side).lower())
    if side not in ("UP","DOWN") or ask is None:
        return {"strength":0,"move":None,"status":"SNIPE WATCH · contract quote unavailable"}
    slug=market_slug(minutes); stamp=time.time()
    row=conn.execute(
        "SELECT ask,ts FROM btc_snipe_ticks WHERE market_slug=? AND timeframe=? AND side=?",
        (slug,minutes,side)
    ).fetchone()
    move=None
    if row and 2<=stamp-float(row["ts"])<=90 and float(row["ask"])>0:
        move=(float(ask)/float(row["ask"])-1)*100
    conn.execute(
        "INSERT INTO btc_snipe_ticks(market_slug,timeframe,side,ask,ts) VALUES(?,?,?,?,?) "
        "ON CONFLICT(market_slug,timeframe,side) DO UPDATE SET ask=excluded.ask,ts=excluded.ts",
        (slug,minutes,side,float(ask),stamp)
    )
    conn.commit()
    if move is None:
        status="SNIPE WATCH · building contract baseline"
    elif flip_risk>=70:
        status="SNIPE BLOCKED · flip risk high"
    elif move>=1 and confirmed:
        status="SNIPE CONFIRMED · contract rising with signal"
    elif move<=-1:
        status="SNIPE WAIT · selected contract weakening"
    else:
        status="SNIPE WATCH · move not strong enough"
    strength=0 if move is None else max(0,min(100,round(max(0.0,move)*3+(20 if move>=1 and confirmed else 0))))
    return {"strength":strength,"move":move,"status":status}


def _dashboard_embed(conn: sqlite3.Connection, tech: dict, opinions: dict,
                     minutes: int = 5) -> dict:
    """Build the branded BTC SIGNAL TERMINAL dashboard for one market window."""
    minutes = 15 if int(minutes) == 15 else 5
    if minutes == 15:
        open_rows = conn.execute(
            "SELECT timeframe,side,entry_price,remaining_quantity,trade_type,last_action "
            "FROM paper_trades WHERE status='OPEN' AND timeframe=15 "
            "ORDER BY id DESC LIMIT 5"
        ).fetchall()
        pnl = daily_closed_pnl(conn, 15)
        perf = recent_paper_performance(conn, 20, 15)
    else:
        # Preserve the existing 5-minute terminal's original combined display.
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
    flip = max(flip, int(opinions.get("flip_risk", flip)))
    if minutes == 15:
        scalp_live = max(0, min(95, 50 + abs(score) * 8 + (8 if confirmed else -12)))
        scalp_call = f"15M {safe_call} LEAN" if confirmed else "15M WAIT"
    else:
        scalp_live = max(0, min(95, 35 + abs(score) * 8
                                + (12 if move1 * move3 > 0 and abs(move3) >= 0.035 else -5)
                                + (8 if abs(move1) >= 0.025 else 0)))
        scalp_call = ("SCALP CALL " + side) if side in ("UP", "DOWN") and scalp_live >= 65 and confirmed else "SCALP STANDBY"
    price = float(tech.get("last_price", 0.0))
    rsi_value = float(tech.get("rsi", 50.0))
    color = 0x19C37D if safe_call == "UP" else 0xEF4444 if safe_call == "DOWN" else 0xF59E0B
    call_icon = "🟢" if safe_call == "UP" else "🔴" if safe_call == "DOWN" else "🟡"
    action_label = (("UP CALL" if safe_call == "UP" else "DOWN CALL") + f" · {abs(score)*9+50}% strength") if confirmed else "WAIT · SIGNALS NOT ALIGNED"

    # Live market-price meters (ask price = buy-side contract pricing).
    live_odds=_live_market_odds(minutes)
    up_odds=live_odds.get("up"); down_odds=live_odds.get("down")
    snipe=btc_snipe_meter(conn,minutes,side,live_odds,confirmed,flip)
    snipe_move="—" if snipe["move"] is None else f"{snipe['move']:+.2f}%"
    snipe_text=(f"**{snipe['status']}**\n**SNIPE STRENGTH {snipe['strength']}%** "
                f"{color_meter(snipe['strength'],'🎯')}\nContract move **{snipe_move}** "
                "· Strength meter, not win odds")
    market_period_seconds=minutes*60
    elapsed_seconds=int(time.time()%market_period_seconds)
    market_progress=int(elapsed_seconds*100/market_period_seconds)
    seconds_to_close=market_period_seconds-elapsed_seconds
    progress_bar=color_meter(market_progress,"🟦",16)
    countdown=f"{seconds_to_close//60:02d}:{seconds_to_close%60:02d}"
    if not confirmed:
        action_now="🟡 WAIT · checks disagree"
    elif open_rows:
        action_now=str(open_rows[0]["last_action"] or "HOLD / MONITOR")
    else:
        action_now=f"🟡 {safe_call} BIAS · wait for entry filters"
    action_upper=action_now.upper()
    if any(word in action_upper for word in ("SELL","EXIT","STOP","PROTECT")):
        color=0xEF4444; action_now="🔴 "+action_now
    elif any(word in action_upper for word in ("TAKE PROFIT","REDUCE")):
        color=0xF59E0B; action_now="🟠 "+action_now
    elif "FLIP" in action_upper or flip>=70:
        color=0xF59E0B; action_now="🟠 FLIP · "+action_now
    elif "HOLD" in action_upper or "MONITOR" in action_upper:
        color=0x14B8A6; action_now="🩵 "+action_now
    elif "WAIT" in action_upper:
        color=0x8794A8; action_now="⚪ "+action_now
    elif safe_call=="UP":
        color=0x19C37D; action_now="🟢 "+action_now
    elif safe_call=="DOWN":
        color=0xEF4444; action_now="🔴 "+action_now
    sell_meter=max(flip,75 if any(word in action_upper for word in ("SELL","EXIT","STOP","PROTECT")) else 0)
    hold_meter=max(0,min(100,100-sell_meter))
    odds_text=(f"UP **{up_odds}%** {color_meter(up_odds,'🟩')}  ·  DOWN **{down_odds}%** {color_meter(down_odds,'🟥')}"
               if up_odds is not None and down_odds is not None else
               "Live contract prices temporarily unavailable")
    description=(
        f"### {call_icon} **{safe_call}** · {action_label}\n"
        f"**{action_now}**\n"
        f"**LIVE ODDS** {odds_text}\n"
        f"**BTC** USD {price:,.2f}  ·  **{minutes}M** closes **{countdown}**\n"
        f"{progress_bar} **{market_progress}%** elapsed\n"
        f"**🩵 HOLD** {hold_meter}% {color_meter(hold_meter,'🩵')}  ·  **🟥 EXIT RISK** {sell_meter}% {color_meter(sell_meter,'🟥')}\n"
        f"*Updated <t:{int(time.time())}:R>*"
    )

    # Discord embeds support real color accents, an author line, thumbnail,
    # field columns, and a footer. Keep gauges as indicator scores, not odds.
    description = (
        f"### {call_icon}  {safe_call}  ·  {action_label}\n"
        f"**ACTION NOW:** {action_now}\n"
        f"**POLYMARKET BTC {minutes}M UP/DOWN**  {market_progress}%  `{progress_bar}`  ·  closes in **{countdown}**\n"
        f"**BTC / USD**  `${price:,.2f}`\n"
        f"`PRIMARY  {side}`  ·  `CHECK  {second}`  ·  "
        f"{'🟢 MATCH' if confirmed else '🟠 MISMATCH'}\n"
        f"**SECOND BRAIN:** {opinions.get('label','WAIT')} · FLIP {opinions.get('flip_risk',flip)}% · "
        f"window flips {opinions.get('market_flips',0)} · memory {opinions.get('memory',{}).get('sample',0)} resolved\n"
        f"*Last refresh <t:{int(time.time())}:R>*"
    )
    perf_text=(f"**{perf['wins']}W · {perf['losses']}L**  ·  "
                f"Win {perf['win_rate']:.0f}%" if perf['count'] else "Collecting paper results")
    perf_text+=f"\nP/L **USD {pnl:+.2f}** · Open **{len(open_rows)}**"
    position_lines=[]
    for row in open_rows[:2]:
        position_lines.append(
            f"**{row['timeframe']}M {row['side']}** · {row['last_action'] or 'HOLD / MONITOR'}"
        )
    position_text="\n".join(position_lines) if position_lines else "👀 No open paper position"
    scalp_text=f"**{scalp_call}** · **{scalp_live}%**\n{color_meter(scalp_live,'🟦')}"
    # Plain-language explanation: show the evidence behind the call, including nearby range edges.
    range_pos = float(tech.get("range_position", 50.0))
    move1_pct = float(tech.get("move1", 0.0))
    move3_pct = float(tech.get("move3", 0.0))
    move10_pct = float(tech.get("move10", 0.0))
    if side == "UP":
        trend_words = "price is moving up"
    elif side == "DOWN":
        trend_words = "price is moving down"
    else:
        trend_words = "price has no clear direction"
    evidence = f"{abs(score)} of 5 checks point {side.lower() if side in ('UP','DOWN') else 'either way'}; {trend_words}."
    evidence += f" Recent moves: 1 candle {move1_pct:+.2f}%, 3 candles {move3_pct:+.2f}%, 10 candles {move10_pct:+.2f}%."
    evidence += f" Price is {range_pos:.0f}% of the way from the recent low (${float(tech.get('range_low',0)):,.0f}) to high (${float(tech.get('range_high',0)):,.0f})."
    if range_pos >= 85:
        evidence += " Price is close to the recent high, so an UP call may be late."
    elif range_pos <= 15:
        evidence += " Price is close to the recent low, so a DOWN call may be late."
    evidence += " The second brain " + ("agrees." if confirmed else "does not agree, so the safe call is WAIT.")
    if flip >= 65:
        evidence += f" Flip warning: risk meter is {flip}%; reconsider if price turns against the call."
    fields=[
        {
            "name":"🧠 WHY THIS CALL? · SIMPLE EXPLANATION",
            "value":evidence[:1024],
            "inline":False
        },
        {
            "name":"📊 SIGNAL",
            "value":(
                f"**{safe_call}** · votes **{score:+d}/5**\n"
                f"UP **{up_bias}%** {color_meter(up_bias,'🟩')}  ·  "
                f"DOWN **{down_bias}%** {color_meter(down_bias,'🟥')}"
            ),
            "inline":False
        },
        {
            "name":"🟢 HOLD  /  🔴 EXIT RISK",
            "value":(
                f"HOLD **{hold_meter}%** {color_meter(hold_meter,'🟩')}\n"
                f"FLIP / EXIT **{sell_meter}%** {color_meter(sell_meter,'🟥')}"
            ),
            "inline":False
        },
        {"name":"⚡ SCALP","value":scalp_text,"inline":True},
        {"name":"📂 POSITION","value":position_text[:900],"inline":True},
        {"name":"📈 PAPER STATS","value":perf_text[:900],"inline":False},
        {"name":"🔥 BTC WIN/LOSS + SNIPE TRACKER","value":btc_tracker_text(conn)[:900],"inline":False},
        {"name":"🔎 LOSS DIAGNOSIS · ENTRY PATTERNS / EXIT CAUSES","value":btc_loss_report_text(conn)[:1000],"inline":False}
    ]

    return {
        "author": {
            "name": f"BTC SIGNAL TERMINAL  /  {minutes}M MARKET INTELLIGENCE",
            "icon_url": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png",
        },
        "title": f"━━━━━━━━  BTC / USD · {minutes}M  ━━━━━━━━",
        "description": description,
        "color": color,
        "thumbnail": {"url": "https://assets.coingecko.com/coins/images/1/large/bitcoin.png"},
        "fields": fields,
        "footer": {"text": f"BTC {minutes}M SIGNAL TERMINAL  •  PAPER MODE  •  NO LIVE ORDERS"},
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
        # Log this once for the chart channel so a missing Render variable
        # is visible without flooding logs on the 1-second scanner loop.
        if state_prefix == "btc_live_chart":
            logged_key = state_prefix + "_missing_webhook_logged"
            logged = conn.execute(
                "SELECT state_value FROM bot_state WHERE state_key=?", (logged_key,)
            ).fetchone()
            if not logged:
                print(f"{state_prefix} skipped: {webhook_env} is missing from the running Render environment.")
                conn.execute(
                    "INSERT INTO bot_state(state_key,state_value) VALUES(?,?) "
                    "ON CONFLICT(state_key) DO UPDATE SET state_value=excluded.state_value",
                    (logged_key, "1"),
                )
                conn.commit()
        return
    if state_prefix == "btc_live_chart":
        conn.execute("DELETE FROM bot_state WHERE state_key=?", (state_prefix + "_missing_webhook_logged",))
        conn.commit()
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
    """Mirror the existing 5-minute BTC terminal into its Discord message."""
    embed = _dashboard_embed(conn, tech, opinions, minutes=5)
    update_btc_channel_message(conn, "DISCORD_BTC_LIVE_WEBHOOK_URL",
                               "btc_live_dashboard", embed, DISCORD_DASHBOARD_UPDATE_SECONDS)


def update_btc_15m_live_channel(conn: sqlite3.Connection, tech: dict, opinions: dict) -> None:
    """Create and refresh a separate 15-minute Polymarket BTC dashboard message."""
    embed = _dashboard_embed(conn, tech, opinions, minutes=15)
    update_btc_channel_message(conn, "DISCORD_BTC_LIVE_WEBHOOK_URL",
                               "btc_live_15m_dashboard", embed, DISCORD_DASHBOARD_UPDATE_SECONDS)


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
    record_btc_forecast(conn, market, minutes, tech)
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
    opinions = second_opinion(tech, stats, conn=conn, minutes=minutes, slug=slug)
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
    if opinions.get("loss_guard",{}).get("block"):
        reasons.append("LOSS-PATTERN PAUSE: repeated conditions found in saved losing-trade reviews")
    # Learn conservatively from completed paper trades: poor recent results can
    # tighten entries, but strong results never loosen the original safety rules.
    performance = recent_paper_performance(conn, 20, minutes)
    if performance["count"] >= 10 and performance["win_rate"] < 60.0:
        reasons.append(f"recent {minutes}m paper results below 60%; adaptive safety pause")
    streak = btc_streak_guard(conn, minutes)
    trade_streak = streak["trades"]
    # After two paper losses, wait for two independently resolved forecast wins
    # before re-entering. A single loss tightens selection; it never loosens caps.
    if trade_streak["current_loss_streak"] >= 2 and streak["forecast_recovery_wins"] < 2:
        reasons.append(
            f"STREAK PROTECTION: {trade_streak['current_loss_streak']} paper losses; "
            f"need 2 resolved forecast wins before new entry "
            f"({streak['forecast_recovery_wins']}/2 so far)"
        )
    elif trade_streak["current_loss_streak"] == 1:
        if abs(int(tech["score"])) < 4 or not opinions["agrees"] or int(opinions.get("flip_risk", 100)) > 50:
            reasons.append("STREAK PROTECTION after 1 loss: require 4/5 votes, second-brain agreement, and flip risk ≤50")
    elif trade_streak["current_win_streak"] >= 3:
        if abs(int(tech["score"])) < 4 or int(opinions.get("flip_risk",100)) > 45 or ask > 0.65:
            reasons.append("WIN-STREAK PROTECTION: skip weak/late entries; require 4/5 votes, flip risk ≤45, and ask ≤$0.65")
    memory = opinions["memory"]
    if memory["caution"]:
        reasons.append(f"second-brain memory caution: {memory['flip_rate']*100:.0f}% of resolved {minutes}m windows flipped")
    if memory["pause"]:
        reasons.append(f"second-brain memory pause: {minutes}m resolved results remain weak")
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
    signal_flip = max(signal_flip, int(opinions.get("flip_risk", signal_flip)))
    signal_hold = max(0, min(100, 100 - signal_flip))
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
        f"{gauge_line('HOLD', signal_hold, '🩵')}\n"
        f"{gauge_line('FLIP RISK', signal_flip, '🟠')}\n"
        f"⚡ SCALP FIT: <b>{scalp_fit}/100</b>  <code>{gauge_bar(scalp_fit)}</code>\n"
        f"⏱ HOLD WINDOW: <b>{hold_window}</b>\n\n"
        f"VOTES {abs(int(tech['score']))}/5 · RSI {tech['rsi']:.0f}\n"
        f"3m {tech['move3']:+.2f}% · 10m {tech['move10']:+.2f}% · 20-candle range {float(tech.get('range_position',50)):.0f}% from low\n"
        f"ASK ${ask:.3f} · BID ${stats['bid']:.3f} · DEPTH ${stats['depth']:.0f}\n"
        f"EDGE {edge*100:+.1f}pp\n"
        f"SECOND BRAIN: {opinions['side']} · {opinions['label']} · {opinions.get('debate','')}\n"
        f"MEMORY: {memory['sample']} resolved · {memory['wins']}W/{memory['losses']}L · "
        f"{memory['flip_rate']*100:.0f}% windows flipped\n"
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
    entry_features = {
        "score":int(tech["score"]),"rsi":float(tech["rsi"]),
        "move1":float(tech.get("move1",0)),"move3":float(tech.get("move3",0)),
        "move10":float(tech.get("move10",0)),"range_position":tech.get("range_position"),
        "ask":ask,"bid":stats["bid"],"spread":stats["spread"],
        "depth":stats["depth"],"imbalance":stats["imbalance"],
        "edge_proxy":edge,"second_brain_agrees":bool(opinions["agrees"]),
        "second_brain_side":opinions["side"],"flip_risk":int(opinions.get("flip_risk",0)),
        "remaining_seconds":remaining
    }
    create_paper_trade(conn, market, minutes, side, token_id, ask, "STANDARD", entry_features)


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
                resolve_btc_forecasts(conn)
                candles = get_candles()
                tech = technical_signal(candles)
                manage_open_trades(conn, tech)
                record_btc_loss_reviews(conn)
                scan_scalp(conn, tech)
                dashboard_opinion = second_opinion(
                    tech, conn=conn, minutes=5, slug=market_slug(5)
                )
                dashboard_opinion_15 = second_opinion(
                    tech, conn=conn, minutes=15, slug=market_slug(15)
                )
                for minutes in TIMEFRAMES:
                    try:
                        scan_one(conn, minutes, tech)
                    except Exception as exc:
                        print(f"BTC {minutes}m scan error: {type(exc).__name__}: {exc}")
                update_dashboard(conn, tech, dashboard_opinion)
                update_btc_live_channel(conn, tech, dashboard_opinion)
                update_btc_15m_live_channel(conn, tech, dashboard_opinion_15)
                # Chart delivery is paused for now. Keep BTC live and signal scanning active.
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
