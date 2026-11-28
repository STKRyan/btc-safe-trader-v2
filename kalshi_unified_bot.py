#!/usr/bin/env python3
"""Standalone Kalshi BTC/ETH/Gold paper bot. No real order API is implemented."""
from __future__ import annotations

import hashlib
import json
import os
import time
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import urlsplit, urlunsplit

import requests
from kalshi_unified import core

API = os.getenv("KALSHI_API_BASE", "https://external-api.kalshi.com/trade-api/v2").rstrip("/")
DB_PATH = os.getenv("KALSHI_UNIFIED_DB", "kalshi_unified.sqlite3")
WEBHOOK = os.getenv("KALSHI_UNIFIED_DISCORD_WEBHOOK_URL", "").strip()
SCAN_SECONDS = max(3, float(os.getenv("KALSHI_SCAN_SECONDS", "5")))
CARD_SECONDS = max(15, int(os.getenv("KALSHI_CARD_UPDATE_SECONDS", "30")))
MAX_CARDS = max(1, min(8, int(os.getenv("KALSHI_MAX_CARDS", "4"))))
HTTP = requests.Session()
HTTP.headers.update({"User-Agent": "STK-Kalshi-Paper/1.0", "Accept": "application/json"})


def utc_iso(ts=None):
    return datetime.fromtimestamp(ts or time.time(), timezone.utc).isoformat()


def get_json(path, params=None):
    response = HTTP.get(API + path, params=params, timeout=12)
    if response.status_code == 429:
        time.sleep(min(20, int(response.headers.get("Retry-After", "3"))))
        response = HTTP.get(API + path, params=params, timeout=12)
    response.raise_for_status()
    return response.json()


def discover_markets():
    """Page through public market metadata; reject any ambiguous settlement contract."""
    cursor, candidates = None, []
    for _ in range(4):
        params = {"status": "open", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data = get_json("/markets", params)
        for m in data.get("markets", []):
            eligible, reason = core.contract_is_eligible(m)
            if not eligible:
                continue
            asset = core.classify_asset(m)
            m["_asset"] = asset
            m["_display"] = " ".join(str(m.get(k) or "") for k in
                ("title", "subtitle", "yes_sub_title", "no_sub_title")).strip()[:240]
            m["_eligibility_reason"] = reason
            candidates.append(m)
        cursor = data.get("cursor")
        if not cursor:
            break
    return candidates


def evaluate(m):
    q = core.make_quote(m)
    if q is None:
        return {"action": "WAIT", "reason": "quote fields incomplete or invalid", "quote": q}
    momentum, age = core.record_sample(DB_PATH, m["ticker"], q["yes_mid"])
    signal = core.signal_gate(quote=q, momentum_pct=momentum, sample_age=age)
    signal["quote"] = q
    signal["asset"] = m["_asset"]
    signal["ticker"] = m["ticker"]
    return signal


def webhook_message_url(message_id=None):
    if not WEBHOOK.startswith("https://discord.com/api/webhooks/"):
        return None
    parts = urlsplit(WEBHOOK)
    path = parts.path.rstrip("/")
    if message_id:
        path += "/messages/" + str(message_id)
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, ""))


def manage_position(m, signal):
    """Paper exit management uses executable bid, records outcomes append-only."""
    ticker = m["ticker"]
    q = signal.get("quote")
    if not q:
        return
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT side,entry,opened_at,status FROM paper_positions WHERE ticker=?",
                         (ticker,)).fetchone()
    if not row or row[3] != "OPEN":
        if signal.get("side"):
            core.open_paper_trade(DB_PATH, ticker, m["_asset"], signal)
        return
    side, entry_text, opened = row[:3]
    entry = core.decimal(entry_text)
    bid = q["yes_bid"] if side == "YES" else q["no_bid"]
    if entry is None or entry <= 0:
        return
    pnl_pct = float((bid-entry)/entry*100)
    age = time.time()-opened
    target = 3.0 if age < 90 else 6.0
    stop = -2.5
    opposite = signal.get("side") and signal.get("side") != side
    reason = None
    if pnl_pct >= target:
        reason = "paper target reached"
    elif pnl_pct <= stop:
        reason = "paper stop reached"
    elif opposite:
        reason = "qualified signal flipped; close and reassess"
    elif age >= 240:
        reason = "maximum paper hold time reached"
    if reason:
        core.close_paper_trade(DB_PATH, ticker, bid, reason)
        if signal.get("side") and signal.get("side") != side:
            core.open_paper_trade(DB_PATH, ticker, m["_asset"], signal)


def render_card(m, s, stats):
    q = s.get("quote") or {}
    action = s.get("action", "WAIT")
    color = 0x22C55E if s.get("side") == "YES" else (0xEF4444 if s.get("side") == "NO" else 0x64748B)
    side = s.get("side")
    entry = s.get("entry")
    with sqlite3.connect(DB_PATH) as db:
        pos = db.execute("SELECT side,entry,opened_at,status FROM paper_positions WHERE ticker=?",
                         (m["ticker"],)).fetchone()
    position_text = "NO OPEN PAPER POSITION"
    if pos and pos[3] == "OPEN":
        position_text = f"{pos[0]} · entry {pos[1]} · {int(time.time()-pos[2])}s held"
    fields = [
        {"name": "YES bid / ask", "value": f"{q.get('yes_bid','—')} / {q.get('yes_ask','—')}", "inline": True},
        {"name": "NO bid / ask", "value": f"{q.get('no_bid','—')} / {q.get('no_ask','—')}", "inline": True},
        {"name": "Paper entry", "value": str(entry) if entry is not None else "WAIT", "inline": True},
        {"name": "Signal / Hold", "value": f"{s.get('signal_strength','—')} / {s.get('hold_strength','—')}%", "inline": True},
        {"name": "Flip risk", "value": f"{s.get('flip_risk','—')}%", "inline": True},
        {"name": "Contract momentum", "value": f"{s.get('momentum_pct',0):+.2f}%" if s.get("momentum_pct") is not None else "Building history", "inline": True},
        {"name": "Paper position", "value": position_text, "inline": False},
        {"name": "Plan", "value": f"Target +{s.get('target_pct',0)*100:.1f}% · Stop -{s.get('stop_pct',0)*100:.1f}% · max {s.get('max_hold_seconds',0)}s" if side else "No entry until all gates pass", "inline": False},
        {"name": "Settled initial calls", "value": f"{stats['wins']}W / {stats['losses']}L · {stats['win_rate_pct'] if stats['win_rate_pct'] is not None else '—'}% · current streak {stats['current_initial_streak']} · best {stats['best_initial_win_streak']}", "inline": False},
    ]
    rules = str(m.get("rules_primary") or "Settlement rules unavailable")[:650]
    embed = {
        "title": f"{m['_asset']} · KALSHI PAPER · {action}",
        "description": f"**{m['_display']}**\n{m.get('ticker')}\n\n{s.get('reason','')}\n\n**Primary settlement rules (excerpt):** {rules}",
        "color": color, "fields": fields,
        "footer": {"text": "PAPER ONLY · contract-price momentum, not spot-price forecast · strength ≠ win probability"},
        "timestamp": utc_iso(),
    }
    close = m.get("close_time") or m.get("expected_expiration_time")
    if close:
        embed["fields"].append({"name": "Market close", "value": str(close), "inline": True})
    return {"embeds": [embed], "allowed_mentions": {"parse": []}}


def send_or_edit(m, s, stats):
    payload = render_card(m, s, stats)
    packed = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(packed.encode()).hexdigest()
    ticker = m["ticker"]
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT message_id,payload_hash,updated_at,last_action FROM card_state WHERE ticker=?",
                         (ticker,)).fetchone()
    now = time.time()
    if row and row[1] == digest:
        return
    if row and now-row[2] < CARD_SECONDS and row[3] == s.get("action"):
        return
    message_id = row[0] if row else None
    if not WEBHOOK:
        print(json.dumps({"market": ticker, "signal": s}, default=str))
        return
    try:
        if message_id:
            response = HTTP.patch(webhook_message_url(message_id), json=payload, timeout=10)
            if response.status_code == 404:
                message_id = None
            else:
                response.raise_for_status()
        if not message_id:
            response = HTTP.post(webhook_message_url(), params={"wait": "true"}, json=payload, timeout=10)
            response.raise_for_status()
            message_id = str(response.json()["id"])
        with sqlite3.connect(DB_PATH) as db:
            db.execute("INSERT INTO card_state VALUES(?,?,?,?,?) ON CONFLICT(ticker) DO UPDATE SET message_id=excluded.message_id,payload_hash=excluded.payload_hash,updated_at=excluded.updated_at,last_action=excluded.last_action",
                       (ticker, message_id, digest, now, s.get("action","WAIT")))
            db.commit()
    except Exception as exc:
        print(f"Discord card error {ticker}: {type(exc).__name__}: {exc}")


def main():
    core.init_ledger(DB_PATH)
    print("Kalshi BTC/ETH/Gold paper bot started. Real order endpoints are not present.")
    while True:
        started = time.time()
        try:
            markets = discover_markets()
            evaluated = []
            for market in markets:
                signal = evaluate(market)
                manage_position(market, signal)
                if signal.get("side"):
                    rank = (signal.get("hold_strength",0), signal.get("signal_strength",0),
                            -signal.get("flip_risk",100))
                    evaluated.append((rank, market, signal))
            evaluated.sort(key=lambda item: item[0], reverse=True)
            overall = core.stats(DB_PATH)
            for _, market, signal in evaluated[:MAX_CARDS]:
                send_or_edit(market, signal, overall)
            if not evaluated:
                print(f"{utc_iso()} · no qualified contract · {len(markets)} verified contracts scanned")
        except Exception as exc:
            print(f"Scan failure: {type(exc).__name__}: {exc}")
        time.sleep(max(0.25, SCAN_SECONDS-(time.time()-started)))


if __name__ == "__main__":
    main()
