#!/usr/bin/env python3
"""Isolated Kalshi BTC/ETH/Gold paper scanner. No order placement."""
from __future__ import annotations
import json, os, re, sqlite3, time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit, urlunsplit
import requests

API = os.getenv("KALSHI_API_BASE", "https://external-api.kalshi.com/trade-api/v2").rstrip("/")
DB_PATH = os.getenv("KALSHI_UNIFIED_DB", "kalshi_unified.sqlite3")
WEBHOOK = os.getenv("KALSHI_UNIFIED_DISCORD_WEBHOOK_URL", "").strip()
SCAN_SECONDS = max(2.0, float(os.getenv("KALSHI_SCAN_SECONDS", "3")))
UPDATE_SECONDS = max(10, int(os.getenv("KALSHI_CARD_UPDATE_SECONDS", "20")))
ASSETS = {
    "BTC": (r"\bbitcoin\b|\bBTC\b",),
    "ETH": (r"\bethereum\b|\bETH\b",),
    "GOLD": (r"\bgold\b|\bXAU\b",),
}
S = requests.Session()
S.headers.update({"User-Agent": "KalshiUnifiedPaperBot/0.1", "Accept": "application/json"})


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def dec(value, default=None):
    try:
        if value is None or value == "":
            return default
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


def init_db():
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS market_samples(
            ticker TEXT NOT NULL, ts REAL NOT NULL, yes_mid REAL, title TEXT,
            PRIMARY KEY(ticker, ts))""")
        db.execute("""CREATE TABLE IF NOT EXISTS active_cards(
            ticker TEXT PRIMARY KEY, message_id TEXT, last_payload TEXT,
            last_sent REAL NOT NULL DEFAULT 0, current_side TEXT,
            initial_side TEXT, initial_call_ts REAL, initial_result TEXT)""")
        db.execute("""CREATE TABLE IF NOT EXISTS paper_events(
            id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL,
            asset TEXT NOT NULL, event_type TEXT NOT NULL, side TEXT,
            price REAL, pct REAL, ts REAL NOT NULL, detail TEXT)""")
        db.execute("""CREATE TABLE IF NOT EXISTS market_outcomes(
            ticker TEXT PRIMARY KEY, asset TEXT NOT NULL, initial_side TEXT,
            final_side TEXT, initial_result TEXT, managed_result TEXT,
            settlement_result TEXT, updated_at TEXT)""")
        db.commit()


def api_get(path, params=None):
    response = S.get(API + path, params=params, timeout=8)
    response.raise_for_status()
    return response.json()


def discover_markets():
    """Collect active markets and retain only unambiguous BTC/ETH/Gold contracts."""
    found, cursor = [], None
    for _ in range(5):
        params = {"status": "open", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        data = api_get("/markets", params)
        for market in data.get("markets", []):
            title = " ".join(str(market.get(k) or "") for k in
                             ("title", "subtitle", "yes_sub_title", "no_sub_title"))
            rules = " ".join(str(market.get(k) or "") for k in ("rules_primary", "rules_secondary"))
            text = (title + " " + rules).lower()
            asset = next((a for a, (pattern,) in ASSETS.items()
                         if re.search(pattern, title, re.I)), None)
            if not asset or market.get("status") not in ("open", "active"):
                continue
            # Only explicit directional/threshold contracts are eligible. Ambiguous contracts are skipped.
            if not re.search(r"\b(above|higher|greater|below|lower|less than|at or|reach|hit)\b", text):
                continue
            market["_asset"] = asset
            market["_display"] = title[:220]
            found.append(market)
        cursor = data.get("cursor")
        if not cursor:
            break
    return found


def quote_market(m):
    yes_bid = dec(m.get("yes_bid_dollars"))
    yes_ask = dec(m.get("yes_ask_dollars"))
    no_bid = dec(m.get("no_bid_dollars"))
    no_ask = dec(m.get("no_ask_dollars"))
    if yes_ask is None and no_bid is not None:
        yes_ask = Decimal("1") - no_bid
    if no_ask is None and yes_bid is not None:
        no_ask = Decimal("1") - yes_bid
    if yes_bid is None or yes_ask is None:
        return None
    return {"yes_bid": yes_bid, "yes_ask": yes_ask, "no_bid": no_bid,
            "no_ask": no_ask, "mid": (yes_bid + yes_ask) / 2,
            "spread": yes_ask - yes_bid}


def sample_history(ticker, mid, title):
    now = time.time()
    with sqlite3.connect(DB_PATH) as db:
        db.execute("INSERT OR IGNORE INTO market_samples VALUES(?,?,?,?)",
                   (ticker, now, float(mid), title))
        db.execute("DELETE FROM market_samples WHERE ticker=? AND ts<?",
                   (ticker, now - 1800))
        row = db.execute("SELECT yes_mid,ts FROM market_samples WHERE ticker=? AND ts<=? ORDER BY ts DESC LIMIT 1",
                         (ticker, now - 20)).fetchone()
        db.commit()
    return (float(mid) - row[0]) / row[0] if row and row[0] else None


def yes_direction(m):
    text = (m.get("_display", "") + " " + str(m.get("rules_primary") or "")).lower()
    if re.search(r"\b(above|higher|greater than|reach|hit)\b", text):
        return "UP"
    if re.search(r"\b(below|lower|less than)\b", text):
        return "DOWN"
    return None


def evaluate(m):
    q = quote_market(m)
    if not q or q["spread"] > Decimal("0.08") or q["yes_ask"] < Decimal("0.08") or q["yes_ask"] > Decimal("0.92"):
        return {"action": "WAIT", "reason": "quote unavailable, spread too wide, or contract price outside safe band", "quote": q}
    momentum = sample_history(m["ticker"], q["mid"], m["_display"])
    if momentum is None:
        return {"action": "WAIT", "reason": "building fresh price history", "quote": q}
    yes_up = yes_direction(m)
    if yes_up is None:
        return {"action": "WAIT", "reason": "contract direction cannot be mapped safely", "quote": q}
    # Two independent checks: short-window price impulse and quote quality.
    impulse = momentum * 100
    if abs(impulse) < 0.35:
        return {"action": "WAIT", "reason": "momentum not strong enough", "quote": q, "momentum": impulse}
    chosen_yes = impulse > 0
    side = yes_up if chosen_yes else ("DOWN" if yes_up == "UP" else "UP")
    entry = q["yes_ask"] if chosen_yes else q["no_ask"]
    if entry is None or entry <= 0 or entry >= 1:
        return {"action": "WAIT", "reason": "no executable side quote", "quote": q}
    flip_risk = min(99.0, max(1.0, 50.0 - abs(impulse) * 8 + float(q["spread"]) * 300))
    hold_strength = 100.0 - flip_risk
    # Second Brain is an independent veto layer, not a claimed AI certainty.
    brain_agrees = abs(impulse) >= 0.55 and float(q["spread"]) <= 0.04 and flip_risk < 42
    if not brain_agrees:
        return {"action": "WAIT", "reason": "Second Brain veto: momentum/quote/flip-risk confirmation missing",
                "quote": q, "momentum": impulse, "flip_risk": flip_risk, "hold_strength": hold_strength}
    kind = "SCALP" if abs(impulse) < 1.2 else "SNIPE"
    return {"action": f"{kind} {side}", "side": side, "yes_side": chosen_yes,
            "entry": float(entry), "quote": q, "momentum": impulse,
            "flip_risk": flip_risk, "hold_strength": hold_strength,
            "signal_strength": min(95.0, 55.0 + abs(impulse) * 12),
            "reason": "directional momentum + independent quote/flip-risk gate passed"}


def discord_url(message_id=None):
    if not WEBHOOK.startswith("https://discord.com/api/webhooks/"):
        return None
    parts = urlsplit(WEBHOOK)
    path = parts.path.rstrip("/")
    if message_id:
        path += "/messages/" + str(message_id)
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


def render_card(m, s):
    q = s.get("quote") or {}
    action = s.get("action", "WAIT")
    color = 0x22C55E if "UP" in action else (0xEF4444 if "DOWN" in action else 0x64748B)
    return {"embeds": [{
        "title": f"{m['_asset']} · KALSHI · {action}",
        "description": m["_display"][:400] + "\n\n" + s.get("reason", ""),
        "color": color,
        "fields": [
            {"name": "YES bid / ask", "value": f"{q.get('yes_bid','—')} / {q.get('yes_ask','—')}", "inline": True},
            {"name": "Entry", "value": f"{s.get('entry','—')}", "inline": True},
            {"name": "Signal strength", "value": f"{s.get('signal_strength',0):.0f}%", "inline": True},
            {"name": "Hold strength", "value": f"{s.get('hold_strength',0):.0f}%", "inline": True},
            {"name": "Flip risk", "value": f"{s.get('flip_risk',50):.0f}%", "inline": True},
            {"name": "Momentum", "value": f"{s.get('momentum',0):+.2f}%", "inline": True},
        ],
        "footer": {"text": "PAPER ONLY · Strength scores are heuristic, not win probabilities"},
        "timestamp": utc_now()
    }], "allowed_mentions": {"parse": []}}


def update_card(m, signal):
    ticker = m["ticker"]
    payload = render_card(m, signal)
    payload_json = json.dumps(payload, sort_keys=True)
    now = time.time()
    with sqlite3.connect(DB_PATH) as db:
        row = db.execute("SELECT message_id,last_payload,last_sent,current_side,initial_side FROM active_cards WHERE ticker=?",
                         (ticker,)).fetchone()
        if row and row[1] == payload_json:
            return
        if row and now - row[2] < UPDATE_SECONDS and row[3] == signal.get("action"):
            return
        message_id = row[0] if row else None
        initial_side = row[4] if row else None
        side = signal.get("side")
        if side and initial_side is None:
            initial_side = side
            db.execute("INSERT OR IGNORE INTO paper_events(ticker,asset,event_type,side,price,pct,ts,detail) VALUES(?,?,?,?,?,?,?,?)",
                       (ticker,m["_asset"],"INITIAL_CALL",side,signal.get("entry"),None,now,signal.get("reason","")))
        elif side and row and row[3] and row[3] != signal.get("action"):
            db.execute("INSERT INTO paper_events(ticker,asset,event_type,side,price,pct,ts,detail) VALUES(?,?,?,?,?,?,?,?)",
                       (ticker,m["_asset"],"FLIP",side,signal.get("entry"),None,now,signal.get("reason","")))
        db.commit()
    if not WEBHOOK:
        print(json.dumps({"ticker":ticker,"signal":signal},default=str)); return
    try:
        if message_id:
            url = discord_url(message_id)
            resp = S.patch(url, json=payload, timeout=8)
            if resp.status_code == 404:
                message_id = None
            else:
                resp.raise_for_status()
        if not message_id:
            resp = S.post(discord_url(), params={"wait":"true"}, json=payload, timeout=8)
            resp.raise_for_status()
            message_id = str(resp.json()["id"])
        with sqlite3.connect(DB_PATH) as db:
            db.execute("""INSERT INTO active_cards(ticker,message_id,last_payload,last_sent,current_side,initial_side,initial_call_ts)
                          VALUES(?,?,?,?,?,?,?) ON CONFLICT(ticker) DO UPDATE SET
                          message_id=excluded.message_id,last_payload=excluded.last_payload,
                          last_sent=excluded.last_sent,current_side=excluded.current_side,
                          initial_side=COALESCE(active_cards.initial_side,excluded.initial_side),
                          initial_call_ts=COALESCE(active_cards.initial_call_ts,excluded.initial_call_ts)""",
                       (ticker,message_id,payload_json,now,signal.get("action"),initial_side,now))
            db.commit()
    except Exception as exc:
        print(f"Discord card update failed for {ticker}: {type(exc).__name__}: {exc}")


def main():
    init_db()
    print("Kalshi unified paper bot started; no orders will be placed.")
    while True:
        started = time.time()
        try:
            markets = discover_markets()
            # Rank qualified candidates; emit WAIT cards only for the best small set to avoid message flooding.
            evaluated = [(m,evaluate(m)) for m in markets]
            qualified = [(m,s) for m,s in evaluated if s.get("side")]
            qualified.sort(key=lambda pair: (pair[1].get("hold_strength",0),
                                            pair[1].get("signal_strength",0),
                                            -pair[1].get("flip_risk",100)), reverse=True)
            selected = {m["ticker"] for m,_ in qualified[:3]}
            for m,s in evaluated:
                if m["ticker"] in selected:
                    update_card(m,s)
            if not markets:
                print("No eligible open BTC/ETH/Gold threshold contracts found.")
        except Exception as exc:
            print(f"Scan error: {type(exc).__name__}: {exc}")
        time.sleep(max(0.2, SCAN_SECONDS - (time.time()-started)))


if __name__ == "__main__":
    main()
