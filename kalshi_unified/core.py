"""Validated primitives for the isolated Kalshi paper-only scanner.

All prices are Decimal dollars as returned by Kalshi's *_dollars fields.
This module never sends order requests.
"""
from __future__ import annotations

import re
import sqlite3
import time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

ASSET_PATTERNS = {
    "BTC": re.compile(r"\\b(bitcoin|btc)\\b", re.I),
    "ETH": re.compile(r"\\b(ethereum|ether|eth)\\b", re.I),
    "GOLD": re.compile(r"\\b(gold|xau)\\b", re.I),
}
DIRECTIONAL_PATTERNS = (
    re.compile(r"\\b(above|higher than|greater than|at least|over|exceed)\\b", re.I),
    re.compile(r"\\b(below|lower than|less than|at most|under)\\b", re.I),
)
PRICE_FIELDS = ("yes_bid_dollars", "yes_ask_dollars", "no_bid_dollars", "no_ask_dollars")


def decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    try:
        if value is None or value == "":
            return default
        result = Decimal(str(value))
        return result if result.is_finite() else default
    except (InvalidOperation, ValueError, TypeError):
        return default


def classify_asset(market: dict[str, Any]) -> str | None:
    """Classify only explicit asset mentions in title/subtitle, never rules alone."""
    title = " ".join(str(market.get(k) or "") for k in
                     ("title", "subtitle", "yes_sub_title", "no_sub_title"))
    matches = [asset for asset, pattern in ASSET_PATTERNS.items() if pattern.search(title)]
    return matches[0] if len(matches) == 1 else None


def yes_outcome_direction(market: dict[str, Any]) -> str | None:
    """Map YES to the underlying asset's threshold direction only when explicit."""
    title = " ".join(str(market.get(k) or "") for k in
                     ("title", "subtitle", "yes_sub_title"))
    if DIRECTIONAL_PATTERNS[0].search(title):
        return "UP"
    if DIRECTIONAL_PATTERNS[1].search(title):
        return "DOWN"
    return None


def contract_is_eligible(market: dict[str, Any]) -> tuple[bool, str]:
    """Fail closed: title must identify one asset and an explicit threshold direction."""
    if market.get("status") != "open":
        return False, "market is not open"
    asset = classify_asset(market)
    if asset is None:
        return False, "asset is ambiguous or absent"
    direction = yes_outcome_direction(market)
    if direction is None:
        return False, "YES settlement direction is ambiguous"
    rules = str(market.get("rules_primary") or "").strip()
    if len(rules) < 30:
        return False, "primary settlement rules are missing/too short"
    close = market.get("close_time") or market.get("expected_expiration_time")
    if not close:
        return False, "market close/expiration metadata missing"
    if not market.get("ticker"):
        return False, "market ticker missing"
    return True, "verified explicit threshold contract"


def make_quote(market: dict[str, Any]) -> dict[str, Decimal] | None:
    yes_bid = decimal(market.get("yes_bid_dollars"))
    yes_ask = decimal(market.get("yes_ask_dollars"))
    no_bid = decimal(market.get("no_bid_dollars"))
    no_ask = decimal(market.get("no_ask_dollars"))
    # In binary contracts, NO bid implies YES ask = 1 - NO bid, and vice versa.
    if yes_ask is None and no_bid is not None:
        yes_ask = Decimal(1) - no_bid
    if no_ask is None and yes_bid is not None:
        no_ask = Decimal(1) - yes_bid
    if yes_bid is None or yes_ask is None or no_bid is None or no_ask is None:
        return None
    if not (Decimal(0) <= yes_bid <= yes_ask <= Decimal(1)):
        return None
    if not (Decimal(0) <= no_bid <= no_ask <= Decimal(1)):
        return None
    return {
        "yes_bid": yes_bid, "yes_ask": yes_ask,
        "no_bid": no_bid, "no_ask": no_ask,
        "yes_mid": (yes_bid + yes_ask) / 2,
        "yes_spread": yes_ask - yes_bid,
        "no_spread": no_ask - no_bid,
    }


def choose_contract_side(yes_momentum_pct: float) -> str:
    """Direction of the Kalshi YES contract price, not a guaranteed asset forecast."""
    return "YES" if yes_momentum_pct > 0 else "NO"


def entry_exit_prices(quote: dict[str, Decimal], side: str) -> tuple[Decimal, Decimal]:
    if side == "YES":
        return quote["yes_ask"], quote["yes_bid"]
    if side == "NO":
        return quote["no_ask"], quote["no_bid"]
    raise ValueError("side must be YES or NO")


def signal_gate(*, quote: dict[str, Decimal] | None, momentum_pct: float | None,
                sample_age: float, minimum_momentum: float = 0.45,
                max_spread: Decimal = Decimal("0.04"),
                min_entry: Decimal = Decimal("0.08"),
                max_entry: Decimal = Decimal("0.92")) -> dict[str, Any]:
    """Conservative two-check gate; strength is heuristic, never a win probability."""
    if quote is None:
        return {"action": "WAIT", "reason": "incomplete or invalid quote"}
    if sample_age > 90:
        return {"action": "WAIT", "reason": "market sample stale"}
    if momentum_pct is None:
        return {"action": "WAIT", "reason": "building independent price history"}
    if abs(momentum_pct) < minimum_momentum:
        return {"action": "WAIT", "reason": "contract-price momentum below threshold"}
    side = choose_contract_side(momentum_pct)
    entry, exit_quote = entry_exit_prices(quote, side)
    spread = quote["yes_spread"] if side == "YES" else quote["no_spread"]
    if spread > max_spread:
        return {"action": "WAIT", "reason": "spread exceeds safety limit"}
    if not min_entry <= entry <= max_entry:
        return {"action": "WAIT", "reason": "entry contract price outside safety band"}
    if exit_quote <= 0 or entry <= exit_quote:
        # An ask must exceed bid; crossed/zero quotes fail closed.
        return {"action": "WAIT", "reason": "invalid executable bid/ask"}
    # Independent second check uses quote quality and momentum stability proxy.
    brain_agrees = abs(momentum_pct) >= minimum_momentum * 1.25 and spread <= max_spread * Decimal("0.75")
    if not brain_agrees:
        return {"action": "WAIT", "reason": "Second Brain veto: independent quote confirmation failed"}
    flip_risk = min(95.0, max(5.0, 52.0 - abs(momentum_pct) * 9 + float(spread) * 500))
    strength = min(90.0, 50.0 + abs(momentum_pct) * 10 - float(spread) * 200)
    kind = "SCALP" if abs(momentum_pct) < 1.0 else "SNIPE"
    return {
        "action": f"{kind} {'UP' if side == 'YES' else 'DOWN'} · BUY {side}", "side": side, "entry": entry,
        "exit_quote": exit_quote, "momentum_pct": momentum_pct,
        "signal_strength": round(strength, 1), "hold_strength": round(100-flip_risk, 1),
        "flip_risk": round(flip_risk, 1),
        "target_pct": 0.03 if kind == "SCALP" else 0.06,
        "stop_pct": 0.025, "max_hold_seconds": 90 if kind == "SCALP" else 240,
        "reason": "momentum + spread + independent confirmation passed",
    }


def init_ledger(db_path: str | Path) -> None:
    with sqlite3.connect(db_path) as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS price_samples(
          ticker TEXT NOT NULL, ts REAL NOT NULL, yes_mid TEXT NOT NULL,
          PRIMARY KEY(ticker,ts));
        CREATE INDEX IF NOT EXISTS idx_price_samples_recent ON price_samples(ticker,ts);
        CREATE TABLE IF NOT EXISTS initial_calls(
          ticker TEXT PRIMARY KEY, asset TEXT NOT NULL, side TEXT NOT NULL,
          entry TEXT NOT NULL, created_at REAL NOT NULL, settlement_result TEXT);
        CREATE TABLE IF NOT EXISTS paper_positions(
          id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL, asset TEXT NOT NULL, side TEXT NOT NULL,
          entry TEXT NOT NULL, opened_at REAL NOT NULL, qty INTEGER NOT NULL DEFAULT 1,
          status TEXT NOT NULL DEFAULT 'OPEN', exit_price TEXT, closed_at REAL,
          pnl_pct REAL, exit_reason TEXT, strategy TEXT, target_pct REAL, stop_pct REAL, max_hold_seconds INTEGER);
        CREATE TABLE IF NOT EXISTS paper_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL, asset TEXT NOT NULL,
          event_type TEXT NOT NULL, side TEXT, price TEXT, pnl_pct REAL,
          ts REAL NOT NULL, detail TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS card_state(
          ticker TEXT PRIMARY KEY, message_id TEXT, payload_hash TEXT,
          updated_at REAL NOT NULL, last_action TEXT);
        """)
        # Upgrade prior isolated versions without deleting paper history.
        existing = {row[1] for row in db.execute("PRAGMA table_info(paper_positions)")}
        if "id" not in existing:
            db.execute("ALTER TABLE paper_positions RENAME TO paper_positions_legacy")
            db.execute("""CREATE TABLE paper_positions(
                id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL, asset TEXT NOT NULL,
                side TEXT NOT NULL, entry TEXT NOT NULL, opened_at REAL NOT NULL,
                qty INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'OPEN',
                exit_price TEXT, closed_at REAL, pnl_pct REAL, exit_reason TEXT,
                strategy TEXT, target_pct REAL, stop_pct REAL, max_hold_seconds INTEGER)""")
            oldcols = {row[1] for row in db.execute("PRAGMA table_info(paper_positions_legacy)")}
            cols = ["ticker","asset","side","entry","opened_at","qty","status","exit_price","closed_at","pnl_pct","exit_reason"]
            copycols = [col for col in cols if col in oldcols]
            if copycols:
                names = ",".join(copycols)
                db.execute(f"INSERT INTO paper_positions({names}) SELECT {names} FROM paper_positions_legacy")
            db.execute("DROP TABLE paper_positions_legacy")
            existing = {row[1] for row in db.execute("PRAGMA table_info(paper_positions)")}
        for name, declaration in (("strategy", "TEXT"), ("target_pct", "REAL"),
                                  ("stop_pct", "REAL"), ("max_hold_seconds", "INTEGER")):
            if name not in existing:
                db.execute(f"ALTER TABLE paper_positions ADD COLUMN {name} {declaration}")
        db.execute("CREATE UNIQUE INDEX IF NOT EXISTS one_open_paper_position_per_ticker ON paper_positions(ticker) WHERE status='OPEN'")
        db.commit()


def record_sample(db_path: str | Path, ticker: str, yes_mid: Decimal,
                  now: float | None = None, lookback: int = 30) -> tuple[float | None, float]:
    now = now or time.time()
    with sqlite3.connect(db_path) as db:
        db.execute("INSERT OR REPLACE INTO price_samples VALUES(?,?,?)",
                   (ticker, now, str(yes_mid)))
        db.execute("DELETE FROM price_samples WHERE ticker=? AND ts<?", (ticker, now-3600))
        row = db.execute("SELECT yes_mid,ts FROM price_samples WHERE ticker=? AND ts<=? ORDER BY ts DESC LIMIT 1",
                         (ticker, now-lookback)).fetchone()
        db.commit()
    if not row or decimal(row[0]) in (None, Decimal(0)):
        return None, 0.0
    age = now - row[1]
    return float((yes_mid-decimal(row[0]))/decimal(row[0])*100), age


def open_paper_trade(db_path: str | Path, ticker: str, asset: str,
                     signal: dict[str, Any], now: float | None = None) -> bool:
    if signal.get("side") not in ("YES", "NO") or signal.get("entry") is None:
        return False
    now = now or time.time()
    entry = decimal(signal["entry"])
    with sqlite3.connect(db_path) as db:
        existing = db.execute("SELECT status FROM paper_positions WHERE ticker=? ORDER BY opened_at DESC LIMIT 1", (ticker,)).fetchone()
        if existing and existing[0] == "OPEN":
            return False
        db.execute("""INSERT INTO paper_positions
                   (ticker,asset,side,entry,opened_at,qty,status,strategy,target_pct,stop_pct,max_hold_seconds)
                   VALUES(?,?,?,?,?,1,'OPEN',?,?,?,?)""",
                   (ticker, asset, signal["side"], str(entry), now,
                    signal.get("action", "SCALP").split()[0],
                    float(signal.get("target_pct", 0.03))*100,
                    float(signal.get("stop_pct", 0.025))*100,
                    int(signal.get("max_hold_seconds", 90))))
        db.execute("INSERT OR IGNORE INTO initial_calls(ticker,asset,side,entry,created_at) VALUES(?,?,?,?,?)",
                   (ticker, asset, signal["side"], str(entry), now))
        db.execute("INSERT INTO paper_events(ticker,asset,event_type,side,price,ts,detail) VALUES(?,?,?,?,?,?,?)",
                   (ticker, asset, "ENTRY", signal["side"], str(entry), now, signal.get("action","")))
        db.commit()
    return True


def close_paper_trade(db_path: str | Path, ticker: str, exit_price: Decimal,
                      reason: str, now: float | None = None) -> dict[str, Any] | None:
    now = now or time.time()
    with sqlite3.connect(db_path) as db:
        row = db.execute("SELECT asset,side,entry,opened_at,status FROM paper_positions WHERE ticker=? AND status='OPEN' ORDER BY opened_at DESC LIMIT 1",
                         (ticker,)).fetchone()
        if not row or row[4] != "OPEN":
            return None
        entry = decimal(row[2])
        # One contract: percentage return on paid premium, sell at executable bid.
        pnl = float((exit_price-entry)/entry*100) if entry and entry > 0 else 0.0
        db.execute("UPDATE paper_positions SET status='CLOSED',exit_price=?,closed_at=?,pnl_pct=?,exit_reason=? WHERE ticker=? AND status='OPEN'",
                   (str(exit_price), now, pnl, reason, ticker))
        db.execute("INSERT INTO paper_events(ticker,asset,event_type,side,price,pnl_pct,ts,detail) VALUES(?,?,?,?,?,?,?,?)",
                   (ticker,row[0],"EXIT",row[1],str(exit_price),pnl,now,reason))
        db.commit()
    return {"ticker": ticker, "asset": row[0], "side": row[1], "entry": entry,
            "exit": exit_price, "pnl_pct": pnl, "reason": reason}


def record_settlement(db_path: str | Path, ticker: str, result: str) -> bool:
    """Record official market result once; never overwrite an existing result."""
    result = result.upper()
    if result not in ("YES", "NO", "VOID"):
        return False
    with sqlite3.connect(db_path) as db:
        row = db.execute("SELECT side,settlement_result FROM initial_calls WHERE ticker=?", (ticker,)).fetchone()
        if not row or row[1] is not None:
            return False
        outcome = "VOID" if result == "VOID" else ("WIN" if row[0] == result else "LOSS")
        db.execute("UPDATE initial_calls SET settlement_result=? WHERE ticker=?", (outcome,ticker))
        db.execute("INSERT INTO paper_events(ticker,asset,event_type,side,ts,detail) SELECT ticker,asset,'SETTLEMENT',side,?,? FROM initial_calls WHERE ticker=?",
                   (time.time(), outcome, ticker))
        db.commit()
    return True


def stats(db_path: str | Path) -> dict[str, Any]:
    with sqlite3.connect(db_path) as db:
        calls = db.execute("SELECT settlement_result FROM initial_calls WHERE settlement_result IN ('WIN','LOSS') ORDER BY created_at").fetchall()
        trades = db.execute("SELECT pnl_pct FROM paper_positions WHERE status='CLOSED' ORDER BY closed_at").fetchall()
    wins = sum(1 for (r,) in calls if r == "WIN")
    losses = len(calls)-wins
    streak = 0
    for (result,) in reversed(calls):
        if not result:
            continue
        if streak == 0:
            streak = 1 if result == "WIN" else -1
        elif (streak > 0 and result == "WIN") or (streak < 0 and result == "LOSS"):
            streak += 1 if streak > 0 else -1
        else:
            break
    best = 0
    run = 0
    for (result,) in calls:
        run = run+1 if result == "WIN" else 0
        best = max(best, run)
    pnl = [r[0] for r in trades if r[0] is not None]
    return {"settled_calls": len(calls), "wins": wins, "losses": losses,
            "win_rate_pct": round(100*wins/len(calls),1) if calls else None,
            "current_initial_streak": streak, "best_initial_win_streak": best,
            "managed_closed_trades": len(pnl), "managed_avg_pnl_pct": round(sum(pnl)/len(pnl),2) if pnl else None}
