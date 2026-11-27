#!/usr/bin/env python3
"""Polymarket multi-asset adaptive paper scanner.

Paper trading only: this program uses public market data and never submits orders.
It discovers supported crypto 5m+15m UP/DOWN markets via Gamma search,
uses independent reference-price feeds, and learns only from resolved paper calls.
"""
from __future__ import annotations
import argparse, json, math, os, re, sqlite3, sys, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
YAHOO = "https://query1.finance.yahoo.com/v8/finance/chart"
DB_PATH = os.getenv("POLY_BOT_DB", "poly_multi_asset.sqlite3")
WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
# Faster risk-aware refresh: full radar cycle targets 5 seconds; API/network delays can extend it.
SCAN_SECONDS = 5
SIGNAL_REPEAT_SECONDS = int(os.getenv("SIGNAL_REPEAT_SECONDS", "30"))
# RISK-FIRST MODE: stricter entry quality, smaller paper exposure, earlier stop.
STAKE = min(1.00, float(os.getenv("PAPER_STAKE_USD", "1")))
DAILY_LOSS_LIMIT = min(4.00, float(os.getenv("DAILY_LOSS_LIMIT_USD", "4")))
MAX_OPEN = min(2, int(os.getenv("MAX_OPEN_PAPER_TRADES", "2")))
MIN_DEPTH = max(1000.00, float(os.getenv("MIN_BOOK_DEPTH_USD", "1000")))
MAX_SPREAD = min(0.025, float(os.getenv("MAX_SPREAD", "0.025")))
MIN_ASK, MAX_ASK = 0.25, 0.75
# Polymarket's actively listed short-window crypto set; do not emit false NO-MARKET cards for unsupported tickers.
TIMEFRAMES = {a: (5, 15) for a in ("BTC","ETH","SOL","XRP","DOGE","BNB","HYPE")}
# Fixed top-five major assets supported by the short-window market scanner.
TOP_ASSETS = ("BTC","ETH","SOL","XRP","BNB")
SYMBOLS = {a: f"{a}USDT" for a in TIMEFRAMES}
_DISCOVERY_CACHE = {}
_CANDLE_CACHE = {}
_ASSET_CYCLE = 0
_ASSETS_PER_CYCLE = 3
_LATEST_LINES = {}
S = requests.Session(); S.headers.update({"User-Agent": "Poly-Multi-Asset-Paper-Scanner/1.0"})


def now(): return datetime.now(timezone.utc)
def iso(): return now().isoformat()
def jlist(v):
    if isinstance(v, list): return v
    if isinstance(v, str):
        try:
            x=json.loads(v); return x if isinstance(x,list) else []
        except Exception: return []
    return []
def get(url, params=None):
    r=S.get(url, params=params, timeout=12); r.raise_for_status(); return r.json()
def send(text, embed=None):
    if not WEBHOOK.startswith(("https://discord.com/api/webhooks/", "https://discordapp.com/api/webhooks/")):
        print("Discord webhook missing or invalid; message not sent."); return False
    payload={"content":text[:1900], "allowed_mentions":{"parse":[]}}
    if embed: payload["embeds"]=embed if isinstance(embed,list) else [embed]
    try:
        r=S.post(WEBHOOK, json=payload, timeout=12); r.raise_for_status(); return True
    except Exception as e: print(f"Discord send failed: {type(e).__name__}: {e}"); return False

def send_signal_once(c, key, message):
    """Repeat a current paper signal only after its cooldown; avoid scan-loop spam."""
    row=c.execute("SELECT sent_at FROM signal_alerts WHERE alert_key=?",(key,)).fetchone()
    if row and time.time()-float(row["sent_at"])<SIGNAL_REPEAT_SECONDS: return False
    if send(message):
        c.execute("INSERT INTO signal_alerts(alert_key,sent_at) VALUES(?,?) ON CONFLICT(alert_key) DO UPDATE SET sent_at=excluded.sent_at",(key,time.time()))
        c.commit()
        print(f"Discord paper signal sent: {key}",flush=True)
        return True
    return False


def db():
    c=sqlite3.connect(DB_PATH); c.row_factory=sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    c.executescript("""
    CREATE TABLE IF NOT EXISTS calls(
      id INTEGER PRIMARY KEY, market_id TEXT NOT NULL, slug TEXT NOT NULL, asset TEXT NOT NULL,
      timeframe INTEGER NOT NULL, side TEXT NOT NULL, token_id TEXT NOT NULL,
      entry REAL NOT NULL, stake REAL NOT NULL, qty REAL NOT NULL, score INTEGER NOT NULL,
      features TEXT NOT NULL, created_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN',
      remaining_qty REAL NOT NULL DEFAULT 0, realized_pnl REAL NOT NULL DEFAULT 0,
      partial_taken INTEGER NOT NULL DEFAULT 0, last_action TEXT NOT NULL DEFAULT '',
      result TEXT, pnl REAL, resolved_at TEXT, UNIQUE(market_id,side));
    CREATE TABLE IF NOT EXISTS observations(
      id INTEGER PRIMARY KEY, asset TEXT NOT NULL, timeframe INTEGER NOT NULL,
      market_id TEXT NOT NULL, side TEXT NOT NULL, score INTEGER NOT NULL,
      features TEXT NOT NULL, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS bot_state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS signal_alerts(
      alert_key TEXT PRIMARY KEY, sent_at REAL NOT NULL);
    CREATE TABLE IF NOT EXISTS snipe_ticks(
      market_id TEXT NOT NULL, side TEXT NOT NULL, ask REAL NOT NULL, ts REAL NOT NULL,
      PRIMARY KEY(market_id,side));
    CREATE TABLE IF NOT EXISTS forecast_journal(
      market_id TEXT NOT NULL, timeframe INTEGER NOT NULL, slug TEXT NOT NULL,
      asset TEXT NOT NULL, initial_side TEXT NOT NULL, current_side TEXT NOT NULL,
      flip_count INTEGER NOT NULL DEFAULT 0, side_history TEXT NOT NULL DEFAULT '[]',
      features TEXT NOT NULL DEFAULT '{}', status TEXT NOT NULL DEFAULT 'OPEN',
      outcome TEXT, winning_side TEXT, last_seen REAL NOT NULL,
      last_check REAL NOT NULL DEFAULT 0, resolved_at TEXT,
      PRIMARY KEY(market_id,timeframe));
    """)
    # Safe additive migrations for databases created by an earlier draft.
    cols={r[1] for r in c.execute("PRAGMA table_info(calls)")}
    for name,decl in {
        "remaining_qty":"REAL NOT NULL DEFAULT 0", "realized_pnl":"REAL NOT NULL DEFAULT 0",
        "partial_taken":"INTEGER NOT NULL DEFAULT 0", "last_action":"TEXT NOT NULL DEFAULT ''"
    }.items():
        if name not in cols: c.execute(f"ALTER TABLE calls ADD COLUMN {name} {decl}")
    c.execute("UPDATE calls SET remaining_qty=qty WHERE status='OPEN' AND remaining_qty=0 AND qty>0")
    c.commit(); return c


def discover(asset, tf):
    """Find current crypto Up/Down markets using their timestamped event slugs."""
    if asset in TIMEFRAMES and tf in (5,15):
        prefix=asset.lower()
        seconds=tf*60
        slot=int(time.time())//seconds*seconds
        cache_key=(asset,tf,slot)
        cached=_DISCOVERY_CACHE.get(cache_key)
        if cached and time.time()-cached[0]<12: return cached[1]
        found=[]
        for stamp in (slot,slot+seconds):
            slug=f"{prefix}-updown-{tf}m-{stamp}"
            try:
                ev=get(f"{GAMMA}/events/slug/{slug}")
                if isinstance(ev,dict): found.extend(ev.get("markets",[]) or [])
                elif isinstance(ev,list):
                    for e in ev: found.extend(e.get("markets",[]) or [])
            except Exception as e:
                print(f"Exact slug {slug}: {type(e).__name__}: {str(e)[:100]}",flush=True)
        candidates=[]
        for m in found:
            active=m.get("active",True) not in (False,"false",0) and m.get("closed",False) not in (True,"true",1)
            ts=jlist(m.get("clobTokenIds")); outs=jlist(m.get("outcomes"))
            if active and len(ts)>=2 and len(outs)>=2 and f"{prefix}-updown-{tf}m-" in str(m.get("slug","")).lower():
                m["_tokens"]=ts; m["_outcomes"]=outs; candidates.append(m)
        if candidates:
            def start(m):
                try:return int(str(m.get("slug","")).rsplit("-",1)[1])
                except Exception:return 10**18
            now_ts=time.time()
            live=[m for m in candidates if start(m)<=now_ts<start(m)+seconds]
            chosen=live[0] if live else min(candidates,key=start)
            _DISCOVERY_CACHE[cache_key]=(time.time(),chosen)
            return chosen
        print(f"Market discovery: exact {asset} {tf}m event slugs not returned.",flush=True)
        _DISCOVERY_CACHE[cache_key]=(time.time(),None)
        return None
    return None

def tokens(m):
    out={}
    for label, tid in zip(m.get("_outcomes",jlist(m.get("outcomes"))), m.get("_tokens",jlist(m.get("clobTokenIds")))):
        x=str(label).strip().lower()
        if x in ("up","yes"): out["UP"]=str(tid)
        elif x in ("down","no"): out["DOWN"]=str(tid)
    return out


def reference_candles(asset):
    if asset in SYMBOLS:
        cached=_CANDLE_CACHE.get(asset)
        if cached and time.time()-cached[0]<8: return cached[1]
        rows=get("https://data-api.binance.vision/api/v3/klines", {"symbol":SYMBOLS[asset],"interval":"1m","limit":100})
        candles=[(float(r[4]),float(r[5]),float(r[2]),float(r[3])) for r in rows[:-1]]
        _CANDLE_CACHE[asset]=(time.time(),candles)
        return candles
    # XAUUSD=X is an indicative public reference feed, not the Polymarket settlement oracle.
    data=get(f"{YAHOO}/XAUUSD=X", {"range":"1d","interval":"1m"})
    result=data["chart"]["result"][0]; q=result["indicators"]["quote"][0]
    closes=q.get("close") or []; volumes=q.get("volume") or [0]*len(closes)
    return [(float(p),float(v or 0)) for p,v in zip(closes,volumes) if p is not None]


def indicators(candles):
    closes=[x[0] for x in candles]; vols=[x[1] for x in candles]
    highs=[x[2] if len(x)>2 else x[0] for x in candles]
    lows=[x[3] if len(x)>3 else x[0] for x in candles]
    if len(closes)<55: raise RuntimeError(f"Reference feed only returned {len(closes)} completed candles")
    recent_high=max(highs[-20:]); recent_low=min(lows[-20:])
    range_position=100*(closes[-1]-recent_low)/max(1e-12,recent_high-recent_low)
    range_pct=100*(recent_high-recent_low)/max(1e-12,closes[-1])
    def ema(vals,n):
        a=2/(n+1); out=[vals[0]]
        for x in vals[1:]: out.append(a*x+(1-a)*out[-1])
        return out
    changes=[closes[i]-closes[i-1] for i in range(1,len(closes))]
    gains=[max(x,0) for x in changes[-14:]]; losses=[max(-x,0) for x in changes[-14:]]
    ag=sum(gains)/14; al=sum(losses)/14; rsi=100-(100/(1+ag/al)) if al else (100 if ag else 50)
    e9,e21,e50=ema(closes,9),ema(closes,21),ema(closes,50)
    macd=ema(closes,12)[-1]-ema(closes,26)[-1]
    prev=ema(closes,12)[-2]-ema(closes,26)[-2]
    move1=(closes[-1]/closes[-2]-1)*100; move3=(closes[-1]/closes[-4]-1)*100; move10=(closes[-1]/closes[-11]-1)*100
    vavg=sum(vols[-21:-1])/max(1,len(vols[-21:-1])); vr=vols[-1]/vavg if vavg else 0
    votes=[1 if e9[-1]>e21[-1]>e50[-1] else -1 if e9[-1]<e21[-1]<e50[-1] else 0,
           1 if 52<=rsi<=68 else -1 if 32<=rsi<=48 else 0,
           1 if macd>prev else -1 if macd<prev else 0,
           1 if move3>=0.035 and move10>0 else -1 if move3<=-0.035 and move10<0 else 0,
           1 if vr>=1.1 and move3>0 else -1 if vr>=1.1 and move3<0 else 0]
    return {"score":sum(votes),"side":"UP" if sum(votes)>0 else "DOWN" if sum(votes)<0 else "WAIT",
            "rsi":rsi,"move1":move1,"move3":move3,"move10":move10,"volume_ratio":vr,"price":closes[-1],"range_high":recent_high,"range_low":recent_low,"range_position":range_position,"range_pct":range_pct,"votes":votes}


def book_stats(token):
    b=get(f"{CLOB}/book",{"token_id":token}); bids=sorted(b.get("bids",[]),key=lambda x:float(x["price"]),reverse=True); asks=sorted(b.get("asks",[]),key=lambda x:float(x["price"]))
    if not bids or not asks:return None
    bid=float(bids[0]["price"]); ask=float(asks[0]["price"])
    bd=sum(float(x["price"])*float(x["size"]) for x in bids[:10]); ad=sum(float(x["price"])*float(x["size"]) for x in asks[:10])
    return {"bid":bid,"ask":ask,"spread":ask-bid,"depth":min(bd,ad),"imbalance":(bd-ad)/max(1,bd+ad)}


def shared_learning_data():
    try: return json.loads(Path("data/shared_learning.json").read_text(encoding="utf-8"))
    except Exception: return {}

def btc_peer_summary():
    """Use the original BTC bot's published results as a conservative peer signal."""
    try:
        data=json.loads(Path("docs/data_btc.json").read_text(encoding="utf-8"))
        updated=datetime.fromisoformat(str(data.get("updated_at","")).replace("Z","+00:00"))
        if (now()-updated).total_seconds()>14400: return {}
        s=data.get("summary",{}); n=int(s.get("resolved_sample",0) or 0); wr=s.get("win_rate")
        return {"sample":n,"win_rate":float(wr) if wr is not None else None,"source":"original BTC dashboard","updated_at":data.get("updated_at")}
    except Exception: return {}

def update_shared_learning(c, bot="multi"):
    rows=c.execute("SELECT pnl FROM calls WHERE status='RESOLVED' ORDER BY id DESC LIMIT 50").fetchall()
    n=len(rows); wins=sum(float(r["pnl"] or 0)>0 for r in rows); losses=sum(float(r["pnl"] or 0)<0 for r in rows)
    data=shared_learning_data()
    data[bot]={"sample":n,"wins":wins,"losses":losses,"win_rate":round(100*wins/n,1) if n else None,"updated_at":iso()}
    data["updated_at"]=iso()
    Path("data").mkdir(exist_ok=True)
    Path("data/shared_learning.json").write_text(json.dumps(data,indent=2),encoding="utf-8")

def learning_adjustment(c, asset, tf):
    """Use resolved outcomes and market churn to tighten, never loosen, entries."""
    rows=c.execute(
        "SELECT outcome,flip_count FROM forecast_journal WHERE asset=? AND timeframe=? "
        "AND status='RESOLVED' ORDER BY resolved_at DESC LIMIT 30",(asset,tf)
    ).fetchall()
    n=len(rows)
    flips=sum(r["outcome"]=="FLIP" or int(r["flip_count"] or 0)>0 for r in rows)
    flip_rate=flips/n if n else 0.0
    peer=btc_peer_summary() or shared_learning_data().get("btc",{})
    peer_pause=peer.get("sample",0)>=10 and peer.get("win_rate") is not None and peer.get("win_rate")<45
    if n>=12:
        wins=sum(r["outcome"]=="WIN" for r in rows)
        losses=sum(r["outcome"]=="LOSS" for r in rows)
        decisive=wins+losses
        wr=100*wins/decisive if decisive else None
        weak=wr is not None and wr<55
        caution=(wr is not None and wr<55) or flip_rate>=0.30
        # Risk-first: pause earlier when enough resolved calls show a weak edge.
        pause=(wr is not None and wr<48) or flip_rate>=0.40 or peer_pause
        return {"sample":n,"threshold":5 if weak or peer_pause or flip_rate>=0.35 else 4,
                "pause":pause,"caution":caution,"win_rate":wr,"flips":flips,
                "flip_rate":flip_rate,"peer":peer}
    old=c.execute(
        "SELECT pnl FROM calls WHERE asset=? AND timeframe=? AND status='RESOLVED' "
        "ORDER BY resolved_at DESC LIMIT 30",(asset,tf)
    ).fetchall()
    old_n=len(old); old_w=sum(float(r["pnl"] or 0)>0 for r in old)
    old_wr=100*old_w/old_n if old_n else None
    weak=old_n>=12 and old_wr<55
    caution=(n>=8 and flip_rate>=0.30) or weak
    # Pause earlier on weak historical paper performance or frequent flips.
    pause=peer_pause or (old_n>=12 and old_wr<48) or (n>=12 and flip_rate>=0.40)
    return {"sample":n,"threshold":5 if peer_pause or weak or (n>=8 and flip_rate>=0.35) else 4,
            "pause":pause,"caution":caution,"win_rate":old_wr,"flips":flips,
            "flip_rate":flip_rate,"peer":peer}

def record_forecast(c, market, asset, tf, side, features):
    """Track first call and every direction change for each market window."""
    if side not in ("UP","DOWN"): return
    market_id=str(market.get("id","")); slug=str(market.get("slug",""))
    if not market_id: return
    row=c.execute("SELECT * FROM forecast_journal WHERE market_id=? AND timeframe=?",(market_id,tf)).fetchone()
    stamp=time.time()
    if row is None:
        history=[{"side":side,"at":iso()}]
        c.execute("INSERT OR IGNORE INTO forecast_journal(market_id,timeframe,slug,asset,initial_side,current_side,flip_count,side_history,features,last_seen) VALUES(?,?,?,?,?,?,?,?,?,?)",(market_id,tf,slug,asset,side,side,0,json.dumps(history),json.dumps(features),stamp))
    else:
        current=row["current_side"]; flips=int(row["flip_count"])
        try: history=json.loads(row["side_history"] or "[]")
        except Exception: history=[]
        if side!=current:
            flips+=1; history.append({"side":side,"at":iso()}); history=history[-30:]
            print(f"Forecast flip tracked: {asset} {tf}m {current} -> {side} (flip {flips})",flush=True)
        c.execute("UPDATE forecast_journal SET current_side=?,flip_count=?,side_history=?,features=?,last_seen=? WHERE market_id=? AND timeframe=?",(side,flips,json.dumps(history),json.dumps(features),stamp,market_id,tf))
    c.commit()

def resolve_forecasts(c):
    """Resolve expired forecasts; correct final direction after a flip is a neutral FLIP."""
    rows=c.execute("SELECT * FROM forecast_journal WHERE status='OPEN' ORDER BY last_seen DESC LIMIT 120").fetchall()
    stamp=time.time()
    for row in rows:
        try:
            try: market_start=int(row["slug"].rsplit("-",1)[1])
            except Exception: continue
            if stamp < market_start+int(row["timeframe"])*60+15: continue
            if stamp-float(row["last_check"] or 0)<180: continue
            c.execute("UPDATE forecast_journal SET last_check=? WHERE market_id=? AND timeframe=?",(stamp,row["market_id"],row["timeframe"])); c.commit()
            data=get(f"{GAMMA}/markets/{row['market_id']}")
            if not data.get("closed"): continue
            outcomes=jlist(data.get("outcomes")); prices=jlist(data.get("outcomePrices"))
            mapping={str(o).lower():float(p) for o,p in zip(outcomes,prices)}
            winning="UP" if mapping.get("up",mapping.get("yes",0))>=0.99 else "DOWN" if mapping.get("down",mapping.get("no",0))>=0.99 else None
            if not winning: continue
            correct=row["current_side"]==winning
            outcome="FLIP" if correct and int(row["flip_count"])>0 else "WIN" if correct else "LOSS"
            c.execute("UPDATE forecast_journal SET status='RESOLVED',outcome=?,winning_side=?,resolved_at=? WHERE market_id=? AND timeframe=?",(outcome,winning,iso(),row["market_id"],row["timeframe"]))
            c.commit()
            print(f"Forecast resolved: {row['asset']} {row['timeframe']}m {outcome} · flips={row['flip_count']}",flush=True)
        except Exception as e: print(f"Forecast resolve {row['market_id']}: {type(e).__name__}: {e}",flush=True)

def save_observation(c, asset, tf, m, tech):
    c.execute("INSERT INTO observations(asset,timeframe,market_id,side,score,features,created_at) VALUES(?,?,?,?,?,?,?)",
              (asset,tf,str(m.get("id")),tech["side"],tech["score"],json.dumps(tech),iso())); c.commit()


def second_brain(tech, stats, side, c=None, asset="BTC", tf=5, market_id=None):
    """Challenge a directional call with current evidence and resolved flip memory."""
    if side not in ("UP","DOWN"):
        return {"agrees":False,"label":"NO VERDICT · WAIT","score":0,
                "flip_risk":95,"memory":None,"debate":"No directional case; WAIT."}
    sign=1 if side=="UP" else -1
    m1=float(tech.get("move1",0)); m3=float(tech.get("move3",0)); m10=float(tech.get("move10",0))
    rsi=float(tech.get("rsi",50)); vr=float(tech.get("volume_ratio",0))
    imbalance=float(stats.get("imbalance",0))*sign
    pos=tech.get("range_position"); support=[]; objections=[]
    if m1*sign>0 and m3*sign>0: support.append("1m/3m momentum aligned")
    else: objections.append("short momentum conflicts")
    if m10*sign>0: support.append("10m trend supports")
    else: objections.append("10m trend unconfirmed")
    if (38<=rsi<=68 if side=="UP" else 32<=rsi<=62): support.append("RSI in workable zone")
    elif (side=="UP" and rsi>=72) or (side=="DOWN" and rsi<=28): objections.append("RSI stretched")
    else: objections.append("RSI outside confirmation zone")
    if vr>=1.10 and m3*sign>0: support.append("volume confirms momentum")
    else: objections.append("volume confirmation missing")
    if imbalance>=0.05: support.append("order-book pressure supports")
    elif imbalance<=-0.10: objections.append("order-book pressure opposes")
    else: objections.append("book pressure weak/neutral")
    if pos is not None:
        p=float(pos)
        if (side=="UP" and p>=90) or (side=="DOWN" and p<=10): objections.append("price near range exhaustion")
        elif (side=="UP" and p<=25) or (side=="DOWN" and p>=75): support.append("price location leaves room")
    memory=learning_adjustment(c,asset,tf) if c is not None else {
        "sample":0,"flip_rate":0.0,"caution":False,"pause":False,"win_rate":None}
    current_flips=0
    if c is not None and market_id:
        row=c.execute("SELECT flip_count FROM forecast_journal WHERE market_id=? AND timeframe=?",
                      (str(market_id),tf)).fetchone()
        current_flips=int(row["flip_count"] or 0) if row else 0
    if current_flips>=2: objections.append(f"this market has flipped {current_flips} times · choppy")
    if memory["sample"]>=8 and memory["flip_rate"]>=0.35:
        objections.append(f"resolved {tf}m windows flip often ({memory['flip_rate']*100:.0f}%)")
    if memory["caution"]: objections.append("recent resolved memory asks for stronger confirmation")
    if memory["pause"]: objections.append("resolved-paper safety pause is active")
    score=len(support)-len(objections)
    required_score=3 if current_flips>=2 or memory["caution"] else 2
    agrees=(score>=required_score and len(support)>=3 and not memory["pause"] and current_flips<3)
    risk=flip_risk(tech,side,float(stats.get("imbalance",0)))
    risk=min(95,risk+min(24,current_flips*8)+int(memory["flip_rate"]*20))
    verdict="DEFENSE WINS · CONFIRMED" if agrees else "PROSECUTION WINS · WAIT"
    debate=(f"FOR: {', '.join(support[:3]) or 'no strong evidence'} | "
            f"AGAINST: {', '.join(objections[:3]) or 'no strong objection'} | "
            f"MEMORY: {memory['sample']} resolved, {memory['flip_rate']*100:.0f}% flip rate | "
            f"CURRENT WINDOW: {current_flips} flips | VERDICT: {verdict}")
    return {"agrees":agrees,"label":"CONFIRMS" if agrees else "DISAGREES · SIT OUT",
            "score":score,"flip_risk":risk,"market_flips":current_flips,
            "memory":memory,"debate":debate}

def scalp_call(c, asset, tf, market, tech):
    """Readable 5m scalp idea only when momentum and liquidity are aligned."""
    if tf!=5: return "SCALP: 5m only · sit out this timeframe"
    seconds_left=int(300-(time.time()%300))
    if not 90<=seconds_left<=240: return "SCALP: SIT OUT · wait for the clean 90–240 second window"
    side=tech.get("side","WAIT")
    if side not in ("UP","DOWN") or abs(int(tech.get("score",0)))<4:
        return "SCALP: SIT OUT · direction is not strong enough"
    m1=float(tech.get("move1",0)); m3=float(tech.get("move3",0)); sign=1 if side=="UP" else -1
    if m1*sign<=0 or m3*sign<=0:
        return "SCALP: SIT OUT · 1m and 3m momentum disagree"
    mapping=tokens(market); tid=mapping.get(side)
    if not tid: return "SCALP: SIT OUT · outcome token not verified"
    stats=book_stats(tid)
    if not stats: return "SCALP: SIT OUT · order book unavailable"
    if not (0.20<=stats["ask"]<=0.80 and stats["spread"]<=0.02 and stats["depth"]>=1000):
        return "SCALP: SIT OUT · price, spread, or liquidity is not safe enough"
    brain=second_brain(tech,stats,side,c=c,asset=asset,tf=tf,market_id=m.get('id'))
    if not brain["agrees"]: return "SCALP: SIT OUT · second brain does not confirm"
    ask=float(stats["ask"]); target=ask*1.04; stop=ask*0.97
    return f"SCALP CALL {side} · entry {ask:.3f} · target {target:.3f} (+4%) · stop {stop:.3f} (-3%) · two brains agree"

def snipe_update(c, market_id, side, stats, tech, brain):
    """Track real Polymarket contract ask movement; meter is strength, not win odds."""
    ask=float(stats["ask"]); stamp=time.time()
    row=c.execute("SELECT ask,ts FROM snipe_ticks WHERE market_id=? AND side=?",
                  (str(market_id),side)).fetchone()
    move=None
    if row and 2<=stamp-float(row["ts"])<=90 and float(row["ask"])>0:
        move=(ask/float(row["ask"])-1)*100
    c.execute("INSERT INTO snipe_ticks(market_id,side,ask,ts) VALUES(?,?,?,?) "
              "ON CONFLICT(market_id,side) DO UPDATE SET ask=excluded.ask,ts=excluded.ts",
              (str(market_id),side,ask,stamp)); c.commit()
    abs_move=abs(move or 0)
    band="NO RECENT BASELINE" if move is None else "MICRO 1–9%" if 1<=abs_move<10 else "FAST 10–30%" if abs_move<30 else "EXTREME 30%+" if abs_move>=30 else "QUIET <1%"
    sign=1 if side=="UP" else -1
    book_ok=float(stats.get("imbalance",0))*sign>=0.05 and float(stats.get("spread",1))<=MAX_SPREAD and float(stats.get("depth",0))>=MIN_DEPTH
    confirmed=move is not None and abs_move>=1 and (move*sign)>0 and book_ok and brain.get("agrees") and int(brain.get("flip_risk",95))<70
    strength=max(0,min(100,round(abs_move*3 + (20 if book_ok else 0) + (20 if brain.get("agrees") else 0)))) if move is not None else 0
    if move is None: status="SNIPE WATCH · building price baseline"
    elif abs_move<1: status="SNIPE WAIT · move under 1%"
    elif int(brain.get("flip_risk",95))>=70: status="SNIPE FLIP RISK · entry blocked"
    elif confirmed: status="SNIPE CONFIRMED · evidence aligned"
    else: status="SNIPE WATCH · not confirmed"
    move_text="—" if move is None else f"{move:+.2f}%"
    return f"🎯 {status} · contract move {move_text} · {band} · strength {strength}/100 (not win probability)"


def maybe_enter(c, asset, tf, m, tech):
    side=tech["side"]
    if side not in ("UP","DOWN"):return "WAIT · mixed indicators"
    mapping=tokens(m); tid=mapping.get(side)
    if not tid:return "WAIT · outcome tokens not mapped safely"
    stats=book_stats(tid)
    if not stats:return "WAIT · order book unavailable"
    learn=learning_adjustment(c,asset,tf)
    required=learn["threshold"]
    signed_imb=stats["imbalance"]*(1 if side=="UP" else -1)
    confirm=(tech["move1"]>0 and tech["move3"]>0) if side=="UP" else (tech["move1"]<0 and tech["move3"]<0)
    brain=second_brain(tech,stats,side,c=c,asset=asset,tf=tf,market_id=m.get('id'))
    snipe=snipe_update(c,m.get("id"),side,stats,tech,brain)
    current_risk=int(brain.get("flip_risk",95))
    pm_quote=(f"POLYMARKET {side} · bid {stats['bid']:.3f} / ask {stats['ask']:.3f} · buy-side implied {stats['ask']*100:.1f}% {pct_bar(round(stats['ask']*100))} (contract price, not bot win probability)")
    reasons=[]
    if abs(tech["score"])<required:reasons.append(f"signal votes {abs(tech['score'])}/5; need {required}")
    if not confirm:reasons.append("short momentum not aligned")
    if not brain["agrees"]:reasons.append("SECOND BRAIN: "+brain["label"])
    if current_risk>=70:reasons.append(f"FLIP RISK {current_risk}/100 · no new entry")
    if signed_imb< -0.10:reasons.append("order book opposes signal")
    if stats["spread"]>MAX_SPREAD:reasons.append("spread too wide")
    if stats["depth"]<MIN_DEPTH:reasons.append("book too thin")
    if not MIN_ASK<=stats["ask"]<=MAX_ASK:reasons.append("contract price outside entry band")
    if learn["pause"]:reasons.append("learning safety pause after weak recent results")
    pnl=c.execute("SELECT COALESCE(SUM(pnl),0) n FROM calls WHERE status='RESOLVED' AND substr(resolved_at,1,10)=?",(now().date().isoformat(),)).fetchone()["n"]
    if pnl<=-DAILY_LOSS_LIMIT:reasons.append("daily paper loss limit reached")
    open_n=c.execute("SELECT COUNT(*) n FROM calls WHERE status='OPEN'").fetchone()["n"]
    if open_n>=MAX_OPEN:reasons.append("maximum open paper calls reached")
    if reasons:return "WAIT · "+"; ".join(reasons)+" · "+pm_quote+" · "+snipe+" · COURT: "+brain["debate"]
    qty=STAKE/stats["ask"]
    try:
        c.execute("INSERT INTO calls(market_id,slug,asset,timeframe,side,token_id,entry,stake,qty,remaining_qty,score,features,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
          (str(m.get("id")),str(m.get("slug","")),asset,tf,side,tid,stats["ask"],STAKE,qty,qty,tech["score"],json.dumps(tech),iso()))
        c.commit()
    except sqlite3.IntegrityError:return "Already paper-tracked this market/side"
    return f"PAPER ENTRY {side} @ {stats['ask']:.3f} · ${STAKE:.2f} stake · book spread {stats['spread']:.3f} · {pm_quote} · {snipe} · COURT: {brain['debate']}"


def resolve_calls(c):
    resolve_forecasts(c)
    rows=c.execute("SELECT * FROM calls WHERE status='OPEN'").fetchall()
    for r in rows:
        try:
            data=get(f"{GAMMA}/markets/{r['market_id']}")
            if not data.get("closed"):continue
            outs=jlist(data.get("outcomes")); prices=jlist(data.get("outcomePrices"))
            mapping={str(o).lower():float(p) for o,p in zip(outs,prices)}
            winning=None
            if mapping.get("up",0)>=0.99 or mapping.get("yes",0)>=0.99:winning="UP"
            elif mapping.get("down",0)>=0.99 or mapping.get("no",0)>=0.99:winning="DOWN"
            if winning is None:continue
            won=(winning==r["side"]); rem=float(r["remaining_qty"] or 0.0)
            payout=rem if won else 0.0
            pnl=float(r["realized_pnl"] or 0.0)+payout-(rem*float(r["entry"]))
            c.execute("UPDATE calls SET status='RESOLVED',result=?,pnl=?,remaining_qty=0,resolved_at=? WHERE id=?",("WIN" if pnl>0 else "LOSS",pnl,iso(),r["id"]))
            c.commit()
            # Result is reflected in the single dashboard; no separate Discord alert.
        except Exception as e:print(f"Resolve check {r['market_id']}: {type(e).__name__}: {e}")



def manage_open_calls(c, asset, tech):
    rows=c.execute("SELECT * FROM calls WHERE status='OPEN' AND asset=?",(asset,)).fetchall()
    for r in rows:
        try:
            stats=book_stats(r["token_id"])
            if not stats: continue
            entry=float(r["entry"]); bid=float(stats["bid"])
            if entry<=0: continue
            roi=bid/entry-1.0; rem=float(r["remaining_qty"] or 0.0)
            realized=float(r["realized_pnl"] or 0.0); partial=int(r["partial_taken"] or 0)
            aligned=int(tech["score"])*(1 if r["side"]=="UP" else -1)
            exit_qty=0.0; action="HOLD"
            if roi<=-0.08: action="PROTECTIVE EXIT"; exit_qty=rem
            elif roi>=0.10: action="TARGET EXIT"; exit_qty=rem
            elif aligned<=-3 and stats["imbalance"]*(1 if r["side"]=="UP" else -1)<-0.10:
                action="FLIP CONFIRMATION EXIT"; exit_qty=rem
            elif roi>=0.05 and not partial and rem>0:
                action="PARTIAL PROFIT · SELL 50%"; exit_qty=rem/2
            if exit_qty>0:
                realized+=(bid-entry)*exit_qty; rem-=exit_qty
                closed=rem<=0.000001
                c.execute("UPDATE calls SET remaining_qty=?,realized_pnl=?,partial_taken=?,last_action=?,status=?,pnl=?,result=?,resolved_at=? WHERE id=?",
                  (max(0,rem),realized,1 if "PARTIAL" in action else partial,action,
                   "RESOLVED" if closed else "OPEN",realized if closed else None,
                   "EXIT" if closed else None,iso() if closed else None,r["id"]))
                c.commit()
            if action!=r["last_action"]:
                # Position changes appear on the dashboard, not as separate alerts.
                c.execute("UPDATE calls SET last_action=? WHERE id=?",(action,r["id"])); c.commit()
        except Exception as e: print(f"Position manager {r['id']}: {type(e).__name__}: {e}")


def flip_risk(tech, side, imbalance=0.0):
    """Heuristic flip-risk score; higher means more reversal warning, not probability."""
    score=int(tech.get('score',0)); m1=float(tech.get('move1',0)); m3=float(tech.get('move3',0)); rsi=float(tech.get('rsi',50))
    risk=48-abs(score)*6
    against=(side=='UP' and (m1<0 or m3<0)) or (side=='DOWN' and (m1>0 or m3>0))
    strongly_against=(side=='UP' and m1<0 and m3<0) or (side=='DOWN' and m1>0 and m3>0)
    if against: risk+=14
    if strongly_against: risk+=12
    if (side=='UP' and rsi>=72) or (side=='DOWN' and rsi<=28): risk+=12
    opposing=imbalance*(1 if side=='DOWN' else -1)
    if opposing>0.10: risk+=min(18,int((opposing-0.10)*60))
    range_pos=tech.get("range_position")
    if range_pos is not None and ((side=="UP" and float(range_pos)>=90) or (side=="DOWN" and float(range_pos)<=10)): risk+=8
    return max(5,min(95,int(round(risk))))

def pct_bar(value, width=12):
    value=max(0,min(100,int(value))); filled=round(value*width/100)
    return '█'*filled+'░'*(width-filled)

def color_pct_bar(value, fill='🟩', width=10):
    value=max(0,min(100,int(value)))
    filled=round(value*width/100)
    return fill*filled+'⬜'*(width-filled)

def score_bar(score, width=10):
    filled=round(abs(int(score))*width/5)
    return '▰'*filled+'▱'*(width-filled)

def line_asset(raw):
    import re
    m=re.search(r"\*\*([A-Z0-9]{2,12}) (?:5|15)m\*\*",raw)
    return m.group(1) if m else None

def select_top_assets(lines):
    """Show the same five major crypto assets consistently, not whichever score is highest."""
    return set(TOP_ASSETS)


def write_site_data(c, lines):
    import re
    markets=[]
    for raw in lines:
        clean=raw.replace('**','').replace('`','')
        match=re.search(r'([A-Z0-9]{2,12})\s+(5|15)m\s*[·|]',clean)
        if not match: continue
        asset,tf=match.group(1),int(match.group(2))
        sm=re.search(r'·\s*(UP|DOWN|WAIT)\s*\(([+-]?\d+)/5\)',clean)
        score=int(sm.group(2)) if sm else 0; side=sm.group(1) if sm else '—'
        rsi_m=re.search(r'RSI\s+(\d+)',clean); rsi=float(rsi_m.group(1)) if rsi_m else 50
        m1=re.search(r'1m\s+([+-]?\d+\.\d+)%',clean); move1=float(m1.group(1)) if m1 else 0
        m3=re.search(r'3m\s+([+-]?\d+\.\d+)%',clean); move3=float(m3.group(1)) if m3 else 0
        m10=re.search(r'10m\s+([+-]?\d+\.\d+)%',clean); move10=float(m10.group(1)) if m10 else 0
        rangem=re.search(r'RANGE 20m\s+(\d+)%',clean); range_position=float(rangem.group(1)) if rangem else None
        risk=flip_risk({'score':score,'move1':move1,'move3':move3,'rsi':rsi,'range_position':range_position},side)
        scalp_m=re.search(r'⚡ SCALP:\s*(.+)',clean)
        scalp=scalp_m.group(1).strip() if scalp_m else 'No scalp call yet'
        status='MARKET NOT FOUND' if 'MARKET NOT FOUND' in clean else 'SCANNED'
        action='NO TRADE' if status!='SCANNED' else ('PAPER ENTRY' if 'PAPER ENTRY' in clean else ('WAIT' if 'WAIT' in clean else 'CHECK SIGNAL'))
        if status!='SCANNED': risk=None
        markets.append({'asset':asset,'timeframe':tf,'side':side,'score':score,'score_bar':score_bar(score),'rsi':rsi,'range_position':range_position,'move1_pct':move1,'move3_pct':move3,'move10_pct':move10,'risk':risk,'risk_bar':pct_bar(risk) if risk is not None else '', 'risk_label':'HIGH' if risk is not None and risk>=70 else ('MODERATE' if risk is not None and risk>=45 else 'LOW'), 'status':status,'action':action,'scalp':scalp,'details':clean[:700]})
    rr=c.execute("SELECT pnl FROM calls WHERE status='RESOLVED' ORDER BY id DESC LIMIT 50").fetchall()
    wins=sum(float(x['pnl'] or 0)>0 for x in rr); n=len(rr); pnl=sum(float(x['pnl'] or 0) for x in rr)
    fr=c.execute("SELECT outcome FROM forecast_journal WHERE status='RESOLVED' ORDER BY resolved_at DESC LIMIT 100").fetchall()
    forecast_wins=sum(x["outcome"]=="WIN" for x in fr); forecast_flips=sum(x["outcome"]=="FLIP" for x in fr); forecast_losses=sum(x["outcome"]=="LOSS" for x in fr)
    positions=[]
    for r in c.execute("SELECT * FROM calls WHERE status='OPEN' ORDER BY id DESC").fetchall():
        try:
            b=book_stats(r['token_id']); bid=float(b['bid']) if b else None; entry=float(r['entry'])
            roi=(bid/entry-1)*100 if bid is not None and entry else None
            feat=json.loads(r['features'] or '{}'); aligned=int(feat.get('score',0))*(1 if r['side']=='UP' else -1)
            risk=flip_risk(feat,r['side'],float(b.get('imbalance',0)) if b else 0)
            if roi is not None and roi<=-8: action='PROTECT / EXIT'; explanation='Down 8% or more from entry: review the protective exit.'
            elif roi is not None and roi>=10: action='TAKE PROFIT'; explanation='The +10% paper target has been reached.'
            elif roi is not None and roi>=5 and not r['partial_taken']: action='SELL 50%'; explanation='The +5% partial-profit level has been reached.'
            elif risk>=70: action='EXIT RISK'; explanation='Flip-risk estimate is elevated; review the position.'
            else: action='HOLD'; explanation='No paper exit threshold is currently triggered.'
            positions.append({'asset':r['asset'],'timeframe':r['timeframe'],'side':r['side'],'entry':entry,'bid':bid,'roi_pct':round(roi,2) if roi is not None else None,'risk':risk,'action':action,'explanation':explanation,'paper_only':True})
        except Exception as e: positions.append({'asset':r['asset'],'timeframe':r['timeframe'],'side':r['side'],'action':'CHECK DATA','explanation':str(e)[:120],'paper_only':True})
    data={'updated_at':iso(),'paper_only':True,'summary':{'resolved_sample':n,'wins':wins,'losses':n-wins,'win_rate':round(100*wins/n,1) if n else None,'loss_rate':round(100*(n-wins)/n,1) if n else None,'realized_pnl':round(pnl,2),'open_positions':len(positions),'forecast_score':{'resolved_sample':len(fr),'wins':forecast_wins,'flips':forecast_flips,'losses':forecast_losses,'flip_is_tie':True}},'markets':markets,'positions':positions,'disclaimer':'Signal and flip-risk scores are heuristic, not probabilities or guarantees. Forecast WIN/FLIP/LOSS uses resolved paper forecasts; FLIP is neutral. Paper trading only.'}
    Path('docs').mkdir(exist_ok=True)
    Path('docs/data.json').write_text(json.dumps(data,indent=2),encoding='utf-8')
def market_embeds(lines):
    import re
    result=[]
    pattern=re.compile(r'\*\*([A-Z0-9]{2,12}) (5|15)m\*\* · \*\*(UP|DOWN|WAIT)\*\* \(([+-]?\d+)/5\) · RSI (\d+) · 1m ([+-]?\d+\.\d+)% · 3m ([+-]?\d+\.\d+)% · RANGE 20m (\d+)%')
    missing=re.compile(r'\*\*([A-Z0-9]{2,12}) (5|15)m\*\* · MARKET NOT FOUND')
    for raw in lines:
        first=raw.split('\n',1)[0]
        no_market=missing.search(first)
        if no_market:
            asset,tf=no_market.groups()
            result.append({'title':f'{asset} · {tf} MIN · NO VERIFIED MARKET','description':'**NO TRADE**\nThe scanner could not verify an active market for this timeframe. It will check again next scan.','color':0x8794a8,'fields':[{'name':'STATUS','value':'Market unavailable','inline':True},{'name':'FLIP RISK','value':'— · no market data','inline':True}],'footer':{'text':'No signal issued · paper only'}})
            continue
        match=pattern.search(first)
        if not match: continue
        asset,tf,side,score,rsi,m1,m3,range_position=match.groups()
        tech={'score':int(score),'move1':float(m1),'move3':float(m3),'rsi':float(rsi),'range_position':float(range_position)}
        risk=flip_risk(tech,side)
        # A directional hold-strength heuristic, not a calibrated probability.
        hold=max(5,min(95,50+abs(int(score))*8+(6 if float(m1)*int(score)>0 else -8)))
        if risk>=70: hold=max(5,hold-10)
        body=raw.split('\n',1)[1] if '\n' in raw else 'No action detail'
        # Explain the call in everyday language, using the same measured inputs.
        momentum_words = "short moves are going up" if float(m1)>0 and float(m3)>0 else "short moves are going down" if float(m1)<0 and float(m3)<0 else "short moves disagree"
        if side == "UP":
            plain_reason = f"{abs(int(score))} of 5 checks lean UP. {momentum_words}. RSI is {rsi}/100."
        elif side == "DOWN":
            plain_reason = f"{abs(int(score))} of 5 checks lean DOWN. {momentum_words}. RSI is {rsi}/100."
        else:
            plain_reason = f"Checks are mixed ({int(score):+d}/5). {momentum_words}. WAIT is safer than guessing."
        plain_reason += f" Price is {float(range_position):.0f}% from the recent low toward the recent high."
        if float(range_position)>=85:
            plain_reason += " Near the recent high: an UP entry may be late."
        elif float(range_position)<=15:
            plain_reason += " Near the recent low: a DOWN entry may be late."
        plain_reason += " The second brain and market checks can still block the entry."
        plain_reason += f" Flip-risk meter: {risk} out of 100. If the short moves turn against this direction, or the second brain changes its view, the bot may switch to WAIT or the other side."
        flip_rate_match=re.search(r'\bflips\s+(\d+)%',body)
        flip_sample_match=re.search(r'(?:recent \d+%/|learning sample )(\d+)',body)
        flip_rate=int(flip_rate_match.group(1)) if flip_rate_match else None
        flip_sample=int(flip_sample_match.group(1)) if flip_sample_match else 0
        pm_match=re.search(r'(POLYMARKET (?:UP|DOWN) · bid [^·]+ · buy-side implied [^·]+)',body)
        if pm_match:
            pm_value=pm_match.group(1)
            odds_match=re.search(r'buy-side implied ([0-9.]+)%',pm_value)
            if odds_match:
                odds=max(0,min(100,round(float(odds_match.group(1)))))
                pm_value=f'{pm_value} {color_pct_bar(odds,'🟩')} **{odds}%**'
        else:
            pm_value='Live Polymarket bid/ask unavailable this scan'
        snipe_line=next((line.replace('🎯 ','').strip() for line in body.splitlines() if 'SNIPE ' in line),'SNIPE WATCH · no confirmed contract move yet')
        scalp_line=next((line.replace('⚡ SCALP:','').strip() for line in raw.splitlines() if '⚡ SCALP:' in line),'SCALP: no scalp details')
        scalp_active='SCALP CALL' in scalp_line
        # Scalp score is an indicator-strength score, not a chance of winning.
        scalp_score=max(5,min(95,50+abs(int(score))*8-risk//4)) if scalp_active else 0
        if risk>=70: level='HIGH · reversal warning'
        elif risk>=45: level='MODERATE · stay alert'
        else: level='LOWER · signal currently aligned'
        if risk>=70: color=0xF59E0B
        elif side=='UP': color=0x19C37D
        elif side=='DOWN': color=0xEF4444
        else: color=0x8794A8
        scalp_value=(f'**{scalp_line}**\n\nSCALP STRENGTH  {pct_bar(scalp_score)} **{scalp_score}/100**\nIndicator score · not win odds' if scalp_active else f'**{scalp_line}**\nNo scalp entry approved on this scan.')
        result.append({'title':f'{asset} · {tf} MIN · {side}','description':f'**BOT CALL**\n{body[:850]}','color':color,'fields':[{'name':'🧠 WHY THIS CALL? · SIMPLE WORDS','value':plain_reason[:1024],'inline':False},{'name':'SCALP SETUP · ENTRY / TARGET / STOP','value':scalp_value[:1024],'inline':False},{'name':'SIGNAL STRENGTH','value':f'{score_bar(int(score))}\n**{int(score):+d}/5 votes**','inline':False},{'name':'🩵 HOLD STRENGTH','value':f'{color_pct_bar(hold,'🩵')} **{hold}%**\nHold outlook · heuristic, not odds','inline':False},{'name':'🟠 FLIP RISK','value':f'{color_pct_bar(risk,'🟧')} **{risk}%**\n{level}','inline':False},{'name':'FLIP RATE','value':f'**{flip_rate}%** · {flip_sample} resolved' if flip_rate is not None else 'No resolved flip history yet','inline':True},{'name':'POLYMARKET LIVE ODDS','value':pm_value[:220],'inline':False},{'name':'🎯 SNIPE · LIVE CONTRACT MOVE','value':snipe_line[:900],'inline':False},{'name':'MOMENTUM','value':f'1m **{float(m1):+.3f}%**\n3m **{float(m3):+.3f}%**\n20m range **{float(range_position):.0f}% from low**','inline':True},{'name':'RSI','value':f'**{rsi}** / 100','inline':True}],'footer':{'text':'Paper only · Polymarket bid/ask is live contract pricing; hold/flip scores are heuristics, not win probabilities'}})
    return result

def position_embeds(c):
    """Return concise position cards with live return, hold and exit meters."""
    result=[]
    rows=c.execute("SELECT * FROM calls WHERE status='OPEN' ORDER BY id DESC").fetchall()
    for r in rows:
        try:
            b=book_stats(r['token_id']); bid=float(b['bid']) if b else None; entry=float(r['entry'])
            roi=(bid/entry-1)*100 if bid is not None and entry>0 else None
            feat=json.loads(r['features'] or '{}')
            aligned=int(feat.get('score',0))*(1 if r['side']=='UP' else -1)
            risk=flip_risk(feat,r['side'],float(b.get('imbalance',0)) if b else 0)
            hold=max(0,min(100,100-risk))
            if roi is not None and roi<=-8: action='EXIT / PROTECT'; reason='Protection threshold reached'
            elif roi is not None and roi>=10: action='TAKE PROFIT'; reason='Profit target reached'
            elif roi is not None and roi>=5 and not r['partial_taken']: action='SELL 50%'; reason='Partial profit level reached'
            elif risk>=70 or aligned<=-3: action='EXIT RISK'; reason='Signal turning against position'
            else: action='HOLD'; reason='No exit trigger detected'
            if 'EXIT' in action: color,emoji=0xEF4444,'🔴'
            elif 'SELL' in action or 'PROFIT' in action: color,emoji=0xF59E0B,'🟠'
            elif action=='HOLD': color,emoji=0x14B8A6,'🩵'
            else: color,emoji=0x8794A8,'⚪'
            roi_text='—' if roi is None else f'{roi:+.2f}%'
            roi_meter=max(0,min(100,50+(roi or 0)*5))
            result.append({
                'title':f'{emoji} {action} · {r["asset"]} {r["timeframe"]}M {r["side"]}',
                'description':f'**{reason}** · PAPER',
                'color':color,
                'fields':[
                    {'name':'RETURN','value':f'**{roi_text}**\n{color_pct_bar(roi_meter,'🟦')}','inline':True},
                    {'name':'🩵 HOLD','value':f'**{hold}%**\n{color_pct_bar(hold,'🩵')}','inline':True},
                    {'name':'FLIP / EXIT RISK','value':f'**{risk}%**\n{pct_bar(risk)}','inline':False},
                    {'name':'ENTRY → BID','value':f'{entry:.3f} → {bid:.3f}' if bid is not None else f'{entry:.3f} → —','inline':True}
                ],
                'footer':{'text':'Paper position · meters are live indicators, not guarantees'}
            })
        except Exception as e: print(f'Position embed error {r["id"]}: {type(e).__name__}: {e}')
    return result

def publish_board(c, embeds):
    """Post once, then edit the same multi-bot Discord message to prevent alert flooding."""
    webhook=os.getenv("DISCORD_WEBHOOK_URL","").strip()
    if not webhook.startswith(("https://discord.com/api/webhooks/","https://discordapp.com/api/webhooks/")):
        print("Multi-bot Discord webhook missing or invalid; board not sent.")
        return
    row=c.execute("SELECT value FROM bot_state WHERE key='multi_dashboard_message_id'").fetchone()
    message_id=str(row["value"]) if row else ""
    payload={"embeds":embeds,"allowed_mentions":{"parse":[]}}
    try:
        if message_id:
            response=S.patch(webhook.rstrip("/")+"/messages/"+message_id,json=payload,timeout=12)
            if response.status_code!=404:
                if not response.ok:
                    print(f"Discord board PATCH rejected: HTTP {response.status_code}: {response.text[:1200]}",flush=True)
                    response.raise_for_status()
                print("Discord multi-bot board updated successfully.",flush=True)
                return
        response=S.post(webhook+"?wait=true",json=payload,timeout=12)
        if not response.ok:
            print(f"Discord board POST rejected: HTTP {response.status_code}: {response.text[:1200]}",flush=True)
            response.raise_for_status()
        new_id=str(response.json().get("id",""))
        print("Discord multi-bot board posted successfully.",flush=True)
        if new_id:
            c.execute("INSERT INTO bot_state(key,value) VALUES('multi_dashboard_message_id',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(new_id,))
            c.commit()
    except Exception as e:
        print(f"Multi-bot Discord board update failed: {type(e).__name__}: {e}")

def dashboard(c, lines):
    update_shared_learning(c)
    rows=c.execute('SELECT score FROM observations ORDER BY id DESC LIMIT 24').fetchall()
    wins_rows=c.execute('SELECT pnl FROM calls WHERE status=\'RESOLVED\' ORDER BY id DESC LIMIT 50').fetchall()
    wins=sum(float(x['pnl'])>0 for x in wins_rows); n=len(wins_rows); pnl=sum(float(x['pnl']) for x in wins_rows)
    opens=c.execute('SELECT COUNT(*) n FROM calls WHERE status=\'OPEN\'').fetchone()['n']
    top_assets=select_top_assets(lines)
    display_lines=[raw for raw in lines if line_asset(raw) in top_assets]
    desc='\n'.join(display_lines)[:2500] or 'No matching active markets found.'
    scores=[max(-5,min(5,int(r['score']))) for r in reversed(rows)]
    bars='▁▂▃▄▅▆▇█'; spark=''.join(bars[round((s+5)/10*7)] for s in scores) or 'No signal history yet'
    write_site_data(c,display_lines)
    print('\n=== PAPER SCANNER SIGNALS ===',flush=True); print(desc,flush=True); print(f'Signal trend (-5 to +5): {spark}',flush=True); print('=== END PAPER SCANNER SIGNALS ===\n',flush=True)
    frows=c.execute("SELECT outcome FROM forecast_journal WHERE status='RESOLVED' ORDER BY resolved_at ASC").fetchall()
    fw=sum(x["outcome"]=="WIN" for x in frows)
    ff=sum(x["outcome"]=="FLIP" for x in frows)
    fl=sum(x["outcome"]=="LOSS" for x in frows)
    current_streak=0; best_streak=0
    for item in frows:
        if item["outcome"] in ("WIN","FLIP"):
            current_streak+=1; best_streak=max(best_streak,current_streak)
        else:
            current_streak=0
    # Persist the all-time record so it survives restarts and remains visible.
    saved=c.execute("SELECT value FROM bot_state WHERE key='forecast_win_streak_record'").fetchone()
    best_streak=max(best_streak,int(saved["value"]) if saved else 0)
    c.execute("INSERT INTO bot_state(key,value) VALUES('forecast_win_streak_record',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",(str(best_streak),))
    c.commit()
    summary={'title':'SIGNALFORGE · PAPER TRADING DESK','description':'**MARKET SCAN COMPLETE**\nMulti-crypto · courtroom review · 5m / 15m\nNo real orders are placed.','color':0x4bd9ff,'fields':[{'name':'SIGNAL TREND','value':f'`-5` {spark} `+5`','inline':False},{'name':'PAPER WIN / LOSS','value':f'**{wins} WINS · {n-wins} LOSSES**\nWin rate {100*wins/n:.1f}% · Loss rate {100*(n-wins)/n:.1f}%' if n else 'Collecting resolved calls','inline':True},{'name':'FORECAST WINS','value':f'**{fw}**','inline':True},{'name':'FORECAST FLIPS','value':f'**{ff}** · correct after changing direction','inline':True},{'name':'FORECAST LOSSES','value':f'**{fl}**','inline':True},{'name':'WIN STREAK','value':f'🔥 **{current_streak}** current\n🏆 **{best_streak}** all-time record','inline':False},{'name':'RESOLVED FORECASTS','value':str(len(frows)),'inline':True},{'name':'RESOLVED PAPER CALLS','value':str(n),'inline':True},{'name':'OPEN POSITIONS','value':str(opens),'inline':True}],'footer':{'text':'UP/DOWN forecast scored at market settlement · FLIP = correct after direction changed · PAPER ONLY'},'timestamp':iso()}
    cards=market_embeds(display_lines); positions=position_embeds(c)
    # One editable message per multi-bot channel: summary, organized crypto radar, and positions.
    # Five concise coin cards, refreshed with the latest scan values.
    import re
    by_asset={}
    for card in cards:
        title=card.get('title','')
        mt=re.match(r'([A-Z0-9]{2,12}) · (5|15) MIN · (.+)$',title)
        if not mt: continue
        asset,tf,side_text=mt.groups()
        side=side_text if side_text in ('UP','DOWN','WAIT') else 'WAIT'
        missing='NO VERIFIED MARKET' in side_text
        fields=card.get('fields',[])
        get_field=lambda label: next((x.get('value','') for x in fields if label in x.get('name','')),'')
        odds_match=re.search(r'buy-side implied ([0-9.]+)%',get_field('POLYMARKET LIVE ODDS'))
        odds=int(round(float(odds_match.group(1)))) if odds_match else None
        hm=re.search(r'\*\*(\d+)%\*\*',get_field('HOLD STRENGTH'))
        fm=re.search(r'\*\*(\d+)%\*\*',get_field('FLIP RISK'))
        frm=re.search(r'\*\*(\d+)%\*\*',get_field('FLIP RATE'))
        sm=re.search(r'([+-]?\d+)/5',get_field('SIGNAL STRENGTH'))
        by_asset.setdefault(asset,{})[tf]={
            'side':side,'missing':missing,'odds':odds,
            'hold':int(hm.group(1)) if hm else None,
            'flip':int(fm.group(1)) if fm else None,
            'flip_rate':int(frm.group(1)) if frm else None,
            'score':int(sm.group(1)) if sm else 0
        }
    asset_embeds=[]
    for asset in TOP_ASSETS:
        windows=by_asset.get(asset,{})
        if not windows: continue
        primary=windows.get('5',windows.get('15',{}))
        side=primary.get('side','WAIT'); risk=primary.get('flip') or 0
        if primary.get('missing'): color,emoji=0x8794a8,'⚪'
        elif risk>=70: color,emoji=0xF59E0B,'🟠'
        elif side=='UP': color,emoji=0x19C37D,'🟢'
        elif side=='DOWN': color,emoji=0xEF4444,'🔴'
        else: color,emoji=0x8794A8,'⚪'
        desc=[]; card_fields=[]
        for tf in ('5','15'):
            item=windows.get(tf)
            if not item:
                desc.append(f'{tf}M ⏳'); continue
            sig='🟠' if (item.get('flip') or 0)>=70 else '🟢' if item['side']=='UP' else '🔴' if item['side']=='DOWN' else '⚪'
            desc.append(f'{tf}M {sig} **{item["side"]}**')
            if item['missing']:
                value='⚪ NO VERIFIED MARKET'
            else:
                odds=item['odds']; h=item['hold']; f=item['flip']
                oddsline=f'**{odds}%** {pct_bar(odds)}' if odds is not None else '—'
                holdline=f'🩵 HOLD **{h}%** {color_pct_bar(h,'🩵')}' if h is not None else '🩵 HOLD —'
                flipline=f'🟠 FLIP RISK **{f}%** {color_pct_bar(f,'🟧')}' if f is not None else '🟠 FLIP —'
                fr=item.get('flip_rate')
                rateline=f'↩️ FLIP RATE **{fr}%** · historical' if fr is not None else '↩️ FLIP RATE —'
                sigp=max(0,min(100,50+item['score']*10))
                value=f'{sig} **{item["side"]}**\nMARKET {oddsline}\n{holdline}\n{flipline}\n{rateline}\nSIGNAL **{sigp}%** {color_pct_bar(sigp,'🟦')}'
            card_fields.append({'name':f'⏱ {tf}M','value':value[:1024],'inline':False})
        asset_embeds.append({
            'title':f'{emoji} {asset} · LIVE RADAR',
            'description':'  |  '.join(desc)+'\n🔄 Refreshing from latest scan',
            'color':color,'fields':card_fields,
            'footer':{'text':'Market % = contract price · hold/flip/signal = indicator meters'},
            'timestamp':iso()
        })
    if not asset_embeds:
        asset_embeds=[{'title':'⚪ CRYPTO RADAR · WAITING','description':'No verified markets this scan.','color':0x8794a8}]
    publish_board(c,[summary]+asset_embeds[:5]+position_embeds(c)[:4])
def cycle(c):
    global _ASSET_CYCLE
    resolve_calls(c); lines=[]
    # Scan every supported top asset each cycle instead of rotating only two.
    # The service remains paper-only; API latency may make a cycle exceed 5 seconds.
    selected=list(TOP_ASSETS)
    _ASSET_CYCLE+=1
    for asset in selected:
        tfs=TIMEFRAMES[asset]
        try: tech=indicators(reference_candles(asset))
        except Exception as e:
            msg=f"**{asset}** · FEED ERROR · {type(e).__name__}: {str(e)[:100]}"; print(msg); lines.append(msg); continue
        manage_open_calls(c,asset,tech)
        for tf in tfs:
            try:
                m=discover(asset,tf)
                if not m:
                    lines.append(f"**{asset} {tf}m** · MARKET NOT FOUND · no call"); continue
                save_observation(c,asset,tf,m,tech)
                record_forecast(c,m,asset,tf,tech.get("side","WAIT"),tech)
                action=maybe_enter(c,asset,tf,m,tech)
                lr=learning_adjustment(c,asset,tf)
                wr=(f" · recent {lr['win_rate']:.0f}%/{lr['sample']} · flips {lr['flip_rate']*100:.0f}%"
                    if lr["win_rate"] is not None else
                    f" · learning sample {lr['sample']}/12 · flips {lr['flip_rate']*100:.0f}%")
                scalp=scalp_call(c,asset,tf,m,tech)
                side=tech["side"]
                # Individual per-market Discord alerts are intentionally suppressed.
                # The persistent, editable board below contains the latest call and avoids channel clutter.
                lines.append(f"**{asset} {tf}m** · **{tech['side']}** ({tech['score']:+d}/5) · RSI {tech['rsi']:.0f} · 1m {tech['move1']:+.3f}% · 3m {tech['move3']:+.3f}% · RANGE 20m {tech['range_position']:.0f}% · 10m {tech['move10']:+.3f}%\n{action}{wr}\n⚡ SCALP: {scalp}")
            except Exception as e:
                msg=f"**{asset} {tf}m** · SCAN ERROR · {type(e).__name__}: {str(e)[:100]}"; print(msg); lines.append(msg)
    # Keep the most recent scan for every supported coin so the site shows the full radar.
    import re
    for raw in lines:
        m=re.search(r'\*\*([A-Z0-9]{2,12}) (5|15)m\*\*',raw)
        if m: _LATEST_LINES[(m.group(1),int(m.group(2)))]=raw
    dashboard(c,list(_LATEST_LINES.values()) or lines)


def main():
    p=argparse.ArgumentParser(description="Polymarket multi-asset paper scanner")
    p.add_argument("--duration",type=int,default=0,help="0 runs continuously")
    args=p.parse_args(); c=db()
    print("SignalForge dashboard scanner online · top five assets · paper only",flush=True)
    start=time.time()
    try:
        while True:
            try:cycle(c)
            except Exception as e:
                print(f"Cycle error: {type(e).__name__}: {e}")
                print(f"Cycle error (dashboard will show next successful scan): {type(e).__name__}: {str(e)[:180]}",flush=True)
            if args.duration and time.time()-start>=args.duration:break
            time.sleep(max(5,SCAN_SECONDS))
    finally:c.close()
    return 0

if __name__=="__main__":sys.exit(main())
