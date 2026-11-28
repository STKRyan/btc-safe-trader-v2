"""Realtime public Polymarket contract tape plus Binance spot trade/book/candle feeds."""
import json
import threading
import time
import urllib.request
import urllib.parse
from collections import deque

# Polymarket outcome-token stream
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_LOCK = threading.RLock()
_STATE = {}
_QUEUE = set()
_SUBSCRIBED = set()
_STARTED = False
_CONNECTED = False

# Binance public spot market stream; exchange data is reference evidence, not settlement.
_BX_LOCK = threading.RLock()
_BX_STATE = {}
_BX_SYMBOLS = set()
_BX_STARTED = False
_BX_CONNECTED = False
_BX_REST_MODE = False
_ASSET_SYMBOLS = {
    "BTC":"btcusdt", "ETH":"ethusdt", "SOL":"solusdt",
    "XRP":"xrpusdt", "DOGE":"dogeusdt", "BNB":"bnbusdt"
}


def _state(token):
    return _STATE.setdefault(str(token), {
        "trades": deque(maxlen=600), "bids": [], "asks": [],
        "bid": None, "ask": None, "imbalance": 0.0,
        "book_ts": 0.0, "quote_ts": 0.0, "book_history":deque(maxlen=1200)
    })


def register_live_tokens(token_ids):
    global _STARTED
    ids=[str(x) for x in token_ids if x]
    if not ids:
        return
    with _LOCK:
        for token in ids:
            _state(token)
            if token not in _SUBSCRIBED:
                _QUEUE.add(token)
        if not _STARTED:
            _STARTED=True
            threading.Thread(target=_loop,daemon=True,name="polymarket-live-tape").start()


def _handle(raw):
    try:
        payload=json.loads(raw)
    except Exception:
        return
    events=payload if isinstance(payload,list) else [payload]
    receipt=time.time()
    for event in events:
        if not isinstance(event,dict):
            continue
        kind=str(event.get("event_type","")).lower()
        if kind=="last_trade_price":
            token=str(event.get("asset_id",""))
            try:
                price=float(event.get("price",0)); size=float(event.get("size",0))
            except (TypeError,ValueError):
                continue
            side=str(event.get("side","")).upper()
            if token and price>0 and size>0 and side in ("BUY","SELL"):
                with _LOCK:
                    _state(token)["trades"].append((receipt,side,price,size))
        elif kind=="book":
            token=str(event.get("asset_id",""))
            try:
                bids=sorted([(float(x["price"]),float(x["size"])) for x in (event.get("bids") or [])],key=lambda x:x[0],reverse=True)
                asks=sorted([(float(x["price"]),float(x["size"])) for x in (event.get("asks") or [])],key=lambda x:x[0])
                if not token or not bids or not asks: continue
                bd=sum(p*z for p,z in bids[:10]); ad=sum(p*z for p,z in asks[:10])
                with _LOCK:
                    st=_state(token)
                    st.update({"bids":bids,"asks":asks,"bid":bids[0][0],"ask":asks[0][0],
                               "imbalance":(bd-ad)/max(1.0,bd+ad),"book_ts":receipt,"quote_ts":receipt})
            except (TypeError,ValueError,KeyError):
                continue
        elif kind in ("price_change","best_bid_ask"):
            changes=event.get("price_changes") or [event]
            with _LOCK:
                for change in changes:
                    token=str(change.get("asset_id",event.get("asset_id","")))
                    if not token: continue
                    st=_state(token)
                    try:
                        if change.get("best_bid") not in (None,""): st["bid"]=float(change["best_bid"])
                        if change.get("best_ask") not in (None,""): st["ask"]=float(change["best_ask"])
                        if st["bid"] is not None and st["ask"] is not None: st["quote_ts"]=receipt
                    except (TypeError,ValueError):
                        continue


def flow_snapshot(token, seconds=10):
    now=time.time()
    empty={"trade_count":0,"buy_notional":0.0,"sell_notional":0.0,"net_flow_pct":0.0,
           "trade_move_pct":None,"last_trade_price":None,"trade_age":None,"bid":None,
           "ask":None,"imbalance":0.0,"book_age":None,"quote_age":None,"stream_connected":_CONNECTED}
    if not token: return empty
    with _LOCK:
        st=_STATE.get(str(token))
        if not st: return empty
        trades=[x for x in st["trades"] if now-x[0]<=seconds]
        bid=st["bid"]; ask=st["ask"]; imbalance=st["imbalance"]; book_ts=st["book_ts"]; quote_ts=st["quote_ts"]
    buy=sum(p*z for _,side,p,z in trades if side=="BUY"); sell=sum(p*z for _,side,p,z in trades if side=="SELL")
    total=buy+sell; prices=[x[2] for x in trades]
    move=(prices[-1]/prices[0]-1)*100 if len(prices)>=2 and prices[0]>0 else None
    last=trades[-1][0] if trades else None
    return {"trade_count":len(trades),"buy_notional":buy,"sell_notional":sell,
            "net_flow_pct":100*(buy-sell)/total if total else 0.0,
            "trade_move_pct":move,"last_trade_price":prices[-1] if prices else None,
            "trade_age":now-last if last else None,"bid":bid,"ask":ask,"imbalance":imbalance,
            "book_age":now-book_ts if book_ts else None,"quote_age":now-quote_ts if quote_ts else None,
            "stream_connected":_CONNECTED}


def streamed_book_stats(token):
    flow=flow_snapshot(token,seconds=0)
    if flow.get("book_age") is None or flow["book_age"]>30: return None
    if flow.get("quote_age") is None or flow["quote_age"]>5: return None
    with _LOCK:
        st=_STATE.get(str(token),{}); bids=list(st.get("bids",[])); asks=list(st.get("asks",[]))
    if not bids or not asks: return None
    bid=float(flow["bid"] if flow["bid"] is not None else bids[0][0])
    ask=float(flow["ask"] if flow["ask"] is not None else asks[0][0])
    bd=sum(p*z for p,z in bids[:10]); ad=sum(p*z for p,z in asks[:10])
    return {"bid":bid,"ask":ask,"spread":ask-bid,"depth":min(bd,ad),
            "imbalance":(bd-ad)/max(1.0,bd+ad),"source":"live websocket"}


def _loop():
    global _CONNECTED
    try: import websocket
    except Exception:
        print("Live Polymarket tape unavailable: websocket-client dependency missing",flush=True); return
    while True:
        ws=None
        try:
            ws=websocket.create_connection(WS_URL,timeout=5,enable_multithread=True); ws.settimeout(1)
            with _LOCK: initial=list(_QUEUE); _QUEUE.clear()
            if not initial:
                ws.close(); time.sleep(1); continue
            ws.send(json.dumps({"assets_ids":initial,"type":"market","custom_feature_enabled":True}))
            with _LOCK: _SUBSCRIBED.update(initial)
            _CONNECTED=True
            print("Polymarket live trade tape connected; outcome tokens subscribed",flush=True)
            last_ping=time.time()
            while True:
                with _LOCK: additions=list(_QUEUE); _QUEUE.clear()
                if additions:
                    ws.send(json.dumps({"assets_ids":additions,"operation":"subscribe"}))
                    with _LOCK: _SUBSCRIBED.update(additions)
                if time.time()-last_ping>=15: ws.ping(); last_ping=time.time()
                try: message=ws.recv()
                except websocket.WebSocketTimeoutException: continue
                if message: _handle(message)
        except Exception as exc:
            _CONNECTED=False
            print(f"Polymarket live tape reconnect: {type(exc).__name__}: {str(exc)[:140]}",flush=True)
            with _LOCK: _QUEUE.update(_SUBSCRIBED); _SUBSCRIBED.clear()
            time.sleep(2)
        finally:
            _CONNECTED=False
            try:
                if ws: ws.close()
            except Exception: pass


def live_flow_snapshot(token, seconds=10):
    return flow_snapshot(token,seconds)


# Binance spot exchange data: aggressor trades, top-10 depth and live 1m candles.
def _bx_state(symbol):
    return _BX_STATE.setdefault(symbol,{"trades":deque(maxlen=3000),"bids":[],"asks":[],
        "bid":None,"ask":None,"book_ts":0.0,"quote_ts":0.0,
        "candles":deque(maxlen=240),"book_history":deque(maxlen=1200),"last_price":None,"last_price_ts":0.0,"last_agg_id":None})


def register_exchange_assets(assets):
    global _BX_STARTED
    symbols={_ASSET_SYMBOLS[a] for a in assets if a in _ASSET_SYMBOLS}
    if not symbols: return
    with _BX_LOCK:
        _BX_SYMBOLS.update(symbols)
        for symbol in symbols: _bx_state(symbol)
        if not _BX_STARTED:
            _BX_STARTED=True
            threading.Thread(target=_bx_loop,daemon=True,name="binance-exchange-tape").start()


def _bx_handle(raw):
    try: envelope=json.loads(raw)
    except Exception: return
    data=envelope.get("data",envelope)
    stream=str(envelope.get("stream","")).lower()
    if not isinstance(data,dict): return
    event=str(data.get("e","")).lower()
    symbol=str(data.get("s","")).lower()
    if not symbol and stream: symbol=stream.split("@",1)[0]
    if not symbol: return
    receipt=time.time()
    with _BX_LOCK:
        st=_bx_state(symbol)
        if event in ("aggtrade","trade"):
            try:
                price=float(data.get("p",0)); qty=float(data.get("q",0))
                # Binance m=true means buyer was maker, hence seller was aggressor.
                side="SELL" if bool(data.get("m",False)) else "BUY"
                if price>0 and qty>0:
                    st["trades"].append((receipt,side,price,qty)); st["last_price"]=price; st["last_price_ts"]=receipt
            except (TypeError,ValueError): pass
        elif event in ("depthupdate",""):
            bids=data.get("b") or data.get("bids") or []
            asks=data.get("a") or data.get("asks") or []
            try:
                parsed_b=sorted([(float(x[0]),float(x[1])) for x in bids if float(x[1])>0],key=lambda x:x[0],reverse=True)
                parsed_a=sorted([(float(x[0]),float(x[1])) for x in asks if float(x[1])>0],key=lambda x:x[0])
                if parsed_b and parsed_a:
                    bd=sum(p*q for p,q in parsed_b[:10]); ad=sum(p*q for p,q in parsed_a[:10])
                    imb=(bd-ad)/max(1.0,bd+ad); depth=min(bd,ad)
                    st.update({"bids":parsed_b[:10],"asks":parsed_a[:10],"bid":parsed_b[0][0],
                               "ask":parsed_a[0][0],"imbalance":imb,"book_ts":receipt,"quote_ts":receipt})
                    st["book_history"].append((receipt,imb,depth))
            except (TypeError,ValueError,IndexError): pass
        elif event=="kline":
            k=data.get("k",{})
            try:
                candle={"open_time":int(k.get("t",0)),"open":float(k.get("o",0)),
                        "high":float(k.get("h",0)),"low":float(k.get("l",0)),
                        "close":float(k.get("c",0)),"volume":float(k.get("v",0)),
                        "closed":bool(k.get("x",False)),"received_at":receipt}
                if candle["open"]>0 and candle["close"]>0:
                    rows=st["candles"]
                    if rows and rows[-1]["open_time"]==candle["open_time"]: rows[-1]=candle
                    else: rows.append(candle)
            except (TypeError,ValueError): pass


def _bx_rest_poll_symbol(symbol):
    """Poll Binance's market-data-only REST API when the public WebSocket is region-blocked."""
    base="https://data-api.binance.vision/api/v3/"
    def fetch(path, params):
        query=urllib.parse.urlencode(params)
        req=urllib.request.Request(base+path+"?"+query,headers={"User-Agent":"SignalForge-Paper-Scanner/1.0"})
        with urllib.request.urlopen(req,timeout=7) as response:
            return json.loads(response.read().decode("utf-8"))
    with _BX_LOCK:
        st=_bx_state(symbol); last_id=st.get("last_agg_id")
    trade_params={"symbol":symbol.upper(),"limit":100}
    if last_id is not None: trade_params["fromId"]=int(last_id)+1
    trades=fetch("aggTrades",trade_params)
    depth=fetch("depth",{"symbol":symbol.upper(),"limit":20})
    klines=fetch("klines",{"symbol":symbol.upper(),"interval":"1m","limit":8})
    receipt=time.time(); now_ms=int(receipt*1000)
    with _BX_LOCK:
        st=_bx_state(symbol)
        for row in trades:
            try:
                trade_id=int(row["a"])
                if st["last_agg_id"] is not None and trade_id<=st["last_agg_id"]: continue
                price=float(row["p"]); qty=float(row["q"])
                side="SELL" if bool(row["m"]) else "BUY"
                if price>0 and qty>0:
                    st["trades"].append((float(row.get("T",now_ms))/1000.0,side,price,qty))
                    st["last_price"]=price; st["last_price_ts"]=float(row.get("T",now_ms))/1000.0
                st["last_agg_id"]=trade_id
            except (TypeError,ValueError,KeyError): continue
        bids=sorted([(float(x[0]),float(x[1])) for x in depth.get("bids",[]) if float(x[1])>0],key=lambda x:x[0],reverse=True)
        asks=sorted([(float(x[0]),float(x[1])) for x in depth.get("asks",[]) if float(x[1])>0],key=lambda x:x[0])
        if bids and asks:
            bd=sum(p*q for p,q in bids[:10]); ad=sum(p*q for p,q in asks[:10])
        imb=(bd-ad)/max(1.0,bd+ad); depth=min(bd,ad)
        st.update({"bids":bids[:10],"asks":asks[:10],"bid":bids[0][0],"ask":asks[0][0],
                       "imbalance":imb,"book_ts":receipt,"quote_ts":receipt})
        st["book_history"].append((receipt,imb,depth))
        rows=st["candles"]
        for k in klines:
            try:
                candle={"open_time":int(k[0]),"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),
                        "close":float(k[4]),"volume":float(k[5]),"closed":int(k[6])<now_ms,
                        "received_at":receipt}
                if candle["open"]<=0 or candle["close"]<=0: continue
                if rows and rows[-1]["open_time"]==candle["open_time"]: rows[-1]=candle
                elif not rows or candle["open_time"]>rows[-1]["open_time"]: rows.append(candle)
            except (TypeError,ValueError,IndexError): continue
        st["last_price_ts"]=max(st.get("last_price_ts",0.0),receipt if not trades else st.get("last_price_ts",receipt))


def _bx_rest_poll_cycle():
    global _BX_CONNECTED
    with _BX_LOCK: symbols=sorted(_BX_SYMBOLS)
    successes=0
    for symbol in symbols:
        try:
            _bx_rest_poll_symbol(symbol); successes+=1
        except Exception as exc:
            if time.time()-getattr(_bx_rest_poll_cycle,"last_error_log",0)>30:
                print(f"Binance REST fallback temporarily unavailable ({symbol}): {type(exc).__name__}: {str(exc)[:120]}",flush=True)
                _bx_rest_poll_cycle.last_error_log=time.time()
    _BX_CONNECTED=successes>0
    if successes: 
        if not getattr(_bx_rest_poll_cycle,"announced",False):
            print(f"Binance REST market-data fallback active: {successes}/{len(symbols)} assets; trades + depth + 1m candles",flush=True)
            _bx_rest_poll_cycle.announced=True
    return successes


def _bx_loop():
    global _BX_CONNECTED, _BX_REST_MODE
    try: import websocket
    except Exception:
        print("Binance WebSocket dependency missing; using REST market-data fallback",flush=True)
        _BX_REST_MODE=True
    while True:
        if _BX_REST_MODE:
            _bx_rest_poll_cycle()
            time.sleep(2)
            continue
        ws=None
        try:
            with _BX_LOCK: symbols=sorted(_BX_SYMBOLS)
            streams=[]
            for symbol in symbols:
                streams.extend([f"{symbol}@aggTrade",f"{symbol}@depth10@100ms",f"{symbol}@kline_1m"])
            if not streams: time.sleep(2); continue
            url="wss://stream.binance.com:9443/stream?streams="+"/".join(streams)
            ws=websocket.create_connection(url,timeout=8,enable_multithread=True); ws.settimeout(1)
            _BX_CONNECTED=True
            print(f"Binance spot live feed connected: {', '.join(symbols)} trades + depth + 1m candles",flush=True)
            last_ping=time.time()
            while True:
                if time.time()-last_ping>=20: ws.ping(); last_ping=time.time()
                try: message=ws.recv()
                except websocket.WebSocketTimeoutException: continue
                if message: _bx_handle(message)
        except Exception as exc:
            _BX_CONNECTED=False
            status=getattr(exc,"status_code",None)
            if status==451 or "Handshake status 451" in str(exc):
                _BX_REST_MODE=True
                print("Binance WebSocket rejected with HTTP 451; switching to data-api.binance.vision REST polling instead of reconnecting endlessly.",flush=True)
            else:
                print(f"Binance exchange feed reconnect: {type(exc).__name__}: {str(exc)[:140]}",flush=True)
            time.sleep(2)
        finally:
            _BX_CONNECTED=False
            try:
                if ws: ws.close()
            except Exception: pass


def exchange_snapshot(asset, seconds=10):
    symbol=_ASSET_SYMBOLS.get(str(asset).upper())
    empty={"asset":str(asset).upper(),"symbol":symbol.upper() if symbol else None,
           "trade_count":0,"buy_notional":0.0,"sell_notional":0.0,"net_flow_pct":0.0,
           "trade_move_pct":None,"trade_age":None,"bid":None,"ask":None,
           "spread_pct":None,"imbalance":0.0,"depth_usd":0.0,"book_age":None,
           "quote_age":None,"last_price":None,"candle_1m_pct":None,"candle_5m_pct":None,
           "candle_bias":"UNKNOWN","candle_high":None,"candle_low":None,
           "stream_connected":_BX_CONNECTED}
    if not symbol: return empty
    now=time.time()
    with _BX_LOCK:
        st=_BX_STATE.get(symbol)
        if not st: return empty
        trades=[x for x in st["trades"] if now-x[0]<=seconds]
        bids=list(st["bids"]); asks=list(st["asks"]); bid=st["bid"]; ask=st["ask"]
        book_ts=st["book_ts"]; quote_ts=st["quote_ts"]; candles=list(st["candles"])
        last_price=st["last_price"]; last_price_ts=st["last_price_ts"]
    buy=sum(p*q for _,side,p,q in trades if side=="BUY")
    sell=sum(p*q for _,side,p,q in trades if side=="SELL")
    total=buy+sell; prices=[x[2] for x in trades]
    move=(prices[-1]/prices[0]-1)*100 if len(prices)>=2 and prices[0]>0 else None
    trade_age=now-trades[-1][0] if trades else None
    bd=sum(p*q for p,q in bids[:10]); ad=sum(p*q for p,q in asks[:10])
    imbalance=(bd-ad)/max(1.0,bd+ad) if bids and asks else 0.0
    depth=min(bd,ad) if bids and asks else 0.0
    spread_pct=((ask-bid)/((ask+bid)/2)*100) if bid and ask and ask>=bid else None
    closed=[x for x in candles if x["closed"]]
    c1=c5=None; bias="UNKNOWN"; hi=lo=None
    if closed:
        latest=closed[-1]; c1=(latest["close"]/latest["open"]-1)*100 if latest["open"] else None
        sample=closed[-5:]
        if len(sample)>=5 and sample[0]["open"]:
            c5=(sample[-1]["close"]/sample[0]["open"]-1)*100
            hi=max(x["high"] for x in sample); lo=min(x["low"] for x in sample)
            bias="UP" if c5>0.03 and c1 is not None and c1>0 else "DOWN" if c5< -0.03 and c1 is not None and c1<0 else "MIXED"
        elif c1 is not None: bias="UP" if c1>0 else "DOWN" if c1<0 else "MIXED"
    return {"asset":str(asset).upper(),"symbol":symbol.upper(),"trade_count":len(trades),
            "buy_notional":buy,"sell_notional":sell,
            "net_flow_pct":100*(buy-sell)/total if total else 0.0,
            "trade_move_pct":move,"trade_age":trade_age,"bid":bid,"ask":ask,
            "spread_pct":spread_pct,"imbalance":imbalance,"depth_usd":depth,
            "book_age":now-book_ts if book_ts else None,"quote_age":now-quote_ts if quote_ts else None,
            "last_price":last_price,"price_age":now-last_price_ts if last_price_ts else None,
            "candle_1m_pct":c1,"candle_5m_pct":c5,"candle_bias":bias,
            "candle_high":hi,"candle_low":lo,"stream_connected":_BX_CONNECTED}


def live_flow_snapshot(token, seconds=10):
    return flow_snapshot(token,seconds)
