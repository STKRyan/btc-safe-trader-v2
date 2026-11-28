"""Realtime public Polymarket outcome-token tape for the paper scanner."""
import json
import threading
import time
from collections import deque

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_LOCK = threading.RLock()
_STATE = {}
_QUEUE = set()
_SUBSCRIBED = set()
_STARTED = False
_CONNECTED = False


def _state(token):
    return _STATE.setdefault(str(token), {
        "trades": deque(maxlen=600), "bids": [], "asks": [],
        "bid": None, "ask": None, "imbalance": 0.0,
        "book_ts": 0.0, "quote_ts": 0.0
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
                price=float(event.get("price",0))
                size=float(event.get("size",0))
            except (TypeError,ValueError):
                continue
            side=str(event.get("side","")).upper()
            if token and price>0 and size>0 and side in ("BUY","SELL"):
                with _LOCK:
                    _state(token)["trades"].append((receipt,side,price,size))
        elif kind=="book":
            token=str(event.get("asset_id",""))
            try:
                bids=sorted([(float(x["price"]),float(x["size"])) for x in (event.get("bids") or [])],
                            key=lambda x:x[0],reverse=True)
                asks=sorted([(float(x["price"]),float(x["size"])) for x in (event.get("asks") or [])],
                            key=lambda x:x[0])
                if not token or not bids or not asks:
                    continue
                bd=sum(p*z for p,z in bids[:10])
                ad=sum(p*z for p,z in asks[:10])
                with _LOCK:
                    st=_state(token)
                    st.update({"bids":bids,"asks":asks,"bid":bids[0][0],"ask":asks[0][0],
                               "imbalance":(bd-ad)/max(1.0,bd+ad),
                               "book_ts":receipt,"quote_ts":receipt})
            except (TypeError,ValueError,KeyError):
                continue
        elif kind in ("price_change","best_bid_ask"):
            changes=event.get("price_changes") or [event]
            with _LOCK:
                for change in changes:
                    token=str(change.get("asset_id",event.get("asset_id","")))
                    if not token:
                        continue
                    st=_state(token)
                    try:
                        if change.get("best_bid") not in (None,""):
                            st["bid"]=float(change["best_bid"])
                        if change.get("best_ask") not in (None,""):
                            st["ask"]=float(change["best_ask"])
                        if st["bid"] is not None and st["ask"] is not None:
                            st["quote_ts"]=receipt
                    except (TypeError,ValueError):
                        continue


def flow_snapshot(token, seconds=10):
    now=time.time()
    empty={"trade_count":0,"buy_notional":0.0,"sell_notional":0.0,
           "net_flow_pct":0.0,"trade_move_pct":None,"last_trade_price":None,
           "trade_age":None,"bid":None,"ask":None,"imbalance":0.0,
           "book_age":None,"quote_age":None,"stream_connected":_CONNECTED}
    if not token:
        return empty
    with _LOCK:
        st=_STATE.get(str(token))
        if not st:
            return empty
        trades=[x for x in st["trades"] if now-x[0]<=seconds]
        bid=st["bid"]; ask=st["ask"]; imbalance=st["imbalance"]
        book_ts=st["book_ts"]; quote_ts=st["quote_ts"]
    buy=sum(p*z for _,side,p,z in trades if side=="BUY")
    sell=sum(p*z for _,side,p,z in trades if side=="SELL")
    total=buy+sell
    prices=[x[2] for x in trades]
    move=(prices[-1]/prices[0]-1)*100 if len(prices)>=2 and prices[0]>0 else None
    last=trades[-1][0] if trades else None
    return {"trade_count":len(trades),"buy_notional":buy,"sell_notional":sell,
            "net_flow_pct":100*(buy-sell)/total if total else 0.0,
            "trade_move_pct":move,"last_trade_price":prices[-1] if prices else None,
            "trade_age":now-last if last else None,"bid":bid,"ask":ask,
            "imbalance":imbalance,"book_age":now-book_ts if book_ts else None,
            "quote_age":now-quote_ts if quote_ts else None,
            "stream_connected":_CONNECTED}


def streamed_book_stats(token):
    flow=flow_snapshot(token,seconds=0)
    if flow.get("book_age") is None or flow["book_age"]>30:
        return None
    if flow.get("quote_age") is None or flow["quote_age"]>5:
        return None
    with _LOCK:
        st=_STATE.get(str(token),{})
        bids=list(st.get("bids",[])); asks=list(st.get("asks",[]))
    if not bids or not asks:
        return None
    bid=float(flow["bid"] if flow["bid"] is not None else bids[0][0])
    ask=float(flow["ask"] if flow["ask"] is not None else asks[0][0])
    bd=sum(p*z for p,z in bids[:10]); ad=sum(p*z for p,z in asks[:10])
    return {"bid":bid,"ask":ask,"spread":ask-bid,"depth":min(bd,ad),
            "imbalance":(bd-ad)/max(1.0,bd+ad),"source":"live websocket"}


def _loop():
    global _CONNECTED
    try:
        import websocket
    except Exception:
        print("Live Polymarket tape unavailable: websocket-client dependency missing",flush=True)
        return
    while True:
        ws=None
        try:
            ws=websocket.create_connection(WS_URL,timeout=5,enable_multithread=True)
            ws.settimeout(1)
            with _LOCK:
                initial=list(_QUEUE)
                _QUEUE.clear()
            if not initial:
                ws.close()
                time.sleep(1)
                continue
            ws.send(json.dumps({"assets_ids":initial,"type":"market","custom_feature_enabled":True}))
            with _LOCK:
                _SUBSCRIBED.update(initial)
            _CONNECTED=True
            print("Polymarket live trade tape connected; outcome tokens subscribed",flush=True)
            last_ping=time.time()
            while True:
                with _LOCK:
                    additions=list(_QUEUE)
                    _QUEUE.clear()
                if additions:
                    ws.send(json.dumps({"assets_ids":additions,"operation":"subscribe"}))
                    with _LOCK:
                        _SUBSCRIBED.update(additions)
                if time.time()-last_ping>=15:
                    ws.ping()
                    last_ping=time.time()
                try:
                    message=ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                if message:
                    _handle(message)
        except Exception as exc:
            _CONNECTED=False
            print(f"Polymarket live tape reconnect: {type(exc).__name__}: {str(exc)[:140]}",flush=True)
            with _LOCK:
                _QUEUE.update(_SUBSCRIBED)
                _SUBSCRIBED.clear()
            time.sleep(2)
        finally:
            _CONNECTED=False
            try:
                if ws:
                    ws.close()
            except Exception:
                pass


def live_flow_snapshot(token, seconds=10):
    return flow_snapshot(token,seconds)
