#!/usr/bin/env python3
"""Standalone 15-second BTC price-snapshot chart. Read-only; never trades."""
from __future__ import annotations
import json, os, time, urllib.parse
from datetime import datetime, timezone
from pathlib import Path
import requests

TICKER = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
HTTP_TIMEOUT = 15
UPDATE_SECONDS = max(15, int(os.getenv("BTC_CHART_UPDATE_SECONDS", "15")))
MAX_POINTS = 240
STATE_PATH = Path(os.getenv("BTC_CHART_STATE_PATH", "data/btc_chart_state.json"))
WEBHOOK_ENV = "DISCORD_BTC_CHART_WEBHOOK_URL"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "BTC-15s-Chart-Agent/1.0"})

def utc_now():
    return datetime.now(timezone.utc)

def get_price():
    response = SESSION.get(TICKER, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    price = float(response.json()["price"])
    if price <= 0:
        raise ValueError("Coinbase returned an invalid BTC price")
    return price

def load_state():
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            return {"message_id": "", "samples": []}
        samples = state.get("samples", [])
        if not isinstance(samples, list):
            samples = []
        return {"message_id": str(state.get("message_id", "")),
                "samples": [s for s in samples if isinstance(s, list) and len(s) == 2][-MAX_POINTS:]}
    except (OSError, ValueError, TypeError):
        return {"message_id": "", "samples": []}

def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = STATE_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps(state), encoding="utf-8")
    temp_path.replace(STATE_PATH)

def make_embed(samples):
    prices = [round(float(s[1]), 2) for s in samples]
    times = [datetime.fromtimestamp(float(s[0]), timezone.utc).strftime("%H:%M:%S") for s in samples]
    latest, first = prices[-1], prices[0]
    change = ((latest / first) - 1.0) * 100.0 if first else 0.0
    short_change = ((latest / prices[-5]) - 1.0) * 100.0 if len(prices) >= 5 and prices[-5] else 0.0
    direction = "📈 RISING" if short_change > 0.015 else "📉 FALLING" if short_change < -0.015 else "➡️ SIDEWAYS"
    config = {
        "type": "line",
        "data": {"labels": times, "datasets": [{
            "label": "BTC/USD sampled price", "data": prices,
            "borderColor": "#2ecc71" if change >= 0 else "#e74c3c",
            "borderWidth": 3, "pointRadius": 0, "fill": False, "tension": 0.15
        }]},
        "options": {"plugins": {"legend": {"display": False}},
                    "scales": {"x": {"ticks": {"maxTicksLimit": 10}},
                              "y": {"title": {"display": True, "text": "BTC price (USD)"}}}}
    }
    chart_url = ("https://quickchart.io/chart?width=900&height=440&format=png"
                 "&backgroundColor=white&c=" + urllib.parse.quote(json.dumps(config), safe=""))
    return {
        "title": "⚡ BTC LIVE · 15-SECOND CHART",
        "description": (
            f"💰 BTC/USD: **USD {latest:,.2f}**\n"
            f"📊 Change in displayed window: **{change:+.2f}%**\n"
            f"⚡ Short trend: **{direction}**\n"
            f"🕒 Samples: {len(samples)} · refresh every {UPDATE_SECONDS}s\n\n"
            "Each point is a live last-trade price snapshot, sampled every 15 seconds. "
            "This is not exchange-generated 15-second OHLC candles."
        ),
        "color": 0x19C37D if change >= 0 else 0xEF4444,
        "image": {"url": chart_url},
        "footer": {"text": "Coinbase BTC-USD · read-only chart · paper trading only"},
        "timestamp": utc_now().isoformat()
    }

def update_discord(webhook, state, embed):
    message_id = state.get("message_id", "")
    payload = {"embeds": [embed], "allowed_mentions": {"parse": []}}
    if message_id:
        response = SESSION.patch(webhook.rstrip("/") + "/messages/" + message_id,
                                json=payload, timeout=HTTP_TIMEOUT)
        if response.status_code != 404:
            response.raise_for_status()
            print("BTC 15-second chart message updated.", flush=True)
            return
        print("Saved chart message was missing; recreating one.", flush=True)
    response = SESSION.post(webhook.rstrip("/") + "?wait=true",
                            json=payload, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    new_id = str(response.json().get("id", ""))
    if not new_id:
        raise RuntimeError("Discord accepted chart request but returned no message ID")
    state["message_id"] = new_id
    save_state(state)
    print("BTC 15-second chart message created; ID saved.", flush=True)

def main():
    print("BTC 15-second price-snapshot chart started; read-only.", flush=True)
    retry_delay, last_missing_notice = 5, 0.0
    state = load_state()
    while True:
        webhook = os.getenv(WEBHOOK_ENV, "").strip()
        if not webhook:
            if time.time() - last_missing_notice >= 300:
                print(f"{WEBHOOK_ENV} is not set on this chart service; waiting safely.", flush=True)
                last_missing_notice = time.time()
            time.sleep(15)
            continue
        if not (webhook.startswith("https://discord.com/api/webhooks/") or
                webhook.startswith("https://discordapp.com/api/webhooks/")):
            print("Chart webhook format is invalid; waiting safely.", flush=True)
            time.sleep(300)
            continue
        try:
            sampled_at = time.time()
            price = get_price()
            state["samples"].append([sampled_at, price])
            state["samples"] = state["samples"][-MAX_POINTS:]
            save_state(state)
            update_discord(webhook, state, make_embed(state["samples"]))
            retry_delay = 5
            time.sleep(UPDATE_SECONDS)
        except Exception as exc:
            print(f"BTC chart update error: {type(exc).__name__}: {exc}; retrying in {retry_delay}s.", flush=True)
            time.sleep(retry_delay)
            retry_delay = min(120, retry_delay * 2)

if __name__ == "__main__":
    main()
