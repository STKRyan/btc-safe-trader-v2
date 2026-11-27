#!/usr/bin/env python3
"""Standalone BTC 15-second sampled candlestick chart. Read-only; never trades."""
from __future__ import annotations
import json, os, time, urllib.parse
from datetime import datetime, timezone
from pathlib import Path
import requests

TICKER = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"
HTTP_TIMEOUT = 15
UPDATE_SECONDS = max(15, int(os.getenv("BTC_CHART_UPDATE_SECONDS", "15")))
SAMPLE_SECONDS = max(5, int(os.getenv("BTC_CHART_SAMPLE_SECONDS", "5")))
CANDLE_SECONDS = 15
MAX_SAMPLES = 720  # approximately one hour at 5-second sampling
MAX_CANDLES = 240  # approximately one hour of 15-second candles
STATE_PATH = Path(os.getenv("BTC_CHART_STATE_PATH", "data/btc_chart_state.json"))
WEBHOOK_ENV = "DISCORD_BTC_CHART_WEBHOOK_URL"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "BTC-15s-Candlestick-Chart-Agent/1.0"})

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
            return {"message_id": "", "samples": [], "last_chart_update": 0.0}
        samples = state.get("samples", [])
        if not isinstance(samples, list):
            samples = []
        valid_samples = []
        for sample in samples:
            if isinstance(sample, list) and len(sample) == 2:
                try:
                    valid_samples.append([float(sample[0]), float(sample[1])])
                except (TypeError, ValueError):
                    pass
        return {
            "message_id": str(state.get("message_id", "")),
            "samples": valid_samples[-MAX_SAMPLES:],
            "last_chart_update": float(state.get("last_chart_update", 0.0) or 0.0),
        }
    except (OSError, ValueError, TypeError):
        return {"message_id": "", "samples": [], "last_chart_update": 0.0}

def save_state(state):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = STATE_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps(state), encoding="utf-8")
    temp_path.replace(STATE_PATH)

def make_candles(samples):
    """Aggregate 5-second price snapshots into sampled 15-second OHLC candles."""
    candles = []
    for sample in samples:
        try:
            timestamp, price = float(sample[0]), float(sample[1])
        except (TypeError, ValueError, IndexError):
            continue
        bucket = int(timestamp // CANDLE_SECONDS) * CANDLE_SECONDS
        if candles and candles[-1]["x"] == bucket * 1000:
            candle = candles[-1]
            candle["h"] = max(candle["h"], price)
            candle["l"] = min(candle["l"], price)
            candle["c"] = price
        else:
            candles.append({"x": bucket * 1000, "o": price, "h": price, "l": price, "c": price})
    return candles[-MAX_CANDLES:]

def make_embed(samples):
    candles = make_candles(samples)
    if not candles:
        raise ValueError("Not enough BTC price samples to build candles")
    latest = float(samples[-1][1])
    first = float(candles[0]["o"])
    change = ((latest / first) - 1.0) * 100.0 if first else 0.0
    recent_price = float(samples[-7][1]) if len(samples) >= 7 else first
    short_change = ((latest / recent_price) - 1.0) * 100.0 if recent_price else 0.0
    direction = "📈 BULLISH" if short_change > 0.025 else "📉 BEARISH" if short_change < -0.025 else "➡️ NEUTRAL"
    range_high = max(float(c["h"]) for c in candles)
    range_low = min(float(c["l"]) for c in candles)
    range_position = 100.0 * (latest - range_low) / max(0.000001, range_high - range_low)
    candle_data = [
        {"x": c["x"], "o": round(c["o"], 2), "h": round(c["h"], 2),
         "l": round(c["l"], 2), "c": round(c["c"], 2)}
        for c in candles
    ]
    config = {
        "type": "candlestick",
        "data": {"datasets": [{
            "label": "BTC/USD · 15-second sampled candles",
            "data": candle_data,
            "color": {"up": "#22c55e", "down": "#ef4444", "unchanged": "#9ca3af"},
            "borderColor": {"up": "#22c55e", "down": "#ef4444", "unchanged": "#9ca3af"},
            "wickColor": {"up": "#22c55e", "down": "#ef4444", "unchanged": "#9ca3af"},
        }]},
        "options": {
            "plugins": {"legend": {"display": False}},
            "title": {"display": True, "text": "BTC / USD · 15 SECOND SAMPLED CANDLES", "color": "#f1f5f9",
                      "font": {"size": 22, "weight": "bold"}, "padding": 16},
            "scales": {
                "x": {"type": "time", "time": {"unit": "minute"},
                     "ticks": {"color": "#f1f5f9", "font": {"size": 16}, "maxTicksLimit": 7},
                     "grid": {"color": "#263244"}},
                "y": {"position": "right", "ticks": {"color": "#f1f5f9", "font": {"size": 16}, "maxTicksLimit": 7},
                      "grid": {"color": "#263244"},
                      "title": {"display": True, "text": "BTC PRICE · USD", "color": "#f1f5f9",
                                "font": {"size": 18, "weight": "bold"}}},
            },
        },
    }
    return {
        "title": "₿ BTC LIVE · 15-SECOND CANDLESTICKS",
        "description": (
            f"💰 **BTC/USD: ${latest:,.2f}**\n"
            f"📊 Window change: **{change:+.2f}%**\n"
            f"⚡ Short trend: **{direction}**\n"
            f"🔺 1-hour high: **${range_high:,.2f}**\n"
            f"🔻 1-hour low: **${range_low:,.2f}**\n"
            f"📍 Price is **{range_position:.0f}%** from the low toward the high.\n"
            f"🕯️ Candles: **{len(candles)}** · chart refresh: **{UPDATE_SECONDS}s**\n\n"
            "EASY GUIDE: green candle = price went up in that candle; red = price went down. "
            "High = highest sampled price in this chart. Low = lowest sampled price. "
            "Near the high can mean price is stretched; near the low can mean price is weak. "
            "Neither guarantees the next move.\n"
            "Candles are built from Coinbase last-trade snapshots collected every "
            f"{SAMPLE_SECONDS}s and grouped into {CANDLE_SECONDS}s OHLC bars; "
            "they are sampled candles, not exchange-native 15-second candles."
        ),
        "color": 0x19C37D if change >= 0 else 0xEF4444,
        "image": {"url": "attachment://btc_15s_chart.png"},
        "footer": {"text": "Coinbase BTC-USD · read-only chart · paper trading only"},
        "timestamp": utc_now().isoformat()
    }, config

def update_discord(webhook, state, embed, chart_config):
    chart_response = SESSION.post(
        "https://quickchart.io/chart",
        json={
            "version": "4", "width": 1400, "height": 800,
            "devicePixelRatio": 2, "format": "png",
            "backgroundColor": "#111627", "chart": chart_config,
        },
        timeout=HTTP_TIMEOUT,
    )
    if chart_response.status_code >= 400:
        detail = chart_response.headers.get("X-quickchart-error", chart_response.text[:600])
        raise RuntimeError(f"QuickChart HTTP {chart_response.status_code}: {detail}")
    if not chart_response.headers.get("Content-Type", "").startswith("image/"):
        raise RuntimeError("QuickChart did not return a chart image")
    message_id = state.get("message_id", "")
    payload = {
        "embeds": [embed],
        "attachments": [{"id": 0, "filename": "btc_15s_chart.png"}],
        "allowed_mentions": {"parse": []},
    }
    files = {"files[0]": ("btc_15s_chart.png", chart_response.content, "image/png")}
    data = {"payload_json": json.dumps(payload)}
    if message_id:
        response = SESSION.patch(
                webhook.rstrip("/") + "/messages/" + message_id,
                data=data, files=files, timeout=HTTP_TIMEOUT,
            )
        if response.status_code != 404:
            if response.status_code >= 400:
                raise RuntimeError(f"Discord edit HTTP {response.status_code}: {response.text[:600]}")
            print("BTC 15-second candlestick chart message updated.", flush=True)
            return
        print("Saved chart message was missing; recreating one.", flush=True)
    response = SESSION.post(
        webhook.rstrip("/") + "?wait=true",
        data=data, files=files, timeout=HTTP_TIMEOUT,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Discord post HTTP {response.status_code}: {response.text[:600]}")
    new_id = str(response.json().get("id", ""))
    if not new_id:
        raise RuntimeError("Discord accepted chart request but returned no message ID")
    state["message_id"] = new_id
    save_state(state)
    print("BTC candlestick chart message created; ID saved.", flush=True)

def main():
    print("BTC 15-second sampled candlestick chart started; read-only.", flush=True)
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
            state["samples"] = state["samples"][-MAX_SAMPLES:]
            save_state(state)
            if sampled_at - float(state.get("last_chart_update", 0.0) or 0.0) >= UPDATE_SECONDS:
                embed, chart_config = make_embed(state["samples"])
                update_discord(webhook, state, embed, chart_config)
                state["last_chart_update"] = sampled_at
                save_state(state)
            retry_delay = 5
            time.sleep(SAMPLE_SECONDS)
        except Exception as exc:
            print(f"BTC chart update error: {type(exc).__name__}: {exc}; retrying in {retry_delay}s.", flush=True)
            time.sleep(retry_delay)
            retry_delay = min(120, retry_delay * 2)

if __name__ == "__main__":
    main()
