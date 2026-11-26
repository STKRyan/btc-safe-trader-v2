#!/usr/bin/env python3
"""Standalone BTC chart updater. Read-only price data; never trades or edits bot state."""
from __future__ import annotations

import json
import os
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

import requests

COINBASE = "https://api.exchange.coinbase.com/products/BTC-USD/candles"
HTTP_TIMEOUT = 15
UPDATE_SECONDS = max(30, int(os.getenv("BTC_CHART_UPDATE_SECONDS", "30")))
STATE_PATH = Path(os.getenv("BTC_CHART_STATE_PATH", "data/btc_chart_state.json"))
WEBHOOK_ENV = "DISCORD_BTC_CHART_WEBHOOK_URL"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "BTC-Chart-Agent/1.0"})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_candles() -> list[list[float]]:
    response = SESSION.get(COINBASE, params={"granularity": 60}, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, list):
        raise ValueError("Coinbase returned an unexpected candle response")
    candles = sorted(
        [c for c in data if isinstance(c, list) and len(c) >= 6],
        key=lambda c: float(c[0]),
    )
    if len(candles) < 2:
        raise ValueError("Not enough BTC candles to draw chart")
    return candles[-60:]


def calculate_rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    gains = []
    losses = []
    for before, after in zip(closes[-period-1:-1], closes[-period:]):
        change = after - before
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))


def make_embed(candles: list[list[float]]) -> dict:
    closes = [round(float(c[4]), 2) for c in candles]
    times = [datetime.fromtimestamp(float(c[0]), timezone.utc).strftime("%H:%M") for c in candles]
    latest = closes[-1]
    change = ((latest / closes[0]) - 1.0) * 100.0 if closes[0] else 0.0
    rsi = calculate_rsi(closes)
    short_move = ((closes[-1] / closes[-4]) - 1.0) * 100.0 if len(closes) >= 4 and closes[-4] else 0.0
    side = "UP" if short_move > 0.02 else "DOWN" if short_move < -0.02 else "WAIT"
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
    chart_url = (
        "https://quickchart.io/chart?width=900&height=420&format=png"
        "&backgroundColor=white&c=" + urllib.parse.quote(json.dumps(config), safe="")
    )
    return {
        "title": "BTC LIVE PRICE GRAPH",
        "description": (
            f"BTC {latest:,.2f} USD · Rolling 60m: {change:+.2f}%\n"
            f"Scanner: {side} · RSI {rsi:.1f}\n"
            "1-minute candles · refreshes about every 30 seconds"
        ),
        "color": 0x19C37D if change >= 0 else 0xEF4444,
        "image": {"url": chart_url},
        "footer": {"text": "BTC price visualization · paper trading only"},
        "timestamp": utc_now().isoformat()
    }


def load_message_id() -> str:
    try:
        state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        return str(state.get("message_id", ""))
    except (OSError, ValueError, TypeError):
        return ""


def save_message_id(message_id: str) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = STATE_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps({"message_id": message_id}), encoding="utf-8")
    temp_path.replace(STATE_PATH)


def update_discord(webhook: str, embed: dict) -> None:
    message_id = load_message_id()
    payload = {"embeds": [embed], "allowed_mentions": {"parse": []}}
    if message_id:
        response = SESSION.patch(
            webhook.rstrip("/") + "/messages/" + message_id,
            json=payload, timeout=HTTP_TIMEOUT
        )
        if response.status_code != 404:
            response.raise_for_status()
            print("BTC chart message updated.", flush=True)
            return
        print("Saved chart message was missing; recreating one.", flush=True)

    response = SESSION.post(
        webhook.rstrip("/") + "?wait=true",
        json=payload, timeout=HTTP_TIMEOUT
    )
    response.raise_for_status()
    new_id = str(response.json().get("id", ""))
    if not new_id:
        raise RuntimeError("Discord accepted chart request but returned no message ID")
    save_message_id(new_id)
    print("BTC chart message created; ID saved.", flush=True)


def main() -> None:
    print("Standalone BTC chart agent started; read-only and paper-mode display only.", flush=True)
    retry_delay = 5
    last_missing_notice = 0.0
    while True:
        webhook = os.getenv(WEBHOOK_ENV, "").strip()
        if not webhook:
            if time.time() - last_missing_notice >= 300:
                print(f"{WEBHOOK_ENV} is not set on this chart service; waiting safely.", flush=True)
                last_missing_notice = time.time()
            time.sleep(30)
            continue
        if not (webhook.startswith("https://discord.com/api/webhooks/") or
                webhook.startswith("https://discordapp.com/api/webhooks/")):
            print("Chart webhook format is invalid; waiting safely.", flush=True)
            time.sleep(300)
            continue
        try:
            candles = get_candles()
            embed = make_embed(candles)
            update_discord(webhook, embed)
            retry_delay = 5
            time.sleep(UPDATE_SECONDS)
        except Exception as exc:
            # Never log the webhook URL or credentials.
            print(f"BTC chart update error: {type(exc).__name__}: {exc}; retrying in {retry_delay}s.", flush=True)
            time.sleep(retry_delay)
            retry_delay = min(120, retry_delay * 2)


if __name__ == "__main__":
    main()
