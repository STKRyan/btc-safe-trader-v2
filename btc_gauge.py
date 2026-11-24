"""
BTC Hold Strength / Flip Risk gauge for a Python Telegram bot.

Uses Binance public Spot market-data endpoints; no Binance API key is required.
This module calculates alerts only. It never places or closes trades.

Install: no third-party packages required (Python 3.9+).

Example:
    from btc_gauge import build_btc_gauge_message

    message = build_btc_gauge_message(
        side="LONG",          # "LONG" or "SHORT"
        entry_price=95000.0,  # set None if you do not have an open position
    )
    await update.message.reply_text(message, parse_mode="HTML")
"""

from __future__ import annotations

import json
import math
import statistics
import urllib.parse
import urllib.request
from typing import Optional

BASE_URL = "https://data-api.binance.vision"
DEFAULT_SYMBOL = "BTCUSDT"


def _get_json(path: str, params: dict):
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{BASE_URL}{path}?{query}",
        headers={"User-Agent": "BTC-Telegram-Gauge/1.0"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _closes_from_klines(rows):
    # Binance's final kline may still be forming. Exclude it.
    completed = rows[:-1]
    return [float(row[4]) for row in completed]


def _rsi(closes, period: int = 14) -> float:
    if len(closes) < period + 1:
        raise ValueError("Not enough completed candles to calculate RSI.")
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = changes[-period:]
    gains = [max(change, 0.0) for change in recent]
    losses = [max(-change, 0.0) for change in recent]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _return_pct(newer: float, older: float) -> float:
    return ((newer / older) - 1.0) * 100.0 if older else 0.0


def _stddev(values) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def get_binance_market_data(symbol: str = DEFAULT_SYMBOL) -> dict:
    """Fetch completed 1m candles, current ticker, and top-20 order book."""
    symbol = symbol.upper().strip()
    if not symbol.isalnum():
        raise ValueError("Invalid Binance symbol.")

    rows = _get_json(
        "/api/v3/klines",
        {"symbol": symbol, "interval": "1m", "limit": 100},
    )
    closes = _closes_from_klines(rows)
    if len(closes) < 40:
        raise ValueError("Binance returned too few completed candles.")

    book = _get_json(
        "/api/v3/depth",
        {"symbol": symbol, "limit": 20},
    )
    ticker = _get_json("/api/v3/ticker/price", {"symbol": symbol})

    bids = [(float(price), float(qty)) for price, qty in book["bids"]]
    asks = [(float(price), float(qty)) for price, qty in book["asks"]]
    if not bids or not asks:
        raise ValueError("Binance order book is empty.")

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    midpoint = (best_bid + best_ask) / 2.0
    spread_pct = ((best_ask - best_bid) / midpoint) * 100.0 if midpoint else 0.0

    bid_qty = sum(qty for _, qty in bids)
    ask_qty = sum(qty for _, qty in asks)
    total_qty = bid_qty + ask_qty
    imbalance = ((bid_qty - ask_qty) / total_qty) if total_qty else 0.0

    one_min_returns = [
        _return_pct(closes[i], closes[i - 1])
        for i in range(1, len(closes))
    ]
    recent_vol = _stddev(one_min_returns[-10:])
    baseline_vol = _stddev(one_min_returns[-40:-10])
    volatility_ratio = (
        recent_vol / baseline_vol if baseline_vol > 0 else 1.0
    )

    return {
        "symbol": symbol,
        "price": float(ticker["price"]),
        "closes": closes,
        "rsi": _rsi(closes),
        "momentum_3m_pct": _return_pct(closes[-1], closes[-4]),
        "momentum_10m_pct": _return_pct(closes[-1], closes[-11]),
        "volatility_ratio": volatility_ratio,
        "book_imbalance": imbalance,
        "spread_pct": spread_pct,
    }


def calculate_btc_gauge(
    market: dict,
    side: str = "LONG",
    entry_price: Optional[float] = None,
) -> dict:
    """
    Return separate 0-100 hold and flip-risk indices.
    side is the position direction: LONG or SHORT.
    """
    side = side.upper().strip()
    if side not in ("LONG", "SHORT"):
        raise ValueError("side must be LONG or SHORT")

    direction = 1.0 if side == "LONG" else -1.0
    price = market["price"]
    rsi = market["rsi"]
    m3 = market["momentum_3m_pct"] * direction
    m10 = market["momentum_10m_pct"] * direction
    book = market["book_imbalance"] * direction
    vol_ratio = market["volatility_ratio"]
    spread = market["spread_pct"]

    # Hold score: starts neutral, then rewards alignment and penalizes stress.
    hold = 50.0
    hold += 12 if m3 > 0 else -12
    hold += 16 if m10 > 0 else -16

    if side == "LONG":
        hold += 9 if 45 <= rsi <= 65 else (-14 if rsi < 40 or rsi > 78 else 0)
    else:
        hold += 9 if 35 <= rsi <= 55 else (-14 if rsi > 60 or rsi < 22 else 0)

    hold += 10 if book > 0.10 else (-10 if book < -0.10 else 0)
    if vol_ratio >= 2.5:
        hold -= 15
    elif vol_ratio >= 1.7:
        hold -= 8
    if spread >= 0.08:
        hold -= 8

    pnl_pct = None
    if entry_price is not None:
        entry_price = float(entry_price)
        if entry_price <= 0:
            raise ValueError("entry_price must be greater than zero.")
        pnl_pct = _return_pct(price, entry_price) * direction
        if pnl_pct < 0:
            hold -= min(18.0, abs(pnl_pct) * 2.0)

    hold = round(_clamp(hold), 1)

    # Flip score: independent index, not mathematically defined as 100 - hold.
    flip = 15.0
    flip += 18 if m3 < 0 else 0
    flip += 22 if m10 < 0 else 0

    if side == "LONG":
        flip += 15 if rsi < 40 else (8 if rsi < 45 else 0)
        flip += 8 if rsi > 78 else 0
    else:
        flip += 15 if rsi > 60 else (8 if rsi > 55 else 0)
        flip += 8 if rsi < 22 else 0

    flip += 12 if book < -0.15 else (6 if book < -0.05 else 0)
    flip += 12 if vol_ratio >= 2.5 else (6 if vol_ratio >= 1.7 else 0)
    flip += 6 if spread >= 0.08 else 0

    if pnl_pct is not None and pnl_pct < 0:
        flip += min(20.0, abs(pnl_pct) * 2.5)

    flip = round(_clamp(flip), 1)

    # Hard thresholds from the requested management rules.
    if pnl_pct is not None and pnl_pct <= -8.0:
        signal = "STOP / EXIT"
    elif pnl_pct is not None and pnl_pct >= 10.0:
        signal = "TAKE PROFIT / EXIT"
    elif flip >= 80:
        signal = "EXIT WARNING"
    elif flip >= 65:
        signal = "CONSIDER PARTIAL PROFIT"
    elif hold >= 70:
        signal = "HOLD"
    else:
        signal = "WATCH CLOSELY"

    return {
        "symbol": market["symbol"],
        "price": price,
        "side": side,
        "hold_strength": hold,
        "flip_risk": flip,
        "signal": signal,
        "rsi": round(rsi, 1),
        "momentum_3m_pct": round(market["momentum_3m_pct"], 3),
        "momentum_10m_pct": round(market["momentum_10m_pct"], 3),
        "volatility_ratio": round(vol_ratio, 2),
        "book_imbalance": round(market["book_imbalance"], 3),
        "spread_pct": round(spread, 4),
        "pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
    }


def format_btc_gauge_message(result: dict) -> str:
    """Format the result as a Telegram HTML message."""
    pnl_line = (
        f"Position P/L: <b>{result['pnl_pct']:+.2f}%</b>\n"
        if result["pnl_pct"] is not None
        else "Position P/L: <i>entry price not set</i>\n"
    )
    return (
        f"📊 <b>{result['symbol']} POSITION GAUGE</b>\n"
        f"Price: <b>${result['price']:,.2f}</b>\n"
        f"Position: <b>{result['side']}</b>\n\n"
        f"🟢 Hold strength: <b>{result['hold_strength']:.1f}/100</b>\n"
        f"🟠 Flip risk: <b>{result['flip_risk']:.1f}/100</b>\n"
        f"🚦 Signal: <b>{result['signal']}</b>\n\n"
        f"3m momentum: {result['momentum_3m_pct']:+.3f}%\n"
        f"10m momentum: {result['momentum_10m_pct']:+.3f}%\n"
        f"RSI(14): {result['rsi']:.1f}\n"
        f"Volatility ratio: {result['volatility_ratio']:.2f}x\n"
        f"Order-book imbalance: {result['book_imbalance']:+.3f}\n"
        f"Bid/ask spread: {result['spread_pct']:.4f}%\n"
        f"{pnl_line}\n"
        "<i>Risk indices, not probabilities or guaranteed predictions. "
        "Alerts only; no trades are executed.</i>"
    )


def build_btc_gauge_message(
    side: str = "LONG",
    entry_price: Optional[float] = None,
    symbol: str = DEFAULT_SYMBOL,
) -> str:
    """One-call helper for a Telegram command or scheduled job."""
    market = get_binance_market_data(symbol)
    result = calculate_btc_gauge(
        market=market,
        side=side,
        entry_price=entry_price,
    )
    return format_btc_gauge_message(result)
