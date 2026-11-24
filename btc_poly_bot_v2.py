
                record_observation(market["slug"], minutes, side, model_p, market_p, edge)
    paper_result = "NO ENTRY"
    if chosen and not reasons:
        paper_result = create_paper_trade(market, minutes, side, token, chosen["ask"])
    return {
        "minutes": minutes, "market": market, "remaining": remaining,
        "tech": tech, "side": side, "book": chosen, "model_p": model_p,
        "market_p": market_p, "edge": edge, "reasons": reasons,
        "paper_result": paper_result,
    }


def format_report(r):
    t = r["tech"]
    lines = [
        f"BTC {r['minutes']}M | {r['market']['slug']}",
        f"Signal: {r['side']} | votes {t['score']}/5",
        f"RSI: {t['rsi']:.1f} | fast move: {t['fast_move']:+.3f}%",
        f"Slow move: {t['slow_move']:+.3f}% | volume: {t['volume_ratio']:.2f}x",
        f"1m volatility: {t['volatility']:.4f}%",
        f"Time remaining: {r['remaining']}s",
    ]
    if r["book"]:
        b = r["book"]
        lines.extend([
            f"Bid/ask: ${b['bid']:.3f}/${b['ask']:.3f} | spread: {b['spread']:.3f}",
            f"Ask depth: ${b['ask_depth_usd']:.0f} | book imbalance: {b['imbalance']:+.2f}",
            f"Raw model estimate: {r['model_p'] * 100:.1f}% (UNCALIBRATED)",
            f"Market midpoint: {r['market_p'] * 100:.1f}%",
            f"Raw edge proxy: {r['edge'] * 100:+.1f} percentage points",
        ])
    lines.append(f"Paper action: {r['paper_result']}")
    if r["reasons"]:
        lines.append("Entry blocked: " + "; ".join(r["reasons"]))
    lines.append("PAPER ONLY — no real orders")
    return "\n".join(lines)


def notify_signal(result, message):
    side = result["side"]
    if side not in ("UP", "DOWN"):
        return
    key = f"alert:{result['minutes']}:{result['market']['slug']}:{side}"
    if scalar("SELECT value FROM bot_state WHERE key=?", (key,)):
        return
    emoji = "🟢" if side == "UP" else "🔴"
    if send_telegram(f"{emoji} BTC {result['minutes']}M {side} SIGNAL\n\n{message}"):
        execute("INSERT OR REPLACE INTO bot_state(key,value) VALUES (?,?)", (key, iso_now()))
    if result["paper_result"].startswith("PAPER ENTRY RECORDED"):
        send_telegram("🧾 " + result["paper_result"] + "\n" + message)


def resolve_paper_trades():
    with db() as con:
        trades = con.execute("""
            SELECT id,market_slug,side,stake_usd,
                   COALESCE(remaining_shares,shares),COALESCE(realized_pnl_usd,0)
            FROM paper_trades WHERE status='OPEN'
        """).fetchall()
    for tid, slug, side, stake, remaining, realized in trades:
        try:
            event = get_json(f"{GAMMA}/events/slug/{slug}")
            if isinstance(event, list):
                event = event[0] if event else None
            markets = (event or {}).get("markets") or []
            market = markets[0] if markets else None
            if not market or not market.get("closed"):
                continue
            outcomes, prices = market.get("outcomes", ["Up", "Down"]), market.get("outcomePrices")
            if isinstance(outcomes, str):
                outcomes = json.loads(outcomes)
            if isinstance(prices, str):
                prices = json.loads(prices)
            if not prices or len(prices) != len(outcomes):
                continue
            pm = {str(o).lower(): float(p) for o, p in zip(outcomes, prices)}
            winner = "UP" if pm.get("up", 0) >= 0.99 else "DOWN" if pm.get("down", 0) >= 0.99 else None
            if not winner:
                continue
            payout = remaining if side == winner else 0.0
            entry = scalar("SELECT entry_price FROM paper_trades WHERE id=?", (tid,))
            pnl = realized + payout - remaining * entry
            execute("""
                UPDATE paper_trades SET status='RESOLVED',exit_value=COALESCE(exit_value,0)+?,
                pnl_usd=?,resolved_at=?,remaining_shares=0
                WHERE id=? AND status='OPEN'
            """, (payout, pnl, iso_now(), tid))
            send_telegram(
                f"📊 PAPER TRADE RESOLVED\n{slug}\nPosition: {side} | Result: {winner}\n"
                f"Stake: ${stake:.2f} | P/L: ${pnl:+.2f}"
            )
        except Exception as exc:
            print(f"Paper resolution error for {slug}: {exc}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--duration", type=int, default=0)
    args = parser.parse_args()
    init_db()
    print("BTC PAPER BOT v4 started. Scalp manager enabled; live orders disabled.")
    print(f"Timeframes: {TIMEFRAMES}; paper stake: ${PAPER_STAKE_USD:.2f}")
    started = time.time()
    while True:
        try:
            resolve_paper_trades()
            resolve_observations()
            candles = fetch_candles()
            for minutes in TIMEFRAMES:
                try:
                    result = scan_timeframe(minutes, candles)
                    message = format_report(result)
                    print("\n" + "-" * 60 + "\n" + message)
                    notify_signal(result, message)
                except Exception as exc:
                    error = f"BTC {minutes}M SCAN ERROR\n{type(exc).__name__}: {exc}\nNo real order was placed."
                    print(error)
                    send_telegram(error)
            print(calibration_summary())
        except Exception:
            traceback.print_exc()
            send_telegram("⚠️ BTC BOT ERROR:\n" + traceback.format_exc()[-2500:])
        if args.once:
            break
        if args.duration and time.time() - started >= args.duration:
            break
        time.sleep(SCAN_INTERVAL_SECONDS)

