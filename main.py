import os
import uuid
import json
import asyncio
import base64
from datetime import datetime, timezone
from typing import Dict

import httpx
from fastapi import FastAPI, HTTPException, Body, Query, BackgroundTasks, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from metaapi_cloud_sdk import MetaApi

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN")
if not METAAPI_TOKEN:
    raise RuntimeError("METAAPI_TOKEN env var is not set")

api = MetaApi(METAAPI_TOKEN)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# accountId -> live RPC connection (in-memory; fine for single-user/dev use)
connections: Dict[str, object] = {}


async def _connect_account(login: str, password: str, server: str, platform: str = "mt5"):
    """Shared connect logic — used by both the manual login and the autotrade loop.
    MetaApi keeps the account deployed/running in ITS OWN cloud once deployed, so this
    reconnects to that same always-on instance rather than spinning up anything new."""
    account_api = api.metatrader_account_api

    existing_accounts = await account_api.get_accounts_with_infinite_scroll_pagination()
    account = next(
        (a for a in existing_accounts if a.login == login),
        None,
    )

    if account is None:
        account = await account_api.create_account(
            {
                "name": f"{login}-{server}-{uuid.uuid4().hex[:6]}",
                "type": "cloud",
                "login": login,
                "password": password,
                "server": server,
                "platform": platform,
                "magic": 1000,
            }
        )

    await account.deploy()
    await account.wait_connected()

    connection = account.get_rpc_connection()
    await connection.connect()
    await connection.wait_synchronized()

    connections[account.id] = connection
    return account.id, connection


@app.post("/api/connect")
async def connect(payload: dict = Body(...)):
    login = payload.get("login")
    password = payload.get("password")
    server = payload.get("server")
    platform = payload.get("platform", "mt5")

    if not all([login, password, server]):
        raise HTTPException(400, "login, password, server are required")

    try:
        account_id, _ = await _connect_account(login, password, server, platform)
        return {"accountId": account_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Connect failed: {str(e)}")


def _get_connection(account_id: str):
    conn = connections.get(account_id)
    if not conn:
        raise HTTPException(404, "No active connection for this accountId. Call /api/connect again.")
    return conn


@app.get("/api/account/{account_id}")
async def get_account_info(account_id: str):
    conn = _get_connection(account_id)
    info = await conn.get_account_information()
    return info


@app.get("/api/positions/{account_id}")
async def get_positions(account_id: str):
    conn = _get_connection(account_id)
    positions = await conn.get_positions()
    return positions


@app.get("/api/price/{account_id}/{symbol}")
async def get_price(account_id: str, symbol: str):
    conn = _get_connection(account_id)
    price = await conn.get_symbol_price(symbol)
    return price


REAL_UNSUPPORTED_SYMBOLS = {"BTCUSD", "ETHUSD"}  # not offered on this MT5 demo — real orders for these always fail


async def _place_trade(conn, symbol: str, side: str, volume: float, sl=None, tp=None,
                        confidence=None, reason: str = "", source: str = "manual", account: str = "real"):
    if account == "real" and symbol.upper() in REAL_UNSUPPORTED_SYMBOLS:
        raise ValueError(f"{symbol} isn't available on the real MT5 account (demo has no crypto) — skipping real trade")

    volume = _enforce_min_lot(symbol, volume)

    # Estimate entry price up front so a real broker-side SL/TP can be attached
    # at order time — this makes the $ profit/loss cap the BROKER's job, closing
    # the position instantly, instead of depending on the bot polling afterward.
    entry_estimate = None
    try:
        price_info = await conn.get_symbol_price(symbol)
        entry_estimate = price_info.get("ask") if side == "buy" else price_info.get("bid")
    except Exception:
        pass

    final_sl = float(sl) if sl else None
    final_tp = float(tp) if tp else None
    if entry_estimate and account == "real":
        final_sl = _price_for_usd_pnl(symbol, side, entry_estimate, volume, -HARD_LOSS_CLOSE_USD)
        final_tp = _price_for_usd_pnl(symbol, side, entry_estimate, volume, HARD_PROFIT_CLOSE_USD)

    opts = {}
    if final_sl:
        opts["stop_loss"] = float(final_sl)
    if final_tp:
        opts["take_profit"] = float(final_tp)

    if side == "buy":
        result = await conn.create_market_buy_order(symbol, float(volume), **opts)
    elif side == "sell":
        result = await conn.create_market_sell_order(symbol, float(volume), **opts)
    else:
        raise HTTPException(400, "side must be 'buy' or 'sell'")

    position_id = (result or {}).get("positionId") or (result or {}).get("orderId") or uuid.uuid4().hex[:10]
    entry_price = (result or {}).get("price") or entry_estimate
    if entry_price is None:
        try:
            price_info = await conn.get_symbol_price(symbol)
            entry_price = price_info.get("ask") if side == "buy" else price_info.get("bid")
        except Exception:
            entry_price = None

    if account == "real":
        _tracked_real_positions[position_id] = {"symbol": symbol, "side": side, "sl": final_sl, "tp": final_tp}

    if entry_price is not None:
        _log_trade_open(
            position_id=position_id, symbol=symbol, side=side, volume=volume, entry=entry_price,
            sl=final_sl, tp=final_tp,
            confidence=confidence, reason=reason, source=source, account=account,
        )
    return result


@app.post("/api/trade/{account_id}")
async def place_trade(account_id: str, payload: dict = Body(...)):
    conn = _get_connection(account_id)
    symbol = payload.get("symbol")
    side = payload.get("side")
    volume = payload.get("volume")
    sl = payload.get("sl")
    tp = payload.get("tp")

    if not all([symbol, side, volume]):
        raise HTTPException(400, "symbol, side, volume are required")

    return await _place_trade(conn, symbol, side, volume, sl, tp, source="manual", account="real")


@app.post("/api/close/{account_id}/{position_id}")
async def close_position(account_id: str, position_id: str):
    conn = _get_connection(account_id)
    result = await conn.close_position(position_id)
    pnl = (result or {}).get("profit") if isinstance(result, dict) else None
    if pnl is not None:
        _log_trade_close(position_id, pnl)
    else:
        for t in reversed(trade_log):
            if t["id"] == position_id and t["status"] == "open":
                t["status"] = "closed"
                break
    return result


TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")


async def _fetch_candles(symbol: str, interval: str = "15min", outputsize: int = 50):
    if not TWELVEDATA_API_KEY:
        raise HTTPException(500, "TWELVEDATA_API_KEY is not set on the server")

    # Twelve Data wants "XAU/USD" style, not "XAUUSD" — normalize either input
    clean = symbol.upper().replace(" ", "")
    if "/" not in clean and len(clean) == 6:
        clean = f"{clean[:3]}/{clean[3:]}"

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": clean,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": TWELVEDATA_API_KEY,
    }
    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params)

    if r.status_code != 200:
        raise HTTPException(502, f"Twelve Data HTTP {r.status_code}: {r.text[:200]}")

    data = r.json()
    if data.get("status") == "error":
        raise HTTPException(502, f"Twelve Data error: {data.get('message', 'unknown')} (code {data.get('code')})")

    values = data.get("values", [])
    candles = [
        {
            "time": v["datetime"],
            "open": float(v["open"]),
            "high": float(v["high"]),
            "low": float(v["low"]),
            "close": float(v["close"]),
        }
        for v in reversed(values)
    ]
    return candles


_chart_cache: Dict[str, tuple] = {}  # "symbol:interval:outputsize" -> (timestamp, candles)
CHART_CACHE_TTL = 60  # seconds


@app.get("/api/chart")
async def get_chart(symbol: str, interval: str = "5min", outputsize: int = 100):
    key = f"{symbol.upper()}:{interval}:{outputsize}"
    now = datetime.now(timezone.utc).timestamp()
    cached = _chart_cache.get(key)
    if cached and now - cached[0] < CHART_CACHE_TTL:
        return {"symbol": symbol.upper(), "candles": cached[1]}
    try:
        candles = await _fetch_candles(symbol, interval, outputsize)
    except HTTPException:
        if cached:
            return {"symbol": symbol.upper(), "candles": cached[1]}
        raise
    _chart_cache[key] = (now, candles)
    return {"symbol": symbol.upper(), "candles": candles}


# ---------------- Built-in simulated demo account (no MetaApi needed) ----------------

sim_account = {"balance": 5000.0, "positions": []}  # positions: list of dicts
_price_cache: Dict[str, tuple] = {}  # symbol -> (timestamp, price)
PRICE_CACHE_TTL = 30  # seconds

# ---------------- Unified trade log ----------------
# Every trade — manual, chat-triggered, or auto-traded, real or demo — is logged
# here so the Chat AI and the Auto-Trading AI share one source of truth.
trade_log = []  # newest last; persisted to GitHub


def _log_trade_open(*, position_id: str, symbol: str, side: str, volume: float, entry: float,
                     sl=None, tp=None, confidence=None, reason: str = "", source: str = "manual", account: str = "demo"):
    expected_profit = _calc_pnl_dollars(symbol, side, entry, tp, volume) if tp else None
    expected_loss = _calc_pnl_dollars(symbol, side, entry, sl, volume) if sl else None
    record = {
        "id": position_id,
        "time": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "side": side,
        "volume": volume,
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "risk_reward": _risk_reward(entry, sl, tp),
        "expected_profit": round(expected_profit, 2) if expected_profit is not None else None,
        "expected_loss": round(expected_loss, 2) if expected_loss is not None else None,
        "actual_pnl": None,
        "confidence": confidence,
        "reason": reason,
        "source": source,  # "manual" | "chat" | "autotrade" | "autotrade-sim"
        "account": account,  # "real" | "demo"
        "status": "open",
    }
    trade_log.append(record)
    del trade_log[:-200]
    return record


def _log_trade_close(position_id: str, actual_pnl):
    for t in reversed(trade_log):
        if t["id"] == position_id and t["status"] == "open":
            t["status"] = "closed"
            t["actual_pnl"] = round(actual_pnl, 2) if actual_pnl is not None else None
            t["closed_time"] = datetime.now(timezone.utc).isoformat()
            return t
    return None

MAX_LOSS_USD = float(os.getenv("MAX_LOSS_USD", "300"))
PROFIT_TARGET_USD = float(os.getenv("PROFIT_TARGET_USD", "300"))

# Hard $ auto-close thresholds — enforced for BOTH the real and demo accounts,
# independent of the AI momentum-management above. No AI judgment call here:
# the instant floating P/L crosses either line, the position is closed. For the
# REAL account this is attached as an actual broker-side SL/TP order at trade
# open time (see _place_trade / _price_for_usd_pnl below), so it fires instantly
# without needing the bot to poll and notice.
HARD_PROFIT_CLOSE_USD = float(os.getenv("HARD_PROFIT_CLOSE_USD", "100"))
HARD_LOSS_CLOSE_USD = float(os.getenv("HARD_LOSS_CLOSE_USD", "50"))

# Daily win/loss streak control. Hitting either limit doesn't stop the bot for
# good — it switches into a slower, choosier, smaller-size "cautious mode" for
# the rest of the UTC day: longer gap between checks, much higher confidence
# bar required to open a new trade, and a much smaller lot size.
DAILY_WIN_LIMIT = int(os.getenv("DAILY_WIN_LIMIT", "2"))
DAILY_LOSS_LIMIT = int(os.getenv("DAILY_LOSS_LIMIT", "3"))
CAUTIOUS_INTERVAL_SECONDS = int(os.getenv("CAUTIOUS_INTERVAL_SECONDS", "3600"))  # 1 hour
CAUTIOUS_MIN_CONFIDENCE = float(os.getenv("CAUTIOUS_MIN_CONFIDENCE", "90"))
CAUTIOUS_LOT_SCALE = float(os.getenv("CAUTIOUS_LOT_SCALE", "0.3"))  # 30% of the normal lot size

_daily_trade_state = {"date": None, "wins": 0, "losses": 0, "mode": "normal"}

# id -> {symbol, side, sl, tp} for real positions opened with a hard-cap broker
# SL/TP, so _manage_real_positions can tell whether a position that vanished
# between polls was closed by hitting the profit side or the loss side.
_tracked_real_positions: Dict[str, dict] = {}


def _reset_daily_trade_state_if_needed():
    today = datetime.now(timezone.utc).date().isoformat()
    if _daily_trade_state["date"] != today:
        _daily_trade_state.update({"date": today, "wins": 0, "losses": 0, "mode": "normal"})


def _record_hard_close(is_win: bool):
    """Call whenever the hard $ rule closes a position (real or demo). Tracks the
    day's wins/losses and flips into cautious mode once either daily limit hits."""
    _reset_daily_trade_state_if_needed()
    if is_win:
        _daily_trade_state["wins"] += 1
    else:
        _daily_trade_state["losses"] += 1
    if _daily_trade_state["wins"] >= DAILY_WIN_LIMIT or _daily_trade_state["losses"] >= DAILY_LOSS_LIMIT:
        _daily_trade_state["mode"] = "cautious"


def _current_min_confidence() -> float:
    _reset_daily_trade_state_if_needed()
    return CAUTIOUS_MIN_CONFIDENCE if _daily_trade_state["mode"] == "cautious" else MIN_CONFIDENCE


def _current_lot_scale() -> float:
    _reset_daily_trade_state_if_needed()
    return CAUTIOUS_LOT_SCALE if _daily_trade_state["mode"] == "cautious" else 1.0


def _current_scan_interval() -> int:
    _reset_daily_trade_state_if_needed()
    return CAUTIOUS_INTERVAL_SECONDS if _daily_trade_state["mode"] == "cautious" else AUTOTRADE_INTERVAL_SECONDS


def _price_for_usd_pnl(symbol: str, side: str, entry_price: float, volume: float, pnl_usd: float) -> float:
    """Inverse of _calc_pnl_dollars: the exit price that yields exactly $pnl_usd
    of P/L for this side/volume starting from entry_price. Used to translate the
    hard $ profit/loss caps into an actual broker-side SL/TP price."""
    contract = CONTRACT_SIZE.get(symbol, DEFAULT_CONTRACT_SIZE)
    divisor = 1 if symbol in CONTRACT_SIZE else 10000
    direction = 1 if side == "buy" else -1
    if volume <= 0 or contract <= 0:
        return entry_price
    delta = pnl_usd * divisor / (volume * contract)
    return entry_price + delta * direction

# Rough per-instrument contract sizing — approximate, NOT exact broker specs.
# This is for realistic-feeling testing, not precise accounting.
CONTRACT_SIZE = {"XAUUSD": 100, "BTCUSD": 1, "ETHUSD": 1}
DEFAULT_CONTRACT_SIZE = 100000  # standard forex lot

# ---------------- Asset classes, per-class minimum/default lot sizes ----------------

MIN_LOT = {"crypto": 0.10, "gold": 0.01, "forex": 0.01}
DEFAULT_LOT = {"crypto": 0.10, "gold": 0.02, "forex": 0.01}


def _asset_class(symbol: str) -> str:
    s = (symbol or "").upper()
    if "BTC" in s or "ETH" in s:
        return "crypto"
    if "XAU" in s or "GOLD" in s:
        return "gold"
    return "forex"


def _enforce_min_lot(symbol: str, volume: float) -> float:
    """Crypto has a 0.10 minimum; Gold and Forex keep the 0.01 minimum."""
    cls = _asset_class(symbol)
    try:
        volume = float(volume)
    except (TypeError, ValueError):
        volume = 0.0
    return round(max(volume, MIN_LOT[cls]), 2)


def _base_lot_for_symbol(symbol: str) -> float:
    """User's saved preferred lot size for this asset class (Settings page),
    falling back to a sane default, clamped to the class minimum."""
    cls = _asset_class(symbol)
    lot_key = {"crypto": "btc", "gold": "gold", "forex": "forex"}[cls]
    base = (settings.get("lot_sizes") or {}).get(lot_key)
    if not base:
        base = DEFAULT_LOT[cls]
    return round(max(float(base), MIN_LOT[cls]), 2)


def _calc_pnl_dollars(symbol: str, side: str, entry_price: float, exit_price: float, volume: float) -> float:
    """Dollar P/L for a position of `volume` lots moving from entry_price to
    exit_price. Used for both live floating P/L and pre-trade profit/loss estimates."""
    direction = 1 if side == "buy" else -1
    contract = CONTRACT_SIZE.get(symbol, DEFAULT_CONTRACT_SIZE)
    divisor = 1 if symbol in CONTRACT_SIZE else 10000  # keep forex P/L in a sane dollar range
    return (exit_price - entry_price) * volume * contract * direction / divisor


def _risk_reward(entry: float, sl: float, tp: float):
    if not sl or not tp:
        return None
    risk = abs(entry - sl)
    reward = abs(tp - entry)
    if risk == 0:
        return None
    return round(reward / risk, 2)


async def _get_price(symbol: str) -> float:
    now = datetime.now(timezone.utc).timestamp()
    cached = _price_cache.get(symbol)
    if cached and now - cached[0] < PRICE_CACHE_TTL:
        return cached[1]
    try:
        candles = await _fetch_candles(symbol, interval="1min", outputsize=1)
        price = candles[-1]["close"] if candles else (cached[1] if cached else 0.0)
    except HTTPException:
        if cached:
            return cached[1]
        raise
    _price_cache[symbol] = (now, price)
    return price


def _sim_pnl(position: dict, current_price: float) -> float:
    return _calc_pnl_dollars(position["symbol"], position["side"], position["entry_price"], current_price, position["volume"])


@app.post("/api/sim/reset")
async def reset_sim_account():
    sim_account["balance"] = 5000.0
    sim_account["positions"] = []
    await _github_save_state()
    return {"status": "reset", "balance": sim_account["balance"]}


@app.get("/api/sim/account")
async def sim_account_info():
    total_pnl = 0.0
    for p in sim_account["positions"]:
        price = await _get_price(p["symbol"])
        total_pnl += _sim_pnl(p, price)
    return {"balance": round(sim_account["balance"], 2), "equity": round(sim_account["balance"] + total_pnl, 2), "currency": "USD"}


@app.get("/api/sim/positions")
async def sim_positions():
    result = []
    for p in sim_account["positions"]:
        price = await _get_price(p["symbol"])
        pnl = _sim_pnl(p, price)
        result.append({**p, "profit": round(pnl, 2), "type": p["side"]})
    return result


@app.post("/api/sim/trade")
async def sim_trade(payload: dict = Body(...)):
    symbol = (payload.get("symbol") or "").upper()
    side = payload.get("side")
    volume = _enforce_min_lot(symbol, payload.get("volume") or 0.01)
    sl = payload.get("sl")
    tp = payload.get("tp")

    if not symbol or side not in ("buy", "sell"):
        raise HTTPException(400, "symbol and a valid side (buy/sell) are required")

    price = await _get_price(symbol)
    pos = {
        "id": uuid.uuid4().hex[:10],
        "symbol": symbol,
        "side": side,
        "volume": volume,
        "entry_price": price,
        "sl": float(sl) if sl else None,
        "tp": float(tp) if tp else None,
        "open_time": datetime.now(timezone.utc).isoformat(),
    }
    sim_account["positions"].append(pos)
    _log_trade_open(
        position_id=pos["id"], symbol=symbol, side=side, volume=volume, entry=price,
        sl=pos["sl"], tp=pos["tp"], confidence=payload.get("confidence"),
        reason=payload.get("reason", ""), source=payload.get("source", "manual"), account="demo",
    )
    await _github_save_state()
    return pos


@app.post("/api/sim/close/{position_id}")
async def sim_close(position_id: str):
    pos = next((p for p in sim_account["positions"] if p["id"] == position_id), None)
    if not pos:
        raise HTTPException(404, "Position not found")
    price = await _get_price(pos["symbol"])
    pnl = _sim_pnl(pos, price)
    sim_account["balance"] += pnl
    sim_account["positions"].remove(pos)
    _log_trade_close(position_id, pnl)
    await _github_save_state()
    return {"closed": True, "pnl": round(pnl, 2)}


MANAGE_PROMPT = """You are managing an open {side} position on {symbol}, opened at {entry_price}.
Current floating P/L: ${pnl:.2f} (max loss allowed: -${max_loss}, profit target: ${profit_target}).

Recent {interval} candles (oldest first): {candles_json}

Decide if this position should be closed now because momentum is fading/reversing against it,
even though it hasn't hit the hard $ targets yet. Be conservative — only recommend closing early
on a genuine reversal signal, not minor noise.

Respond with ONLY raw JSON: {{"close": true|false, "reason": "one short sentence"}}
"""


async def _ask_manage_gemini(pos: dict, candles: list, pnl: float) -> dict:
    prompt = MANAGE_PROMPT.format(
        side=pos["side"], symbol=pos["symbol"], entry_price=pos["entry_price"], pnl=pnl,
        max_loss=MAX_LOSS_USD, profit_target=PROFIT_TARGET_USD,
        interval=settings["interval"], candles_json=json.dumps(candles),
    )
    try:
        text = await _call_llm_text(prompt, timeout=60)
        return json.loads(_clean_json_text(text))
    except Exception:
        return {"close": False, "reason": "management check failed"}


async def _manage_sim_positions():
    """Runs every autotrade-sim cycle before scanning for new trades. Closes
    positions that hit their actual SL/TP price level, the $ stop/target, or
    that the AI judges to be losing momentum even before hitting those limits."""
    closed = []
    for pos in list(sim_account["positions"]):
        price = await _get_price(pos["symbol"])
        pnl = _sim_pnl(pos, price)

        hit_sl = pos.get("sl") is not None and (
            (pos["side"] == "buy" and price <= pos["sl"]) or (pos["side"] == "sell" and price >= pos["sl"])
        )
        hit_tp = pos.get("tp") is not None and (
            (pos["side"] == "buy" and price >= pos["tp"]) or (pos["side"] == "sell" and price <= pos["tp"])
        )
        hit_hard_profit = pnl >= HARD_PROFIT_CLOSE_USD
        hit_hard_loss = pnl <= -HARD_LOSS_CLOSE_USD

        if hit_hard_profit or hit_hard_loss:
            reason = f"Hit hard profit close (${pnl:.2f} >= ${HARD_PROFIT_CLOSE_USD})" if hit_hard_profit else \
                     f"Hit hard stop loss close (${pnl:.2f} <= -${HARD_LOSS_CLOSE_USD})"
            result = await sim_close(pos["id"])
            _record_hard_close(hit_hard_profit)
            closed.append({"symbol": pos["symbol"], "reason": reason, "pnl": result["pnl"]})
            continue

        if hit_sl or hit_tp or pnl <= -MAX_LOSS_USD or pnl >= PROFIT_TARGET_USD:
            reason = ("Hit stop loss price level" if hit_sl else
                      "Hit take profit price level" if hit_tp else
                      f"Hit {'stop loss' if pnl < 0 else 'profit target'} (${pnl:.2f})")
            result = await sim_close(pos["id"])
            closed.append({"symbol": pos["symbol"], "reason": reason, "pnl": result["pnl"]})
            continue

        try:
            candles = await _fetch_candles(pos["symbol"], interval=settings["interval"], outputsize=20)
            decision = await _ask_manage_gemini(pos, candles, pnl)
            if decision.get("close"):
                result = await sim_close(pos["id"])
                closed.append({"symbol": pos["symbol"], "reason": decision.get("reason", "AI judged momentum was fading"), "pnl": result["pnl"]})
        except Exception:
            pass  # leave position open if the check itself fails

    return closed


async def _manage_real_positions():
    """Safety-net + tracking for the REAL MetaApi account. The hard $ profit/loss
    cap is normally enforced instantly by the broker-side SL/TP order attached
    when the trade was opened (see _place_trade) — this function (a) catches any
    position the broker didn't close for some reason (e.g. it rejected the exact
    SL/TP distance), and (b) notices positions the broker already closed on its
    own so the daily win/loss streak counters stay accurate."""
    closed = []
    if not all([MT_LOGIN, MT_PASSWORD, MT_SERVER]):
        return closed

    try:
        _, conn = await _connect_account(MT_LOGIN, MT_PASSWORD, MT_SERVER, MT_PLATFORM)
        positions = await conn.get_positions()
    except Exception:
        return closed  # connection/fetch failed — leave positions alone, try again next cycle

    open_ids = {pos["id"] for pos in positions}

    # Anything we were tracking that's no longer open was closed by the broker's
    # own SL/TP order already. Figure out win vs. loss from which target the last
    # known price sat closer to, and count it toward today's streak.
    for tracked_id in list(_tracked_real_positions.keys()):
        if tracked_id in open_ids:
            continue
        info = _tracked_real_positions.pop(tracked_id)
        try:
            price = await _get_price(info["symbol"])
            dist_to_tp = abs(price - info["tp"]) if info.get("tp") else None
            dist_to_sl = abs(price - info["sl"]) if info.get("sl") else None
            is_win = dist_to_tp is not None and (dist_to_sl is None or dist_to_tp <= dist_to_sl)
        except Exception:
            is_win = True  # can't tell — doesn't change which streak limit blocks trading
        _record_hard_close(is_win)
        closed.append({"symbol": info["symbol"], "reason": "Closed by broker SL/TP (hard cap)", "pnl": None})

    for pos in positions:
        pnl = pos.get("profit")
        if pnl is None:
            continue

        hit_hard_profit = pnl >= HARD_PROFIT_CLOSE_USD
        hit_hard_loss = pnl <= -HARD_LOSS_CLOSE_USD
        if not (hit_hard_profit or hit_hard_loss):
            continue

        reason = f"Hit hard profit close (${pnl:.2f} >= ${HARD_PROFIT_CLOSE_USD})" if hit_hard_profit else \
                 f"Hit hard stop loss close (${pnl:.2f} <= -${HARD_LOSS_CLOSE_USD})"
        try:
            await conn.close_position(pos["id"])
            _log_trade_close(pos["id"], pnl)
            _record_hard_close(hit_hard_profit)
            _tracked_real_positions.pop(pos["id"], None)
            closed.append({"symbol": pos.get("symbol"), "reason": reason, "pnl": pnl})
        except Exception:
            pass  # leave it open if the close call itself fails — retried next cycle

    return closed


MARKET_HOURS_ENABLED = os.getenv("MARKET_HOURS_ENABLED", "false").lower() == "true"


def _market_status():
    """Forex market: open Sunday ~22:00 UTC through Friday ~22:00 UTC.
    Approximate — doesn't account for broker-specific holidays or DST edge cases."""
    now = datetime.now(timezone.utc)
    weekday = now.weekday()  # Mon=0 ... Sun=6
    hour = now.hour
    if weekday == 5:  # Saturday — always closed
        is_open = False
    elif weekday == 6:  # Sunday — opens ~22:00 UTC
        is_open = hour >= 22
    elif weekday == 4:  # Friday — closes ~22:00 UTC
        is_open = hour < 22
    else:
        is_open = True
    return {"is_open": is_open, "checked_at": now.isoformat(), "enforced": MARKET_HOURS_ENABLED}


_daily_run_count = {"date": None, "count": 0}
_daily_gemini_calls = {"date": None, "count": 0}
GEMINI_DAILY_SCAN_BUDGET = int(os.getenv("GEMINI_DAILY_SCAN_BUDGET", "12"))  # free tier RPD is 20 — leave headroom for chat


def _gemini_scan_budget_available() -> bool:
    """Tracks Gemini calls made by the automated scan loop only (not chat), so the
    automated 15-min cycle doesn't burn through the whole daily quota by itself."""
    today = datetime.now(timezone.utc).date().isoformat()
    if _daily_gemini_calls["date"] != today:
        _daily_gemini_calls["date"] = today
        _daily_gemini_calls["count"] = 0
    return _daily_gemini_calls["count"] < GEMINI_DAILY_SCAN_BUDGET


def _bump_gemini_scan_count():
    _daily_gemini_calls["count"] += 1


def _bump_daily_run_count():
    today = datetime.now(timezone.utc).date().isoformat()
    if _daily_run_count["date"] != today:
        _daily_run_count["date"] = today
        _daily_run_count["count"] = 0
    _daily_run_count["count"] += 1
    return _daily_run_count["count"]


async def _fetch_forex_news():
    """Free, unofficial Forex Factory calendar mirror — no API key needed.
    Returns today's high-impact events, or a note if the fetch fails."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json")
        if r.status_code != 200:
            return "(couldn't fetch news calendar right now)"
        events = r.json()
        today = datetime.now(timezone.utc).date().isoformat()
        high_impact_today = [
            f"{e.get('country')}: {e.get('title')} (forecast: {e.get('forecast', 'n/a')}, previous: {e.get('previous', 'n/a')})"
            for e in events
            if e.get("impact") == "High" and str(e.get("date", "")).startswith(today)
        ]
        if not high_impact_today:
            return "No high-impact news events scheduled today."
        return "High-impact news today: " + "; ".join(high_impact_today[:10])
    except Exception as e:
        return f"(news fetch failed: {str(e)})"


def _confidence_volume(base_volume: float, confidence: float) -> float:
    """Scale lot size with how confident the AI is — a bare-minimum-confidence
    setup gets the base size, a maximum-confidence one gets up to 3x that."""
    span = max(100 - MIN_CONFIDENCE, 1)
    scale = 1 + 2 * max(0, min(confidence - MIN_CONFIDENCE, span)) / span  # 1x to 3x
    return round(base_volume * scale, 2)


MANAGE_CHECK_INTERVAL_WHEN_MAXED = int(os.getenv("MANAGE_CHECK_INTERVAL_WHEN_MAXED", "3600"))  # 1 hour
_last_maxed_check = {"time": None}


async def _sim_positions():
    return list(sim_account["positions"])


async def _run_trade_leg(*, label: str, provider: str, positions: list, pairs_data: dict,
                          news_context: str, place_trade, get_price, entry_base: dict) -> dict:
    """One AI's independent decision + execution against one account (real+Gemini or
    sim+Groq). Both legs share this exact risk pipeline — stack rules, staleness
    check, DeepSeek veto — so a future change only has to be made once."""
    entry = dict(entry_base)
    entry["account"] = label

    open_count = len(positions)
    if open_count >= MAX_OPEN_POSITIONS:
        entry.update({"status": "skipped", "reason": f"{open_count} open {label} position(s), max is {MAX_OPEN_POSITIONS}"})
        return entry

    scan = await _ask_single_ai_scan(provider, pairs_data, news_context)
    entry["scanned"] = scan.get("scanned", {})

    action = scan.get("action", "hold")
    best_symbol = scan.get("best_symbol")
    confidence = scan.get("confidence", 0)
    min_confidence = _current_min_confidence()
    already_holding = best_symbol and any(p["symbol"] == best_symbol for p in positions)

    stack_reason = None
    if action in ("buy", "sell") and best_symbol and confidence >= min_confidence:
        if already_holding:
            stack_reason = f"Already holding an open position on {best_symbol} — skipping duplicate"
        elif open_count > 0 and confidence < STACK_MIN_CONFIDENCE:
            stack_reason = f"{open_count} position(s) already open — need {STACK_MIN_CONFIDENCE}+ confidence to stack another, got {confidence}"

    if action not in ("buy", "sell") or not best_symbol or confidence < min_confidence or stack_reason:
        entry.update({"status": "hold", "decision": scan, "hold_reason": stack_reason})
        opinion = await _ask_deepseek_opinion(
            f"[{label}/{provider}] Decision: HOLD ({stack_reason or f'no pair met the {min_confidence}+ confidence threshold'}). "
            f"Full scan: {json.dumps(scan)[:2000]}"
        )
        if opinion:
            deepseek_review["time"] = datetime.now(timezone.utc).isoformat()
            deepseek_review["text"] = f"[{label}/{provider}] {opinion}"
        return entry

    # Independent sanity check before firing — DeepSeek reviews this AI's decision.
    # (Not a "must-agree" council anymore: Gemini and Groq each own their own account now.)
    verdict_context = (
        f"[{label}/{provider}] Decision: {action.upper()} {best_symbol}, confidence {confidence}%, "
        f"reason: {scan.get('reason', '')}. Full scan: {json.dumps(scan)[:2000]}"
    )
    verdict = await _ask_deepseek_verdict(verdict_context)
    if verdict is not None and verdict.get("agree") is False:
        entry.update({
            "status": "hold",
            "decision": scan,
            "veto": {"deepseek_agree": False, "deepseek_note": verdict.get("note")},
        })
        deepseek_review["time"] = datetime.now(timezone.utc).isoformat()
        deepseek_review["text"] = f"[{label}/{provider}] Vetoed {action.upper()} {best_symbol}: {verdict.get('note', '')}"
        return entry

    # Staleness check — re-fetch price right before executing, since the AI call and
    # DeepSeek review both take real time. Applied to BOTH legs now (previously only sim had it).
    try:
        price_at_scan = await get_price(best_symbol)
        price_now = await get_price(best_symbol)
        moved_pct = abs(price_now - price_at_scan) / price_at_scan * 100 if price_at_scan else 0
    except Exception:
        price_now, moved_pct = None, 0

    if moved_pct > PRICE_STALENESS_PCT:
        entry.update({
            "status": "hold",
            "decision": scan,
            "reason": f"Price moved {moved_pct:.3f}% (>{PRICE_STALENESS_PCT}%) between decision and execution — stale, skipped",
        })
        return entry

    volume = _smart_volume(best_symbol, confidence, entry_price=price_now, sl=scan.get("stop_loss"))
    volume = _enforce_min_lot(best_symbol, round(volume * _current_lot_scale(), 2))

    try:
        result = await place_trade(best_symbol, action, volume, scan)
    except Exception as e:
        entry.update({"status": "error", "reason": f"trade execution failed: {str(e)[:200]}"})
        return entry

    entry.update({"status": "trade_placed", "decision": scan, "result": result})
    opinion = await _ask_deepseek_opinion(
        f"[{label}/{provider}] Decision: {action.upper()} {best_symbol} @ {volume} lots, confidence {confidence}%, "
        f"reason: {scan.get('reason', '')}. Full scan: {json.dumps(scan)[:2000]}"
    )
    if opinion:
        deepseek_review["time"] = datetime.now(timezone.utc).isoformat()
        deepseek_review["text"] = f"[{label}/{provider}] {opinion}"
    return entry


async def _run_autotrade_cycle():
    """One scan cycle, one shared market snapshot, two independent AIs each trading
    their own account: Gemini decides for the real MetaApi account, Groq decides for
    the built-in sim account. This is the single thing every trigger (cron, dashboard
    button, internal scheduler) calls now — no second cron job needed to compare them."""
    if autotrade_status.get("state") == "analyzing":
        return  # already running (internal scheduler + external cron overlapped) — skip
    autotrade_status.update({"state": "analyzing", "since": datetime.now(timezone.utc).isoformat(), "note": "Scanning watchlist..."})

    market = _market_status()
    if MARKET_HOURS_ENABLED and not market["is_open"]:
        autotrade_log.append({"time": datetime.now(timezone.utc).isoformat(), "market": market,
                               "status": "skipped", "reason": "Market closed (MARKET_HOURS_ENABLED)"})
        del autotrade_log[:-50]
        autotrade_status.update({"state": "idle", "note": "Market closed — skipped this cycle"})
        return

    run_number_today = _bump_daily_run_count()
    news_context = await _fetch_forex_news() if run_number_today <= 3 else "not checked this run"
    entry_base = {"time": datetime.now(timezone.utc).isoformat(), "market": market, "news_check": news_context}

    # ---- Real / Gemini leg setup ----
    real_available = all([MT_LOGIN, MT_PASSWORD, MT_SERVER])
    real_conn, real_positions = None, []
    if real_available:
        try:
            _, real_conn = await _connect_account(MT_LOGIN, MT_PASSWORD, MT_SERVER, MT_PLATFORM)
            closed = await _manage_real_positions()
            real_positions = await real_conn.get_positions()
            if closed:
                entry_base["real_closed"] = closed
        except Exception as e:
            real_available = False
            autotrade_log.append({**entry_base, "account": "real", "status": "error",
                                   "reason": f"MetaApi connect/fetch failed: {str(e)[:200]}"})

    # ---- Sim / Groq leg setup (with the existing "at max" cooldown so we're not
    # hammering the safety-close check every cycle once the book is full) ----
    sim_at_max = len(sim_account["positions"]) >= MAX_OPEN_POSITIONS
    run_sim_safety_check = True
    if sim_at_max:
        now_ts = datetime.now(timezone.utc).timestamp()
        last = _last_maxed_check["time"]
        run_sim_safety_check = last is None or now_ts - last >= MANAGE_CHECK_INTERVAL_WHEN_MAXED
        if run_sim_safety_check:
            _last_maxed_check["time"] = now_ts
    if run_sim_safety_check:
        closed = await _manage_sim_positions()
        if closed:
            entry_base["sim_closed"] = closed
    sim_positions = await _sim_positions()

    real_has_room = real_available and len(real_positions) < MAX_OPEN_POSITIONS
    sim_has_room = len(sim_positions) < MAX_OPEN_POSITIONS

    if not real_has_room and not sim_has_room:
        legs = (["real"] if real_available else []) + ["sim"]
        for label in legs:
            autotrade_log.append({**entry_base, "account": label, "status": "skipped",
                                   "reason": f"At max positions, max is {MAX_OPEN_POSITIONS}"})
        del autotrade_log[:-50]
        autotrade_status.update({"state": "idle", "note": "Both accounts at max positions — skipped scan"})
        return

    # One shared snapshot for both AIs — this is the whole point of merging the two
    # cron paths: Twelve Data's free-tier budget only gets spent once per cycle now.
    pairs_data = await _fetch_multi_snapshot()
    if pairs_data.get("_fetch_errors"):
        entry_base["fetch_errors"] = pairs_data["_fetch_errors"]

    results = []
    if real_available:
        autotrade_status["note"] = "Gemini analyzing real-account setups..."
        results.append(await _run_trade_leg(
            label="real", provider="gemini", positions=real_positions, pairs_data=pairs_data,
            news_context=news_context, entry_base=entry_base, get_price=_get_price,
            place_trade=lambda symbol, action, volume, scan: _place_trade(
                real_conn, symbol, action, volume, sl=scan.get("stop_loss"), tp=scan.get("take_profit"),
                confidence=scan.get("confidence", 0), reason=scan.get("reason", ""), source="autotrade", account="real",
            ),
        ))
    elif not all([MT_LOGIN, MT_PASSWORD, MT_SERVER]):
        results.append({**entry_base, "account": "real", "status": "skipped",
                         "reason": "MT_LOGIN/MT_PASSWORD/MT_SERVER not set — real leg disabled"})

    autotrade_status["note"] = "Groq analyzing sim-account setups..."
    results.append(await _run_trade_leg(
        label="sim", provider="groq", positions=sim_positions, pairs_data=pairs_data,
        news_context=news_context, entry_base=entry_base, get_price=_get_price,
        place_trade=lambda symbol, action, volume, scan: sim_trade({
            "symbol": symbol, "side": action, "volume": volume,
            "sl": scan.get("stop_loss"), "tp": scan.get("take_profit"),
            "confidence": scan.get("confidence", 0), "reason": scan.get("reason", ""), "source": "autotrade-sim",
        }),
    ))

    autotrade_log.extend(results)
    del autotrade_log[:-50]
    placed = [r["account"] for r in results if r.get("status") == "trade_placed"]
    autotrade_status.update({"state": "idle", "note": f"Done — placed on: {', '.join(placed) if placed else 'none (hold on both)'}"})


@app.get("/api/ai-conversation")
async def get_ai_conversation():
    return list(reversed(ai_conversation[-15:]))


@app.get("/api/autotrade-status")
async def get_autotrade_status():
    return autotrade_status


@app.post("/api/autotrade-run")
async def autotrade_run(background_tasks: BackgroundTasks):
    """Manual trigger for the 'Run Scan Now' button on the dashboard — runs the same
    dual-account cycle (Gemini -> real, Groq -> sim) as the cron-triggered endpoints."""
    if autotrade_status.get("state") == "analyzing":
        return {"status": "already_running", "note": "A scan is already in progress"}
    background_tasks.add_task(_run_autotrade_cycle)
    return {"status": "started"}


@app.head("/api/autotrade-sim")
async def autotrade_sim_head(secret: str = Query(...)):
    """Uptime monitors (e.g. UptimeRobot) send HEAD requests before/instead of GET —
    this just validates without triggering a scan, so monitoring doesn't 405."""
    if not AUTOTRADE_SECRET or secret != AUTOTRADE_SECRET:
        raise HTTPException(403, "Invalid secret")
    return Response(status_code=200)


@app.get("/api/autotrade-sim")
async def autotrade_sim(secret: str = Query(...), background_tasks: BackgroundTasks = None):
    """Kept for backward compatibility with any cron already pointed at this URL —
    now triggers the SAME dual-account cycle as /api/autotrade (Gemini -> real,
    Groq -> sim), so you don't need two cron jobs pointed at two different endpoints."""
    if not AUTOTRADE_SECRET or secret != AUTOTRADE_SECRET:
        raise HTTPException(403, "Invalid secret")
    background_tasks.add_task(_run_autotrade_cycle)
    return {"status": "started", "note": "Scan running in background — check the AI Auto-Trade Log on your dashboard in ~2-3 min"}


# ---------------- Autotrade (AI-driven, triggered by a free external cron) ----------------

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.1-8b-instant")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
AUTOTRADE_SECRET = os.getenv("AUTOTRADE_SECRET", "")
MT_LOGIN = os.getenv("MT_LOGIN", "")
MT_PASSWORD = os.getenv("MT_PASSWORD", "")
MT_SERVER = os.getenv("MT_SERVER", "")
MT_PLATFORM = os.getenv("MT_PLATFORM", "mt5")
TRADE_VOLUME_DEFAULT = float(os.getenv("TRADE_VOLUME", "0.01"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
STACK_MIN_CONFIDENCE = int(os.getenv("STACK_MIN_CONFIDENCE", "80"))  # bar to add another trade while one's already open

# Mutable at runtime via the chat panel — starts from env var defaults.
# NOTE: resets to these defaults on every redeploy/restart (in-memory only).
settings = {
    "symbol": os.getenv("TRADE_SYMBOL", "XAUUSD"),
    "interval": "15min",
    "volume": TRADE_VOLUME_DEFAULT,
    "risk_notes": "",  # free-text risk preferences the AI should respect
    "lot_sizes": {  # user's preferred base lot size per asset class (Settings page) — different
        "gold": float(os.getenv("LOT_GOLD", "0.02")),  # prop firms have different leverage, so the
        "btc": float(os.getenv("LOT_BTC", "0.10")),    # Auto Trader uses these instead of one tiny
        "forex": float(os.getenv("LOT_FOREX", "0.01")),  # fixed size for every symbol.
    },
}


def _smart_volume(symbol: str, confidence: float, entry_price=None, sl=None) -> float:
    """Combines the user's saved per-asset lot size, confidence scaling (1x-3x),
    and a hard risk cap so a position's stop-loss hit never exceeds MAX_LOSS_USD —
    so good high-confidence trades size up instead of always making a few dollars,
    while risk stays bounded."""
    cls = _asset_class(symbol)
    base = _base_lot_for_symbol(symbol)
    scaled = _confidence_volume(base, confidence)

    if entry_price is not None and sl:
        risk_per_lot = abs(_calc_pnl_dollars(symbol, "buy", entry_price, sl, 1.0))
        if risk_per_lot > 0:
            max_lot_by_risk = MAX_LOSS_USD / risk_per_lot
            scaled = min(scaled, max_lot_by_risk)

    return round(max(scaled, MIN_LOT[cls]), 2)

WATCHLIST = ["XAUUSD", "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD"]
SCAN_TIMEFRAMES = ["15min", "1h", "4h"]  # 30min dropped — it was fetched but never reached the prompt
MIN_CONFIDENCE = int(os.getenv("MIN_CONFIDENCE", "70"))
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-flash-latest")  # override via env var if needed


def _clean_json_text(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
    return text.strip()


async def _call_gemini_raw(prompt: str, timeout=90) -> str:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}
    body = {"contents": [{"parts": [{"text": prompt}]}]}
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, headers=headers, json=body)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini failed: {r.text[:300]}")
    try:
        return r.json()["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected Gemini response: {r.text[:300]}")


async def _call_groq_raw(prompt: str, timeout=90) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set")
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    body = {"model": GROQ_MODEL, "messages": [{"role": "user", "content": prompt}], "temperature": 0.3}
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, headers=headers, json=body)
    if r.status_code != 200:
        raise RuntimeError(f"Groq failed: {r.text[:300]}")
    try:
        return r.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected Groq response: {r.text[:300]}")


async def _call_llm_text(prompt: str, timeout=90) -> str:
    """Single-turn call for JSON-schema prompts (scan/strategy/manage). Tries Gemini
    first, falls back to Groq automatically if Gemini is down/quota-blocked."""
    errors = []
    for name, fn in (("Gemini", _call_gemini_raw), ("Groq", _call_groq_raw)):
        try:
            return await fn(prompt, timeout=timeout)
        except Exception as e:
            errors.append(f"{name}: {e}")
    raise HTTPException(502, "All LLM providers failed: " + " | ".join(errors))


async def _call_gemini_chat_raw(system: str, history: list, message: str, timeout=60) -> str:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set")
    contents = [{"role": h["role"], "parts": [{"text": h["text"]}]} for h in history]
    contents.append({"role": "user", "parts": [{"text": message}]})
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}
    body = {"system_instruction": {"parts": [{"text": system}]}, "contents": contents}
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, headers=headers, json=body)
    if r.status_code != 200:
        raise RuntimeError(f"Gemini failed: {r.text[:300]}")
    try:
        return r.json()["candidates"][0]["content"]["parts"][0]["text"]
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected Gemini response: {r.text[:300]}")


async def _call_groq_chat_raw(system: str, history: list, message: str, timeout=60) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set")
    messages = [{"role": "system", "content": system}]
    for h in history:
        messages.append({"role": "assistant" if h["role"] == "model" else "user", "content": h["text"]})
    messages.append({"role": "user", "content": message})
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    body = {"model": GROQ_MODEL, "messages": messages, "temperature": 0.4}
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, headers=headers, json=body)
    if r.status_code != 200:
        raise RuntimeError(f"Groq failed: {r.text[:300]}")
    try:
        return r.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected Groq response: {r.text[:300]}")


async def _call_deepseek_chat_raw(system: str, history: list, message: str, timeout=60) -> str:
    if not DEEPSEEK_API_KEY:
        raise RuntimeError("DEEPSEEK_API_KEY not set")
    messages = [{"role": "system", "content": system}]
    for h in history:
        messages.append({"role": "assistant" if h["role"] == "model" else "user", "content": h["text"]})
    messages.append({"role": "user", "content": message})
    url = "https://api.deepseek.com/chat/completions"
    headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}
    body = {"model": DEEPSEEK_MODEL, "messages": messages, "temperature": 0.4}
    async with httpx.AsyncClient(timeout=timeout) as client:
        r = await client.post(url, headers=headers, json=body)
    if r.status_code != 200:
        raise RuntimeError(f"DeepSeek failed: {r.text[:300]}")
    try:
        return r.json()["choices"][0]["message"]["content"]
    except (KeyError, IndexError):
        raise RuntimeError(f"Unexpected DeepSeek response: {r.text[:300]}")


async def _call_llm_chat(system: str, history: list, message: str, timeout=60) -> str:
    """Multi-turn call for the chat endpoint. Tries Gemini first, falls back to Groq."""
    errors = []
    for name, fn in (("Gemini", _call_gemini_chat_raw), ("Groq", _call_groq_chat_raw)):
        try:
            return await fn(system, history, message, timeout=timeout)
        except Exception as e:
            errors.append(f"{name}: {e}")
    raise HTTPException(502, "All LLM providers failed: " + " | ".join(errors))


# Latest DeepSeek second-opinion — set after each autotrade scan, readable by the chat AI
# so the trading AI and chat AI's combined findings get a third perspective.
deepseek_review = {"time": None, "text": None}

# Live status of the current scan cycle — polled by the frontend to show a "thinking" indicator.
autotrade_status = {"state": "idle", "since": None, "note": None}

PRICE_STALENESS_PCT = float(os.getenv("PRICE_STALENESS_PCT", "0.15"))  # abort trade if price moved >0.15% since scan

DEEPSEEK_VERDICT_PROMPT = """You are the second seat on a two-AI trading council. The lead AI just
finished a scan cycle and wants to act on this decision:

{context}

Give your independent verdict. Respond with ONLY raw JSON (no markdown), in exactly this shape:
{{"agree": true|false, "note": "one short sentence explaining your verdict"}}
Disagree only if you see a real, specific flaw (bad risk:reward, conflicting structure, over-extension).
Default to agree if the setup looks reasonable — you are a check, not a competing strategy."""


async def _ask_deepseek_verdict(context: str) -> dict | None:
    """Structured agree/disagree verdict used to gate trade execution. Returns None (no veto)
    if DeepSeek is unavailable — the council falls back to the lead AI's decision alone."""
    if not DEEPSEEK_API_KEY:
        return None
    try:
        url = "https://api.deepseek.com/chat/completions"
        headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}
        body = {
            "model": DEEPSEEK_MODEL,
            "messages": [{"role": "user", "content": DEEPSEEK_VERDICT_PROMPT.format(context=context)}],
            "temperature": 0.2,
        }
        async with httpx.AsyncClient(timeout=45) as client:
            r = await client.post(url, headers=headers, json=body)
        if r.status_code != 200:
            return None
        text = _clean_json_text(r.json()["choices"][0]["message"]["content"])
        return json.loads(text)
    except Exception:
        return None

DEEPSEEK_REVIEW_PROMPT = """You are a second-opinion trading reviewer. The primary trading AI just
finished a scan cycle. Here is what it found and decided:

{context}

In 2-3 sentences, give your own independent take: do you agree with the decision, and is there
anything the primary AI may have missed or gotten wrong? Be direct and concise — no markdown, no
JSON, just plain text commentary."""


async def _ask_deepseek_opinion(context: str) -> str | None:
    """Best-effort — never raises, never blocks the autotrade cycle if DeepSeek is unavailable."""
    if not DEEPSEEK_API_KEY:
        return None
    try:
        url = "https://api.deepseek.com/chat/completions"
        headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}
        body = {
            "model": DEEPSEEK_MODEL,
            "messages": [{"role": "user", "content": DEEPSEEK_REVIEW_PROMPT.format(context=context)}],
            "temperature": 0.3,
        }
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(url, headers=headers, json=body)
        if r.status_code != 200:
            return None
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception:
        return None

SCAN_PROMPT = """You are a disciplined ICT / Smart Money Concepts trader scanning multiple pairs
to find the single best trade opportunity right now.

For each pair below you're given, per timeframe: "c" = candles oldest-first as [open, high, low, close]
arrays (most recent {candles_per_tf} only), plus sma20, sma50 (simple moving averages) and rsi14
(14-period RSI) already calculated for you — use these alongside your own reading of the candles
rather than re-deriving them.

For EACH pair, analyze for: liquidity sweeps, break of structure (BOS), change of character (CHoCH),
fair value gaps (FVG), order blocks, and notable candle patterns (doji, engulfing, pin bars).
Use the 4h timeframe to establish higher-timeframe directional bias, the 1h timeframe to confirm
structure within that bias, and the 15min timeframe to judge entry timing. Only take a 15min/1h
entry that agrees with the 4h bias — don't fight the higher timeframe.
Give each pair a confidence score 0-100 for a trade opportunity RIGHT NOW (0 = no setup, 100 = extremely clear).

Trader's risk management preferences (respect these strictly): {risk_notes}

Today's news context (factor this in — avoid new trades right before/during major high-impact
releases unless the setup is exceptionally clear): {news_context}

Respond with ONLY raw JSON (no markdown, no code fences), in exactly this shape:
{{
  "scanned": {{"XAUUSD": {{"action": "buy"|"sell"|"hold", "confidence": number, "note": "one short phrase"}}, ... one entry per pair ...}},
  "best_symbol": string | null,
  "action": "buy" | "sell" | "hold",
  "confidence": number,
  "stop_loss": number | null,
  "take_profit": number | null,
  "reason": "one short sentence explaining the pick"
}}

best_symbol/action should be the single highest-confidence non-hold setup across ALL pairs, only if its
confidence is at least {min_confidence}. Otherwise action must be "hold" and best_symbol null.
stop_loss/take_profit must be realistic absolute prices for best_symbol, consistent with its recent price levels.

Data:
{pairs_json}
"""


def _sma(closes: list, period: int):
    if len(closes) < period:
        return None
    return round(sum(closes[-period:]) / period, 5)


def _rsi(closes: list, period: int = 14):
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def _with_indicators(candles: list):
    closes = [c["close"] for c in candles]
    return {
        "candles": candles,
        "sma20": _sma(closes, 20),
        "sma50": _sma(closes, 50),
        "rsi14": _rsi(closes, 14),
    }


_candle_cache: Dict[str, tuple] = {}  # "symbol:interval" -> (timestamp, candles)
CANDLE_CACHE_TTL = {"15min": 300, "30min": 600, "1h": 1200, "4h": 3600}


async def _fetch_candles_cached(symbol: str, interval: str, outputsize: int = 30):
    key = f"{symbol}:{interval}:{outputsize}"
    now = datetime.now(timezone.utc).timestamp()
    cached = _candle_cache.get(key)
    ttl = CANDLE_CACHE_TTL.get(interval, 300)
    if cached and now - cached[0] < ttl:
        return cached[1]
    candles = await _fetch_candles(symbol, interval, outputsize)
    _candle_cache[key] = (now, candles)
    return candles


async def _fetch_multi_snapshot():
    """Fetch 15min/30min/1h/4h candles for every pair in the watchlist.
    Cached per timeframe (longer timeframes cached longer, since they barely
    change minute to minute) and throttled to respect Twelve Data's free-tier
    limit of 8 requests/min — real API calls are spaced 8s apart; cache hits
    don't count against that at all."""
    pairs_data = {}
    errors = []
    for symbol in WATCHLIST:
        pairs_data[symbol] = {}
        for tf in SCAN_TIMEFRAMES:
            key = f"{symbol}:{tf}:30"
            was_cached = key in _candle_cache and (datetime.now(timezone.utc).timestamp() - _candle_cache[key][0]) < CANDLE_CACHE_TTL.get(tf, 300)
            try:
                candles = await _fetch_candles_cached(symbol, tf, outputsize=30)
                pairs_data[symbol][tf] = _with_indicators(candles) if candles else {"candles": [], "sma20": None, "sma50": None, "rsi14": None}
            except Exception as e:
                errors.append(f"{symbol} {tf}: {str(e)}")
                pairs_data[symbol][tf] = {"candles": [], "sma20": None, "sma50": None, "rsi14": None}
            if not was_cached:
                await asyncio.sleep(8)  # stay under 8 req/min for real calls only
    if errors:
        pairs_data["_fetch_errors"] = errors
    return pairs_data


# Live back-and-forth between Gemini and Groq during each scan — polled by the frontend
# to show a small live conversation bar.
ai_conversation = []


def _log_conversation(speaker: str, text: str):
    ai_conversation.append({"time": datetime.now(timezone.utc).isoformat(), "speaker": speaker, "text": text[:400]})
    del ai_conversation[:-40]


PROMPT_TIMEFRAMES = ["15min", "1h", "4h"]  # all three timeframes the prompt actually asks the AI to use


def _compact_for_prompt(pairs_data: dict, candles_per_tf: int = 12) -> dict:
    """The full snapshot (6 pairs x 4 timeframes x 30 candles) is ~700+ candle objects —
    way past free-tier TPM limits (Groq's 8B model caps at 6000 TPM, shared across ALL
    calls in that same minute — not just this one). This trims it to just what the LLM
    needs: only the 2 timeframes it's instructed to actually use, fewer recent candles,
    compact [o,h,l,c] arrays instead of repeated keys, and rounded prices."""
    compact = {}
    for symbol, timeframes in pairs_data.items():
        if symbol == "_fetch_errors":
            continue
        compact[symbol] = {}
        for tf, tf_data in timeframes.items():
            if tf not in PROMPT_TIMEFRAMES:
                continue
            candles = tf_data.get("candles", [])[-candles_per_tf:]
            compact[symbol][tf] = {
                "c": [[round(c["open"], 5), round(c["high"], 5), round(c["low"], 5), round(c["close"], 5)] for c in candles],
                "sma20": round(tf_data["sma20"], 5) if tf_data.get("sma20") is not None else None,
                "sma50": round(tf_data["sma50"], 5) if tf_data.get("sma50") is not None else None,
                "rsi14": round(tf_data["rsi14"], 1) if tf_data.get("rsi14") is not None else None,
            }
    return compact


async def _ask_gemini_scan(pairs_data: dict, news_context: str = "not checked this run") -> dict:
    """Gemini and Groq each independently analyze the same data, then a trade only
    fires if they agree — otherwise it's held as a split decision. Their reasoning
    is logged as a live conversation for the dashboard."""
    if not GEMINI_API_KEY and not GROQ_API_KEY:
        raise HTTPException(500, "Neither GEMINI_API_KEY nor GROQ_API_KEY is set on the server")

    candles_per_tf = 15
    prompt = SCAN_PROMPT.format(
        risk_notes=settings["risk_notes"] or "No specific preferences stated — use conservative default risk management.",
        min_confidence=MIN_CONFIDENCE,
        news_context=news_context,
        candles_per_tf=candles_per_tf,
        pairs_json=json.dumps(_compact_for_prompt(pairs_data, candles_per_tf)),
    )

    async def _try(name, fn):
        try:
            text = _clean_json_text(await fn(prompt, timeout=90))
            return name, json.loads(text), None
        except Exception as e:
            return name, None, str(e)[:200]

    tasks = [_try("Groq", _call_groq_raw)]
    if _gemini_scan_budget_available():
        _bump_gemini_scan_count()
        tasks.append(_try("Gemini", _call_gemini_raw))
    else:
        _log_conversation("Gemini", f"Sitting this one out — daily auto-scan budget ({GEMINI_DAILY_SCAN_BUDGET}) spent, saving quota for chat.")

    outcomes = await asyncio.gather(*tasks)
    results = {name: data for name, data, _ in outcomes}
    errors = {name: err for name, _, err in outcomes if err}

    for name, data in results.items():
        if data:
            action = data.get("action", "hold")
            if action in ("buy", "sell"):
                _log_conversation(name, f"I like {action.upper()} {data.get('best_symbol')} at {data.get('confidence', 0)}% — {data.get('reason', '')[:200]}")
            else:
                _log_conversation(name, f"Nothing worth taking right now. {data.get('reason', '')[:200]}")
        else:
            _log_conversation(name, f"(failed: {errors.get(name, 'unknown error')})")

    gemini_d, groq_d = results.get("Gemini"), results.get("Groq")

    if gemini_d and not groq_d:
        _log_conversation("Groq", f"Didn't get a response from me ({errors.get('Groq', 'unknown error')}) — going with Gemini's read alone.")
        return gemini_d
    if groq_d and not gemini_d:
        _log_conversation("Gemini", f"Didn't get a response from me ({errors.get('Gemini', 'unknown error')}) — going with Groq's read alone.")
        return groq_d
    if not gemini_d and not groq_d:
        raise HTTPException(502, f"Both Gemini and Groq failed — Gemini: {errors.get('Gemini')} | Groq: {errors.get('Groq')}")

    same_call = (
        gemini_d.get("action") == groq_d.get("action")
        and gemini_d.get("best_symbol") == groq_d.get("best_symbol")
        and gemini_d.get("action") in ("buy", "sell")
    )
    if same_call:
        merged = dict(gemini_d)
        merged["confidence"] = min(gemini_d.get("confidence", 0), groq_d.get("confidence", 0))
        merged["reason"] = f"Gemini: {gemini_d.get('reason', '')} | Groq agrees: {groq_d.get('reason', '')}"
        merged["scanned"] = {**gemini_d.get("scanned", {}), **groq_d.get("scanned", {})}
        _log_conversation("Council", f"We agree — {merged['action'].upper()} {merged['best_symbol']} at {merged['confidence']}% combined confidence.")
        return merged

    _log_conversation("Council", "We don't agree — holding this cycle.")
    return {
        "action": "hold", "best_symbol": None, "confidence": 0,
        "reason": f"Gemini and Groq disagreed (Gemini: {gemini_d.get('action')} {gemini_d.get('best_symbol')}, Groq: {groq_d.get('action')} {groq_d.get('best_symbol')})",
        "scanned": {**gemini_d.get("scanned", {}), **groq_d.get("scanned", {})},
    }


STRATEGY_PROMPT = """You are a disciplined ICT / Smart Money Concepts forex and gold trader.
You will be given the most recent {n} candles for {symbol} on the {interval} timeframe,
oldest first, as JSON: [{{time, open, high, low, close}}, ...].

Analyze the candles for: liquidity sweeps, break of structure (BOS), change of character (CHoCH),
fair value gaps (FVG), and order blocks. Only recommend a trade when there is a clear, high-probability
setup. Most of the time the correct answer is "hold" — do not force a trade.

Trader's risk management preferences (respect these strictly): {risk_notes}

Respond with ONLY raw JSON (no markdown, no code fences, no extra text), in exactly this shape:
{{"action": "buy" | "sell" | "hold", "stop_loss": number | null, "take_profit": number | null, "reason": "one short sentence"}}

stop_loss and take_profit must be realistic absolute prices for {symbol}, consistent with recent price levels.
If action is "hold", stop_loss and take_profit must be null.
Candles:
{candles_json}
"""


async def _ask_gemini(symbol: str, interval: str, candles: list) -> dict:
    if not GEMINI_API_KEY and not GROQ_API_KEY:
        raise HTTPException(500, "Neither GEMINI_API_KEY nor GROQ_API_KEY is set on the server")

    prompt = STRATEGY_PROMPT.format(
        n=len(candles),
        symbol=symbol,
        interval=interval,
        risk_notes=settings["risk_notes"] or "No specific preferences stated — use conservative default risk management.",
        candles_json=json.dumps(candles),
    )

    text = await _call_llm_text(prompt, timeout=60)
    text = _clean_json_text(text)

    try:
        decision = json.loads(text)
    except json.JSONDecodeError:
        raise HTTPException(502, f"LLM did not return valid JSON: {text[:300]}")

    return decision


autotrade_log = []  # in-memory log of recent AI decisions, newest last


@app.get("/api/scan-test")
async def scan_test(secret: str = Query(...)):
    """Test the AI's multi-pair scan WITHOUT connecting to MetaApi or placing any
    trade — safe to use right now while your MetaApi account is blocked on billing."""
    if not AUTOTRADE_SECRET or secret != AUTOTRADE_SECRET:
        raise HTTPException(403, "Invalid secret")

    pairs_data = await _fetch_multi_snapshot()
    scan = await _ask_gemini_scan(pairs_data)
    return scan


def _slim_response(entry: dict):
    """cron-job.org rejects large response bodies — send it just a small
    confirmation, while the FULL detail still lives in autotrade_log for the
    dashboard's AI Auto-Trade Log panel."""
    decision = entry.get("decision") or {}
    return {
        "status": entry.get("status"),
        "time": entry.get("time"),
        "symbol": decision.get("best_symbol"),
        "action": decision.get("action"),
        "confidence": decision.get("confidence"),
        "reason": entry.get("reason") or decision.get("reason"),
    }


async def _run_autotrade():
    entry = {"time": datetime.now(timezone.utc).isoformat()}
    entry["market"] = _market_status()  # logged only — not enforced yet, per Icon's instruction

    run_number_today = _bump_daily_run_count()
    if run_number_today <= 3:
        entry["news_check"] = await _fetch_forex_news()

    try:
        account_id, conn = await _connect_account(MT_LOGIN, MT_PASSWORD, MT_SERVER, MT_PLATFORM)

        closed = await _manage_real_positions()
        if closed:
            entry["closed"] = closed

        positions = await conn.get_positions()
        if len(positions) >= MAX_OPEN_POSITIONS:
            entry.update({"status": "skipped", "reason": f"{len(positions)} open position(s), max is {MAX_OPEN_POSITIONS}"})
            autotrade_log.append(entry)
            del autotrade_log[:-50]
            return

        pairs_data = await _fetch_multi_snapshot()
        if pairs_data.get("_fetch_errors"):
            entry["fetch_errors"] = pairs_data["_fetch_errors"]
        scan = await _ask_gemini_scan(pairs_data, entry.get("news_check", "not checked this run"))

        entry["scanned"] = scan.get("scanned", {})
        action = scan.get("action", "hold")
        best_symbol = scan.get("best_symbol")
        confidence = scan.get("confidence", 0)
        min_confidence = _current_min_confidence()

        if action not in ("buy", "sell") or not best_symbol or confidence < min_confidence:
            entry.update({"status": "hold", "decision": scan})
            autotrade_log.append(entry)
            del autotrade_log[:-50]
            return

        entry_price_est = None
        try:
            price_info = await conn.get_symbol_price(best_symbol)
            entry_price_est = price_info.get("ask") if action == "buy" else price_info.get("bid")
        except Exception:
            pass
        volume = _smart_volume(best_symbol, confidence, entry_price=entry_price_est, sl=scan.get("stop_loss"))
        volume = _enforce_min_lot(best_symbol, round(volume * _current_lot_scale(), 2))

        result = await _place_trade(
            conn, best_symbol, action, volume,
            sl=scan.get("stop_loss"), tp=scan.get("take_profit"),
            confidence=confidence, reason=scan.get("reason", ""), source="autotrade", account="real",
        )
        entry.update({"status": "trade_placed", "decision": scan, "result": str(result)})
        autotrade_log.append(entry)
        del autotrade_log[:-50]

    except Exception as e:
        entry.update({"status": "error", "reason": str(e)})
        autotrade_log.append(entry)
        del autotrade_log[:-50]


@app.get("/api/autotrade")
async def autotrade(secret: str = Query(...), background_tasks: BackgroundTasks = None):
    """Hit this URL from a free external cron every hour. Returns immediately;
    the actual scan (which can take a couple minutes due to free-tier rate
    limits) runs in the background — check the AI Auto-Trade Log for results."""
    if not AUTOTRADE_SECRET or secret != AUTOTRADE_SECRET:
        raise HTTPException(403, "Invalid secret")
    if not all([MT_LOGIN, MT_PASSWORD, MT_SERVER]):
        raise HTTPException(500, "MT_LOGIN, MT_PASSWORD, MT_SERVER env vars must be set for autotrade")
    background_tasks.add_task(_run_autotrade)
    return {"status": "started", "note": "Scan running in background — check the AI Auto-Trade Log on your dashboard in ~2-3 min"}


@app.get("/api/autotrade/log")
async def get_autotrade_log():
    return list(reversed(autotrade_log))


@app.delete("/api/autotrade/log")
async def clear_autotrade_log():
    autotrade_log.clear()
    return {"status": "cleared"}


GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "")  # e.g. "yourname/tradeweb"
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
STATE_FILE_PATH = "autotrade_state.json"


async def _github_get_state():
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return None, None
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{STATE_FILE_PATH}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers=headers, params={"ref": GITHUB_BRANCH})
    if r.status_code == 200:
        data = r.json()
        content = base64.b64decode(data["content"]).decode()
        return json.loads(content), data["sha"]
    return None, None


async def _github_save_state():
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return
    _, sha = await _github_get_state()
    payload = {"settings": settings, "chat_history": chat_history[-40:], "sim_account": sim_account, "trade_log": trade_log[-200:]}
    content_b64 = base64.b64encode(json.dumps(payload, indent=2).encode()).decode()
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{STATE_FILE_PATH}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
    body = {"message": "Update autotrade state", "content": content_b64, "branch": GITHUB_BRANCH}
    if sha:
        body["sha"] = sha
    async with httpx.AsyncClient() as client:
        r = await client.put(url, headers=headers, json=body)
    if r.status_code not in (200, 201):
        # Most common cause: two saves racing (stale sha -> 409/422). Log loudly instead
        # of losing sync silently — Railway logs will show this even without a UI for it.
        print(f"[github_save_state] FAILED ({r.status_code}): {r.text[:300]}")
    return r.status_code in (200, 201)


chat_history = []  # list of {role, text} — persisted to GitHub


AUTOTRADE_INTERVAL_SECONDS = int(os.getenv("AUTOTRADE_INTERVAL_SECONDS", "900"))  # 15 min
HEARTBEAT_INTERVAL_SECONDS = int(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "600"))  # 10 min


async def _autotrade_scheduler_loop():
    while True:
        await asyncio.sleep(_current_scan_interval())
        try:
            await _manage_real_positions()
        except Exception as e:
            print(f"[scheduler] real position management failed: {e}")
        try:
            await _run_autotrade_sim()
        except Exception as e:
            print(f"[scheduler] autotrade cycle failed: {e}")


async def _heartbeat_loop():
    while True:
        print(f"[heartbeat] app alive at {datetime.now(timezone.utc).isoformat()}")
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)


@app.on_event("startup")
async def _start_background_loops():
    asyncio.create_task(_autotrade_scheduler_loop())
    asyncio.create_task(_heartbeat_loop())


@app.on_event("startup")
async def _load_state_on_startup():
    state, _ = await _github_get_state()
    if state:
        saved_settings = state.get("settings", {})
        # merge lot_sizes rather than clobber, so new asset classes get their default
        if saved_settings.get("lot_sizes"):
            settings["lot_sizes"].update(saved_settings["lot_sizes"])
            saved_settings = {k: v for k, v in saved_settings.items() if k != "lot_sizes"}
        settings.update(saved_settings)
        chat_history.extend(state.get("chat_history", []))
        saved_sim = state.get("sim_account")
        if saved_sim:
            sim_account["balance"] = saved_sim.get("balance", sim_account["balance"])
            sim_account["positions"] = saved_sim.get("positions", [])
        saved_trades = state.get("trade_log")
        if saved_trades:
            trade_log.extend(saved_trades)


@app.get("/api/chat/history")
async def get_chat_history():
    return chat_history


@app.get("/api/settings")
async def get_settings():
    return settings


@app.post("/api/settings")
async def update_settings(payload: dict = Body(...)):
    """Settings page: save preferred lot size per asset class (Gold/BTC/Forex)
    and risk notes. The Auto Trader reads these via _base_lot_for_symbol/_smart_volume
    instead of always using one tiny fixed lot."""
    lot_sizes = payload.get("lot_sizes")
    if lot_sizes:
        sample_symbol = {"gold": "XAUUSD", "btc": "BTCUSD", "forex": "EURUSD"}
        for key in ("gold", "btc", "forex"):
            if lot_sizes.get(key) is not None:
                settings["lot_sizes"][key] = _enforce_min_lot(sample_symbol[key], lot_sizes[key])
    if payload.get("risk_notes") is not None:
        settings["risk_notes"] = payload["risk_notes"]
    if payload.get("volume") is not None:
        settings["volume"] = float(payload["volume"])
    await _github_save_state()
    return settings


@app.get("/api/trades")
async def get_trades():
    """Unified trade log — every trade taken (manual, chat, or auto), real or
    demo, with entry/SL/TP/lot size/risk-reward/expected & actual P/L/confidence/reason."""
    return list(reversed(trade_log))


@app.delete("/api/trades")
async def clear_trades():
    trade_log.clear()
    return {"status": "cleared"}


@app.get("/api/trade-preview")
async def trade_preview(symbol: str, side: str, volume: float, sl: float = None, tp: float = None):
    """Called before placing a trade so the UI can show real dollar amounts
    (not just pips) for potential profit/loss and risk:reward."""
    symbol = symbol.upper()
    volume = _enforce_min_lot(symbol, volume)
    price = await _get_price(symbol)
    expected_profit = _calc_pnl_dollars(symbol, side, price, tp, volume) if tp else None
    expected_loss = _calc_pnl_dollars(symbol, side, price, sl, volume) if sl else None
    return {
        "symbol": symbol,
        "entry_estimate": price,
        "volume": volume,
        "expected_profit": round(expected_profit, 2) if expected_profit is not None else None,
        "expected_loss": round(expected_loss, 2) if expected_loss is not None else None,
        "risk_reward": _risk_reward(price, sl, tp) if sl and tp else None,
    }


CHAT_SYSTEM_PROMPT = """You are the trading assistant embedded in Icon's trading dashboard.
You can chat normally about markets, strategy, and risk management.

The auto-trader scans this ENTIRE watchlist every cycle, not just one pair: {watchlist}
It checks these timeframes on each pair: {scan_timeframes}
A trade only fires if a pair scores {min_confidence}+ confidence out of 100.
Max loss per position: ${max_loss}, profit target: ${profit_target}.

You also directly control: symbol={symbol}, timeframe={interval}, lot size={volume}, risk notes="{risk_notes}"
Saved per-asset lot sizes (Settings page — the Auto Trader sizes from these, not a single fixed lot):
Gold {lot_gold}, BTC/crypto {lot_btc}, Forex {lot_forex}
(these are separate manual-trade defaults, distinct from the full watchlist scan above)

You can place a trade immediately if the user explicitly asks (e.g. "buy gold now").
Only trigger a trade on a clear, explicit instruction — never on your own initiative.

IMPORTANT — you and the Auto-Trading AI share ONE unified trade log (below, under "Recent trades").
It contains every trade either of you has taken, real or demo, with entry/SL/TP/lot size/risk-reward/
expected P/L/actual P/L/confidence/reason. Always check it before answering questions like "did you buy
gold" or "what happened to my trade" — if a trade is listed there, it WAS placed; never claim no trade
was placed just because you personally didn't place it in this chat turn.

LIVE DATA (use this — do not guess or use outdated training knowledge):
{live_data}

DeepSeek's independent second opinion on the last scan cycle (if the user asks what DeepSeek
thinks, or wants a second opinion, relay this — do not claim you have no access to it):
{deepseek_review}

If the user asks to change the pair, timeframe, lot size, or states a risk management
preference, update it via settings_update. Timeframe must be one of: 1min, 5min, 15min, 1h, 4h, 1day.
Symbol should be a 6-letter forex/metal pair like XAUUSD, EURUSD, GBPUSD (no slash).

Respond with ONLY raw JSON (no markdown, no code fences), in exactly this shape:
{{"reply": "your conversational reply to show the user",
  "settings_update": {{"symbol": string|null, "interval": string|null, "volume": number|null, "risk_notes": string|null}},
  "trade_action": {{"side": "buy"|"sell", "symbol": string, "volume": number, "stop_loss": number|null, "take_profit": number|null}} | null,
  "close_action": {{"symbol": string|null, "all": true|false}} | null}}

trade_action must be null unless the user just explicitly asked you to place a NEW trade right now.
If they didn't mention a symbol/volume for the trade, use the current settings' symbol/volume.

close_action is for CLOSING existing open positions (e.g. "close all trades", "close my gold position",
"close everything"). NEVER use trade_action with an opposite side to "close" a position — that opens a
brand new position instead of closing the existing one, and doubles the user's exposure. Use close_action
instead: set "all": true to close every open position, or "symbol" to close only positions on that symbol.
trade_action and close_action are mutually exclusive — only one may be non-null per response.
"""


async def _live_data_snapshot():
    parts = []
    try:
        candles = await _fetch_candles(settings["symbol"], interval=settings["interval"], outputsize=20)
        if candles:
            last = candles[-1]
            first = candles[0]
            change = last["close"] - first["close"]
            direction = "up" if change > 0 else "down" if change < 0 else "flat"
            parts.append(
                f"{settings['symbol']} last price: {last['close']}, "
                f"{direction} {abs(change):.2f} over last {len(candles)} {settings['interval']} candles "
                f"(recent high {max(c['high'] for c in candles)}, low {min(c['low'] for c in candles)})"
            )
    except Exception:
        parts.append(f"(couldn't fetch live price for {settings['symbol']} right now)")

    if all([MT_LOGIN, MT_PASSWORD, MT_SERVER]):
        try:
            _, conn = await _connect_account(MT_LOGIN, MT_PASSWORD, MT_SERVER, MT_PLATFORM)
            info = await conn.get_account_information()
            positions = await conn.get_positions()
            parts.append(f"Account balance: {info.get('balance')} {info.get('currency')}, equity: {info.get('equity')}")
            if positions:
                pos_desc = ", ".join(f"{p['symbol']} {p['type']} {p['volume']} lots (P/L {p['profit']})" for p in positions)
                parts.append(f"Open positions: {pos_desc}")
            else:
                parts.append("Open positions: none")
        except Exception:
            parts.append("(couldn't fetch account balance right now)")

    if autotrade_log:
        last_entry = autotrade_log[-1]
        scanned = last_entry.get("scanned") or (last_entry.get("decision") or {}).get("scanned")
        if scanned:
            per_pair = "; ".join(f"{sym}: {info.get('action')} ({info.get('confidence')}% conf) — {info.get('note','')}" for sym, info in scanned.items())
            parts.append(f"Last full scan ({last_entry.get('time')}): {per_pair}")
        else:
            parts.append(f"Last auto-trade check: {last_entry.get('status')} — {last_entry.get('reason') or (last_entry.get('decision') or {}).get('reason', '')}")

    if trade_log:
        recent = trade_log[-6:]
        lines = []
        for t in recent:
            pnl_str = f", actual P/L ${t['actual_pnl']}" if t.get("actual_pnl") is not None else ""
            exp_str = f", expected profit ${t['expected_profit']}/loss ${t['expected_loss']}" if t.get("expected_profit") is not None or t.get("expected_loss") is not None else ""
            rr_str = f", R:R 1:{t['risk_reward']}" if t.get("risk_reward") is not None else ""
            conf_str = f", {t['confidence']}% confidence" if t.get("confidence") is not None else ""
            lines.append(
                f"[{t['status'].upper()}] {t['time']}: {t['side'].upper()} {t['volume']} lots {t['symbol']} @ {t['entry']} "
                f"(SL {t.get('sl')}, TP {t.get('tp')}{rr_str}{exp_str}{conf_str}) via {t['source']}/{t['account']}{pnl_str} — {t.get('reason','')}"
            )
        parts.append("Recent trades (unified trade log, shared with Auto-Trading AI):\n" + "\n".join(lines))

    return "\n".join(parts)


@app.post("/api/chat")
async def chat(payload: dict = Body(...)):
    if not GEMINI_API_KEY and not GROQ_API_KEY:
        raise HTTPException(500, "Neither GEMINI_API_KEY nor GROQ_API_KEY is set on the server")

    message = payload.get("message", "")
    if not message:
        raise HTTPException(400, "message is required")

    # Talk to a specific council AI directly — either via the dropdown (payload["ai"])
    # or by typing "@deepseek ..." at the start of the message (kept for compatibility).
    addressed_ai = payload.get("ai") if payload.get("ai") in ("groq", "deepseek", "gemini") else None
    stripped = message.strip()
    if not addressed_ai:
        for name in ("groq", "deepseek", "gemini"):
            if stripped.lower().startswith(f"@{name}"):
                addressed_ai = name
                message = stripped[len(name) + 1:].strip(" :,-") or "What's your take?"
                break

    live_data = await _live_data_snapshot()

    if deepseek_review["text"]:
        deepseek_review_str = f"[{deepseek_review['time']}] {deepseek_review['text']}"
    else:
        deepseek_review_str = "No DeepSeek review yet (set DEEPSEEK_API_KEY, or wait for the next autotrade scan)."

    system = CHAT_SYSTEM_PROMPT.format(
        symbol=settings["symbol"],
        interval=settings["interval"],
        volume=settings["volume"],
        risk_notes=settings["risk_notes"] or "none set",
        live_data=live_data,
        deepseek_review=deepseek_review_str,
        watchlist=", ".join(WATCHLIST),
        scan_timeframes=", ".join(SCAN_TIMEFRAMES),
        min_confidence=MIN_CONFIDENCE,
        max_loss=MAX_LOSS_USD,
        profit_target=PROFIT_TARGET_USD,
        lot_gold=settings["lot_sizes"]["gold"],
        lot_btc=settings["lot_sizes"]["btc"],
        lot_forex=settings["lot_sizes"]["forex"],
    )

    history = [{"role": h["role"], "text": h["text"]} for h in chat_history[-20:]]

    if addressed_ai:
        advisory_note = ("\n\nNOTE: the user is addressing you directly and specifically as a council "
                          "member for a second opinion. Respond conversationally only — your reply cannot "
                          "place or close trades in this mode, even if the schema below allows it, so leave "
                          "trade_action and close_action null no matter what.")
        chat_fn = {"groq": _call_groq_chat_raw, "deepseek": _call_deepseek_chat_raw, "gemini": _call_gemini_chat_raw}[addressed_ai]
        try:
            text = await chat_fn(system + advisory_note, history, message, timeout=60)
        except Exception as e:
            raise HTTPException(502, f"{addressed_ai.title()} call failed: {e}")
    else:
        text = await _call_llm_chat(system, history, message, timeout=60)
    text = _clean_json_text(text)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        chat_history.append({"role": "user", "text": message})
        chat_history.append({"role": "model", "text": text})
        await _github_save_state()
        return {"reply": text, "settings": settings}

    update = parsed.get("settings_update") or {}
    if update.get("symbol"):
        settings["symbol"] = update["symbol"].upper().replace("/", "")
    if update.get("interval"):
        settings["interval"] = update["interval"]
    if update.get("volume"):
        settings["volume"] = float(update["volume"])
    if update.get("risk_notes"):
        settings["risk_notes"] = update["risk_notes"]

    reply = parsed.get("reply", "")
    close_action = parsed.get("close_action") if not addressed_ai else None

    if close_action and (close_action.get("all") or close_action.get("symbol")):
        target_symbol = (close_action.get("symbol") or "").upper().replace("/", "") or None
        closed = []

        for pos in list(sim_account["positions"]):
            if target_symbol and pos["symbol"].upper() != target_symbol:
                continue
            result = await sim_close(pos["id"])
            closed.append(f"{pos['symbol']} ({result.get('pnl')} P/L, demo)")

        real_account_available = all([MT_LOGIN, MT_PASSWORD, MT_SERVER])
        if real_account_available:
            try:
                _, conn = await _connect_account(MT_LOGIN, MT_PASSWORD, MT_SERVER, MT_PLATFORM)
                real_positions = await conn.get_positions()
                for pos in real_positions:
                    if target_symbol and pos.get("symbol", "").upper() != target_symbol:
                        continue
                    await conn.close_position(pos["id"])
                    _log_trade_close(pos["id"], (pos or {}).get("profit"))
                    closed.append(f"{pos.get('symbol')} (real)")
            except Exception:
                pass

        if closed:
            reply += "\n\n✅ Closed: " + ", ".join(closed)
        else:
            reply += "\n\nNo matching open positions found to close."

    trade_action = parsed.get("trade_action") if not addressed_ai else None

    if close_action:
        trade_action = None  # never fake a close via an opposite trade_action

    if trade_action and trade_action.get("side") in ("buy", "sell"):
        trade_symbol = trade_action.get("symbol") or settings["symbol"]
        trade_volume = _enforce_min_lot(trade_symbol, trade_action.get("volume") or _base_lot_for_symbol(trade_symbol))

        real_account_available = all([MT_LOGIN, MT_PASSWORD, MT_SERVER])
        placed_on = None

        if real_account_available:
            try:
                _, conn = await _connect_account(MT_LOGIN, MT_PASSWORD, MT_SERVER, MT_PLATFORM)
                result = await _place_trade(
                    conn, trade_symbol, trade_action["side"], trade_volume,
                    sl=trade_action.get("stop_loss"), tp=trade_action.get("take_profit"),
                    reason="Manual command via chat", source="chat", account="real",
                )
                placed_on = "real"
            except Exception as e:
                result = None
                real_error = str(e)

        if placed_on != "real":
            # fall back to the built-in demo account so trade commands still work
            # while the real MetaApi account is blocked/unavailable
            result = await sim_trade({
                "symbol": trade_symbol, "side": trade_action["side"], "volume": trade_volume,
                "sl": trade_action.get("stop_loss"), "tp": trade_action.get("take_profit"),
                "reason": "Manual command via chat", "source": "chat",
            })
            placed_on = "demo"

        label = "on your REAL account" if placed_on == "real" else "on your DEMO account (real account unavailable right now)"
        reply += f"\n\n✅ Placed {trade_action['side'].upper()} {trade_volume} lots on {trade_symbol} {label}."
        autotrade_log.append({
            "time": datetime.now(timezone.utc).isoformat(),
            "status": "trade_placed",
            "decision": {"action": trade_action["side"], "reason": f"Manual command via chat ({placed_on})"},
            "result": str(result),
        })
        del autotrade_log[:-50]

    chat_history.append({"role": "user", "text": message})
    chat_history.append({"role": "model", "text": reply})
    await _github_save_state()

    return {"reply": reply, "settings": settings}


# serve the frontend
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return FileResponse("static/index.html")
