"""
Vercel Serverless FastAPI Backend for NSE Live Terminal (NSEKE)
Automatically handles /api/symbols and /api/history on Vercel.
"""

import json
import random
import string
import time
from datetime import datetime, timezone

import requests
import websocket
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

app = FastAPI(title="NSEKE Cloud TradingView Scraper")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TV_SCANNER_URL = "https://scanner.tradingview.com/kenya/scan"
TV_WS_URL = "wss://data.tradingview.com/socket.io/websocket?from=chart%2FIInGcnS5%2F"
CACHE = {"symbols": [], "symbols_ts": 0.0, "history": {}}


def encode_frame(payload) -> str:
    raw = payload if isinstance(payload, str) else json.dumps(payload, separators=(",", ":"))
    return f"~m~{len(raw)}~m~{raw}"


def parse_frames(raw: str):
    out, i, n = [], 0, len(raw)
    while i < n and raw[i:i + 3] == "~m~":
        end_len = raw.find("~m~", i + 3)
        if end_len == -1:
            break
        length = int(raw[i + 3:end_len])
        start = end_len + 3
        out.append(raw[start:start + length])
        i = start + length
    return out


@app.get("/api/symbols")
def get_symbols(force: int = 0):
    if not force and CACHE["symbols"] and time.time() - CACHE["symbols_ts"] < 15:
        return {"ok": True, "count": len(CACHE["symbols"]), "symbols": CACHE["symbols"]}

    payload = {
        "filter": [{"left": "exchange", "operation": "equal", "right": "NSEKE"}],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": [
            "name", "description", "close", "change", "change_abs",
            "open", "high", "low", "volume", "Perf.W", "Perf.1M",
            "Perf.3M", "Perf.Y", "price_52_week_high", "price_52_week_low",
            "RSI", "SMA20", "EMA50", "sector"
        ],
        "sort": {"sortBy": "volume", "sortOrder": "desc"},
        "range": [0, 250]
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/",
        "Content-Type": "application/json"
    }
    try:
        resp = requests.post(TV_SCANNER_URL, json=payload, headers=headers, timeout=8)
        resp.raise_for_status()
    except Exception as exc:
        return JSONResponse(status_code=502, content={"ok": False, "error": f"TradingView scanner error: {exc}"})

    stocks = []
    for row in resp.json().get("data", []):
        d = row.get("d", [])
        if len(d) < 9 or not d[0] or not d[2]:
            continue
        close = float(d[2])
        chg = float(d[4] or 0)
        stocks.append({
            "ticker": str(d[0]).upper(),
            "name": d[1] or d[0],
            "price": round(close, 2),
            "changePct": round(float(d[3] or 0), 2),
            "change": round(chg, 2),
            "prevClose": round(close - chg, 2),
            "open": round(float(d[5] or close), 2),
            "high": round(float(d[6] or close), 2),
            "low": round(float(d[7] or close), 2),
            "volume": int(round(float(d[8] or 0))),
            "perfW": round(float(d[9]), 2) if d[9] is not None else None,
            "perf1M": round(float(d[10]), 2) if d[10] is not None else None,
            "perf3M": round(float(d[11]), 2) if d[11] is not None else None,
            "perf1Y": round(float(d[12]), 2) if d[12] is not None else None,
            "high52": round(float(d[13]), 2) if d[13] is not None else None,
            "low52": round(float(d[14]), 2) if d[14] is not None else None,
            "rsi": round(float(d[15]), 1) if d[15] is not None else None,
            "sma20": round(float(d[16]), 2) if d[16] is not None else None,
            "ema50": round(float(d[17]), 2) if d[17] is not None else None,
            "sector": d[18] or "NSEKE"
        })

    if stocks:
        CACHE["symbols"] = stocks
        CACHE["symbols_ts"] = time.time()
    return {"ok": True, "count": len(stocks), "symbols": stocks}


@app.get("/api/history")
def get_history(symbol: str = Query("SCOM"), tf: str = Query("1D"), bars: int = Query(500)):
    ticker = symbol.upper().replace("NSEKE:", "")
    key = f"{ticker}_{tf}"
    cached = CACHE["history"].get(key)
    if cached and time.time() - cached["ts"] < 20:
        return {"ok": True, "symbol": ticker, "tf": tf, "bars": cached["bars"]}

    tf_map = {"1m": "1", "5m": "5", "15m": "15", "1H": "60", "1D": "D", "1W": "W", "1M": "M"}
    res = tf_map.get(tf, "D")
    session = "cs_" + "".join(random.choices(string.ascii_lowercase + string.digits, k=12))

    try:
        ws = websocket.create_connection(
            TV_WS_URL,
            origin="https://www.tradingview.com",
            header=["User-Agent: Mozilla/5.0"],
            timeout=7
        )
    except Exception as exc:
        return JSONResponse(status_code=502, content={"ok": False, "error": f"WebSocket connection failed: {exc}"})

    out_bars = []
    try:
        ws.send(encode_frame({"m": "set_auth_token", "p": ["unauthorized_user_token"]}))
        ws.send(encode_frame({"m": "chart_create_session", "p": [session, ""]}))
        ws.send(encode_frame({
            "m": "resolve_symbol",
            "p": [session, "sds_sym_1", f'={{"adjustment":"splits","session":"regular","symbol":"NSEKE:{ticker}"}}']
        }))
        ws.send(encode_frame({
            "m": "create_series",
            "p": [session, "sds_1", "s1", "sds_sym_1", res, int(bars), ""]
        }))

        deadline = time.time() + 6.5
        while time.time() < deadline:
            raw = ws.recv()
            for frame in parse_frames(raw or ""):
                if frame.startswith("~h~"):
                    ws.send(encode_frame(frame))
                elif frame.startswith("{"):
                    pkt = json.loads(frame)
                    if pkt.get("m") in ("timescale_update", "du"):
                        series = pkt.get("p", [None, {}])[1].get("sds_1", {}).get("s", [])
                        if series:
                            for item in series:
                                v = item.get("v", [])
                                if len(v) < 5:
                                    continue
                                dt = datetime.fromtimestamp(v[0], tz=timezone.utc)
                                d_str = dt.strftime("%Y-%m-%d %H:%M") if res in ("1", "5", "15", "60") else dt.strftime("%Y-%m-%d")
                                out_bars.append({
                                    "date": d_str,
                                    "open": round(float(v[1]), 2),
                                    "high": round(float(v[2]), 2),
                                    "low": round(float(v[3]), 2),
                                    "close": round(float(v[4]), 2),
                                    "volume": int(round(float(v[5] if len(v) > 5 else 0)))
                                })
                            break
            if out_bars:
                break
    finally:
        ws.close()

    if out_bars:
        CACHE["history"][key] = {"bars": out_bars, "ts": time.time()}
        return {"ok": True, "symbol": ticker, "tf": tf, "bars": out_bars}
    return JSONResponse(status_code=502, content={"ok": False, "error": "No bars returned from TradingView"})