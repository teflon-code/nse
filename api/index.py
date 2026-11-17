"""
Vercel Serverless FastAPI Backend for NSE Live Terminal (NSEKE)
Automatically handles /api/symbols and /api/history on Vercel.
"""

import json
import sys
import os
import random
import string
import time
from datetime import datetime, timezone

import requests
import websocket
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from page import PAGE_HTML  # noqa: E402
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

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


@app.get("/", response_class=HTMLResponse)
def index_page():
    return HTMLResponse(PAGE_HTML)


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

# ---------------------------------------------------------------------------
# Research + AI layer
# The Groq key is read from the GROQ_API_KEY environment variable on the server
# only. It is never sent to the browser or written to the repo.
# ---------------------------------------------------------------------------
import os
import re
import html
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from fastapi import Request

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
SCAN_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Origin": "https://www.tradingview.com",
    "Referer": "https://www.tradingview.com/",
    "Content-Type": "application/json",
}
FUND_COLUMNS = [
    "market_cap_basic", "price_earnings_ttm", "earnings_per_share_basic_ttm",
    "dividend_yield_recent", "price_book_fq", "return_on_equity", "debt_to_equity",
]
FUND_KEYS = ["marketCap", "pe", "eps", "divYield", "pb", "roe", "debtEquity"]
RESEARCH_CACHE = {"fund": {}, "news": {}, "ai": {}, "scan": None}
AI_SYSTEM = (
    "You are an equity research assistant for the Nairobi Securities Exchange (NSE, Kenya). "
    "Use only the numbers and headlines provided. If something is missing, say it is missing. "
    "Do not invent prices, financials, or news. Give balanced views: strengths, risks, and what "
    "would change the view. Plain text with short bold-free headings. Under 300 words. "
    "End with one line saying this is not financial advice."
)


def _fund_data(ticker):
    hit = RESEARCH_CACHE["fund"].get(ticker)
    if hit and time.time() - hit["ts"] < 3600:
        return hit["data"]
    payload = {
        "options": {"lang": "en"},
        "symbols": {"tickers": [f"NSEKE:{ticker}"], "query": {"types": []}},
        "columns": FUND_COLUMNS,
        "range": [0, 1],
    }
    resp = requests.post(TV_SCANNER_URL, json=payload, headers=SCAN_HEADERS, timeout=8)
    resp.raise_for_status()
    rows = resp.json().get("data", [])
    if not rows:
        raise LookupError("No fundamentals for this ticker")
    vals = rows[0].get("d", [])
    data = {}
    for k, v in zip(FUND_KEYS, vals):
        data[k] = round(float(v), 2) if isinstance(v, (int, float)) else None
    RESEARCH_CACHE["fund"][ticker] = {"data": data, "ts": time.time()}
    return data


def _extract_text(raw):
    t = re.sub(r"(?is)<(script|style|noscript|svg|nav|footer|header|form)[^>]*>.*?</\1>", " ", raw)
    t = re.sub(r"(?s)<[^>]+>", " ", t)
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def _scrape_article(url):
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (compatible; NSETerminal/1.0)"},
                         timeout=6, allow_redirects=True)
        if r.status_code != 200 or "html" not in r.headers.get("content-type", ""):
            return ""
        meta = re.search(r'(?is)<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)', r.text[:60000])
        body = _extract_text(r.text[:400000])
        sentences = re.findall(r"[A-Z][^.!?]{50,}[.!?]", body)
        text = " ".join(sentences[:12])[:1500]
        if meta:
            text = html.unescape(meta.group(1))[:300] + " " + text
        return text.strip()
    except Exception:
        return ""


def _news_data(ticker):
    hit = RESEARCH_CACHE["news"].get(ticker)
    if hit and time.time() - hit["ts"] < 900:
        return hit["items"]
    q = urllib.parse.quote(f"{ticker} NSE Kenya")
    url = f"https://news.google.com/rss/search?q={q}&hl=en-KE&gl=KE&ceid=KE:en"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)
    items = [
        {
            "title": (i.findtext("title") or "").strip(),
            "link": (i.findtext("link") or "").strip(),
            "published": (i.findtext("pubDate") or "").strip(),
            "source": (i.findtext("source") or "").strip(),
        }
        for i in root.iter("item")
    ][:8]
    with ThreadPoolExecutor(max_workers=6) as ex:
        bodies = list(ex.map(_scrape_article, [i["link"] for i in items]))
    for it, body in zip(items, bodies):
        it["excerpt"] = body
    RESEARCH_CACHE["news"][ticker] = {"items": items, "ts": time.time()}
    return items


GROQ_FALLBACKS = ["llama-3.3-70b-versatile", "llama-3.1-8b-instant"]


def _groq(messages, max_tokens=600):
    key = os.environ.get("GROQ_API_KEY", "").strip()
    if not key:
        raise RuntimeError("GROQ_API_KEY is not set on the server")
    models = []
    for m in [os.environ.get("GROQ_MODEL", "").strip()] + GROQ_FALLBACKS:
        if m and m not in models:
            models.append(m)
    last_err = None
    for model in models:
        resp = requests.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model": model, "messages": messages, "temperature": 0.2, "max_tokens": max_tokens},
            timeout=25,
        )
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"].strip()
        detail = resp.text[:300]
        if resp.status_code == 401:
            raise RuntimeError("Groq rejected the API key (401). Check GROQ_API_KEY on Vercel.")
        last_err = f"Groq {resp.status_code} for model '{model}': {detail}"
        if resp.status_code not in (400, 404):
            break
    raise RuntimeError(last_err or "Groq request failed")


def _quote_snapshot(ticker):
    for s in CACHE.get("symbols", []):
        if s.get("ticker") == ticker:
            return s
    return None


@app.get("/api/fundamentals")
def get_fundamentals(symbol: str = Query("SCOM")):
    t = symbol.upper().replace("NSEKE:", "")
    try:
        return {"ok": True, "symbol": t, "data": _fund_data(t)}
    except LookupError as exc:
        return JSONResponse(status_code=404, content={"ok": False, "error": str(exc)})
    except Exception as exc:
        return JSONResponse(status_code=502, content={"ok": False, "error": f"Fundamentals unavailable: {exc}"})


@app.get("/api/news")
def get_news(symbol: str = Query("SCOM")):
    t = symbol.upper().replace("NSEKE:", "")
    try:
        return {"ok": True, "symbol": t, "items": _news_data(t)}
    except Exception as exc:
        return JSONResponse(status_code=502, content={"ok": False, "error": f"News unavailable: {exc}"})


@app.get("/api/ai/analyze")
def ai_analyze(symbol: str = Query("SCOM"), force: int = 0):
    t = symbol.upper().replace("NSEKE:", "")
    hit = RESEARCH_CACHE["ai"].get(t)
    if hit and not force and time.time() - hit["ts"] < 600:
        return {"ok": True, "symbol": t, "analysis": hit["text"], "cached": True}

    quote = _quote_snapshot(t) or {}
    try:
        fund = _fund_data(t)
    except Exception:
        fund = None
    try:
        news = _news_data(t)
    except Exception:
        news = []

    prompt = (
        f"Ticker: {t} ({quote.get('name', 'unknown name')}), sector: {quote.get('sector', 'unknown')}\n"
        f"Quote and technicals: {json.dumps({k: quote.get(k) for k in ['price','changePct','change','open','high','low','volume','perfW','perf1M','perf3M','perf1Y','high52','low52','rsi','sma20','ema50']})}\n"
        f"Fundamentals (TradingView): {json.dumps(fund) if fund else 'not available'}\n"
        f"Recent headlines: {json.dumps([n['title'] + ' (' + n['source'] + ', ' + n['published'] + ')' for n in news]) if news else 'none found'}\n\n"
        "Write: 1) what the numbers say about valuation and momentum, 2) the main news themes, "
        "3) the case for and against buying, 4) what to watch next."
    )
    try:
        text = _groq([{"role": "system", "content": AI_SYSTEM}, {"role": "user", "content": prompt}])
    except RuntimeError as exc:
        return JSONResponse(status_code=503, content={"ok": False, "error": str(exc)})
    except Exception as exc:
        return JSONResponse(status_code=502, content={"ok": False, "error": f"AI request failed: {exc}"})

    RESEARCH_CACHE["ai"][t] = {"text": text, "ts": time.time()}
    return {"ok": True, "symbol": t, "analysis": text, "cached": False}


@app.get("/api/ai/scan")
def ai_scan(force: int = 0):
    hit = RESEARCH_CACHE["scan"]
    if hit and not force and time.time() - hit["ts"] < 300:
        return hit["result"]

    if not CACHE.get("symbols"):
        res = get_symbols()
        if isinstance(res, dict):
            CACHE["symbols"] = res.get("symbols", [])
    candidates = []
    for s in CACHE.get("symbols", []):
        flags = []
        rsi = s.get("rsi")
        if rsi is not None and rsi < 35:
            flags.append(f"RSI {rsi} (oversold)")
        if rsi is not None and rsi > 70:
            flags.append(f"RSI {rsi} (overbought, risk)")
        if s.get("perf1M") is not None and s["perf1M"] > 8:
            flags.append(f"Up {s['perf1M']}% in a month")
        if s.get("perf1M") is not None and s["perf1M"] < -8:
            flags.append(f"Down {s['perf1M']}% in a month")
        if s.get("low52") and s.get("price") and s["price"] <= s["low52"] * 1.05:
            flags.append("Within 5% of 52-week low")
        if s.get("sma20") and s.get("ema50") and s.get("price") and s["price"] > s["sma20"] and s["price"] > s["ema50"]:
            flags.append("Above SMA 20 and EMA 50")
        if flags:
            candidates.append({
                "ticker": s["ticker"], "name": s.get("name"), "price": s.get("price"),
                "changePct": s.get("changePct"), "rsi": rsi, "flags": flags,
            })
    candidates.sort(key=lambda c: -len(c["flags"]))
    candidates = candidates[:12]

    commentary, note = None, None
    if candidates:
        prompt = (
            "These NSE stocks triggered technical screens today (price, change %, RSI, flags):\n"
            + json.dumps(candidates) +
            "\nRank the 3 most interesting for a closer look, say why in one sentence each, "
            "name the main risk, and say which ones only look interesting because of a single flag. "
            "Use only these numbers. Under 220 words. End with: not financial advice."
        )
        try:
            commentary = _groq([{"role": "system", "content": AI_SYSTEM}, {"role": "user", "content": prompt}], max_tokens=400)
        except RuntimeError as exc:
            note = str(exc)
        except Exception as exc:
            note = f"AI commentary unavailable: {exc}"

    result = {"ok": True, "candidates": candidates, "commentary": commentary, "note": note}
    RESEARCH_CACHE["scan"] = {"result": result, "ts": time.time()}
    return result


def _parse_json_loose(text):
    cleaned = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    try:
        return json.loads(cleaned)
    except Exception:
        m = re.search(r"\{.*\}", cleaned, re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                pass
    return None


@app.get("/api/ai/synthesize")
def ai_synthesize(symbol: str = Query("SCOM"), force: int = 0):
    t = symbol.upper().replace("NSEKE:", "")
    hit = RESEARCH_CACHE.setdefault("synth", {}).get(t)
    if hit and not force and time.time() - hit["ts"] < 900:
        return {**hit["payload"], "cached": True}

    quote = _quote_snapshot(t) or {}
    try:
        fund = _fund_data(t)
    except Exception:
        fund = None
    try:
        news = _news_data(t)
    except Exception:
        news = []
    articles = [
        {"title": n["title"], "source": n["source"], "published": n["published"],
         "excerpt": n.get("excerpt", "")}
        for n in news
    ]
    prompt = (
        f"Ticker: {t} ({quote.get('name', 'unknown')}), sector {quote.get('sector', 'unknown')}.\n"
        f"Quote and technicals: {json.dumps({k: quote.get(k) for k in ['price','changePct','perf1M','perf1Y','high52','low52','rsi','sma20','ema50']})}\n"
        f"Fundamentals: {json.dumps(fund) if fund else 'not available'}\n"
        f"News articles (title, source, date, excerpt where we could read the page):\n{json.dumps(articles)[:9000]}\n\n"
        "Synthesize what the news means for this stock. Return ONLY a JSON object with keys: "
        '"sentiment" (one of "bullish", "neutral", "bearish"), '
        '"confidence" ("low", "medium" or "high", low if few or thin articles), '
        '"summary" (2 to 3 sentences on what is actually happening), '
        '"themes" (array of up to 4 short strings), '
        '"catalysts" (array of up to 3 upcoming or possible events), '
        '"risks" (array of up to 3), '
        '"watch" (array of up to 3 concrete things to monitor). '
        "Base every point on the articles or numbers above. If there are no articles, say so in the summary and rely on the numbers only."
    )
    try:
        raw = _groq([{"role": "system", "content": AI_SYSTEM}, {"role": "user", "content": prompt}], max_tokens=700)
    except RuntimeError as exc:
        return JSONResponse(status_code=503, content={"ok": False, "error": str(exc)})
    except Exception as exc:
        return JSONResponse(status_code=502, content={"ok": False, "error": f"AI request failed: {exc}"})

    result = _parse_json_loose(raw) or {"sentiment": "neutral", "confidence": "low", "summary": raw,
                                        "themes": [], "catalysts": [], "risks": [], "watch": []}
    payload = {
        "ok": True, "symbol": t, "result": result,
        "articles": [{"title": a["title"], "source": a["source"], "published": a["published"]} for a in articles],
        "articlesRead": sum(1 for a in articles if a["excerpt"]),
    }
    RESEARCH_CACHE["synth"][t] = {"payload": payload, "ts": time.time()}
    return {**payload, "cached": False}


@app.post("/api/ai/chat")
async def ai_chat(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"ok": False, "error": "Invalid JSON"})
    msgs = body.get("messages", [])[-12:]
    ctx = body.get("context", {})
    clean = [{"role": m.get("role"), "content": str(m.get("content", ""))[:2000]}
             for m in msgs if m.get("role") in ("user", "assistant")]
    if not clean or clean[-1]["role"] != "user":
        return JSONResponse(status_code=400, content={"ok": False, "error": "Send a user message"})
    system = (
        "You are the built-in assistant of the NSE Terminal, a research and paper-trading tool for the "
        "Nairobi Securities Exchange. Answer the user's question directly. Use the context below for the "
        "current ticker, quote, fundamentals, news synthesis and the user's paper positions. Do not claim "
        "trades were placed in a real account; all trading here is simulated. Never invent figures. "
        "Plain text, short paragraphs, no headings. Not financial advice.\n\nCONTEXT:\n"
        + json.dumps(ctx)[:6000]
    )
    try:
        reply = _groq([{"role": "system", "content": system}] + clean, max_tokens=500)
    except RuntimeError as exc:
        return JSONResponse(status_code=503, content={"ok": False, "error": str(exc)})
    except Exception as exc:
        return JSONResponse(status_code=502, content={"ok": False, "error": f"AI request failed: {exc}"})
    return {"ok": True, "reply": reply}
