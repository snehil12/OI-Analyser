"""OI Pulse clone - backend.

Everything REST/Kotak related lives here. The browser only talks to /api/*.
Run:  uvicorn backend.main:app --port 8000   (from the oipulse folder)
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import gzip
import json
import math
import os
import random
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parent.parent
FRONTEND = ROOT / "frontend"
CONFIG_DIR = Path(os.environ.get("OIPULSE_HOME", Path.home() / ".oipulse"))
CONFIG_FILE = CONFIG_DIR / "config.json"
HISTORY_DB = ROOT / "logs" / "oipulse_history.sqlite3"
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))

INDEX_STEP = {"NIFTY": 50, "BANKNIFTY": 100, "FINNIFTY": 50, "MIDCPNIFTY": 25, "SENSEX": 100, "BANKEX": 100}
INDEX_SPOT = {"NIFTY": 24850, "BANKNIFTY": 53600, "FINNIFTY": 26100, "MIDCPNIFTY": 12900, "SENSEX": 81500, "BANKEX": 62000}
BSE_UNDERLYINGS = {"SENSEX", "BANKEX"}
CACHE_TTL = 8          # seconds: protects Kotak rate limits when many tabs poll
SNAP_MIN_GAP = 20      # seconds between stored history snapshots
CHAIN_SNAP_MIN_GAP = 60  # persist option-chain history once per minute


# --------------------------------------------------------------------------- state
class State:
    client: Any = None
    consumer_key: str = ""
    demo: bool = True
    remembered: bool = False
    last_error: str = ""
    cache: dict[tuple, tuple[float, dict]] = {}
    baseline: dict[tuple, dict] = {}        # (und, expiry) -> {"date":..., "strikes": {strike: {...}}}
    history: dict[tuple, list[dict]] = {}   # (und, expiry) -> [{t, pcr, spot, ce_oi, pe_oi}]
    chain_snapshots: dict[tuple, float] = {}


S = State()


@contextlib.contextmanager
def history_connection():
    HISTORY_DB.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(HISTORY_DB, timeout=5)
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS option_chain_snapshots (
            id INTEGER PRIMARY KEY, ts REAL NOT NULL, underlying TEXT NOT NULL,
            expiry TEXT NOT NULL, mode TEXT NOT NULL, spot REAL NOT NULL, chain_gzip BLOB NOT NULL
        )""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_chain_history ON option_chain_snapshots(underlying, expiry, ts)")
        con.execute("""CREATE TABLE IF NOT EXISTS futures_snapshots (
            id INTEGER PRIMARY KEY, ts REAL NOT NULL, neosymbol TEXT NOT NULL,
            trading_symbol TEXT NOT NULL, price REAL NOT NULL, oi REAL NOT NULL
        )""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_futures_history ON futures_snapshots(neosymbol, ts)")
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def save_chain_snapshot(underlying: str, expiry: str, mode: str, spot: float, chain: list[dict]) -> None:
    key = (underlying, expiry)
    now = time.time()
    if now - S.chain_snapshots.get(key, 0) < CHAIN_SNAP_MIN_GAP:
        return
    payload = gzip.compress(json.dumps(chain, separators=(",", ":")).encode("utf-8"))
    with history_connection() as con:
        previous = con.execute(
            "SELECT ts FROM option_chain_snapshots WHERE underlying=? AND expiry=? ORDER BY ts DESC LIMIT 1",
            key,
        ).fetchone()
        if previous and now - previous[0] < CHAIN_SNAP_MIN_GAP:
            S.chain_snapshots[key] = previous[0]
            return
        con.execute(
            "INSERT INTO option_chain_snapshots(ts,underlying,expiry,mode,spot,chain_gzip) VALUES(?,?,?,?,?,?)",
            (now, underlying, expiry, mode, spot, payload),
        )
        con.execute("DELETE FROM option_chain_snapshots WHERE ts < ?", (now - 30 * 86400,))
    S.chain_snapshots[key] = now


def save_futures_snapshot(neosymbol: str, trading_symbol: str, price: float, oi: float) -> None:
    now = time.time()
    with history_connection() as con:
        previous = con.execute(
            "SELECT ts FROM futures_snapshots WHERE neosymbol=? ORDER BY ts DESC LIMIT 1", (neosymbol,)
        ).fetchone()
        if previous and now - previous[0] < 50:
            return
        con.execute(
            "INSERT INTO futures_snapshots(ts,neosymbol,trading_symbol,price,oi) VALUES(?,?,?,?,?)",
            (now, neosymbol, trading_symbol, price, oi),
        )
        con.execute("DELETE FROM futures_snapshots WHERE ts < ?", (now - 30 * 86400,))


FUTURE_WINDOWS = (("5 min", 300), ("15 min", 900), ("30 min", 1800),
                  ("1 hour", 3600), ("4 hours", 14400), ("1 day", 86400))


def classify_future(price_change: float, oi_change: float) -> str:
    if oi_change > 0 and price_change > 0:
        return "Long Buildup"
    if oi_change > 0 and price_change < 0:
        return "Short Buildup"
    if oi_change < 0 and price_change > 0:
        return "Short Covering"
    if oi_change < 0 and price_change < 0:
        return "Long Unwinding"
    return "Neutral"


def futures_signals_from_history(neosymbol: str, price: float, oi: float, now: float | None = None) -> tuple[list[dict], int]:
    now = now or time.time()
    with history_connection() as con:
        history = con.execute(
            "SELECT ts,price,oi FROM futures_snapshots WHERE neosymbol=? AND ts>=? ORDER BY ts",
            (neosymbol, now - 86400 - 120),
        ).fetchall()
    signals = []
    for label, seconds in FUTURE_WINDOWS:
        cutoff = now - seconds
        baseline = next((row for row in history if row[0] <= cutoff), None)
        if baseline is None:
            signals.append({"timeframe": label, "status": "collecting", "signal": "Collecting data"})
            continue
        price_change = price - baseline[1]
        oi_change = oi - baseline[2]
        signals.append({
            "timeframe": label, "status": "ready", "signal": classify_future(price_change, oi_change),
            "price_change": price_change, "oi_change": oi_change,
            "price_change_pct": price_change / baseline[1] * 100 if baseline[1] else 0,
            "oi_change_pct": oi_change / baseline[2] * 100 if baseline[2] else 0,
            "from": baseline[0], "to": now,
        })
    return signals, len(history)


def records_from_scrip_response(response: Any) -> list[dict]:
    if isinstance(response, list):
        return [r for r in response if isinstance(r, dict)]
    if isinstance(response, dict):
        return next((v for v in response.values() if isinstance(v, list)), [])
    return []


def expiry_from_future_record(record: dict, exchange_segment: str) -> str | None:
    raw = record.get("lExpiryDate") or record.get("pExpiryDate")
    try:
        value = float(raw)
        if value > 0:
            if exchange_segment == "nse_fo":
                value += 315511200  # Kotak's NSE FO scrip-master epoch offset
            return dt.datetime.fromtimestamp(value, IST).date().isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        pass
    if isinstance(raw, str):
        try:
            return dt.date.fromisoformat(raw[:10]).isoformat()
        except ValueError:
            return None
    return None


def exchange_for(und: str) -> str:
    return "bse_fo" if und.upper() in BSE_UNDERLYINGS else "nse_fo"


def today_ist() -> str:
    return dt.datetime.now(IST).strftime("%Y-%m-%d")


# --------------------------------------------------------------------------- config
def save_key(key: str) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps({"consumer_key": key}))
    try:
        os.chmod(CONFIG_FILE, 0o600)
    except OSError:
        pass


def clear_key() -> None:
    try:
        CONFIG_FILE.unlink()
    except FileNotFoundError:
        pass


def make_client(key: str):
    from neo_api_client import NeoAPI  # imported lazily so demo mode works without the SDK

    return NeoAPI(consumer_key=key, environment="prod")


def err_text(resp: Any) -> str | None:
    """Return an error message if a Kotak response looks like an error, else None."""
    if isinstance(resp, dict):
        if "Error" in resp:
            return f"{resp.get('Error')} (HTTP {resp.get('StatusCode')}): {str(resp.get('ResponseText',''))[:200]}"
        e = resp.get("error") or resp.get("Error")
        if e:
            if isinstance(e, list) and e and isinstance(e[0], dict):
                return str(e[0].get("message") or e[0])
            return str(e)
        if str(resp.get("stat", "")).lower() == "not_ok":
            return str(resp.get("emsg") or resp)
    return None


async def connect_with_key(key: str, remember: bool) -> None:
    def _verify():
        c = make_client(key)
        r = c.expiries(exchange="nse_fo", underlying="NIFTY", instrument_type="option")
        return c, r

    try:
        client, resp = await asyncio.to_thread(_verify)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Could not reach Kotak Neo: {exc}")
    msg = err_text(resp)
    if msg:
        raise HTTPException(400, f"Kotak rejected the key: {msg}")
    S.client, S.consumer_key, S.demo, S.last_error = client, key, False, ""
    S.cache.clear()
    if remember:
        save_key(key)
        S.remembered = True


# --------------------------------------------------------------------------- tolerant parsing
STRIKE_KEYS = ("strike_price", "strikeprice", "strike", "stkprc", "strkprc", "strk", "sp")
OI_KEYS = ("oi", "open_interest", "openinterest", "openint", "open_int")
OICHG_KEYS = ("oi_change", "oichange", "change_in_oi", "changeinoi", "chng_in_oi", "oi_chg", "oichg", "oi_chng", "chg_oi")
PREVOI_KEYS = ("prev_oi", "previous_oi", "prevoi", "prev_open_interest", "previous_open_interest")
LTP_KEYS = ("ltp", "last_price", "lastprice", "last_traded_price", "lp", "price")
LTPCHG_KEYS = ("net_change", "netchange", "change", "price_change", "ltp_change", "chg", "ltpchg", "pchange")
VOL_KEYS = ("volume", "vol", "total_volume", "totalvolume", "v", "last_volume", "ltq_volume", "total_traded_volume")
IV_KEYS = ("iv", "implied_volatility", "impliedvolatility")
SPOT_KEYS = ("underlying_value", "underlyingvalue", "underlying_price", "underlying_ltp", "underlying_spot_price",
             "spot", "spot_price", "index_ltp", "underlying_last_price")
CE_NAMES = ("ce", "call", "calls", "call_options", "calloptions", "call_option", "calloption", "c")
PE_NAMES = ("pe", "put", "puts", "put_options", "putoptions", "put_option", "putoption", "p")


def _lk(d: dict) -> dict:
    return {str(k).lower(): v for k, v in d.items()}


def pick(d: dict, keys: tuple, default=None):
    low = _lk(d)
    for k in keys:
        if k in low and low[k] not in (None, "", "-"):
            return low[k]
    return default


def num(x, default=0.0) -> float:
    try:
        if x is None or x == "" or x == "-":
            return default
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return default


def find_rows(obj: Any, depth: int = 0):
    """Locate the list of dict rows that carry a strike key anywhere in the JSON."""
    if depth > 6:
        return None
    if isinstance(obj, list):
        dicts = [x for x in obj if isinstance(x, dict)]
        if dicts and any(pick(x, STRIKE_KEYS) is not None for x in dicts[:5]):
            return dicts
        for x in obj:
            r = find_rows(x, depth + 1)
            if r:
                return r
    elif isinstance(obj, dict):
        for v in obj.values():
            r = find_rows(v, depth + 1)
            if r:
                return r
    return None


def find_spot(obj: Any, depth: int = 0) -> float | None:
    if depth > 4 or not isinstance(obj, (dict, list)):
        return None
    if isinstance(obj, dict):
        v = pick(obj, SPOT_KEYS)
        if v is not None and num(v) > 0:
            return num(v)
        for val in obj.values():
            if isinstance(val, (dict, list)):
                r = find_spot(val, depth + 1)
                if r:
                    return r
    else:
        for x in obj[:3]:
            r = find_spot(x, depth + 1)
            if r:
                return r
    return None


def side_from_dict(d: dict) -> dict:
    return {
        "oi": num(pick(d, OI_KEYS)),
        "oi_chg": pick(d, OICHG_KEYS),
        "prev_oi": pick(d, PREVOI_KEYS),
        "ltp": num(pick(d, LTP_KEYS)),
        "ltp_chg": pick(d, LTPCHG_KEYS),
        "vol": num(pick(d, VOL_KEYS)),
        "iv": num(pick(d, IV_KEYS)),
    }


def side_from_prefixed(row: dict, prefixes: tuple) -> dict | None:
    low = _lk(row)
    sub = {}
    for k, v in low.items():
        for p in prefixes:
            if k.startswith(p + "_") or k.startswith(p):
                rest = k[len(p):].lstrip("_")
                if rest in OI_KEYS + OICHG_KEYS + PREVOI_KEYS + LTP_KEYS + LTPCHG_KEYS + VOL_KEYS + IV_KEYS:
                    sub[rest] = v
    return side_from_dict(sub) if sub else None


def opt_type_of(row: dict) -> str | None:
    t = pick(row, ("option_type", "optiontype", "opt_type", "opttype", "instrument_type", "type", "pc", "right"))
    if t is not None:
        t = str(t).upper()
        if t in ("CE", "CALL", "C") or t.endswith("CE"):
            return "CE"
        if t in ("PE", "PUT", "P") or t.endswith("PE"):
            return "PE"
    sym = str(pick(row, ("trading_symbol", "tradingsymbol", "symbol", "ts", "pTrdSymbol".lower()), "")).upper()
    if sym.endswith("CE"):
        return "CE"
    if sym.endswith("PE"):
        return "PE"
    return None


def normalize(raw: Any) -> tuple[list[dict], float | None]:
    # Kotak's option_chain endpoint returns separate `call` and `put` arrays;
    # each contract carries its strike and market data in nested objects.
    if isinstance(raw, dict) and (isinstance(raw.get("call"), list) or isinstance(raw.get("put"), list)):
        by_strike: dict[float, dict] = {}
        for side_name, side_key in (("call", "ce"), ("put", "pe")):
            contracts = raw.get(side_name, [])
            for contract in contracts:
                if not isinstance(contract, dict):
                    continue
                inst = contract.get("inst") or {}
                quote = contract.get("quote") or {}
                oi_data = contract.get("oi") or {}
                strike = num(pick(inst, STRIKE_KEYS), -1)
                if strike <= 0:
                    continue
                if strike > 1e6:
                    strike /= 100
                rec = by_strike.setdefault(strike, {"strike": strike, "ce": None, "pe": None})
                current_oi = num(pick(oi_data, ("cur", "current", "oi")))
                previous_oi = num(pick(oi_data, ("prev", "previous", "prev_oi")))
                ltp = num(pick(quote, LTP_KEYS))
                previous_close = num(pick(quote, ("pc", "prev_close")))
                rec[side_key] = {
                    "oi": current_oi,
                    "oi_chg": current_oi - previous_oi if previous_oi else None,
                    "prev_oi": pick(oi_data, ("prev", "previous", "prev_oi")),
                    "ltp": ltp,
                    "ltp_chg": ltp - previous_close if previous_close else None,
                    "vol": num(pick(quote, VOL_KEYS)),
                    "iv": num(pick(quote, IV_KEYS)),
                }
        if by_strike:
            empty = {"oi": 0.0, "oi_chg": None, "prev_oi": None, "ltp": 0.0, "ltp_chg": None, "vol": 0.0, "iv": 0.0}
            out = []
            for strike in sorted(by_strike):
                rec = by_strike[strike]
                rec["ce"] = rec["ce"] or dict(empty)
                rec["pe"] = rec["pe"] or dict(empty)
                out.append(rec)
            return out, find_spot(raw)

    rows = find_rows(raw)
    if not rows:
        raise ValueError("Option chain response did not contain strike rows")
    by_strike: dict[float, dict] = {}
    for row in rows:
        k = num(pick(row, STRIKE_KEYS), -1)
        if k <= 0:
            continue
        if k > 1e6:      # some feeds send strike * 100
            k /= 100
        rec = by_strike.setdefault(k, {"strike": k, "ce": None, "pe": None})
        low = _lk(row)
        ce = next((low[n] for n in CE_NAMES if isinstance(low.get(n), dict)), None)
        pe = next((low[n] for n in PE_NAMES if isinstance(low.get(n), dict)), None)
        if ce is not None or pe is not None:
            if ce is not None:
                rec["ce"] = side_from_dict(ce)
            if pe is not None:
                rec["pe"] = side_from_dict(pe)
            continue
        ot = opt_type_of(row)
        if ot:
            rec["ce" if ot == "CE" else "pe"] = side_from_dict(row)
            continue
        c = side_from_prefixed(row, ("ce", "call"))
        p = side_from_prefixed(row, ("pe", "put"))
        if c:
            rec["ce"] = c
        if p:
            rec["pe"] = p
    empty = {"oi": 0.0, "oi_chg": None, "prev_oi": None, "ltp": 0.0, "ltp_chg": None, "vol": 0.0, "iv": 0.0}
    out = []
    for k in sorted(by_strike):
        r = by_strike[k]
        r["ce"] = r["ce"] or dict(empty)
        r["pe"] = r["pe"] or dict(empty)
        out.append(r)
    return out, find_spot(raw)


# --------------------------------------------------------------------------- demo data
def demo_chain(und: str, expiry: str) -> tuple[list[dict], float]:
    und = und.upper()
    step = INDEX_STEP.get(und, 50)
    base = INDEX_SPOT.get(und, 1000.0)
    bucket = int(time.time() // 15)
    rnd = random.Random(f"{und}{bucket // 4}")          # drifts slowly
    jitter = random.Random(f"{und}{bucket}")
    walk = sum(random.Random(f"{und}w{i}").uniform(-1, 1) for i in range(bucket % 400)) * step * 0.05
    spot = round(base + walk, 2)
    atm = round(spot / step) * step
    days = max(1, (dt.date.fromisoformat(expiry) - dt.datetime.now(IST).date()).days) if expiry else 3
    t = math.sqrt(days / 365)
    sigma = 0.14
    strikes = [atm + i * step for i in range(-25, 26)]
    d_seed = random.Random(f"{und}{expiry}")
    ce_wall = atm + d_seed.choice([3, 4, 5, 6]) * step
    pe_wall = atm - d_seed.choice([3, 4, 5, 6]) * step
    out = []
    for k in strikes:
        def oi_for(wall, otm):
            peak = 1.0 * math.exp(-((k - wall) / (4 * step)) ** 2)
            base_oi = (0.25 + peak * 1.6 + (0.4 if otm else 0.1)) * 9_000_000 / (step / 50)
            return max(0, base_oi * rnd.uniform(0.8, 1.2))

        def ltp_for(is_ce):
            intrinsic = max(0, spot - k) if is_ce else max(0, k - spot)
            tv = spot * sigma * t * 0.4 * math.exp(-((k - spot) / (spot * sigma * t * 1.4)) ** 2)
            return round(intrinsic + max(tv, 0.05), 2)

        ce_oi, pe_oi = oi_for(ce_wall, k >= atm), oi_for(pe_wall, k <= atm)
        side = lambda oi, is_ce: {
            "oi": round(oi),
            "oi_chg": round(oi * jitter.uniform(-0.12, 0.28)),
            "prev_oi": None,
            "ltp": ltp_for(is_ce),
            "ltp_chg": round(ltp_for(is_ce) * jitter.uniform(-0.15, 0.15), 2),
            "vol": round(oi * jitter.uniform(0.3, 1.4)),
            "iv": round(sigma * 100 * (1 + abs(k - spot) / spot * 3) + jitter.uniform(-0.5, 0.5), 2),
        }
        out.append({"strike": k, "ce": side(ce_oi, True), "pe": side(pe_oi, False)})
    return out, spot


def demo_expiries() -> list[str]:
    d = dt.datetime.now(IST).date()
    out = []
    while len(out) < 5:
        d += dt.timedelta(days=1)
        if d.weekday() == 1:  # Tuesday
            out.append(d.isoformat())
    return out


# --------------------------------------------------------------------------- analytics
def estimate_spot(chain: list[dict]) -> float:
    best, best_k = 1e18, chain[len(chain) // 2]["strike"]
    for r in chain:
        if r["ce"]["ltp"] > 0 and r["pe"]["ltp"] > 0:
            diff = abs(r["ce"]["ltp"] - r["pe"]["ltp"])
            if diff < best:
                best, best_k = diff, r["strike"] + (r["ce"]["ltp"] - r["pe"]["ltp"])
    return round(best_k, 2)


def buildup(price_chg: float | None, oi_chg: float | None) -> str:
    if oi_chg is None or price_chg is None:
        return "Flat"
    if oi_chg == 0:
        return "No OI Change" if price_chg != 0 else "Flat"
    if price_chg == 0:
        return "Flat"
    if oi_chg > 0:
        return "Long Buildup" if price_chg > 0 else "Short Buildup"
    return "Short Covering" if price_chg > 0 else "Long Unwinding"


def apply_baseline(key: tuple, chain: list[dict]) -> str:
    """Fill oi_chg / ltp_chg from API values or, when absent, from the first snapshot of the day."""
    base = S.baseline.get(key)
    if not base or base["date"] != today_ist():
        base = {"date": today_ist(), "strikes": {r["strike"]: {"ce": dict(r["ce"]), "pe": dict(r["pe"])} for r in chain}}
        S.baseline[key] = base
    source = "api"
    for r in chain:
        b = base["strikes"].get(r["strike"])
        for side in ("ce", "pe"):
            s = r[side]
            if s["oi_chg"] is None:
                if s["prev_oi"] is not None:
                    s["oi_chg"] = s["oi"] - num(s["prev_oi"])
                elif b:
                    s["oi_chg"] = s["oi"] - b[side]["oi"]
                    source = "session"
                else:
                    s["oi_chg"] = 0
            s["oi_chg"] = num(s["oi_chg"])
            if s["ltp_chg"] is None:
                s["ltp_chg"] = (s["ltp"] - b[side]["ltp"]) if b else 0
            s["ltp_chg"] = num(s["ltp_chg"])
            s["signal"] = buildup(s["ltp_chg"], s["oi_chg"])
    return source


def max_pain(chain: list[dict]) -> float:
    best, best_k = None, chain[0]["strike"]
    for cand in chain:
        k = cand["strike"]
        pain = sum(max(0, k - r["strike"]) * r["ce"]["oi"] + max(0, r["strike"] - k) * r["pe"]["oi"] for r in chain)
        if best is None or pain < best:
            best, best_k = pain, k
    return best_k


def analyse(key: tuple, chain: list[dict], spot: float, source: str) -> dict:
    ce_oi = sum(r["ce"]["oi"] for r in chain)
    pe_oi = sum(r["pe"]["oi"] for r in chain)
    ce_chg = sum(r["ce"]["oi_chg"] for r in chain)
    pe_chg = sum(r["pe"]["oi_chg"] for r in chain)
    pcr = round(pe_oi / ce_oi, 3) if ce_oi else 0
    top_ce = sorted(chain, key=lambda r: r["ce"]["oi"], reverse=True)[:3]
    top_pe = sorted(chain, key=lambda r: r["pe"]["oi"], reverse=True)[:3]
    step = min((b["strike"] - a["strike"]) for a, b in zip(chain, chain[1:])) if len(chain) > 1 else 50
    atm = min(chain, key=lambda r: abs(r["strike"] - spot))["strike"]
    mood = "Bullish" if pcr >= 1.2 else "Bearish" if pcr <= 0.8 else "Neutral"
    if pe_chg > 0 > ce_chg or (pe_chg > 0 and pe_chg > ce_chg * 1.5):
        flow = "Put writing dominates: support building"
    elif ce_chg > 0 > pe_chg or (ce_chg > 0 and ce_chg > pe_chg * 1.5):
        flow = "Call writing dominates: resistance building"
    else:
        flow = "Balanced OI change"
    now = time.time()
    hist = S.history.setdefault(key, [])
    if not hist or now - hist[-1]["t"] >= SNAP_MIN_GAP:
        prev = hist[-1] if hist else None
        # First point uses the broker's daily OI change; later points use the
        # difference between successive snapshots, like the interval table in
        # the reference image.
        call_change = ce_chg if prev is None else ce_oi - prev["ce_oi"]
        put_change = pe_chg if prev is None else pe_oi - prev["pe_oi"]
        diff_oi = put_change - call_change
        direction_change = diff_oi - prev["diff_oi"] if prev else None
        direction_pct = (direction_change / abs(prev["diff_oi"]) * 100
                         if prev and prev["diff_oi"] else None)
        net_pcr = abs(put_change / call_change) if call_change else None
        sentiment = ("Bullish" if net_pcr is not None and net_pcr > 1
                     else "Bearish" if net_pcr is not None and net_pcr < 1
                     else "Neutral")
        hist.append({
            "t": now, "pcr": pcr, "spot": spot, "ce_oi": ce_oi, "pe_oi": pe_oi,
            "call_change": call_change, "put_change": put_change,
            "diff_oi": diff_oi, "direction_change": direction_change,
            "direction_pct": direction_pct, "net_pcr": net_pcr,
            "direction": "up" if direction_change is not None and direction_change > 0
                        else "down" if direction_change is not None and direction_change < 0
                        else "flat",
            "sentiment": sentiment,
        })
        del hist[:-720]
    return {
        "spot": spot, "atm": atm, "step": step, "pcr": pcr, "mood": mood, "flow": flow,
        "ce_oi": ce_oi, "pe_oi": pe_oi, "ce_oi_chg": ce_chg, "pe_oi_chg": pe_chg,
        "max_pain": max_pain(chain),
        "resistance": [r["strike"] for r in top_ce],
        "support": [r["strike"] for r in top_pe],
        "oi_change_source": source,
        "history": hist,
    }


# --------------------------------------------------------------------------- app
app = FastAPI(title="OI Pulse clone")


@app.on_event("startup")
async def startup():
    with history_connection():
        pass
    key = os.environ.get("KOTAK_CONSUMER_KEY", "")
    remembered = False
    if not key and CONFIG_FILE.exists():
        try:
            key, remembered = json.loads(CONFIG_FILE.read_text()).get("consumer_key", ""), True
        except Exception:  # noqa: BLE001
            key = ""
    if key:
        try:
            await connect_with_key(key, remember=False)
            S.remembered = remembered
        except HTTPException as e:
            S.last_error = str(e.detail)


class KeyIn(BaseModel):
    consumer_key: str
    remember: bool = True


@app.get("/api/status")
def status():
    return {"connected": not S.demo, "demo": S.demo, "remembered": S.remembered, "error": S.last_error}


@app.post("/api/connect")
async def connect(body: KeyIn):
    key = body.consumer_key.strip()
    if len(key) < 8:
        raise HTTPException(400, "That does not look like a consumer key / access token.")
    await connect_with_key(key, body.remember)
    return status()


@app.post("/api/demo")
def go_demo():
    S.demo, S.client, S.consumer_key, S.last_error = True, None, "", ""
    S.cache.clear()
    return status()


@app.post("/api/disconnect")
def disconnect():
    clear_key()
    S.remembered = False
    return go_demo()


@app.get("/api/expiries")
async def expiries(underlying: str = "NIFTY"):
    und = underlying.upper()
    if S.demo:
        return {"expiries": demo_expiries()}
    resp = await asyncio.to_thread(
        S.client.expiries, exchange=exchange_for(und), underlying=und, instrument_type="option")
    msg = err_text(resp)
    if msg:
        raise HTTPException(502, msg)
    found = sorted(set(re.findall(r"\d{4}-\d{2}-\d{2}", json.dumps(resp))))
    today = today_ist()
    return {"expiries": [d for d in found if d >= today]}


@app.get("/api/history/chain")
def chain_history(
    underlying: str = "NIFTY", expiry: str = "nearest",
    date: str | None = None, snapshot_id: int | None = None,
):
    und = underlying.upper()
    with history_connection() as con:
        if snapshot_id is not None:
            row = con.execute(
                "SELECT id,ts,mode,spot,chain_gzip FROM option_chain_snapshots WHERE id=? AND underlying=? AND expiry=?",
                (snapshot_id, und, expiry),
            ).fetchone()
            if not row:
                raise HTTPException(404, "That historical option-chain snapshot was not found.")
            chain = json.loads(gzip.decompress(row[4]).decode("utf-8"))
            return {"id": row[0], "t": row[1], "mode": row[2], "spot": row[3], "chain": chain}

        rows = con.execute(
            "SELECT id,ts,spot,mode FROM option_chain_snapshots WHERE underlying=? AND expiry=? ORDER BY ts DESC",
            (und, expiry),
        ).fetchall()
    if date is None:
        dates = sorted({dt.datetime.fromtimestamp(r[1], IST).strftime("%Y-%m-%d") for r in rows}, reverse=True)
        return {"dates": dates}
    try:
        day = dt.date.fromisoformat(date)
    except ValueError:
        raise HTTPException(400, "Date must use YYYY-MM-DD format.")
    start = dt.datetime.combine(day, dt.time.min, IST).timestamp()
    end = dt.datetime.combine(day + dt.timedelta(days=1), dt.time.min, IST).timestamp()
    snapshots = [{"id": r[0], "t": r[1], "spot": r[2], "mode": r[3]}
                 for r in rows if start <= r[1] < end]
    return {"snapshots": snapshots}


@app.get("/api/futures/contracts")
async def futures_contracts(symbol: str = "NIFTY", exchange_segment: str = "nse_fo"):
    if S.demo:
        raise HTTPException(400, "Connect your Kotak Neo key to search live futures contracts.")
    symbol = symbol.strip().upper()
    if not re.fullmatch(r"[A-Z0-9&_-]{1,24}", symbol):
        raise HTTPException(400, "Enter a valid futures symbol, such as NIFTY or RELIANCE.")
    if exchange_segment not in ("nse_fo", "bse_fo"):
        raise HTTPException(400, "Choose NSE F&O or BSE F&O.")
    response = await asyncio.to_thread(
        S.client.search_scrip, exchange_segment=exchange_segment, symbol=symbol,
        expiry="", option_type="FUT", strike_price="",
    )
    message = err_text(response)
    if message:
        raise HTTPException(502, f"Kotak futures search failed: {message}")
    records = records_from_scrip_response(response)
    contracts = []
    for record in records:
        if not isinstance(record, dict) or not record.get("pSymbol"):
            continue
        token = str(record["pSymbol"])
        trade_symbol = str(record.get("pTrdSymbol") or record.get("pSymbolName") or symbol)
        contracts.append({
            "neosymbol": f"{exchange_segment}|{token}",
            "trading_symbol": trade_symbol,
            "expiry": expiry_from_future_record(record, exchange_segment),
        })
    if not contracts:
        raise HTTPException(404, f"No futures contracts found for {symbol} on {exchange_segment}.")
    return {"symbol": symbol, "contracts": contracts}


@app.get("/api/futures/heatmap")
async def futures_heatmap(exchange_segment: str = "all"):
    if S.demo:
        raise HTTPException(400, "Connect your Kotak Neo key to scan live futures.")
    segments = ("nse_fo", "bse_fo") if exchange_segment == "all" else (exchange_segment,)
    if any(segment not in ("nse_fo", "bse_fo") for segment in segments):
        raise HTTPException(400, "Choose all exchanges, NSE F&O, or BSE F&O.")

    # An empty search symbol asks Kotak's scrip search for every FUT contract.
    # Keep only the nearest non-expired contract for each underlying.
    contracts = []
    for segment in segments:
        response = await asyncio.to_thread(
            S.client.search_scrip, exchange_segment=segment, symbol="",
            expiry="", option_type="FUT", strike_price="",
        )
        message = err_text(response)
        if message:
            raise HTTPException(502, f"Kotak {segment} scrip search failed: {message}")
        by_underlying: dict[str, list[tuple[str, dict]]] = {}
        for record in records_from_scrip_response(response):
            token = record.get("pSymbol")
            symbol = str(record.get("pSymbolName") or record.get("pScripRefKey") or "").strip().upper()
            trade_symbol = str(record.get("pTrdSymbol") or "").strip().upper()
            if not token or not symbol or not trade_symbol:
                continue
            expiry = expiry_from_future_record(record, segment)
            if expiry and expiry < today_ist():
                continue
            by_underlying.setdefault(symbol, []).append((expiry or "9999-12-31", record))
        for symbol, candidates in by_underlying.items():
            expiry, record = min(candidates, key=lambda item: item[0])
            token = str(record["pSymbol"])
            contracts.append({
                "symbol": symbol, "exchange_segment": segment,
                "neosymbol": f"{segment}|{token}", "token": token,
                "trading_symbol": str(record.get("pTrdSymbol") or symbol),
                "expiry": expiry if expiry != "9999-12-31" else "",
            })

    if not contracts:
        return {"exchange_segment": exchange_segment, "rows": [], "sampled_at": time.time()}

    now = time.time()
    results = []
    # Kotak accepts at most 50 instruments per quote request.
    for start in range(0, len(contracts), 50):
        batch = contracts[start:start + 50]
        response = await asyncio.to_thread(
            S.client.quotes,
            instrument_tokens=[{"instrument_token": c["token"], "exchange_segment": c["exchange_segment"]} for c in batch],
            quote_type="all",
        )
        message = err_text(response)
        if message:
            raise HTTPException(502, f"Kotak futures quotes failed: {message}")
        if isinstance(response, list):
            quote_rows = response
        elif isinstance(response, dict):
            quote_rows = next((v for v in response.values() if isinstance(v, list)), [response])
        else:
            quote_rows = []
        token_map = {str(q.get("exchange_token") or q.get("instrument_token") or ""): q
                     for q in quote_rows if isinstance(q, dict)}
        for i, contract in enumerate(batch):
            quote = token_map.get(contract["token"])
            if quote is None and i < len(quote_rows) and isinstance(quote_rows[i], dict):
                quote = quote_rows[i]
            row = {k: v for k, v in contract.items() if k != "token"}
            if not quote:
                row["error"] = "No quote returned."
                results.append(row)
                continue
            price = num(pick(quote, ("ltp", "last_price", "last_traded_price")))
            oi = num(pick(quote, ("open_int", "oi", "open_interest", "openint")))
            if price <= 0 or oi <= 0:
                row["error"] = "Live price or OI is unavailable."
                results.append(row)
                continue
            save_futures_snapshot(contract["neosymbol"], contract["trading_symbol"], price, oi)
            signals, sample_count = futures_signals_from_history(contract["neosymbol"], price, oi, now)
            row.update({"price": price, "oi": oi, "signals": signals, "sample_count": sample_count})
            results.append(row)
    results.sort(key=lambda row: (row["exchange_segment"], row["symbol"]))
    return {"exchange_segment": exchange_segment, "rows": results, "sampled_at": now}


@app.get("/api/futures/signals")
async def futures_signals(neosymbol: str, trading_symbol: str = ""):
    if S.demo:
        raise HTTPException(400, "Connect your Kotak Neo key to calculate futures signals.")
    parts = neosymbol.split("|", 1)
    if len(parts) != 2 or parts[0] not in ("nse_fo", "bse_fo") or not parts[1].isdigit():
        raise HTTPException(400, "Select a valid NSE or BSE futures contract.")
    response = await asyncio.to_thread(
        S.client.quotes,
        instrument_tokens=[{"instrument_token": parts[1], "exchange_segment": parts[0]}],
        quote_type="all",
    )
    message = err_text(response)
    if message:
        raise HTTPException(502, f"Kotak quote failed: {message}")
    if isinstance(response, list):
        quote = response[0] if response else {}
    elif isinstance(response, dict):
        quote = next((v[0] for v in response.values() if isinstance(v, list) and v), response)
    else:
        quote = {}
    price = num(pick(quote, ("ltp", "last_price", "last_traded_price")))
    oi = num(pick(quote, ("open_int", "oi", "open_interest", "openint")))
    if price <= 0:
        raise HTTPException(502, "Kotak did not return a usable futures price.")
    if oi <= 0:
        raise HTTPException(502, "Kotak did not return futures OI. Signals need both live price and OI.")
    trading_symbol = trading_symbol or str(quote.get("display_symbol") or neosymbol)
    save_futures_snapshot(neosymbol, trading_symbol, price, oi)
    now = time.time()
    signals, sample_count = futures_signals_from_history(neosymbol, price, oi, now)
    return {
        "neosymbol": neosymbol, "trading_symbol": trading_symbol, "price": price,
        "oi": oi, "sampled_at": now, "sample_count": sample_count, "signals": signals,
        "note": "Signals compare the latest quote with the closest saved quote at or before each timeframe cutoff.",
    }


@app.get("/api/chain")
async def chain(underlying: str = "NIFTY", expiry: str | None = None, count: int = Query(40, ge=10, le=100)):
    und = underlying.upper()
    ck = (und, expiry or "", count, S.demo)
    hit = S.cache.get(ck)
    if hit and time.time() - hit[0] < CACHE_TTL:
        return hit[1]
    if S.demo:
        if not expiry:
            expiry = demo_expiries()[0]
        rows, spot = demo_chain(und, expiry)
        source_tag = "demo"
    else:
        count10 = max(10, round(count / 10) * 10)
        raw = await asyncio.to_thread(
            S.client.option_chain, exchange=exchange_for(und), underlying=und,
            expiry=expiry, instrument_type="option", count=count10)
        msg = err_text(raw)
        if msg:
            raise HTTPException(502, f"Kotak error: {msg}")
        try:
            rows, spot = normalize(raw)
        except ValueError as exc:
            snippet = json.dumps(raw, default=str)[:1500]
            raise HTTPException(502, f"{exc}. Kotak replied: {snippet}")
        spot = spot or estimate_spot(rows)
        source_tag = "live"
    key = (und, expiry or "nearest")
    src = apply_baseline(key, rows)
    result = {
        "underlying": und, "expiry": expiry, "mode": source_tag,
        "updated": dt.datetime.now(IST).strftime("%H:%M:%S"),
        "analytics": analyse(key, rows, spot, src),
        "chain": rows,
    }
    save_chain_snapshot(und, expiry or "nearest", source_tag, spot, rows)
    S.cache[ck] = (time.time(), result)
    return result


@app.get("/api/raw")
async def raw(underlying: str = "NIFTY", expiry: str | None = None):
    """Debug helper: the untouched Kotak option-chain response (first 30 KB)."""
    if S.demo:
        raise HTTPException(400, "Connect a key first.")
    und = underlying.upper()
    r = await asyncio.to_thread(S.client.option_chain, exchange=exchange_for(und), underlying=und,
                                expiry=expiry, instrument_type="option", count=10)
    return json.loads(json.dumps(r)[:30000]) if len(json.dumps(r)) < 30000 else {"truncated": json.dumps(r)[:30000]}


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=FRONTEND), name="static")
