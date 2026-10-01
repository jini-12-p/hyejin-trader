#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JAN-AUG 2026 CURRENT P BACKTEST (BALANCED-MEM resumable / background-safe)

Goal
----
Approximate 2026-01-01 ~ 2026-08-31 with the CURRENT P logic:
  historical dynamic-universe approximation
  -> P_V22 -> P_V25 WATCH/5m confirmation
  -> SAFE(C/RN/RS) + historical RELAX proxy
  -> MKT100
  -> market-risk performance switch N=9, +9 OFF / -6 ON
  -> V22 quality OR LOCK0
  -> 4 slots / rolling15m max2 / CD90 / STOP-CD180 / STOP-pause45
  -> TP2.0 FULL / PP12 / Final4 / V27-1 fallback
  -> taker fees, no Recovery/DCA

Important validation limits
---------------------------
The 9/28 historical-candle calibration reproduced:
  V22 recall/precision ~96%, exact V25 setup IDs 73/82 (89%).
The remaining error is mostly live intraminute price + WATCH/lock state.
For Jan-Aug this script therefore reports an APPROXIMATE historical backtest,
not an exact reconstruction of live scans.

Additional approximation:
  Historical bid/ask spread snapshots are unavailable from klines, therefore
  dynamic-universe spread is treated as 0 for historical ranking.
  RELAX's old P_FWD_CONTROL stream does not exist in Jan-Aug, so the script
  builds a causal control proxy using the current base-exit replay.

Safety / resilience
-------------------
- NO DB writes.
- NO live/demo/private orders.
- Public Bybit market-data reads only.
- Each completed month writes checkpoint files and a .done marker.
- Exit simulations are checkpointed too.
- Re-running with --resume skips completed month generation and cached sims.
- Designed to run under nohup so SSH/terminal disconnect does not stop it.
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.util
import json
import math
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from collections import Counter, defaultdict, deque, OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

START_KST = datetime(2026, 1, 1, 0, 0, 0, tzinfo=KST)
END_KST = datetime(2026, 9, 1, 0, 0, 0, tzinfo=KST)
START_UTC = START_KST.astimezone(UTC)
END_UTC = END_KST.astimezone(UTC)

MARKET_V25_2H_MIN = 8
MARKET_ABS4H_AVG_MIN = 0.40
REGIME_N = 9
REGIME_OFF_AT = 9.0
REGIME_ON_AT = -6.0
FINAL_STOP_PAUSE_MIN = 45
REFERENCE_STOP_PAUSE_MIN = 30

SCAN_STEP_MIN = 5
WATCH_WARM_MIN = 60
UNIVERSE_WARM_DAYS = 7
HOURLY_PREFILTER_MIN_TOP = 80
HOURLY_LOOSE_TURNOVER_FACTOR = 0.50
HOURLY_LOOSE_RANGE_FACTOR = 0.70
HOURLY_LOOSE_CHANGE_PAD = 1.50
API_SLEEP = 0.035
FLUSH_SIM_EVERY = 50
HIST_MEM_MAX = 8
REPLAY_SYMBOL_LRU = 48
REPLAY_LRU_SOFT = 32
REPLAY_LRU_HARD = 16
RSS_SOFT_MB = 1000.0
RSS_HARD_MB = 1400.0
EXIT_KLINE_MEM_MAX = 32
REPLAY_LOG_EVERY = 250


def pflush(*a):
    print(*a, flush=True)


def rss_mb():
    """Current process RSS in MB from /proc; defensive fallback to 0."""
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return float(line.split()[1]) / 1024.0
    except Exception:
        pass
    return 0.0


def fnum(v, default=None):
    try:
        if v is None:
            return default
        s = str(v).strip()
        if not s or s.lower() == "nan":
            return default
        x = float(v)
        return default if math.isnan(x) else x
    except Exception:
        return default


def json_default(o):
    if hasattr(o, "item"):
        try:
            return o.item()
        except Exception:
            pass
    if isinstance(o, (datetime, pd.Timestamp)):
        return o.isoformat()
    return str(o)


def jdumps(x):
    return json.dumps(x, ensure_ascii=False, separators=(",", ":"), default=json_default)


def jloads(x):
    try:
        if x is None or str(x).strip() in ("", "nan"):
            return {}
        return json.loads(str(x))
    except Exception:
        return {}


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def month_ranges():
    out = []
    cur = START_KST
    while cur < END_KST:
        if cur.month == 12:
            nxt = cur.replace(year=cur.year+1, month=1, day=1)
        else:
            nxt = cur.replace(month=cur.month+1, day=1)
        out.append((cur, min(nxt, END_KST)))
        cur = nxt
    return out


def month_key(d: datetime) -> str:
    return d.strftime("%Y-%m")


def safe_name(s: str) -> str:
    return "".join(c for c in str(s) if c.isalnum() or c in "_-.")[:80]


class StateFile:
    def __init__(self, path: Path):
        self.path = path
        self.data = {}
        if path.exists():
            try:
                self.data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                self.data = {}

    def update(self, **kw):
        self.data.update(kw)
        self.data["updated_kst"] = datetime.now(KST).isoformat()
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)


class Http:
    def __init__(self, retries=7):
        self.retries = retries

    def json(self, url: str) -> dict:
        last = None
        for attempt in range(self.retries):
            try:
                req = urllib.request.Request(url, headers={"User-Agent":"HJ-JANAUG-BT/1.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    obj = json.loads(resp.read().decode("utf-8"))
                if int(obj.get("retCode", -1)) != 0:
                    raise RuntimeError(f"{obj.get('retCode')} {obj.get('retMsg')}")
                return obj
            except Exception as e:
                last = e
                time.sleep(min(10.0, 0.8*(attempt+1)))
        raise RuntimeError(f"HTTP failed: {last}")


HTTP = Http()


def instrument_payload(cache: Path):
    if cache.exists() and cache.stat().st_size > 100:
        try:
            x = json.loads(cache.read_text(encoding="utf-8"))
            if isinstance(x, dict) and "rows" in x:
                return x
            if isinstance(x, list):
                return {"rows":x,"warnings":[]}
        except Exception:
            pass

    merged = {}
    warnings = []
    for status in ("Trading", "Settled"):
        cursor = ""
        pages = 0
        while True:
            params = {"category":"linear", "limit":1000, "status":status}
            if cursor:
                params["cursor"] = cursor
            url = "https://api.bybit.com/v5/market/instruments-info?" + urllib.parse.urlencode(params)
            try:
                obj = HTTP.json(url)
            except Exception as e:
                warnings.append(f"{status}:{e}")
                break
            result = obj.get("result", {})
            rows = result.get("list", []) or []
            for r in rows:
                sym = str(r.get("symbol") or "")
                if sym:
                    merged[sym] = r
            cursor = str(result.get("nextPageCursor") or "")
            pages += 1
            if not cursor or pages > 30:
                break
            time.sleep(API_SLEEP)

    payload = {"rows":list(merged.values()),"warnings":warnings}
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return payload


def active_in_window(inst: dict, start: datetime, end: datetime) -> bool:
    try:
        launch = int(inst.get("launchTime") or 0) / 1000.0
    except Exception:
        launch = 0
    try:
        delivery_raw = int(inst.get("deliveryTime") or 0)
        delivery = delivery_raw / 1000.0 if delivery_raw > 0 else 0
    except Exception:
        delivery = 0
    st = start.astimezone(UTC).timestamp()
    en = end.astimezone(UTC).timestamp()
    if launch and launch >= en:
        return False
    if delivery and delivery < st:
        return False
    return True


def noncrypto_match(symbol: str, blocked_bases: set[str]) -> bool:
    normalized = symbol.upper().replace("-", "").replace("_", "")
    base = normalized[:-4] if normalized.endswith("USDT") else normalized
    return any(base == b or base.endswith(b) or b in base for b in blocked_bases)


class HistCache:
    BASE = "https://api.bybit.com/v5/market/kline"
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.mem = OrderedDict()
        self.max_mem = HIST_MEM_MAX

    def clear_mem(self):
        self.mem.clear()
        gc.collect()

    def _remember(self, key, df):
        if key in self.mem:
            self.mem.pop(key, None)
        self.mem[key] = df
        while len(self.mem) > self.max_mem:
            self.mem.popitem(last=False)
        return df

    def _path(self, symbol, interval, start, end):
        return self.root / f"{safe_name(symbol)}_{interval}_{start.strftime('%Y%m%d%H')}_{end.strftime('%Y%m%d%H')}.csv.gz"

    def get(self, symbol: str, interval: str, start: datetime, end: datetime) -> pd.DataFrame:
        start = start.astimezone(UTC).replace(second=0,microsecond=0)
        end = end.astimezone(UTC).replace(second=0,microsecond=0)
        key = (symbol,str(interval),int(start.timestamp()),int(end.timestamp()))
        if key in self.mem:
            df = self.mem.pop(key)
            self.mem[key] = df
            return df
        p = self._path(symbol, interval, start, end)
        if p.exists() and p.stat().st_size > 50:
            try:
                df = pd.read_csv(p, compression="gzip")
                if not df.empty:
                    df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
                return self._remember(key, df)
            except Exception:
                try: p.unlink()
                except Exception: pass

        iv = int(interval)
        step_ms = iv * 60_000
        start_ms = int(start.timestamp()*1000)
        end_ms = int((end-timedelta(milliseconds=1)).timestamp()*1000)
        rows = {}
        cur = start_ms
        while cur <= end_ms:
            chunk_end = min(end_ms, cur + step_ms*999)
            params = {
                "category":"linear","symbol":symbol,"interval":str(interval),
                "start":cur,"end":chunk_end,"limit":1000,
            }
            url = self.BASE + "?" + urllib.parse.urlencode(params)
            obj = HTTP.json(url)
            data = obj.get("result",{}).get("list",[]) or []
            for r in data:
                try:
                    ts = int(r[0])
                    if start_ms <= ts <= end_ms:
                        rows[ts] = r
                except Exception:
                    pass
            cur = chunk_end + step_ms
            time.sleep(API_SLEEP)

        cols = ["ts","open","high","low","close","volume","turnover"]
        arr = [rows[k] for k in sorted(rows)]
        df = pd.DataFrame(arr, columns=cols if arr else cols)
        if not df.empty:
            for c in ["open","high","low","close","volume","turnover"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            df["ts"] = pd.to_numeric(df["ts"], errors="coerce").astype("Int64")
            df["start_time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            df = df.sort_values("start_time").drop_duplicates("start_time",keep="last").reset_index(drop=True)
        else:
            df["start_time"] = pd.to_datetime(pd.Series([],dtype="datetime64[ns, UTC]"))
        tmp = p.with_suffix(p.suffix+".tmp")
        df.to_csv(tmp,index=False,compression="gzip")
        tmp.replace(p)
        return self._remember(key, df)

def loose_hourly_candidates(df: pd.DataFrame, cfg) -> pd.DataFrame:
    if df.empty or len(df) < 30:
        return pd.DataFrame(columns=["time","score"])
    z = df.set_index("start_time").sort_index().copy()
    open24 = z["open"].shift(23)
    high24 = z["high"].rolling(24,min_periods=20).max()
    low24 = z["low"].rolling(24,min_periods=20).min()
    turn24 = z["turnover"].rolling(24,min_periods=20).sum()
    change = (z["close"]/open24-1)*100
    rng = (high24/low24-1)*100
    liq = (turn24.clip(lower=1)).apply(lambda x: math.log10(x)*4 if pd.notna(x) else float("nan"))
    score = liq + change.clip(upper=35)*8 + rng.clip(upper=14)*3
    mask = (
        (turn24 >= float(cfg.min_quote_volume_24h_usdt)*HOURLY_LOOSE_TURNOVER_FACTOR)
        & (rng >= float(cfg.min_range_24h_pct)*HOURLY_LOOSE_RANGE_FACTOR)
        & (rng <= float(cfg.max_range_24h_pct)*1.20)
        & (change >= float(cfg.min_change_24h_pct)-HOURLY_LOOSE_CHANGE_PAD)
        & (change <= float(cfg.max_abs_change_24h_pct)+10.0)
    )
    return pd.DataFrame({"time":z.index,"score":score})[mask].dropna()


def prep_15_metrics(df: pd.DataFrame, cfg) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    z = df.set_index("start_time").sort_index().copy()
    open24 = z["open"].shift(95)
    high24 = z["high"].rolling(96,min_periods=80).max()
    low24 = z["low"].rolling(96,min_periods=80).min()
    turn24 = z["turnover"].rolling(96,min_periods=80).sum()
    change24 = (z["close"]/open24-1)*100
    range24 = (high24/low24-1)*100

    r4 = (z["high"].rolling(4,min_periods=4).max() / z["low"].rolling(4,min_periods=4).min() - 1)*100
    recent4h = (z["high"].rolling(16,min_periods=16).max() / z["low"].rolling(16,min_periods=16).min() - 1)*100
    avg_hourly = (r4 + r4.shift(4) + r4.shift(8) + r4.shift(12))/4.0
    move1h = (z["close"]/z["close"].shift(4)-1).abs()*100

    liq = (turn24.clip(lower=1)).apply(lambda x: math.log10(x)*4 if pd.notna(x) else float("nan"))
    ticker_score = liq + change24.clip(upper=35)*8 + range24.clip(upper=14)*3
    intraday = recent4h.clip(upper=8)*10 + avg_hourly.clip(upper=3)*12
    final_score = ticker_score + intraday

    pass_ticker = (
        (turn24 >= float(cfg.min_quote_volume_24h_usdt))
        & (range24 >= float(cfg.min_range_24h_pct))
        & (range24 <= float(cfg.max_range_24h_pct))
        & (change24 >= float(cfg.min_change_24h_pct))
        & (change24 <= float(cfg.max_abs_change_24h_pct))
    )
    pass_recent = (
        (recent4h >= float(cfg.min_recent_4h_range_pct))
        & (avg_hourly >= float(cfg.min_avg_hourly_range_pct))
        & (move1h <= float(cfg.max_recent_1h_move_pct))
        & (move1h >= float(cfg.min_recent_1h_move_pct))
    )
    return pd.DataFrame({
        "ticker_score":ticker_score,"final_score":final_score,
        "quote_volume":turn24,"range24":range24,"change24":change24,
        "recent4h":recent4h,"avg_hourly":avg_hourly,"move1h":move1h,
        "pass_ticker":pass_ticker,"pass_recent":pass_recent,
    })


@dataclass
class SymbolData:
    m5: pd.DataFrame
    m15: pd.DataFrame  # indicators precomputed
    h1: pd.DataFrame   # indicators precomputed


def _pos_before(df: pd.DataFrame, t: datetime) -> int:
    """Index position of last bar whose start_time is strictly before t."""
    if df.empty:
        return -1
    return int(df["start_time"].searchsorted(pd.Timestamp(t), side="left")) - 1


def fast_v22_details(sd: SymbolData, now: datetime, cfg, market: dict[str, Any]) -> dict[str, Any]:
    """Fast 5m-cadence reproduction of the P_V22 fields needed by V25/final filters.

    It uses precomputed 15m/1h indicators, so each scan avoids rerunning pandas
    EMA/RSI over 220+140 rows. This is materially faster for Jan-Aug replay.
    """
    now = now.astimezone(UTC)
    b15 = now.replace(minute=(now.minute // 15) * 15, second=0, microsecond=0)
    b5 = now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)
    bh = now.replace(minute=0, second=0, microsecond=0)

    i15 = _pos_before(sd.m15, b15)
    ih = _pos_before(sd.h1, bh)
    i5 = _pos_before(sd.m5, b5)
    if i15 < 69 or ih < 69 or i5 < 0:
        return {"p_v22_candidate": False, "reason": "candle_short"}

    row = sd.m15.iloc[i15]
    prev = sd.m15.iloc[i15 - 1]
    prevprev = sd.m15.iloc[i15 - 2]
    prev3 = sd.m15.iloc[i15 - 3]
    hrow = sd.h1.iloc[ih]
    hprev = sd.h1.iloc[ih - 1]

    # Current partial 15m bar from completed 5m bars in this 15m bucket.
    j0 = int(sd.m5["start_time"].searchsorted(pd.Timestamp(b15), side="left"))
    j1 = int(sd.m5["start_time"].searchsorted(pd.Timestamp(b5), side="left"))
    part = sd.m5.iloc[j0:j1]
    if part.empty:
        live_open = live_high = live_low = live_close = float(row["close"])
        live_vol = 0.0
    else:
        live_open = float(part.iloc[0]["open"])
        live_high = float(part["high"].max())
        live_low = float(part["low"].min())
        live_close = float(part.iloc[-1]["close"])
        live_vol = float(part["volume"].sum())

    price = float(row["close"])
    vol_avg = fnum(row.get("vol_avg"), 0.0) or 0.0
    volume_ratio = float(row["volume"]) / vol_avg if vol_avg > 0 else 0.0

    recent = sd.m15.iloc[max(0, i15 - 15):i15 + 1]
    recent_high = float(recent.iloc[:-1]["high"].max()) if len(recent) > 1 else float(row["high"])
    recent_low = float(recent["low"].min())
    pullback_from_high = (recent_high / price - 1) * 100 if price > 0 else 99.0
    rebound_from_low = (price / recent_low - 1) * 100 if recent_low > 0 else 0.0
    entry_candle_gain = (float(row["close"]) / float(row["open"]) - 1) * 100 if float(row["open"]) > 0 else 99.0
    distance_to_high = (recent_high / price - 1) * 100 if price > 0 else 0.0
    one_hour_move = abs(float(row["close"]) / float(sd.m15.iloc[i15 - 4]["close"]) - 1) * 100
    candle_range = abs(float(row["high"]) / float(row["low"]) - 1) * 100 if float(row["low"]) > 0 else 99.0

    h1_up = bool(float(hrow["ema20"]) > float(hrow["ema60"]) and float(hrow["ema20"]) >= float(hprev["ema20"]))
    pullback_ok = bool(float(cfg.min_pullback_from_high_pct) <= pullback_from_high <= float(cfg.max_pullback_from_high_pct))
    not_chasing = bool(entry_candle_gain <= float(cfg.max_entry_candle_gain_pct) and distance_to_high >= float(cfg.max_near_high_pct))
    rebound = bool(float(row["close"]) > float(row["open"]) and float(row["close"]) > float(prev["close"]) and float(row["close"]) >= float(row["ema9"]))
    rsi_now = float(row["rsi"])
    rsi_prev = float(prev["rsi"])
    rsi_delta = rsi_now - rsi_prev
    momentum_ok = bool(40 <= rsi_now <= 75 and rsi_now >= rsi_prev)
    volume_ok = bool(volume_ratio >= float(cfg.entry_min_volume_ratio))
    not_extreme = bool(one_hour_move <= float(cfg.max_recent_1h_move_pct) and candle_range <= 4.0)

    rebound_setup = bool(float(prev["close"]) > float(prev["open"]) and float(prev["close"]) > float(prevprev["close"]) and float(prev["close"]) >= float(prev["ema9"]))
    confirmation_hold = bool(float(row["low"]) >= float(prev["low"]) and float(row["close"]) >= float(prev["close"]) * 0.998 and float(row["close"]) > float(row["open"]))
    rebound = bool(rebound and (not bool(cfg.require_rebound_confirmation_candle) or (rebound_setup and confirmation_hold)))

    vols = sd.m15.iloc[max(0, i15 - 2):i15 + 1]["volume"].tolist()
    volume_declining_3 = bool(len(vols) == 3 and vols[0] > vols[1] > vols[2])
    volume_trend_ok = bool(not bool(cfg.reject_three_bar_volume_decline) or not volume_declining_3)
    movement_ok = bool(one_hour_move >= float(cfg.min_recent_1h_move_pct))

    ema_ordered = bool(float(row["ema9"]) > float(row["ema20"]) > float(row["ema60"]))
    ema9_rising = bool(float(row["ema9"]) > float(prev["ema9"]) > float(prevprev["ema9"]))
    higher_lows = bool(float(row["low"]) > float(prev["low"]) and float(prev["low"]) >= float(prevprev["low"]))
    higher_highs = bool(float(row["high"]) > float(prev["high"]) and float(prev["high"]) >= float(prevprev["high"]))

    # live_candle_quality_ok() equivalent needed by p_v2_soft_score.
    live_gain = (live_close / live_open - 1) * 100 if live_open > 0 else 0.0
    bullish = live_close > live_open
    body = max(live_close - live_open, 0.0)
    body_pct = (body / live_open) * 100 if live_open > 0 else 0.0
    body_ok = body_pct >= float(cfg.live_reversal_min_body_pct)
    upper_wick = max(live_high - max(live_open, live_close), 0.0)
    wick_ratio = upper_wick / body if body > 0 else 999.0
    wick_ok = wick_ratio <= float(cfg.live_reversal_max_upper_wick_to_body)
    prev_bearish = float(row["close"]) < float(row["open"])
    recovery_ok = True
    if prev_bearish:
        prev_body = float(row["open"]) - float(row["close"])
        recovered = max(live_close - float(row["close"]), 0.0)
        recovery_ratio = recovered / prev_body if prev_body > 0 else 1.0
        recovery_ok = recovery_ratio >= float(cfg.live_reversal_min_prev_body_recovery)
    five_ok = False
    if i5 >= 0:
        last5 = sd.m5.iloc[i5]
        five_ok = float(last5["close"]) > float(last5["open"])
    quality_ok = bool(bullish and body_ok and wick_ok and recovery_ok and (five_ok if bool(cfg.live_reversal_require_5m_bullish) else True))

    one_hour_signed_move = (float(row["close"]) / float(sd.m15.iloc[i15 - 4]["close"]) - 1) * 100
    ema9_slope_pct = (float(row["ema9"]) / float(prev["ema9"]) - 1) * 100 if float(prev["ema9"]) > 0 else 0.0
    ema20_slope_pct = (float(row["ema20"]) / float(prev["ema20"]) - 1) * 100 if float(prev["ema20"]) > 0 else 0.0
    ema9_ema20_gap_pct = (float(row["ema9"]) / float(row["ema20"]) - 1) * 100 if float(row["ema20"]) > 0 else 0.0

    def pct(a, b):
        return (float(a) / float(b) - 1) * 100 if float(b) != 0 else 0.0

    rsi_prev2 = float(prevprev["rsi"])
    rsi_prev3 = float(prev3["rsi"])
    rsi_change_prev1 = float(prev["rsi"]) - float(prevprev["rsi"])
    ema9_slope_prev1_pct = pct(prev["ema9"], prevprev["ema9"])
    ema20_slope_prev2_pct = pct(prevprev["ema20"], prev3["ema20"])

    p_v2_trend_score = (
        min(15.0, max(0.0, one_hour_signed_move) * 6.0)
        + min(10.0, max(0.0, ema9_slope_pct) * 25.0)
        + min(10.0, max(0.0, ema20_slope_pct) * 30.0)
        + min(10.0, max(0.0, ema9_ema20_gap_pct) * 6.67)
    )
    p_v2_structure_score = (
        min(15.0, max(0.0, rebound_from_low) * 2.0)
        + (5.0 if higher_highs else 0.0)
        + (5.0 if higher_lows else 0.0)
    )
    if 62.0 <= rsi_now <= 72.0:
        rsi_level_score = 12.0
    elif 58.0 <= rsi_now < 62.0:
        rsi_level_score = 8.0
    elif 54.0 <= rsi_now < 58.0:
        rsi_level_score = 4.0
    elif 72.0 < rsi_now <= 75.0:
        rsi_level_score = 8.0
    elif rsi_now > 75.0:
        rsi_level_score = 3.0
    else:
        rsi_level_score = 0.0
    rsi_delta_score = max(-3.0, min(5.0, rsi_delta * 1.5))
    live_score = 5.0 if 0.20 <= live_gain <= 0.60 else (2.0 if -0.10 <= live_gain <= 0.90 else 0.0)
    momentum_score = rsi_level_score + rsi_delta_score + live_score
    soft_score = (
        (3.0 if pullback_ok else 0.0)
        + (3.0 if rebound else 0.0)
        + (2.0 if momentum_ok else 0.0)
        + (2.0 if volume_ok else 0.0)
        + (2.0 if volume_trend_ok else 0.0)
        + (3.0 if quality_ok else 0.0)
        + (3.0 if not_chasing else 0.0)
    )
    p_v2_score = float(p_v2_trend_score + p_v2_structure_score + momentum_score + soft_score)
    signal_checks = (
        one_hour_signed_move >= 0.75,
        ema9_slope_pct >= 0.12,
        ema20_slope_pct >= 0.08,
        ema9_ema20_gap_pct >= 0.20,
        rebound_from_low >= 3.50,
        rsi_now >= 56.0,
        rsi_delta >= -1.0,
    )
    signal_pass = sum(int(x) for x in signal_checks)
    persistence = float(p_v2_trend_score + p_v2_structure_score)

    heat_count = sum([
        int(rsi_now >= 75.0),
        int(rebound_from_low >= 12.0),
        int(ema9_ema20_gap_pct >= 2.0),
        int(one_hour_signed_move >= 3.5),
        int(live_gain >= 0.45),
    ])
    late_extension = bool(
        heat_count > int(cfg.research_pv22_max_heat_count)
        or (rsi_now >= 78.0 and rsi_delta >= 4.0 and heat_count >= 2)
    )
    weak_count = sum([
        int(ema9_ema20_gap_pct < 0.05),
        int(ema20_slope_pct < 0.06),
        int(ema9_slope_pct < 0.08),
        int(p_v2_structure_score < 12.0),
        int(one_hour_signed_move < 0.40),
    ])
    structure_incomplete = bool(
        weak_count > int(cfg.research_pv22_max_structure_weak_count)
        or (ema9_ema20_gap_pct <= 0.0 and (ema20_slope_pct <= 0.0 or p_v2_structure_score < 14.0))
    )
    hard_ok = bool(h1_up and movement_ok and not_extreme and live_gain <= float(cfg.research_live_cap_060_pct))
    p_v22 = bool(
        bool(cfg.research_shadow_enabled) and hard_ok
        and p_v2_score >= float(cfg.research_pv22_total_min)
        and persistence >= float(cfg.research_pv22_persistence_min)
        and signal_pass >= int(cfg.research_pv22_signal_pass_min)
        and not structure_incomplete
        and not late_extension
    )

    out = {
        "price": price,
        "live_price": live_close,
        "p_v22_candidate": p_v22,
        "p_v2_score": p_v2_score,
        "p_v21_persistence_score": persistence,
        "p_v2_signal_pass_count": signal_pass,
        "p_v22_structure_incomplete": structure_incomplete,
        "p_v22_late_extension": late_extension,
        "rsi": rsi_now,
        "rsi_delta": rsi_delta,
        "rsi_prev2": rsi_prev2,
        "rsi_prev3": rsi_prev3,
        "rsi_change_prev1": rsi_change_prev1,
        "ema9_slope_pct": ema9_slope_pct,
        "ema20_slope_pct": ema20_slope_pct,
        "ema9_slope_prev1_pct": ema9_slope_prev1_pct,
        "ema20_slope_prev2_pct": ema20_slope_prev2_pct,
        "ema9_ema20_gap_pct": ema9_ema20_gap_pct,
        "pullback_from_high_pct": pullback_from_high,
        "rebound_from_low_pct": rebound_from_low,
        "volume_ratio": volume_ratio,
        "live_candle_gain_pct": live_gain,
        "quality_ok": quality_ok,
    }
    out.update({k: v for k, v in market.items() if v is not None})
    return out


class HistoricalClient:
    def __init__(self, data: dict[str,SymbolData]):
        self.data = data
        self.now = START_UTC

    def set_now(self, now: datetime):
        self.now = now.astimezone(UTC)

    @staticmethod
    def _dummy_partial(base: pd.DataFrame, bucket: datetime):
        if base.empty:
            return None
        prev = base[base["start_time"] < pd.Timestamp(bucket)]
        if prev.empty:
            return None
        px = float(prev.iloc[-1]["close"])
        return {
            "ts":int(pd.Timestamp(bucket).timestamp()*1000),
            "open":px,"high":px,"low":px,"close":px,"volume":0.0,"turnover":0.0,
            "start_time":pd.Timestamp(bucket),"confirm":"1",
        }

    def candles(self, symbol: str, interval: str="15m", limit: int=200) -> pd.DataFrame:
        sd = self.data[symbol]
        s = str(interval).lower()
        if s in ("5","5m"):
            base=sd.m5
            bucket=self.now.replace(minute=(self.now.minute//5)*5,second=0,microsecond=0)
            closed=base[base["start_time"] < pd.Timestamp(bucket)].copy()
            part=self._dummy_partial(base,bucket)
        elif s in ("15","15m"):
            base=sd.m15
            bucket=self.now.replace(minute=(self.now.minute//15)*15,second=0,microsecond=0)
            closed=base[base["start_time"] < pd.Timestamp(bucket)].copy()
            m=sd.m5
            zz=m[(m["start_time"]>=pd.Timestamp(bucket)) & (m["start_time"]<pd.Timestamp(self.now.replace(second=0,microsecond=0)))].copy()
            if not zz.empty:
                first=zz.iloc[0]; last=zz.iloc[-1]
                part={
                    "ts":int(pd.Timestamp(bucket).timestamp()*1000),
                    "open":float(first["open"]),"high":float(zz["high"].max()),
                    "low":float(zz["low"].min()),"close":float(last["close"]),
                    "volume":float(zz["volume"].sum()),"turnover":float(zz["turnover"].sum()),
                    "start_time":pd.Timestamp(bucket),"confirm":"1",
                }
            else:
                part=self._dummy_partial(base,bucket)
        elif s in ("60","1h"):
            base=sd.h1
            bucket=self.now.replace(minute=0,second=0,microsecond=0)
            closed=base[base["start_time"] < pd.Timestamp(bucket)].copy()
            part=self._dummy_partial(base,bucket)
        else:
            raise ValueError("unsupported interval="+str(interval))

        if not closed.empty:
            closed=closed.copy()
            closed["confirm"]="1"
        frames=[closed]
        if part is not None:
            frames.append(pd.DataFrame([part]))
        out=pd.concat(frames,ignore_index=True) if frames else pd.DataFrame()
        if out.empty:
            return out
        out=out.sort_values("start_time").drop_duplicates("start_time",keep="last")
        cols=[c for c in ["ts","open","high","low","close","volume","turnover","start_time","confirm"] if c in out.columns]
        return out[cols].tail(int(limit)).reset_index(drop=True)


@dataclass
class Watch:
    setup_id: str
    symbol: str
    first_seen: datetime
    trigger_price: float
    lowest_price: float
    last_price: float
    watch_details: dict
    status: str="WATCH"
    last_5m_bucket: str=""
    drop_reason: str=""


def market_snapshot_from_5m(m5map: dict[str,pd.DataFrame], now: datetime) -> dict[str,Any]:
    out={}
    target=pd.Timestamp(now.replace(second=0,microsecond=0))
    for sym,prefix in (("BTCUSDT","btc"),("ETHUSDT","eth")):
        df=m5map.get(sym)
        if df is None or df.empty:
            out[f"{prefix}_15m_change_pct"]=None
            out[f"{prefix}_4h_change_pct"]=None
            continue
        z=df[df["start_time"] < target]
        if len(z)<49:
            out[f"{prefix}_15m_change_pct"]=None
            out[f"{prefix}_4h_change_pct"]=None
            continue
        cur=float(z.iloc[-1]["close"])
        p15=float(z.iloc[-4]["close"])
        p4h=float(z.iloc[-49]["close"])
        out[f"{prefix}_15m_change_pct"]=(cur/p15-1)*100 if p15>0 else None
        out[f"{prefix}_4h_change_pct"]=(cur/p4h-1)*100 if p4h>0 else None
    return out


def generate_month(root:Path, work:Path, cache:HistCache, BOT, cfg,
                   inst_rows:list[dict], mstart_kst:datetime, mend_kst:datetime,
                   state:StateFile, force=False):
    """Memory-bounded month replay. Strategy logic/thresholds are unchanged.

    Key change vs v1: do not keep 5m/15m/60m frames for every working symbol in RAM.
    Universe metrics are built first; V22/V25 replay then loads only currently selected
    symbols through a small LRU. Completed-month checkpoints remain compatible with v1.
    """
    mk=month_key(mstart_kst)
    done=work/f"{mk}.done"
    cand_file=work/f"{mk}_CANDIDATES.csv.gz"
    uni_file=work/f"{mk}_UNIVERSE.csv.gz"
    meta_file=work/f"{mk}_META.json"
    if done.exists() and cand_file.exists() and uni_file.exists() and not force:
        pflush(f"[{mk}] checkpoint exists -> skip generation")
        return

    state.update(phase="candidate_generation",month=mk)
    pflush(f"\n=== {mk} CANDIDATE GENERATION (LOWMEM) ===")

    mstart=mstart_kst.astimezone(UTC)
    mend=mend_kst.astimezone(UTC)
    warm=mstart-timedelta(days=UNIVERSE_WARM_DAYS)
    scan_start=mstart-timedelta(minutes=WATCH_WARM_MIN)

    blocked_bases={str(x).upper().replace("-","").replace("_","") for x in cfg.non_crypto_base_exclusions}
    excluded=set(str(x) for x in cfg.slow_symbol_exclusions)

    active=[]
    for r in inst_rows:
        sym=str(r.get("symbol") or "")
        if not sym or not sym.upper().endswith("USDT"):
            continue
        if sym in excluded or noncrypto_match(sym,blocked_bases):
            continue
        settle=str(r.get("settleCoin") or "USDT")
        if settle and settle!="USDT":
            continue
        if active_in_window(r, warm, mend):
            active.append(sym)
    active=sorted(set(active)|set(map(str,cfg.symbols))|{"BTCUSDT","ETHUSDT"})
    pflush(f"[{mk}] instrument symbols={len(active)}")

    by_hour=defaultdict(list)
    hourly_errors=[]
    for i,sym in enumerate(active,1):
        try:
            df=cache.get(sym,"60",warm-timedelta(days=1),mend)
            loose=loose_hourly_candidates(df,cfg)
            for tt,score in zip(loose["time"],loose["score"]):
                by_hour[pd.Timestamp(tt).floor("h")].append((float(score),sym))
        except Exception as e:
            hourly_errors.append({"symbol":sym,"phase":"60m_prefilter","error":str(e)})
        if i%25==0 or i==len(active):
            pflush(f"[{mk}] hourly prefilter {i}/{len(active)} errors={len(hourly_errors)}")

    topn=max(int(cfg.top_gainers_pool_size)+30, HOURLY_PREFILTER_MIN_TOP)
    month_syms=set(map(str,cfg.symbols))|{"BTCUSDT","ETHUSDT"}
    for arr in by_hour.values():
        arr.sort(reverse=True)
        month_syms.update(sym for _,sym in arr[:topn])
    month_syms=sorted(month_syms)
    pflush(f"[{mk}] 15m/5m working symbols={len(month_syms)}")
    del by_hour
    cache.clear_mem()

    # Validate all three timeframes exactly as v1 did, but retain only 15m universe metrics.
    metrics={}
    available=set()
    fetch_errors=list(hourly_errors)
    for i,sym in enumerate(month_syms,1):
        try:
            d15=cache.get(sym,"15",warm,mend)
            d5=cache.get(sym,"5",warm,mend+timedelta(hours=4))
            d60=cache.get(sym,"60",warm-timedelta(days=1),mend)
            if d15.empty or d5.empty or d60.empty:
                raise RuntimeError(f"empty 5={len(d5)} 15={len(d15)} 60={len(d60)}")
            metrics[sym]=prep_15_metrics(d15,cfg)
            available.add(sym)
            # Let raw frames fall out of the bounded HistCache; do not keep SymbolData yet.
            del d15, d5, d60
        except Exception as e:
            fetch_errors.append({"symbol":sym,"phase":"5m15m_load","error":str(e)})
        if i%10==0 or i==len(month_syms):
            pflush(f"[{mk}] candles {i}/{len(month_syms)} ok={len(available)} errors={len(fetch_errors)}")
        if i%50==0:
            gc.collect()

    if "BTCUSDT" not in available or "ETHUSDT" not in available:
        raise RuntimeError(f"[{mk}] BTC/ETH data missing")

    universe={}
    uni_rows=[]
    default_syms=[s for s in cfg.symbols if s in available]
    t=scan_start.replace(minute=(scan_start.minute//15)*15,second=0,microsecond=0)
    while t<mend:
        bar_time=pd.Timestamp(t-timedelta(minutes=15))
        ranked_ticker=[]
        for sym,md in metrics.items():
            if bar_time not in md.index:
                continue
            r=md.loc[bar_time]
            if bool(r.get("pass_ticker",False)):
                ranked_ticker.append((float(r["ticker_score"]),sym,r))
        ranked_ticker.sort(reverse=True,key=lambda x:x[0])
        pre=ranked_ticker[:max(int(cfg.top_gainers_pool_size),int(cfg.universe_size))]
        ranked=[]
        for _,sym,r in pre:
            if bool(r.get("pass_recent",False)):
                ranked.append((float(r["final_score"]),sym,r))
        ranked.sort(reverse=True,key=lambda x:x[0])
        selected=[sym for _,sym,_ in ranked[:max(1,int(cfg.universe_size))]]
        if not selected:
            selected=list(default_syms)
        universe[t]=selected
        for rank,sym in enumerate(selected,1):
            uni_rows.append({"time_utc":t.isoformat(),"time_kst":t.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
                             "rank":rank,"symbol":sym})
        t+=timedelta(minutes=15)

    pd.DataFrame(uni_rows).to_csv(uni_file,index=False,compression="gzip",encoding="utf-8-sig")
    pflush(f"[{mk}] universe points={len(universe)} rows={len(uni_rows)}")
    del uni_rows, metrics
    cache.clear_mem()

    # Keep BTC/ETH 5m frames pinned for market telemetry only.
    btc5=cache.get("BTCUSDT","5",warm,mend+timedelta(hours=4))
    eth5=cache.get("ETHUSDT","5",warm,mend+timedelta(hours=4))
    market5={"BTCUSDT":btc5,"ETHUSDT":eth5}

    # Balanced LRU: faster than LOWMEM24, with automatic RSS guard to avoid OOM.
    replay_lru=OrderedDict()
    failed_replay_syms=set()
    mem_band="NORMAL"

    def get_sd(sym):
        nonlocal mem_band
        if sym in replay_lru:
            sd=replay_lru.pop(sym)
            replay_lru[sym]=sd
            return sd
        if sym in failed_replay_syms or sym not in available:
            return None
        try:
            d15=cache.get(sym,"15",warm,mend)
            d5=cache.get(sym,"5",warm,mend+timedelta(hours=4))
            d60=cache.get(sym,"60",warm-timedelta(days=1),mend)
            if d15.empty or d5.empty or d60.empty:
                raise RuntimeError(f"empty 5={len(d5)} 15={len(d15)} 60={len(d60)}")
            sd=SymbolData(d5,BOT.indicators(d15),BOT.indicators(d60))
            replay_lru[sym]=sd

            # Default to 48 recent symbols for speed. If RSS rises, shrink automatically.
            cur_rss=rss_mb()
            if cur_rss >= RSS_HARD_MB:
                target=REPLAY_LRU_HARD
                band="HARD"
            elif cur_rss >= RSS_SOFT_MB:
                target=REPLAY_LRU_SOFT
                band="SOFT"
            else:
                target=REPLAY_SYMBOL_LRU
                band="NORMAL"
            while len(replay_lru)>target:
                replay_lru.popitem(last=False)
            if band != mem_band:
                pflush(f"[{mk}] MEMORY_GUARD {mem_band}->{band} rss={cur_rss:.0f}MB lru_target={target}")
                mem_band=band
            if band != "NORMAL":
                cache.clear_mem()
                gc.collect()
            return sd
        except Exception as e:
            failed_replay_syms.add(sym)
            fetch_errors.append({"symbol":sym,"phase":"lazy_replay_load","error":str(e)})
            return None

    last_setup={}
    candidates=[]
    signal_errors=[]
    scan_steps=0

    t=scan_start.replace(second=0,microsecond=0)
    add=(SCAN_STEP_MIN - (t.minute%SCAN_STEP_MIN))%SCAN_STEP_MIN
    t=t+timedelta(minutes=add)
    while t<mend:
        ukey=t.replace(minute=(t.minute//15)*15,second=0,microsecond=0)
        selected=universe.get(ukey,default_syms)
        now=t+timedelta(seconds=1)
        msnap=market_snapshot_from_5m(market5,now)
        for sym in selected:
            sd=get_sd(sym)
            if sd is None:
                continue
            try:
                d=fast_v22_details(sd,now,cfg,msnap)
                live=float(d.get("live_price") or d.get("price") or 0)
                replay_v22=bool(d.get("p_v22_candidate"))
                bucket=int(now.timestamp())//900
                d["p_setup_id"]=f"P65SET-{sym}-{bucket}"
            except Exception as e:
                signal_errors.append({"time_utc":now.isoformat(),"symbol":sym,"error":str(e)})
                continue

            recent=last_setup.get(sym)
            if recent is not None and recent.first_seen < now-timedelta(minutes=max(1,int(cfg.research_pv25_same_setup_lock_minutes))):
                recent=None

            if recent is None:
                if replay_v22 and live>0:
                    sid=f"P25SET-{sym}-{bucket}"
                    w=Watch(sid,sym,now,live,live,live,dict(d))
                    last_setup[sym]=w
                continue

            if recent.status!="WATCH" or live<=0:
                continue

            recent.lowest_price=min(recent.lowest_price,live)
            recent.last_price=live
            age=(now-recent.first_seen).total_seconds()/60.0
            adverse=(recent.lowest_price/recent.trigger_price-1)*100 if recent.trigger_price>0 else 0
            if age>float(cfg.research_pv25_confirm_max_minutes) or adverse<=-abs(float(cfg.research_pv25_max_adverse_before_confirm_pct)):
                recent.status="DROPPED"; recent.drop_reason="expired_or_adverse"
                continue

            b5=str(int(now.timestamp())//300)
            if recent.last_5m_bucket==b5:
                continue
            recent.last_5m_bucket=b5
            if age<float(cfg.research_pv25_confirm_min_minutes):
                continue
            try:
                d5=sd.m5
                floor5=now.replace(minute=(now.minute//5)*5,second=0,microsecond=0)
                pos=int(d5["start_time"].searchsorted(pd.Timestamp(floor5),side="left"))
                if pos<2: continue
                row5,prev5=d5.iloc[pos-1],d5.iloc[pos-2]
                bullish=float(row5["close"])>float(row5["open"])
                high_break=float(row5["close"])>float(prev5["high"])
            except Exception as e:
                signal_errors.append({"time_utc":now.isoformat(),"symbol":sym,"error":"V25_5M:"+str(e)})
                continue
            if bullish and high_break:
                recent.status="CONFIRMED"
                candidates.append({
                    "setup_id":recent.setup_id,"symbol":sym,
                    "first_seen_utc":recent.first_seen.isoformat(),
                    "first_seen_kst":recent.first_seen.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "entry_time_utc":now.isoformat(),
                    "entry_time_kst":now.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "entry_price":live,
                    "trigger_price":recent.trigger_price,
                    "lowest_price_before_confirm":recent.lowest_price,
                    "watch_age_min":round(age,3),
                    "watch_details_json":jdumps(recent.watch_details),
                    "details_json":jdumps(d),
                    "watch_p_v2_score":fnum(recent.watch_details.get("p_v2_score")),
                    "watch_ema_gap":fnum(recent.watch_details.get("ema9_ema20_gap_pct")),
                    "watch_rebound":fnum(recent.watch_details.get("rebound_from_low_pct")),
                    "watch_rsi_delta":fnum(recent.watch_details.get("rsi_delta")),
                    "watch_btc15":fnum(recent.watch_details.get("btc_15m_change_pct")),
                })

        scan_steps+=1
        if scan_steps%REPLAY_LOG_EVERY==0:
            pflush(f"[{mk}] replay steps={scan_steps} candidates={len(candidates)} errors={len(signal_errors)} lru={len(replay_lru)} rss={rss_mb():.0f}MB")
            state.update(phase="candidate_generation",month=mk,scan_steps=scan_steps,candidates=len(candidates))
        if scan_steps%1000==0:
            gc.collect()
        t+=timedelta(minutes=SCAN_STEP_MIN)

    candidate_columns=[
        "setup_id","symbol","first_seen_utc","first_seen_kst","entry_time_utc","entry_time_kst",
        "entry_price","trigger_price","lowest_price_before_confirm","watch_age_min",
        "watch_details_json","details_json","watch_p_v2_score","watch_ema_gap","watch_rebound",
        "watch_rsi_delta","watch_btc15"
    ]
    cdf=pd.DataFrame(candidates,columns=candidate_columns)
    if not cdf.empty:
        et=pd.to_datetime(cdf["entry_time_utc"],utc=True)
        cdf=cdf[(et>=pd.Timestamp(mstart))&(et<pd.Timestamp(mend))].copy()
        cdf=cdf.sort_values(["entry_time_utc","setup_id"]).drop_duplicates("setup_id",keep="first")
    cdf.to_csv(cand_file,index=False,compression="gzip",encoding="utf-8-sig")

    meta={
        "month":mk,"candidate_count":len(cdf),"universe_rows":sum(len(v) for v in universe.values()),
        "instrument_count":len(active),"working_symbols":len(month_syms),
        "fetch_errors":fetch_errors,"signal_errors":signal_errors,
        "execution_mode":"BALANCED_LRU48_AUTO_GUARD_SAME_LOGIC",
        "approximation":{
            "candidate_scan_cadence_min":SCAN_STEP_MIN,
            "dynamic_universe_cadence_min":15,
            "historical_spread_assumption_pct":0,
            "watch_warm_min":WATCH_WARM_MIN,
        }
    }
    meta_file.write_text(json.dumps(meta,ensure_ascii=False,indent=2,default=json_default),encoding="utf-8")
    done.write_text(datetime.now(KST).isoformat(),encoding="utf-8")
    pflush(f"[{mk}] DONE candidates={len(cdf)} fetch_errors={len(fetch_errors)} signal_errors={len(signal_errors)}")
    replay_lru.clear(); cache.clear_mem(); gc.collect()

def load_candidates(work:Path) -> pd.DataFrame:
    parts=[]
    for mstart,_ in month_ranges():
        p=work/f"{month_key(mstart)}_CANDIDATES.csv.gz"
        if not p.exists():
            raise RuntimeError(f"missing candidate checkpoint {p}")
        try:
            df=pd.read_csv(p,compression="gzip",low_memory=False)
        except pd.errors.EmptyDataError:
            df=pd.DataFrame()
        if not df.empty: parts.append(df)
    if not parts:
        return pd.DataFrame()
    df=pd.concat(parts,ignore_index=True)
    df["entry_dt"]=pd.to_datetime(df["entry_time_utc"],utc=True)
    return df.sort_values(["entry_dt","setup_id"]).drop_duplicates("setup_id",keep="first").reset_index(drop=True)


class PerfSwitch:
    def __init__(self,events):
        self.events=sorted(events,key=lambda x:(x["exit_time"],x["setup_id"]))
        self.i=0; self.q=deque(maxlen=REGIME_N); self.on=True; self.transitions=[]
    def snapshot(self,now):
        while self.i<len(self.events) and self.events[self.i]["exit_time"]<=now:
            e=self.events[self.i]; self.i+=1
            self.q.append(float(e["net_pct"]))
            if len(self.q)<REGIME_N: continue
            s=sum(self.q); old=self.on
            if self.on and s>=REGIME_OFF_AT: self.on=False
            elif (not self.on) and s<=REGIME_ON_AT: self.on=True
            if self.on!=old:
                self.transitions.append({
                    "time_kst":e["exit_time"].astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "new_state":"ON" if self.on else "OFF","rolling_n":len(self.q),
                    "rolling_sum":round(s,6),"trigger_setup_id":e["setup_id"],"trigger_symbol":e["symbol"],
                })
        return self.on,len(self.q),(sum(self.q) if self.q else None)


def load_sim_checkpoint(U, path:Path):
    out={}
    if not path.exists() or path.stat().st_size<30:
        return out
    df=pd.read_csv(path,compression="gzip",low_memory=False)
    for r in df.to_dict("records"):
        sid=str(r.get("setup_id") or "")
        if not sid: continue
        try:
            et=datetime.fromisoformat(str(r.get("exit_time_utc")).replace("Z","+00:00"))
            if et.tzinfo is None: et=et.replace(tzinfo=UTC)
            et=et.astimezone(UTC)
        except Exception:
            continue
        out[sid]=U.SimResult(
            result=str(r.get("result") or ""),exit_time=et,
            terminal_price=fnum(r.get("terminal_price"),0) or 0,fills=[],
            gross_pct=fnum(r.get("gross_pct"),0) or 0,fee_pct=fnum(r.get("fee_pct"),0) or 0,
            net_pct=fnum(r.get("net_pct"),0) or 0,mfe_pct=fnum(r.get("mfe_pct"),0) or 0,
            mae_pct=fnum(r.get("mae_pct"),0) or 0,stop_stage=str(r.get("stop_stage") or ""),
            detail="CHECKPOINT",data_error="" if str(r.get("data_error") or "").lower()=="nan" else str(r.get("data_error") or ""),
        )
    return out


def flush_sim_checkpoint(path:Path, cache:dict):
    rows=[]
    for sid,s in cache.items():
        rows.append({
            "setup_id":sid,"result":s.result,"exit_time_utc":s.exit_time.isoformat(),
            "terminal_price":s.terminal_price,"gross_pct":s.gross_pct,"fee_pct":s.fee_pct,
            "net_pct":s.net_pct,"mfe_pct":s.mfe_pct,"mae_pct":s.mae_pct,
            "stop_stage":s.stop_stage,"data_error":s.data_error,
        })
    tmp=path.with_suffix(path.suffix+".tmp")
    pd.DataFrame(rows).to_csv(tmp,index=False,compression="gzip",encoding="utf-8-sig")
    tmp.replace(path)


def run_final(root:Path,work:Path,U,cand:pd.DataFrame,state:StateFile):
    if cand.empty:
        raise RuntimeError("no Jan-Aug candidates")

    state.update(phase="final_backtest",candidate_count=len(cand))
    pflush(f"\n=== FINAL BACKTEST candidates={len(cand)} ===")

    U.START_KST=START_KST; U.END_KST=END_KST
    U.START_UTC=START_UTC; U.END_UTC=END_UTC
    U.EXPECTED_V25=-1
    U.CACHE_DIR=root/".janaug_exit_cache_v1"
    U.CACHE_DIR.mkdir(exist_ok=True)
    U.KC=U.KlineCache()

    rows=[]
    for r in cand.to_dict("records"):
        dt=pd.Timestamp(r["entry_dt"]).to_pydatetime().astimezone(UTC)
        rows.append({
            "setup_id":str(r["setup_id"]),"symbol":str(r["symbol"]),"entry":dt,
            "entry_price":float(r["entry_price"]),"details":jloads(r.get("details_json")),
            "watch":jloads(r.get("watch_details_json")),"raw":r,
        })
    rows.sort(key=lambda x:x["entry"])

    sim_path=work/"JAN_AUG_SIM_RESULTS.csv.gz"
    sim_cache=load_sim_checkpoint(U,sim_path)
    new_sim=0

    def get_sim(st):
        nonlocal new_sim
        sid=st["setup_id"]
        if sid not in sim_cache:
            sim_cache[sid]=U.simulate_base(st)
            new_sim+=1
            # unified_current KlineCache is unbounded; cap its RAM footprint during long Jan-Aug replay.
            try:
                if hasattr(U, "KC") and hasattr(U.KC, "mem") and len(U.KC.mem) > EXIT_KLINE_MEM_MAX:
                    U.KC.mem.clear()
                    gc.collect()
            except Exception:
                pass
            if new_sim%FLUSH_SIM_EVERY==0:
                flush_sim_checkpoint(sim_path,sim_cache)
                state.update(phase="final_backtest",sim_cached=len(sim_cache))
                pflush(f"[SIM] cached={len(sim_cache)}/{len(rows)}")
        return sim_cache[sid]

    q=deque(); market_meta={}
    for st in rows:
        t=st["entry"]
        while q and q[0]<t-timedelta(hours=2): q.popleft()
        q.append(t)
        d=st["details"]
        b4=fnum(d.get("btc_4h_change_pct")); e4=fnum(d.get("eth_4h_change_pct"))
        av=None if b4 is None or e4 is None else (abs(b4)+abs(e4))/2
        market_meta[st["setup_id"]]={
            "v25_2h_count":len(q),"btc4h":b4,"eth4h":e4,"abs4h_avg":av,
            "risk":bool(len(q)>=MARKET_V25_2H_MIN and av is not None and av>=MARKET_ABS4H_AVG_MIN),
        }

    U.STOP_PAUSE_WINDOW_MIN=REFERENCE_STOP_PAUSE_MIN
    ctl_sched=U.Scheduler()
    controls=[]
    for i,st in enumerate(rows,1):
        ok,_=ctl_sched.can_open(st["entry"],st["symbol"])
        if not ok: continue
        sim=get_sim(st)
        ctl_sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        sm=U.safe_meta(st["details"]); mm=U.micro_meta(st["details"])
        controls.append(U.ControlObs(
            key=st["setup_id"],source="JAN_AUG_CURRENT_EXIT_PROXY",variant="CURRENT_BASE_PROXY",
            result_ts=sim.exit_time,opened_at=st["entry"],safe_block=bool(sm["block"]),
            micro_available=bool(mm["available"]),micro_risk=bool(mm["risk"]),
            positive=bool(sim.terminal_price>st["entry_price"]),
        ))
        if i%500==0: pflush(f"[CONTROL] {i}/{len(rows)} obs={len(controls)}")
    controls.sort(key=lambda x:x.result_ts)

    ref_sched=U.Scheduler()
    ref_events=[]
    for i,st in enumerate(rows,1):
        ef=U.entry_filter(st,controls)
        if not ef["pass"]: continue
        ok,_=ref_sched.can_open(st["entry"],st["symbol"])
        if not ok: continue
        sim=get_sim(st)
        ref_sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        if market_meta[st["setup_id"]]["risk"]:
            ref_events.append({
                "setup_id":st["setup_id"],"symbol":st["symbol"],"entry_time":st["entry"],
                "exit_time":sim.exit_time,"net_pct":float(sim.net_pct),
            })
        if i%500==0: pflush(f"[BASE_REF] {i}/{len(rows)} events={len(ref_events)}")

    U.STOP_PAUSE_WINDOW_MIN=FINAL_STOP_PAUSE_MIN
    sched=U.Scheduler()
    sw=PerfSwitch(ref_events)
    out=[]; trades=[]; blocks=[]; stops=[]

    for i,st in enumerate(rows,1):
        sid=st["setup_id"]; w=st["watch"]; mm=market_meta[sid]
        ef=U.entry_filter(st,controls)
        ron,rn,rsum=sw.snapshot(st["entry"])

        score=fnum(w.get("p_v2_score"),fnum(st["raw"].get("watch_p_v2_score")))
        gap=fnum(w.get("ema9_ema20_gap_pct"),fnum(st["raw"].get("watch_ema_gap")))
        reb=fnum(w.get("rebound_from_low_pct"),fnum(st["raw"].get("watch_rebound")))
        rd=fnum(w.get("rsi_delta"),fnum(st["raw"].get("watch_rsi_delta")))
        b15=fnum(w.get("btc_15m_change_pct"),fnum(st["raw"].get("watch_btc15")))
        over=bool(score is not None and gap is not None and score>=90.0 and gap>=1.20)
        weak=bool(reb is not None and rd is not None and b15 is not None and reb<=5.0 and rd<=7.0 and b15>=-0.08)
        v22q=bool(over or weak)

        row={
            "setup_id":sid,"symbol":st["symbol"],
            "entry_time_kst":st["entry"].astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
            "entry_time_utc":st["entry"].isoformat(),"entry_price":st["entry_price"],
            "safe_block":int(bool(ef.get("safe",{}).get("block"))),
            "safe_relaxed":int(bool(ef.get("safe_relaxed"))),
            "mkt100_block":int(bool(ef.get("mkt",{}).get("block"))),
            "market_condition":int(mm["risk"]),"market_regime_on":int(ron),
            "market_shadow_n":rn,"market_shadow_sum":"" if rsum is None else round(rsum,6),
            "v25_2h_count":mm["v25_2h_count"],"abs4h_avg":mm["abs4h_avg"],
            "v22q":int(v22q),"v22_overext":int(over),"v22_weak_reaccel":int(weak),
            "accepted":0,"block_reason":"","result":"","exit_time_kst":"","net_pct":"",
            "gross_pct":"","fee_pct":"","mfe_pct":"","mae_pct":"","stop_stage":"","data_error":"",
        }
        if not ef["pass"]:
            row["block_reason"]=ef.get("reason") or "BASE_FILTER"
        elif ron and mm["risk"]:
            row["block_reason"]="MARKET_GUARD_ACTIVE"
        elif v22q:
            row["block_reason"]="V22_QUALITY_OR"
        else:
            ok,why=sched.can_open(st["entry"],st["symbol"])
            if not ok:
                row["block_reason"]="STOP_PAUSE45" if why=="STOP_PAUSE30" else why
            else:
                sim=get_sim(st)
                sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
                row.update({
                    "accepted":1,"result":sim.result,
                    "exit_time_kst":sim.exit_time.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "net_pct":round(float(sim.net_pct),6),"gross_pct":round(float(sim.gross_pct),6),
                    "fee_pct":round(float(sim.fee_pct),6),"mfe_pct":round(float(sim.mfe_pct),6),
                    "mae_pct":round(float(sim.mae_pct),6),"stop_stage":sim.stop_stage,
                    "data_error":sim.data_error or "",
                })
                trades.append(row.copy())
                if sim.result=="STOP": stops.append(row.copy())
        if not row["accepted"]: blocks.append(row.copy())
        out.append(row)
        if i%250==0:
            pflush(f"[FINAL] {i}/{len(rows)} entries={len(trades)} STOP={len(stops)}")
            state.update(phase="final_backtest",final_i=i,entries=len(trades),stops=len(stops),sim_cached=len(sim_cache))

    flush_sim_checkpoint(sim_path,sim_cache)
    return out,trades,blocks,stops,sw.transitions,controls,ref_events


def write_outputs(root:Path,work:Path,cand:pd.DataFrame,out,trades,blocks,stops,transitions,controls,ref_events,state:StateFile,source_hashes,inst_warnings):
    stamp=datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    pref=f"CURRENT_P_JANAUG_2026_{stamp}"
    cand_out=root/f"{pref}_CANDIDATES.csv.gz"
    tr_out=root/f"{pref}_TRADES.csv"
    bl_out=root/f"{pref}_BLOCKS.csv.gz"
    stop_out=root/f"{pref}_STOP_ONLY.csv"
    day_out=root/f"{pref}_DAILY.csv"
    mon_out=root/f"{pref}_MONTHLY.csv"
    trans_out=root/f"{pref}_REGIME_TRANSITIONS.csv"
    sum_out=root/f"{pref}_SUMMARY.txt"
    notes_out=root/f"{pref}_APPROXIMATION_NOTES.txt"
    zip_out=root/f"{pref}_RESULTS.zip"

    cand.drop(columns=["entry_dt"],errors="ignore").to_csv(cand_out,index=False,compression="gzip",encoding="utf-8-sig")
    pd.DataFrame(trades).to_csv(tr_out,index=False,encoding="utf-8-sig")
    pd.DataFrame(blocks).to_csv(bl_out,index=False,compression="gzip",encoding="utf-8-sig")
    pd.DataFrame(stops).to_csv(stop_out,index=False,encoding="utf-8-sig")
    pd.DataFrame(transitions).to_csv(trans_out,index=False,encoding="utf-8-sig")

    df=pd.DataFrame(out)
    df["date"]=df["entry_time_kst"].astype(str).str[:10]
    df["month"]=df["entry_time_kst"].astype(str).str[:7]
    daily=[]; monthly=[]
    for d,g in df.groupby("date",sort=True):
        a=g[g["accepted"]==1]
        rc=Counter(a["result"].astype(str))
        bc=Counter(g[g["accepted"]==0]["block_reason"].astype(str))
        daily.append({
            "date":d,"v25_candidates":len(g),"entries":len(a),
            "net_pct":round(pd.to_numeric(a["net_pct"],errors="coerce").fillna(0).sum(),6),
            "TP20_FULL":rc["TP20_FULL"],"PROFIT_PROTECT_EXIT":rc["PROFIT_PROTECT_EXIT"],
            "STOP":rc["STOP"],"LATE_FAILURE_EXIT":rc["LATE_FAILURE_EXIT"],"TIME_EXIT":rc["TIME_EXIT"],
            "SAFE_BLOCK":bc["SAFE"],"SAFE_MICRO_BLOCK":bc["SAFE_MICRO"],"MKT100_BLOCK":bc["MKT100"],
            "MARKET_GUARD_BLOCK":bc["MARKET_GUARD_ACTIVE"],"V22_BLOCK":bc["V22_QUALITY_OR"],
            "CAP15_BLOCK":bc["CAP15_2"],"SLOT4_BLOCK":bc["SLOT4"],
            "COOLDOWN90_BLOCK":bc["COOLDOWN90"],"COOLDOWN180_BLOCK":bc["COOLDOWN180"],
            "STOP_PAUSE45_BLOCK":bc["STOP_PAUSE45"],
        })
    for m,g in df.groupby("month",sort=True):
        a=g[g["accepted"]==1]
        rc=Counter(a["result"].astype(str))
        monthly.append({
            "month":m,"v25_candidates":len(g),"entries":len(a),
            "net_pct":round(pd.to_numeric(a["net_pct"],errors="coerce").fillna(0).sum(),6),
            "TP20_FULL":rc["TP20_FULL"],"PROFIT_PROTECT_EXIT":rc["PROFIT_PROTECT_EXIT"],
            "STOP":rc["STOP"],"LATE_FAILURE_EXIT":rc["LATE_FAILURE_EXIT"],"TIME_EXIT":rc["TIME_EXIT"],
        })
    pd.DataFrame(daily).to_csv(day_out,index=False,encoding="utf-8-sig")
    pd.DataFrame(monthly).to_csv(mon_out,index=False,encoding="utf-8-sig")

    net=sum(float(r["net_pct"]) for r in trades if str(r.get("net_pct")) not in ("","nan"))
    outcomes=Counter(r["result"] for r in trades)
    blockc=Counter(r["block_reason"] for r in blocks)
    derr=sum(1 for r in trades if r.get("data_error") or r.get("result")=="DATA_ERROR")

    notes=[
        "JAN-AUG 2026 HISTORICAL APPROXIMATION NOTES",
        "",
        "Calibration reference (9/28): V22 recall/precision ~96%; exact V25 setup-ID reproduction 73/82 = 89%.",
        "Jan-Aug uses a 5-minute candidate scan cadence to reduce runtime; live bot scanned more frequently.",
        "Dynamic universe is rebuilt from historical klines. Historical bid/ask spread is unavailable, so spread=0 is assumed.",
        "Hourly loose prefilter is used only to reduce symbols requiring 5m/15m downloads; final universe ranking uses 15m rolling metrics.",
        "P_V25 adverse/expiry and same-setup lock are replayed causally, but intraminute live-price paths cannot be exact from completed bars.",
        "SAFE/RELAX/MKT100, market N9 +9/-6, V22 quality LOCK0, slot/cooldown/pause and current base exits are applied causally.",
        "Jan-Aug has no historical P_FWD_CONTROL stream; RELAX uses a causal CURRENT_BASE exit proxy. This is an approximation.",
        "Net %p includes the same taker-fee model as current unified replay; funding/slippage are not included.",
        "No Recovery/DCA/Observer is applied.",
    ]
    notes_out.write_text("\n".join(notes)+"\n",encoding="utf-8")

    lines=[
        "CURRENT P JAN-AUG 2026 HISTORICAL BACKTEST",
        "STATUS=APPROXIMATE_HISTORICAL_REPLAY",
        "",
        "[FIXED STACK]",
        "historical dynamic-universe approximation -> V22 -> V25",
        "-> SAFE(C/RN/RS)+RELAX proxy -> MKT100",
        "-> market risk V25_2h>=8 & abs4h_avg>=0.40, performance switch N9 +9/-6",
        "-> V22 quality OR LOCK0",
        "-> 4 slots / 15m max2 / CD90 / STOP-CD180 / STOP-pause45",
        "-> TP2.0 / PP12 / Final4 / V27-1",
        "-> Recovery/DCA NOT APPLIED",
        "",
        "[TOTAL]",
        f"V25 candidates={len(cand)}",
        f"entries={len(trades)} blocks={len(blocks)} strict_STOP={len(stops)}",
        f"net={net:+.6f}%p",
        f"outcomes={dict(outcomes)}",
        f"block_counts={dict(blockc)}",
        f"data_errors={derr}",
        f"RELAX control proxy observations={len(controls)}",
        f"market BASE_REF events={len(ref_events)}",
        f"regime transitions={len(transitions)}",
        "",
        "[MONTHLY]",
    ]
    for r in monthly:
        lines.append(
            f"{r['month']} candidates={r['v25_candidates']} entries={r['entries']} "
            f"TP={r['TP20_FULL']} PP={r['PROFIT_PROTECT_EXIT']} STOP={r['STOP']} "
            f"LATE={r['LATE_FAILURE_EXIT']} TIME={r['TIME_EXIT']} NET={r['net_pct']:+.6f}%p"
        )
    lines += ["","[SOURCE SHA256]"]+[f"{k}={v}" for k,v in source_hashes.items()]
    if inst_warnings:
        lines += ["","[INSTRUMENT API WARNINGS]"]+list(map(str,inst_warnings))
    lines += ["","[IMPORTANT]","Interpret monthly direction/robustness first; do not treat this as exact live P&L.",
              "See APPROXIMATION_NOTES for known reconstruction limits."]
    sum_out.write_text("\n".join(lines)+"\n",encoding="utf-8")

    state.update(phase="done",result_zip=str(zip_out),net_pct=net,entries=len(trades),stops=len(stops))

    members=[cand_out,tr_out,bl_out,stop_out,day_out,mon_out,trans_out,sum_out,notes_out,work/"STATE.json",work/"JAN_AUG_SIM_RESULTS.csv.gz"]
    for mstart,_ in month_ranges():
        mk=month_key(mstart)
        for suffix in ("_META.json","_UNIVERSE.csv.gz","_CANDIDATES.csv.gz"):
            p=work/f"{mk}{suffix}"
            if p.exists(): members.append(p)
    with zipfile.ZipFile(zip_out,"w",zipfile.ZIP_DEFLATED) as z:
        for p in members:
            if p.exists():
                z.write(p,arcname=p.name)

    pflush("\n".join(lines))
    pflush("RESULT_ZIP="+str(zip_out))


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--root",default="/root/hyejin-trader/bybit_swing")
    ap.add_argument("--resume",action="store_true",default=True)
    ap.add_argument("--force-month",default="",help="e.g. 2026-03 to regenerate one month")
    args=ap.parse_args()

    root=Path(args.root).expanduser().resolve()
    bot_path=root/"bot.py"
    unified_path=root/"unified_current_0901_0922.py"
    if not bot_path.exists() or not unified_path.exists():
        raise SystemExit(f"MISSING bot/unified source under {root}")

    for p in (str(root.parent),str(root)):
        if p not in sys.path: sys.path.insert(0,p)

    BOT=load_module("BOT_JANAUG_HIST",bot_path)
    cfg=BOT.DailyConfig.load()
    cfg.research_shadow_enabled=True
    U=load_module("U_JANAUG_FIXED",unified_path)

    work=root/"JANAUG_FIXED_2026_WORK"
    work.mkdir(exist_ok=True)
    state=StateFile(work/"STATE.json")
    state.update(phase="start",bot_runtime=getattr(BOT,"BOT_RUNTIME_VERSION",""),
                 bot_sha256=sha256_file(bot_path),unified_sha256=sha256_file(unified_path))

    payload=instrument_payload(work/"INSTRUMENTS.json")
    inst_rows=payload.get("rows",[])
    inst_warnings=payload.get("warnings",[])
    if not inst_rows:
        raise SystemExit("NO INSTRUMENTS. Cannot reconstruct historical universe.")
    pflush(f"INSTRUMENTS={len(inst_rows)} warnings={len(inst_warnings)}")

    cache=HistCache(root/".janaug_hist_cache_v1")
    for mstart,mend in month_ranges():
        generate_month(root,work,cache,BOT,cfg,inst_rows,mstart,mend,state,
                       force=(args.force_month==month_key(mstart)))

    cand=load_candidates(work)
    pflush(f"\nALL JAN-AUG V25 CANDIDATES={len(cand)}")
    out,trades,blocks,stops,transitions,controls,ref_events=run_final(root,work,U,cand,state)

    source_hashes={"bot.py":sha256_file(bot_path),"unified_current_0901_0922.py":sha256_file(unified_path)}
    write_outputs(root,work,cand,out,trades,blocks,stops,transitions,controls,ref_events,state,source_hashes,inst_warnings)


if __name__=="__main__":
    main()
