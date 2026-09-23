#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UNIFIED CURRENT P BACKTEST — 2026-09-01 ~ 2026-09-22 KST

BASE
  V25 confirmed -> SAFE(C/RN/RS) -> SAFE RELAX(14h & 18h) -> MKT100
  -> 4 slots / rolling 15m max2 / same-symbol cooldowns / recent STOP pause
  -> TP +2.0% FULL -> PP12 -> Final 4-stage stop -> V27-1 fallback
  -> taker fee 0.055% entry + exits

R100 comparison
  Replays only BASE-accepted rows. BASE rows whose terminal result == STOP are
  replaced by R100 + MAX2_EXIT recovery; other BASE rows retain BASE exits.
  Old CUT3/CUT6 guard experiments are OFF by default on purpose.

Outputs are written next to this script.
"""
from __future__ import annotations

import csv
import glob
import importlib.util
import json
import math
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from bisect import bisect_right
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime as RealDateTime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent
# bot.py imports bybit_swing.bybit_api; make the project parent importable even when this script is run from inside bybit_swing/.
for _p in (str(ROOT.parent), str(ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)
DB_PATH = ROOT / "bybit_swing_bot.db"
CACHE_DIR = ROOT / ".unified_kline_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

OUT_BASE_DAILY = ROOT / "UNIFIED_CURRENT_0901_0922_DAILY.csv"
OUT_BASE_TRADES = ROOT / "UNIFIED_CURRENT_0901_0922_TRADES.csv"
OUT_SUMMARY = ROOT / "UNIFIED_CURRENT_0901_0922_SUMMARY.txt"
OUT_R100_DAILY = ROOT / "UNIFIED_R100_MAX2_0901_0922_DAILY.csv"
OUT_R100_TRADES = ROOT / "UNIFIED_R100_MAX2_0901_0922_TRADES.csv"
OUT_COMPARE = ROOT / "UNIFIED_BASE_VS_R100_0901_0922.csv"

KST = timezone(timedelta(hours=9))
UTC = timezone.utc
START_KST = RealDateTime(2026, 9, 1, 0, 0, tzinfo=KST)
END_KST = RealDateTime(2026, 9, 23, 0, 0, tzinfo=KST)  # exclusive
START_UTC = START_KST.astimezone(UTC)
END_UTC = END_KST.astimezone(UTC)
EXPECTED_V25 = 1553
FEE_PCT = 0.055
TP_PCT = 2.0
PP_ARM_MFE = 1.20
PP_CLOSE_PCT = -1.15
SAFE_EDGE_PP = 8.0
SAFE_MIN_N = 5
PASS_MIN_N = 5
RELAX_WINDOWS_H = (14, 18)
MKT_FLAT_ABS_4H = 0.08
MICRO_AVG15_MAX = -0.05
MICRO_GAP_MAX = 1.20

MAX_SLOTS = 4
MAX_15M_ENTRIES = 2
SAME_SYMBOL_CD_MIN = 90
STOP_SYMBOL_CD_MIN = 180
STOP_PAUSE_WINDOW_MIN = 30
STOP_PAUSE_COUNT = 2

# Recovery final requested candidate. Old CUT guard experiments are intentionally OFF.
R100_REBOUND_PCT = 1.50
R100_MAX_ATTEMPTS = 2
R100_MAX_HOURS = 24
R100_OLD_GUARD = False
R100_GUARD_3H_NET = -10.0
R100_GUARD_6H_NET = -7.0

SCRIPT_VERSION = "UNIFIED_CURRENT_0901_0922_v1_20260923"


def dt_utc(v: Any) -> RealDateTime | None:
    if v in (None, ""):
        return None
    try:
        if isinstance(v, RealDateTime):
            d = v
        elif isinstance(v, (int, float)):
            x = float(v)
            if x > 1e12:
                x /= 1000.0
            d = RealDateTime.fromtimestamp(x, tz=UTC)
        else:
            s = str(v).strip().replace("Z", "+00:00")
            d = RealDateTime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except Exception:
        return None


def kst_stamp(d: RealDateTime | None) -> str:
    return "" if d is None else d.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def kst_date(d: RealDateTime | None) -> str:
    return "" if d is None else d.astimezone(KST).strftime("%Y-%m-%d")


def f(v: Any, default: float | None = None) -> float | None:
    try:
        if v in (None, ""):
            return default
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def jload(v: Any) -> dict[str, Any]:
    if isinstance(v, dict):
        return dict(v)
    if not v:
        return {}
    try:
        x = json.loads(str(v))
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def first_f(d: dict[str, Any], *keys: str) -> float | None:
    for k in keys:
        x = f(d.get(k))
        if x is not None:
            return x
    return None


def safe_meta(d: dict[str, Any]) -> dict[str, Any]:
    rsi = first_f(d, "rsi")
    e20p2 = first_f(d, "ema20_slope_prev2_pct")
    e9p1 = first_f(d, "ema9_slope_prev1_pct")
    pull = first_f(d, "pullback_from_high_pct")
    rchg = first_f(d, "rsi_change_prev1")
    c = bool(rsi is not None and e20p2 is not None and rsi <= 62.46 and e20p2 >= 0.0815)
    rn = bool(e9p1 is not None and pull is not None and e9p1 >= 0.39405 and pull <= -0.472)
    rs = bool(rsi is not None and rchg is not None and rsi >= 66.005 and rchg <= -1.9521)
    flags = [n for n, ok in (("C", c), ("RN", rn), ("RS", rs)) if ok]
    return {"block": bool(flags), "c": c, "rn": rn, "rs": rs, "flags": ",".join(flags)}


def micro_meta(d: dict[str, Any]) -> dict[str, Any]:
    b15 = first_f(d, "btc_15m_change_pct", "btc_15m")
    e15 = first_f(d, "eth_15m_change_pct", "eth_15m")
    gap = first_f(d, "ema9_ema20_gap_pct")
    if b15 is None or e15 is None or gap is None:
        return {"available": False, "risk": False, "avg15": None, "gap": gap}
    avg = (b15 + e15) / 2.0
    return {"available": True, "risk": bool(avg <= MICRO_AVG15_MAX and gap <= MICRO_GAP_MAX), "avg15": avg, "gap": gap}


def mkt100_meta(d: dict[str, Any]) -> dict[str, Any]:
    b4 = first_f(d, "btc_4h_change_pct", "btc_4h")
    e4 = first_f(d, "eth_4h_change_pct", "eth_4h")
    if b4 is None or e4 is None:
        return {"available": False, "block": False, "btc4": b4, "eth4": e4}
    return {"available": True, "block": bool(abs(b4) <= MKT_FLAT_ABS_4H and abs(e4) <= MKT_FLAT_ABS_4H), "btc4": b4, "eth4": e4}


def load_current_bot():
    cands: list[Path] = []
    if (ROOT / "bot.py").exists():
        cands.append(ROOT / "bot.py")
    exact = ROOT / "bot_v4.3.90_ManualPShadow_Cycle05RB03C1.py"
    if exact.exists():
        cands.append(exact)
    for p in sorted(ROOT.glob("bot*.py"), key=lambda x: x.stat().st_mtime, reverse=True):
        if p not in cands and p.name != Path(__file__).name:
            cands.append(p)
    last_err = None
    for p in cands:
        try:
            spec = importlib.util.spec_from_file_location("unified_bot_source", str(p))
            if spec is None or spec.loader is None:
                continue
            m = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = m
            spec.loader.exec_module(m)
            required = [
                "DailyConfig", "early_crash_failure_signal", "p_catastrophic_failure_signal",
                "early_failure_signal", "late_trend_failure_signal", "hj_structure_broken",
                "flat_exit_signal", "pv26_late_failure_signal", "indicators", "confirmed"
            ]
            if all(hasattr(m, x) for x in required):
                return m, p
        except Exception as e:
            last_err = e
    raise RuntimeError(f"current bot source not loadable; last={last_err}")


BOT, BOT_PATH = load_current_bot()
CFG = BOT.DailyConfig.load()
# Force current unified research constants, independent of old Fixed Shadow's 1.8% TP.
try:
    CFG.research_final_tp_pct = TP_PCT
except Exception:
    pass


class FrozenDateTime(RealDateTime):
    current: RealDateTime = START_UTC

    @classmethod
    def now(cls, tz=None):
        x = cls.current
        if tz is None:
            return x.replace(tzinfo=None)
        return x.astimezone(tz)

    @classmethod
    def utcnow(cls):
        return cls.current.astimezone(UTC).replace(tzinfo=None)


# The source bot imports datetime as a class, so this replacement is local to that module.
BOT.datetime = FrozenDateTime


class KlineCache:
    BASE = "https://api.bybit.com/v5/market/kline"

    def __init__(self):
        self.mem: dict[tuple, pd.DataFrame] = {}

    @staticmethod
    def iv_minutes(interval: str | int) -> int:
        s = str(interval).lower()
        return {"1m": 1, "1": 1, "5m": 5, "5": 5, "15m": 15, "15": 15, "1h": 60, "60": 60}.get(s, int(s[:-1]) if s.endswith("m") and s[:-1].isdigit() else 1)

    @staticmethod
    def bybit_interval(interval: str | int) -> str:
        m = KlineCache.iv_minutes(interval)
        return "60" if m == 60 else str(m)

    def _api(self, symbol: str, interval: str | int, start: RealDateTime, end: RealDateTime) -> pd.DataFrame:
        ivm = self.iv_minutes(interval)
        step_ms = ivm * 60_000
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        rows: dict[int, tuple] = {}
        cur = start_ms
        while cur <= end_ms:
            chunk_end = min(end_ms, cur + step_ms * 999)
            params = urllib.parse.urlencode({
                "category": "linear", "symbol": symbol, "interval": self.bybit_interval(interval),
                "start": cur, "end": chunk_end, "limit": 1000,
            })
            url = self.BASE + "?" + params
            err = None
            payload = None
            for attempt in range(5):
                try:
                    req = urllib.request.Request(url, headers={"User-Agent": "HJ-UNIFIED-BT/1.0"})
                    with urllib.request.urlopen(req, timeout=20) as resp:
                        payload = json.loads(resp.read().decode("utf-8"))
                    if int(payload.get("retCode", -1)) != 0:
                        raise RuntimeError(f"Bybit {payload.get('retCode')} {payload.get('retMsg')}")
                    break
                except Exception as e:
                    err = e
                    time.sleep(0.5 * (attempt + 1))
            if payload is None:
                raise RuntimeError(f"Bybit kline failed {symbol} {interval}: {err}")
            data = payload.get("result", {}).get("list", []) or []
            for r in data:
                try:
                    ts = int(r[0])
                    rows[ts] = (ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]))
                except Exception:
                    pass
            cur = chunk_end + step_ms
            time.sleep(0.08)
        if not rows:
            return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"])
        arr = [rows[k] for k in sorted(rows)]
        return pd.DataFrame(arr, columns=["ts_ms", "open", "high", "low", "close", "volume"]).assign(
            ts=lambda x: pd.to_datetime(x["ts_ms"], unit="ms", utc=True)
        )[["ts", "open", "high", "low", "close", "volume"]]

    def _cached_file(self, symbol: str, interval: str | int, start: RealDateTime, end: RealDateTime) -> Path:
        s = ''.join(ch for ch in symbol if ch.isalnum())
        return CACHE_DIR / f"{s}_{self.iv_minutes(interval)}m_{int(start.timestamp())}_{int(end.timestamp())}.csv.gz"

    def get(self, symbol: str, interval: str | int, start: RealDateTime, end: RealDateTime) -> pd.DataFrame:
        start = start.astimezone(UTC).replace(second=0, microsecond=0)
        end = end.astimezone(UTC).replace(second=0, microsecond=0)
        key = (symbol, self.iv_minutes(interval), int(start.timestamp()), int(end.timestamp()))
        if key in self.mem:
            return self.mem[key]
        p = self._cached_file(symbol, interval, start, end)
        if p.exists():
            try:
                df = pd.read_csv(p, compression="gzip")
                df["ts"] = pd.to_datetime(df["ts"], utc=True)
                self.mem[key] = df
                return df
            except Exception:
                try:
                    p.unlink()
                except Exception:
                    pass
        df = self._api(symbol, interval, start, end)
        if len(df):
            tmp = p.with_suffix(p.suffix + ".tmp")
            df.to_csv(tmp, index=False, compression="gzip")
            tmp.replace(p)
        self.mem[key] = df
        return df

    def trade_windows(self, symbol: str, entry: RealDateTime) -> dict[int, pd.DataFrame]:
        e = entry.astimezone(UTC)
        # 1m: 6h-normalized block, 12h range. Max BASE hold=3h, so this safely covers the path.
        h6 = (e.hour // 6) * 6
        b6 = e.replace(hour=h6, minute=0, second=0, microsecond=0)
        d0 = e.replace(hour=0, minute=0, second=0, microsecond=0)
        w1 = self.get(symbol, "1m", b6, b6 + timedelta(hours=12))
        w5 = self.get(symbol, "5m", d0 - timedelta(hours=12), d0 + timedelta(hours=36))
        w15 = self.get(symbol, "15m", d0 - timedelta(hours=36), d0 + timedelta(hours=36))
        return {1: w1, 5: w5, 15: w15}


KC = KlineCache()


class ReplayClient:
    def __init__(self, symbol: str, entry: RealDateTime):
        self.symbol = symbol
        self.now = entry
        self.frames = KC.trade_windows(symbol, entry)

    def set_now(self, now: RealDateTime):
        self.now = now.astimezone(UTC)
        FrozenDateTime.current = self.now

    def candles(self, symbol: str, interval: str, limit: int) -> pd.DataFrame:
        iv = KC.iv_minutes(interval)
        if iv not in self.frames:
            # only defensive; fallback helpers in current P use 5m/15m here.
            e = self.now
            self.frames[iv] = KC.get(symbol, interval, e - timedelta(minutes=iv * (limit + 10)), e + timedelta(minutes=iv * 2))
        df = self.frames[iv]
        bucket_minute = (self.now.minute // iv) * iv if iv < 60 else 0
        if iv < 60:
            bucket = self.now.replace(minute=bucket_minute, second=0, microsecond=0)
        else:
            bucket = self.now.replace(minute=0, second=0, microsecond=0)
        out = df[df["ts"] <= pd.Timestamp(bucket)].tail(int(limit)).copy()
        return out[["open", "high", "low", "close", "volume"]].reset_index(drop=True)

    def last_confirmed(self, interval: int) -> pd.Series | None:
        df = self.frames[interval]
        if interval < 60:
            b = self.now.replace(minute=(self.now.minute // interval) * interval, second=0, microsecond=0)
        else:
            b = self.now.replace(minute=0, second=0, microsecond=0)
        x = df[df["ts"] < pd.Timestamp(b)]
        return None if len(x) == 0 else x.iloc[-1]

    def path_1m(self, entry: RealDateTime, minutes: int = 185) -> pd.DataFrame:
        df = self.frames[1]
        floor_entry = entry.replace(second=0, microsecond=0)
        end = entry + timedelta(minutes=minutes)
        # v4.3.84 principle: do not use the entry minute, because it contains pre-entry price path.
        return df[(df["ts"] > pd.Timestamp(floor_entry)) & (df["ts"] <= pd.Timestamp(end))].copy()


# Market telemetry recompute series; only used when DB snapshot telemetry is missing.
_market_1m: dict[str, pd.DataFrame] = {}
_market_recompute_count = 0


def load_market_series():
    global _market_1m
    mstart = START_UTC - timedelta(hours=5)
    mend = END_UTC + timedelta(minutes=2)
    for sym in ("BTCUSDT", "ETHUSDT"):
        p = CACHE_DIR / f"MARKET_{sym}_1m_20260831_20260923.csv.gz"
        if p.exists():
            df = pd.read_csv(p, compression="gzip")
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
        else:
            df = KC._api(sym, "1m", mstart, mend)
            df.to_csv(p, index=False, compression="gzip")
        _market_1m[sym] = df.sort_values("ts").reset_index(drop=True)


def close_at_or_before(df: pd.DataFrame, t: RealDateTime) -> float | None:
    arr = df["ts"].astype("int64").to_numpy()
    target = int(pd.Timestamp(t).value)
    i = arr.searchsorted(target, side="right") - 1
    if i < 0:
        return None
    return f(df.iloc[int(i)]["close"])


def market_at(t: RealDateTime) -> dict[str, float | None]:
    out: dict[str, float | None] = {}
    # Use the last completed 1m bar to avoid historical look-ahead inside the current minute.
    cur_t = t.astimezone(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
    for sym, prefix in (("BTCUSDT", "btc"), ("ETHUSDT", "eth")):
        df = _market_1m[sym]
        cur = close_at_or_before(df, cur_t)
        p15 = close_at_or_before(df, cur_t - timedelta(minutes=15))
        p4h = close_at_or_before(df, cur_t - timedelta(hours=4))
        out[f"{prefix}_15m_change_pct"] = ((cur / p15 - 1) * 100.0) if cur and p15 else None
        out[f"{prefix}_4h_change_pct"] = ((cur / p4h - 1) * 100.0) if cur and p4h else None
    return out


def fill_missing_market(details: dict[str, Any], t: RealDateTime) -> bool:
    global _market_recompute_count
    names = ("btc_15m_change_pct", "eth_15m_change_pct", "btc_4h_change_pct", "eth_4h_change_pct")
    missing = [k for k in names if f(details.get(k)) is None]
    if not missing:
        return False
    vals = market_at(t)
    for k in missing:
        if vals.get(k) is not None:
            details[k] = vals[k]
    _market_recompute_count += 1
    return True


def db_rows(sql: str, params=()):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def load_setups() -> list[dict[str, Any]]:
    rows = db_rows("SELECT * FROM research_pv25_setups WHERE status='CONFIRMED' ORDER BY confirmed_at")
    out = []
    for r in rows:
        x = dict(r)
        t = dt_utc(x.get("confirmed_at"))
        if t is None or not (START_UTC <= t < END_UTC):
            continue
        ep = f(x.get("confirmed_price"))
        if ep is None or ep <= 0:
            continue
        d = jload(x.get("snapshot_json"))
        recomputed = fill_missing_market(d, t)
        out.append({
            "id": x.get("id"), "setup_id": str(x.get("setup_id") or ""), "symbol": str(x.get("symbol") or ""),
            "entry": t, "entry_price": ep, "details": d, "market_recomputed": recomputed,
        })
    out.sort(key=lambda z: z["entry"])
    return out


@dataclass
class ControlObs:
    key: str
    source: str
    variant: str
    result_ts: RealDateTime
    opened_at: RealDateTime
    safe_block: bool
    micro_available: bool
    micro_risk: bool
    positive: bool


def load_control_proxy() -> list[ControlObs]:
    """Build the historical RELAX CONTROL stream.

    - From the first real P_FWD_CONTROL onward, use only real P_FWD_CONTROL rows.
    - Before P_FWD_CONTROL existed, use P_V27_1 > P_V26 > P_V25 as the requested proxy.
    - De-duplicate variants from the same V25 opportunity by symbol + setup bucket suffix.
    """
    variants = ("P_FWD_CONTROL", "P_V27_1", "P_V26", "P_V25")
    q = """SELECT * FROM research_shadow_reviews
           WHERE completed=1 AND variant IN (?,?,?,?) AND result_ts IS NOT NULL
           ORDER BY result_ts"""
    raw = [dict(x) for x in db_rows(q, variants)]
    actual_opened = [dt_utc(r.get("opened_at")) for r in raw if str(r.get("variant") or "") == "P_FWD_CONTROL"]
    actual_opened = [x for x in actual_opened if x is not None]
    first_actual = min(actual_opened) if actual_opened else None
    pri = {"P_FWD_CONTROL": 4, "P_V27_1": 3, "P_V26": 2, "P_V25": 1}
    best: dict[str, tuple[int, ControlObs]] = {}

    def norm_key(r: dict[str, Any], d: dict[str, Any], op: RealDateTime) -> str:
        sym = str(r.get("symbol") or "")
        setup = str(
            d.get("final_source_setup_id")
            or d.get("p_v25_setup_id")
            or r.get("setup_id")
            or d.get("p_setup_id")
            or d.get("setup_id")
            or ""
        )
        setup = setup.split("|", 1)[0]
        if setup and "-" in setup:
            # P25SET-SYMBOL-BUCKET and P271SET/P26SET clones preserve the last bucket token.
            bucket = setup.rsplit("-", 1)[-1]
            if bucket:
                return f"{sym}|{bucket}"
        return f"{sym}|{op.replace(second=0, microsecond=0).isoformat()}"

    for r in raw:
        rt = dt_utc(r.get("result_ts")); op = dt_utc(r.get("opened_at"))
        if rt is None or op is None:
            continue
        if rt < START_UTC - timedelta(hours=24) or rt >= END_UTC:
            continue
        v = str(r.get("variant") or "")
        # Once real CONTROL exists, proxies would add observations that current RELAX never saw.
        if first_actual is not None and op >= first_actual and v != "P_FWD_CONTROL":
            continue
        d = jload(r.get("snapshot_json"))
        fill_missing_market(d, op)
        sm = safe_meta(d); mm = micro_meta(d)
        micro_avail = bool(mm["available"])
        ep = f(r.get("entry_price")); rp = f(r.get("result_price"))
        if ep is None or rp is None:
            continue
        key = norm_key(r, d, op)
        obs = ControlObs(
            key=key,
            source=("ACTUAL_FWD_CONTROL" if v == "P_FWD_CONTROL" else f"PROXY_{v}"),
            variant=v, result_ts=rt, opened_at=op,
            safe_block=bool(sm["block"]), micro_available=micro_avail,
            micro_risk=bool(mm["risk"]), positive=bool(rp > ep),
        )
        if key not in best or pri.get(v, 0) > best[key][0]:
            best[key] = (pri.get(v, 0), obs)
    return sorted((v[1] for v in best.values()), key=lambda o: o.result_ts)


def relax_at(now: RealDateTime, controls: list[ControlObs]) -> dict[str, Any]:
    stats = {}
    all_on = True
    for wh in RELAX_WINDOWS_H:
        lo = now - timedelta(hours=wh)
        safe = []
        pas = []
        for o in controls:
            if o.result_ts > now:
                break
            if o.result_ts < lo:
                continue
            if o.safe_block:
                if not o.micro_available:
                    continue
                if not o.micro_risk:
                    safe.append(o.positive)
            else:
                pas.append(o.positive)
        sn, pn = len(safe), len(pas)
        sr = 100.0 * sum(safe) / sn if sn else None
        pr = 100.0 * sum(pas) / pn if pn else None
        edge = (sr - pr) if sr is not None and pr is not None else None
        enough = sn >= SAFE_MIN_N and pn >= PASS_MIN_N
        on = bool(enough and edge is not None and edge >= SAFE_EDGE_PP)
        stats[wh] = {"safe_n": sn, "pass_n": pn, "safe_rate": sr, "pass_rate": pr, "edge_pp": edge, "on": on}
        all_on = all_on and on
    return {"active": bool(all_on), "w14": stats[14], "w18": stats[18]}


@dataclass
class Fill:
    qty_mult: float
    price: float
    kind: str


@dataclass
class SimResult:
    result: str
    exit_time: RealDateTime
    terminal_price: float
    fills: list[Fill]
    gross_pct: float
    fee_pct: float
    net_pct: float
    mfe_pct: float
    mae_pct: float
    stop_stage: str = ""
    detail: str = ""
    data_error: str = ""


def calc_perf(entry: float, fills: list[Fill], extra_entries: list[Fill] | None = None) -> tuple[float, float, float]:
    gross = 0.0
    exit_fee = 0.0
    for x in fills:
        gross += x.qty_mult * (x.price / entry - 1.0) * 100.0
        exit_fee += FEE_PCT * x.qty_mult * (x.price / entry)
    entry_fee = FEE_PCT
    if extra_entries:
        for x in extra_entries:
            # extra entry qty is expressed as multiple of original quantity.
            entry_fee += FEE_PCT * x.qty_mult * (x.price / entry)
            # gross contribution of added quantity uses its own cost basis.
            # caller will add add-position gross separately when needed.
    fee = entry_fee + exit_fee
    return gross, fee, gross - fee


def result_from_core(entry: float, fills: list[Fill], result: str, et: RealDateTime, terminal: float,
                     mfe: float, mae: float, stage: str = "", detail: str = "", data_error: str = "") -> SimResult:
    gross, fee, net = calc_perf(entry, fills)
    return SimResult(result, et, terminal, list(fills), gross, fee, net, mfe, mae, stage, detail, data_error)


def simulate_base(s: dict[str, Any]) -> SimResult:
    symbol = s["symbol"]; entry_t = s["entry"]; entry = s["entry_price"]
    try:
        client = ReplayClient(symbol, entry_t)
        path = client.path_1m(entry_t, 186)
        if len(path) == 0:
            raise RuntimeError("no 1m path")
    except Exception as e:
        return SimResult("DATA_ERROR", entry_t, entry, [], 0, 0, 0, 0, 0, data_error=str(e))

    remaining = 1.0
    fills: list[Fill] = []
    mfe = 0.0; mae = 0.0
    pp_armed = False
    cp10 = cp15 = cp25 = cp30 = False
    p25_pnl: float | None = None
    late_streak = 0
    stage_active = False
    stage_start: RealDateTime | None = None
    stage_signal = 0.0
    stop_stage = ""
    hard = entry * (1.0 - abs(float(getattr(CFG, "research_pv271_disaster_stop_pct", 3.0))) / 100.0)
    rebound_pct = float(getattr(CFG, "research_pv271_rebound_from_signal_pct", 0.50))
    stage_wait = float(getattr(CFG, "research_pv271_wait_minutes", 3.0))
    stage_frac = float(getattr(CFG, "research_pv271_stage_fraction", 0.50))
    max_hold_min = float(getattr(CFG, "max_hold_hours", 3)) * 60.0

    def close_frac(frac: float, px: float, kind: str):
        nonlocal remaining
        q = min(remaining, max(0.0, frac))
        if q > 1e-12:
            fills.append(Fill(q, float(px), kind))
            remaining -= q

    def finish(reason: str, now: RealDateTime, px: float, kind: str, detail: str = "") -> SimResult:
        close_frac(remaining, px, kind)
        return result_from_core(entry, fills, reason, now, px, mfe, mae, stop_stage, detail)

    for _, bar in path.iterrows():
        bar_t = pd.Timestamp(bar["ts"]).to_pydatetime().astimezone(UTC)
        now = bar_t + timedelta(minutes=1)
        client.set_now(now)
        age = (now - entry_t).total_seconds() / 60.0
        high = float(bar["high"]); low = float(bar["low"]); price = float(bar["close"])
        mfe = max(mfe, (high / entry - 1.0) * 100.0)
        mae = min(mae, (low / entry - 1.0) * 100.0)
        tp = entry * (1.0 + TP_PCT / 100.0)

        # If V27-1 stage is active, the current bot manages only rebound / disaster / 3m timeout;
        # original TP does not jump ahead of the staged exit.
        if stage_active and stage_start is not None:
            rb = stage_signal * (1.0 + rebound_pct / 100.0)
            stage_age = (now - stage_start).total_seconds() / 60.0
            hit_d = low <= hard
            hit_r = high >= rb
            if hit_d and hit_r:
                return finish("STOP", now, hard, "V271_STAGE_DISASTER_AMBIG", "stage hard+rebound same 1m")
            if hit_d:
                return finish("STOP", now, hard, "V271_STAGE_DISASTER", "stage disaster")
            if hit_r:
                return finish("STOP", now, rb, "V271_STAGE_REBOUND", "stage rebound close")
            if stage_age >= stage_wait:
                return finish("STOP", now, price, "V271_STAGE_TIMEOUT", "stage 3m timeout")
            continue

        # Exact conservative intraminute priority shared by current V27-1: if TP and -3% hard occur in same 1m, hard wins.
        hit_tp = high >= tp
        hit_hard = low <= hard
        if hit_tp and hit_hard:
            return finish("STOP", now, hard, "V271_DISASTER_AMBIG", "TP and -3% hard in same 1m; hard priority")
        if hit_hard:
            return finish("STOP", now, hard, "V271_DISASTER", "direct -3% disaster")
        if hit_tp:
            return finish("TP20_FULL", now, tp, "TP20_FULL", "+2.0% full TP")

        five_due = (now.minute % 5 == 0)
        fifteen_due = (now.minute % 15 == 0)
        pnl = (price / entry - 1.0) * 100.0

        # PP12 arm as soon as MFE reaches +1.2.
        if not pp_armed and mfe >= PP_ARM_MFE:
            pp_armed = True

        # PP12 is checked only on a completed 5m close, and never overwrites TP touched in that same 5m.
        if pp_armed and five_due:
            b5 = client.last_confirmed(5)
            if b5 is not None:
                c5 = float(b5["close"]); h5 = float(b5["high"])
                c5pct = (c5 / entry - 1.0) * 100.0
                if h5 < tp and c5pct <= PP_CLOSE_PCT:
                    stop_stage = "PP12_CLOSE_M115_FULL"
                    return finish("PROFIT_PROTECT_EXIT", now, c5, "PP12", f"5m close {c5pct:.4f}%")

        # Current Final 4-stage checkpoints — first observation at/after each age only.
        if not cp10 and age >= 10.0:
            cp10 = True
            if pnl <= -1.00 and mfe <= 0.10:
                q = remaining * 0.50
                close_frac(q, price, "STOP10_DEAD50")
                stop_stage = "STOP10_DEAD50"
        if not cp15 and age >= 15.0:
            cp15 = True
            if pnl <= -1.50:
                stop_stage = "STOP15_FULL"
                return finish("STOP", now, price, "STOP15_FULL")
        if not cp25 and age >= 25.0:
            cp25 = True
            p25_pnl = pnl
            if 0.30 <= mfe <= 1.20 and pnl <= -1.20:
                q = remaining * 0.50
                close_frac(q, price, "STOP25_GIVEBACK50")
                stop_stage = "STOP25_GIVEBACK50"
        if not cp30 and age >= 30.0:
            cp30 = True
            delta5 = (pnl - p25_pnl) if p25_pnl is not None else None
            if pnl <= -0.90 and delta5 is not None and delta5 <= -0.30:
                stop_stage = "STOP30_DETERIORATION"
                return finish("STOP", now, price, "STOP30_DETERIORATION", f"delta5={delta5:.4f}")

        # Current V26 late-failure direct exit is evaluated before V27-1 staged fallback.
        if five_due and bool(getattr(CFG, "research_pv26_late_failure_enabled", True)):
            try:
                late, ld = BOT.pv26_late_failure_signal(client, symbol, entry, price, age, mfe, CFG)
            except Exception:
                late, ld = False, {}
            late_streak = late_streak + 1 if late else 0
            req = max(1, int(getattr(CFG, "research_pv26_late_confirmations", 2)))
            if late and late_streak >= req:
                stop_stage = "V26_LATE_FAILURE"
                return finish("LATE_FAILURE_EXIT", now, price, "LATE_FAILURE_EXIT", json.dumps(ld, ensure_ascii=False, default=str)[:500])

        # V27-1 fallback stop signals on each completed 5m.
        if five_due:
            stop_hit = False; stop_type = ""; meta: dict[str, Any] = {}
            try:
                ok, d = BOT.early_crash_failure_signal(client, symbol, entry_t.isoformat(), entry, price, CFG)
                if ok: stop_hit, stop_type, meta = True, "EARLY_CRASH", d
            except Exception as e:
                meta = {"early_crash_error": str(e)}
            if not stop_hit:
                try:
                    ok, d = BOT.p_catastrophic_failure_signal(client, symbol, entry_t.isoformat(), entry, price, CFG)
                    if ok: stop_hit, stop_type, meta = True, "P_CATASTROPHIC", d
                except Exception as e:
                    meta["p_cat_error"] = str(e)
            if not stop_hit and bool(getattr(CFG, "early_failure_enabled", True)):
                try:
                    ok, d = BOT.early_failure_signal(client, symbol, entry_t.isoformat(), CFG)
                    if ok: stop_hit, stop_type, meta = True, str(d.get("failure_type") or "EARLY_FAILURE"), d
                except Exception as e:
                    meta["early_failure_error"] = str(e)
            if not stop_hit and age >= 45.0:
                try:
                    # late_trend_failure_signal uses time.time() only for age. Supply a synthetic entry_ts that yields historical age.
                    fake_entry_ms = int((time.time() - age * 60.0) * 1000)
                    ok, d = BOT.late_trend_failure_signal(client, symbol, fake_entry_ms, False)
                    if ok: stop_hit, stop_type, meta = True, "LATE_TREND_FAILURE", d
                except Exception as e:
                    meta["late_failure_error"] = str(e)
            if stop_hit:
                stage_active = True
                stage_start = now
                stage_signal = price
                q = remaining * max(0.05, min(0.95, stage_frac))
                close_frac(q, stage_signal, f"V271_STAGE1_{stop_type}")
                stop_stage = f"V271_STAGE1_{stop_type}"
                continue

        # Confirmed 15m structure fallback.
        emergency_now = price <= entry * (1.0 - abs(float(getattr(CFG, "structure_emergency_stop_pct", 8.0))) / 100.0)
        if fifteen_due or emergency_now:
            try:
                broken, sd = BOT.hj_structure_broken(client, symbol, CFG, base_price=entry, live_price=price)
            except Exception:
                broken, sd = False, {}
            if broken:
                sig = f(sd.get("price"), price) or price
                stage_active = True
                stage_start = now
                stage_signal = sig
                q = remaining * max(0.05, min(0.95, stage_frac))
                close_frac(q, sig, "V271_STAGE1_STRUCTURE")
                stop_stage = "V271_STAGE1_STRUCTURE"
                continue

        # Flat exit at confirmed 15m checkpoint.
        if fifteen_due and age >= float(getattr(CFG, "flat_exit_minutes", 60)) and mfe < float(getattr(CFG, "flat_min_favorable_pct", 1.0)):
            try:
                flat, fd = BOT.flat_exit_signal(client, symbol, entry, CFG)
            except Exception:
                flat, fd = False, {}
            if flat:
                return finish("FLAT_EXIT_75M", now, price, "FLAT_EXIT", json.dumps(fd, ensure_ascii=False, default=str)[:500])

        if age >= max_hold_min:
            return finish("TIME_EXIT", now, price, "TIME_EXIT")

    # Defensive end if Bybit path is short.
    last = path.iloc[-1]
    et = pd.Timestamp(last["ts"]).to_pydatetime().astimezone(UTC) + timedelta(minutes=1)
    return finish("TIME_EXIT", et, float(last["close"]), "TIME_EXIT_EOD")


@dataclass
class PortState:
    accepted_entries: deque = field(default_factory=deque)
    open_trades: list[tuple[RealDateTime, str]] = field(default_factory=list)
    last_exit: dict[str, RealDateTime] = field(default_factory=dict)
    last_stop_exit: dict[str, RealDateTime] = field(default_factory=dict)
    stop_exits: deque = field(default_factory=deque)

    def prune(self, now: RealDateTime):
        while self.accepted_entries and self.accepted_entries[0] < now - timedelta(minutes=15):
            self.accepted_entries.popleft()
        self.open_trades = [(e, s) for e, s in self.open_trades if e > now]
        while self.stop_exits and self.stop_exits[0] < now - timedelta(minutes=STOP_PAUSE_WINDOW_MIN):
            self.stop_exits.popleft()

    def can_open(self, now: RealDateTime, symbol: str) -> tuple[bool, str]:
        self.prune(now)
        if len(self.accepted_entries) >= MAX_15M_ENTRIES:
            return False, "CAP15_2"
        if len(self.open_trades) >= MAX_SLOTS:
            return False, "SLOT4"
        if any(sym == symbol for _, sym in self.open_trades):
            return False, "SAME_SYMBOL_OPEN"
        le = self.last_exit.get(symbol)
        if le and now - le < timedelta(minutes=SAME_SYMBOL_CD_MIN):
            return False, "COOLDOWN90"
        ls = self.last_stop_exit.get(symbol)
        if ls and now - ls < timedelta(minutes=STOP_SYMBOL_CD_MIN):
            return False, "COOLDOWN180"
        if len(self.stop_exits) >= STOP_PAUSE_COUNT:
            return False, "STOP_PAUSE30"
        return True, ""

    def add(self, entry: RealDateTime, symbol: str, sim: SimResult, stop_like: bool):
        self.accepted_entries.append(entry)
        self.open_trades.append((sim.exit_time, symbol))
        # Future exits are registered only when chronological time reaches them via refresh_completed().


# For portfolio chronology we cannot immediately apply future cooldowns/stops. Pending completions are released by time.
@dataclass
class Scheduler:
    state: PortState = field(default_factory=PortState)
    pending: list[tuple[RealDateTime, str, bool]] = field(default_factory=list)

    def release(self, now: RealDateTime):
        due = [x for x in self.pending if x[0] <= now]
        self.pending = [x for x in self.pending if x[0] > now]
        for et, sym, stop_like in sorted(due, key=lambda z: z[0]):
            self.state.last_exit[sym] = max(self.state.last_exit.get(sym, et), et)
            if stop_like:
                self.state.last_stop_exit[sym] = max(self.state.last_stop_exit.get(sym, et), et)
                self.state.stop_exits.append(et)
        self.state.prune(now)

    def can_open(self, now: RealDateTime, symbol: str):
        self.release(now)
        return self.state.can_open(now, symbol)

    def add(self, entry: RealDateTime, symbol: str, sim: SimResult, stop_like: bool):
        self.state.accepted_entries.append(entry)
        self.state.open_trades.append((sim.exit_time, symbol))
        self.pending.append((sim.exit_time, symbol, stop_like))


def entry_filter(s: dict[str, Any], controls: list[ControlObs]) -> dict[str, Any]:
    d = s["details"]
    sm = safe_meta(d); mm = micro_meta(d); rm = relax_at(s["entry"], controls); mk = mkt100_meta(d)
    safe_relaxed = bool(sm["block"] and rm["active"] and mm["available"] and not mm["risk"])
    if sm["block"] and not safe_relaxed:
        reason = "SAFE_MICRO" if mm["risk"] else "SAFE"
        passed = False
    elif mk["block"]:
        reason = "MKT100"
        passed = False
    else:
        reason = ""
        passed = True
    return {"pass": passed, "reason": reason, "safe": sm, "micro": mm, "relax": rm, "mkt": mk, "safe_relaxed": safe_relaxed}


def row_base_stub(s: dict[str, Any], ef: dict[str, Any]) -> dict[str, Any]:
    r14 = ef["relax"]["w14"]; r18 = ef["relax"]["w18"]
    d = s["details"]
    return {
        "setup_id": s["setup_id"], "symbol": s["symbol"], "entry_time_kst": kst_stamp(s["entry"]),
        "entry_ts_utc": s["entry"].isoformat(), "entry_price": s["entry_price"],
        "market_recomputed": int(bool(s["market_recomputed"])),
        "safe_block_raw": int(ef["safe"]["block"]), "safe_flags": ef["safe"]["flags"],
        "micro_available": int(ef["micro"]["available"]), "micro_risk": int(ef["micro"]["risk"]),
        "safe_relax_on": int(ef["relax"]["active"]), "safe_relaxed": int(ef["safe_relaxed"]),
        "relax14_safe_n": r14["safe_n"], "relax14_pass_n": r14["pass_n"], "relax14_edge_pp": r14["edge_pp"],
        "relax18_safe_n": r18["safe_n"], "relax18_pass_n": r18["pass_n"], "relax18_edge_pp": r18["edge_pp"],
        "btc15": first_f(d, "btc_15m_change_pct", "btc_15m"), "eth15": first_f(d, "eth_15m_change_pct", "eth_15m"),
        "btc4h": first_f(d, "btc_4h_change_pct", "btc_4h"), "eth4h": first_f(d, "eth_4h_change_pct", "eth_4h"),
        "mkt100_block": int(ef["mkt"]["block"]), "accepted": 0, "block_reason": ef["reason"],
        "exit_time_kst": "", "result": "", "stop_stage": "", "mfe_pct": "", "mae_pct": "",
        "gross_pct": "", "fee_pct": "", "net_pct": "", "data_error": "",
    }


def run_base(setups: list[dict[str, Any]], controls: list[ControlObs]):
    sched = Scheduler()
    all_rows: list[dict[str, Any]] = []
    accepted: list[dict[str, Any]] = []
    for i, s in enumerate(setups, 1):
        ef = entry_filter(s, controls)
        row = row_base_stub(s, ef)
        if ef["pass"]:
            ok, why = sched.can_open(s["entry"], s["symbol"])
            if not ok:
                row["block_reason"] = why
            else:
                sim = simulate_base(s)
                row.update({
                    "accepted": 1, "block_reason": "", "exit_time_kst": kst_stamp(sim.exit_time), "exit_ts_utc": sim.exit_time.isoformat(),
                    "result": sim.result, "stop_stage": sim.stop_stage, "terminal_price": sim.terminal_price,
                    "mfe_pct": round(sim.mfe_pct, 6), "mae_pct": round(sim.mae_pct, 6),
                    "gross_pct": round(sim.gross_pct, 6), "fee_pct": round(sim.fee_pct, 6), "net_pct": round(sim.net_pct, 6),
                    "data_error": sim.data_error, "sim_obj": sim, "setup_obj": s,
                })
                stop_like = sim.result in ("STOP", "LATE_FAILURE_EXIT")
                sched.add(s["entry"], s["symbol"], sim, stop_like)
                accepted.append(row)
        all_rows.append(row)
        if i % 50 == 0 or i == len(setups):
            print(f"[BASE] {i}/{len(setups)} accepted={len(accepted)}", flush=True)
    return all_rows, accepted


def r100_post_bars(symbol: str, start: RealDateTime, hours: int = R100_MAX_HOURS) -> pd.DataFrame:
    st = start.astimezone(UTC).replace(second=0, microsecond=0)
    en = st + timedelta(hours=hours, minutes=2)
    return KC.get(symbol, "1m", st, en)


def simulate_r100(base_row: dict[str, Any]) -> SimResult:
    s = base_row["setup_obj"]; base: SimResult = base_row["sim_obj"]
    entry = float(s["entry_price"]); symbol = s["symbol"]
    start = base.exit_time
    try:
        bars = r100_post_bars(symbol, start, R100_MAX_HOURS)
        floor_start = start.replace(second=0, microsecond=0)
        bars = bars[(bars["ts"] > pd.Timestamp(floor_start)) & (bars["ts"] <= pd.Timestamp(start + timedelta(hours=R100_MAX_HOURS)))].copy()
        if len(bars) == 0:
            raise RuntimeError("no recovery 1m bars")
    except Exception as e:
        return SimResult("R100_DATA_ERROR", start, base.terminal_price, [], 0, 0, 0, base.mfe_pct, base.mae_pct, data_error=str(e))

    # Pure STOP replacement: original core 100% remains open. BASE stop partials/staged exits are not carried into recovery.
    # This matches the research question: "if this STOP had not stopped and instead used R100 recovery".
    swing_low = float(base.terminal_price)
    if swing_low <= 0:
        swing_low = entry
    attempt = 0
    add_open = False
    add_price = 0.0
    add_low = 0.0
    prior_add_realized = 0.0  # %p relative to original entry notional
    extra_entries: list[Fill] = []
    extra_exit_fills: list[Fill] = []
    core_exit_price = 0.0
    result = ""
    exit_t = start
    terminal = swing_low
    mfe = base.mfe_pct; mae = base.mae_pct
    add_bar_ts: RealDateTime | None = None

    def total_open_net(mark: float) -> float:
        gross = prior_add_realized + (mark / entry - 1.0) * 100.0
        if add_open:
            gross += (mark - add_price) / entry * 100.0
        # approximate full fees if closed now, including actual extra add entries/removals so far
        entry_fee = FEE_PCT
        for x in extra_entries:
            entry_fee += FEE_PCT * x.qty_mult * (x.price / entry)
        exit_fee = sum(FEE_PCT * x.qty_mult * (x.price / entry) for x in extra_exit_fills)
        exit_fee += FEE_PCT * (mark / entry)  # core
        if add_open:
            exit_fee += FEE_PCT * (mark / entry)
        return gross - entry_fee - exit_fee

    for _, bar in bars.iterrows():
        bt = pd.Timestamp(bar["ts"]).to_pydatetime().astimezone(UTC)
        now = bt + timedelta(minutes=1)
        lo = float(bar["low"]); hi = float(bar["high"]); cl = float(bar["close"])
        mfe = max(mfe, (hi / entry - 1.0) * 100.0)
        mae = min(mae, (lo / entry - 1.0) * 100.0)
        elapsed_h = (now - start).total_seconds() / 3600.0

        if R100_OLD_GUARD:
            net_now = total_open_net(cl)
            cut = (3.0 <= elapsed_h < 6.0 and net_now <= R100_GUARD_3H_NET) or (elapsed_h >= 6.0 and net_now <= R100_GUARD_6H_NET)
            if cut:
                if add_open:
                    prior_add_realized += (cl - add_price) / entry * 100.0
                    extra_exit_fills.append(Fill(1.0, cl, "R100_ADD_GUARD_EXIT"))
                    add_open = False
                core_exit_price = cl; terminal = cl; exit_t = now; result = "CUT6" if elapsed_h >= 6 else "CUT3"
                break

        if not add_open:
            # New low resets the rebound anchor; same-minute rebound is ignored conservatively.
            if lo < swing_low:
                swing_low = lo
                continue
            trigger = swing_low * (1.0 + R100_REBOUND_PCT / 100.0)
            if hi >= trigger:
                attempt += 1
                add_open = True
                add_price = trigger
                add_low = swing_low
                add_bar_ts = bt
                extra_entries.append(Fill(1.0, add_price, f"R100_ADD{attempt}"))
                # Do not allow AVG/break decision in the same minute as the add.
                continue
        else:
            if add_bar_ts is not None and bt <= add_bar_ts:
                continue
            avg = (entry + add_price) / 2.0
            hit_break = lo <= add_low
            hit_avg = hi >= avg
            # Conservative same-minute order: old-low rebreak wins over average recovery.
            if hit_break:
                prior_add_realized += (add_low - add_price) / entry * 100.0
                extra_exit_fills.append(Fill(1.0, add_low, f"R100_ADD{attempt}_FALSE_EXIT"))
                add_open = False
                if attempt >= R100_MAX_ATTEMPTS:
                    core_exit_price = add_low; terminal = add_low; exit_t = now; result = "MAX2_EXIT"
                    break
                swing_low = min(lo, add_low)
                continue
            if hit_avg:
                # Current add + core exit at new average; their current-cycle gross sums to zero for equal quantity.
                prior_add_realized += (avg - add_price) / entry * 100.0
                extra_exit_fills.append(Fill(1.0, avg, f"R100_ADD{attempt}_AVG_EXIT"))
                core_exit_price = avg; terminal = avg; exit_t = now; result = "AVG_EXIT"
                add_open = False
                break

    if not result:
        last = bars.iloc[-1]
        cl = float(last["close"])
        exit_t = pd.Timestamp(last["ts"]).to_pydatetime().astimezone(UTC) + timedelta(minutes=1)
        if add_open:
            prior_add_realized += (cl - add_price) / entry * 100.0
            extra_exit_fills.append(Fill(1.0, cl, f"R100_ADD{attempt}_24H_EXIT"))
            add_open = False
        core_exit_price = cl; terminal = cl; result = "RECOVERY_24H_EXIT"

    # R100 performance relative to original core notional.
    core_gross = (core_exit_price / entry - 1.0) * 100.0
    gross = core_gross + prior_add_realized
    fee = FEE_PCT  # original core entry
    for x in extra_entries:
        fee += FEE_PCT * x.qty_mult * (x.price / entry)
    for x in extra_exit_fills:
        fee += FEE_PCT * x.qty_mult * (x.price / entry)
    fee += FEE_PCT * (core_exit_price / entry)
    net = gross - fee
    fills = [Fill(1.0, core_exit_price, "R100_CORE_EXIT")] + extra_exit_fills
    return SimResult(result, exit_t, terminal, fills, gross, fee, net, mfe, mae,
                     stop_stage=f"R100_ATTEMPTS_{attempt}", detail=f"base_stop={base.stop_stage}; rebound={R100_REBOUND_PCT}%")


def run_r100(base_accepted: list[dict[str, Any]]):
    sched = Scheduler()
    rows = []
    accepted = []
    for i, b in enumerate(base_accepted, 1):
        entry = dt_utc(b["entry_ts_utc"]); symbol = b["symbol"]
        assert entry is not None
        rr = {
            "setup_id": b["setup_id"], "symbol": symbol, "entry_time_kst": b["entry_time_kst"], "entry_ts_utc": b["entry_ts_utc"],
            "entry_price": b["entry_price"], "base_result": b["result"], "base_exit_time_kst": b["exit_time_kst"],
            "base_net_pct": b["net_pct"], "r100_accepted": 0, "r100_block_reason": "", "r100_result": "",
            "r100_exit_time_kst": "", "r100_gross_pct": "", "r100_fee_pct": "", "r100_net_pct": "", "delta_vs_base_pct": "",
            "mfe_pct": "", "mae_pct": "", "data_error": "",
        }
        ok, why = sched.can_open(entry, symbol)
        if not ok:
            rr["r100_block_reason"] = why
            rows.append(rr)
            continue
        if b["result"] == "STOP":
            sim = simulate_r100(b)
        else:
            sim = b["sim_obj"]
        rr.update({
            "r100_accepted": 1, "r100_result": sim.result, "r100_exit_time_kst": kst_stamp(sim.exit_time),
            "r100_exit_ts_utc": sim.exit_time.isoformat(), "r100_gross_pct": round(sim.gross_pct, 6),
            "r100_fee_pct": round(sim.fee_pct, 6), "r100_net_pct": round(sim.net_pct, 6),
            "delta_vs_base_pct": round(sim.net_pct - float(b["net_pct"]), 6),
            "mfe_pct": round(sim.mfe_pct, 6), "mae_pct": round(sim.mae_pct, 6), "data_error": sim.data_error,
            "sim_obj": sim, "base_obj": b,
        })
        stop_like = sim.result in ("STOP", "LATE_FAILURE_EXIT", "MAX2_EXIT", "CUT3", "CUT6", "RECOVERY_24H_EXIT")
        sched.add(entry, symbol, sim, stop_like)
        rows.append(rr); accepted.append(rr)
        if i % 25 == 0 or i == len(base_accepted):
            print(f"[R100] {i}/{len(base_accepted)} accepted={len(accepted)}", flush=True)
    return rows, accepted


def strip_objects(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        out.append({k: v for k, v in r.items() if not k.endswith("_obj")})
    return out


def write_csv(path: Path, rows: list[dict[str, Any]]):
    rows = strip_objects(rows)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    keys = []
    seen = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        w = csv.DictWriter(fp, fieldnames=keys, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)


def daily_base(all_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    days = [(START_KST + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(22)]
    out = []
    for day in days:
        rr = [r for r in all_rows if str(r.get("entry_time_kst", "")).startswith(day)]
        acc = [r for r in rr if int(r.get("accepted") or 0) == 1]
        def bc(reason): return sum(1 for r in rr if r.get("block_reason") == reason)
        def rc(res): return sum(1 for r in acc if r.get("result") == res)
        net = sum(float(r.get("net_pct") or 0) for r in acc)
        gross = sum(float(r.get("gross_pct") or 0) for r in acc)
        fee = sum(float(r.get("fee_pct") or 0) for r in acc)
        out.append({
            "date": day, "v25_confirmed": len(rr), "base_entries": len(acc),
            "safe_block": sum(1 for r in rr if str(r.get("block_reason", "")).startswith("SAFE")),
            "mkt100_block": bc("MKT100"), "cap15_block": bc("CAP15_2"), "slot4_block": bc("SLOT4"),
            "same_symbol_open_block": bc("SAME_SYMBOL_OPEN"), "cooldown90_block": bc("COOLDOWN90"),
            "cooldown180_block": bc("COOLDOWN180"), "stop_pause_block": bc("STOP_PAUSE30"),
            "tp": rc("TP20_FULL"), "stop": rc("STOP"), "late": rc("LATE_FAILURE_EXIT"),
            "pp12": rc("PROFIT_PROTECT_EXIT"), "flat": rc("FLAT_EXIT_75M"), "time": rc("TIME_EXIT"),
            "data_error": rc("DATA_ERROR"), "gross_pct": round(gross, 6), "fee_pct": round(fee, 6), "net_pct": round(net, 6),
        })
    return out


def daily_r100(rrows: list[dict[str, Any]], base_daily_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    base_by_day = {r["date"]: r for r in base_daily_rows}
    days = list(base_by_day)
    daily = []; comp = []
    for day in days:
        rr = [r for r in rrows if str(r.get("entry_time_kst", "")).startswith(day)]
        acc = [r for r in rr if int(r.get("r100_accepted") or 0) == 1]
        def bc(reason): return sum(1 for r in rr if r.get("r100_block_reason") == reason)
        def rc(res): return sum(1 for r in acc if r.get("r100_result") == res)
        net = sum(float(r.get("r100_net_pct") or 0) for r in acc)
        b = base_by_day[day]; bnet = float(b["net_pct"])
        d = {
            "date": day, "base_cohort": len(rr), "r100_accepted": len(acc), "r100_net_pct": round(net, 6),
            "avg_exit": rc("AVG_EXIT"), "max2_exit": rc("MAX2_EXIT"), "cut3": rc("CUT3"), "cut6": rc("CUT6"),
            "recovery_24h_exit": rc("RECOVERY_24H_EXIT"), "tp": rc("TP20_FULL"), "stop": rc("STOP") + rc("LATE_FAILURE_EXIT"),
            "slot4_block": bc("SLOT4"), "cap15_block": bc("CAP15_2"), "cooldown90_block": bc("COOLDOWN90"),
            "cooldown180_block": bc("COOLDOWN180"), "stop_pause_block": bc("STOP_PAUSE30"), "same_symbol_open_block": bc("SAME_SYMBOL_OPEN"),
            "delta_vs_base_pct": round(net - bnet, 6),
        }
        daily.append(d)
        comp.append({
            "date": day, "BASE_ENTRIES": b["base_entries"], "BASE_NET": b["net_pct"], "R100_NET": round(net, 6),
            "DELTA": round(net - bnet, 6), "TP": d["tp"], "STOP": d["stop"], "AVG_EXIT": d["avg_exit"],
            "MAX2_EXIT": d["max2_exit"], "SLOT_BLOCK": d["slot4_block"],
            "R100_ACCEPTED": d["r100_accepted"], "COOLDOWN_BLOCK": d["cooldown90_block"] + d["cooldown180_block"],
            "STOP_PAUSE_BLOCK": d["stop_pause_block"], "RECOVERY_24H_EXIT": d["recovery_24h_exit"],
        })
    return daily, comp


def summary_text(setups, controls, base_rows, base_acc, bdaily, rrows, racc, rdaily) -> str:
    actual = sum(1 for o in controls if o.source == "ACTUAL_FWD_CONTROL")
    proxy = len(controls) - actual
    base_net = sum(float(r.get("net_pct") or 0) for r in base_acc)
    r_net = sum(float(r.get("r100_net_pct") or 0) for r in racc)
    bstop = sum(1 for r in base_acc if r.get("result") == "STOP")
    avg = sum(1 for r in racc if r.get("r100_result") == "AVG_EXIT")
    mx2 = sum(1 for r in racc if r.get("r100_result") == "MAX2_EXIT")
    r24 = sum(1 for r in racc if r.get("r100_result") == "RECOVERY_24H_EXIT")
    base_err = sum(1 for r in base_acc if r.get("data_error"))
    r_err = sum(1 for r in racc if r.get("data_error"))
    lines = [
        "UNIFIED CURRENT 2026-09-01~09-22 KST",
        f"script={SCRIPT_VERSION}",
        f"bot_source={BOT_PATH}",
        f"bot_runtime_version={getattr(BOT, 'BOT_RUNTIME_VERSION', '')}",
        f"db={DB_PATH}",
        "",
        "[INPUT]",
        f"V25 confirmed={len(setups)} (expected={EXPECTED_V25})",
        f"market telemetry recomputed rows={_market_recompute_count}",
        f"RELAX control observations={len(controls)} actual_fwd_control={actual} proxy={proxy}",
        "RELAX proxy priority=P_FWD_CONTROL > P_V27_1 > P_V26 > P_V25; dedup by setup/minute key",
        "",
        "[BASE RULES]",
        "SAFE=C/RN/RS current thresholds; RELAX=14h&18h both +8pp, n>=5/5; micro-risk never relaxed",
        "MKT100=BTC4h & ETH4h both abs<=0.08%",
        "TP=+2.0% FULL; PP12=MFE+1.2 arm / confirmed 5m close<=-1.15 full protect",
        "Final4=10m 50%; 15m full; 25m 50%; 30m full; else current bot V27-1 fallback",
        "portfolio=4 slots / rolling15m max2 / exit90 / STOP-LATE180 / 30m STOP-LATE>=2 pause",
        f"fee=taker {FEE_PCT}% entry + each exit",
        "",
        "[BASE RESULT]",
        f"accepted={len(base_acc)} blocked={len(base_rows)-len(base_acc)} STOP={bstop} NET={base_net:.6f}%p",
        f"data_errors={base_err}",
        "",
        "[R100 MAX2 RESULT]",
        "scope=BASE accepted cohort only; only BASE result==STOP is replaced by recovery",
        f"rebound=low +{R100_REBOUND_PCT:.2f}% / add=+100% equal qty / avg exit / second false rebound=MAX2_EXIT",
        f"old CUT3/CUT6 guard enabled={R100_OLD_GUARD} (default OFF for this requested final comparison)",
        f"recovery unresolved horizon={R100_MAX_HOURS}h -> RECOVERY_24H_EXIT",
        f"accepted={len(racc)} blocked={len(rrows)-len(racc)} AVG_EXIT={avg} MAX2_EXIT={mx2} R24={r24} NET={r_net:.6f}%p",
        f"DELTA_R100_vs_BASE={r_net-base_net:.6f}%p",
        f"data_errors={r_err}",
        "",
        "[FILES]",
        str(OUT_BASE_DAILY), str(OUT_BASE_TRADES), str(OUT_R100_DAILY), str(OUT_R100_TRADES), str(OUT_COMPARE),
    ]
    if len(setups) != EXPECTED_V25:
        lines += ["", f"WARNING: V25 count mismatch: got {len(setups)}, expected {EXPECTED_V25}. Check DB/version before final judgment."]
    if base_err or r_err:
        lines += ["", "WARNING: data_errors > 0. Inspect TRADES CSV; do not use totals as final until cleared."]
    return "\n".join(lines) + "\n"


def main():
    if not DB_PATH.exists():
        raise SystemExit(f"DB not found: {DB_PATH}")
    print(f"=== {SCRIPT_VERSION} ===", flush=True)
    print(f"BOT: {BOT_PATH} / {getattr(BOT, 'BOT_RUNTIME_VERSION', '')}", flush=True)
    print("[1/6] market BTC/ETH 1m causal cache", flush=True)
    load_market_series()
    print("[2/6] V25 setups + missing market telemetry", flush=True)
    setups = load_setups()
    print(f"V25 confirmed: {len(setups)}", flush=True)
    print("[3/6] RELAX CONTROL actual/proxy history", flush=True)
    controls = load_control_proxy()
    print(f"control obs: {len(controls)}", flush=True)
    print("[4/6] BASE replay", flush=True)
    base_rows, base_acc = run_base(setups, controls)
    bd = daily_base(base_rows)
    write_csv(OUT_BASE_TRADES, base_rows)
    write_csv(OUT_BASE_DAILY, bd)
    print("[5/6] R100 + MAX2 replay over BASE accepted cohort", flush=True)
    rrows, racc = run_r100(base_acc)
    rd, comp = daily_r100(rrows, bd)
    write_csv(OUT_R100_TRADES, rrows)
    write_csv(OUT_R100_DAILY, rd)
    write_csv(OUT_COMPARE, comp)
    print("[6/6] summary", flush=True)
    txt = summary_text(setups, controls, base_rows, base_acc, bd, rrows, racc, rd)
    OUT_SUMMARY.write_text(txt, encoding="utf-8")
    print(txt)
    print("DONE")


if __name__ == "__main__":
    main()
