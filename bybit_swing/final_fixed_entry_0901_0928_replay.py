#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FINAL FIXED ENTRY REPLAY — 2026-09-01 ~ 2026-09-28 KST

Purpose
-------
Rebuild ONE common entry ledger before any Recovery/DCA research.

Fixed entry stack:
  V25 confirmed
  -> SAFE(C/RN/RS) + SAFE RELAX
  -> existing MKT100
  -> performance market guard:
       market condition = rolling 2h V25 confirmed >= 8
                          AND mean(abs(BTC4h), abs(ETH4h)) >= 0.40%
       state starts ON
       N=9 completed BASE_REF market-shadow trades
       ON -> OFF at rolling sum >= +9%p
       OFF -> ON at rolling sum <= -6%p
  -> V22 quality OR, LOCK0 (block immediately; no reserved slot)
  -> chronological portfolio scheduler:
       4 slots / rolling 15m max 2 / same-symbol 90m / stop-symbol 180m
       2 STOP-like exits inside trailing 45m => new entry blocked

Crucial
-------
- EVERY candidate is processed chronologically.
- If a candidate is blocked, it occupies no slot; later eligible candidates are reconsidered
  automatically, so replacement/backfill entries are reflected.
- Base/current exits only are used for this ledger:
  TP2.0 / PP12 / Final4 / V27-1 fallback.
- NO Observer / Recovery / DCA is applied here.
- The strict STOP list is produced only AFTER the fixed entry ledger is rebuilt.
- No DB writes, no live orders, no live bot changes.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import json
import math
import sys
import zipfile
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

START = datetime(2026, 9, 1, 0, 0, 0, tzinfo=KST)
SPLIT = datetime(2026, 9, 23, 0, 0, 0, tzinfo=KST)
END = datetime(2026, 9, 29, 0, 0, 0, tzinfo=KST)

MARKET_V25_2H_MIN = 8
MARKET_ABS4H_AVG_MIN = 0.40
REGIME_N = 9
REGIME_OFF_AT = 9.0
REGIME_ON_AT = -6.0

FINAL_STOP_PAUSE_MIN = 45
REFERENCE_STOP_PAUSE_MIN = 30

V22_SCAN_COLS = {
    "time_kst", "result", "p_v25_setup_id", "p_v2_score",
    "ema9_ema20_gap_pct", "rebound_from_low_pct", "rsi_delta",
    "btc_15m_change_pct",
}

def load_module(name: str, path: Path):
    if not path.exists():
        raise SystemExit(f"MISSING_SOURCE: {path}")
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise SystemExit(f"CANNOT_IMPORT: {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

def fv(v, default=None):
    try:
        if v is None or str(v).strip() == "":
            return default
        x = float(v)
        return default if math.isnan(x) else x
    except Exception:
        return default

def dt_utc(v):
    if not v:
        return None
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except Exception:
        return None

def kst_text(t):
    return t.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

def sha256_file(path: Path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()

def write_csv(path: Path, rows: list[dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    keys, seen = [], set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                keys.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

def read_v22_scan(path: Path, M):
    if not path.exists():
        raise SystemExit(f"MISSING_V22_SCAN: {path}")
    hdr = pd.read_csv(path, nrows=0).columns.tolist()
    use = [c for c in hdr if c in V22_SCAN_COLS]
    need = {"result", "p_v25_setup_id"}
    if not need.issubset(use):
        raise SystemExit(f"V22_SCAN_MISSING_COLUMNS: {path.name}: {sorted(need-set(use))}")
    df = pd.read_csv(path, usecols=use, low_memory=False)
    return M.load_exact_watch_features(df)

def scan_coverage(path: Path):
    if not path.exists():
        return {"exists": 0}
    hdr = pd.read_csv(path, nrows=0).columns.tolist()
    if "time_kst" not in hdr:
        return {"exists": 1, "time_col": 0}
    df = pd.read_csv(path, usecols=["time_kst"], low_memory=False)
    ts = pd.to_datetime(df["time_kst"], errors="coerce").dropna().sort_values()
    if len(ts) == 0:
        return {"exists": 1, "time_col": 1, "rows": len(df), "valid": 0}
    gaps = ts.diff().dt.total_seconds().div(60).dropna()
    return {
        "exists": 1,
        "time_col": 1,
        "rows": len(df),
        "valid": len(ts),
        "first": str(ts.iloc[0]),
        "last": str(ts.iloc[-1]),
        "max_gap_min": float(gaps.max()) if len(gaps) else 0.0,
    }

def load_hist_sim_cache(U, path: Path):
    cache = {}
    if not path.exists():
        return cache
    df = pd.read_csv(path, low_memory=False)
    for r in df.to_dict("records"):
        if int(fv(r.get("accepted"), 0) or 0) != 1:
            continue
        et = dt_utc(r.get("exit_ts_utc"))
        if et is None:
            try:
                et = datetime.strptime(
                    str(r.get("exit_time_kst"))[:19], "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=KST).astimezone(UTC)
            except Exception:
                continue
        sid = str(r.get("setup_id") or "")
        if not sid:
            continue
        ep = fv(r.get("entry_price"), 0.0) or 0.0
        cache[sid] = U.SimResult(
            result=str(r.get("result") or ""),
            exit_time=et,
            terminal_price=fv(r.get("terminal_price"), ep) or ep,
            fills=[],
            gross_pct=fv(r.get("gross_pct"), 0.0) or 0.0,
            fee_pct=fv(r.get("fee_pct"), 0.0) or 0.0,
            net_pct=fv(r.get("net_pct"), 0.0) or 0.0,
            mfe_pct=fv(r.get("mfe_pct"), 0.0) or 0.0,
            mae_pct=fv(r.get("mae_pct"), 0.0) or 0.0,
            stop_stage=str(r.get("stop_stage") or ""),
            detail="CACHE_UNIFIED_HIST",
            data_error=str(r.get("data_error") or ""),
        )
    return cache

def load_hist_v22(path: Path):
    if not path.exists():
        raise SystemExit(f"MISSING_V22_HIST: {path}")
    df = pd.read_csv(path, low_memory=False)
    if "dataset" in df.columns:
        df = df[df["dataset"].astype(str) == "HIST"]
    v22 = {}
    meta = {}
    for r in df.to_dict("records"):
        sid = str(r.get("setup_id") or "")
        if not sid or sid in v22:
            continue
        over = bool(int(fv(r.get("overext"), 0) or 0))
        weak = bool(int(fv(r.get("weak_reaccel"), 0) or 0))
        v22[sid] = bool(over or weak)
        meta[sid] = {
            "watch_time_kst": r.get("watch_time_kst", ""),
            "p_v2_score": fv(r.get("p_v2_score")),
            "ema9_ema20_gap_pct": fv(r.get("ema9_ema20_gap_pct")),
            "rebound_from_low_pct": fv(r.get("rebound_from_low_pct")),
            "rsi_delta": fv(r.get("rsi_delta")),
            "btc_15m_change_pct": fv(r.get("btc_15m_change_pct")),
            "overext": over,
            "weak_reaccel": weak,
            "v22_quality_or": bool(over or weak),
        }
    return v22, meta

def load_legacy_gate(root: Path):
    p = root / "MKT_REGIME_PERF_B_N9_P9_N6_TRADES.csv"
    if p.exists():
        df = pd.read_csv(p, low_memory=False)
        return {
            str(r.get("setup_id") or ""): r
            for r in df.to_dict("records")
            if str(r.get("setup_id") or "")
        }
    zpath = root / "MKT_REGIME_EXACT_RESULTS.zip"
    if zpath.exists():
        with zipfile.ZipFile(zpath) as z:
            names = [
                n for n in z.namelist()
                if "PERF_B_N9_P9_N6" in n and n.endswith("_TRADES.csv")
            ]
            if names:
                with z.open(names[0]) as raw:
                    df = pd.read_csv(raw, low_memory=False)
                return {
                    str(r.get("setup_id") or ""): r
                    for r in df.to_dict("records")
                    if str(r.get("setup_id") or "")
                }
    return {}

class PerfSwitch:
    def __init__(self, events):
        self.events = sorted(events, key=lambda x: (x["exit_time"], x["setup_id"]))
        self.i = 0
        self.q = deque(maxlen=REGIME_N)
        self.on = True
        self.transitions = []

    def snapshot(self, now):
        while self.i < len(self.events) and self.events[self.i]["exit_time"] <= now:
            e = self.events[self.i]
            self.i += 1
            self.q.append(float(e["net_pct"]))
            if len(self.q) < REGIME_N:
                continue
            s = sum(self.q)
            old = self.on
            if self.on and s >= REGIME_OFF_AT:
                self.on = False
            elif (not self.on) and s <= REGIME_ON_AT:
                self.on = True
            if self.on != old:
                self.transitions.append({
                    "time_kst": kst_text(e["exit_time"]),
                    "new_state": "ON" if self.on else "OFF",
                    "rolling_n": len(self.q),
                    "rolling_sum": round(s, 6),
                    "trigger_setup_id": e["setup_id"],
                    "trigger_symbol": e["symbol"],
                    "trigger_net": round(float(e["net_pct"]), 6),
                })
        return self.on, len(self.q), (sum(self.q) if self.q else None)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/root/hyejin-trader/bybit_swing")
    args = ap.parse_args()
    R = Path(args.root).expanduser().resolve()

    U_PATH = R / "unified_current_0901_0922.py"
    FWD_PATH = R / "forward_full_0924_0925.py"
    BOT_PATH = R / "bot.py"
    V22_HIST = R / "V22_QUALITY_RESCHEDULE_TRADES.csv"
    HIST_BASE = R / "UNIFIED_CURRENT_0901_0922_TRADES.csv"
    PRE28_SCAN_CANDS = [
        R / "scan_FORWARD_20260923_TO_NOW_MKTREGIME_KST.csv",
        R / "scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.csv",
    ]
    PRE28_SCAN = next((p for p in PRE28_SCAN_CANDS if p.exists()), None)
    SCAN28 = R / "SCAN_FULL_20260928_0000_2359_KST.csv"

    required = [U_PATH, FWD_PATH, BOT_PATH, V22_HIST, HIST_BASE, SCAN28]
    if PRE28_SCAN is None:
        required.append(PRE28_SCAN_CANDS[0])
    miss = [str(p) for p in required if not p.exists()]
    if miss:
        raise SystemExit("MISSING_REQUIRED:\n" + "\n".join(miss))

    print("=== FINAL FIXED ENTRY 09/01~09/28 ===", flush=True)
    print("NO Recovery/DCA. Base exits only.", flush=True)

    # HIST 09/01~09/22
    UH = load_module("U_BASELINE_FIXED_HIST", U_PATH)
    UH.START_KST = START
    UH.END_KST = SPLIT
    UH.START_UTC = START.astimezone(UTC)
    UH.END_UTC = SPLIT.astimezone(UTC)
    UH.EXPECTED_V25 = -1
    UH.CACHE_DIR = R / ".unified_kline_cache"
    UH.CACHE_DIR.mkdir(exist_ok=True)
    UH.KC = UH.KlineCache()
    UH._market_1m = {}
    UH._market_recompute_count = 0
    UH.load_market_series()
    hset = [
        s for s in UH.load_setups()
        if START.astimezone(UTC) <= s["entry"] < SPLIT.astimezone(UTC)
    ]
    hctl = UH.load_control_proxy()
    hv22, hv22meta = load_hist_v22(V22_HIST)
    hcache = load_hist_sim_cache(UH, HIST_BASE)

    def hsim(st):
        sid = st["setup_id"]
        if sid not in hcache:
            hcache[sid] = UH.simulate_base(st)
        return hcache[sid]

    # FORWARD 09/23~09/28
    M = load_module("M_BASELINE_FIXED_FWD", FWD_PATH)
    UF = M.U
    M.WARM_START_KST = datetime(2026, 9, 22, 8, 0, 0, tzinfo=KST)
    M.EVAL_START_KST = SPLIT
    M.configure_unified(END - timedelta(seconds=1))

    # Fresh forward cache prevents contamination from the previously found corrupt cache.
    CLEAN_FWD_CACHE = R / ".baseline_fixed_0901_0928_fwd_clean_v1"
    CLEAN_FWD_CACHE.mkdir(exist_ok=True)
    UF.CACHE_DIR = CLEAN_FWD_CACHE
    UF.KC = UF.KlineCache()
    UF._market_1m = {}
    UF._market_recompute_count = 0
    UF.load_market_series()

    fset_all = UF.load_setups()
    fset = [
        s for s in fset_all
        if SPLIT.astimezone(UTC) <= s["entry"] < END.astimezone(UTC)
    ]
    fctl = UF.load_control_proxy()
    fcache = {}

    def fsim(st):
        sid = st["setup_id"]
        if sid not in fcache:
            fcache[sid] = UF.simulate_base(st)
        return fcache[sid]

    qpre = read_v22_scan(PRE28_SCAN, M)
    q28 = read_v22_scan(SCAN28, M)

    fv22 = {}
    fv22meta = {}
    for st in fset:
        day = st["entry"].astimezone(KST).strftime("%Y-%m-%d")
        qm = (q28 if day == "2026-09-28" else qpre).get(st["setup_id"])
        if qm:
            fv22[st["setup_id"]] = bool(qm["v22_quality_or"])
            fv22meta[st["setup_id"]] = qm
        else:
            fv22[st["setup_id"]] = False

    # One chronological stream
    items = []
    seen = set()
    for st in hset:
        sid = st["setup_id"]
        if sid in seen:
            raise SystemExit(f"DUPLICATE_SETUP_ID: {sid}")
        seen.add(sid)
        items.append({"dataset": "HIST", "st": st})
    for st in fset:
        sid = st["setup_id"]
        if sid in seen:
            raise SystemExit(f"DUPLICATE_SETUP_ID: {sid}")
        seen.add(sid)
        items.append({"dataset": "FORWARD", "st": st})
    items.sort(key=lambda x: x["st"]["entry"])

    def eng(item):
        if item["dataset"] == "HIST":
            return UH, hctl, hsim, hv22, hv22meta
        return UF, fctl, fsim, fv22, fv22meta

    # Market condition on all V25 setups
    q = deque()
    market = {}
    market_missing = 0
    for item in items:
        st = item["st"]
        t = st["entry"]
        while q and q[0] < t - timedelta(hours=2):
            q.popleft()
        q.append(t)
        U, _, _, _, _ = eng(item)
        d = st["details"]
        U.fill_missing_market(d, t)
        b4 = U.first_f(d, "btc_4h_change_pct", "btc_4h")
        e4 = U.first_f(d, "eth_4h_change_pct", "eth_4h")
        av = None if b4 is None or e4 is None else (abs(b4) + abs(e4)) / 2.0
        if b4 is None or e4 is None:
            market_missing += 1
        market[st["setup_id"]] = {
            "v25_2h_count": len(q),
            "btc4h": b4,
            "eth4h": e4,
            "abs4h_avg": av,
            "risk": bool(
                len(q) >= MARKET_V25_2H_MIN
                and av is not None
                and av >= MARKET_ABS4H_AVG_MIN
            ),
        }

    # Independent BASE_REF with original validated 30m pause.
    UH.STOP_PAUSE_WINDOW_MIN = REFERENCE_STOP_PAUSE_MIN
    ref_sched = UH.Scheduler()
    ref_events = []
    ref_rows = []
    for idx, item in enumerate(items, 1):
        st = item["st"]
        U, ctl, getsim, _, _ = eng(item)
        ef = U.entry_filter(st, ctl)
        if not ef["pass"]:
            continue
        ok, _ = ref_sched.can_open(st["entry"], st["symbol"])
        if not ok:
            continue
        sim = getsim(st)
        ref_sched.add(
            st["entry"], st["symbol"], sim,
            sim.result in ("STOP", "LATE_FAILURE_EXIT")
        )
        if market[st["setup_id"]]["risk"]:
            e = {
                "setup_id": st["setup_id"],
                "symbol": st["symbol"],
                "entry_time": st["entry"],
                "exit_time": sim.exit_time,
                "net_pct": float(sim.net_pct),
            }
            ref_events.append(e)
            ref_rows.append({
                "setup_id": e["setup_id"],
                "symbol": e["symbol"],
                "entry_time_kst": kst_text(e["entry_time"]),
                "exit_time_kst": kst_text(e["exit_time"]),
                "net_pct": round(e["net_pct"], 6),
            })
        if idx % 250 == 0:
            print(f"[REF] {idx}/{len(items)} events={len(ref_events)}", flush=True)

    # Final fixed entry replay with 45m STOP pause and base exits only.
    UH.STOP_PAUSE_WINDOW_MIN = FINAL_STOP_PAUSE_MIN
    sched = UH.Scheduler()
    sw = PerfSwitch(ref_events)
    legacy = load_legacy_gate(R)

    allrows, trades, blocks, strict_stops, stoplike = [], [], [], [], []
    data_errors = 0
    for idx, item in enumerate(items, 1):
        st = item["st"]
        sid = st["setup_id"]
        U, ctl, getsim, v22map, v22meta = eng(item)
        ef = U.entry_filter(st, ctl)
        ron, rn, rsum = sw.snapshot(st["entry"])
        mm = market[sid]
        qm = v22meta.get(sid)
        qmissing = int(qm is None)

        safe = ef.get("safe", {})
        micro = ef.get("micro", {})
        mkt100 = ef.get("mkt", {})
        leg = legacy.get(sid, {})
        leg_acc = int(fv(leg.get("accepted"), 0) or 0) if leg else ""
        leg_reason = str(leg.get("block_reason") or "") if leg else ""

        row = {
            "dataset": item["dataset"],
            "setup_id": sid,
            "symbol": st["symbol"],
            "entry_time_kst": kst_text(st["entry"]),
            "entry_price": st["entry_price"],
            "safe_block": int(bool(safe.get("block"))),
            "safe_flags": safe.get("flags", ""),
            "safe_relaxed": int(bool(ef.get("safe_relaxed"))),
            "micro_available": int(bool(micro.get("available"))),
            "micro_risk": int(bool(micro.get("risk"))),
            "mkt100_available": int(bool(mkt100.get("available"))),
            "mkt100_block": int(bool(mkt100.get("block"))),
            "base_filter_pass": int(bool(ef["pass"])),
            "base_filter_reason": ef.get("reason", ""),
            "v25_2h_count": mm["v25_2h_count"],
            "btc4h": mm["btc4h"],
            "eth4h": mm["eth4h"],
            "abs4h_avg": mm["abs4h_avg"],
            "market_condition": int(mm["risk"]),
            "market_regime_on": int(ron),
            "market_shadow_n": rn,
            "market_shadow_sum": "" if rsum is None else round(rsum, 6),
            "v22_watch_missing": qmissing,
            "v22q": int(bool(v22map.get(sid, False))),
            "v22_overext": int(bool(qm and qm.get("overext"))),
            "v22_weak_reaccel": int(bool(qm and qm.get("weak_reaccel"))),
            "v22_score": "" if not qm else qm.get("p_v2_score"),
            "v22_gap": "" if not qm else qm.get("ema9_ema20_gap_pct"),
            "v22_rebound": "" if not qm else qm.get("rebound_from_low_pct"),
            "v22_rsi_delta": "" if not qm else qm.get("rsi_delta"),
            "v22_btc15": "" if not qm else qm.get("btc_15m_change_pct"),
            "legacy_gate_available": int(bool(leg)),
            "legacy_accepted30": leg_acc,
            "legacy_block_reason30": leg_reason,
            "accepted": 0,
            "block_reason": "",
            "result": "",
            "exit_time_kst": "",
            "net_pct": "",
            "gross_pct": "",
            "fee_pct": "",
            "mfe_pct": "",
            "mae_pct": "",
            "stop_stage": "",
            "data_error": "",
        }

        if not ef["pass"]:
            row["block_reason"] = ef.get("reason", "") or "BASE_FILTER"
            blocks.append(row.copy())
            allrows.append(row)
            continue
        if ron and mm["risk"]:
            row["block_reason"] = "MARKET_GUARD_ACTIVE"
            blocks.append(row.copy())
            allrows.append(row)
            continue
        if v22map.get(sid, False):
            row["block_reason"] = "V22_QUALITY_OR"
            blocks.append(row.copy())
            allrows.append(row)
            continue

        ok, why = sched.can_open(st["entry"], st["symbol"])
        if not ok:
            row["block_reason"] = "STOP_PAUSE45" if why == "STOP_PAUSE30" else why
            blocks.append(row.copy())
            allrows.append(row)
            continue

        sim = getsim(st)
        sched.add(
            st["entry"], st["symbol"], sim,
            sim.result in ("STOP", "LATE_FAILURE_EXIT")
        )
        row.update({
            "accepted": 1,
            "result": sim.result,
            "exit_time_kst": kst_text(sim.exit_time),
            "net_pct": round(float(sim.net_pct), 6),
            "gross_pct": round(float(sim.gross_pct), 6),
            "fee_pct": round(float(sim.fee_pct), 6),
            "mfe_pct": round(float(sim.mfe_pct), 6),
            "mae_pct": round(float(sim.mae_pct), 6),
            "stop_stage": getattr(sim, "stop_stage", "") or "",
            "data_error": getattr(sim, "data_error", "") or "",
        })
        if row["data_error"] or sim.result == "DATA_ERROR":
            data_errors += 1
        trades.append(row.copy())
        if sim.result == "STOP":
            strict_stops.append(row.copy())
        if sim.result in ("STOP", "LATE_FAILURE_EXIT"):
            stoplike.append(row.copy())
        allrows.append(row)

        if idx % 250 == 0:
            print(
                f"[FINAL] {idx}/{len(items)} entries={len(trades)} STOP={len(strict_stops)}",
                flush=True
            )

    dates = [
        (START + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((END - START).days)
    ]
    daily = []
    for d in dates:
        rr = [r for r in allrows if r["entry_time_kst"].startswith(d)]
        aa = [r for r in rr if int(r["accepted"]) == 1]
        bc = Counter(str(r.get("block_reason") or "") for r in rr if not int(r["accepted"]))
        rc = Counter(str(r.get("result") or "") for r in aa)
        daily.append({
            "date": d,
            "v25_candidates": len(rr),
            "entries": len(aa),
            "net_pct": round(sum(float(r["net_pct"]) for r in aa), 6),
            "TP20_FULL": rc["TP20_FULL"],
            "PROFIT_PROTECT_EXIT": rc["PROFIT_PROTECT_EXIT"],
            "STOP": rc["STOP"],
            "LATE_FAILURE_EXIT": rc["LATE_FAILURE_EXIT"],
            "TIME_EXIT": rc["TIME_EXIT"],
            "DATA_ERROR": rc["DATA_ERROR"],
            "SAFE_BLOCK": bc["SAFE"],
            "SAFE_MICRO_BLOCK": bc["SAFE_MICRO"],
            "MKT100_BLOCK": bc["MKT100"],
            "MARKET_GUARD_BLOCK": bc["MARKET_GUARD_ACTIVE"],
            "V22_BLOCK": bc["V22_QUALITY_OR"],
            "CAP15_BLOCK": bc["CAP15_2"],
            "SLOT4_BLOCK": bc["SLOT4"],
            "SAME_SYMBOL_OPEN_BLOCK": bc["SAME_SYMBOL_OPEN"],
            "COOLDOWN90_BLOCK": bc["COOLDOWN90"],
            "COOLDOWN180_BLOCK": bc["COOLDOWN180"],
            "STOP_PAUSE45_BLOCK": bc["STOP_PAUSE45"],
            "v22_watch_missing": sum(int(r["v22_watch_missing"]) for r in rr),
        })

    changes = []
    for r in allrows:
        if not r["legacy_gate_available"]:
            continue
        fa = int(r["accepted"])
        la = int(r["legacy_accepted30"])
        if fa == la:
            continue
        changes.append({
            "change_type": "NEW_FINAL_ENTRY_VS_LEGACY30" if fa else "LOST_LEGACY30_ENTRY",
            "setup_id": r["setup_id"],
            "symbol": r["symbol"],
            "entry_time_kst": r["entry_time_kst"],
            "final_accepted": fa,
            "final_block_reason": r["block_reason"],
            "legacy_accepted30": la,
            "legacy_block_reason30": r["legacy_block_reason30"],
            "final_result": r["result"],
            "final_net_pct": r["net_pct"],
        })

    cov28 = scan_coverage(SCAN28)
    accepted_net = sum(float(r["net_pct"]) for r in trades)
    outcome = Counter(r["result"] for r in trades)
    blockc = Counter(r["block_reason"] for r in blocks)

    qmiss_hist = sum(1 for s in hset if s["setup_id"] not in hv22meta)
    qmiss_fwd_pre28 = sum(
        1 for s in fset
        if s["entry"].astimezone(KST).strftime("%Y-%m-%d") != "2026-09-28"
        and s["setup_id"] not in fv22meta
    )
    qmiss_28 = sum(
        1 for s in fset
        if s["entry"].astimezone(KST).strftime("%Y-%m-%d") == "2026-09-28"
        and s["setup_id"] not in fv22meta
    )

    source_hashes = {
        p.name: sha256_file(p)
        for p in [BOT_PATH, U_PATH, FWD_PATH, V22_HIST, PRE28_SCAN, SCAN28]
    }

    audit_flags = []
    if data_errors:
        audit_flags.append(f"DATA_ERRORS={data_errors}")
    if market_missing:
        audit_flags.append(f"MARKET_4H_MISSING={market_missing}")
    if cov28.get("valid", 0):
        first = str(cov28.get("first", ""))
        last = str(cov28.get("last", ""))
        gap = float(cov28.get("max_gap_min", 999999))
        if not first.startswith("2026-09-28 00:"):
            audit_flags.append("SCAN28_FIRST_NOT_00XX")
        if not last.startswith("2026-09-28 23:59"):
            audit_flags.append("SCAN28_LAST_NOT_2359")
        if gap > 2.0:
            audit_flags.append(f"SCAN28_MAX_GAP={gap:.3f}m")
    else:
        audit_flags.append("SCAN28_NO_VALID_TIMES")

    status = "PASS" if not audit_flags else "REVIEW"

    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    prefix = f"FINAL_FIXED_ENTRY_0901_0928_{stamp}"
    out_all = R / f"{prefix}_CANDIDATES.csv"
    out_tr = R / f"{prefix}_TRADES.csv"
    out_bl = R / f"{prefix}_BLOCKS.csv"
    out_day = R / f"{prefix}_DAILY.csv"
    out_stop = R / f"{prefix}_STOP_ONLY.csv"
    out_stoplike = R / f"{prefix}_STOPLIKE.csv"
    out_ref = R / f"{prefix}_MARKET_BASE_REF.csv"
    out_trans = R / f"{prefix}_REGIME_TRANSITIONS.csv"
    out_chg = R / f"{prefix}_RESCHEDULE_CHANGES.csv"
    out_sum = R / f"{prefix}_SUMMARY.txt"
    out_zip = R / f"{prefix}_RESULTS.zip"

    write_csv(out_all, allrows)
    write_csv(out_tr, trades)
    write_csv(out_bl, blocks)
    write_csv(out_day, daily)
    write_csv(out_stop, strict_stops)
    write_csv(out_stoplike, stoplike)
    write_csv(out_ref, ref_rows)
    write_csv(out_trans, sw.transitions)
    write_csv(out_chg, changes)

    summary = [
        "FINAL FIXED ENTRY REPLAY 2026-09-01 ~ 2026-09-28 KST",
        "Recovery/DCA/Observer: NOT APPLIED",
        "",
        "[FIXED ENTRY STACK]",
        "V25 confirmed",
        "-> SAFE(C/RN/RS) + SAFE RELAX",
        "-> existing MKT100 (BTC/ETH 4h both within +/-0.08%)",
        "-> market condition: rolling V25 2h >=8 AND mean(abs(BTC4h),abs(ETH4h)) >=0.40%",
        "-> performance regime BASE_REF: N=9, ON->OFF at +9%p, OFF->ON at -6%p",
        "-> V22 quality OR LOCK0",
        "-> 4 slots / rolling15m max2 / CD90 / STOP-CD180 / STOP-pause45 (2 stop-like exits)",
        "-> base exits only: TP2.0 / PP12 / Final4 / V27-1 fallback",
        "",
        "[COUNTS]",
        f"V25 candidates={len(allrows)} HIST={len(hset)} FORWARD={len(fset)}",
        f"accepted entries={len(trades)}",
        f"blocks={len(blocks)}",
        f"strict STOP={len(strict_stops)}",
        f"STOP-like(STOP+LATE)={len(stoplike)}",
        f"net={accepted_net:+.6f}%p",
        f"outcomes={dict(outcome)}",
        f"block_counts={dict(blockc)}",
        f"market BASE_REF events={len(ref_events)}",
        f"regime transitions={len(sw.transitions)}",
        "",
        "[REPLACEMENT/RESCHEDULE]",
        "Every candidate was re-evaluated chronologically after blocks.",
        f"legacy30 comparison rows changed={len(changes)}",
        f"new final entries vs legacy30={sum(x['change_type']=='NEW_FINAL_ENTRY_VS_LEGACY30' for x in changes)}",
        f"lost legacy30 entries={sum(x['change_type']=='LOST_LEGACY30_ENTRY' for x in changes)}",
        "",
        "[V22 WATCH COVERAGE]",
        f"hist missing={qmiss_hist}",
        f"forward pre-9/28 missing={qmiss_fwd_pre28}",
        f"9/28 missing={qmiss_28}",
        "Missing exact WATCH => not blocked, matching the frozen V22 rule.",
        "",
        "[MARKET TELEMETRY]",
        f"BTC/ETH 4h missing candidates={market_missing}",
        "",
        "[9/28 SCAN COVERAGE]",
        json.dumps(cov28, ensure_ascii=False, sort_keys=True),
        "",
        "[AUDIT]",
        f"status={status}",
        "flags=" + ("; ".join(audit_flags) if audit_flags else "NONE"),
        f"data_errors={data_errors}",
        "",
        "[SOURCE SHA256]",
    ]
    summary += [f"{k}={v}" for k, v in source_hashes.items()]
    summary += [
        "",
        "[IMPORTANT]",
        "This ZIP is the fixed ENTRY baseline.",
        "Do not use older STOP/B1 cohorts for the next Recovery study.",
        "Next Recovery research must start from *_STOP_ONLY.csv generated here.",
    ]
    out_sum.write_text("\n".join(summary) + "\n", encoding="utf-8")

    files = [
        out_all, out_tr, out_bl, out_day, out_stop, out_stoplike,
        out_ref, out_trans, out_chg, out_sum,
    ]
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for p in files:
            z.write(p, arcname=p.name)

    print("\n".join(summary), flush=True)
    print("RESULT_ZIP=" + str(out_zip), flush=True)

if __name__ == "__main__":
    main()
