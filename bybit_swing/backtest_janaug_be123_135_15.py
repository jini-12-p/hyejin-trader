#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JAN-AUG 2026 CURRENT fixed-entry BE overlay replay

Purpose
-------
Keep the exact CURRENT accepted entry cohort (5,925 trades from the latest
CURRENT output) and replace current PP12 with one of three BE rules:
  A) arm at +1.2% MFE -> protect at entry price (gross 0% BE)
  B) arm at +1.3% MFE -> protect at entry price
  C) arm at +1.5% MFE -> protect at entry price

Everything else stays frozen to unified_current_0901_0922.py:
  TP +2.0% FULL
  Final 4-stage stop
  V26 late failure
  V27-1 staged fallback
  flat/time exits
  taker fee model

Important
---------
- This is a FIXED-ENTRY cohort replay: it does NOT add replacement entries when
  earlier BE exits free a slot. That is intentional for first-pass exit-quality
  comparison. Once a BE threshold is selected, portfolio rescheduling can be
  replayed separately.
- PP12 is REPLACED, not layered on top of BE.
- 1m OHLC cannot determine intraminute high/low order. Two deterministic bounds
  are reported for all three thresholds in one shared replay:
    CONSERVATIVE: first arm bar does not use that bar's low; once armed, if BE
                  and TP are both touched in one 1m bar, BE wins.
    OPTIMISTIC:   first arm bar may BE if arm-high and BE-low are both touched;
                  once armed, if BE and TP are both touched, TP wins.
- The frozen BASE convention that simultaneous TP and -3% hard-stop in a
  pre-arm minute resolves to hard-stop is preserved.

No candidate generation. No live/private orders. Existing Jan-Aug kline cache is reused.
"""
from __future__ import annotations

import argparse
import gc
import importlib.util
import json
import math
import sys
import time
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

UTC = timezone.utc
KST = timezone(timedelta(hours=9))
ARMS = (1.20, 1.30, 1.50)
MODES = ("CONSERVATIVE", "OPTIMISTIC")
CACHE_MEM_MAX = 32
PROGRESS_EVERY = 250
SCRIPT_VERSION = "JANAUG_BE_12_13_15_FIXED_COHORT_v1_20261002"


def p(*a):
    print(*a, flush=True)


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def fnum(v, default=0.0):
    try:
        x = float(v)
        return default if math.isnan(x) else x
    except Exception:
        return default


def parse_dt_utc(v) -> datetime:
    s = str(v or "").strip()
    if not s or s.lower() == "nan":
        raise ValueError("empty datetime")
    dt = pd.Timestamp(s).to_pydatetime()
    if dt.tzinfo is None:
        # CURRENT entry_time_utc is normally aware. Naive fallback is UTC.
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_exit_kst(v) -> datetime:
    s = str(v or "").strip()
    if not s or s.lower() == "nan":
        raise ValueError("empty exit datetime")
    dt = pd.Timestamp(s).to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt.astimezone(UTC)


def find_latest_current_trades(root: Path) -> Path:
    pats = [
        "CURRENT_VS_NOFILTER_JANAUG_2026_*_CURRENT_TRADES.csv",
        "CURRENT_P_JANAUG_2026_*_TRADES.csv",
    ]
    files = []
    for pat in pats:
        files.extend(root.glob(pat))
    files = [x for x in files if x.is_file()]
    if not files:
        raise FileNotFoundError("No CURRENT Jan-Aug trades CSV found")
    return max(files, key=lambda x: x.stat().st_mtime)


@dataclass
class ScenarioState:
    arm_pct: float
    mode: str
    remaining: float = 1.0
    fills: list = field(default_factory=list)
    done: bool = False
    result_obj: Any = None
    armed: bool = False
    arm_bar_ts: datetime | None = None
    arm_time: datetime | None = None
    cp10: bool = False
    cp15: bool = False
    cp25: bool = False
    cp30: bool = False
    p25_pnl: float | None = None
    late_streak: int = 0
    stage_active: bool = False
    stage_start: datetime | None = None
    stage_signal: float = 0.0
    stop_stage: str = ""
    ambiguous_arm_bar: bool = False
    ambiguous_tp_be: bool = False


def replay_one_multi(U, row: dict[str, Any]) -> dict[tuple[float, str], Any]:
    symbol = str(row["symbol"])
    entry_t = parse_dt_utc(row.get("entry_time_utc") or row.get("entry_ts_utc"))
    entry = float(row["entry_price"])

    states = {(a, m): ScenarioState(a, m) for a in ARMS for m in MODES}

    try:
        client = U.ReplayClient(symbol, entry_t)
        path = client.path_1m(entry_t, 186)
        if len(path) == 0:
            raise RuntimeError("no 1m path")
    except Exception as e:
        out = {}
        for k in states:
            out[k] = U.SimResult("DATA_ERROR", entry_t, entry, [], 0, 0, 0, 0, 0, data_error=str(e))
        return out

    CFG = U.CFG
    BOT = U.BOT
    TP_PCT = float(U.TP_PCT)
    mfe = 0.0
    mae = 0.0
    hard = entry * (1.0 - abs(float(getattr(CFG, "research_pv271_disaster_stop_pct", 3.0))) / 100.0)
    rebound_pct = float(getattr(CFG, "research_pv271_rebound_from_signal_pct", 0.50))
    stage_wait = float(getattr(CFG, "research_pv271_wait_minutes", 3.0))
    stage_frac = float(getattr(CFG, "research_pv271_stage_fraction", 0.50))
    max_hold_min = float(getattr(CFG, "max_hold_hours", 3)) * 60.0
    tp = entry * (1.0 + TP_PCT / 100.0)

    def close_frac(st: ScenarioState, frac: float, px: float, kind: str):
        q = min(st.remaining, max(0.0, float(frac)))
        if q > 1e-12:
            st.fills.append(U.Fill(q, float(px), kind))
            st.remaining -= q

    def finish(st: ScenarioState, reason: str, now: datetime, px: float, kind: str, detail: str = ""):
        if st.done:
            return
        close_frac(st, st.remaining, px, kind)
        extras = {
            "arm_pct": st.arm_pct,
            "mode": st.mode,
            "armed": st.armed,
            "arm_time": "" if st.arm_time is None else st.arm_time.isoformat(),
            "ambiguous_arm_bar": st.ambiguous_arm_bar,
            "ambiguous_tp_be": st.ambiguous_tp_be,
        }
        d = detail
        if d:
            d += "; "
        d += json.dumps(extras, ensure_ascii=False, default=str)
        st.result_obj = U.result_from_core(entry, st.fills, reason, now, px, mfe, mae, st.stop_stage, d)
        st.done = True

    for _, bar in path.iterrows():
        if all(st.done for st in states.values()):
            break

        bar_t = pd.Timestamp(bar["ts"]).to_pydatetime().astimezone(UTC)
        now = bar_t + timedelta(minutes=1)
        client.set_now(now)
        age = (now - entry_t).total_seconds() / 60.0
        high = float(bar["high"])
        low = float(bar["low"])
        price = float(bar["close"])
        mfe = max(mfe, (high / entry - 1.0) * 100.0)
        mae = min(mae, (low / entry - 1.0) * 100.0)
        pnl = (price / entry - 1.0) * 100.0
        five_due = (now.minute % 5 == 0)
        fifteen_due = (now.minute % 15 == 0)

        # First handle already-active V27-1 stages per scenario. BE/TP does not jump ahead of stage.
        for st in states.values():
            if st.done or not st.stage_active or st.stage_start is None:
                continue
            rb = st.stage_signal * (1.0 + rebound_pct / 100.0)
            stage_age = (now - st.stage_start).total_seconds() / 60.0
            hit_d = low <= hard
            hit_r = high >= rb
            if hit_d and hit_r:
                finish(st, "STOP", now, hard, "V271_STAGE_DISASTER_AMBIG", "stage hard+rebound same 1m")
            elif hit_d:
                finish(st, "STOP", now, hard, "V271_STAGE_DISASTER", "stage disaster")
            elif hit_r:
                finish(st, "STOP", now, rb, "V271_STAGE_REBOUND", "stage rebound close")
            elif stage_age >= stage_wait:
                finish(st, "STOP", now, price, "V271_STAGE_TIMEOUT", "stage 3m timeout")

        # BE/TP/hard before Final4 / V26 / V27 for non-stage scenarios.
        for st in states.values():
            if st.done or st.stage_active:
                continue
            arm_px = entry * (1.0 + st.arm_pct / 100.0)

            # Already armed before this 1m: BE and TP are both live.
            if st.armed and st.arm_bar_ts is not None and bar_t > st.arm_bar_ts:
                hit_tp = high >= tp
                hit_be = low <= entry
                if hit_tp and hit_be:
                    st.ambiguous_tp_be = True
                    if st.mode == "CONSERVATIVE":
                        st.stop_stage = f"BE{st.arm_pct:.2f}_AMBIG_BE_FIRST"
                        finish(st, f"BE{int(round(st.arm_pct*100)):03d}_EXIT", now, entry, "BE", "armed prior; TP/BE same 1m; CONSERVATIVE=BE")
                    else:
                        finish(st, "TP20_FULL", now, tp, "TP20_FULL", "armed prior; TP/BE same 1m; OPTIMISTIC=TP")
                    continue
                if hit_be:
                    st.stop_stage = f"BE{st.arm_pct:.2f}_RETURN"
                    finish(st, f"BE{int(round(st.arm_pct*100)):03d}_EXIT", now, entry, "BE", "armed prior; returned to entry")
                    continue
                if hit_tp:
                    finish(st, "TP20_FULL", now, tp, "TP20_FULL", "armed prior; +2.0% TP")
                    continue

            hit_tp = high >= tp
            hit_hard = low <= hard

            # Preserve frozen BASE hard priority for pre-arm TP/hard ambiguity.
            if hit_tp and hit_hard:
                finish(st, "STOP", now, hard, "V271_DISASTER_AMBIG", "pre-arm TP and -3% hard same 1m; BASE hard priority")
                continue
            if hit_hard:
                if st.mode == "OPTIMISTIC" and high >= arm_px and low <= entry:
                    st.ambiguous_arm_bar = True
                    st.armed = True
                    st.arm_bar_ts = bar_t
                    st.arm_time = now
                    st.stop_stage = f"BE{st.arm_pct:.2f}_SAMEBAR_OPT"
                    finish(st, f"BE{int(round(st.arm_pct*100)):03d}_EXIT", now, entry, "BE", "first arm bar also hit hard; OPTIMISTIC=BE")
                else:
                    finish(st, "STOP", now, hard, "V271_DISASTER", "direct -3% disaster")
                continue
            if hit_tp:
                finish(st, "TP20_FULL", now, tp, "TP20_FULL", "+2.0% full TP")
                continue

            if (not st.armed) and high >= arm_px:
                st.armed = True
                st.arm_bar_ts = bar_t
                st.arm_time = now
                if low <= entry:
                    st.ambiguous_arm_bar = True
                    if st.mode == "OPTIMISTIC":
                        st.stop_stage = f"BE{st.arm_pct:.2f}_SAMEBAR_OPT"
                        finish(st, f"BE{int(round(st.arm_pct*100)):03d}_EXIT", now, entry, "BE", "same first-arm bar ambiguity; OPTIMISTIC=BE")

        # Final4 checkpoints per active, non-stage scenario. PP12 intentionally absent.
        for st in states.values():
            if st.done or st.stage_active:
                continue
            if not st.cp10 and age >= 10.0:
                st.cp10 = True
                if pnl <= -1.00 and mfe <= 0.10:
                    close_frac(st, st.remaining * 0.50, price, "STOP10_DEAD50")
                    st.stop_stage = "STOP10_DEAD50"
            if not st.cp15 and age >= 15.0:
                st.cp15 = True
                if pnl <= -1.50:
                    st.stop_stage = "STOP15_FULL"
                    finish(st, "STOP", now, price, "STOP15_FULL")
                    continue
            if not st.cp25 and age >= 25.0:
                st.cp25 = True
                st.p25_pnl = pnl
                if 0.30 <= mfe <= 1.20 and pnl <= -1.20:
                    close_frac(st, st.remaining * 0.50, price, "STOP25_GIVEBACK50")
                    st.stop_stage = "STOP25_GIVEBACK50"
            if not st.cp30 and age >= 30.0:
                st.cp30 = True
                delta5 = (pnl - st.p25_pnl) if st.p25_pnl is not None else None
                if pnl <= -0.90 and delta5 is not None and delta5 <= -0.30:
                    st.stop_stage = "STOP30_DETERIORATION"
                    finish(st, "STOP", now, price, "STOP30_DETERIORATION", f"delta5={delta5:.4f}")

        active = [st for st in states.values() if not st.done and not st.stage_active]
        if active:
            # Shared V26 late-failure observation; streak is scenario state.
            late = False
            ld = {}
            if five_due and bool(getattr(CFG, "research_pv26_late_failure_enabled", True)):
                try:
                    late, ld = BOT.pv26_late_failure_signal(client, symbol, entry, price, age, mfe, CFG)
                except Exception:
                    late, ld = False, {}
                req = max(1, int(getattr(CFG, "research_pv26_late_confirmations", 2)))
                for st in active:
                    st.late_streak = st.late_streak + 1 if late else 0
                    if late and st.late_streak >= req:
                        st.stop_stage = "V26_LATE_FAILURE"
                        finish(st, "LATE_FAILURE_EXIT", now, price, "LATE_FAILURE_EXIT", json.dumps(ld, ensure_ascii=False, default=str)[:500])

            active = [st for st in states.values() if not st.done and not st.stage_active]
            if five_due and active:
                stop_hit = False
                stop_type = ""
                meta = {}
                try:
                    ok, d = BOT.early_crash_failure_signal(client, symbol, entry_t.isoformat(), entry, price, CFG)
                    if ok:
                        stop_hit, stop_type, meta = True, "EARLY_CRASH", d
                except Exception as e:
                    meta = {"early_crash_error": str(e)}
                if not stop_hit:
                    try:
                        ok, d = BOT.p_catastrophic_failure_signal(client, symbol, entry_t.isoformat(), entry, price, CFG)
                        if ok:
                            stop_hit, stop_type, meta = True, "P_CATASTROPHIC", d
                    except Exception as e:
                        meta["p_cat_error"] = str(e)
                if not stop_hit and bool(getattr(CFG, "early_failure_enabled", True)):
                    try:
                        ok, d = BOT.early_failure_signal(client, symbol, entry_t.isoformat(), CFG)
                        if ok:
                            stop_hit, stop_type, meta = True, str(d.get("failure_type") or "EARLY_FAILURE"), d
                    except Exception as e:
                        meta["early_failure_error"] = str(e)
                if not stop_hit and age >= 45.0:
                    try:
                        fake_entry_ms = int((time.time() - age * 60.0) * 1000)
                        ok, d = BOT.late_trend_failure_signal(client, symbol, fake_entry_ms, False)
                        if ok:
                            stop_hit, stop_type, meta = True, "LATE_TREND_FAILURE", d
                    except Exception as e:
                        meta["late_failure_error"] = str(e)
                if stop_hit:
                    for st in active:
                        st.stage_active = True
                        st.stage_start = now
                        st.stage_signal = price
                        close_frac(st, st.remaining * max(0.05, min(0.95, stage_frac)), price, f"V271_STAGE1_{stop_type}")
                        st.stop_stage = f"V271_STAGE1_{stop_type}"

            active = [st for st in states.values() if not st.done and not st.stage_active]
            emergency_now = price <= entry * (1.0 - abs(float(getattr(CFG, "structure_emergency_stop_pct", 8.0))) / 100.0)
            if active and (fifteen_due or emergency_now):
                try:
                    broken, sd = BOT.hj_structure_broken(client, symbol, CFG, base_price=entry, live_price=price)
                except Exception:
                    broken, sd = False, {}
                if broken:
                    sig = U.f(sd.get("price"), price) or price
                    for st in active:
                        st.stage_active = True
                        st.stage_start = now
                        st.stage_signal = sig
                        close_frac(st, st.remaining * max(0.05, min(0.95, stage_frac)), sig, "V271_STAGE1_STRUCTURE")
                        st.stop_stage = "V271_STAGE1_STRUCTURE"

            active = [st for st in states.values() if not st.done and not st.stage_active]
            if active and fifteen_due and age >= float(getattr(CFG, "flat_exit_minutes", 60)) and mfe < float(getattr(CFG, "flat_min_favorable_pct", 1.0)):
                try:
                    flat, fd = BOT.flat_exit_signal(client, symbol, entry, CFG)
                except Exception:
                    flat, fd = False, {}
                if flat:
                    for st in active:
                        finish(st, "FLAT_EXIT_75M", now, price, "FLAT_EXIT", json.dumps(fd, ensure_ascii=False, default=str)[:500])

        if age >= max_hold_min:
            for st in states.values():
                if not st.done:
                    finish(st, "TIME_EXIT", now, price, "TIME_EXIT")

    if not all(st.done for st in states.values()):
        last = path.iloc[-1]
        et = pd.Timestamp(last["ts"]).to_pydatetime().astimezone(UTC) + timedelta(minutes=1)
        px = float(last["close"])
        for st in states.values():
            if not st.done:
                finish(st, "TIME_EXIT", et, px, "TIME_EXIT_EOD")

    return {k: st.result_obj for k, st in states.items()}


def summarize(df: pd.DataFrame, arm: float, mode: str) -> dict[str, Any]:
    tag = f"A{int(round(arm*100)):03d}_{mode}"
    netcol = f"{tag}_net_pct"
    rescol = f"{tag}_result"
    delta = f"{tag}_delta_pct"
    total = float(pd.to_numeric(df[netcol], errors="coerce").fillna(0).sum())
    base_total = float(pd.to_numeric(df["base_net_pct"], errors="coerce").fillna(0).sum())
    rc = Counter(df[rescol].astype(str))
    return {
        "arm_pct": arm,
        "mode": mode,
        "trades": len(df),
        "net_pct": total,
        "base_net_pct": base_total,
        "delta_pct": total - base_total,
        "TP20_FULL": rc.get("TP20_FULL", 0),
        "BE_EXIT": sum(v for k,v in rc.items() if k.startswith("BE")),
        "STOP": rc.get("STOP", 0),
        "LATE_FAILURE_EXIT": rc.get("LATE_FAILURE_EXIT", 0),
        "FLAT_EXIT_75M": rc.get("FLAT_EXIT_75M", 0),
        "TIME_EXIT": rc.get("TIME_EXIT", 0),
        "DATA_ERROR": rc.get("DATA_ERROR", 0),
        "tp_impaired": int(((df["base_result"] == "TP20_FULL") & (df[rescol] != "TP20_FULL")).sum()),
        "base_loss_to_be": int(((df["base_net_pct"] < 0) & df[rescol].astype(str).str.startswith("BE")).sum()),
        "base_pp_to_be": int(((df["base_result"] == "PROFIT_PROTECT_EXIT") & df[rescol].astype(str).str.startswith("BE")).sum()),
        "improved_trades": int((pd.to_numeric(df[delta], errors="coerce").fillna(0) > 1e-9).sum()),
        "worsened_trades": int((pd.to_numeric(df[delta], errors="coerce").fillna(0) < -1e-9).sum()),
    }


def daily_monthly(df: pd.DataFrame, arm: float, mode: str):
    tag = f"A{int(round(arm*100)):03d}_{mode}"
    netcol = f"{tag}_net_pct"
    x = df.copy()
    x["date"] = pd.to_datetime(x["entry_time_kst"], errors="coerce").dt.strftime("%Y-%m-%d")
    x["month"] = pd.to_datetime(x["entry_time_kst"], errors="coerce").dt.strftime("%Y-%m")
    x[netcol] = pd.to_numeric(x[netcol], errors="coerce").fillna(0)
    x["base_net_pct"] = pd.to_numeric(x["base_net_pct"], errors="coerce").fillna(0)
    d = x.groupby("date", dropna=False).agg(entries=("setup_id","size"), net_pct=(netcol,"sum"), base_net_pct=("base_net_pct","sum")).reset_index()
    d["delta_pct"] = d["net_pct"] - d["base_net_pct"]
    m = x.groupby("month", dropna=False).agg(entries=("setup_id","size"), net_pct=(netcol,"sum"), base_net_pct=("base_net_pct","sum")).reset_index()
    m["delta_pct"] = m["net_pct"] - m["base_net_pct"]
    return d,m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/root/hyejin-trader/bybit_swing")
    args = ap.parse_args()
    root = Path(args.root).expanduser().resolve()
    unified_path = root / "unified_current_0901_0922.py"
    if not unified_path.exists():
        raise SystemExit(f"missing {unified_path}")

    U = load_module("U_JANAUG_BE_TEST", unified_path)
    U.START_KST = datetime(2026,1,1,tzinfo=KST)
    U.END_KST = datetime(2026,9,1,tzinfo=KST)
    U.START_UTC = U.START_KST.astimezone(UTC)
    U.END_UTC = U.END_KST.astimezone(UTC)
    U.EXPECTED_V25 = -1
    U.CACHE_DIR = root / ".janaug_exit_cache_v1"
    U.CACHE_DIR.mkdir(exist_ok=True)
    U.KC = U.KlineCache()

    base_path = find_latest_current_trades(root)
    base = pd.read_csv(base_path, low_memory=False)
    if "accepted" in base.columns:
        base = base[pd.to_numeric(base["accepted"], errors="coerce").fillna(1).astype(int)==1].copy()
    if len(base) != 5925:
        p(f"WARNING: expected 5925 CURRENT trades, found {len(base)} in {base_path.name}")
    p(f"BASE_TRADES={base_path}")
    p(f"CURRENT_TRADES={len(base)} base_net={pd.to_numeric(base['net_pct'],errors='coerce').fillna(0).sum():.6f}%p")
    p("SCENARIOS=ARM1.2->BE, ARM1.3->BE, ARM1.5->BE; PP12 replaced; fixed entry cohort")

    rows=[]
    for i,r in enumerate(base.to_dict("records"),1):
        rec = {
            "setup_id": str(r.get("setup_id") or ""),
            "symbol": str(r.get("symbol") or ""),
            "entry_time_kst": r.get("entry_time_kst"),
            "entry_time_utc": r.get("entry_time_utc") or r.get("entry_ts_utc"),
            "entry_price": fnum(r.get("entry_price")),
            "base_result": str(r.get("result") or ""),
            "base_stop_stage": str(r.get("stop_stage") or ""),
            "base_mfe_pct": fnum(r.get("mfe_pct")),
            "base_mae_pct": fnum(r.get("mae_pct")),
            "base_net_pct": fnum(r.get("net_pct")),
        }
        sims = replay_one_multi(U, r)
        for (arm,mode),s in sims.items():
            tag = f"A{int(round(arm*100)):03d}_{mode}"
            rec[f"{tag}_result"] = s.result
            rec[f"{tag}_exit_time_kst"] = s.exit_time.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
            rec[f"{tag}_stop_stage"] = s.stop_stage
            rec[f"{tag}_gross_pct"] = s.gross_pct
            rec[f"{tag}_fee_pct"] = s.fee_pct
            rec[f"{tag}_net_pct"] = s.net_pct
            rec[f"{tag}_delta_pct"] = s.net_pct - rec["base_net_pct"]
            rec[f"{tag}_mfe_pct"] = s.mfe_pct
            rec[f"{tag}_mae_pct"] = s.mae_pct
            rec[f"{tag}_data_error"] = s.data_error
            rec[f"{tag}_detail"] = s.detail
        rows.append(rec)
        if i % PROGRESS_EVERY == 0 or i == len(base):
            p(f"[BE] {i}/{len(base)}")
            try:
                if hasattr(U,"KC") and hasattr(U.KC,"mem") and len(U.KC.mem) > CACHE_MEM_MAX:
                    U.KC.mem.clear(); gc.collect()
            except Exception:
                pass

    out = pd.DataFrame(rows)
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    pref = f"CURRENT_BE_12_13_15_JANAUG_2026_{stamp}"
    trades_out = root / f"{pref}_TRADES.csv"
    summary_out = root / f"{pref}_SUMMARY.csv"
    txt_out = root / f"{pref}_SUMMARY.txt"
    zip_out = root / f"{pref}_RESULTS.zip"
    out.to_csv(trades_out,index=False,encoding="utf-8-sig")

    summaries=[]
    daily_files=[]; monthly_files=[]
    for arm in ARMS:
        for mode in MODES:
            summaries.append(summarize(out,arm,mode))
            d,m = daily_monthly(out,arm,mode)
            tag=f"A{int(round(arm*100)):03d}_{mode}"
            dp=root/f"{pref}_{tag}_DAILY.csv"; mp=root/f"{pref}_{tag}_MONTHLY.csv"
            d.to_csv(dp,index=False,encoding="utf-8-sig")
            m.to_csv(mp,index=False,encoding="utf-8-sig")
            daily_files.append(dp); monthly_files.append(mp)
    sdf=pd.DataFrame(summaries)
    sdf.to_csv(summary_out,index=False,encoding="utf-8-sig")

    lines=[
        f"SCRIPT_VERSION={SCRIPT_VERSION}",
        f"BASE_TRADES={base_path}",
        f"TRADES={len(out)}",
        f"BASE_NET={out['base_net_pct'].sum():.6f}%p",
        "PP12 is REPLACED by BE arm rule; CURRENT entry cohort is fixed.",
        "CONSERVATIVE: first-arm bar low ignored; after arm TP+BE same 1m => BE.",
        "OPTIMISTIC: first-arm same-bar BE allowed; after arm TP+BE same 1m => TP.",
        "",
    ]
    for s in summaries:
        lines.append(
            f"ARM={s['arm_pct']:.2f} MODE={s['mode']} NET={s['net_pct']:.6f}%p DELTA={s['delta_pct']:+.6f}%p "
            f"TP={s['TP20_FULL']} BE={s['BE_EXIT']} STOP={s['STOP']} LATE={s['LATE_FAILURE_EXIT']} "
            f"FLAT={s['FLAT_EXIT_75M']} TIME={s['TIME_EXIT']} DATAERR={s['DATA_ERROR']} "
            f"TP_IMPAIRED={s['tp_impaired']} BASE_LOSS_TO_BE={s['base_loss_to_be']} BASE_PP_TO_BE={s['base_pp_to_be']} "
            f"IMPROVED={s['improved_trades']} WORSENED={s['worsened_trades']}"
        )
    txt_out.write_text("\n".join(lines)+"\n",encoding="utf-8")

    with zipfile.ZipFile(zip_out,"w",compression=zipfile.ZIP_DEFLATED) as z:
        for fp in [trades_out,summary_out,txt_out,*daily_files,*monthly_files]:
            z.write(fp,arcname=fp.name)

    p("\n".join(lines))
    p("RESULT_ZIP="+str(zip_out))

if __name__ == "__main__":
    main()
