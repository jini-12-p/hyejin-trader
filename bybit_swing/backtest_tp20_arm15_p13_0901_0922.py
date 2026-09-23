#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TP2.0 + ARM1.5 + PROFIT PROTECT1.3 exact 1m replay
Period: 2026-09-01 ~ 2026-09-22 KST

Uses unified_current_0901_0922.py as the frozen source of:
- V25 setups / SAFE / RELAX / MKT100
- current 4-stage stop
- V27-1 fallback
- Bybit 1m/5m/15m causal replay
- fee model

This script tests replacing current PP12 with:
- TP +2.0% FULL
- ARM when price reaches +1.5%
- after ARM, full profit-protect exit at +1.3%

1-minute OHLC cannot identify intraminute high/low order, so two cohort-fixed bounds are reported:
- WORST: ambiguous bars are resolved against the new protection.
  * first ARM bar cannot immediately use that bar's low for P13
  * if already armed and the same 1m bar touches TP2.0 and P13, P13 wins
- BEST: ambiguous bars are resolved in favor of the new protection/TP.
  * first ARM bar may protect at P13 if its low also touched P13
  * if already armed and the same 1m bar touches TP2.0 and P13, TP2.0 wins

Also runs a full portfolio-rescheduled replay using WORST rules:
4 slots / rolling 15m max2 / 90m cooldown / STOP-LATE 180m / recent STOP pause.
PROTECT13 is a profitable exit, so it does NOT count as STOP/LATE for 180m cooldown or STOP pause.
"""
from __future__ import annotations

import csv
import importlib.util
import json
import math
import os
import sys
import time
import zipfile
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent
UNIFIED_PATH = ROOT / "unified_current_0901_0922.py"
BASE_TRADES_PATH = ROOT / "UNIFIED_CURRENT_0901_0922_TRADES.csv"
BASE_DAILY_PATH = ROOT / "UNIFIED_CURRENT_0901_0922_DAILY.csv"

OUT_COHORT_TRADES = ROOT / "TP20_ARM15_P13_COHORT_TRADES.csv"
OUT_COHORT_DAILY = ROOT / "TP20_ARM15_P13_COHORT_DAILY.csv"
OUT_PORT_TRADES = ROOT / "TP20_ARM15_P13_PORTFOLIO_TRADES.csv"
OUT_PORT_DAILY = ROOT / "TP20_ARM15_P13_PORTFOLIO_DAILY.csv"
OUT_SUMMARY = ROOT / "TP20_ARM15_P13_SUMMARY.txt"
OUT_ZIP = ROOT / "TP20_ARM15_P13_RESULTS.zip"

ARM_PCT = 1.50
PROTECT_PCT = 1.30
SCRIPT_VERSION = "TP20_ARM15_P13_1M_v1_20260923"

if not UNIFIED_PATH.exists():
    raise SystemExit(f"missing: {UNIFIED_PATH}")
if not BASE_TRADES_PATH.exists():
    raise SystemExit(f"missing: {BASE_TRADES_PATH}; run unified_current_0901_0922.py first")
if not BASE_DAILY_PATH.exists():
    raise SystemExit(f"missing: {BASE_DAILY_PATH}; run unified_current_0901_0922.py first")

spec = importlib.util.spec_from_file_location("U090122", str(UNIFIED_PATH))
if spec is None or spec.loader is None:
    raise SystemExit("cannot load unified_current_0901_0922.py")
U = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = U
spec.loader.exec_module(U)

# Aliases from frozen unified script.
UTC = U.UTC
KST = U.KST
BOT = U.BOT
CFG = U.CFG
TP_PCT = U.TP_PCT
FEE_PCT = U.FEE_PCT
pd = U.pd


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    keys = []
    seen = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                keys.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        w = csv.DictWriter(fp, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def simulate_arm15_p13(s: dict[str, Any], mode: str = "WORST") -> U.SimResult:
    """Current BASE replay with PP12 replaced by ARM1.5 -> P13 full protection.

    mode:
      WORST = conservative intraminute ordering for the new rule.
      BEST  = optimistic boundary for unavoidable 1m ordering ambiguity.
    """
    mode = str(mode).upper()
    if mode not in {"WORST", "BEST"}:
        raise ValueError(mode)

    symbol = s["symbol"]
    entry_t = s["entry"]
    entry = float(s["entry_price"])

    try:
        client = U.ReplayClient(symbol, entry_t)
        path = client.path_1m(entry_t, 186)
        if len(path) == 0:
            raise RuntimeError("no 1m path")
    except Exception as e:
        return U.SimResult("DATA_ERROR", entry_t, entry, [], 0, 0, 0, 0, 0, data_error=str(e))

    remaining = 1.0
    fills: list[U.Fill] = []
    mfe = 0.0
    mae = 0.0
    arm_active = False
    arm_bar_ts = None
    arm_time = None
    ambiguous_arm_bar = False
    ambiguous_tp_protect = False

    cp10 = cp15 = cp25 = cp30 = False
    p25_pnl = None
    late_streak = 0
    stage_active = False
    stage_start = None
    stage_signal = 0.0
    stop_stage = ""

    hard = entry * (1.0 - abs(float(getattr(CFG, "research_pv271_disaster_stop_pct", 3.0))) / 100.0)
    rebound_pct = float(getattr(CFG, "research_pv271_rebound_from_signal_pct", 0.50))
    stage_wait = float(getattr(CFG, "research_pv271_wait_minutes", 3.0))
    stage_frac = float(getattr(CFG, "research_pv271_stage_fraction", 0.50))
    max_hold_min = float(getattr(CFG, "max_hold_hours", 3)) * 60.0

    tp = entry * (1.0 + TP_PCT / 100.0)
    arm_px = entry * (1.0 + ARM_PCT / 100.0)
    protect_px = entry * (1.0 + PROTECT_PCT / 100.0)

    def close_frac(frac: float, px: float, kind: str):
        nonlocal remaining
        q = min(remaining, max(0.0, float(frac)))
        if q > 1e-12:
            fills.append(U.Fill(q, float(px), kind))
            remaining -= q

    def finish(reason: str, now, px: float, kind: str, detail: str = "") -> U.SimResult:
        close_frac(remaining, px, kind)
        extras = {
            "mode": mode,
            "arm_pct": ARM_PCT,
            "protect_pct": PROTECT_PCT,
            "arm_active": arm_active,
            "arm_time": "" if arm_time is None else arm_time.isoformat(),
            "ambiguous_arm_bar": ambiguous_arm_bar,
            "ambiguous_tp_protect": ambiguous_tp_protect,
        }
        d = detail
        if d:
            d += "; "
        d += json.dumps(extras, ensure_ascii=False, default=str)
        return U.result_from_core(entry, fills, reason, now, px, mfe, mae, stop_stage, d)

    for _, bar in path.iterrows():
        bar_t = pd.Timestamp(bar["ts"]).to_pydatetime().astimezone(UTC)
        now = bar_t + timedelta(minutes=1)
        client.set_now(now)
        age = (now - entry_t).total_seconds() / 60.0
        high = float(bar["high"])
        low = float(bar["low"])
        price = float(bar["close"])

        mfe = max(mfe, (high / entry - 1.0) * 100.0)
        mae = min(mae, (low / entry - 1.0) * 100.0)

        # If V27-1 staged stop is already active, preserve the frozen current behavior.
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

        # If ARM was already active before this minute, TP/P13 are both live.
        if arm_active and arm_bar_ts is not None and bar_t > arm_bar_ts:
            hit_tp = high >= tp
            hit_p13 = low <= protect_px
            if hit_tp and hit_p13:
                ambiguous_tp_protect = True
                if mode == "WORST":
                    stop_stage = "ARM15_P13_AMBIG_PROTECT"
                    return finish("PROTECT13_EXIT", now, protect_px, "ARM15_P13", "armed prior; TP2/P13 same 1m; WORST=P13")
                return finish("TP20_FULL", now, tp, "TP20_FULL", "armed prior; TP2/P13 same 1m; BEST=TP2")
            if hit_p13:
                stop_stage = "ARM15_P13_PROTECT"
                return finish("PROTECT13_EXIT", now, protect_px, "ARM15_P13", "armed prior; P13 touched")
            if hit_tp:
                return finish("TP20_FULL", now, tp, "TP20_FULL", "armed prior; +2.0% TP")

        # Before ARM exists, preserve current hard-vs-TP conservative priority.
        hit_tp = high >= tp
        hit_hard = low <= hard

        # TP2 is guaranteed if high reached +2.0; however the frozen BASE uses hard priority
        # when hard and TP occur in the same 1m, so keep that exact convention pre-ARM.
        if not arm_active:
            if hit_tp and hit_hard:
                return finish("STOP", now, hard, "V271_DISASTER_AMBIG", "pre-arm TP and -3% hard same 1m; BASE hard priority")
            if hit_hard:
                # BEST bound: if this same bar also reached ARM and crossed P13 afterward,
                # 1m order is unknowable. BEST may take P13; WORST keeps BASE hard.
                if mode == "BEST" and high >= arm_px and low <= protect_px:
                    ambiguous_arm_bar = True
                    arm_active = True
                    arm_bar_ts = bar_t
                    arm_time = now
                    stop_stage = "ARM15_P13_SAMEBAR_BEST"
                    return finish("PROTECT13_EXIT", now, protect_px, "ARM15_P13", "same first-ARM bar ambiguity; BEST=P13")
                return finish("STOP", now, hard, "V271_DISASTER", "direct -3% disaster")
            if hit_tp:
                return finish("TP20_FULL", now, tp, "TP20_FULL", "+2.0% full TP")

            # First ARM bar. WORST does not use this bar's low after the unknown high/low order.
            if high >= arm_px:
                arm_active = True
                arm_bar_ts = bar_t
                arm_time = now
                if low <= protect_px:
                    ambiguous_arm_bar = True
                    if mode == "BEST":
                        stop_stage = "ARM15_P13_SAMEBAR_BEST"
                        return finish("PROTECT13_EXIT", now, protect_px, "ARM15_P13", "same first-ARM bar ambiguity; BEST=P13")
                # WORST: protection becomes live from the next 1m bar.

        five_due = (now.minute % 5 == 0)
        fifteen_due = (now.minute % 15 == 0)
        pnl = (price / entry - 1.0) * 100.0

        # Current Final 4-stage checkpoints — unchanged.
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

        # Current V26 late-failure direct exit — unchanged.
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

        # V27-1 fallback stop signals — unchanged.
        if five_due:
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
                stage_active = True
                stage_start = now
                stage_signal = price
                q = remaining * max(0.05, min(0.95, stage_frac))
                close_frac(q, stage_signal, f"V271_STAGE1_{stop_type}")
                stop_stage = f"V271_STAGE1_{stop_type}"
                continue

        # Confirmed 15m structure fallback — unchanged.
        emergency_now = price <= entry * (1.0 - abs(float(getattr(CFG, "structure_emergency_stop_pct", 8.0))) / 100.0)
        if fifteen_due or emergency_now:
            try:
                broken, sd = BOT.hj_structure_broken(client, symbol, CFG, base_price=entry, live_price=price)
            except Exception:
                broken, sd = False, {}
            if broken:
                sig = U.f(sd.get("price"), price) or price
                stage_active = True
                stage_start = now
                stage_signal = sig
                q = remaining * max(0.05, min(0.95, stage_frac))
                close_frac(q, sig, "V271_STAGE1_STRUCTURE")
                stop_stage = "V271_STAGE1_STRUCTURE"
                continue

        # Flat exit — unchanged.
        if fifteen_due and age >= float(getattr(CFG, "flat_exit_minutes", 60)) and mfe < float(getattr(CFG, "flat_min_favorable_pct", 1.0)):
            try:
                flat, fd = BOT.flat_exit_signal(client, symbol, entry, CFG)
            except Exception:
                flat, fd = False, {}
            if flat:
                return finish("FLAT_EXIT_75M", now, price, "FLAT_EXIT", json.dumps(fd, ensure_ascii=False, default=str)[:500])

        if age >= max_hold_min:
            return finish("TIME_EXIT", now, price, "TIME_EXIT")

    last = path.iloc[-1]
    et = pd.Timestamp(last["ts"]).to_pydatetime().astimezone(UTC) + timedelta(minutes=1)
    return finish("TIME_EXIT", et, float(last["close"]), "TIME_EXIT_EOD")


def load_base_files():
    bt = pd.read_csv(BASE_TRADES_PATH)
    bd = pd.read_csv(BASE_DAILY_PATH)
    bt["accepted"] = pd.to_numeric(bt["accepted"], errors="coerce").fillna(0).astype(int)
    bt["net_pct"] = pd.to_numeric(bt["net_pct"], errors="coerce")
    return bt, bd


def run_cohort(setups: list[dict[str, Any]], base_trades: pd.DataFrame):
    by_id = {s["setup_id"]: s for s in setups}
    base_acc = base_trades[base_trades["accepted"] == 1].copy()
    rows = []

    for i, r in enumerate(base_acc.to_dict("records"), 1):
        sid = str(r["setup_id"])
        s = by_id.get(sid)
        if s is None:
            rows.append({
                "setup_id": sid, "symbol": r.get("symbol"), "entry_time_kst": r.get("entry_time_kst"),
                "base_result": r.get("result"), "base_net_pct": r.get("net_pct"),
                "worst_result": "DATA_ERROR", "worst_net_pct": "", "worst_delta_pct": "",
                "best_result": "DATA_ERROR", "best_net_pct": "", "best_delta_pct": "",
                "data_error": "setup_id not found",
            })
            continue

        sw = simulate_arm15_p13(s, "WORST")
        sb = simulate_arm15_p13(s, "BEST")
        bnet = float(r.get("net_pct") or 0.0)

        rows.append({
            "setup_id": sid,
            "symbol": s["symbol"],
            "entry_time_kst": r.get("entry_time_kst"),
            "entry_ts_utc": r.get("entry_ts_utc"),
            "entry_price": s["entry_price"],
            "base_result": r.get("result"),
            "base_stop_stage": r.get("stop_stage"),
            "base_mfe_pct": r.get("mfe_pct"),
            "base_mae_pct": r.get("mae_pct"),
            "base_net_pct": round(bnet, 6),

            "worst_result": sw.result,
            "worst_exit_time_kst": U.kst_stamp(sw.exit_time),
            "worst_stop_stage": sw.stop_stage,
            "worst_mfe_pct": round(sw.mfe_pct, 6),
            "worst_mae_pct": round(sw.mae_pct, 6),
            "worst_gross_pct": round(sw.gross_pct, 6),
            "worst_fee_pct": round(sw.fee_pct, 6),
            "worst_net_pct": round(sw.net_pct, 6),
            "worst_delta_pct": round(sw.net_pct - bnet, 6),
            "worst_detail": sw.detail,
            "worst_data_error": sw.data_error,

            "best_result": sb.result,
            "best_exit_time_kst": U.kst_stamp(sb.exit_time),
            "best_stop_stage": sb.stop_stage,
            "best_mfe_pct": round(sb.mfe_pct, 6),
            "best_mae_pct": round(sb.mae_pct, 6),
            "best_gross_pct": round(sb.gross_pct, 6),
            "best_fee_pct": round(sb.fee_pct, 6),
            "best_net_pct": round(sb.net_pct, 6),
            "best_delta_pct": round(sb.net_pct - bnet, 6),
            "best_detail": sb.detail,
            "best_data_error": sb.data_error,

            "tp_impaired_worst": int(str(r.get("result")) == "TP20_FULL" and sw.result == "PROTECT13_EXIT"),
            "tp_impaired_best": int(str(r.get("result")) == "TP20_FULL" and sb.result == "PROTECT13_EXIT"),
            "loss_to_positive_worst": int(bnet < 0 and sw.net_pct > 0),
            "loss_to_positive_best": int(bnet < 0 and sb.net_pct > 0),
        })

        if i % 50 == 0 or i == len(base_acc):
            print(f"[COHORT] {i}/{len(base_acc)}", flush=True)

    return rows


def cohort_daily(rows: list[dict[str, Any]]):
    days = [(U.START_KST + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(22)]
    out = []
    for day in days:
        z = [r for r in rows if str(r.get("entry_time_kst", "")).startswith(day)]
        b = sum(float(r.get("base_net_pct") or 0) for r in z)
        w = sum(float(r.get("worst_net_pct") or 0) for r in z)
        q = sum(float(r.get("best_net_pct") or 0) for r in z)
        out.append({
            "date": day,
            "trades": len(z),
            "BASE_NET": round(b, 6),
            "WORST_NET": round(w, 6),
            "WORST_DELTA": round(w - b, 6),
            "BEST_NET": round(q, 6),
            "BEST_DELTA": round(q - b, 6),
            "WORST_PROTECT13": sum(r.get("worst_result") == "PROTECT13_EXIT" for r in z),
            "BEST_PROTECT13": sum(r.get("best_result") == "PROTECT13_EXIT" for r in z),
            "WORST_TP": sum(r.get("worst_result") == "TP20_FULL" for r in z),
            "BEST_TP": sum(r.get("best_result") == "TP20_FULL" for r in z),
            "WORST_STOP": sum(r.get("worst_result") == "STOP" for r in z),
            "BEST_STOP": sum(r.get("best_result") == "STOP" for r in z),
            "TP_IMPAIRED_WORST": sum(int(r.get("tp_impaired_worst") or 0) for r in z),
            "TP_IMPAIRED_BEST": sum(int(r.get("tp_impaired_best") or 0) for r in z),
            "LOSS_TO_POS_WORST": sum(int(r.get("loss_to_positive_worst") or 0) for r in z),
            "LOSS_TO_POS_BEST": sum(int(r.get("loss_to_positive_best") or 0) for r in z),
        })
    return out


def run_portfolio_worst(setups, controls):
    sched = U.Scheduler()
    rows = []
    accepted = []

    for i, s in enumerate(setups, 1):
        ef = U.entry_filter(s, controls)
        row = {
            "setup_id": s["setup_id"],
            "symbol": s["symbol"],
            "entry_time_kst": U.kst_stamp(s["entry"]),
            "entry_ts_utc": s["entry"].isoformat(),
            "entry_price": s["entry_price"],
            "filter_pass": int(bool(ef["pass"])),
            "accepted": 0,
            "block_reason": ef["reason"],
            "result": "",
            "exit_time_kst": "",
            "net_pct": "",
            "mfe_pct": "",
            "mae_pct": "",
            "stop_stage": "",
            "data_error": "",
        }
        if ef["pass"]:
            ok, why = sched.can_open(s["entry"], s["symbol"])
            if not ok:
                row["block_reason"] = why
            else:
                sim = simulate_arm15_p13(s, "WORST")
                row.update({
                    "accepted": 1,
                    "block_reason": "",
                    "result": sim.result,
                    "exit_time_kst": U.kst_stamp(sim.exit_time),
                    "exit_ts_utc": sim.exit_time.isoformat(),
                    "net_pct": round(sim.net_pct, 6),
                    "gross_pct": round(sim.gross_pct, 6),
                    "fee_pct": round(sim.fee_pct, 6),
                    "mfe_pct": round(sim.mfe_pct, 6),
                    "mae_pct": round(sim.mae_pct, 6),
                    "stop_stage": sim.stop_stage,
                    "detail": sim.detail,
                    "data_error": sim.data_error,
                })
                # P13 is profitable protection, not a STOP/LATE event.
                stop_like = sim.result in ("STOP", "LATE_FAILURE_EXIT")
                sched.add(s["entry"], s["symbol"], sim, stop_like)
                accepted.append(row)
        rows.append(row)
        if i % 50 == 0 or i == len(setups):
            print(f"[PORT WORST] {i}/{len(setups)} accepted={len(accepted)}", flush=True)

    return rows, accepted


def portfolio_daily(rows, base_daily: pd.DataFrame):
    base_map = {str(r["date"]): r for r in base_daily.to_dict("records")}
    days = [(U.START_KST + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(22)]
    out = []
    for day in days:
        z = [r for r in rows if str(r.get("entry_time_kst", "")).startswith(day)]
        a = [r for r in z if int(r.get("accepted") or 0) == 1]
        net = sum(float(r.get("net_pct") or 0) for r in a)
        b = float(base_map.get(day, {}).get("net_pct") or 0)
        out.append({
            "date": day,
            "BASE_ENTRIES": int(float(base_map.get(day, {}).get("base_entries") or 0)),
            "BASE_NET": round(b, 6),
            "P13_ENTRIES": len(a),
            "P13_NET": round(net, 6),
            "DELTA": round(net - b, 6),
            "PROTECT13": sum(r.get("result") == "PROTECT13_EXIT" for r in a),
            "TP": sum(r.get("result") == "TP20_FULL" for r in a),
            "STOP": sum(r.get("result") == "STOP" for r in a),
            "LATE": sum(r.get("result") == "LATE_FAILURE_EXIT" for r in a),
            "FLAT": sum(r.get("result") == "FLAT_EXIT_75M" for r in a),
            "TIME": sum(r.get("result") == "TIME_EXIT" for r in a),
            "SLOT4_BLOCK": sum(r.get("block_reason") == "SLOT4" for r in z),
            "CAP15_BLOCK": sum(r.get("block_reason") == "CAP15_2" for r in z),
            "CD90_BLOCK": sum(r.get("block_reason") == "COOLDOWN90" for r in z),
            "CD180_BLOCK": sum(r.get("block_reason") == "COOLDOWN180" for r in z),
            "STOP_PAUSE_BLOCK": sum(r.get("block_reason") == "STOP_PAUSE30" for r in z),
        })
    return out


def segment_stats(rows: list[dict[str, Any]], start_day: str, end_day: str):
    z = [r for r in rows if start_day <= str(r.get("entry_time_kst", ""))[:10] <= end_day]
    b = sum(float(r.get("base_net_pct") or 0) for r in z)
    w = sum(float(r.get("worst_net_pct") or 0) for r in z)
    q = sum(float(r.get("best_net_pct") or 0) for r in z)
    return {
        "n": len(z),
        "base": b, "worst": w, "best": q,
        "worst_delta": w-b, "best_delta": q-b,
        "worst_p13": sum(r.get("worst_result") == "PROTECT13_EXIT" for r in z),
        "best_p13": sum(r.get("best_result") == "PROTECT13_EXIT" for r in z),
        "worst_impair": sum(int(r.get("tp_impaired_worst") or 0) for r in z),
        "best_impair": sum(int(r.get("tp_impaired_best") or 0) for r in z),
    }


def make_summary(cohort_rows, cohort_day, port_rows, port_acc, port_day, setups):
    base_net = sum(float(r.get("base_net_pct") or 0) for r in cohort_rows)
    worst_net = sum(float(r.get("worst_net_pct") or 0) for r in cohort_rows)
    best_net = sum(float(r.get("best_net_pct") or 0) for r in cohort_rows)

    s1 = segment_stats(cohort_rows, "2026-09-01", "2026-09-17")
    s2 = segment_stats(cohort_rows, "2026-09-18", "2026-09-22")

    pnet = sum(float(r.get("net_pct") or 0) for r in port_acc)
    base_entries = len(cohort_rows)
    p13_count = sum(r.get("result") == "PROTECT13_EXIT" for r in port_acc)
    perr = sum(bool(r.get("data_error")) for r in port_acc)
    cerr_w = sum(bool(r.get("worst_data_error")) for r in cohort_rows)
    cerr_b = sum(bool(r.get("best_data_error")) for r in cohort_rows)

    worst_counter = Counter(str(r.get("worst_result") or "") for r in cohort_rows)
    best_counter = Counter(str(r.get("best_result") or "") for r in cohort_rows)
    port_counter = Counter(str(r.get("result") or "") for r in port_acc)

    lines = [
        "TP2.0 + ARM1.5 + PROTECT1.3 — 1m REPLAY",
        f"script={SCRIPT_VERSION}",
        f"source={UNIFIED_PATH.name}",
        f"unified_source_version={getattr(U, 'SCRIPT_VERSION', '')}",
        f"bot_source={getattr(U, 'BOT_PATH', '')}",
        "",
        "[RULE]",
        "TP +2.0% FULL",
        "ARM at +1.5%",
        "After ARM: full protection exit at +1.3%",
        "Current PP12 is REPLACED (disabled in this test)",
        "Final4 + V27-1 fallback + fees are unchanged",
        "",
        "[1m AMBIGUITY BOUNDS]",
        "WORST: first ARM bar cannot protect on its own low; if already armed and TP2/P13 touch same 1m, P13 wins",
        "BEST: first ARM bar may protect on its own low; if already armed and TP2/P13 touch same 1m, TP2 wins",
        "",
        "[COHORT FIXED — same BASE accepted trades]",
        f"trades={base_entries}",
        f"BASE_NET={base_net:.6f}%p",
        f"WORST_NET={worst_net:.6f}%p DELTA={worst_net-base_net:+.6f}%p",
        f"BEST_NET={best_net:.6f}%p DELTA={best_net-base_net:+.6f}%p",
        f"WORST outcomes={dict(worst_counter)}",
        f"BEST outcomes={dict(best_counter)}",
        f"WORST TP impairment={sum(int(r.get('tp_impaired_worst') or 0) for r in cohort_rows)}",
        f"BEST TP impairment={sum(int(r.get('tp_impaired_best') or 0) for r in cohort_rows)}",
        f"WORST loss->positive={sum(int(r.get('loss_to_positive_worst') or 0) for r in cohort_rows)}",
        f"BEST loss->positive={sum(int(r.get('loss_to_positive_best') or 0) for r in cohort_rows)}",
        f"data_errors worst={cerr_w} best={cerr_b}",
        "",
        "[SEGMENT 09/01~09/17]",
        f"n={s1['n']} BASE={s1['base']:.6f} WORST={s1['worst']:.6f} ({s1['worst_delta']:+.6f}) BEST={s1['best']:.6f} ({s1['best_delta']:+.6f})",
        f"P13 worst/best={s1['worst_p13']}/{s1['best_p13']} TP_impair worst/best={s1['worst_impair']}/{s1['best_impair']}",
        "",
        "[SEGMENT 09/18~09/22]",
        f"n={s2['n']} BASE={s2['base']:.6f} WORST={s2['worst']:.6f} ({s2['worst_delta']:+.6f}) BEST={s2['best']:.6f} ({s2['best_delta']:+.6f})",
        f"P13 worst/best={s2['worst_p13']}/{s2['best_p13']} TP_impair worst/best={s2['worst_impair']}/{s2['best_impair']}",
        "",
        "[FULL PORTFOLIO RESCHEDULE — WORST]",
        f"V25={len(setups)} accepted={len(port_acc)} (BASE accepted={base_entries})",
        f"NET={pnet:.6f}%p vs BASE={base_net:.6f}%p DELTA={pnet-base_net:+.6f}%p",
        f"PROTECT13={p13_count}",
        f"outcomes={dict(port_counter)}",
        f"data_errors={perr}",
        "",
        "[FILES]",
        str(OUT_COHORT_DAILY.name),
        str(OUT_COHORT_TRADES.name),
        str(OUT_PORT_DAILY.name),
        str(OUT_PORT_TRADES.name),
        str(OUT_SUMMARY.name),
        str(OUT_ZIP.name),
    ]
    return "\n".join(lines) + "\n"


def main():
    print(f"=== {SCRIPT_VERSION} ===", flush=True)
    print("[1/7] load market cache", flush=True)
    U.load_market_series()

    print("[2/7] load V25 setups / RELAX controls", flush=True)
    setups = U.load_setups()
    controls = U.load_control_proxy()
    print(f"V25={len(setups)} controls={len(controls)}", flush=True)

    print("[3/7] load BASE output", flush=True)
    base_trades, base_daily = load_base_files()
    base_acc_n = int((base_trades["accepted"] == 1).sum())
    print(f"BASE accepted={base_acc_n}", flush=True)

    print("[4/7] same-cohort 1m replay: WORST + BEST", flush=True)
    cohort_rows = run_cohort(setups, base_trades)
    cohort_day = cohort_daily(cohort_rows)
    write_csv(OUT_COHORT_TRADES, cohort_rows)
    write_csv(OUT_COHORT_DAILY, cohort_day)

    print("[5/7] full portfolio reschedule: WORST", flush=True)
    port_rows, port_acc = run_portfolio_worst(setups, controls)
    port_day = portfolio_daily(port_rows, base_daily)
    write_csv(OUT_PORT_TRADES, port_rows)
    write_csv(OUT_PORT_DAILY, port_day)

    print("[6/7] summary", flush=True)
    txt = make_summary(cohort_rows, cohort_day, port_rows, port_acc, port_day, setups)
    OUT_SUMMARY.write_text(txt, encoding="utf-8")
    print(txt, flush=True)

    print("[7/7] zip", flush=True)
    files = [OUT_COHORT_TRADES, OUT_COHORT_DAILY, OUT_PORT_TRADES, OUT_PORT_DAILY, OUT_SUMMARY]
    with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as z:
        for p in files:
            z.write(p, arcname=p.name)
            print("ZIP:", p.name, flush=True)
    print("DONE:", OUT_ZIP, flush=True)


if __name__ == "__main__":
    main()
