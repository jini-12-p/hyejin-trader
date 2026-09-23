#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Full portfolio reschedule comparison for current P strategy.

A) BASE: existing UNIFIED_CURRENT_0901_0922 result
B) RISK-only P13:
   risk trades use TP2.0 + ARM1.5 -> P1.3
   normal trades use current BASE exits
   Two 1m ambiguity branches are reported (WORST/BEST rules from prior P13 script).
C) RISK BLOCK:
   risk trades are blocked BEFORE portfolio scheduling
   normal trades use current BASE strategy.

Risk regime (causal, at setup time):
  rolling last 2h V25 confirmed count INCLUDING current >= 8
  AND mean(abs(BTC 4h change), abs(ETH 4h change)) >= 0.40%

Most importantly, C records:
- BASE risk trades removed
- NEW entries that were not in BASE but enter because slots/cooldowns changed
- BASE non-risk entries lost after portfolio reshuffle
- P&L contribution of each group

Outputs:
  RISK_RESCHEDULE_COMPARISON.csv
  RISK_BLOCK_DAILY.csv
  RISK_BLOCK_TRADES.csv
  RISK_BLOCK_NEW_ENTRIES.csv
  RISK_BLOCK_LOST_BASE_ENTRIES.csv
  RISK_P13_WORST_DAILY.csv / TRADES.csv
  RISK_P13_BEST_DAILY.csv / TRADES.csv
  RISK_RESCHEDULE_SUMMARY.txt
  RISK_RESCHEDULE_RESULTS.zip
"""
from __future__ import annotations

import csv
import importlib.util
import math
import sys
import zipfile
from collections import Counter, deque
from datetime import timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent
U_PATH = ROOT / "unified_current_0901_0922.py"
P13_PATH = ROOT / "backtest_tp20_arm15_p13_0901_0922.py"
BASE_TRADES_PATH = ROOT / "UNIFIED_CURRENT_0901_0922_TRADES.csv"
BASE_DAILY_PATH = ROOT / "UNIFIED_CURRENT_0901_0922_DAILY.csv"

OUT_COMPARE = ROOT / "RISK_RESCHEDULE_COMPARISON.csv"
OUT_BLOCK_DAILY = ROOT / "RISK_BLOCK_DAILY.csv"
OUT_BLOCK_TRADES = ROOT / "RISK_BLOCK_TRADES.csv"
OUT_NEW = ROOT / "RISK_BLOCK_NEW_ENTRIES.csv"
OUT_LOST = ROOT / "RISK_BLOCK_LOST_BASE_ENTRIES.csv"
OUT_P13W_DAILY = ROOT / "RISK_P13_WORST_DAILY.csv"
OUT_P13W_TRADES = ROOT / "RISK_P13_WORST_TRADES.csv"
OUT_P13B_DAILY = ROOT / "RISK_P13_BEST_DAILY.csv"
OUT_P13B_TRADES = ROOT / "RISK_P13_BEST_TRADES.csv"
OUT_SUMMARY = ROOT / "RISK_RESCHEDULE_SUMMARY.txt"
OUT_ZIP = ROOT / "RISK_RESCHEDULE_RESULTS.zip"

RISK_V25_2H_MIN = 8
RISK_4H_ABS_AVG_MIN = 0.40
SCRIPT_VERSION = "RISK_RESCHEDULE_v1_20260923"


def load_module(name: str, path: Path):
    if not path.exists():
        raise SystemExit(f"missing: {path}")
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot import {path}")
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


U = load_module("U_RISK_COMPARE", U_PATH)
P13 = load_module("P13_RISK_COMPARE", P13_PATH)


def f(v, default=None):
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def s(v) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    return str(v)


def write_csv(path: Path, rows: list[dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    keys, seen = [], set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        w = csv.DictWriter(fp, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def cached_sim_from_base(r: dict[str, Any]) -> U.SimResult:
    et = U.dt_utc(r.get("exit_ts_utc"))
    if et is None:
        # KST formatted fallback
        txt = s(r.get("exit_time_kst"))
        if txt:
            et = pd.Timestamp(txt, tz=U.KST).to_pydatetime().astimezone(U.UTC)
    if et is None:
        raise RuntimeError(f"BASE exit timestamp missing: {r.get('setup_id')}")
    ep = f(r.get("entry_price"), 0.0) or 0.0
    terminal = f(r.get("terminal_price"), ep) or ep
    return U.SimResult(
        result=s(r.get("result")),
        exit_time=et,
        terminal_price=terminal,
        fills=[],
        gross_pct=f(r.get("gross_pct"), 0.0) or 0.0,
        fee_pct=f(r.get("fee_pct"), 0.0) or 0.0,
        net_pct=f(r.get("net_pct"), 0.0) or 0.0,
        mfe_pct=f(r.get("mfe_pct"), 0.0) or 0.0,
        mae_pct=f(r.get("mae_pct"), 0.0) or 0.0,
        stop_stage=s(r.get("stop_stage")),
        detail="CACHED_FROM_UNIFIED_BASE",
        data_error=s(r.get("data_error")),
    )


def build_risk_meta(setups: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Rolling V25 count over [t-2h, t], including current setup."""
    q = deque()
    out = {}
    for st in setups:
        now = st["entry"]
        cutoff = now - timedelta(hours=2)
        while q and q[0] < cutoff:
            q.popleft()
        q.append(now)
        n2h = len(q)
        d = st["details"]
        b4 = U.first_f(d, "btc_4h_change_pct", "btc_4h")
        e4 = U.first_f(d, "eth_4h_change_pct", "eth_4h")
        avgabs = None if b4 is None or e4 is None else (abs(b4) + abs(e4)) / 2.0
        risk = bool(n2h >= RISK_V25_2H_MIN and avgabs is not None and avgabs >= RISK_4H_ABS_AVG_MIN)
        out[st["setup_id"]] = {
            "v25_2h_count": n2h,
            "btc4h": b4,
            "eth4h": e4,
            "abs4h_avg": avgabs,
            "risk": risk,
        }
    return out


def base_row_map(df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    return {str(r["setup_id"]): r for r in df.to_dict("records")}


def run_scenario(
    name: str,
    setups: list[dict[str, Any]],
    controls,
    risk_meta: dict[str, dict[str, Any]],
    base_map: dict[str, dict[str, Any]],
    mode: str,
):
    """
    mode:
      BLOCK
      P13_WORST
      P13_BEST
    """
    sched = U.Scheduler()
    rows = []
    accepted = []

    for i, st in enumerate(setups, 1):
        sid = st["setup_id"]
        rm = risk_meta[sid]
        ef = U.entry_filter(st, controls)
        br = base_map.get(sid, {})
        row = {
            "scenario": name,
            "setup_id": sid,
            "symbol": st["symbol"],
            "entry_time_kst": U.kst_stamp(st["entry"]),
            "entry_ts_utc": st["entry"].isoformat(),
            "entry_price": st["entry_price"],
            "v25_2h_count": rm["v25_2h_count"],
            "btc4h": rm["btc4h"],
            "eth4h": rm["eth4h"],
            "abs4h_avg": rm["abs4h_avg"],
            "risk_regime": int(rm["risk"]),
            "filter_pass": int(bool(ef["pass"])),
            "accepted": 0,
            "block_reason": ef["reason"],
            "base_accepted": int(f(br.get("accepted"), 0) or 0),
            "base_block_reason": s(br.get("block_reason")),
            "result": "",
            "exit_time_kst": "",
            "net_pct": "",
            "mfe_pct": "",
            "mae_pct": "",
            "stop_stage": "",
            "sim_source": "",
            "data_error": "",
        }

        if not ef["pass"]:
            rows.append(row)
            continue

        if mode == "BLOCK" and rm["risk"]:
            row["block_reason"] = "RISK_2H8_4H04"
            rows.append(row)
            continue

        ok, why = sched.can_open(st["entry"], st["symbol"])
        if not ok:
            row["block_reason"] = why
            rows.append(row)
            continue

        # Select exit engine.
        if mode in ("P13_WORST", "P13_BEST") and rm["risk"]:
            p13_mode = "WORST" if mode == "P13_WORST" else "BEST"
            sim = P13.simulate_arm15_p13(st, p13_mode)
            source = f"P13_{p13_mode}"
        else:
            # Same setup under BASE rules has deterministic same exit; reuse old result
            # where possible. Only newly admitted setups need fresh Bybit replay.
            if int(f(br.get("accepted"), 0) or 0) == 1:
                sim = cached_sim_from_base(br)
                source = "BASE_CACHE"
            else:
                sim = U.simulate_base(st)
                source = "BASE_FRESH"

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
            "sim_source": source,
            "data_error": sim.data_error,
        })
        stop_like = sim.result in ("STOP", "LATE_FAILURE_EXIT")
        sched.add(st["entry"], st["symbol"], sim, stop_like)
        rows.append(row)
        accepted.append(row)

        if i % 100 == 0:
            print(f"[{name}] {i}/{len(setups)} accepted={len(accepted)}", flush=True)

    print(f"[{name}] DONE accepted={len(accepted)}", flush=True)
    return rows, accepted


def make_daily(rows, base_daily: pd.DataFrame, risk_meta):
    base_map_day = {str(r["date"]): r for r in base_daily.to_dict("records")}
    days = [(U.START_KST + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(22)]
    out = []
    for day in days:
        z = [r for r in rows if str(r["entry_time_kst"]).startswith(day)]
        a = [r for r in z if int(r["accepted"]) == 1]
        b = base_map_day.get(day, {})
        base_net = f(b.get("net_pct"), 0.0) or 0.0
        base_entries = int(f(b.get("base_entries"), 0) or 0)
        net = sum(float(r["net_pct"]) for r in a)
        new = [r for r in a if not int(r["base_accepted"])]
        out.append({
            "date": day,
            "BASE_ENTRIES": base_entries,
            "BASE_NET": round(base_net, 6),
            "SCENARIO_ENTRIES": len(a),
            "SCENARIO_NET": round(net, 6),
            "DELTA": round(net - base_net, 6),
            "RISK_BLOCK_COUNT": sum(r["block_reason"] == "RISK_2H8_4H04" for r in z),
            "NEW_ENTRIES": len(new),
            "NEW_ENTRIES_NET": round(sum(float(r["net_pct"]) for r in new), 6),
            "TP": sum(r["result"] == "TP20_FULL" for r in a),
            "P13": sum(r["result"] == "PROTECT13_EXIT" for r in a),
            "STOP": sum(r["result"] == "STOP" for r in a),
            "LATE": sum(r["result"] == "LATE_FAILURE_EXIT" for r in a),
            "FLAT": sum(r["result"] == "FLAT_EXIT_75M" for r in a),
            "TIME": sum(r["result"] == "TIME_EXIT" for r in a),
        })
    return out


def set_diff_details(block_rows, base_df, risk_meta):
    base_acc = base_df[base_df["accepted"] == 1].copy()
    base_ids = set(base_acc["setup_id"].astype(str))
    c_acc = [r for r in block_rows if int(r["accepted"]) == 1]
    c_ids = {str(r["setup_id"]) for r in c_acc}

    base_dict = {str(r["setup_id"]): r for r in base_acc.to_dict("records")}
    c_dict = {str(r["setup_id"]): r for r in c_acc}

    removed_risk_ids = sorted(sid for sid in base_ids - c_ids if risk_meta[sid]["risk"])
    lost_nonrisk_ids = sorted(sid for sid in base_ids - c_ids if not risk_meta[sid]["risk"])
    new_ids = sorted(c_ids - base_ids)
    common_ids = base_ids & c_ids

    new_rows = []
    full_base = {str(r["setup_id"]): r for r in base_df.to_dict("records")}
    for sid in new_ids:
        cr = c_dict[sid]
        br = full_base.get(sid, {})
        new_rows.append({
            **cr,
            "original_base_block_reason": s(br.get("block_reason")),
            "original_base_result": s(br.get("result")),
            "original_base_net_pct": br.get("net_pct", ""),
        })

    lost_rows = []
    for sid in lost_nonrisk_ids:
        br = base_dict[sid]
        # find reason in C schedule
        rr = next((x for x in block_rows if str(x["setup_id"]) == sid), {})
        lost_rows.append({
            "setup_id": sid,
            "symbol": br.get("symbol"),
            "entry_time_kst": br.get("entry_time_kst"),
            "base_result": br.get("result"),
            "base_net_pct": br.get("net_pct"),
            "risk_regime": 0,
            "new_block_reason": rr.get("block_reason", ""),
        })

    return {
        "base_ids": base_ids, "c_ids": c_ids,
        "removed_risk_ids": removed_risk_ids,
        "lost_nonrisk_ids": lost_nonrisk_ids,
        "new_ids": new_ids, "common_ids": common_ids,
        "new_rows": new_rows, "lost_rows": lost_rows,
    }


def scenario_summary(name, rows):
    a = [r for r in rows if int(r["accepted"]) == 1]
    return {
        "scenario": name,
        "entries": len(a),
        "net_pct": round(sum(float(r["net_pct"]) for r in a), 6),
        "tp": sum(r["result"] == "TP20_FULL" for r in a),
        "p13": sum(r["result"] == "PROTECT13_EXIT" for r in a),
        "stop": sum(r["result"] == "STOP" for r in a),
        "late": sum(r["result"] == "LATE_FAILURE_EXIT" for r in a),
        "flat": sum(r["result"] == "FLAT_EXIT_75M" for r in a),
        "time": sum(r["result"] == "TIME_EXIT" for r in a),
        "data_errors": sum(bool(r.get("data_error")) for r in a),
    }


def main():
    print(f"=== {SCRIPT_VERSION} ===", flush=True)
    for p in [U_PATH, P13_PATH, BASE_TRADES_PATH, BASE_DAILY_PATH]:
        if not p.exists():
            raise SystemExit(f"missing: {p}")

    print("[1/8] load market cache / setups / control proxy", flush=True)
    U.load_market_series()
    setups = U.load_setups()
    controls = U.load_control_proxy()
    base_df = pd.read_csv(BASE_TRADES_PATH)
    base_daily = pd.read_csv(BASE_DAILY_PATH)
    base_df["accepted"] = pd.to_numeric(base_df["accepted"], errors="coerce").fillna(0).astype(int)
    base_df["net_pct"] = pd.to_numeric(base_df["net_pct"], errors="coerce")
    bmap = base_row_map(base_df)

    print("[2/8] causal risk regime tag", flush=True)
    rmeta = build_risk_meta(setups)

    # Sanity check against the already-established cohort numbers.
    base_acc = base_df[base_df["accepted"] == 1]
    base_risk = base_acc[base_acc["setup_id"].astype(str).map(lambda x: bool(rmeta[x]["risk"]))]
    sanity_n = len(base_risk)
    sanity_net = float(base_risk["net_pct"].sum())
    print(f"SANITY BASE accepted risk: n={sanity_n}, net={sanity_net:.6f}%p (expected about 214 / -74.344307)", flush=True)

    print("[3/8] full reschedule C = RISK BLOCK", flush=True)
    block_rows, block_acc = run_scenario("RISK_BLOCK", setups, controls, rmeta, bmap, "BLOCK")
    block_daily = make_daily(block_rows, base_daily, rmeta)
    write_csv(OUT_BLOCK_TRADES, block_rows)
    write_csv(OUT_BLOCK_DAILY, block_daily)

    print("[4/8] substitution / cascade analysis", flush=True)
    diff = set_diff_details(block_rows, base_df, rmeta)
    write_csv(OUT_NEW, diff["new_rows"])
    write_csv(OUT_LOST, diff["lost_rows"])

    print("[5/8] full reschedule B1 = risk-only P13 WORST 1m ordering branch", flush=True)
    p13w_rows, p13w_acc = run_scenario("RISK_P13_WORST", setups, controls, rmeta, bmap, "P13_WORST")
    p13w_daily = make_daily(p13w_rows, base_daily, rmeta)
    write_csv(OUT_P13W_TRADES, p13w_rows)
    write_csv(OUT_P13W_DAILY, p13w_daily)

    print("[6/8] full reschedule B2 = risk-only P13 BEST 1m ordering branch", flush=True)
    p13b_rows, p13b_acc = run_scenario("RISK_P13_BEST", setups, controls, rmeta, bmap, "P13_BEST")
    p13b_daily = make_daily(p13b_rows, base_daily, rmeta)
    write_csv(OUT_P13B_TRADES, p13b_rows)
    write_csv(OUT_P13B_DAILY, p13b_daily)

    print("[7/8] compare / summary", flush=True)
    base_net = float(base_acc["net_pct"].sum())
    base_summary = {
        "scenario": "BASE",
        "entries": len(base_acc),
        "net_pct": round(base_net, 6),
        "tp": int((base_acc["result"] == "TP20_FULL").sum()),
        "p13": 0,
        "stop": int((base_acc["result"] == "STOP").sum()),
        "late": int((base_acc["result"] == "LATE_FAILURE_EXIT").sum()),
        "flat": int((base_acc["result"] == "FLAT_EXIT_75M").sum()),
        "time": int((base_acc["result"] == "TIME_EXIT").sum()),
        "data_errors": int(base_acc["data_error"].fillna("").astype(str).ne("").sum()),
    }
    summaries = [
        base_summary,
        scenario_summary("RISK_P13_WORST", p13w_rows),
        scenario_summary("RISK_P13_BEST", p13b_rows),
        scenario_summary("RISK_BLOCK", block_rows),
    ]
    for x in summaries:
        x["delta_vs_base"] = round(float(x["net_pct"]) - base_net, 6)
    write_csv(OUT_COMPARE, summaries)

    removed_risk_base = base_acc[base_acc["setup_id"].astype(str).isin(diff["removed_risk_ids"])]
    lost_nonrisk_base = base_acc[base_acc["setup_id"].astype(str).isin(diff["lost_nonrisk_ids"])]
    new_net = sum(float(r["net_pct"]) for r in diff["new_rows"])
    removed_risk_net = float(removed_risk_base["net_pct"].sum())
    lost_nonrisk_net = float(lost_nonrisk_base["net_pct"].sum())

    new_reason_counts = Counter(s(r.get("original_base_block_reason")) for r in diff["new_rows"])
    new_result_counts = Counter(s(r.get("result")) for r in diff["new_rows"])
    lost_reason_counts = Counter(s(r.get("new_block_reason")) for r in diff["lost_rows"])

    block_s = summaries[-1]
    lines = [
        "RISK REGIME FULL PORTFOLIO RESCHEDULE",
        f"script={SCRIPT_VERSION}",
        "",
        "[RISK DEFINITION]",
        f"rolling 2h V25 count INCLUDING current >= {RISK_V25_2H_MIN}",
        f"AND mean(abs(BTC4h),abs(ETH4h)) >= {RISK_4H_ABS_AVG_MIN:.2f}%",
        "",
        "[SANITY]",
        f"BASE accepted risk n={sanity_n}; NET={sanity_net:.6f}%p",
        "Expected from prior cohort check: n=214, NET≈-74.344307%p",
        "",
        "[FULL RESCHEDULE RESULTS]",
    ]
    for x in summaries:
        lines.append(
            f"{x['scenario']}: entries={x['entries']} NET={x['net_pct']:.6f}%p "
            f"DELTA={x['delta_vs_base']:+.6f}%p TP={x['tp']} P13={x['p13']} "
            f"STOP={x['stop']} LATE={x['late']} FLAT={x['flat']} TIME={x['time']} errors={x['data_errors']}"
        )
    lines += [
        "",
        "[RISK BLOCK — DOES ANOTHER TRADE ENTER?]",
        f"BASE accepted={len(diff['base_ids'])}",
        f"RISK_BLOCK accepted={len(diff['c_ids'])}",
        f"direct BASE risk trades removed={len(diff['removed_risk_ids'])}; their BASE NET={removed_risk_net:.6f}%p",
        f"NEW entries admitted after slots/cooldowns reshuffle={len(diff['new_ids'])}; NEW NET={new_net:.6f}%p",
        f"NEW original BASE block reasons={dict(new_reason_counts)}",
        f"NEW outcomes={dict(new_result_counts)}",
        f"BASE non-risk entries later lost because of reshuffle={len(diff['lost_nonrisk_ids'])}; their BASE NET={lost_nonrisk_net:.6f}%p",
        f"LOST new block reasons={dict(lost_reason_counts)}",
        f"COMMON entries={len(diff['common_ids'])}",
        "",
        "[DELTA DECOMPOSITION]",
        f"remove risk contribution: {-removed_risk_net:+.6f}%p",
        f"add NEW entries: {new_net:+.6f}%p",
        f"remove BASE non-risk entries displaced by reshuffle: {-lost_nonrisk_net:+.6f}%p",
        f"estimated decomposition total={(-removed_risk_net + new_net - lost_nonrisk_net):+.6f}%p",
        f"actual RISK_BLOCK delta={float(block_s['delta_vs_base']):+.6f}%p",
        "",
        "[IMPORTANT]",
        "RISK_BLOCK is exact under current BASE exit rules and full portfolio rescheduling.",
        "RISK_P13_WORST/BEST are two 1-minute ordering branches; if they materially differ, tick-order resolution is still needed for a single exact P13 number.",
        "",
        "[FILES]",
        OUT_COMPARE.name,
        OUT_BLOCK_DAILY.name,
        OUT_BLOCK_TRADES.name,
        OUT_NEW.name,
        OUT_LOST.name,
        OUT_P13W_DAILY.name,
        OUT_P13W_TRADES.name,
        OUT_P13B_DAILY.name,
        OUT_P13B_TRADES.name,
    ]
    OUT_SUMMARY.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines), flush=True)

    print("[8/8] zip", flush=True)
    files = [
        OUT_COMPARE, OUT_BLOCK_DAILY, OUT_BLOCK_TRADES, OUT_NEW, OUT_LOST,
        OUT_P13W_DAILY, OUT_P13W_TRADES, OUT_P13B_DAILY, OUT_P13B_TRADES, OUT_SUMMARY
    ]
    with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as z:
        for p in files:
            z.write(p, arcname=p.name)
            print("ZIP:", p.name, flush=True)
    print("DONE:", OUT_ZIP, flush=True)


if __name__ == "__main__":
    main()
