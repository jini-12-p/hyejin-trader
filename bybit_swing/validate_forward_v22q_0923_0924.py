#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PURE FORWARD validation after rules were frozen.

Source window:
  2026-09-23 22:10:44 KST -> current scan file end
Evaluation:
  same start -> scan file end - 3h

Scenario 1 CURRENT:
  exact current BASE entry filter (SAFE/RELAX/MKT100)
  + current discovered market risk guard:
      V25 rolling 2h >= 8
      AND mean(abs(BTC4h),abs(ETH4h)) >= 0.40%
  + exact current portfolio scheduling
  + exact current TP2.0 / PP12 / Final4 / V27-1 / fee replay

Scenario 2 MODIFIED:
  identical to CURRENT
  + V22 quality OR, using exact candidate-time WATCH telemetry from
    scan_FORWARD_20260923_221044_to_20260924_NOW_KST.csv

  OVEREXT:
    p_v2_score >= 90
    AND ema9_ema20_gap_pct >= 1.20

  WEAK_REACCEL:
    rebound_from_low_pct <= 5
    AND rsi_delta <= 7
    AND btc_15m_change_pct >= -0.08

Missing exact WATCH telemetry is never blocked.

Both scenarios inherit the SAME BASE portfolio state before evaluation start.
No DB writes. No orders.
"""

from __future__ import annotations

import copy
import csv
import importlib.util
import math
import sys
import zipfile
from collections import Counter, deque
from datetime import datetime as RealDateTime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path("/root/hyejin-trader/bybit_swing")
U_PATH = ROOT / "unified_current_0901_0922.py"
SCAN = ROOT / "scan_FORWARD_20260923_221044_to_20260924_NOW_KST.csv"

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

WARM_START_KST = RealDateTime(2026, 9, 23, 16, 0, 0, tzinfo=KST)
EVAL_START_KST = RealDateTime(2026, 9, 23, 22, 10, 44, tzinfo=KST)

RISK_V25_2H_MIN = 8
RISK_ABS4H_AVG_MIN = 0.40

OUT_SUM = ROOT / "FORWARD_0923_0924_CURRENT_VS_V22Q_SUMMARY.csv"
OUT_DAILY = ROOT / "FORWARD_0923_0924_CURRENT_VS_V22Q_DAILY.csv"
OUT_CURRENT = ROOT / "FORWARD_0923_0924_CURRENT_TRADES.csv"
OUT_MOD = ROOT / "FORWARD_0923_0924_V22Q_TRADES.csv"
OUT_BLOCKED = ROOT / "FORWARD_0923_0924_V22Q_DIRECT_BLOCKS.csv"
OUT_NEW = ROOT / "FORWARD_0923_0924_V22Q_NEW_ENTRIES.csv"
OUT_DISPLACED = ROOT / "FORWARD_0923_0924_V22Q_DISPLACED_CURRENT.csv"
OUT_TXT = ROOT / "FORWARD_0923_0924_CURRENT_VS_V22Q_SUMMARY.txt"
OUT_ZIP = ROOT / "FORWARD_0923_0924_CURRENT_VS_V22Q_RESULTS.zip"

SCRIPT_VERSION = "PURE_FORWARD_V22Q_v1_20260924"


def load_module(name: str, path: Path):
    if not path.exists():
        raise SystemExit(f"missing: {path}")
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot import: {path}")
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


U = load_module("U_PURE_FORWARD_V22Q", U_PATH)


def fv(v, default=None):
    try:
        if v is None or str(v).strip() == "":
            return default
        x = float(v)
        if math.isnan(x):
            return default
        return x
    except Exception:
        return default


def dt_kst(v):
    try:
        return RealDateTime.strptime(str(v)[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
    except Exception:
        return None


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


def load_scan_window():
    if not SCAN.exists():
        raise SystemExit(f"missing scan: {SCAN}")
    df = pd.read_csv(SCAN, low_memory=False)
    if "time_kst" not in df.columns:
        raise SystemExit("scan missing time_kst")

    ts = pd.to_datetime(df["time_kst"], errors="coerce")
    valid = ts.dropna()
    if len(valid) == 0:
        raise SystemExit("scan has no valid timestamps")

    data_first = valid.min().to_pydatetime().replace(tzinfo=KST)
    data_end = valid.max().to_pydatetime().replace(tzinfo=KST)
    eval_end = data_end - timedelta(hours=3)

    return df, data_first, data_end, eval_end


def load_exact_watch_features(df):
    w = df[df["result"].astype(str) == "RESEARCH_P_CONFIRM_WATCH"].copy()
    if len(w) == 0:
        return {}

    w = w.sort_values("time_kst").drop_duplicates("p_v25_setup_id", keep="first")
    out = {}
    for r in w.to_dict("records"):
        sid = str(r.get("p_v25_setup_id") or "")
        if not sid or sid == "nan":
            continue
        score = fv(r.get("p_v2_score"))
        gap = fv(r.get("ema9_ema20_gap_pct"))
        rebound = fv(r.get("rebound_from_low_pct"))
        rd = fv(r.get("rsi_delta"))
        btc15 = fv(r.get("btc_15m_change_pct"))

        over = bool(
            score is not None
            and gap is not None
            and score >= 90.0
            and gap >= 1.20
        )
        weak = bool(
            rebound is not None
            and rd is not None
            and btc15 is not None
            and rebound <= 5.0
            and rd <= 7.0
            and btc15 >= -0.08
        )
        out[sid] = {
            "watch_time_kst": str(r.get("time_kst") or ""),
            "p_v2_score": score,
            "ema9_ema20_gap_pct": gap,
            "rebound_from_low_pct": rebound,
            "rsi_delta": rd,
            "btc_15m_change_pct": btc15,
            "overext": over,
            "weak_reaccel": weak,
            "v22_quality_or": bool(over or weak),
        }
    return out


def configure_unified(eval_end_kst):
    U.START_KST = WARM_START_KST
    U.END_KST = eval_end_kst + timedelta(seconds=1)
    U.START_UTC = U.START_KST.astimezone(UTC)
    U.END_UTC = U.END_KST.astimezone(UTC)
    U.EXPECTED_V25 = -1

    U.CACHE_DIR = ROOT / ".pure_forward_v22q_kline_cache"
    U.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    U.KC = U.KlineCache()
    U._market_1m = {}
    U._market_recompute_count = 0


def build_risk_meta(setups):
    q = deque()
    out = {}
    for st in setups:
        now = st["entry"]
        cutoff = now - timedelta(hours=2)
        while q and q[0] < cutoff:
            q.popleft()
        q.append(now)

        d = st["details"]
        # load_setups normally fills missing market fields,
        # but do it defensively here too.
        U.fill_missing_market(d, now)
        b4 = U.first_f(d, "btc_4h_change_pct", "btc_4h")
        e4 = U.first_f(d, "eth_4h_change_pct", "eth_4h")
        avgabs = None if b4 is None or e4 is None else (abs(b4) + abs(e4)) / 2.0

        out[st["setup_id"]] = {
            "v25_2h_count": len(q),
            "btc4h": b4,
            "eth4h": e4,
            "abs4h_avg": avgabs,
            "risk": bool(
                len(q) >= RISK_V25_2H_MIN
                and avgabs is not None
                and avgabs >= RISK_ABS4H_AVG_MIN
            ),
        }
    return out


def get_sim(st, cache):
    sid = st["setup_id"]
    if sid not in cache:
        cache[sid] = U.simulate_base(st)
    return cache[sid]


def warm_base(setups, controls, sim_cache):
    sched = U.Scheduler()
    n = 0
    boundary = EVAL_START_KST.astimezone(UTC)

    for st in setups:
        if st["entry"] >= boundary:
            break
        ef = U.entry_filter(st, controls)
        if not ef["pass"]:
            continue
        ok, _ = sched.can_open(st["entry"], st["symbol"])
        if not ok:
            continue
        sim = get_sim(st, sim_cache)
        sched.add(
            st["entry"],
            st["symbol"],
            sim,
            sim.result in ("STOP", "LATE_FAILURE_EXIT"),
        )
        n += 1

    print(f"[WARM] common BASE accepted={n}", flush=True)
    return sched


def base_row(st, ef, rm, qm, scenario):
    return {
        "scenario": scenario,
        "setup_id": st["setup_id"],
        "symbol": st["symbol"],
        "entry_time_kst": U.kst_stamp(st["entry"]),
        "entry_price": st["entry_price"],
        "risk_regime": int(rm["risk"]),
        "v25_2h_count": rm["v25_2h_count"],
        "btc4h": rm["btc4h"],
        "eth4h": rm["eth4h"],
        "abs4h_avg": rm["abs4h_avg"],
        "exact_watch": int(bool(qm)),
        "watch_time_kst": "" if not qm else qm["watch_time_kst"],
        "p_v2_score": "" if not qm else qm["p_v2_score"],
        "ema9_ema20_gap_pct": "" if not qm else qm["ema9_ema20_gap_pct"],
        "rebound_from_low_pct": "" if not qm else qm["rebound_from_low_pct"],
        "rsi_delta": "" if not qm else qm["rsi_delta"],
        "btc15": "" if not qm else qm["btc_15m_change_pct"],
        "overext": int(bool(qm and qm["overext"])),
        "weak_reaccel": int(bool(qm and qm["weak_reaccel"])),
        "v22_quality_or": int(bool(qm and qm["v22_quality_or"])),
        "safe_block_raw": int(ef["safe"]["block"]),
        "safe_relaxed": int(ef["safe_relaxed"]),
        "mkt100_block": int(ef["mkt"]["block"]),
        "filter_pass": int(bool(ef["pass"])),
        "accepted": 0,
        "block_reason": ef["reason"],
        "result": "",
        "net_pct": "",
        "exit_time_kst": "",
        "mfe_pct": "",
        "mae_pct": "",
        "stop_stage": "",
        "data_error": "",
    }


def attach_sim(row, sim):
    row.update({
        "accepted": 1,
        "block_reason": "",
        "result": sim.result,
        "net_pct": round(sim.net_pct, 6),
        "gross_pct": round(sim.gross_pct, 6),
        "fee_pct": round(sim.fee_pct, 6),
        "exit_time_kst": U.kst_stamp(sim.exit_time),
        "mfe_pct": round(sim.mfe_pct, 6),
        "mae_pct": round(sim.mae_pct, 6),
        "stop_stage": sim.stop_stage,
        "data_error": sim.data_error,
    })


def run_scenario(
    name,
    setups,
    controls,
    rmeta,
    qmeta,
    warm_sched,
    sim_cache,
    eval_end_kst,
    use_quality,
):
    sched = copy.deepcopy(warm_sched)
    rows, accepted = [], []

    start_utc = EVAL_START_KST.astimezone(UTC)
    end_utc = eval_end_kst.astimezone(UTC)
    eval_setups = [s for s in setups if start_utc <= s["entry"] <= end_utc]

    for i, st in enumerate(eval_setups, 1):
        sid = st["setup_id"]
        rm = rmeta[sid]
        qm = qmeta.get(sid)
        ef = U.entry_filter(st, controls)
        row = base_row(st, ef, rm, qm, name)

        if not ef["pass"]:
            rows.append(row)
            continue

        # Current market risk guard.
        if rm["risk"]:
            row["block_reason"] = "CURRENT_RISK_V25_2H8_ABS4H04"
            rows.append(row)
            continue

        # Proposed V22 quality add-on.
        if use_quality and qm and qm["v22_quality_or"]:
            row["block_reason"] = "V22_QUALITY_OR"
            rows.append(row)
            continue

        ok, why = sched.can_open(st["entry"], st["symbol"])
        if not ok:
            row["block_reason"] = why
            rows.append(row)
            continue

        sim = get_sim(st, sim_cache)
        attach_sim(row, sim)
        sched.add(
            st["entry"],
            st["symbol"],
            sim,
            sim.result in ("STOP", "LATE_FAILURE_EXIT"),
        )
        rows.append(row)
        accepted.append(row)

        if i % 20 == 0 or i == len(eval_setups):
            print(
                f"[{name}] {i}/{len(eval_setups)} accepted={len(accepted)}",
                flush=True,
            )

    return rows, accepted


def outcome_counts(rows):
    return dict(Counter(str(r["result"]) for r in rows))


def daily_compare(cur_acc, mod_acc):
    dates = sorted(set(
        str(r["entry_time_kst"])[:10]
        for r in cur_acc + mod_acc
    ))
    out = []
    for d in dates:
        c = [r for r in cur_acc if str(r["entry_time_kst"]).startswith(d)]
        m = [r for r in mod_acc if str(r["entry_time_kst"]).startswith(d)]
        cn = sum(float(r["net_pct"]) for r in c)
        mn = sum(float(r["net_pct"]) for r in m)
        out.append({
            "date": d,
            "current_entries": len(c),
            "current_net": round(cn, 6),
            "modified_entries": len(m),
            "modified_net": round(mn, 6),
            "delta": round(mn - cn, 6),
        })
    return out


def main():
    print(f"=== {SCRIPT_VERSION} ===", flush=True)

    df, data_first, data_end, eval_end = load_scan_window()
    print("scan first =", data_first.isoformat(), flush=True)
    print("scan last  =", data_end.isoformat(), flush=True)
    print("eval end   =", eval_end.isoformat(), flush=True)

    if eval_end <= EVAL_START_KST:
        raise SystemExit("not enough forward data for 3h horizon")

    qmeta = load_exact_watch_features(df)
    print("exact WATCH feature setups =", len(qmeta), flush=True)
    print(
        "WATCH quality flags =",
        sum(1 for x in qmeta.values() if x["v22_quality_or"]),
        flush=True,
    )

    configure_unified(eval_end)
    print("[1/6] load market series", flush=True)
    U.load_market_series()

    print("[2/6] load confirmed V25 and RELAX controls", flush=True)
    setups = U.load_setups()
    controls = U.load_control_proxy()
    print("setups warm+eval =", len(setups), flush=True)

    print("[3/6] causal current risk tags", flush=True)
    rmeta = build_risk_meta(setups)

    print("[4/6] common warm-up", flush=True)
    sim_cache = {}
    warm = warm_base(setups, controls, sim_cache)

    print("[5/6] CURRENT exact replay", flush=True)
    cur_rows, cur_acc = run_scenario(
        "CURRENT",
        setups,
        controls,
        rmeta,
        qmeta,
        warm,
        sim_cache,
        eval_end,
        use_quality=False,
    )

    print("[6/6] MODIFIED exact replay", flush=True)
    mod_rows, mod_acc = run_scenario(
        "CURRENT_PLUS_V22Q",
        setups,
        controls,
        rmeta,
        qmeta,
        warm,
        sim_cache,
        eval_end,
        use_quality=True,
    )

    cur_ids = {r["setup_id"] for r in cur_acc}
    mod_ids = {r["setup_id"] for r in mod_acc}
    cmap = {r["setup_id"]: r for r in cur_acc}
    mmap = {r["setup_id"]: r for r in mod_acc}

    removed_ids = sorted(cur_ids - mod_ids)
    new_ids = sorted(mod_ids - cur_ids)

    removed = [cmap[x] for x in removed_ids]
    new = [mmap[x] for x in new_ids]

    direct_ids = {
        str(r["setup_id"])
        for r in mod_rows
        if str(r.get("block_reason")) == "V22_QUALITY_OR"
    }
    direct = [cmap[x] for x in removed_ids if x in direct_ids]
    displaced = [cmap[x] for x in removed_ids if x not in direct_ids]

    cur_net = sum(float(r["net_pct"]) for r in cur_acc)
    mod_net = sum(float(r["net_pct"]) for r in mod_acc)

    coverage_cur = sum(int(r["exact_watch"]) for r in cur_acc)
    coverage_pct = 100.0 * coverage_cur / len(cur_acc) if cur_acc else 0.0

    summary = [
        {
            "scenario": "CURRENT",
            "entries": len(cur_acc),
            "net_pct": round(cur_net, 6),
            "delta_vs_current": 0.0,
            "tp": sum(r["result"] == "TP20_FULL" for r in cur_acc),
            "stop": sum(r["result"] == "STOP" for r in cur_acc),
            "pp12": sum(r["result"] == "PROFIT_PROTECT_EXIT" for r in cur_acc),
            "late": sum(r["result"] == "LATE_FAILURE_EXIT" for r in cur_acc),
            "exact_watch_coverage": coverage_cur,
            "exact_watch_coverage_pct": round(coverage_pct, 3),
            "outcomes": str(outcome_counts(cur_acc)),
        },
        {
            "scenario": "CURRENT_PLUS_V22Q",
            "entries": len(mod_acc),
            "net_pct": round(mod_net, 6),
            "delta_vs_current": round(mod_net - cur_net, 6),
            "tp": sum(r["result"] == "TP20_FULL" for r in mod_acc),
            "stop": sum(r["result"] == "STOP" for r in mod_acc),
            "pp12": sum(r["result"] == "PROFIT_PROTECT_EXIT" for r in mod_acc),
            "late": sum(r["result"] == "LATE_FAILURE_EXIT" for r in mod_acc),
            "exact_watch_coverage": sum(int(r["exact_watch"]) for r in mod_acc),
            "exact_watch_coverage_pct": round(
                100.0 * sum(int(r["exact_watch"]) for r in mod_acc) / len(mod_acc),
                3,
            ) if mod_acc else 0.0,
            "outcomes": str(outcome_counts(mod_acc)),
        },
    ]

    write_csv(OUT_SUM, summary)
    write_csv(OUT_DAILY, daily_compare(cur_acc, mod_acc))
    write_csv(OUT_CURRENT, cur_rows)
    write_csv(OUT_MOD, mod_rows)
    write_csv(OUT_BLOCKED, direct)
    write_csv(OUT_NEW, new)
    write_csv(OUT_DISPLACED, displaced)

    direct_net = sum(float(r["net_pct"]) for r in direct)
    new_net = sum(float(r["net_pct"]) for r in new)
    displaced_net = sum(float(r["net_pct"]) for r in displaced)

    lines = [
        "PURE FORWARD CURRENT vs CURRENT+V22Q",
        f"script={SCRIPT_VERSION}",
        f"source={data_first.isoformat()} ~ {data_end.isoformat()}",
        f"evaluation={EVAL_START_KST.isoformat()} ~ {eval_end.isoformat()}",
        "",
        f"CURRENT: entries={len(cur_acc)} NET={cur_net:.6f} outcomes={outcome_counts(cur_acc)}",
        f"MODIFIED: entries={len(mod_acc)} NET={mod_net:.6f} outcomes={outcome_counts(mod_acc)}",
        f"DELTA MOD-CURRENT={mod_net-cur_net:+.6f}",
        "",
        f"direct V22Q removed={len(direct)} net_was={direct_net:+.6f}",
        f"new entries after reschedule={len(new)} new_net={new_net:+.6f}",
        f"displaced CURRENT entries={len(displaced)} net_was={displaced_net:+.6f}",
        f"current exact WATCH coverage={coverage_cur}/{len(cur_acc)} ({coverage_pct:.2f}%)",
        "",
        "V22Q rules:",
        "OVEREXT = score>=90 AND gap>=1.20",
        "WEAK_REACCEL = rebound<=5 AND rsi_delta<=7 AND BTC15>=-0.08",
    ]
    OUT_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as z:
        for p in [
            OUT_SUM,
            OUT_DAILY,
            OUT_CURRENT,
            OUT_MOD,
            OUT_BLOCKED,
            OUT_NEW,
            OUT_DISPLACED,
            OUT_TXT,
        ]:
            z.write(p, arcname=p.name)

    print("\n".join(lines), flush=True)
    print("DONE:", OUT_ZIP, flush=True)


if __name__ == "__main__":
    main()
