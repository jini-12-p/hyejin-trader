#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PURE FORWARD validation after rules were frozen.

Source window:
  2026-09-24 20:43:03 KST -> current scan file end
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
    scan_FORWARD_20260924_204303_to_20260925_NOW_KST.csv

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
import gzip
import io
import json
import time
import urllib.parse
import urllib.request
from collections import Counter, deque
from datetime import datetime as RealDateTime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path("/root/hyejin-trader/bybit_swing")
U_PATH = ROOT / "unified_current_0901_0922.py"
SCAN = ROOT / "scan_FORWARD_20260924_204303_to_20260925_NOW_KST.csv"

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

WARM_START_KST = RealDateTime(2026, 9, 24, 14, 0, 0, tzinfo=KST)
EVAL_START_KST = RealDateTime(2026, 9, 24, 20, 43, 3, tzinfo=KST)

RISK_V25_2H_MIN = 8
RISK_ABS4H_AVG_MIN = 0.40

OUT_SUM = ROOT / "FORWARD_0924_0925_CURRENT_VS_V22Q_SUMMARY.csv"
OUT_DAILY = ROOT / "FORWARD_0924_0925_CURRENT_VS_V22Q_DAILY.csv"
OUT_CURRENT = ROOT / "FORWARD_0924_0925_CURRENT_TRADES.csv"
OUT_MOD = ROOT / "FORWARD_0924_0925_V22Q_TRADES.csv"
OUT_BLOCKED = ROOT / "FORWARD_0924_0925_V22Q_DIRECT_BLOCKS.csv"
OUT_NEW = ROOT / "FORWARD_0924_0925_V22Q_NEW_ENTRIES.csv"
OUT_DISPLACED = ROOT / "FORWARD_0924_0925_V22Q_DISPLACED_CURRENT.csv"
OUT_TXT = ROOT / "FORWARD_0924_0925_CURRENT_VS_V22Q_SUMMARY.txt"
OUT_ZIP = ROOT / "FORWARD_0924_0925_CURRENT_VS_V22Q_RESULTS.zip"

SCRIPT_VERSION = "PURE_FORWARD_V22Q_PLUS_DCA_v1_20260925"


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



# =============================
# 2026-09-25 integrated helpers
# =============================
SCAN_START_TEXT = "2026-09-24 20:43:03"
RAW_SCAN_ZIP = ROOT / "scan_FORWARD_20260924_204303_to_20260925_NOW_KST.zip"

# Frozen recovery / selective DCA rules from the prior analysis.
OBS_VOL_MAX = 2.61
OBS_SLOPE_MIN = 0.00061
GREEN_LOW_MIN = -2.74314
GREEN_RSI_MIN = 43.89902
SAFEGRAY_LOW_TO_TRIGGER_MAX_MIN = 12.0
SAFEGRAY_PREV3M_MIN = -1.0
DCA_REBOUND_PCT = 1.5
DCA_HORIZON_H = 6
TAKER_FEE_PCT = 0.055

STOP_DETAIL = ROOT / "FORWARD_0924_0925_DCA_STOP_DETAIL.csv"
STOP_DAILY = ROOT / "FORWARD_0924_0925_DCA_STOP_DAILY.csv"
STOP_SUM = ROOT / "FORWARD_0924_0925_DCA_STOP_SUMMARY.txt"
STOP_ZIP = ROOT / "FORWARD_0924_0925_DCA_STOP_RESULTS.zip"
FULL_ZIP = ROOT / "FORWARD_0924_0925_FULL_VALIDATION_RESULTS.zip"


def build_forward_scan():
    """Build a de-duplicated scan file strictly after the prior source end."""
    out_csv = SCAN
    out_zip = RAW_SCAN_ZIP
    sources = []
    arc = ROOT / "scan_archive"
    if arc.exists():
        for p in arc.rglob("*"):
            if p.is_file() and (
                p.suffix.lower() == ".csv"
                or p.name.lower().endswith(".csv.gz")
                or p.suffix.lower() == ".zip"
            ):
                sources.append(p)

    for p in ROOT.glob("scan*"):
        if not p.is_file():
            continue
        if p.name in {out_csv.name, out_zip.name}:
            continue
        if p.name == "scan_rejected.csv" or "20260924" in p.name or "20260925" in p.name:
            if p.suffix.lower() in (".csv", ".zip") or p.name.lower().endswith(".csv.gz"):
                sources.append(p)

    # De-duplicate source paths while preserving order.
    sources = list(dict.fromkeys(sources))
    fields, seen_fields, rows, seen = [], set(), [], set()

    def add_text(text):
        rd = csv.DictReader(io.StringIO(text))
        if not rd.fieldnames:
            return
        for fld in rd.fieldnames:
            if fld not in seen_fields:
                seen_fields.add(fld)
                fields.append(fld)
        tk = next(
            (k for k in ("time_kst", "timestamp_kst", "datetime_kst", "time", "timestamp") if k in rd.fieldnames),
            rd.fieldnames[0],
        )
        for r in rd:
            ts = str(r.get(tk, "")).strip().strip('"')
            if not ts or ts < SCAN_START_TEXT:
                continue
            key = json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
            if key in seen:
                continue
            seen.add(key)
            r["__TS__"] = ts
            rows.append(r)

    for idx, src in enumerate(sources, 1):
        try:
            if src.name.lower().endswith(".csv.gz"):
                with gzip.open(src, "rt", encoding="utf-8-sig", errors="replace") as fh:
                    add_text(fh.read())
            elif src.suffix.lower() == ".zip":
                with zipfile.ZipFile(src) as z:
                    for name in z.namelist():
                        if name.lower().endswith(".csv"):
                            add_text(z.read(name).decode("utf-8-sig", errors="replace"))
            elif src.suffix.lower() == ".csv":
                add_text(src.read_text(encoding="utf-8-sig", errors="replace"))
            if idx % 20 == 0:
                print(f"[SCAN] {idx}/{len(sources)} source files", flush=True)
        except Exception as e:
            print("[SCAN WARN]", src.name, repr(e), flush=True)

    if not rows:
        raise SystemExit("NO SCAN DATA AFTER 2026-09-24 20:43:03")

    rows.sort(key=lambda r: r["__TS__"])
    first_ts, last_ts = rows[0]["__TS__"], rows[-1]["__TS__"]
    for r in rows:
        r.pop("__TS__", None)

    with out_csv.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(out_csv, arcname=out_csv.name)

    print("=== FORWARD SCAN BUILT ===", flush=True)
    print("sources=", len(sources), "rows=", len(rows), flush=True)
    print("first=", first_ts, "last=", last_ts, flush=True)
    print("raw zip=", out_zip, flush=True)


def api_1m(symbol, start_utc, end_utc):
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": "1",
        "start": int(start_utc.timestamp() * 1000),
        "end": int(end_utc.timestamp() * 1000),
        "limit": 1000,
    }
    url = "https://api.bybit.com/v5/market/kline?" + urllib.parse.urlencode(params)
    last = None
    for k in range(8):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "HJ-FWD-DCA-VALIDATION/1.0"})
            with urllib.request.urlopen(req, timeout=25) as resp:
                j = json.loads(resp.read().decode("utf-8"))
            if int(j.get("retCode", -1)) != 0:
                raise RuntimeError(f"{j.get('retCode')} {j.get('retMsg')}")
            out = []
            for z in j.get("result", {}).get("list", []):
                try:
                    out.append({
                        "ts": RealDateTime.fromtimestamp(int(z[0]) / 1000, tz=UTC),
                        "o": float(z[1]), "h": float(z[2]), "l": float(z[3]),
                        "c": float(z[4]), "v": float(z[5]),
                    })
                except Exception:
                    pass
            out.sort(key=lambda x: x["ts"])
            return out
        except Exception as e:
            last = e
            time.sleep(min(8, 0.8 * (k + 1)))
    raise RuntimeError(last)


def pct_change(a, b):
    return (a / b - 1.0) * 100.0 if b else None


def mean_num(xs):
    z = [float(x) for x in xs if x is not None and math.isfinite(float(x))]
    return sum(z) / len(z) if z else None


def ema_num(vals, n):
    if not vals:
        return None
    a = 2.0 / (n + 1.0)
    e = float(vals[0])
    for x in vals[1:]:
        e = a * float(x) + (1 - a) * e
    return e


def rsi_num(vals, n=14):
    if len(vals) < n + 1:
        return None
    gains, losses = [], []
    for a, b in zip(vals[-(n + 1):-1], vals[-n:]):
        d = b - a
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag = sum(gains) / n
    al = sum(losses) / n
    if al == 0:
        return 100.0
    rs = ag / al
    return 100.0 - 100.0 / (1.0 + rs)


def five_from_one(one):
    buckets = {}
    for b in one:
        t = b["ts"].replace(minute=(b["ts"].minute // 5) * 5, second=0, microsecond=0)
        x = buckets.get(t)
        if x is None:
            buckets[t] = {"ts": t, "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"], "v": b["v"]}
        else:
            x["h"] = max(x["h"], b["h"])
            x["l"] = min(x["l"], b["l"])
            x["c"] = b["c"]
            x["v"] += b["v"]
    return [buckets[k] for k in sorted(buckets)]


def completed_before(one, checkpoint_utc):
    cp = checkpoint_utc.replace(second=0, microsecond=0)
    p1 = [b for b in one if b["ts"] < cp]
    f5 = [b for b in five_from_one(one) if b["ts"] + timedelta(minutes=5) <= cp]
    out = {}
    if p1:
        last1 = p1[-1]
        vol10 = [b["v"] for b in p1[-11:-1]] if len(p1) >= 2 else []
        mv = mean_num(vol10)
        out["prev1_vol_ratio10"] = last1["v"] / mv if mv not in (None, 0) else None
        last3 = p1[-3:]
        out["prev3_ret"] = pct_change(last3[-1]["c"], last3[0]["o"]) if last3 else None
    else:
        out["prev1_vol_ratio10"] = None
        out["prev3_ret"] = None
    if f5:
        closes = [b["c"] for b in f5]
        e20 = ema_num(closes[-80:], 20)
        e20p = ema_num(closes[-81:-1], 20) if len(closes) >= 2 else None
        out["ema20_slope"] = (e20 / e20p - 1.0) * 100.0 if e20 and e20p else None
        out["rsi5"] = rsi_num(closes, 14)
    else:
        out["ema20_slope"] = None
        out["rsi5"] = None
    return out


def one_position_exit_net(entry, exitp):
    gross = (exitp / entry - 1.0) * 100.0
    fees = TAKER_FEE_PCT + TAKER_FEE_PCT * (exitp / entry)
    return gross - fees


def dca_avg_exit_net(entry, addp):
    # Same quantity added (100% of original). Gross PnL is zero at arithmetic avg.
    avg = (entry + addp) / 2.0
    fees = (
        TAKER_FEE_PCT
        + TAKER_FEE_PCT * (addp / entry)
        + TAKER_FEE_PCT * (2.0 * avg / entry)
    )
    return -fees


def dca_fail_exit_net(entry, addp, exitp):
    # Hypothetical protective exit of both equal-quantity legs at pre-add swing-low break.
    gross = ((exitp - entry) + (exitp - addp)) / entry * 100.0
    fees = (
        TAKER_FEE_PCT
        + TAKER_FEE_PCT * (addp / entry)
        + TAKER_FEE_PCT * (2.0 * exitp / entry)
    )
    return gross - fees


def validate_stop_dca():
    if not OUT_MOD.exists():
        raise SystemExit(f"missing modified trades: {OUT_MOD}")

    df_scan, _, data_end, _ = load_scan_window()
    eligibility_end = data_end - timedelta(hours=DCA_HORIZON_H)

    with OUT_MOD.open("r", encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))

    stops = []
    for r in rows:
        if str(r.get("scenario")) != "CURRENT_PLUS_V22Q":
            continue
        if int(float(r.get("accepted") or 0)) != 1:
            continue
        if str(r.get("result")) != "STOP":
            continue
        ex = dt_kst(r.get("exit_time_kst"))
        if ex and ex <= eligibility_end:
            stops.append(r)

    details = []
    errors = []
    for idx, r in enumerate(stops, 1):
        try:
            symbol = str(r["symbol"])
            entry = float(r["entry_price"])
            stop_kst = dt_kst(r["exit_time_kst"])
            stop_utc = stop_kst.astimezone(UTC)
            one = api_1m(symbol, stop_utc - timedelta(hours=3), stop_utc + timedelta(hours=DCA_HORIZON_H, minutes=5))
            if len(one) < 30:
                raise RuntimeError("too few 1m bars")

            sf = completed_before(one, stop_utc)
            obs = bool(
                sf.get("prev1_vol_ratio10") is not None
                and sf.get("ema20_slope") is not None
                and sf["prev1_vol_ratio10"] <= OBS_VOL_MAX
                and sf["ema20_slope"] >= OBS_SLOPE_MIN
            )

            current_net = float(r["net_pct"])
            policy_net = current_net
            cls = "NOT_OBSERVER"
            trigger_kst = ""
            low_pct = ""
            rsi5 = ""
            prev3 = ""
            low_to_trigger = ""
            dca = 0
            dca_success = ""
            dca_false = ""
            policy_note = "KEEP_CURRENT_STOP"

            if obs:
                stop_floor = stop_utc.replace(second=0, microsecond=0)
                post = [b for b in one if stop_floor + timedelta(minutes=1) <= b["ts"] <= stop_floor + timedelta(hours=DCA_HORIZON_H)]
                low = None
                low_ts = None
                trigger_ts = None
                trigger_price = None

                for b in post:
                    if low is None or b["l"] < low:
                        low = b["l"]
                        low_ts = b["ts"]
                    if b["ts"] <= low_ts:
                        continue
                    trg = low * (1.0 + DCA_REBOUND_PCT / 100.0)
                    if b["h"] >= trg:
                        trigger_ts = b["ts"]
                        trigger_price = trg
                        break

                if trigger_ts is None:
                    cls = "OBSERVER_NO_TRIGGER_6H"
                    policy_note = "UNRESOLVED_KEEP_FOR_RESEARCH"
                    policy_net = current_net
                else:
                    tf = completed_before(one, trigger_ts)
                    lp = pct_change(low, entry)
                    ltt = (trigger_ts - low_ts).total_seconds() / 60.0
                    pr3 = tf.get("prev3_ret")
                    rrsi = tf.get("rsi5")
                    green = bool(lp >= GREEN_LOW_MIN and rrsi is not None and rrsi >= GREEN_RSI_MIN)
                    safegray = bool(ltt < SAFEGRAY_LOW_TO_TRIGGER_MAX_MIN and pr3 is not None and pr3 > SAFEGRAY_PREV3M_MIN)
                    should_dca = bool(green or safegray)

                    trigger_kst = trigger_ts.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
                    low_pct = round(lp, 6)
                    rsi5 = "" if rrsi is None else round(rrsi, 6)
                    prev3 = "" if pr3 is None else round(pr3, 6)
                    low_to_trigger = round(ltt, 3)

                    if should_dca:
                        dca = 1
                        cls = "GREEN_DCA" if green else "SAFEGRAY_DCA"
                        avg = (entry + trigger_price) / 2.0
                        recovered = False
                        false_break = False
                        exitp = None
                        # Conservative: next minute onward, old-low break checked first.
                        for b in post:
                            if b["ts"] <= trigger_ts:
                                continue
                            if b["l"] <= low:
                                false_break = True
                                exitp = low
                                break
                            if b["h"] >= avg:
                                recovered = True
                                exitp = avg
                                break
                        dca_success = int(recovered)
                        dca_false = int(false_break)
                        if recovered:
                            policy_net = dca_avg_exit_net(entry, trigger_price)
                            policy_note = "DCA100_EXIT_AT_NEW_AVG"
                        elif false_break:
                            policy_net = dca_fail_exit_net(entry, trigger_price, low)
                            policy_note = "DCA_FAIL_HYPOTHETICAL_EXIT_AT_OLD_LOW"
                        else:
                            policy_net = current_net
                            policy_note = "DCA_UNRESOLVED_6H"
                    else:
                        cls = "RISK_NO_DCA_EXIT_REBOUND"
                        policy_net = one_position_exit_net(entry, trigger_price)
                        policy_note = "NO_DCA_EXIT_AT_LOW_PLUS_1P5"

            details.append({
                "date": str(r.get("entry_time_kst") or "")[:10],
                "setup_id": r.get("setup_id"),
                "symbol": r.get("symbol"),
                "entry_time_kst": r.get("entry_time_kst"),
                "stop_time_kst": r.get("exit_time_kst"),
                "current_stop_net": round(current_net, 6),
                "stop_prev1_vol_ratio10": "" if sf.get("prev1_vol_ratio10") is None else round(sf["prev1_vol_ratio10"], 6),
                "stop_ema20_slope": "" if sf.get("ema20_slope") is None else round(sf["ema20_slope"], 6),
                "observer": int(obs),
                "class": cls,
                "trigger_time_kst": trigger_kst,
                "swing_low_pct": low_pct,
                "rebound_rsi5": rsi5,
                "rebound_prev3m_ret": prev3,
                "low_to_trigger_min": low_to_trigger,
                "dca100": dca,
                "dca_success": dca_success,
                "dca_false_break": dca_false,
                "policy_net": round(policy_net, 6),
                "delta_vs_current": round(policy_net - current_net, 6),
                "policy_note": policy_note,
            })
            if idx % 5 == 0 or idx == len(stops):
                print(f"[DCA] {idx}/{len(stops)} eligible STOPs", flush=True)
            time.sleep(0.03)
        except Exception as e:
            errors.append({"setup_id": r.get("setup_id"), "symbol": r.get("symbol"), "error": repr(e)})
            print("[DCA ERR]", r.get("symbol"), repr(e), flush=True)

    # Daily rollup.
    daily = []
    for d in sorted(set(x["date"] for x in details)):
        z = [x for x in details if x["date"] == d]
        cur = sum(float(x["current_stop_net"]) for x in z)
        pol = sum(float(x["policy_net"]) for x in z)
        daily.append({
            "date": d,
            "eligible_stops": len(z),
            "observers": sum(int(x["observer"]) for x in z),
            "dca100": sum(int(x["dca100"]) for x in z),
            "dca_success": sum(1 for x in z if str(x["dca_success"]) == "1"),
            "risk_no_dca": sum(x["class"] == "RISK_NO_DCA_EXIT_REBOUND" for x in z),
            "current_stop_net": round(cur, 6),
            "policy_stop_net": round(pol, 6),
            "delta": round(pol - cur, 6),
        })

    write_csv(STOP_DETAIL, details)
    write_csv(STOP_DAILY, daily)

    cur_total = sum(float(x["current_stop_net"]) for x in details)
    pol_total = sum(float(x["policy_net"]) for x in details)
    lines = [
        "PURE FORWARD DCA/STOP VALIDATION",
        f"source_end={data_end.isoformat()}",
        f"eligible_stop_end={eligibility_end.isoformat()} (needs full {DCA_HORIZON_H}h post-stop path)",
        f"eligible_STOPs={len(details)} errors={len(errors)}",
        f"observers={sum(int(x['observer']) for x in details)}",
        f"DCA100={sum(int(x['dca100']) for x in details)}",
        f"DCA_success={sum(1 for x in details if str(x['dca_success']) == '1')}",
        f"DCA_false_break={sum(1 for x in details if str(x['dca_false_break']) == '1')}",
        f"risk_no_dca={sum(x['class']=='RISK_NO_DCA_EXIT_REBOUND' for x in details)}",
        f"current_stop_net={cur_total:+.6f}",
        f"policy_stop_net={pol_total:+.6f}",
        f"delta={pol_total-cur_total:+.6f}",
        "",
        "Frozen rules:",
        f"observer = prev1_vol_ratio10<={OBS_VOL_MAX} AND ema20_slope>={OBS_SLOPE_MIN}",
        f"green = swing_low>={GREEN_LOW_MIN}% AND RSI5>={GREEN_RSI_MIN}",
        f"safegray = low_to_trigger<{SAFEGRAY_LOW_TO_TRIGGER_MAX_MIN}m AND prev3m>{SAFEGRAY_PREV3M_MIN}%",
        f"DCA trigger = low +{DCA_REBOUND_PCT}% ; size=100% original",
    ]
    STOP_SUM.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with zipfile.ZipFile(STOP_ZIP, "w", zipfile.ZIP_DEFLATED) as z:
        for pp in (STOP_DETAIL, STOP_DAILY, STOP_SUM):
            z.write(pp, arcname=pp.name)
        if errors:
            ep = ROOT / "FORWARD_0924_0925_DCA_STOP_ERRORS.csv"
            write_csv(ep, errors)
            z.write(ep, arcname=ep.name)

    print("\n".join(lines), flush=True)
    print("DONE:", STOP_ZIP, flush=True)


def make_full_bundle():
    with zipfile.ZipFile(FULL_ZIP, "w", zipfile.ZIP_DEFLATED) as z:
        for p in (RAW_SCAN_ZIP, OUT_ZIP, STOP_ZIP):
            if p.exists():
                z.write(p, arcname=p.name)
        # Also include the most useful flat summaries for quick inspection.
        for p in (OUT_SUM, OUT_DAILY, OUT_TXT, STOP_DETAIL, STOP_DAILY, STOP_SUM):
            if p.exists():
                z.write(p, arcname=p.name)
    print("=== FULL BUNDLE ===", flush=True)
    print(FULL_ZIP, flush=True)


def full_main():
    build_forward_scan()
    main()  # CURRENT vs CURRENT+V22Q entry/market protection replay
    validate_stop_dca()
    make_full_bundle()


if __name__ == "__main__":
    full_main()
