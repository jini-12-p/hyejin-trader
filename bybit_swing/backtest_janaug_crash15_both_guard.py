#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JAN-AUG 2026 CURRENT vs 15m BTC+ETH simultaneous crash guards.

Purpose
-------
Test whether blocking NEW P entries when BOTH BTC and ETH are sharply down over
15 minutes reduces the Jan-Aug CURRENT losses without over-blocking TP trades.

Scenarios (all are CURRENT + one extra entry guard):
  CRASH050: BTC15 <= -0.50% AND ETH15 <= -0.50%
  CRASH075: BTC15 <= -0.75% AND ETH15 <= -0.75%
  CRASH100: BTC15 <= -1.00% AND ETH15 <= -1.00%

Everything else is unchanged:
  V25 confirmed candidates -> SAFE/RELAX -> MKT100 -> MARKET_GUARD -> V22Q
  -> EXTRA 15m crash guard -> 4 slots / rolling15m max2 / CD90 /
     STOP-CD180 / STOP-pause45
  -> same TP2.0 / PP12 / Final4 / V27-1 exits and fee model.

Important
---------
- Candidate generation is NOT run. Reuses the completed 20,500 Jan-Aug candidates.
- Existing exit simulations are reused. Only a newly accepted replacement entry
  whose exit is absent from the sim checkpoint is simulated.
- The original JANAUG_FIXED_2026_WORK is never modified.
- Crash telemetry uses confirm/entry-time details_json, i.e. causal completed-bar
  telemetry available at the candidate's entry time.
- Missing BTC/ETH 15m telemetry does NOT block an entry.
"""
from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.util
import json
import math
import shutil
import sys
import zipfile
from collections import Counter, deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

EXPECTED_CANDIDATES = 20500
SCENARIOS = [
    ("CRASH050", -0.50),
    ("CRASH075", -0.75),
    ("CRASH100", -1.00),
]
RESULT_NAMES = [
    "TP20_FULL", "PROFIT_PROTECT_EXIT", "STOP", "LATE_FAILURE_EXIT",
    "TIME_EXIT", "FLAT_EXIT_75M", "DATA_ERROR",
]


def pflush(*a):
    print(*a, flush=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def fnum(v, default=None):
    try:
        if v is None:
            return default
        x = float(v)
        return default if math.isnan(x) else x
    except Exception:
        return default


def build_rows(base, cand: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for r in cand.to_dict("records"):
        dt = pd.Timestamp(r["entry_dt"]).to_pydatetime().astimezone(base.UTC)
        details = base.jloads(r.get("details_json"))
        watch = base.jloads(r.get("watch_details_json"))
        b15 = fnum(details.get("btc_15m_change_pct"))
        e15 = fnum(details.get("eth_15m_change_pct"))
        avg15 = None if b15 is None or e15 is None else (b15 + e15) / 2.0
        rows.append({
            "setup_id": str(r["setup_id"]),
            "symbol": str(r["symbol"]),
            "entry": dt,
            "entry_price": float(r["entry_price"]),
            "details": details,
            "watch": watch,
            "raw": r,
            "btc15": b15,
            "eth15": e15,
            "avg15": avg15,
        })
    rows.sort(key=lambda x: (x["entry"], x["setup_id"]))
    return rows


def build_market_meta(base, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    q = deque()
    out = {}
    for st in rows:
        t = st["entry"]
        while q and q[0] < t - timedelta(hours=2):
            q.popleft()
        q.append(t)
        d = st["details"]
        b4 = fnum(d.get("btc_4h_change_pct"))
        e4 = fnum(d.get("eth_4h_change_pct"))
        av = None if b4 is None or e4 is None else (abs(b4) + abs(e4)) / 2.0
        out[st["setup_id"]] = {
            "v25_2h_count": len(q),
            "btc4h": b4,
            "eth4h": e4,
            "abs4h_avg": av,
            "risk": bool(
                len(q) >= base.MARKET_V25_2H_MIN
                and av is not None
                and av >= base.MARKET_ABS4H_AVG_MIN
            ),
        }
    return out


def v22_quality(base, st: dict[str, Any]) -> tuple[bool, bool, bool]:
    w = st["watch"]
    score = fnum(w.get("p_v2_score"), fnum(st["raw"].get("watch_p_v2_score")))
    gap = fnum(w.get("ema9_ema20_gap_pct"), fnum(st["raw"].get("watch_ema_gap")))
    reb = fnum(w.get("rebound_from_low_pct"), fnum(st["raw"].get("watch_rebound")))
    rd = fnum(w.get("rsi_delta"), fnum(st["raw"].get("watch_rsi_delta")))
    b15 = fnum(w.get("btc_15m_change_pct"), fnum(st["raw"].get("watch_btc15")))
    over = bool(score is not None and gap is not None and score >= 90.0 and gap >= 1.20)
    weak = bool(
        reb is not None and rd is not None and b15 is not None
        and reb <= 5.0 and rd <= 7.0 and b15 >= -0.08
    )
    return bool(over or weak), over, weak


def crash_hit(st: dict[str, Any], threshold: float) -> bool:
    """Require BOTH BTC and ETH 15m changes to be at/below the negative threshold."""
    b = st.get("btc15")
    e = st.get("eth15")
    return bool(b is not None and e is not None and b <= threshold and e <= threshold)


def write_month_checkpoint(cpdir: Path, scenario: str, month: str, rows: list[dict]):
    if not rows:
        return
    cpdir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv(cpdir / f"{scenario}_{month}_ALL.csv.gz", index=False,
              compression="gzip", encoding="utf-8-sig")
    df[df["accepted"] == 1].to_csv(cpdir / f"{scenario}_{month}_TRADES.csv",
                                    index=False, encoding="utf-8-sig")
    pflush(f"[{scenario} CHECKPOINT] {month} rows={len(df)} entries={int((df['accepted']==1).sum())}")


def run_guard_scenario(base, U, root: Path, work: Path,
                       rows: list[dict[str, Any]], market_meta: dict[str, dict[str, Any]],
                       controls, ref_events, sim_cache: dict, sim_path: Path,
                       threshold: float, scenario: str, state):
    pflush(f"\n=== {scenario}: BOTH BTC15/ETH15 <= {threshold:.2f}% ===")
    U.STOP_PAUSE_WINDOW_MIN = base.FINAL_STOP_PAUSE_MIN
    sched = U.Scheduler()
    sw = base.PerfSwitch(ref_events)
    new_sim = 0

    def get_sim(st):
        nonlocal new_sim
        sid = st["setup_id"]
        if sid not in sim_cache:
            sim_cache[sid] = U.simulate_base(st)
            new_sim += 1
            try:
                if hasattr(U, "KC") and hasattr(U.KC, "mem") and len(U.KC.mem) > base.EXIT_KLINE_MEM_MAX:
                    U.KC.mem.clear()
                    gc.collect()
            except Exception:
                pass
            if new_sim % max(1, int(base.FLUSH_SIM_EVERY)) == 0:
                base.flush_sim_checkpoint(sim_path, sim_cache)
                pflush(f"[{scenario} SIM] cache={len(sim_cache)} new={new_sim}")
        return sim_cache[sid]

    out, trades, blocks, stops = [], [], [], []
    direct_guard = []
    cpdir = work / "CRASH15_MONTHLY_CHECKPOINTS"
    month_rows: list[dict] = []
    current_month = None

    for i, st in enumerate(rows, 1):
        sid = st["setup_id"]
        mm = market_meta[sid]
        ef = U.entry_filter(st, controls)
        ron, rn, rsum = sw.snapshot(st["entry"])
        v22q, over, weak = v22_quality(base, st)
        ghit = crash_hit(st, threshold)

        mk = st["entry"].astimezone(base.KST).strftime("%Y-%m")
        if current_month is None:
            current_month = mk
        elif mk != current_month:
            write_month_checkpoint(cpdir, scenario, current_month, month_rows)
            base.flush_sim_checkpoint(sim_path, sim_cache)
            state.update(phase=scenario, completed_month=current_month, i=i-1,
                         entries=len(trades), stops=len(stops), sim_cached=len(sim_cache))
            month_rows = []
            current_month = mk

        row = {
            "scenario": scenario,
            "setup_id": sid,
            "symbol": st["symbol"],
            "entry_time_kst": st["entry"].astimezone(base.KST).strftime("%Y-%m-%d %H:%M:%S"),
            "entry_time_utc": st["entry"].isoformat(),
            "entry_price": st["entry_price"],
            "btc15_pct": "" if st["btc15"] is None else round(float(st["btc15"]), 6),
            "eth15_pct": "" if st["eth15"] is None else round(float(st["eth15"]), 6),
            "avg15_pct": "" if st["avg15"] is None else round(float(st["avg15"]), 6),
            "crash_threshold_pct": threshold,
            "crash_hit": int(ghit),
            "safe_block": int(bool(ef.get("safe", {}).get("block"))),
            "safe_relaxed": int(bool(ef.get("safe_relaxed"))),
            "mkt100_block": int(bool(ef.get("mkt", {}).get("block"))),
            "market_condition": int(mm["risk"]),
            "market_regime_on": int(ron),
            "market_shadow_n": rn,
            "market_shadow_sum": "" if rsum is None else round(float(rsum), 6),
            "v25_2h_count": mm["v25_2h_count"],
            "abs4h_avg": "" if mm["abs4h_avg"] is None else round(float(mm["abs4h_avg"]), 6),
            "v22q": int(v22q), "v22_overext": int(over), "v22_weak_reaccel": int(weak),
            "accepted": 0, "block_reason": "", "result": "", "exit_time_kst": "",
            "net_pct": "", "gross_pct": "", "fee_pct": "", "mfe_pct": "", "mae_pct": "",
            "stop_stage": "", "data_error": "",
        }

        if not ef["pass"]:
            row["block_reason"] = ef.get("reason") or "BASE_FILTER"
        elif ron and mm["risk"]:
            row["block_reason"] = "MARKET_GUARD_ACTIVE"
        elif v22q:
            row["block_reason"] = "V22_QUALITY_OR"
        elif ghit:
            row["block_reason"] = scenario
            direct_guard.append(row.copy())
        else:
            ok, why = sched.can_open(st["entry"], st["symbol"])
            if not ok:
                row["block_reason"] = "STOP_PAUSE45" if why == "STOP_PAUSE30" else why
            else:
                sim = get_sim(st)
                sched.add(st["entry"], st["symbol"], sim,
                          sim.result in ("STOP", "LATE_FAILURE_EXIT"))
                row.update({
                    "accepted": 1,
                    "result": sim.result,
                    "exit_time_kst": sim.exit_time.astimezone(base.KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "net_pct": round(float(sim.net_pct), 6),
                    "gross_pct": round(float(sim.gross_pct), 6),
                    "fee_pct": round(float(sim.fee_pct), 6),
                    "mfe_pct": round(float(sim.mfe_pct), 6),
                    "mae_pct": round(float(sim.mae_pct), 6),
                    "stop_stage": sim.stop_stage,
                    "data_error": sim.data_error or "",
                })
                trades.append(row.copy())
                if sim.result == "STOP":
                    stops.append(row.copy())

        if not row["accepted"]:
            blocks.append(row.copy())
        out.append(row)
        month_rows.append(row.copy())

        if i % 250 == 0:
            pflush(f"[{scenario}] {i}/{len(rows)} entries={len(trades)} STOP={len(stops)} "
                   f"guard={len(direct_guard)} new_sim={new_sim}")
            state.update(phase=scenario, i=i, entries=len(trades), stops=len(stops),
                         guard_blocks=len(direct_guard), sim_cached=len(sim_cache), new_sim=new_sim)

    if current_month is not None:
        write_month_checkpoint(cpdir, scenario, current_month, month_rows)
    base.flush_sim_checkpoint(sim_path, sim_cache)
    return out, trades, blocks, stops, direct_guard, new_sim


def trade_stats(trades: list[dict]) -> dict[str, Any]:
    rc = Counter(str(r.get("result", "")) for r in trades)
    net = float(pd.to_numeric(pd.Series([r.get("net_pct", 0) for r in trades]), errors="coerce").fillna(0).sum()) if trades else 0.0
    d = {"entries": len(trades), "net_pct": net}
    for k in RESULT_NAMES:
        d[k] = int(rc[k])
    return d


def period_table(trades: list[dict], period: str, start_kst, end_kst) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame()
    df = pd.DataFrame(trades).copy()
    df["net_num"] = pd.to_numeric(df["net_pct"], errors="coerce").fillna(0.0)
    if period == "month":
        df["period"] = df["entry_time_kst"].astype(str).str[:7]
        full = [f"2026-{m:02d}" for m in range(1, 9)]
    else:
        df["period"] = df["entry_time_kst"].astype(str).str[:10]
        full = pd.date_range(start_kst.date(), (end_kst - pd.Timedelta(days=1)).date(), freq="D").strftime("%Y-%m-%d").tolist()
    rows = []
    for p in full:
        g = df[df["period"] == p]
        rc = Counter(g["result"].astype(str).tolist())
        row = {"period": p, "entries": len(g), "net_pct": float(g["net_num"].sum())}
        for k in RESULT_NAMES:
            row[k] = int(rc[k])
        rows.append(row)
    return pd.DataFrame(rows)


def stability(daily: pd.DataFrame) -> dict[str, Any]:
    if daily.empty:
        return {}
    x = pd.to_numeric(daily["net_pct"], errors="coerce").fillna(0.0).tolist()
    pos = sum(v > 1e-12 for v in x)
    neg = sum(v < -1e-12 for v in x)
    zero = len(x) - pos - neg
    longest = cur = 0
    for v in x:
        if v < -1e-12:
            cur += 1
            longest = max(longest, cur)
        else:
            cur = 0
    mn_i = min(range(len(x)), key=lambda i: x[i])
    mx_i = max(range(len(x)), key=lambda i: x[i])
    return {
        "positive_days": pos, "negative_days": neg, "zero_days": zero,
        "median_day": float(pd.Series(x).median()),
        "worst_date": str(daily.iloc[mn_i]["period"]), "worst_net": float(x[mn_i]),
        "best_date": str(daily.iloc[mx_i]["period"]), "best_net": float(x[mx_i]),
        "max_consecutive_negative_days": longest,
    }


def compare_entry_sets(current_trades: list[dict], variant_trades: list[dict]) -> dict[str, Any]:
    cur = {str(r["setup_id"]): r for r in current_trades}
    var = {str(r["setup_id"]): r for r in variant_trades}
    cids, vids = set(cur), set(var)
    removed_ids = cids - vids
    added_ids = vids - cids
    common = cids & vids

    def pack(ids, src):
        rr = [src[s] for s in ids]
        rc = Counter(str(r.get("result", "")) for r in rr)
        net = float(pd.to_numeric(pd.Series([r.get("net_pct", 0) for r in rr]), errors="coerce").fillna(0).sum()) if rr else 0.0
        return rr, rc, net

    removed, rrc, rnet = pack(removed_ids, cur)
    added, arc, anet = pack(added_ids, var)
    return {
        "common_count": len(common),
        "removed": removed, "removed_count": len(removed), "removed_net": rnet, "removed_rc": rrc,
        "added": added, "added_count": len(added), "added_net": anet, "added_rc": arc,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/root/hyejin-trader/bybit_swing")
    ap.add_argument("--allow-candidate-count-mismatch", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).expanduser().resolve()
    balanced_path = root / "backtest_current_p_jan_aug_2026_balanced.py"
    unified_path = root / "unified_current_0901_0922.py"
    source_work = root / "JANAUG_FIXED_2026_WORK"
    work = root / "JANAUG_CRASH15_COMPARE_WORK"

    for p in (balanced_path, unified_path, source_work / "JAN_AUG_SIM_RESULTS.csv.gz"):
        if not p.exists():
            raise SystemExit(f"MISSING required source/checkpoint: {p}")

    work.mkdir(exist_ok=True)
    for m in range(1, 9):
        name = f"2026-{m:02d}_CANDIDATES.csv.gz"
        src = source_work / name
        dst = work / name
        if not src.exists():
            raise SystemExit(f"MISSING candidate checkpoint: {src}")
        if not dst.exists():
            dst.symlink_to(src)
    sim_path = work / "JAN_AUG_SIM_RESULTS.csv.gz"
    if not sim_path.exists():
        shutil.copy2(source_work / "JAN_AUG_SIM_RESULTS.csv.gz", sim_path)
        pflush(f"SEEDED sim cache -> {sim_path}")

    for p in (str(root.parent), str(root)):
        if p not in sys.path:
            sys.path.insert(0, p)

    base = load_module("JANAUG_BALANCED_BASE_CRASH15", balanced_path)
    base.EXIT_KLINE_MEM_MAX = min(int(getattr(base, "EXIT_KLINE_MEM_MAX", 32)), 12)
    base.FLUSH_SIM_EVERY = min(int(getattr(base, "FLUSH_SIM_EVERY", 50)), 25)
    U = base.load_module("U_JANAUG_CRASH15", unified_path)
    state = base.StateFile(work / "CRASH15_COMPARE_STATE.json")
    state.update(phase="start", balanced_sha256=sha256_file(balanced_path), unified_sha256=sha256_file(unified_path))

    cand = base.load_candidates(work)
    months = sorted(cand["entry_time_kst"].astype(str).str[:7].unique().tolist()) if not cand.empty else []
    pflush(f"FINAL-ONLY: loaded candidates={len(cand)} months={months}")
    if len(cand) != EXPECTED_CANDIDATES and not args.allow_candidate_count_mismatch:
        raise SystemExit(f"CANDIDATE COUNT MISMATCH got={len(cand)} expected={EXPECTED_CANDIDATES}")
    if months != [f"2026-{m:02d}" for m in range(1, 9)]:
        raise SystemExit(f"MONTH COVERAGE MISMATCH: {months}")

    # Reproduce CURRENT exactly from saved candidates + sim cache. No candidate generation.
    pflush("\n=== REPLAY CURRENT (same as balanced FINAL) ===")
    current_out, current_trades, current_blocks, current_stops, transitions, controls, ref_events = \
        base.run_final(root, work, U, cand, state)

    # Re-load shared sim cache after CURRENT may have extended it.
    sim_cache = base.load_sim_checkpoint(U, sim_path)
    rows = build_rows(base, cand)
    market_meta = build_market_meta(base, rows)
    telemetry_available = sum(1 for st in rows if st["btc15"] is not None and st["eth15"] is not None)
    telemetry_missing = len(rows) - telemetry_available
    pflush(f"15m telemetry BOTH available={telemetry_available}/{len(rows)} missing={telemetry_missing}")

    telemetry_rows = [{
        "setup_id": st["setup_id"], "symbol": st["symbol"],
        "entry_time_kst": st["entry"].astimezone(base.KST).strftime("%Y-%m-%d %H:%M:%S"),
        "btc15_pct": st["btc15"], "eth15_pct": st["eth15"], "avg15_pct": st["avg15"],
    } for st in rows]

    scenarios = {}
    total_new_sim = 0
    for name, threshold in SCENARIOS:
        try:
            if hasattr(U, "KC") and hasattr(U.KC, "mem"):
                U.KC.mem.clear()
                gc.collect()
        except Exception:
            pass
        out, trades, blocks, stops, direct_guard, new_sim = run_guard_scenario(
            base, U, root, work, rows, market_meta, controls, ref_events,
            sim_cache, sim_path, threshold, name, state,
        )
        total_new_sim += new_sim
        scenarios[name] = {
            "threshold": threshold, "out": out, "trades": trades, "blocks": blocks,
            "stops": stops, "direct_guard": direct_guard, "new_sim": new_sim,
        }

    stamp = datetime.now(base.KST).strftime("%Y%m%d_%H%M%S")
    pref = f"CURRENT_VS_CRASH15_BOTH_JANAUG_2026_{stamp}"
    outdir = root

    files = []
    # Current files
    cur_all = outdir / f"{pref}_CURRENT_ALL.csv.gz"
    cur_tr = outdir / f"{pref}_CURRENT_TRADES.csv"
    pd.DataFrame(current_out).to_csv(cur_all, index=False, compression="gzip", encoding="utf-8-sig")
    pd.DataFrame(current_trades).to_csv(cur_tr, index=False, encoding="utf-8-sig")
    files += [cur_all, cur_tr]

    telem = outdir / f"{pref}_CRASH15_TELEMETRY.csv.gz"
    pd.DataFrame(telemetry_rows).to_csv(telem, index=False, compression="gzip", encoding="utf-8-sig")
    files.append(telem)

    monthly_frames = {}
    daily_frames = {}
    cur_month = period_table(current_trades, "month", base.START_KST, base.END_KST)
    cur_day = period_table(current_trades, "date", base.START_KST, base.END_KST)
    monthly_frames["CURRENT"] = cur_month
    daily_frames["CURRENT"] = cur_day

    cur_ids = {str(r["setup_id"]): r for r in current_trades}
    scenario_audit = {}

    for name, threshold in SCENARIOS:
        s = scenarios[name]
        allp = outdir / f"{pref}_{name}_ALL.csv.gz"
        trp = outdir / f"{pref}_{name}_TRADES.csv"
        blp = outdir / f"{pref}_{name}_BLOCKS.csv.gz"
        stp = outdir / f"{pref}_{name}_STOP_ONLY.csv"
        grp = outdir / f"{pref}_{name}_DIRECT_GUARD_BLOCKS.csv"
        pd.DataFrame(s["out"]).to_csv(allp, index=False, compression="gzip", encoding="utf-8-sig")
        pd.DataFrame(s["trades"]).to_csv(trp, index=False, encoding="utf-8-sig")
        pd.DataFrame(s["blocks"]).to_csv(blp, index=False, compression="gzip", encoding="utf-8-sig")
        pd.DataFrame(s["stops"]).to_csv(stp, index=False, encoding="utf-8-sig")
        pd.DataFrame(s["direct_guard"]).to_csv(grp, index=False, encoding="utf-8-sig")
        files += [allp, trp, blp, stp, grp]

        comp = compare_entry_sets(current_trades, s["trades"])
        remp = outdir / f"{pref}_{name}_REMOVED_CURRENT_TRADES.csv"
        addp = outdir / f"{pref}_{name}_ADDED_REPLACEMENT_TRADES.csv"
        pd.DataFrame(comp["removed"]).to_csv(remp, index=False, encoding="utf-8-sig")
        pd.DataFrame(comp["added"]).to_csv(addp, index=False, encoding="utf-8-sig")
        files += [remp, addp]

        direct_ids = {str(r["setup_id"]) for r in s["direct_guard"]}
        direct_current = [cur_ids[x] for x in sorted(direct_ids & set(cur_ids))]
        dcp = outdir / f"{pref}_{name}_DIRECTLY_BLOCKED_CURRENT_TRADES.csv"
        pd.DataFrame(direct_current).to_csv(dcp, index=False, encoding="utf-8-sig")
        files.append(dcp)
        dc_rc = Counter(str(r.get("result", "")) for r in direct_current)
        dc_net = float(pd.to_numeric(pd.Series([r.get("net_pct", 0) for r in direct_current]), errors="coerce").fillna(0).sum()) if direct_current else 0.0

        monthly_frames[name] = period_table(s["trades"], "month", base.START_KST, base.END_KST)
        daily_frames[name] = period_table(s["trades"], "date", base.START_KST, base.END_KST)
        scenario_audit[name] = {
            "threshold": threshold,
            "stats": trade_stats(s["trades"]),
            "direct_guard_blocks": len(s["direct_guard"]),
            "directly_blocked_current_trades": len(direct_current),
            "directly_blocked_current_net": dc_net,
            "directly_blocked_current_results": dict(dc_rc),
            "common_entries": comp["common_count"],
            "removed_current_count": comp["removed_count"],
            "removed_current_net": comp["removed_net"],
            "removed_current_results": dict(comp["removed_rc"]),
            "added_replacement_count": comp["added_count"],
            "added_replacement_net": comp["added_net"],
            "added_replacement_results": dict(comp["added_rc"]),
            "new_simulated": s["new_sim"],
            "daily_stability": stability(daily_frames[name]),
        }

    # Wide monthly comparison
    mon = cur_month.rename(columns={c: f"CURRENT_{c}" for c in cur_month.columns if c != "period"})
    for name, _ in SCENARIOS:
        x = monthly_frames[name].rename(columns={c: f"{name}_{c}" for c in monthly_frames[name].columns if c != "period"})
        mon = mon.merge(x, on="period", how="outer")
        mon[f"{name}_delta_net_vs_CURRENT"] = mon[f"{name}_net_pct"] - mon["CURRENT_net_pct"]
    mon = mon.rename(columns={"period": "month"})
    monp = outdir / f"{pref}_MONTHLY_COMPARE.csv"
    mon.to_csv(monp, index=False, encoding="utf-8-sig")
    files.append(monp)

    day = cur_day.rename(columns={c: f"CURRENT_{c}" for c in cur_day.columns if c != "period"})
    for name, _ in SCENARIOS:
        x = daily_frames[name].rename(columns={c: f"{name}_{c}" for c in daily_frames[name].columns if c != "period"})
        day = day.merge(x, on="period", how="outer")
        day[f"{name}_delta_net_vs_CURRENT"] = day[f"{name}_net_pct"] - day["CURRENT_net_pct"]
    day = day.rename(columns={"period": "date"})
    dayp = outdir / f"{pref}_DAILY_COMPARE.csv"
    day.to_csv(dayp, index=False, encoding="utf-8-sig")
    files.append(dayp)

    cur_stats = trade_stats(current_trades)
    cur_stab = stability(cur_day)
    audit = {
        "generated_at_kst": datetime.now(base.KST).isoformat(),
        "candidate_count": len(cand), "expected_candidate_count": EXPECTED_CANDIDATES,
        "candidate_generation_run": False,
        "telemetry_both_available": telemetry_available, "telemetry_missing": telemetry_missing,
        "guard_definition": "block only when BOTH confirm-time BTC15 and ETH15 changes <= threshold",
        "thresholds": {name: thr for name, thr in SCENARIOS},
        "current": {"stats": cur_stats, "daily_stability": cur_stab},
        "scenarios": scenario_audit,
        "total_new_simulated_across_guards": total_new_sim,
        "balanced_sha256": sha256_file(balanced_path),
        "unified_sha256": sha256_file(unified_path),
    }
    auditp = outdir / f"{pref}_AUDIT.json"
    auditp.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    files.append(auditp)

    lines = [
        "CURRENT vs BTC+ETH 15m SIMULTANEOUS CRASH GUARDS — JAN-AUG 2026",
        "STATUS=APPROXIMATE_HISTORICAL_REPLAY / FINAL-ONLY CACHE REUSE",
        "",
        "[RULE]",
        "Extra guard blocks a new entry only when BOTH BTC and ETH confirm-time 15m changes are <= threshold.",
        "CRASH050=-0.50% / CRASH075=-0.75% / CRASH100=-1.00%.",
        "Everything else remains CURRENT: SAFE/RELAX, MKT100, MARKET_GUARD, V22Q, scheduler, exits, fees.",
        "Missing BTC/ETH telemetry does not block.",
        "",
        "[AUDIT]",
        f"V25 candidates={len(cand)} candidate_generation=NOT RUN",
        f"BTC+ETH15 telemetry available={telemetry_available} missing={telemetry_missing}",
        f"CURRENT entries={cur_stats['entries']} STOP={cur_stats['STOP']} TP={cur_stats['TP20_FULL']} net={cur_stats['net_pct']:+.6f}%p",
        "",
        "[TOTAL COMPARISON]",
    ]
    for name, threshold in SCENARIOS:
        a = scenario_audit[name]
        s = a["stats"]
        delta = s["net_pct"] - cur_stats["net_pct"]
        lines.append(
            f"{name} BOTH<={threshold:.2f}%: entries={s['entries']} TP={s['TP20_FULL']} STOP={s['STOP']} "
            f"net={s['net_pct']:+.6f}%p delta_vs_CURRENT={delta:+.6f}%p "
            f"direct_guard_blocks={a['direct_guard_blocks']} directly_blocked_CURRENT={a['directly_blocked_current_trades']}"
        )
        lines.append(
            f"  removed CURRENT: n={a['removed_current_count']} net={a['removed_current_net']:+.6f}%p "
            f"results={a['removed_current_results']}"
        )
        lines.append(
            f"  added replacements: n={a['added_replacement_count']} net={a['added_replacement_net']:+.6f}%p "
            f"results={a['added_replacement_results']}"
        )
        lines.append(
            f"  directly blocked CURRENT trades: n={a['directly_blocked_current_trades']} "
            f"net={a['directly_blocked_current_net']:+.6f}%p results={a['directly_blocked_current_results']}"
        )
    lines += ["", "[MONTHLY NET %p]"]
    for _, r in mon.iterrows():
        s = f"{r['month']} CURRENT={float(r['CURRENT_net_pct']):+.3f}"
        for name, _ in SCENARIOS:
            s += f" | {name}={float(r[f'{name}_net_pct']):+.3f} (Δ{float(r[f'{name}_delta_net_vs_CURRENT']):+.3f})"
        lines.append(s)
    lines += ["", "[DAILY STABILITY]",
              f"CURRENT {cur_stab}"]
    for name, _ in SCENARIOS:
        lines.append(f"{name} {scenario_audit[name]['daily_stability']}")
    lines += [
        "",
        "[INTERPRETATION RULE]",
        "Do not pick a threshold from total net alone. Check monthly consistency, daily worst-day improvement,",
        "STOPs removed versus TPs removed, and replacement-entry effects. This is path-dependent scheduling.",
        "Net is sum of per-trade net %p, not account return.",
    ]
    sump = outdir / f"{pref}_SUMMARY.txt"
    sump.write_text("\n".join(lines) + "\n", encoding="utf-8")
    files.append(sump)

    state.update(phase="done", current_net=cur_stats["net_pct"], result_prefix=pref)

    zipp = outdir / f"{pref}_RESULTS.zip"
    with zipfile.ZipFile(zipp, "w", zipfile.ZIP_DEFLATED) as z:
        seen = set()
        for p in files + [sim_path, work / "CRASH15_COMPARE_STATE.json"]:
            if p.exists() and str(p) not in seen:
                z.write(p, arcname=p.name)
                seen.add(str(p))
        cpdir = work / "CRASH15_MONTHLY_CHECKPOINTS"
        if cpdir.exists():
            for p in sorted(cpdir.glob("CRASH*")):
                z.write(p, arcname=f"monthly_checkpoints/{p.name}")

    pflush("\n" + "\n".join(lines))
    pflush("RESULT_ZIP=" + str(zipp))


if __name__ == "__main__":
    main()
