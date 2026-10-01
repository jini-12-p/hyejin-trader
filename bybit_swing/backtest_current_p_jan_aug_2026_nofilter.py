#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JAN-AUG 2026 CURRENT vs NO-FILTER V25 comparison (FINAL-ONLY / cache reuse)

This script intentionally DOES NOT regenerate historical candidates.
It reuses the completed Jan-Aug 2026 checkpoints produced by:
    backtest_current_p_jan_aug_2026_balanced.py

CURRENT scenario:
    Exact same final replay path as the balanced script:
    SAFE/RELAX -> MKT100 -> MARKET_GUARD -> V22Q -> portfolio scheduler

NO-FILTER V25 scenario:
    V25 confirmed candidates are unchanged.
    OFF: SAFE / RELAX / MKT100 / MARKET_GUARD / V22 QUALITY
    ON : 4 slots / rolling 15m max2 / CD90 / STOP-CD180 / STOP-pause45
    EXIT: same TP2.0 / PP12 / Final4 / V27-1 via the same U.simulate_base()

Important:
- No candidate generation.
- No dynamic-universe rebuild.
- Existing exit simulations are reused.
- Only a NO-FILTER accepted candidate missing from the sim checkpoint is simulated.
- Original JANAUG_FIXED_2026_WORK is never modified; comparison uses a separate work directory.
- Monthly checkpoint output is written while the NO-FILTER scheduler advances.
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
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

EXPECTED_BALANCED_SHA256 = "8cac368aff58784eabd249d4a8ec0efb924928dd0921999eeb67b55fde886b41"
EXPECTED_CANDIDATES = 20500
KNOWN_OUTCOMES = [
    "TP20_FULL",
    "PROFIT_PROTECT_EXIT",
    "STOP",
    "LATE_FAILURE_EXIT",
    "TIME_EXIT",
    "FLAT_EXIT_75M",
    "DATA_ERROR",
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


def num(v, default=0.0):
    try:
        x = float(v)
        return default if math.isnan(x) else x
    except Exception:
        return default


def build_rows(base, cand: pd.DataFrame):
    rows = []
    for r in cand.to_dict("records"):
        dt = pd.Timestamp(r["entry_dt"]).to_pydatetime().astimezone(base.UTC)
        rows.append({
            "setup_id": str(r["setup_id"]),
            "symbol": str(r["symbol"]),
            "entry": dt,
            "entry_price": float(r["entry_price"]),
            "details": base.jloads(r.get("details_json")),
            "watch": base.jloads(r.get("watch_details_json")),
            "raw": r,
        })
    rows.sort(key=lambda x: (x["entry"], x["setup_id"]))
    return rows


def write_month_checkpoint(base, cpdir: Path, month: str, month_rows: list[dict]):
    if not month_rows:
        return
    cpdir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(month_rows)
    allp = cpdir / f"NOFILTER_{month}_ALL.csv.gz"
    trp = cpdir / f"NOFILTER_{month}_TRADES.csv"
    blp = cpdir / f"NOFILTER_{month}_BLOCKS.csv.gz"
    df.to_csv(allp, index=False, compression="gzip", encoding="utf-8-sig")
    df[df["accepted"] == 1].to_csv(trp, index=False, encoding="utf-8-sig")
    df[df["accepted"] == 0].to_csv(blp, index=False, compression="gzip", encoding="utf-8-sig")
    pflush(f"[NF CHECKPOINT] {month} rows={len(df)} entries={int((df['accepted']==1).sum())}")


def run_nofilter(base, U, root: Path, work: Path, cand: pd.DataFrame,
                 state, fill_all_sims: bool = False):
    """Replay V25 candidates with entry filters OFF and scheduler constraints ON."""
    pflush("\n=== NO-FILTER V25 REPLAY ===")
    pflush("OFF=SAFE/RELAX/MKT100/MARKET_GUARD/V22Q")
    pflush("ON=4slots/15m-max2/CD90/STOP-CD180/STOP-pause45 + SAME EXITS")

    U.START_KST = base.START_KST
    U.END_KST = base.END_KST
    U.START_UTC = base.START_UTC
    U.END_UTC = base.END_UTC
    U.EXPECTED_V25 = -1
    U.CACHE_DIR = root / ".janaug_exit_cache_v1"
    U.CACHE_DIR.mkdir(exist_ok=True)
    U.KC = U.KlineCache()
    U.STOP_PAUSE_WINDOW_MIN = base.FINAL_STOP_PAUSE_MIN

    rows = build_rows(base, cand)
    current_sim_path = work / "JAN_AUG_SIM_RESULTS.csv.gz"
    nf_sim_path = work / "JAN_AUG_SIM_RESULTS_NOFILTER.csv.gz"
    if not current_sim_path.exists():
        raise RuntimeError(f"missing CURRENT sim checkpoint: {current_sim_path}")
    if not nf_sim_path.exists():
        shutil.copy2(current_sim_path, nf_sim_path)
        pflush(f"[NF SIM] seeded from CURRENT cache -> {nf_sim_path}")

    sim_cache = base.load_sim_checkpoint(U, nf_sim_path)
    initial_cached = len(sim_cache)
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
            if new_sim % base.FLUSH_SIM_EVERY == 0:
                base.flush_sim_checkpoint(nf_sim_path, sim_cache)
                state.update(phase="nofilter_sim", sim_cached=len(sim_cache), new_sim=new_sim)
                pflush(f"[NF SIM] cached={len(sim_cache)}/{len(rows)} new={new_sim}")
        return sim_cache[sid]

    if fill_all_sims:
        pflush("[NF SIM] --fill-all-sims enabled: filling every missing V25 candidate outcome")
        for i, st in enumerate(rows, 1):
            get_sim(st)
            if i % 250 == 0:
                pflush(f"[NF FILL] {i}/{len(rows)} cached={len(sim_cache)} new={new_sim}")
        base.flush_sim_checkpoint(nf_sim_path, sim_cache)

    sched = U.Scheduler()
    out, trades, blocks, stops = [], [], [], []
    cpdir = work / "NOFILTER_MONTHLY_CHECKPOINTS"
    current_month = None
    month_rows: list[dict] = []

    for i, st in enumerate(rows, 1):
        mk = st["entry"].astimezone(base.KST).strftime("%Y-%m")
        if current_month is None:
            current_month = mk
        elif mk != current_month:
            write_month_checkpoint(base, cpdir, current_month, month_rows)
            base.flush_sim_checkpoint(nf_sim_path, sim_cache)
            state.update(phase="nofilter_replay", completed_month=current_month,
                         i=i-1, entries=len(trades), stops=len(stops), sim_cached=len(sim_cache))
            month_rows = []
            current_month = mk

        row = {
            "setup_id": st["setup_id"],
            "symbol": st["symbol"],
            "entry_time_kst": st["entry"].astimezone(base.KST).strftime("%Y-%m-%d %H:%M:%S"),
            "entry_time_utc": st["entry"].isoformat(),
            "entry_price": st["entry_price"],
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

        ok, why = sched.can_open(st["entry"], st["symbol"])
        if not ok:
            row["block_reason"] = "STOP_PAUSE45" if why == "STOP_PAUSE30" else why
            blocks.append(row.copy())
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

        out.append(row)
        month_rows.append(row.copy())

        if i % 250 == 0:
            pflush(f"[NOFILTER] {i}/{len(rows)} entries={len(trades)} STOP={len(stops)} "
                   f"sim_cached={len(sim_cache)} new_sim={new_sim}")
            state.update(phase="nofilter_replay", i=i, entries=len(trades), stops=len(stops),
                         sim_cached=len(sim_cache), new_sim=new_sim)

    if current_month is not None:
        write_month_checkpoint(base, cpdir, current_month, month_rows)
    base.flush_sim_checkpoint(nf_sim_path, sim_cache)
    state.update(phase="nofilter_replay_done", entries=len(trades), stops=len(stops),
                 sim_cached=len(sim_cache), new_sim=new_sim)
    return out, trades, blocks, stops, nf_sim_path, initial_cached, len(sim_cache), new_sim


def scenario_table(out: list[dict], period: str, start_kst, end_kst) -> pd.DataFrame:
    df = pd.DataFrame(out)
    if df.empty:
        return pd.DataFrame()
    df["accepted"] = pd.to_numeric(df["accepted"], errors="coerce").fillna(0).astype(int)
    df["net_num"] = pd.to_numeric(df["net_pct"], errors="coerce").fillna(0.0)
    if period == "date":
        df["period"] = df["entry_time_kst"].astype(str).str[:10]
        full = pd.date_range(start_kst.date(), (end_kst - pd.Timedelta(days=1)).date(), freq="D").strftime("%Y-%m-%d")
    elif period == "month":
        df["period"] = df["entry_time_kst"].astype(str).str[:7]
        full = pd.period_range(start=start_kst.strftime("%Y-%m"), end=(end_kst - pd.Timedelta(days=1)).strftime("%Y-%m"), freq="M").astype(str)
    else:
        raise ValueError(period)

    rows = []
    for key in full:
        g = df[df["period"] == key]
        a = g[g["accepted"] == 1]
        rc = Counter(a["result"].astype(str))
        bc = Counter(g[g["accepted"] == 0]["block_reason"].astype(str))
        known_sum = sum(rc[k] for k in KNOWN_OUTCOMES)
        row = {
            period: key,
            "v25_candidates": len(g),
            "entries": len(a),
            "net_pct": round(float(a["net_num"].sum()), 6),
        }
        for k in KNOWN_OUTCOMES:
            row[k] = rc[k]
        row["OTHER_OUTCOME"] = max(0, len(a) - known_sum)
        for reason in ("CAP15_2", "SLOT4", "COOLDOWN90", "COOLDOWN180", "STOP_PAUSE45"):
            row[f"BLOCK_{reason}"] = bc[reason]
        rows.append(row)
    return pd.DataFrame(rows)


def comparison_table(cur: pd.DataFrame, nf: pd.DataFrame, key: str) -> pd.DataFrame:
    c = cur.copy().add_prefix("current_").rename(columns={f"current_{key}": key})
    n = nf.copy().add_prefix("nofilter_").rename(columns={f"nofilter_{key}": key})
    x = c.merge(n, on=key, how="outer").fillna(0)
    for col in ("entries", "net_pct", "TP20_FULL", "PROFIT_PROTECT_EXIT", "STOP",
                "LATE_FAILURE_EXIT", "TIME_EXIT", "FLAT_EXIT_75M", "DATA_ERROR"):
        cc, nn = f"current_{col}", f"nofilter_{col}"
        if cc in x and nn in x:
            x[f"delta_{col}"] = pd.to_numeric(x[nn], errors="coerce").fillna(0) - pd.to_numeric(x[cc], errors="coerce").fillna(0)
    return x


def max_negative_streak(daily: pd.DataFrame) -> int:
    best = cur = 0
    for x in pd.to_numeric(daily["net_pct"], errors="coerce").fillna(0):
        if x < 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def stability_lines(name: str, daily: pd.DataFrame) -> list[str]:
    z = pd.to_numeric(daily["net_pct"], errors="coerce").fillna(0)
    plus = int((z > 0).sum())
    minus = int((z < 0).sum())
    zero = int((z == 0).sum())
    near = int((z.abs() < 1.0).sum())
    worst_i = z.idxmin()
    best_i = z.idxmax()
    return [
        f"{name}: plus_days={plus} minus_days={minus} zero_days={zero} abs(net)<1_days={near}",
        f"{name}: median_day={z.median():+.6f}%p mean_day={z.mean():+.6f}%p",
        f"{name}: worst_day={daily.loc[worst_i,'date']} {z.loc[worst_i]:+.6f}%p",
        f"{name}: best_day={daily.loc[best_i,'date']} {z.loc[best_i]:+.6f}%p",
        f"{name}: max_consecutive_negative_days={max_negative_streak(daily)}",
    ]


def write_outputs(base, root: Path, work: Path, cand: pd.DataFrame,
                  current_out, current_trades, current_blocks, current_stops,
                  nf_out, nf_trades, nf_blocks, nf_stops,
                  nf_sim_path: Path, state, source_hashes: dict,
                  initial_cached: int, final_cached: int, new_sim: int):
    stamp = datetime.now(base.KST).strftime("%Y%m%d_%H%M%S")
    pref = f"CURRENT_VS_NOFILTER_JANAUG_2026_{stamp}"

    files = {
        "cur_all": root / f"{pref}_CURRENT_ALL.csv.gz",
        "cur_trades": root / f"{pref}_CURRENT_TRADES.csv",
        "nf_all": root / f"{pref}_NOFILTER_ALL.csv.gz",
        "nf_trades": root / f"{pref}_NOFILTER_TRADES.csv",
        "nf_blocks": root / f"{pref}_NOFILTER_BLOCKS.csv.gz",
        "nf_stops": root / f"{pref}_NOFILTER_STOP_ONLY.csv",
        "cur_daily": root / f"{pref}_CURRENT_DAILY.csv",
        "nf_daily": root / f"{pref}_NOFILTER_DAILY.csv",
        "cur_monthly": root / f"{pref}_CURRENT_MONTHLY.csv",
        "nf_monthly": root / f"{pref}_NOFILTER_MONTHLY.csv",
        "cmp_daily": root / f"{pref}_COMPARE_DAILY.csv",
        "cmp_monthly": root / f"{pref}_COMPARE_MONTHLY.csv",
        "changes": root / f"{pref}_ENTRY_SET_CHANGES.csv",
        "nf_only": root / f"{pref}_NOFILTER_ONLY_TRADES.csv",
        "cur_only": root / f"{pref}_CURRENT_ONLY_TRADES.csv",
        "summary": root / f"{pref}_SUMMARY.txt",
        "audit": root / f"{pref}_AUDIT.json",
        "zip": root / f"{pref}_RESULTS.zip",
    }

    pd.DataFrame(current_out).to_csv(files["cur_all"], index=False, compression="gzip", encoding="utf-8-sig")
    pd.DataFrame(current_trades).to_csv(files["cur_trades"], index=False, encoding="utf-8-sig")
    pd.DataFrame(nf_out).to_csv(files["nf_all"], index=False, compression="gzip", encoding="utf-8-sig")
    pd.DataFrame(nf_trades).to_csv(files["nf_trades"], index=False, encoding="utf-8-sig")
    pd.DataFrame(nf_blocks).to_csv(files["nf_blocks"], index=False, compression="gzip", encoding="utf-8-sig")
    pd.DataFrame(nf_stops).to_csv(files["nf_stops"], index=False, encoding="utf-8-sig")

    cur_daily = scenario_table(current_out, "date", base.START_KST, base.END_KST)
    nf_daily = scenario_table(nf_out, "date", base.START_KST, base.END_KST)
    cur_monthly = scenario_table(current_out, "month", base.START_KST, base.END_KST)
    nf_monthly = scenario_table(nf_out, "month", base.START_KST, base.END_KST)
    cmp_daily = comparison_table(cur_daily, nf_daily, "date")
    cmp_monthly = comparison_table(cur_monthly, nf_monthly, "month")

    cur_daily.to_csv(files["cur_daily"], index=False, encoding="utf-8-sig")
    nf_daily.to_csv(files["nf_daily"], index=False, encoding="utf-8-sig")
    cur_monthly.to_csv(files["cur_monthly"], index=False, encoding="utf-8-sig")
    nf_monthly.to_csv(files["nf_monthly"], index=False, encoding="utf-8-sig")
    cmp_daily.to_csv(files["cmp_daily"], index=False, encoding="utf-8-sig")
    cmp_monthly.to_csv(files["cmp_monthly"], index=False, encoding="utf-8-sig")

    cur_map = {str(r["setup_id"]): r for r in current_trades}
    nf_map = {str(r["setup_id"]): r for r in nf_trades}
    cur_ids, nf_ids = set(cur_map), set(nf_map)
    both = cur_ids & nf_ids
    nf_only_ids = nf_ids - cur_ids
    cur_only_ids = cur_ids - nf_ids

    changes = []
    for sid in sorted(nf_only_ids, key=lambda s: (nf_map[s].get("entry_time_utc", ""), s)):
        r = nf_map[sid]
        changes.append({"class": "NOFILTER_ONLY", **r})
    for sid in sorted(cur_only_ids, key=lambda s: (cur_map[s].get("entry_time_utc", ""), s)):
        r = cur_map[sid]
        changes.append({"class": "CURRENT_ONLY", **r})
    pd.DataFrame(changes).to_csv(files["changes"], index=False, encoding="utf-8-sig")
    pd.DataFrame([nf_map[s] for s in sorted(nf_only_ids, key=lambda s: (nf_map[s].get("entry_time_utc", ""), s))]).to_csv(
        files["nf_only"], index=False, encoding="utf-8-sig")
    pd.DataFrame([cur_map[s] for s in sorted(cur_only_ids, key=lambda s: (cur_map[s].get("entry_time_utc", ""), s))]).to_csv(
        files["cur_only"], index=False, encoding="utf-8-sig")

    cur_net = float(pd.to_numeric(pd.Series([r.get("net_pct", 0) for r in current_trades]), errors="coerce").fillna(0).sum())
    nf_net = float(pd.to_numeric(pd.Series([r.get("net_pct", 0) for r in nf_trades]), errors="coerce").fillna(0).sum())
    cur_rc = Counter(str(r.get("result", "")) for r in current_trades)
    nf_rc = Counter(str(r.get("result", "")) for r in nf_trades)

    lines = [
        "CURRENT vs NO-FILTER V25 — JAN-AUG 2026",
        "STATUS=APPROXIMATE_HISTORICAL_REPLAY / FINAL-ONLY CACHE REUSE",
        "",
        "[AUDIT]",
        f"V25 candidates={len(cand)}",
        f"candidate_months={sorted(cand['entry_time_kst'].astype(str).str[:7].unique().tolist())}",
        f"balanced_sha256={source_hashes.get('balanced')}",
        f"unified_sha256={source_hashes.get('unified')}",
        f"nofilter_sim_cache_initial={initial_cached}",
        f"nofilter_sim_cache_final={final_cached}",
        f"nofilter_new_simulated={new_sim}",
        "candidate_generation=NOT RUN",
        "",
        "[SCENARIO RULES]",
        "CURRENT = original balanced FINAL replay",
        "NOFILTER = SAFE/RELAX/MKT100/MARKET_GUARD/V22Q OFF",
        "BOTH = 4 slots / rolling15m max2 / CD90 / STOP-CD180 / STOP-pause45",
        "BOTH = same TP2.0 / PP12 / Final4 / V27-1 exit simulator and fee model",
        "",
        "[TOTAL]",
        f"CURRENT entries={len(current_trades)} STOP={cur_rc['STOP']} net={cur_net:+.6f}%p",
        f"NOFILTER entries={len(nf_trades)} STOP={nf_rc['STOP']} net={nf_net:+.6f}%p",
        f"DELTA(NOFILTER-CURRENT) entries={len(nf_trades)-len(current_trades):+d} "
        f"STOP={nf_rc['STOP']-cur_rc['STOP']:+d} net={nf_net-cur_net:+.6f}%p",
        f"entry_overlap BOTH={len(both)} NOFILTER_ONLY={len(nf_only_ids)} CURRENT_ONLY={len(cur_only_ids)}",
        "",
        "[MONTHLY]",
    ]
    for _, r in cmp_monthly.iterrows():
        lines.append(
            f"{r['month']} CURRENT entries={int(r['current_entries'])} STOP={int(r['current_STOP'])} net={float(r['current_net_pct']):+.6f}%p | "
            f"NOFILTER entries={int(r['nofilter_entries'])} STOP={int(r['nofilter_STOP'])} net={float(r['nofilter_net_pct']):+.6f}%p | "
            f"DELTA net={float(r['delta_net_pct']):+.6f}%p"
        )
    lines += ["", "[DAILY STABILITY]"]
    lines += stability_lines("CURRENT", cur_daily)
    lines += stability_lines("NOFILTER", nf_daily)
    lines += [
        "",
        "[IMPORTANT]",
        "Net is the sum of per-trade net %p, not account return.",
        "NO-FILTER comparison is path-dependent because earlier accepted candidates can change slots/cooldowns/pause and therefore later entries.",
        "Do not compare filters by simply summing blocked candidate outcomes; this script re-schedules candidates chronologically.",
    ]
    files["summary"].write_text("\n".join(lines) + "\n", encoding="utf-8")

    audit = {
        "generated_at_kst": datetime.now(base.KST).isoformat(),
        "candidate_count": len(cand),
        "expected_candidate_count": EXPECTED_CANDIDATES,
        "source_hashes": source_hashes,
        "nofilter_sim_cache_initial": initial_cached,
        "nofilter_sim_cache_final": final_cached,
        "nofilter_new_simulated": new_sim,
        "current": {"entries": len(current_trades), "stops": cur_rc["STOP"], "net_pct": cur_net},
        "nofilter": {"entries": len(nf_trades), "stops": nf_rc["STOP"], "net_pct": nf_net},
        "entry_sets": {"both": len(both), "nofilter_only": len(nf_only_ids), "current_only": len(cur_only_ids)},
        "candidate_generation_run": False,
    }
    files["audit"].write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")

    state.update(phase="done", result_zip=str(files["zip"]),
                 current_net=cur_net, nofilter_net=nf_net,
                 current_entries=len(current_trades), nofilter_entries=len(nf_trades))

    members = [p for k, p in files.items() if k != "zip"]
    members += [nf_sim_path, work / "NOFILTER_COMPARE_STATE.json"]
    with zipfile.ZipFile(files["zip"], "w", zipfile.ZIP_DEFLATED) as z:
        seen = set()
        for p in members:
            if p.exists() and str(p) not in seen:
                z.write(p, arcname=p.name)
                seen.add(str(p))
        cpdir = work / "NOFILTER_MONTHLY_CHECKPOINTS"
        if cpdir.exists():
            for p in sorted(cpdir.glob("NOFILTER_*")):
                z.write(p, arcname=f"monthly_checkpoints/{p.name}")

    pflush("\n" + "\n".join(lines))
    pflush("RESULT_ZIP=" + str(files["zip"]))
    return files["zip"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/root/hyejin-trader/bybit_swing")
    ap.add_argument("--fill-all-sims", action="store_true",
                    help="Optional: fill outcomes for all 20,500 candidates. NOT required for exact NO-FILTER scheduling.")
    ap.add_argument("--allow-candidate-count-mismatch", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).expanduser().resolve()
    balanced_path = root / "backtest_current_p_jan_aug_2026_balanced.py"
    unified_path = root / "unified_current_0901_0922.py"
    source_work = root / "JANAUG_FIXED_2026_WORK"
    work = root / "JANAUG_NOFILTER_COMPARE_WORK"

    for p in (balanced_path, unified_path, source_work / "JAN_AUG_SIM_RESULTS.csv.gz"):
        if not p.exists():
            raise SystemExit(f"MISSING required source/checkpoint: {p}")

    # Protect the completed CURRENT research work. Candidate checkpoints are symlinked
    # read-only by convention; the sim checkpoint is copied and only the copy is extended.
    work.mkdir(exist_ok=True)
    for m in range(1, 9):
        name = f"2026-{m:02d}_CANDIDATES.csv.gz"
        src = source_work / name
        dst = work / name
        if not src.exists():
            raise SystemExit(f"MISSING candidate checkpoint: {src}")
        if not dst.exists():
            dst.symlink_to(src)
    compare_sim = work / "JAN_AUG_SIM_RESULTS.csv.gz"
    if not compare_sim.exists():
        shutil.copy2(source_work / "JAN_AUG_SIM_RESULTS.csv.gz", compare_sim)
        pflush(f"SEEDED compare CURRENT sim cache -> {compare_sim}")

    for p in (str(root.parent), str(root)):
        if p not in sys.path:
            sys.path.insert(0, p)

    balanced_sha = sha256_file(balanced_path)
    if balanced_sha != EXPECTED_BALANCED_SHA256:
        pflush("WARNING: balanced script SHA256 differs from the uploaded reference.")
        pflush(f" expected={EXPECTED_BALANCED_SHA256}")
        pflush(f" actual  ={balanced_sha}")
        pflush("Proceeding because the server source is authoritative; verify if unexpected.")

    base = load_module("JANAUG_BALANCED_BASE_FOR_NF", balanced_path)
    # This comparison runs beside the live/shadow bot. Lower only the in-RAM kline
    # cache cap (no trading-rule change) to reduce memory pressure on the VPS.
    base.EXIT_KLINE_MEM_MAX = min(int(getattr(base, "EXIT_KLINE_MEM_MAX", 32)), 12)
    base.FLUSH_SIM_EVERY = min(int(getattr(base, "FLUSH_SIM_EVERY", 50)), 25)
    U = base.load_module("U_JANAUG_NF_COMPARE", unified_path)
    state = base.StateFile(work / "NOFILTER_COMPARE_STATE.json")
    state.update(phase="start", balanced_sha256=balanced_sha, unified_sha256=sha256_file(unified_path))

    cand = base.load_candidates(work)
    months = sorted(cand["entry_time_kst"].astype(str).str[:7].unique().tolist()) if not cand.empty else []
    pflush(f"FINAL-ONLY: loaded candidates={len(cand)} months={months}")
    if len(cand) != EXPECTED_CANDIDATES and not args.allow_candidate_count_mismatch:
        raise SystemExit(
            f"CANDIDATE COUNT MISMATCH: got {len(cand)}, expected {EXPECTED_CANDIDATES}. "
            "Candidate generation is intentionally disabled. Investigate checkpoints first."
        )
    expected_months = [f"2026-{m:02d}" for m in range(1, 9)]
    if months != expected_months:
        raise SystemExit(f"MONTH COVERAGE MISMATCH: got {months}, expected {expected_months}")

    # Reproduce CURRENT using the original function. This does NOT regenerate candidates.
    pflush("\n=== REPLAY CURRENT FROM EXISTING CANDIDATES/SIM CACHE ===")
    current_out, current_trades, current_blocks, current_stops, transitions, controls, ref_events = \
        base.run_final(root, work, U, cand, state)

    # Exact NO-FILTER scheduler replay using the same simulator and portfolio constraints.
    nf_out, nf_trades, nf_blocks, nf_stops, nf_sim_path, initial_cached, final_cached, new_sim = \
        run_nofilter(base, U, root, work, cand, state, fill_all_sims=args.fill_all_sims)

    source_hashes = {"balanced": balanced_sha, "unified": sha256_file(unified_path)}
    write_outputs(
        base, root, work, cand,
        current_out, current_trades, current_blocks, current_stops,
        nf_out, nf_trades, nf_blocks, nf_stops,
        nf_sim_path, state, source_hashes,
        initial_cached, final_cached, new_sim,
    )


if __name__ == "__main__":
    main()
