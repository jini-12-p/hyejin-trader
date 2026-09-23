#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fresh out-of-sample validation:
Current exact BASE vs RISK BLOCK on NEW data only.

Evaluation source window supplied by user:
  2026-09-22 14:18:34 KST ~ 2026-09-23 22:10:43 KST

To avoid treating still-open late entries as completed, exact replay uses only
entries with a full 3-hour outcome horizon:
  2026-09-22 14:18:34 KST ~ 2026-09-23 19:10:43 KST

Both scenarios start from the SAME BASE portfolio state at evaluation start.
Warm-up runs BASE from 2026-09-22 08:00 KST to build:
- open slots
- rolling 15m entry cap
- same-symbol cooldown
- STOP/LATE 180m cooldown
- 30m STOP pause

Scenario A = current exact BASE
  SAFE/RELAX/MKT100
  4 slots / rolling15m max2 / cooldowns / STOP pause
  TP +2.0% FULL
  PP12
  Final 4-stage stop
  V27-1 fallback
  taker 0.055% entry + exits

Scenario B = RISK_BLOCK
  identical to BASE, except BEFORE portfolio scheduling:
  rolling 2h V25 confirmed count INCLUDING current >= 8
  AND mean(abs(BTC4h), abs(ETH4h)) >= 0.40%
  -> block entry 100%

Outputs identify:
- direct BASE risk trades removed
- NEW trades admitted because slots/cooldowns changed
- BASE non-risk trades later displaced by reshuffle
- exact P&L of all groups
"""
from __future__ import annotations

import copy
import csv
import importlib.util
import sys
import zipfile
from collections import Counter, deque
from datetime import datetime as RealDateTime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
U_PATH = ROOT / "unified_current_0901_0922.py"

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

WARM_START_KST = RealDateTime(2026, 9, 22, 8, 0, 0, tzinfo=KST)
EVAL_START_KST = RealDateTime(2026, 9, 22, 14, 18, 34, tzinfo=KST)
DATA_END_KST = RealDateTime(2026, 9, 23, 22, 10, 43, tzinfo=KST)
# Full 3h observation horizon for current BASE max-hold/path.
EVAL_END_KST = DATA_END_KST - timedelta(hours=3)

RISK_V25_2H_MIN = 8
RISK_ABS4H_AVG_MIN = 0.40

OUT_COMPARE = ROOT / "FRESH_RISK_COMPARE.csv"
OUT_DAILY = ROOT / "FRESH_RISK_DAILY.csv"
OUT_BASE = ROOT / "FRESH_BASE_TRADES.csv"
OUT_BLOCK = ROOT / "FRESH_RISK_BLOCK_TRADES.csv"
OUT_NEW = ROOT / "FRESH_RISK_BLOCK_NEW_ENTRIES.csv"
OUT_REMOVED = ROOT / "FRESH_RISK_BLOCK_REMOVED_BASE.csv"
OUT_LOST = ROOT / "FRESH_RISK_BLOCK_LOST_NONRISK.csv"
OUT_SUMMARY = ROOT / "FRESH_RISK_SUMMARY.txt"
OUT_ZIP = ROOT / "FRESH_RISK_OOS_RESULTS.zip"

SCRIPT_VERSION = "FRESH_RISK_OOS_v1_20260923"


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


U = load_module("UNIFIED_FRESH_OOS", U_PATH)


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
    with path.open("w", newline="", encoding="utf-8-sig") as fp:
        w = csv.DictWriter(fp, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def configure_unified():
    # Fresh range.
    U.START_KST = WARM_START_KST
    U.END_KST = EVAL_END_KST + timedelta(seconds=1)
    U.START_UTC = U.START_KST.astimezone(UTC)
    U.END_UTC = U.END_KST.astimezone(UTC)
    U.EXPECTED_V25 = -1

    # Do not reuse the old 9/1~9/22 market cache file, which may end too early.
    U.CACHE_DIR = ROOT / ".fresh_risk_oos_kline_cache"
    U.CACHE_DIR.mkdir(parents=True, exist_ok=True)
    U.KC = U.KlineCache()
    U._market_1m = {}
    U._market_recompute_count = 0


def build_risk_meta(setups: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    q = deque()
    out = {}
    for st in setups:
        now = st["entry"]
        cutoff = now - timedelta(hours=2)
        while q and q[0] < cutoff:
            q.popleft()
        q.append(now)
        d = st["details"]
        b4 = U.first_f(d, "btc_4h_change_pct", "btc_4h")
        e4 = U.first_f(d, "eth_4h_change_pct", "eth_4h")
        avgabs = None if b4 is None or e4 is None else (abs(b4) + abs(e4)) / 2.0
        risk = bool(
            len(q) >= RISK_V25_2H_MIN
            and avgabs is not None
            and avgabs >= RISK_ABS4H_AVG_MIN
        )
        out[st["setup_id"]] = {
            "v25_2h_count": len(q),
            "btc4h": b4,
            "eth4h": e4,
            "abs4h_avg": avgabs,
            "risk": risk,
        }
    return out


def row_for(st, ef, rm, scenario):
    return {
        "scenario": scenario,
        "setup_id": st["setup_id"],
        "symbol": st["symbol"],
        "entry_time_kst": U.kst_stamp(st["entry"]),
        "entry_ts_utc": st["entry"].isoformat(),
        "entry_price": st["entry_price"],
        "risk_regime": int(rm["risk"]),
        "v25_2h_count": rm["v25_2h_count"],
        "btc4h": rm["btc4h"],
        "eth4h": rm["eth4h"],
        "abs4h_avg": rm["abs4h_avg"],
        "safe_block_raw": int(ef["safe"]["block"]),
        "safe_relaxed": int(ef["safe_relaxed"]),
        "mkt100_block": int(ef["mkt"]["block"]),
        "filter_pass": int(bool(ef["pass"])),
        "accepted": 0,
        "block_reason": ef["reason"],
        "result": "",
        "exit_time_kst": "",
        "gross_pct": "",
        "fee_pct": "",
        "net_pct": "",
        "stop_stage": "",
        "mfe_pct": "",
        "mae_pct": "",
        "data_error": "",
    }


def attach_sim(row, sim):
    row.update({
        "accepted": 1,
        "block_reason": "",
        "result": sim.result,
        "exit_time_kst": U.kst_stamp(sim.exit_time),
        "exit_ts_utc": sim.exit_time.isoformat(),
        "terminal_price": sim.terminal_price,
        "gross_pct": round(sim.gross_pct, 6),
        "fee_pct": round(sim.fee_pct, 6),
        "net_pct": round(sim.net_pct, 6),
        "stop_stage": sim.stop_stage,
        "mfe_pct": round(sim.mfe_pct, 6),
        "mae_pct": round(sim.mae_pct, 6),
        "data_error": sim.data_error,
    })


def warm_base(setups, controls, risk_meta, sim_cache):
    sched = U.Scheduler()
    n = 0
    for st in setups:
        if st["entry"] >= EVAL_START_KST.astimezone(UTC):
            break
        ef = U.entry_filter(st, controls)
        if not ef["pass"]:
            continue
        ok, _ = sched.can_open(st["entry"], st["symbol"])
        if not ok:
            continue
        sid = st["setup_id"]
        sim = sim_cache.get(sid)
        if sim is None:
            sim = U.simulate_base(st)
            sim_cache[sid] = sim
        stop_like = sim.result in ("STOP", "LATE_FAILURE_EXIT")
        sched.add(st["entry"], st["symbol"], sim, stop_like)
        n += 1
    print(f"[WARM] accepted={n}", flush=True)
    return sched


def run_eval(setups, controls, rmeta, warm_sched, sim_cache, risk_block=False):
    sched = copy.deepcopy(warm_sched)
    rows = []
    accepted = []
    start_utc = EVAL_START_KST.astimezone(UTC)
    end_utc = EVAL_END_KST.astimezone(UTC)

    eval_setups = [x for x in setups if start_utc <= x["entry"] <= end_utc]
    for i, st in enumerate(eval_setups, 1):
        rm = rmeta[st["setup_id"]]
        ef = U.entry_filter(st, controls)
        row = row_for(st, ef, rm, "RISK_BLOCK" if risk_block else "BASE")

        if not ef["pass"]:
            rows.append(row)
            continue

        if risk_block and rm["risk"]:
            row["block_reason"] = "RISK_2H8_ABS4H04"
            rows.append(row)
            continue

        ok, why = sched.can_open(st["entry"], st["symbol"])
        if not ok:
            row["block_reason"] = why
            rows.append(row)
            continue

        sid = st["setup_id"]
        sim = sim_cache.get(sid)
        if sim is None:
            sim = U.simulate_base(st)
            sim_cache[sid] = sim
        attach_sim(row, sim)
        stop_like = sim.result in ("STOP", "LATE_FAILURE_EXIT")
        sched.add(st["entry"], st["symbol"], sim, stop_like)
        rows.append(row)
        accepted.append(row)

        if i % 25 == 0 or i == len(eval_setups):
            print(
                f"[{'RISK' if risk_block else 'BASE'}] "
                f"{i}/{len(eval_setups)} accepted={len(accepted)}",
                flush=True,
            )
    return rows, accepted


def day(s):
    return str(s)[:10]


def daily_compare(base_rows, risk_rows):
    dates = sorted(set(day(r["entry_time_kst"]) for r in base_rows + risk_rows))
    out = []
    for d in dates:
        bz = [r for r in base_rows if day(r["entry_time_kst"]) == d and int(r["accepted"]) == 1]
        rz = [r for r in risk_rows if day(r["entry_time_kst"]) == d and int(r["accepted"]) == 1]
        bn = sum(float(r["net_pct"]) for r in bz)
        rn = sum(float(r["net_pct"]) for r in rz)
        b_ids = {r["setup_id"] for r in bz}
        r_ids = {r["setup_id"] for r in rz}
        new = [r for r in rz if r["setup_id"] not in b_ids]
        removed = [r for r in bz if r["setup_id"] not in r_ids]
        out.append({
            "date": d,
            "BASE_ENTRIES": len(bz),
            "BASE_NET": round(bn, 6),
            "RISK_BLOCK_ENTRIES": len(rz),
            "RISK_BLOCK_NET": round(rn, 6),
            "DELTA": round(rn - bn, 6),
            "NEW_ENTRIES": len(new),
            "NEW_NET": round(sum(float(r["net_pct"]) for r in new), 6),
            "REMOVED_BASE": len(removed),
            "REMOVED_BASE_NET": round(sum(float(r["net_pct"]) for r in removed), 6),
        })
    return out


def main():
    print(f"=== {SCRIPT_VERSION} ===", flush=True)
    configure_unified()

    if not U.DB_PATH.exists():
        raise SystemExit(f"DB not found: {U.DB_PATH}")

    print("[1/7] fresh market cache", flush=True)
    U.load_market_series()

    print("[2/7] load V25 / CONTROL", flush=True)
    setups = U.load_setups()
    controls = U.load_control_proxy()
    print(
        f"setups warm+eval={len(setups)} "
        f"range={U.kst_stamp(setups[0]['entry']) if setups else ''} ~ "
        f"{U.kst_stamp(setups[-1]['entry']) if setups else ''}",
        flush=True,
    )

    print("[3/7] causal risk tags", flush=True)
    rmeta = build_risk_meta(setups)

    print("[4/7] common BASE warm-up", flush=True)
    sim_cache = {}
    warm_sched = warm_base(setups, controls, rmeta, sim_cache)

    print("[5/7] exact BASE evaluation", flush=True)
    base_rows, base_acc = run_eval(
        setups, controls, rmeta, warm_sched, sim_cache, risk_block=False
    )

    print("[6/7] exact RISK_BLOCK evaluation", flush=True)
    risk_rows, risk_acc = run_eval(
        setups, controls, rmeta, warm_sched, sim_cache, risk_block=True
    )

    base_ids = {r["setup_id"] for r in base_acc}
    risk_ids = {r["setup_id"] for r in risk_acc}
    bmap = {r["setup_id"]: r for r in base_acc}
    rmap = {r["setup_id"]: r for r in risk_acc}

    removed_ids = sorted(base_ids - risk_ids)
    new_ids = sorted(risk_ids - base_ids)
    common_ids = base_ids & risk_ids

    removed = [bmap[x] for x in removed_ids]
    direct_risk_removed = [r for r in removed if int(r["risk_regime"]) == 1]
    lost_nonrisk = [r for r in removed if int(r["risk_regime"]) == 0]
    new_rows = [rmap[x] for x in new_ids]

    base_net = sum(float(r["net_pct"]) for r in base_acc)
    risk_net = sum(float(r["net_pct"]) for r in risk_acc)

    def outcomes(rows):
        return dict(Counter(str(r["result"]) for r in rows))

    compare = [{
        "scenario": "BASE",
        "entries": len(base_acc),
        "net_pct": round(base_net, 6),
        "delta_vs_base": 0.0,
        "outcomes": str(outcomes(base_acc)),
    }, {
        "scenario": "RISK_BLOCK",
        "entries": len(risk_acc),
        "net_pct": round(risk_net, 6),
        "delta_vs_base": round(risk_net-base_net, 6),
        "outcomes": str(outcomes(risk_acc)),
    }]

    write_csv(OUT_COMPARE, compare)
    write_csv(OUT_BASE, base_rows)
    write_csv(OUT_BLOCK, risk_rows)
    write_csv(OUT_NEW, new_rows)
    write_csv(OUT_REMOVED, direct_risk_removed)
    write_csv(OUT_LOST, lost_nonrisk)
    write_csv(OUT_DAILY, daily_compare(base_rows, risk_rows))

    direct_net = sum(float(r["net_pct"]) for r in direct_risk_removed)
    new_net = sum(float(r["net_pct"]) for r in new_rows)
    lost_net = sum(float(r["net_pct"]) for r in lost_nonrisk)

    risk_base_acc = [r for r in base_acc if int(r["risk_regime"]) == 1]
    normal_base_acc = [r for r in base_acc if int(r["risk_regime"]) == 0]

    lines = [
        "FRESH OUT-OF-SAMPLE RISK BLOCK",
        f"script={SCRIPT_VERSION}",
        f"evaluation={EVAL_START_KST.isoformat()} ~ {EVAL_END_KST.isoformat()}",
        f"source_data_end={DATA_END_KST.isoformat()}",
        "late 3h is intentionally excluded so every evaluated entry has a full BASE outcome horizon",
        "",
        "[RISK RULE — FROZEN BEFORE THIS SAMPLE]",
        f"V25 rolling2h >= {RISK_V25_2H_MIN}",
        f"AND mean(abs(BTC4h),abs(ETH4h)) >= {RISK_ABS4H_AVG_MIN:.2f}%",
        "",
        "[BASE]",
        f"entries={len(base_acc)} NET={base_net:.6f}%p outcomes={outcomes(base_acc)}",
        f"BASE risk entries={len(risk_base_acc)} NET={sum(float(r['net_pct']) for r in risk_base_acc):.6f}%p",
        f"BASE normal entries={len(normal_base_acc)} NET={sum(float(r['net_pct']) for r in normal_base_acc):.6f}%p",
        "",
        "[RISK BLOCK FULL RESCHEDULE]",
        f"entries={len(risk_acc)} NET={risk_net:.6f}%p",
        f"DELTA_vs_BASE={risk_net-base_net:+.6f}%p",
        "",
        "[DO OTHER TRADES ENTER?]",
        f"direct BASE risk trades removed={len(direct_risk_removed)} NET_was={direct_net:.6f}%p",
        f"NEW trades admitted={len(new_rows)} NEW_NET={new_net:.6f}%p outcomes={outcomes(new_rows)}",
        f"BASE non-risk trades later lost by reshuffle={len(lost_nonrisk)} NET_was={lost_net:.6f}%p",
        f"common trades={len(common_ids)}",
        "",
        "[DELTA DECOMPOSITION]",
        f"remove direct risk = {-direct_net:+.6f}%p",
        f"add NEW trades = {new_net:+.6f}%p",
        f"remove displaced non-risk = {-lost_net:+.6f}%p",
        f"sum={(-direct_net + new_net - lost_net):+.6f}%p",
        f"actual delta={risk_net-base_net:+.6f}%p",
        "",
        "[FILES]",
        OUT_COMPARE.name,
        OUT_DAILY.name,
        OUT_BASE.name,
        OUT_BLOCK.name,
        OUT_NEW.name,
        OUT_REMOVED.name,
        OUT_LOST.name,
    ]
    OUT_SUMMARY.write_text("\n".join(lines)+"\n", encoding="utf-8")
    print("\n".join(lines), flush=True)

    with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as z:
        for p in [
            OUT_COMPARE, OUT_DAILY, OUT_BASE, OUT_BLOCK, OUT_NEW,
            OUT_REMOVED, OUT_LOST, OUT_SUMMARY
        ]:
            z.write(p, arcname=p.name)
    print("DONE:", OUT_ZIP, flush=True)


if __name__ == "__main__":
    main()
