#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
9/24 20:43:03 ~ 9/25 Forward — 4-way exact portfolio replay

Compares, on the SAME data and SAME common warm portfolio state:

A) V25_BASE
   = existing base entry stack only
     (V25 + SAFE/RELAX + existing MKT100 + portfolio scheduler)
   = NO newly discovered V25 market risk guard
   = NO V22 quality add-on

B) V25_PLUS_MARKET
   = A + V25 market risk guard
     V25 confirmed count rolling 2h >= 8
     AND mean(abs(BTC4h), abs(ETH4h)) >= 0.40%

C) V25_PLUS_V22Q
   = A + V22 quality OR
     OVEREXT:
       p_v2_score >= 90 AND ema9_ema20_gap_pct >= 1.20
     WEAK_REACCEL:
       rebound_from_low_pct <= 5
       AND rsi_delta <= 7
       AND btc_15m_change_pct >= -0.08

D) V25_PLUS_MARKET_PLUS_V22Q
   = A + both B and C

This imports the already-used forward_full_0924_0925.py so the
entry filter, market series, TP/PP12/Final4/V27-1 replay and scheduler
remain identical to the previous validation.

No DB writes. No orders.
"""

from __future__ import annotations

import copy
import csv
import importlib.util
import sys
import zipfile
from collections import Counter
from pathlib import Path

ROOT = Path("/root/hyejin-trader/bybit_swing")
BASE_SCRIPT = ROOT / "forward_full_0924_0925.py"

OUT_SUM = ROOT / "FORWARD_0924_0925_4WAY_SUMMARY.csv"
OUT_DAILY = ROOT / "FORWARD_0924_0925_4WAY_DAILY.csv"
OUT_BLOCKS = ROOT / "FORWARD_0924_0925_4WAY_DIRECT_BLOCKS.csv"
OUT_TXT = ROOT / "FORWARD_0924_0925_4WAY_SUMMARY.txt"
OUT_ZIP = ROOT / "FORWARD_0924_0925_4WAY_RESULTS.zip"

SCENARIOS = [
    ("V25_BASE", False, False),
    ("V25_PLUS_MARKET", True, False),
    ("V25_PLUS_V22Q", False, True),
    ("V25_PLUS_MARKET_PLUS_V22Q", True, True),
]

def load_module(name, path):
    if not path.exists():
        raise SystemExit(f"missing: {path}")
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot import: {path}")
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m

M = load_module("FWD4_BASE", BASE_SCRIPT)
U = M.U

def write_csv(path, rows):
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    keys=[]; seen=set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with path.open("w",newline="",encoding="utf-8-sig") as fh:
        w=csv.DictWriter(fh,fieldnames=keys,extrasaction="ignore")
        w.writeheader(); w.writerows(rows)

def run_scenario(name, setups, controls, rmeta, qmeta, warm_sched,
                 sim_cache, eval_end_kst, use_market, use_quality):
    sched=copy.deepcopy(warm_sched)
    rows=[]; accepted=[]

    start_utc=M.EVAL_START_KST.astimezone(M.UTC)
    end_utc=eval_end_kst.astimezone(M.UTC)
    eval_setups=[s for s in setups if start_utc <= s["entry"] <= end_utc]

    for n,st in enumerate(eval_setups,1):
        sid=st["setup_id"]
        rm=rmeta[sid]
        qm=qmeta.get(sid)
        ef=U.entry_filter(st,controls)
        row=M.base_row(st,ef,rm,qm,name)

        if not ef["pass"]:
            rows.append(row)
            continue

        if use_market and rm["risk"]:
            row["block_reason"]="V25_MARKET_GUARD_2H8_ABS4H04"
            rows.append(row)
            continue

        if use_quality and qm and qm["v22_quality_or"]:
            row["block_reason"]="V22_QUALITY_OR"
            rows.append(row)
            continue

        ok,why=sched.can_open(st["entry"],st["symbol"])
        if not ok:
            row["block_reason"]=why
            rows.append(row)
            continue

        sim=M.get_sim(st,sim_cache)
        M.attach_sim(row,sim)
        sched.add(
            st["entry"],st["symbol"],sim,
            sim.result in ("STOP","LATE_FAILURE_EXIT")
        )
        rows.append(row)
        accepted.append(row)

        if n%25==0 or n==len(eval_setups):
            print(f"[{name}] {n}/{len(eval_setups)} accepted={len(accepted)}",flush=True)

    return rows,accepted

def counts(acc):
    c=Counter(str(r["result"]) for r in acc)
    return {
        "TP":c.get("TP20_FULL",0),
        "STOP":c.get("STOP",0),
        "PP12":c.get("PROFIT_PROTECT_EXIT",0),
        "LATE":c.get("LATE_FAILURE_EXIT",0),
        "TIME":c.get("TIME_EXIT",0),
        "OTHER":sum(v for k,v in c.items() if k not in
                    ("TP20_FULL","STOP","PROFIT_PROTECT_EXIT","LATE_FAILURE_EXIT","TIME_EXIT")),
    }

def net(acc):
    return sum(float(r["net_pct"]) for r in acc)

def main():
    print("=== FORWARD 4-WAY REPLAY ===",flush=True)

    df,data_first,data_end,eval_end=M.load_scan_window()
    print("source =",data_first.isoformat(),"~",data_end.isoformat(),flush=True)
    print("evaluation =",M.EVAL_START_KST.isoformat(),"~",eval_end.isoformat(),flush=True)

    if eval_end <= M.EVAL_START_KST:
        raise SystemExit("not enough data")

    qmeta=M.load_exact_watch_features(df)

    M.configure_unified(eval_end)
    print("[1/4] market series",flush=True)
    U.load_market_series()

    print("[2/4] V25 setups + controls",flush=True)
    setups=U.load_setups()
    controls=U.load_control_proxy()

    print("[3/4] causal V25 market-risk tags",flush=True)
    rmeta=M.build_risk_meta(setups)

    print("[4/4] common warm state",flush=True)
    sim_cache={}
    warm=M.warm_base(setups,controls,sim_cache)

    all_rows={}
    all_acc={}

    for name,use_market,use_quality in SCENARIOS:
        rows,acc=run_scenario(
            name,setups,controls,rmeta,qmeta,warm,sim_cache,eval_end,
            use_market,use_quality
        )
        all_rows[name]=rows
        all_acc[name]=acc
        write_csv(ROOT/f"FORWARD_0924_0925_4WAY_{name}_TRADES.csv",rows)

    base_net=net(all_acc["V25_BASE"])
    summary=[]
    for name,use_market,use_quality in SCENARIOS:
        acc=all_acc[name]
        cc=counts(acc)
        sm={
            "scenario":name,
            "market_guard":int(use_market),
            "v22_quality":int(use_quality),
            "entries":len(acc),
            "net_pct":round(net(acc),6),
            "delta_vs_v25_base":round(net(acc)-base_net,6),
            **cc,
            "exact_watch_coverage":sum(int(r.get("exact_watch",0)) for r in acc),
        }
        summary.append(sm)

    # Daily 4-way
    dates=sorted({
        str(r["entry_time_kst"])[:10]
        for acc in all_acc.values() for r in acc
    })
    daily=[]
    for d in dates:
        row={"date":d}
        for name,_,_ in SCENARIOS:
            z=[r for r in all_acc[name] if str(r["entry_time_kst"]).startswith(d)]
            row[f"{name}_entries"]=len(z)
            row[f"{name}_net"]=round(net(z),6)
        daily.append(row)

    # Direct rule blocks: not portfolio displacement, just candidates whose own rule fired
    block_rows=[]
    for name,use_market,use_quality in SCENARIOS:
        rows=all_rows[name]
        for r in rows:
            reason=str(r.get("block_reason") or "")
            if reason not in ("V25_MARKET_GUARD_2H8_ABS4H04","V22_QUALITY_OR"):
                continue
            block_rows.append({
                "scenario":name,
                "setup_id":r.get("setup_id"),
                "symbol":r.get("symbol"),
                "entry_time_kst":r.get("entry_time_kst"),
                "block_reason":reason,
                "risk_regime":r.get("risk_regime"),
                "v25_2h_count":r.get("v25_2h_count"),
                "abs4h_avg":r.get("abs4h_avg"),
                "overext":r.get("overext"),
                "weak_reaccel":r.get("weak_reaccel"),
                "p_v2_score":r.get("p_v2_score"),
                "ema9_ema20_gap_pct":r.get("ema9_ema20_gap_pct"),
                "rebound_from_low_pct":r.get("rebound_from_low_pct"),
                "rsi_delta":r.get("rsi_delta"),
                "btc15":r.get("btc15"),
            })

    write_csv(OUT_SUM,summary)
    write_csv(OUT_DAILY,daily)
    write_csv(OUT_BLOCKS,block_rows)

    lines=[
        "FORWARD 9/24 20:43 -> 9/25 FOUR-WAY COMPARISON",
        f"source={data_first.isoformat()} ~ {data_end.isoformat()}",
        f"evaluation={M.EVAL_START_KST.isoformat()} ~ {eval_end.isoformat()}",
        "",
        "A V25_BASE = base V25 stack; no NEW market guard; no V22 quality",
        "B V25_PLUS_MARKET = A + V25 2h>=8 & BTC/ETH abs4h avg>=0.40",
        "C V25_PLUS_V22Q = A + OVEREXT/WEAK_REACCEL",
        "D V25_PLUS_MARKET_PLUS_V22Q = A + both",
        "",
    ]
    for s in summary:
        lines.append(
            f"{s['scenario']}: entries={s['entries']} net={s['net_pct']:+.6f} "
            f"delta_vs_base={s['delta_vs_v25_base']:+.6f} "
            f"TP={s['TP']} STOP={s['STOP']} PP12={s['PP12']} "
            f"LATE={s['LATE']} TIME={s['TIME']}"
        )

    lines += ["","DAILY"]
    for r in daily:
        lines.append(
            f"{r['date']}: "
            + " | ".join(
                f"{name} {r[f'{name}_entries']}tr {r[f'{name}_net']:+.6f}"
                for name,_,_ in SCENARIOS
            )
        )

    OUT_TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")

    with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
        for p in [OUT_SUM,OUT_DAILY,OUT_BLOCKS,OUT_TXT]:
            z.write(p,arcname=p.name)
        for name,_,_ in SCENARIOS:
            p=ROOT/f"FORWARD_0924_0925_4WAY_{name}_TRADES.csv"
            z.write(p,arcname=p.name)

    print()
    print("\n".join(lines))
    print()
    print("DONE:",OUT_ZIP,flush=True)

if __name__=="__main__":
    main()
