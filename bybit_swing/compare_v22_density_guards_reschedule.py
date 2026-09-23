#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V22 DENSITY GUARD — exact portfolio reschedule comparison

Compares current exact P BASE against:
  CUR_V25_8_M04 : current discovered guard
      rolling 2h V25 confirmed count >= 8
      AND V25-entry mean(abs(BTC4h), abs(ETH4h)) >= 0.40%

  V22_15_M04:
      rolling 2h V22 candidate count at candidate first_seen >= 15
      AND candidate-time mean(abs(BTC4h), abs(ETH4h)) >= 0.40%

  V22_18_M04
  V22_20_M04
  V22_20_M03

V22 count is causal and uses research_pv25_setups first_seen_at:
a row is created when P_V22 becomes a V25 WATCH.

Candidate-time BTC/ETH 4h telemetry is recomputed causally from completed 1m bars
using unified_current_0901_0922.market_at(first_seen_at).

Two tests:
1) HIST 2026-09-01~09-22 full reschedule
   + period splits 09/01~17 and 09/18~22
2) FRESH OOS
   Common BASE warm-up 2026-09-22 08:00~14:18 KST, then evaluate
   2026-09-22 14:18:34~2026-09-23 19:10:43 KST
   (late 3h excluded so every entry has full outcome horizon)

For each guard:
- direct BASE trades removed
- NEW trades admitted because slots/cooldowns changed
- BASE non-risk trades displaced by reshuffle
- exact final NET and daily NET

No DB writes. No orders.
"""
from __future__ import annotations

import copy
import csv
import importlib.util
import math
import sqlite3
import sys
import zipfile
from collections import Counter, deque
from datetime import datetime as RealDateTime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path("/root/hyejin-trader/bybit_swing")
U_PATH = ROOT / "unified_current_0901_0922.py"
DB = ROOT / "bybit_swing_bot.db"

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

HIST_START = RealDateTime(2026,9,1,0,0,0,tzinfo=KST)
HIST_END = RealDateTime(2026,9,23,0,0,0,tzinfo=KST)  # exclusive

FRESH_WARM = RealDateTime(2026,9,22,8,0,0,tzinfo=KST)
FRESH_START = RealDateTime(2026,9,22,14,18,34,tzinfo=KST)
FRESH_END = RealDateTime(2026,9,23,19,10,43,tzinfo=KST)

GUARDS = {
    "BASE": None,
    "CUR_V25_8_M04": ("V25", 8, 0.40),
    "V22_15_M04": ("V22", 15, 0.40),
    "V22_18_M04": ("V22", 18, 0.40),
    "V22_20_M04": ("V22", 20, 0.40),
    "V22_20_M03": ("V22", 20, 0.30),
}

OUT_SUMMARY_CSV = ROOT / "V22_GUARD_RESCHEDULE_SUMMARY.csv"
OUT_DAILY = ROOT / "V22_GUARD_RESCHEDULE_DAILY.csv"
OUT_SUBS = ROOT / "V22_GUARD_RESCHEDULE_SUBSTITUTIONS.csv"
OUT_SUMMARY_TXT = ROOT / "V22_GUARD_RESCHEDULE_SUMMARY.txt"
OUT_ZIP = ROOT / "V22_GUARD_RESCHEDULE_RESULTS.zip"

SCRIPT_VERSION = "V22_GUARD_RESCHEDULE_v1_20260923"


def load_module(name, path):
    spec=importlib.util.spec_from_file_location(name,str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    m=importlib.util.module_from_spec(spec)
    sys.modules[name]=m
    spec.loader.exec_module(m)
    return m

U=load_module("U_V22_GUARD",U_PATH)

def fv(v, default=None):
    try:
        if v is None or str(v).strip()=="":
            return default
        x=float(v)
        if math.isnan(x): return default
        return x
    except Exception:
        return default

def dt(v):
    if v in (None,""): return None
    try:
        d=RealDateTime.fromisoformat(str(v).replace("Z","+00:00"))
        if d.tzinfo is None: d=d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except Exception:
        return None

def kst(d):
    return d.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S") if d else ""

def write_csv(path,rows):
    if not rows:
        path.write_text("",encoding="utf-8-sig"); return
    keys=[]; seen=set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with path.open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=keys,extrasaction="ignore")
        w.writeheader(); w.writerows(rows)

def configure_range(start_kst,end_kst,cache_name):
    U.START_KST=start_kst
    U.END_KST=end_kst
    U.START_UTC=start_kst.astimezone(UTC)
    U.END_UTC=end_kst.astimezone(UTC)
    U.EXPECTED_V25=-1
    U.CACHE_DIR=ROOT/cache_name
    U.CACHE_DIR.mkdir(parents=True,exist_ok=True)
    U.KC=U.KlineCache()
    U._market_1m={}
    U._market_recompute_count=0
    U.load_market_series()

def load_v22_rows(start_utc,end_utc):
    con=sqlite3.connect(DB); con.row_factory=sqlite3.Row
    rows=con.execute("""
      SELECT setup_id,symbol,first_seen_at,status,confirmed_at
      FROM research_pv25_setups
      ORDER BY first_seen_at
    """).fetchall()
    con.close()
    out=[]
    for r in rows:
        first=dt(r["first_seen_at"])
        if not first or not (start_utc <= first < end_utc):
            continue
        out.append({
            "setup_id":str(r["setup_id"]),
            "symbol":str(r["symbol"]),
            "first":first,
            "status":str(r["status"] or ""),
            "confirmed":dt(r["confirmed_at"]),
        })
    return out

def build_guard_meta(v22rows,setups):
    # V22 rolling 2h density + candidate-time market
    v22_meta={}
    q=deque()
    for r in v22rows:
        t=r["first"]
        while q and q[0] < t-timedelta(hours=2):
            q.popleft()
        q.append(t)
        ma=U.market_at(t)
        b4=fv(ma.get("btc_4h_change_pct"))
        e4=fv(ma.get("eth_4h_change_pct"))
        avg=None if b4 is None or e4 is None else (abs(b4)+abs(e4))/2
        v22_meta[r["setup_id"]]={
            "v22_2h":len(q),"v22_btc4":b4,"v22_eth4":e4,"v22_abs4h":avg,
            "v22_first_kst":kst(t),
        }

    # V25 rolling 2h density + entry-time market
    v25_meta={}
    q=deque()
    for st in setups:
        t=st["entry"]
        while q and q[0] < t-timedelta(hours=2):
            q.popleft()
        q.append(t)
        d=st["details"]
        U.fill_missing_market(d,t)
        b4=U.first_f(d,"btc_4h_change_pct","btc_4h")
        e4=U.first_f(d,"eth_4h_change_pct","eth_4h")
        avg=None if b4 is None or e4 is None else (abs(b4)+abs(e4))/2
        v25_meta[st["setup_id"]]={
            "v25_2h":len(q),"v25_btc4":b4,"v25_eth4":e4,"v25_abs4h":avg,
        }

    meta={}
    for st in setups:
        sid=st["setup_id"]
        meta[sid]={**v22_meta.get(sid,{}),**v25_meta.get(sid,{})}
    return meta

def blocked_by_guard(name,meta):
    rule=GUARDS[name]
    if rule is None: return False
    typ,nmin,mmin=rule
    if typ=="V25":
        n=meta.get("v25_2h")
        m=meta.get("v25_abs4h")
    else:
        n=meta.get("v22_2h")
        m=meta.get("v22_abs4h")
    return bool(n is not None and m is not None and n>=nmin and m>=mmin)

def load_result_cache(csv_path,eval_start=None):
    out={}
    p=ROOT/csv_path
    if not p.exists(): return out
    df=pd.read_csv(p)
    for r in df.to_dict("records"):
        if int(fv(r.get("accepted"),0) or 0)!=1: continue
        if eval_start is not None:
            t=str(r.get("entry_time_kst") or "")
            if t and t < eval_start.strftime("%Y-%m-%d %H:%M:%S"):
                continue
        et=dt(r.get("exit_ts_utc"))
        if et is None: continue
        ep=fv(r.get("entry_price"),0) or 0
        terminal=fv(r.get("terminal_price"),ep) or ep
        out[str(r["setup_id"])]=U.SimResult(
            result=str(r.get("result") or ""),
            exit_time=et,
            terminal_price=terminal,
            fills=[],
            gross_pct=fv(r.get("gross_pct"),0) or 0,
            fee_pct=fv(r.get("fee_pct"),0) or 0,
            net_pct=fv(r.get("net_pct"),0) or 0,
            mfe_pct=fv(r.get("mfe_pct"),0) or 0,
            mae_pct=fv(r.get("mae_pct"),0) or 0,
            stop_stage=str(r.get("stop_stage") or ""),
            detail="CACHED",
            data_error=str(r.get("data_error") or ""),
        )
    return out

def get_sim(st,cache):
    sid=st["setup_id"]
    if sid not in cache:
        cache[sid]=U.simulate_base(st)
    return cache[sid]

def warm_base(setups,controls,meta,until_utc,cache):
    sched=U.Scheduler()
    accepted=[]
    for st in setups:
        if st["entry"]>=until_utc: break
        ef=U.entry_filter(st,controls)
        if not ef["pass"]: continue
        ok,_=sched.can_open(st["entry"],st["symbol"])
        if not ok: continue
        sim=get_sim(st,cache)
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        accepted.append(st["setup_id"])
    return sched,accepted

def run_scenario(name,setups,controls,meta,cache,
                 eval_start_utc=None,eval_end_utc=None,warm_sched=None):
    sched=copy.deepcopy(warm_sched) if warm_sched is not None else U.Scheduler()
    rows=[]; acc=[]
    for i,st in enumerate(setups,1):
        if eval_start_utc and st["entry"]<eval_start_utc: continue
        if eval_end_utc and st["entry"]>eval_end_utc: continue
        ef=U.entry_filter(st,controls)
        gm=meta.get(st["setup_id"],{})
        row={
            "scenario":name,"setup_id":st["setup_id"],"symbol":st["symbol"],
            "entry_time_kst":kst(st["entry"]),
            "v22_2h":gm.get("v22_2h"),"v22_abs4h":gm.get("v22_abs4h"),
            "v25_2h":gm.get("v25_2h"),"v25_abs4h":gm.get("v25_abs4h"),
            "accepted":0,"block_reason":ef["reason"],"guard_block":0,
            "result":"","net_pct":"","exit_time_kst":"","data_error":"",
        }
        if not ef["pass"]:
            rows.append(row); continue
        if blocked_by_guard(name,gm):
            row["block_reason"]=name
            row["guard_block"]=1
            rows.append(row); continue
        ok,why=sched.can_open(st["entry"],st["symbol"])
        if not ok:
            row["block_reason"]=why
            rows.append(row); continue
        sim=get_sim(st,cache)
        row.update({
            "accepted":1,"block_reason":"","result":sim.result,
            "net_pct":round(sim.net_pct,6),"exit_time_kst":kst(sim.exit_time),
            "data_error":sim.data_error,
        })
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        rows.append(row); acc.append(row)
        if i%200==0:
            print(f"[{name}] {i}/{len(setups)} accepted={len(acc)}",flush=True)
    return rows,acc

def summarize_scenario(dataset,name,rows,base_rows,period_filter=None):
    def use(r):
        if int(r.get("accepted") or 0)!=1: return False
        if period_filter is None: return True
        d=str(r["entry_time_kst"])[:10]
        lo,hi=period_filter
        return lo<=d<=hi
    a=[r for r in rows if use(r)]
    b=[r for r in base_rows if use(r)]
    aids={r["setup_id"] for r in a}
    bids={r["setup_id"] for r in b}
    amap={r["setup_id"]:r for r in a}
    bmap={r["setup_id"]:r for r in b}
    new=[amap[x] for x in aids-bids]
    removed=[bmap[x] for x in bids-aids]
    direct=[r for r in removed if int(r.get("guard_block") or 0)==0]  # may be displaced
    net=sum(float(r["net_pct"]) for r in a)
    bnet=sum(float(r["net_pct"]) for r in b)
    return {
        "dataset":dataset,"period":("ALL" if period_filter is None else f"{period_filter[0]}_{period_filter[1]}"),
        "scenario":name,"entries":len(a),"net_pct":round(net,6),
        "base_entries":len(b),"base_net":round(bnet,6),"delta":round(net-bnet,6),
        "tp":sum(r["result"]=="TP20_FULL" for r in a),
        "stop":sum(r["result"]=="STOP" for r in a),
        "pp12":sum(r["result"]=="PROFIT_PROTECT_EXIT" for r in a),
        "late":sum(r["result"]=="LATE_FAILURE_EXIT" for r in a),
        "new_entries":len(new),"new_net":round(sum(float(r["net_pct"]) for r in new),6),
        "removed_base":len(removed),"removed_base_net":round(sum(float(r["net_pct"]) for r in removed),6),
        "data_errors":sum(bool(r.get("data_error")) for r in a),
    }

def daily_rows(dataset,scenario,rows,base_rows):
    dates=sorted(set(str(r["entry_time_kst"])[:10] for r in rows+base_rows))
    out=[]
    for d in dates:
        a=[r for r in rows if int(r.get("accepted") or 0)==1 and str(r["entry_time_kst"]).startswith(d)]
        b=[r for r in base_rows if int(r.get("accepted") or 0)==1 and str(r["entry_time_kst"]).startswith(d)]
        net=sum(float(r["net_pct"]) for r in a); bnet=sum(float(r["net_pct"]) for r in b)
        out.append({
            "dataset":dataset,"date":d,"scenario":scenario,
            "entries":len(a),"net_pct":round(net,6),
            "base_entries":len(b),"base_net":round(bnet,6),"delta":round(net-bnet,6),
        })
    return out

def substitution_rows(dataset,scenario,rows,base_rows):
    a=[r for r in rows if int(r.get("accepted") or 0)==1]
    b=[r for r in base_rows if int(r.get("accepted") or 0)==1]
    aids={r["setup_id"] for r in a}; bids={r["setup_id"] for r in b}
    amap={r["setup_id"]:r for r in a}; bmap={r["setup_id"]:r for r in b}
    out=[]
    for sid in sorted(aids-bids):
        r=amap[sid]
        out.append({"dataset":dataset,"scenario":scenario,"type":"NEW",
                    "setup_id":sid,"symbol":r["symbol"],"entry_time_kst":r["entry_time_kst"],
                    "result":r["result"],"net_pct":r["net_pct"]})
    for sid in sorted(bids-aids):
        r=bmap[sid]
        out.append({"dataset":dataset,"scenario":scenario,"type":"REMOVED_BASE",
                    "setup_id":sid,"symbol":r["symbol"],"entry_time_kst":r["entry_time_kst"],
                    "result":r["result"],"net_pct":r["net_pct"]})
    return out

all_summary=[]; all_daily=[]; all_subs=[]

# ---------- HIST ----------
print("=== HIST ===",flush=True)
configure_range(HIST_START,HIST_END,".v22_guard_hist_cache")
hist_setups=U.load_setups()
hist_controls=U.load_control_proxy()
hist_v22=load_v22_rows(HIST_START.astimezone(UTC),HIST_END.astimezone(UTC))
hist_meta=build_guard_meta(hist_v22,hist_setups)
hist_cache=load_result_cache("UNIFIED_CURRENT_0901_0922_TRADES.csv")

hist_runs={}
for name in GUARDS:
    print("HIST",name,flush=True)
    rows,acc=run_scenario(name,hist_setups,hist_controls,hist_meta,hist_cache)
    hist_runs[name]=rows

base_hist=hist_runs["BASE"]
for name,rows in hist_runs.items():
    for period in [None,("2026-09-01","2026-09-17"),("2026-09-18","2026-09-22")]:
        all_summary.append(summarize_scenario("HIST",name,rows,base_hist,period))
    if name!="BASE":
        all_daily += daily_rows("HIST",name,rows,base_hist)
        all_subs += substitution_rows("HIST",name,rows,base_hist)

# ---------- FRESH ----------
print("=== FRESH ===",flush=True)
configure_range(FRESH_WARM,FRESH_END+timedelta(seconds=1),".v22_guard_fresh_cache")
fresh_setups=U.load_setups()
fresh_controls=U.load_control_proxy()
fresh_v22=load_v22_rows(FRESH_WARM.astimezone(UTC),(FRESH_END+timedelta(seconds=1)).astimezone(UTC))
fresh_meta=build_guard_meta(fresh_v22,fresh_setups)
fresh_cache=load_result_cache("FRESH_BASE_TRADES.csv",FRESH_START)

warm_sched,_=warm_base(fresh_setups,fresh_controls,fresh_meta,FRESH_START.astimezone(UTC),fresh_cache)

fresh_runs={}
for name in GUARDS:
    print("FRESH",name,flush=True)
    rows,acc=run_scenario(
        name,fresh_setups,fresh_controls,fresh_meta,fresh_cache,
        eval_start_utc=FRESH_START.astimezone(UTC),
        eval_end_utc=FRESH_END.astimezone(UTC),
        warm_sched=warm_sched,
    )
    fresh_runs[name]=rows

base_fresh=fresh_runs["BASE"]
for name,rows in fresh_runs.items():
    all_summary.append(summarize_scenario("FRESH",name,rows,base_fresh,None))
    if name!="BASE":
        all_daily += daily_rows("FRESH",name,rows,base_fresh)
        all_subs += substitution_rows("FRESH",name,rows,base_fresh)

write_csv(OUT_SUMMARY_CSV,all_summary)
write_csv(OUT_DAILY,all_daily)
write_csv(OUT_SUBS,all_subs)

# Human summary
lines=[
    "V22 DENSITY GUARD EXACT RESCHEDULE",
    f"script={SCRIPT_VERSION}",
    "",
    "[GUARDS]",
]
for k,v in GUARDS.items():
    lines.append(f"{k} = {v}")
lines += ["","[RESULTS]"]
for r in all_summary:
    lines.append(
        f"{r['dataset']} {r['period']} {r['scenario']}: "
        f"entries={r['entries']} NET={r['net_pct']:.6f} "
        f"delta={r['delta']:+.6f} new={r['new_entries']}({r['new_net']:+.6f}) "
        f"removed={r['removed_base']}({r['removed_base_net']:+.6f}) "
        f"TP={r['tp']} STOP={r['stop']} PP12={r['pp12']} LATE={r['late']} err={r['data_errors']}"
    )

# Rank only candidate guards, requiring positive improvement in pre/post/fresh.
rank=[]
for name in GUARDS:
    if name=="BASE": continue
    def find(dataset,period):
        return next(r for r in all_summary if r["dataset"]==dataset and r["period"]==period and r["scenario"]==name)
    pre=find("HIST","2026-09-01_2026-09-17")
    post=find("HIST","2026-09-18_2026-09-22")
    fresh=find("FRESH","ALL")
    rank.append({
        "name":name,"pre":pre["delta"],"post":post["delta"],"fresh":fresh["delta"],
        "score":pre["delta"]+post["delta"]+fresh["delta"],
        "all_positive":pre["delta"]>0 and post["delta"]>0 and fresh["delta"]>0,
    })
rank.sort(key=lambda x:(x["all_positive"],x["score"]),reverse=True)
lines += ["","[ROBUSTNESS RANK]"]
for x in rank:
    lines.append(
        f"{x['name']}: pre={x['pre']:+.6f}, post={x['post']:+.6f}, fresh={x['fresh']:+.6f}, "
        f"all_positive={x['all_positive']}, sum={x['score']:+.6f}"
    )

OUT_SUMMARY_TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")
print("\n".join(lines),flush=True)

with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in [OUT_SUMMARY_CSV,OUT_DAILY,OUT_SUBS,OUT_SUMMARY_TXT]:
        z.write(p,arcname=p.name)
print("DONE:",OUT_ZIP,flush=True)
