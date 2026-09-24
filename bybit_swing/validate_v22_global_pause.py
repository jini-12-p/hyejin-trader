#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GLOBAL V22 MARKET PAUSE — exact portfolio reschedule validation

Current V25 risk guard remains ON:
  rolling 2h V25 confirmed >= 8
  AND current mean(abs(BTC4h),abs(ETH4h)) >= 0.40%

Global V22 pause trigger (frozen candidate):
  rolling 2h V22 candidates >= 20
  AND current mean(abs(BTC4h),abs(ETH4h)) >= 0.30%

Unlike the prior V22 per-setup block, this is a GLOBAL MARKET PAUSE:
when active, ALL otherwise-eligible new P entries are blocked, regardless of
which V22 setup they originated from. Existing positions are managed normally.

Pause variants:
  CUR_V25_ONLY   = current V25 guard only
  V22_PAUSE_0    = current V25 guard + pause only while V22 global condition active
  V22_PAUSE_15   = + 15m hold after last active V22-risk minute
  V22_PAUSE_30
  V22_PAUSE_45
  V22_PAUSE_60

Causal minute state:
- V22 density uses research_pv25_setups.first_seen_at in trailing 2 hours
- market uses last completed 1m BTC/ETH bar via unified market_at(t)
- no future data

Validation:
1) HIST full exact reschedule: 2026-09-01~09-22
   splits 09/01~17 and 09/18~22
2) FRESH OOS exact reschedule:
   common BASE warm-up 2026-09-22 08:00~14:18 KST
   evaluate 2026-09-22 14:18:34~2026-09-23 19:10:43 KST
   (last 3h excluded so each entry has full outcome horizon)

Outputs:
- period summary
- daily results
- marginal effect vs current V25 guard
- all trade rows
- pause-time statistics

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
from bisect import bisect_right
from collections import deque
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
HIST_END   = RealDateTime(2026,9,23,0,0,0,tzinfo=KST)  # exclusive

FRESH_WARM  = RealDateTime(2026,9,22,8,0,0,tzinfo=KST)
FRESH_START = RealDateTime(2026,9,22,14,18,34,tzinfo=KST)
FRESH_END   = RealDateTime(2026,9,23,19,10,43,tzinfo=KST)

V25_N = 8
V25_MKT = 0.40
V22_N = 20
V22_MKT = 0.30

PAUSE_HOLDS = {
    "CUR_V25_ONLY": None,
    "V22_PAUSE_0": 0,
    "V22_PAUSE_15": 15,
    "V22_PAUSE_30": 30,
    "V22_PAUSE_45": 45,
    "V22_PAUSE_60": 60,
}

OUT_SUM = ROOT / "V22_GLOBAL_PAUSE_SUMMARY.csv"
OUT_DAY = ROOT / "V22_GLOBAL_PAUSE_DAILY.csv"
OUT_INC = ROOT / "V22_GLOBAL_PAUSE_INCREMENTAL.csv"
OUT_PAUSE = ROOT / "V22_GLOBAL_PAUSE_STATE.csv"
OUT_TRADES = ROOT / "V22_GLOBAL_PAUSE_TRADES.csv"
OUT_TXT = ROOT / "V22_GLOBAL_PAUSE_SUMMARY.txt"
OUT_ZIP = ROOT / "V22_GLOBAL_PAUSE_RESULTS.zip"

SCRIPT_VERSION = "V22_GLOBAL_PAUSE_v1_20260924"


def load_module(name,path):
    spec=importlib.util.spec_from_file_location(name,str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    m=importlib.util.module_from_spec(spec)
    sys.modules[name]=m
    spec.loader.exec_module(m)
    return m

U=load_module("U_V22_GLOBAL_PAUSE",U_PATH)

def fv(v,default=None):
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

def floor_min(d):
    return d.astimezone(UTC).replace(second=0,microsecond=0)

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

def load_v22_times(start_utc,end_utc):
    con=sqlite3.connect(DB); con.row_factory=sqlite3.Row
    rr=con.execute("""
        SELECT first_seen_at
        FROM research_pv25_setups
        ORDER BY first_seen_at
    """).fetchall()
    con.close()
    out=[]
    for r in rr:
        d=dt(r["first_seen_at"])
        if d and start_utc-timedelta(hours=2) <= d < end_utc:
            out.append(d)
    return sorted(out)

def build_pause_state(start_utc,end_utc,v22_times):
    """
    Minute-level raw V22 risk state and hold variants.
    Returns state dict keyed by UTC minute.
    """
    # rolling queue preloaded naturally because v22_times includes 2h before start
    idx=0
    q=deque()
    n=len(v22_times)
    # load items <= start minute first
    t=floor_min(start_utc)
    while idx<n and v22_times[idx] <= t:
        q.append(v22_times[idx]); idx+=1
    while q and q[0] < t-timedelta(hours=2):
        q.popleft()

    state={}
    last_raw_true=None
    while t <= floor_min(end_utc):
        # include any V22 first_seen in (previous minute, current minute]
        while idx<n and v22_times[idx] <= t:
            q.append(v22_times[idx]); idx+=1
        while q and q[0] < t-timedelta(hours=2):
            q.popleft()

        ma=U.market_at(t)
        b4=fv(ma.get("btc_4h_change_pct"))
        e4=fv(ma.get("eth_4h_change_pct"))
        avg=None if b4 is None or e4 is None else (abs(b4)+abs(e4))/2.0
        raw=bool(len(q)>=V22_N and avg is not None and avg>=V22_MKT)
        if raw:
            last_raw_true=t

        rec={
            "time_utc":t.isoformat(),
            "time_kst":kst(t),
            "v22_2h":len(q),
            "abs4h_avg":avg,
            "raw":int(raw),
        }
        for name,hold in PAUSE_HOLDS.items():
            if hold is None:
                rec[name]=0
            elif last_raw_true is None:
                rec[name]=0
            else:
                rec[name]=int((t-last_raw_true).total_seconds()/60.0 <= hold)
                # For hold 0, this is true only on raw-true minutes because last_raw_true=t.
        state[t]=rec
        t += timedelta(minutes=1)
    return state

def pause_at(state,t,name):
    if name=="CUR_V25_ONLY": return False
    m=floor_min(t)
    rec=state.get(m)
    return bool(rec and int(rec.get(name,0))==1)

def build_v25_meta(setups):
    q=deque()
    out={}
    for st in setups:
        t=st["entry"]
        while q and q[0] < t-timedelta(hours=2):
            q.popleft()
        q.append(t)
        d=st["details"]
        U.fill_missing_market(d,t)
        b4=U.first_f(d,"btc_4h_change_pct","btc_4h")
        e4=U.first_f(d,"eth_4h_change_pct","eth_4h")
        avg=None if b4 is None or e4 is None else (abs(b4)+abs(e4))/2.0
        out[st["setup_id"]]={
            "v25_2h":len(q),
            "v25_abs4h":avg,
            "v25_guard":bool(len(q)>=V25_N and avg is not None and avg>=V25_MKT),
        }
    return out

def load_cache(path_name,eval_start=None):
    p=ROOT/path_name
    out={}
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
        derr=str(r.get("data_error") or "")
        if derr.lower()=="nan": derr=""
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
            data_error=derr,
        )
    return out

def getsim(st,cache):
    sid=st["setup_id"]
    if sid not in cache:
        cache[sid]=U.simulate_base(st)
    return cache[sid]

def warm_base(setups,controls,until_utc,cache):
    sched=U.Scheduler()
    for st in setups:
        if st["entry"]>=until_utc: break
        ef=U.entry_filter(st,controls)
        if not ef["pass"]: continue
        ok,_=sched.can_open(st["entry"],st["symbol"])
        if not ok: continue
        sim=getsim(st,cache)
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
    return sched

def run(name,setups,controls,v25meta,pause_state,cache,start_utc=None,end_utc=None,warm=None):
    sched=copy.deepcopy(warm) if warm is not None else U.Scheduler()
    rows=[]
    for st in setups:
        if start_utc and st["entry"]<start_utc: continue
        if end_utc and st["entry"]>end_utc: continue

        ef=U.entry_filter(st,controls)
        vm=v25meta.get(st["setup_id"],{})
        pactive=pause_at(pause_state,st["entry"],name)

        row={
            "scenario":name,
            "setup_id":st["setup_id"],
            "symbol":st["symbol"],
            "entry_time_kst":kst(st["entry"]),
            "v25_2h":vm.get("v25_2h"),
            "v25_abs4h":vm.get("v25_abs4h"),
            "v25_guard":int(bool(vm.get("v25_guard"))),
            "v22_global_pause":int(bool(pactive)),
            "accepted":0,
            "block_reason":ef["reason"],
            "result":"",
            "net_pct":"",
            "exit_time_kst":"",
            "data_error":"",
        }

        if not ef["pass"]:
            rows.append(row); continue

        # Current V25 guard ALWAYS remains on for every tested scenario.
        if bool(vm.get("v25_guard")):
            row["block_reason"]="CUR_V25_GUARD"
            rows.append(row); continue

        if pactive:
            row["block_reason"]=name
            rows.append(row); continue

        ok,why=sched.can_open(st["entry"],st["symbol"])
        if not ok:
            row["block_reason"]=why
            rows.append(row); continue

        sim=getsim(st,cache)
        row.update({
            "accepted":1,
            "block_reason":"",
            "result":sim.result,
            "net_pct":round(sim.net_pct,6),
            "exit_time_kst":kst(sim.exit_time),
            "data_error":sim.data_error,
        })
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        rows.append(row)
    return rows

def acc(rows,lo=None,hi=None):
    out=[]
    for r in rows:
        if int(r.get("accepted") or 0)!=1: continue
        d=str(r["entry_time_kst"])[:10]
        if lo and d<lo: continue
        if hi and d>hi: continue
        out.append(r)
    return out

def stat(dataset,period,name,rows,base_rows,lo=None,hi=None):
    a=acc(rows,lo,hi); b=acc(base_rows,lo,hi)
    an=sum(float(r["net_pct"]) for r in a); bn=sum(float(r["net_pct"]) for r in b)
    aids={r["setup_id"] for r in a}; bids={r["setup_id"] for r in b}
    amap={r["setup_id"]:r for r in a}; bmap={r["setup_id"]:r for r in b}
    new=[amap[x] for x in aids-bids]
    rem=[bmap[x] for x in bids-aids]
    return {
        "dataset":dataset,"period":period,"scenario":name,
        "entries":len(a),"net_pct":round(an,6),
        "current_v25_entries":len(b),"current_v25_net":round(bn,6),
        "delta_vs_current_v25":round(an-bn,6),
        "tp":sum(r["result"]=="TP20_FULL" for r in a),
        "stop":sum(r["result"]=="STOP" for r in a),
        "pp12":sum(r["result"]=="PROFIT_PROTECT_EXIT" for r in a),
        "late":sum(r["result"]=="LATE_FAILURE_EXIT" for r in a),
        "new_entries":len(new),"new_net":round(sum(float(r["net_pct"]) for r in new),6),
        "removed_current":len(rem),"removed_current_net":round(sum(float(r["net_pct"]) for r in rem),6),
        "errors":sum(bool(r.get("data_error")) for r in a),
    }

def day_rows(dataset,name,rows,base_rows):
    dates=sorted(set(str(r["entry_time_kst"])[:10] for r in rows+base_rows))
    out=[]
    for d in dates:
        a=acc(rows,d,d); b=acc(base_rows,d,d)
        an=sum(float(r["net_pct"]) for r in a); bn=sum(float(r["net_pct"]) for r in b)
        out.append({
            "dataset":dataset,"date":d,"scenario":name,
            "entries":len(a),"net_pct":round(an,6),
            "cur_entries":len(b),"cur_net":round(bn,6),
            "delta_vs_cur":round(an-bn,6),
        })
    return out

def incremental(dataset,name,rows,cur_rows):
    a=acc(rows); b=acc(cur_rows)
    aids={r["setup_id"] for r in a}; bids={r["setup_id"] for r in b}
    amap={r["setup_id"]:r for r in a}; bmap={r["setup_id"]:r for r in b}
    new=[amap[x] for x in aids-bids]
    rem=[bmap[x] for x in bids-aids]
    # Direct pause removals are rows that scenario itself blocks as V22 pause in its all-row output.
    paused_ids={str(r["setup_id"]) for r in rows if str(r.get("block_reason"))==name}
    direct=[bmap[x] for x in (bids-aids) if x in paused_ids]
    displaced=[bmap[x] for x in (bids-aids) if x not in paused_ids]
    return {
        "dataset":dataset,"scenario":name,
        "cur_entries":len(b),"cur_net":round(sum(float(r["net_pct"]) for r in b),6),
        "scenario_entries":len(a),"scenario_net":round(sum(float(r["net_pct"]) for r in a),6),
        "delta_vs_cur":round(sum(float(r["net_pct"]) for r in a)-sum(float(r["net_pct"]) for r in b),6),
        "direct_pause_removed":len(direct),
        "direct_pause_removed_net":round(sum(float(r["net_pct"]) for r in direct),6),
        "displaced_current":len(displaced),
        "displaced_current_net":round(sum(float(r["net_pct"]) for r in displaced),6),
        "new_after_pause":len(new),
        "new_after_pause_net":round(sum(float(r["net_pct"]) for r in new),6),
    }

def pause_stats(dataset,state,start_utc,end_utc):
    z=[r for t,r in state.items() if start_utc <= t <= end_utc]
    out=[]
    total=len(z)
    for name,hold in PAUSE_HOLDS.items():
        if hold is None: continue
        mins=sum(int(r.get(name,0)) for r in z)
        out.append({
            "dataset":dataset,"scenario":name,"hold_min":hold,
            "window_minutes":total,"pause_minutes":mins,
            "pause_time_pct":round(100*mins/total,3) if total else 0,
        })
    return out

all_sum=[]; all_day=[]; all_inc=[]; all_state=[]; all_trades=[]

# HIST
print("=== HIST ===",flush=True)
configure_range(HIST_START,HIST_END,".v22_global_pause_hist_cache")
hs=U.load_setups()
hc=U.load_control_proxy()
hv22=load_v22_times(HIST_START.astimezone(UTC),HIST_END.astimezone(UTC))
hstate=build_pause_state(HIST_START.astimezone(UTC),HIST_END.astimezone(UTC)-timedelta(minutes=1),hv22)
hv25=build_v25_meta(hs)
hcache=load_cache("UNIFIED_CURRENT_0901_0922_TRADES.csv")

hruns={}
for name in PAUSE_HOLDS:
    print("HIST",name,flush=True)
    hruns[name]=run(name,hs,hc,hv25,hstate,hcache)
    for r in hruns[name]:
        rr=dict(r); rr["dataset"]="HIST"; all_trades.append(rr)

hcur=hruns["CUR_V25_ONLY"]
for name in PAUSE_HOLDS:
    all_sum.append(stat("HIST","PRE_0901_0917",name,hruns[name],hcur,"2026-09-01","2026-09-17"))
    all_sum.append(stat("HIST","POST_0918_0922",name,hruns[name],hcur,"2026-09-18","2026-09-22"))
    all_sum.append(stat("HIST","ALL_0901_0922",name,hruns[name],hcur))
    if name!="CUR_V25_ONLY":
        all_day += day_rows("HIST",name,hruns[name],hcur)
        all_inc.append(incremental("HIST_ALL",name,hruns[name],hcur))
all_state += pause_stats("HIST",hstate,HIST_START.astimezone(UTC),HIST_END.astimezone(UTC)-timedelta(minutes=1))

# FRESH
print("=== FRESH ===",flush=True)
configure_range(FRESH_WARM,FRESH_END+timedelta(seconds=1),".v22_global_pause_fresh_cache")
fs=U.load_setups()
fc=U.load_control_proxy()
fv22=load_v22_times(FRESH_WARM.astimezone(UTC),FRESH_END.astimezone(UTC)+timedelta(minutes=1))
fstate=build_pause_state(FRESH_WARM.astimezone(UTC),FRESH_END.astimezone(UTC),fv22)
fv25=build_v25_meta(fs)
fcache=load_cache("FRESH_BASE_TRADES.csv",FRESH_START)
warm=warm_base(fs,fc,FRESH_START.astimezone(UTC),fcache)

fruns={}
for name in PAUSE_HOLDS:
    print("FRESH",name,flush=True)
    fruns[name]=run(name,fs,fc,fv25,fstate,fcache,
                    FRESH_START.astimezone(UTC),FRESH_END.astimezone(UTC),warm)
    for r in fruns[name]:
        rr=dict(r); rr["dataset"]="FRESH"; all_trades.append(rr)

fcur=fruns["CUR_V25_ONLY"]
for name in PAUSE_HOLDS:
    all_sum.append(stat("FRESH","ALL",name,fruns[name],fcur))
    if name!="CUR_V25_ONLY":
        all_day += day_rows("FRESH",name,fruns[name],fcur)
        all_inc.append(incremental("FRESH",name,fruns[name],fcur))
all_state += pause_stats("FRESH",fstate,FRESH_START.astimezone(UTC),FRESH_END.astimezone(UTC))

write_csv(OUT_SUM,all_sum)
write_csv(OUT_DAY,all_day)
write_csv(OUT_INC,all_inc)
write_csv(OUT_PAUSE,all_state)
write_csv(OUT_TRADES,all_trades)

def pick(dataset,period,scenario):
    return next(r for r in all_sum if r["dataset"]==dataset and r["period"]==period and r["scenario"]==scenario)

lines=[
    "GLOBAL V22 MARKET PAUSE — EXACT RESCHEDULE",
    f"script={SCRIPT_VERSION}",
    "",
    "[FROZEN RULES]",
    f"Current V25 guard: V25 2h>={V25_N} AND abs4h>={V25_MKT:.2f}%",
    f"Global V22 trigger: V22 2h>={V22_N} AND abs4h>={V22_MKT:.2f}%",
    "Existing positions continue normally; only NEW entries pause.",
    "",
    "[PERIOD RESULTS vs CURRENT V25 GUARD]",
]
for r in all_sum:
    lines.append(
        f"{r['dataset']} {r['period']} {r['scenario']}: "
        f"entries={r['entries']} NET={r['net_pct']:.6f} "
        f"delta_vs_CUR={r['delta_vs_current_v25']:+.6f} "
        f"new={r['new_entries']}({r['new_net']:+.6f}) "
        f"removed={r['removed_current']}({r['removed_current_net']:+.6f}) "
        f"TP={r['tp']} STOP={r['stop']} PP12={r['pp12']} LATE={r['late']} err={r['errors']}"
    )

lines += ["","[ROBUSTNESS BY HOLD]"]
rank=[]
for name,hold in PAUSE_HOLDS.items():
    if hold is None: continue
    pre=pick("HIST","PRE_0901_0917",name)
    post=pick("HIST","POST_0918_0922",name)
    fresh=pick("FRESH","ALL",name)
    vals=(pre["delta_vs_current_v25"],post["delta_vs_current_v25"],fresh["delta_vs_current_v25"])
    rank.append((name,hold,vals,min(vals),sum(vals)))
rank.sort(key=lambda x:(x[3]>0,x[3],x[4]),reverse=True)
for name,hold,vals,mn,sm in rank:
    lines.append(
        f"{name}: PRE={vals[0]:+.6f}, POST={vals[1]:+.6f}, FRESH={vals[2]:+.6f}, "
        f"all_positive={all(v>0 for v in vals)}, worst_delta={mn:+.6f}, sum={sm:+.6f}"
    )

lines += ["","[PAUSE TIME]"]
for r in all_state:
    lines.append(
        f"{r['dataset']} {r['scenario']}: pause={r['pause_minutes']}m / {r['window_minutes']}m "
        f"({r['pause_time_pct']}%)"
    )

lines += ["","[MARGINAL STRUCTURE]"]
for r in all_inc:
    lines.append(
        f"{r['dataset']} {r['scenario']}: CUR={r['cur_net']:.6f} -> {r['scenario_net']:.6f} "
        f"delta={r['delta_vs_cur']:+.6f}; direct_pause_removed={r['direct_pause_removed']} "
        f"net_was={r['direct_pause_removed_net']:+.6f}; displaced={r['displaced_current']} "
        f"net_was={r['displaced_current_net']:+.6f}; new_after_pause={r['new_after_pause']} "
        f"new_net={r['new_after_pause_net']:+.6f}"
    )

OUT_TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")
print("\n".join(lines),flush=True)

with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in [OUT_SUM,OUT_DAY,OUT_INC,OUT_PAUSE,OUT_TRADES,OUT_TXT]:
        z.write(p,arcname=p.name)
print("DONE:",OUT_ZIP,flush=True)
