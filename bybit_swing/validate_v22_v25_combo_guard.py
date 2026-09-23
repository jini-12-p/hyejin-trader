#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Combined risk guard validation — exact portfolio reschedule.

Compare:
  BASE
  CUR_V25        = V25 rolling2h >= 8 AND V25-entry mean(abs(BTC4h),abs(ETH4h)) >= 0.40%
  V22_ONLY       = V22 rolling2h >= 20 AND V22-candidate-time mean(abs(BTC4h),abs(ETH4h)) >= 0.30%
  OR_COMBO       = CUR_V25 OR V22_ONLY
  AND_COMBO      = CUR_V25 AND V22_ONLY

Periods:
  HIST_PRE  2026-09-01 ~ 09-17
  HIST_POST 2026-09-18 ~ 09-22
  FRESH OOS common BASE warmup from 2026-09-22 08:00 KST,
            evaluate 2026-09-22 14:18:34 ~ 2026-09-23 19:10:43 KST

Causal V22 source:
  research_pv25_setups.first_seen_at = P_V22 candidate -> V25 WATCH creation time.

Portfolio:
  current SAFE/RELAX/MKT100
  4 slots / rolling15m max2
  same-symbol 90m
  STOP/LATE 180m
  STOP pause 30m
  TP2.0 + PP12 + Final4 + V27-1
  fees identical to unified current BASE

Outputs quantify:
- OR vs current V25 guard incremental improvement
- additional V22-only blocks
- new replacement entries after reschedule
- displaced BASE/current-V25 entries
- daily and period P&L
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
HIST_END   = RealDateTime(2026,9,23,0,0,0,tzinfo=KST)

FRESH_WARM  = RealDateTime(2026,9,22,8,0,0,tzinfo=KST)
FRESH_START = RealDateTime(2026,9,22,14,18,34,tzinfo=KST)
FRESH_END   = RealDateTime(2026,9,23,19,10,43,tzinfo=KST)

SCENARIOS = ["BASE","CUR_V25","V22_ONLY","OR_COMBO","AND_COMBO"]

OUT_SUMMARY = ROOT / "V22_V25_COMBO_SUMMARY.csv"
OUT_DAILY = ROOT / "V22_V25_COMBO_DAILY.csv"
OUT_INCREMENTAL = ROOT / "V22_V25_COMBO_INCREMENTAL.csv"
OUT_TRADES = ROOT / "V22_V25_COMBO_TRADES.csv"
OUT_TXT = ROOT / "V22_V25_COMBO_SUMMARY.txt"
OUT_ZIP = ROOT / "V22_V25_COMBO_RESULTS.zip"

SCRIPT_VERSION = "V22_V25_COMBO_v1_20260924"

def load_module(name,path):
    spec=importlib.util.spec_from_file_location(name,str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    m=importlib.util.module_from_spec(spec)
    sys.modules[name]=m
    spec.loader.exec_module(m)
    return m

U=load_module("U_COMBO_GUARD",U_PATH)

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

def build_meta(v22rows,setups):
    v22={}
    q=deque()
    for r in v22rows:
        t=r["first"]
        while q and q[0] < t-timedelta(hours=2):
            q.popleft()
        q.append(t)
        m=U.market_at(t)
        b4=fv(m.get("btc_4h_change_pct"))
        e4=fv(m.get("eth_4h_change_pct"))
        avg=None if b4 is None or e4 is None else (abs(b4)+abs(e4))/2
        v22[r["setup_id"]]={
            "v22_2h":len(q),"v22_abs4h":avg,
            "v22_guard":bool(len(q)>=20 and avg is not None and avg>=0.30),
        }

    v25={}
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
        v25[st["setup_id"]]={
            "v25_2h":len(q),"v25_abs4h":avg,
            "v25_guard":bool(len(q)>=8 and avg is not None and avg>=0.40),
        }

    out={}
    for st in setups:
        sid=st["setup_id"]
        out[sid]={**v22.get(sid,{}),**v25.get(sid,{})}
        a=bool(out[sid].get("v25_guard"))
        b=bool(out[sid].get("v22_guard"))
        out[sid]["or_guard"]=a or b
        out[sid]["and_guard"]=a and b
        out[sid]["v22_only_incremental"]=b and not a
        out[sid]["v25_only_incremental"]=a and not b
    return out

def guard_hit(name,m):
    if name=="BASE": return False
    if name=="CUR_V25": return bool(m.get("v25_guard"))
    if name=="V22_ONLY": return bool(m.get("v22_guard"))
    if name=="OR_COMBO": return bool(m.get("or_guard"))
    if name=="AND_COMBO": return bool(m.get("and_guard"))
    return False

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
            data_error="" if str(r.get("data_error") or "").lower()=="nan" else str(r.get("data_error") or ""),
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

def run(name,setups,controls,meta,cache,start_utc=None,end_utc=None,warm=None):
    sched=copy.deepcopy(warm) if warm is not None else U.Scheduler()
    rows=[]
    for st in setups:
        if start_utc and st["entry"]<start_utc: continue
        if end_utc and st["entry"]>end_utc: continue

        ef=U.entry_filter(st,controls)
        m=meta.get(st["setup_id"],{})
        row={
            "scenario":name,"setup_id":st["setup_id"],"symbol":st["symbol"],
            "entry_time_kst":kst(st["entry"]),
            "v22_2h":m.get("v22_2h"),"v22_abs4h":m.get("v22_abs4h"),
            "v25_2h":m.get("v25_2h"),"v25_abs4h":m.get("v25_abs4h"),
            "v22_guard":int(bool(m.get("v22_guard"))),
            "v25_guard":int(bool(m.get("v25_guard"))),
            "v22_only_incremental":int(bool(m.get("v22_only_incremental"))),
            "v25_only_incremental":int(bool(m.get("v25_only_incremental"))),
            "accepted":0,"block_reason":ef["reason"],"result":"","net_pct":"",
            "exit_time_kst":"","data_error":"",
        }
        if not ef["pass"]:
            rows.append(row); continue
        if guard_hit(name,m):
            row["block_reason"]=name
            rows.append(row); continue
        ok,why=sched.can_open(st["entry"],st["symbol"])
        if not ok:
            row["block_reason"]=why
            rows.append(row); continue

        sim=getsim(st,cache)
        row.update({
            "accepted":1,"block_reason":"","result":sim.result,
            "net_pct":round(sim.net_pct,6),
            "exit_time_kst":kst(sim.exit_time),
            "data_error":sim.data_error,
        })
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        rows.append(row)
    return rows

def accepted(rows,lo=None,hi=None):
    z=[]
    for r in rows:
        if int(r.get("accepted") or 0)!=1: continue
        d=str(r["entry_time_kst"])[:10]
        if lo and d<lo: continue
        if hi and d>hi: continue
        z.append(r)
    return z

def stats(dataset,period,name,rows,base_rows,lo=None,hi=None):
    a=accepted(rows,lo,hi); b=accepted(base_rows,lo,hi)
    an=sum(float(r["net_pct"]) for r in a); bn=sum(float(r["net_pct"]) for r in b)
    aids={r["setup_id"] for r in a}; bids={r["setup_id"] for r in b}
    amap={r["setup_id"]:r for r in a}; bmap={r["setup_id"]:r for r in b}
    new=[amap[x] for x in aids-bids]
    rem=[bmap[x] for x in bids-aids]
    return {
        "dataset":dataset,"period":period,"scenario":name,
        "entries":len(a),"net_pct":round(an,6),
        "base_entries":len(b),"base_net":round(bn,6),"delta":round(an-bn,6),
        "tp":sum(r["result"]=="TP20_FULL" for r in a),
        "stop":sum(r["result"]=="STOP" for r in a),
        "pp12":sum(r["result"]=="PROFIT_PROTECT_EXIT" for r in a),
        "late":sum(r["result"]=="LATE_FAILURE_EXIT" for r in a),
        "new_entries":len(new),"new_net":round(sum(float(r["net_pct"]) for r in new),6),
        "removed_entries":len(rem),"removed_net":round(sum(float(r["net_pct"]) for r in rem),6),
    }

def daily(dataset,name,rows,base_rows):
    dates=sorted(set(str(r["entry_time_kst"])[:10] for r in rows+base_rows))
    out=[]
    for d in dates:
        a=accepted(rows,d,d); b=accepted(base_rows,d,d)
        an=sum(float(r["net_pct"]) for r in a); bn=sum(float(r["net_pct"]) for r in b)
        out.append({
            "dataset":dataset,"date":d,"scenario":name,
            "entries":len(a),"net_pct":round(an,6),
            "base_entries":len(b),"base_net":round(bn,6),"delta":round(an-bn,6),
        })
    return out

def incremental(dataset,combo_rows,cur_rows,meta):
    ca=accepted(combo_rows); va=accepted(cur_rows)
    cids={r["setup_id"] for r in ca}; vids={r["setup_id"] for r in va}
    cmap={r["setup_id"]:r for r in ca}; vmap={r["setup_id"]:r for r in va}

    # What OR removes relative to current V25 guard after full reschedule.
    removed=[vmap[x] for x in vids-cids]
    new=[cmap[x] for x in cids-vids]

    direct_v22_only=[r for r in removed if bool(meta.get(r["setup_id"],{}).get("v22_only_incremental"))]
    displaced=[r for r in removed if not bool(meta.get(r["setup_id"],{}).get("v22_only_incremental"))]

    return {
        "dataset":dataset,
        "cur_entries":len(va),
        "cur_net":round(sum(float(r["net_pct"]) for r in va),6),
        "or_entries":len(ca),
        "or_net":round(sum(float(r["net_pct"]) for r in ca),6),
        "or_delta_vs_cur":round(sum(float(r["net_pct"]) for r in ca)-sum(float(r["net_pct"]) for r in va),6),
        "direct_v22_only_removed":len(direct_v22_only),
        "direct_v22_only_removed_net":round(sum(float(r["net_pct"]) for r in direct_v22_only),6),
        "displaced_cur_entries":len(displaced),
        "displaced_cur_net":round(sum(float(r["net_pct"]) for r in displaced),6),
        "new_entries_after_or":len(new),
        "new_entries_after_or_net":round(sum(float(r["net_pct"]) for r in new),6),
    }

all_stats=[]; all_daily=[]; all_inc=[]; all_trade_rows=[]

# HIST
print("=== HIST ===",flush=True)
configure_range(HIST_START,HIST_END,".combo_hist_cache")
hs=U.load_setups()
hc=U.load_control_proxy()
hv22=load_v22_rows(HIST_START.astimezone(UTC),HIST_END.astimezone(UTC))
hm=build_meta(hv22,hs)
hcache=load_cache("UNIFIED_CURRENT_0901_0922_TRADES.csv")

hruns={}
for name in SCENARIOS:
    print("HIST",name,flush=True)
    hruns[name]=run(name,hs,hc,hm,hcache)
    for r in hruns[name]:
        rr=dict(r); rr["dataset"]="HIST"; all_trade_rows.append(rr)

hbase=hruns["BASE"]
for name in SCENARIOS:
    all_stats.append(stats("HIST","PRE_0901_0917",name,hruns[name],hbase,"2026-09-01","2026-09-17"))
    all_stats.append(stats("HIST","POST_0918_0922",name,hruns[name],hbase,"2026-09-18","2026-09-22"))
    all_stats.append(stats("HIST","ALL_0901_0922",name,hruns[name],hbase))
    if name!="BASE":
        all_daily += daily("HIST",name,hruns[name],hbase)

all_inc.append(incremental("HIST_ALL",hruns["OR_COMBO"],hruns["CUR_V25"],hm))

# FRESH
print("=== FRESH ===",flush=True)
configure_range(FRESH_WARM,FRESH_END+timedelta(seconds=1),".combo_fresh_cache")
fs=U.load_setups()
fc=U.load_control_proxy()
fv22=load_v22_rows(FRESH_WARM.astimezone(UTC),(FRESH_END+timedelta(seconds=1)).astimezone(UTC))
fm=build_meta(fv22,fs)
fcache=load_cache("FRESH_BASE_TRADES.csv",FRESH_START)
warm=warm_base(fs,fc,FRESH_START.astimezone(UTC),fcache)

fruns={}
for name in SCENARIOS:
    print("FRESH",name,flush=True)
    fruns[name]=run(
        name,fs,fc,fm,fcache,
        FRESH_START.astimezone(UTC),FRESH_END.astimezone(UTC),warm
    )
    for r in fruns[name]:
        rr=dict(r); rr["dataset"]="FRESH"; all_trade_rows.append(rr)

fbase=fruns["BASE"]
for name in SCENARIOS:
    all_stats.append(stats("FRESH","ALL",name,fruns[name],fbase))
    if name!="BASE":
        all_daily += daily("FRESH",name,fruns[name],fbase)

all_inc.append(incremental("FRESH",fruns["OR_COMBO"],fruns["CUR_V25"],fm))

write_csv(OUT_SUMMARY,all_stats)
write_csv(OUT_DAILY,all_daily)
write_csv(OUT_INCREMENTAL,all_inc)
write_csv(OUT_TRADES,all_trade_rows)

# Human-readable conclusion
lines=[
    "V22 + V25 COMBINED GUARD — EXACT RESCHEDULE",
    f"script={SCRIPT_VERSION}",
    "",
    "[RULES]",
    "CUR_V25 = V25 rolling2h>=8 AND V25-entry BTC/ETH abs4h avg>=0.40%",
    "V22_ONLY = V22 rolling2h>=20 AND V22-candidate BTC/ETH abs4h avg>=0.30%",
    "OR_COMBO = CUR_V25 OR V22_ONLY",
    "AND_COMBO = CUR_V25 AND V22_ONLY",
    "",
    "[RESULTS]",
]
for r in all_stats:
    lines.append(
        f"{r['dataset']} {r['period']} {r['scenario']}: "
        f"entries={r['entries']} NET={r['net_pct']:.6f} "
        f"delta_vs_BASE={r['delta']:+.6f} "
        f"new={r['new_entries']}({r['new_net']:+.6f}) "
        f"removed={r['removed_entries']}({r['removed_net']:+.6f}) "
        f"TP={r['tp']} STOP={r['stop']} PP12={r['pp12']} LATE={r['late']}"
    )

lines += ["","[OR vs CURRENT V25 — MARGINAL VALUE]"]
for r in all_inc:
    lines.append(
        f"{r['dataset']}: CUR={r['cur_net']:.6f} -> OR={r['or_net']:.6f} "
        f"delta={r['or_delta_vs_cur']:+.6f}; "
        f"direct V22-only removed={r['direct_v22_only_removed']} "
        f"net_was={r['direct_v22_only_removed_net']:+.6f}; "
        f"displaced current={r['displaced_cur_entries']} "
        f"net_was={r['displaced_cur_net']:+.6f}; "
        f"new after OR={r['new_entries_after_or']} "
        f"new_net={r['new_entries_after_or_net']:+.6f}"
    )

# Robustness verdict based only on PRE, POST, FRESH.
def pick(dataset,period,scenario):
    return next(r for r in all_stats if r["dataset"]==dataset and r["period"]==period and r["scenario"]==scenario)

lines += ["","[ROBUSTNESS]"]
for name in ["CUR_V25","V22_ONLY","OR_COMBO","AND_COMBO"]:
    pre=pick("HIST","PRE_0901_0917",name)
    post=pick("HIST","POST_0918_0922",name)
    fresh=pick("FRESH","ALL",name)
    ok=pre["delta"]>0 and post["delta"]>0 and fresh["delta"]>0
    lines.append(
        f"{name}: PRE={pre['delta']:+.6f}, POST={post['delta']:+.6f}, "
        f"FRESH={fresh['delta']:+.6f}, all_positive={ok}, "
        f"sum_delta={pre['delta']+post['delta']+fresh['delta']:+.6f}"
    )

OUT_TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")
print("\n".join(lines),flush=True)

with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in [OUT_SUMMARY,OUT_DAILY,OUT_INCREMENTAL,OUT_TRADES,OUT_TXT]:
        z.write(p,arcname=p.name)
print("DONE:",OUT_ZIP,flush=True)
