#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Historical exact 4-way replay: 2026-09-01 ~ 2026-09-22

A V25_BASE
B V25_PLUS_MARKET
C V25_PLUS_V22Q
D V25_PLUS_MARKET_PLUS_V22Q

Uses the same current entry/exit engine and portfolio scheduler.
V22 flags are read from the already-produced V22_QUALITY_RESCHEDULE_TRADES.csv
so candidate-time feature recovery is identical to the prior validation.

No DB writes. No orders.
"""
from __future__ import annotations
import csv, importlib.util, math, sys, zipfile
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import deque, Counter
import pandas as pd

ROOT = Path("/root/hyejin-trader/bybit_swing")
U_PATH = ROOT / "unified_current_0901_0922.py"
BASE_TRADES = ROOT / "UNIFIED_CURRENT_0901_0922_TRADES.csv"
V22_TRADES = ROOT / "V22_QUALITY_RESCHEDULE_TRADES.csv"

OUT_SUM = ROOT / "HIST_0901_0922_4WAY_SUMMARY.csv"
OUT_DAY = ROOT / "HIST_0901_0922_4WAY_DAILY.csv"
OUT_TXT = ROOT / "HIST_0901_0922_4WAY_SUMMARY.txt"
OUT_ZIP = ROOT / "HIST_0901_0922_4WAY_RESULTS.zip"

KST = timezone(timedelta(hours=9))
UTC = timezone.utc
START_KST = datetime(2026,9,1,0,0,0,tzinfo=KST)
END_KST = datetime(2026,9,23,0,0,0,tzinfo=KST)
RISK_V25_2H_MIN = 8
RISK_ABS4H_AVG_MIN = 0.40

SCENARIOS = [
    ("V25_BASE", False, False),
    ("V25_PLUS_MARKET", True, False),
    ("V25_PLUS_V22Q", False, True),
    ("V25_PLUS_MARKET_PLUS_V22Q", True, True),
]

def load_module(name,path):
    spec=importlib.util.spec_from_file_location(name,str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    m=importlib.util.module_from_spec(spec)
    sys.modules[name]=m
    spec.loader.exec_module(m)
    return m

U=load_module("U_HIST4WAY",U_PATH)

def fv(v,default=None):
    try:
        if v is None or str(v).strip()=="" or pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default

def dt_utc(v):
    if v in (None,""): return None
    try:
        d=datetime.fromisoformat(str(v).replace("Z","+00:00"))
        if d.tzinfo is None: d=d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except Exception:
        return None

def kst_stamp(d):
    return d.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

def write_csv(path, rows):
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

def load_v22_flags():
    if not V22_TRADES.exists():
        raise RuntimeError(f"missing {V22_TRADES}")
    df=pd.read_csv(V22_TRADES,low_memory=False)
    # HIST rows only; flags are identical across scenarios, take first per setup.
    df=df[df["dataset"].astype(str)=="HIST"].copy()
    out={}
    for r in df.to_dict("records"):
        sid=str(r.get("setup_id") or "")
        if not sid or sid in out: continue
        out[sid]=bool(int(fv(r.get("overext"),0) or 0) or int(fv(r.get("weak_reaccel"),0) or 0))
    return out

def build_market_guard(setups):
    q=deque(); out={}
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
        out[st["setup_id"]]=bool(len(q)>=RISK_V25_2H_MIN and avg is not None and avg>=RISK_ABS4H_AVG_MIN)
    return out

def load_base_cache():
    out={}
    if not BASE_TRADES.exists():
        return out
    df=pd.read_csv(BASE_TRADES,low_memory=False)
    for r in df.to_dict("records"):
        if int(fv(r.get("accepted"),0) or 0)!=1:
            continue
        et=dt_utc(r.get("exit_ts_utc"))
        if et is None:
            txt=str(r.get("exit_time_kst") or "")
            if txt:
                try:
                    et=datetime.strptime(txt[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).astimezone(UTC)
                except Exception:
                    et=None
        if et is None: continue
        ep=fv(r.get("entry_price"),0) or 0
        terminal=fv(r.get("terminal_price"),ep) or ep
        out[str(r["setup_id"])]=U.SimResult(
            result=str(r.get("result") or ""),
            exit_time=et, terminal_price=terminal, fills=[],
            gross_pct=fv(r.get("gross_pct"),0) or 0,
            fee_pct=fv(r.get("fee_pct"),0) or 0,
            net_pct=fv(r.get("net_pct"),0) or 0,
            mfe_pct=fv(r.get("mfe_pct"),0) or 0,
            mae_pct=fv(r.get("mae_pct"),0) or 0,
            stop_stage=str(r.get("stop_stage") or ""),
            detail="CACHED_BASE",
            data_error=str(r.get("data_error") or ""),
        )
    return out

def getsim(st,cache):
    sid=st["setup_id"]
    if sid not in cache:
        cache[sid]=U.simulate_base(st)
    return cache[sid]

def run(name,setups,controls,market_guard,v22_flags,cache,use_market,use_v22):
    sched=U.Scheduler()
    rows=[]
    for n,st in enumerate(setups,1):
        sid=st["setup_id"]
        ef=U.entry_filter(st,controls)
        row={
            "scenario":name,
            "setup_id":sid,
            "symbol":st["symbol"],
            "entry_time_kst":kst_stamp(st["entry"]),
            "accepted":0,
            "block_reason":ef["reason"],
            "market_guard":int(bool(market_guard.get(sid))),
            "v22q":int(bool(v22_flags.get(sid))),
            "result":"",
            "net_pct":"",
            "exit_time_kst":"",
        }
        if not ef["pass"]:
            rows.append(row); continue
        if use_market and market_guard.get(sid):
            row["block_reason"]="V25_MARKET_GUARD"
            rows.append(row); continue
        if use_v22 and v22_flags.get(sid):
            row["block_reason"]="V22_QUALITY_OR"
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
            "exit_time_kst":kst_stamp(sim.exit_time),
        })
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        rows.append(row)
        if n%100==0:
            print(name,n,"/",len(setups),flush=True)
    return rows

def accepted(rows,lo=None,hi=None):
    out=[]
    for r in rows:
        if int(r["accepted"])!=1: continue
        d=str(r["entry_time_kst"])[:10]
        if lo and d<lo: continue
        if hi and d>hi: continue
        out.append(r)
    return out

def stats(name,rows,period,lo=None,hi=None):
    z=accepted(rows,lo,hi)
    c=Counter(str(r["result"]) for r in z)
    return {
        "scenario":name,"period":period,
        "entries":len(z),
        "net_pct":round(sum(float(r["net_pct"]) for r in z),6),
        "tp":c.get("TP20_FULL",0),
        "stop":c.get("STOP",0),
        "pp12":c.get("PROFIT_PROTECT_EXIT",0),
        "late":c.get("LATE_FAILURE_EXIT",0),
        "time":c.get("TIME_EXIT",0),
    }

# Force exact historical range.
U.START_KST=START_KST
U.END_KST=END_KST
U.START_UTC=START_KST.astimezone(UTC)
U.END_UTC=END_KST.astimezone(UTC)
U.EXPECTED_V25=-1
U.CACHE_DIR=ROOT/".hist4way_cache"
U.CACHE_DIR.mkdir(parents=True,exist_ok=True)
U.KC=U.KlineCache()
U._market_1m={}
U._market_recompute_count=0

print("Loading market series...",flush=True)
U.load_market_series()
print("Loading setups...",flush=True)
setups=U.load_setups()
controls=U.load_control_proxy()
setups=[s for s in setups if U.START_UTC <= s["entry"] < U.END_UTC]
print("setups =",len(setups),flush=True)

v22_flags=load_v22_flags()
market_guard=build_market_guard(setups)
cache=load_base_cache()

runs={}
for name,use_market,use_v22 in SCENARIOS:
    print("RUN",name,flush=True)
    runs[name]=run(name,setups,controls,market_guard,v22_flags,cache,use_market,use_v22)
    write_csv(ROOT/f"HIST_0901_0922_4WAY_{name}_TRADES.csv",runs[name])

summary=[]
for name,_,_ in SCENARIOS:
    summary += [
        stats(name,runs[name],"PRE_0901_0917","2026-09-01","2026-09-17"),
        stats(name,runs[name],"POST_0918_0922","2026-09-18","2026-09-22"),
        stats(name,runs[name],"ALL_0901_0922"),
    ]

dates=[f"2026-09-{d:02d}" for d in range(1,23)]
daily=[]
for d in dates:
    row={"date":d}
    for name,_,_ in SCENARIOS:
        z=accepted(runs[name],d,d)
        row[f"{name}_entries"]=len(z)
        row[f"{name}_net"]=round(sum(float(r["net_pct"]) for r in z),6)
    daily.append(row)

write_csv(OUT_SUM,summary)
write_csv(OUT_DAY,daily)

lines=[
    "HISTORICAL 4-WAY EXACT REPLAY 2026-09-01~22",
    "",
    "A V25_BASE",
    "B V25_PLUS_MARKET",
    "C V25_PLUS_V22Q",
    "D V25_PLUS_MARKET_PLUS_V22Q",
    "",
]
for r in summary:
    lines.append(
        f"{r['period']} {r['scenario']}: entries={r['entries']} "
        f"NET={r['net_pct']:+.6f} TP={r['tp']} STOP={r['stop']} "
        f"PP12={r['pp12']} LATE={r['late']} TIME={r['time']}"
    )
OUT_TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")
print("\n".join(lines),flush=True)

with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in [OUT_SUM,OUT_DAY,OUT_TXT]:
        z.write(p,arcname=p.name)
    for name,_,_ in SCENARIOS:
        p=ROOT/f"HIST_0901_0922_4WAY_{name}_TRADES.csv"
        z.write(p,arcname=p.name)

print("DONE:",OUT_ZIP,flush=True)
