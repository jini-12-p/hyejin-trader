#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lightweight audit for latest DCA successes vs MON/NEAR failures.
Reads only latest DCA_RECOVER/DCA_FAIL rows from FINAL_0901_0928_RESULTS.zip,
reconstructs setup metadata from latest scan, pulls ~9h 1m candles per trade,
and computes DCA-decision and first 1/2/3m post-add behavior.

No orders. No DB writes. No bot changes.
"""
from __future__ import annotations
import csv, io, zipfile, math, importlib.util, sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

ROOT=Path("/root/hyejin-trader/bybit_swing")
FINALZIP=ROOT/"FINAL_0901_0928_RESULTS.zip"
FWD=ROOT/"forward_full_0924_0925.py"
OUT=ROOT/"LATEST_DCA_MON_NEAR_FEATURES.csv"
SUM=ROOT/"LATEST_DCA_MON_NEAR_FEATURES.txt"
ZIP=ROOT/"LATEST_DCA_MON_NEAR_FEATURES_RESULTS.zip"

KST=timezone(timedelta(hours=9)); UTC=timezone.utc

def fv(v,d=None):
    try:
        x=float(v); return d if math.isnan(x) else x
    except:return d
def pct(a,b): return (a/b-1)*100 if b else None
def mean(a):
    z=[float(x) for x in a if x is not None]
    return sum(z)/len(z) if z else None
def ema(v,n):
    if not v:return None
    a=2/(n+1);e=float(v[0])
    for x in v[1:]:e=a*float(x)+(1-a)*e
    return e
def rsi(v,n=14):
    if len(v)<n+1:return None
    ds=[b-a for a,b in zip(v[-n-1:-1],v[-n:])]
    g=sum(max(x,0) for x in ds)/n;l=sum(max(-x,0) for x in ds)/n
    return 100 if l==0 else 100-100/(1+g/l)
def writecsv(p,rows):
    if not rows:p.write_text("",encoding="utf-8-sig");return
    ks=[]
    for r in rows:
        for k in r:
            if k not in ks:ks.append(k)
    with p.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=ks,extrasaction="ignore");w.writeheader();w.writerows(rows)

if not FINALZIP.exists():raise SystemExit("missing FINAL_0901_0928_RESULTS.zip")
with zipfile.ZipFile(FINALZIP) as z:
    n=[x for x in z.namelist() if x.endswith("FINAL_0901_0928_STOP_DETAIL.csv")][0]
    with z.open(n) as raw:
        rr=list(csv.DictReader(io.TextIOWrapper(raw,encoding="utf-8-sig")))
targets=[r for r in rr if r.get("policy_class") in ("DCA_RECOVER","DCA_FAIL") and str(r.get("entry_time_kst",""))[:10]>="2026-09-24"]
print("latest DCA targets =",[(r["symbol"],r["entry_time_kst"],r["policy_class"]) for r in targets],flush=True)

spec=importlib.util.spec_from_file_location("MLATESTDCA",str(FWD))
M=importlib.util.module_from_spec(spec);sys.modules["MLATESTDCA"]=M;spec.loader.exec_module(M)
scan=next((p for p in [ROOT/"scan_FORWARD_20260923_TO_NOW_MKTREGIME_KST.csv",ROOT/"scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.csv",ROOT/"scan_rejected.csv"] if p.exists()),None)
if scan is None:raise SystemExit("no forward scan")
M.SCAN=scan;M.EVAL_START_KST=datetime(2026,9,23,tzinfo=KST);M.WARM_START_KST=datetime(2026,9,22,8,tzinfo=KST)
df,a,b,end=M.load_scan_window();M.configure_unified(end);M.U.load_market_series()
setups=M.U.load_setups()

# match by symbol + nearest entry second
def dtk(s): return datetime.strptime(str(s)[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).astimezone(UTC)
def find_setup(r):
    t=dtk(r["entry_time_kst"]);sym=r["symbol"]
    z=[s for s in setups if s["symbol"]==sym and abs((s["entry"]-t).total_seconds())<=2]
    if not z:
        z=[s for s in setups if s["symbol"]==sym and abs((s["entry"]-t).total_seconds())<=90]
    return min(z,key=lambda s:abs((s["entry"]-t).total_seconds())) if z else None

def five(one):
    d={}
    for b in one:
        t=b["ts"].replace(minute=b["ts"].minute//5*5,second=0,microsecond=0)
        if t not in d:d[t]={"ts":t,"o":b["o"],"h":b["h"],"l":b["l"],"c":b["c"],"v":b["v"]}
        else:
            x=d[t];x["h"]=max(x["h"],b["h"]);x["l"]=min(x["l"],b["l"]);x["c"]=b["c"];x["v"]+=b["v"]
    return [d[k] for k in sorted(d)]
def feat(one,cp):
    c=cp.replace(second=0,microsecond=0)
    p=[x for x in one if x["ts"]<c]; q=[x for x in five(one) if x["ts"]+timedelta(minutes=5)<=c]
    o={"vr":None,"ret1":None,"ret3":None,"slope":None,"rsi":None}
    if p:
        m=mean([x["v"] for x in p[-11:-1]]);o["vr"]=p[-1]["v"]/m if m else None
        o["ret1"]=pct(p[-1]["c"],p[-1]["o"])
        z=p[-3:];o["ret3"]=pct(z[-1]["c"],z[0]["o"]) if z else None
    if q:
        cs=[x["c"] for x in q];e=ema(cs[-80:],20);ep=ema(cs[-81:-1],20) if len(cs)>=2 else None
        o["slope"]=(e/ep-1)*100 if e and ep else None;o["rsi"]=rsi(cs)
    return o

out=[]
for i,r in enumerate(targets,1):
    st=find_setup(r)
    if st is None:
        print("NO SETUP",r["symbol"],r["entry_time_kst"]);continue
    sim=M.get_sim(st,{})
    entry=st["entry_price"];stop=sim.exit_time
    one=M.api_1m(st["symbol"],st["entry"]-timedelta(minutes=90),stop+timedelta(hours=6,minutes=5))
    sf=feat(one,stop)
    fl=stop.replace(second=0,microsecond=0)
    post=[b for b in one if fl+timedelta(minutes=1)<=b["ts"]<=fl+timedelta(hours=6)]
    low=lowts=trg=addp=None
    for b in post:
        if low is None or b["l"]<low:low=b["l"];lowts=b["ts"]
        if b["ts"]>lowts and b["h"]>=low*1.015:
            trg=b["ts"];addp=low*1.015;break
    if trg is None:
        print("NO TRIGGER",r["symbol"]);continue
    tf=feat(one,trg);lp=pct(low,entry);mins=(trg-lowts).total_seconds()/60
    green=lp>=-2.74314 and tf["rsi"] is not None and tf["rsi"]>=43.89902
    safe=(not green and mins<12 and tf["ret3"] is not None and tf["ret3"]>-1)
    av=(entry+addp)/2

    after=[b for b in post if b["ts"]>trg]
    vals={}
    for nmin in (1,2,3,5):
        z=after[:nmin]
        if z:
            vals[f"post{nmin}_close_from_add"]=pct(z[-1]["c"],addp)
            vals[f"post{nmin}_low_from_add"]=pct(min(x["l"] for x in z),addp)
            vals[f"post{nmin}_high_from_add"]=pct(max(x["h"] for x in z),addp)
    broke=recovered=False;event_min=None
    for b in after:
        if b["l"]<=low:
            broke=True;event_min=(b["ts"]-trg).total_seconds()/60;break
        if b["h"]>=av:
            recovered=True;event_min=(b["ts"]-trg).total_seconds()/60;break

    out.append({
        "symbol":r["symbol"],"entry_time_kst":r["entry_time_kst"],"actual":r["policy_class"],
        "entry_price":entry,"stop_time_kst":stop.astimezone(KST).strftime("%F %T"),
        "stop_vol_ratio":sf["vr"],"stop_ema20_slope":sf["slope"],"stop_rsi5":sf["rsi"],
        "swing_low_pct":lp,"low_to_trigger_min":mins,
        "trigger_time_kst":trg.astimezone(KST).strftime("%F %T"),
        "trigger_rsi5":tf["rsi"],"trigger_ret1":tf["ret1"],"trigger_ret3":tf["ret3"],
        "trigger_vol_ratio":tf["vr"],"trigger_ema20_slope":tf["slope"],
        "green":int(green),"safe_gray":int(safe),
        "add_price_pct":pct(addp,entry),"new_avg_pct":pct(av,entry),
        **vals,
        "old_low_break":int(broke),"recover_avg":int(recovered),"event_after_add_min":event_min
    })
    print(f"[{i}/{len(targets)}] {r['symbol']} {r['policy_class']} low={lp:.3f} RSI={tf['rsi']:.2f} p3={tf['ret3']:.3f}",flush=True)

writecsv(OUT,out)

succ=[x for x in out if x["actual"]=="DCA_RECOVER"];fail=[x for x in out if x["actual"]=="DCA_FAIL"]
features=["stop_vol_ratio","stop_ema20_slope","stop_rsi5","swing_low_pct","low_to_trigger_min",
          "trigger_rsi5","trigger_ret1","trigger_ret3","trigger_vol_ratio","trigger_ema20_slope",
          "add_price_pct","new_avg_pct",
          "post1_close_from_add","post1_low_from_add","post2_close_from_add","post2_low_from_add",
          "post3_close_from_add","post3_low_from_add","post5_close_from_add","post5_low_from_add"]
def med(z):
    z=sorted(z);return z[len(z)//2] if z else None
lines=["LATEST DCA MON/NEAR FEATURE AUDIT",f"success={len(succ)} fail={len(fail)}",""]
for group,name in ((succ,"SUCCESS"),(fail,"FAIL")):
    lines.append("["+name+"]")
    for k in features:
        z=[fv(x.get(k)) for x in group if fv(x.get(k)) is not None]
        if z:lines.append(f"{k}: median={med(z):.6f} range=({min(z):.6f},{max(z):.6f})")
    lines.append("")
lines.append("[ROWS]")
for x in out:lines.append(str(x))
SUM.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(ZIP,"w",zipfile.ZIP_DEFLATED) as z:z.write(OUT,arcname=OUT.name);z.write(SUM,arcname=SUM.name)
print("\n".join(lines));print("DONE:",ZIP)
