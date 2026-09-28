#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DCA post-add recovery-time audit for all allowed DCA trades.
Tests whether MON/NEAR can be protected by a timeout after 100% add.

For every GREEN/SAFE_GRAY DCA:
- reconstruct add trigger
- measure minutes to new-average recovery or old-low break
- evaluate timeout candidates 5/10/15/20/30/45/60m
- at timeout, if new average not recovered, compare:
  A) full exit at timeout
  B) current policy (wait for avg or old-low break)
No orders, DB writes, or bot changes.
"""
import csv,io,zipfile,math,importlib.util,sys
from pathlib import Path
from datetime import datetime,timedelta,timezone

ROOT=Path("/root/hyejin-trader/bybit_swing")
FINALZIP=ROOT/"FINAL_0901_0928_RESULTS.zip"
SEPZIP=ROOT/"DCA100_SEPARATION_RESULTS.zip"
FWD=ROOT/"forward_full_0924_0925.py"
OUT=ROOT/"DCA_POSTADD_TIMEOUT_DETAIL.csv"
SUM=ROOT/"DCA_POSTADD_TIMEOUT_SUMMARY.csv"
TXT=ROOT/"DCA_POSTADD_TIMEOUT_SUMMARY.txt"
ZIP=ROOT/"DCA_POSTADD_TIMEOUT_RESULTS.zip"
KST=timezone(timedelta(hours=9));UTC=timezone.utc
FEE=.055
TIMEOUTS=(5,10,15,20,30,45,60)

def f(v,d=None):
    try:
        x=float(v);return d if math.isnan(x) else x
    except:return d
def pct(a,b):return (a/b-1)*100 if b else None
def rz(zp,needle):
    with zipfile.ZipFile(zp) as z:
        n=[x for x in z.namelist() if needle in x][0]
        with z.open(n) as raw:return list(csv.DictReader(io.TextIOWrapper(raw,encoding="utf-8-sig")))
def wr(p,rows):
    if not rows:p.write_text("",encoding="utf-8-sig");return
    ks=[]
    for r in rows:
        for k in r:
            if k not in ks:ks.append(k)
    with p.open("w",encoding="utf-8-sig",newline="") as h:
        w=csv.DictWriter(h,fieldnames=ks,extrasaction="ignore");w.writeheader();w.writerows(rows)
def avg_net(e,a):
    av=(e+a)/2
    return -(FEE+FEE*a/e+FEE*2*av/e)
def fail_net(e,a,x):
    return ((x-e)+(x-a))/e*100-(FEE+FEE*a/e+FEE*2*x/e)
def exit_both_net(e,a,x):
    return ((x-e)+(x-a))/e*100-(FEE+FEE*a/e+FEE*2*x/e)

# targets from historical 27 + latest DCA recover/fail
targets=[]
sep=rz(SEPZIP,"DCA100_SEPARATION_DETAIL.csv")
for x in sep:
    vr=f(x.get("STOP_prev1m_vol_ratio10"));sl=f(x.get("STOP_ema20_slope"))
    if vr is None or sl is None or not(vr<=2.61 and sl>=.00061):continue
    green=f(x.get("RB_swing_low_pct"),-999)>=-2.74314 and f(x.get("RB_rsi14_5m"),-999)>=43.89902
    safe=(not green and f(x.get("RB_low_to_trigger_min"),999)<12 and f(x.get("RB_prev3m_ret"),-999)>-1)
    if green or safe:
        targets.append({"symbol":x["symbol"],"entry_time_kst":x["entry_time_kst"],
                        "expected":"SUCCESS","source":"HIST"})
final=rz(FINALZIP,"FINAL_0901_0928_STOP_DETAIL.csv")
for x in final:
    if x.get("policy_class") in ("DCA_RECOVER","DCA_FAIL") and str(x.get("entry_time_kst",""))[:10]>="2026-09-24":
        targets.append({"symbol":x["symbol"],"entry_time_kst":x["entry_time_kst"],
                        "expected":"SUCCESS" if x["policy_class"]=="DCA_RECOVER" else "FAIL",
                        "source":"LATEST"})

# de-dupe
seen=set();targets=[x for x in targets if not ((x["symbol"],x["entry_time_kst"]) in seen or seen.add((x["symbol"],x["entry_time_kst"])))]
print("targets",len(targets),flush=True)

spec=importlib.util.spec_from_file_location("MTIMEOUT",str(FWD))
M=importlib.util.module_from_spec(spec);sys.modules["MTIMEOUT"]=M;spec.loader.exec_module(M)
scan=next((p for p in [ROOT/"scan_FORWARD_20260923_TO_NOW_MKTREGIME_KST.csv",
                       ROOT/"scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.csv",
                       ROOT/"scan_rejected.csv"] if p.exists()),None)
M.SCAN=scan;M.EVAL_START_KST=datetime(2026,9,23,tzinfo=KST);M.WARM_START_KST=datetime(2026,9,22,8,tzinfo=KST)
df,a,b,end=M.load_scan_window();M.configure_unified(end);M.U.load_market_series()
fset=M.U.load_setups()

# historical setup engine too
U_PATH=ROOT/"unified_current_0901_0922.py"
sp=importlib.util.spec_from_file_location("UTIMEOUT",str(U_PATH));U=importlib.util.module_from_spec(sp);sys.modules["UTIMEOUT"]=U;sp.loader.exec_module(U)
U.START_KST=datetime(2026,9,1,tzinfo=KST);U.END_KST=datetime(2026,9,23,tzinfo=KST)
U.START_UTC=U.START_KST.astimezone(UTC);U.END_UTC=U.END_KST.astimezone(UTC);U.EXPECTED_V25=-1
U.CACHE_DIR=ROOT/".dca_timeout_hist";U.CACHE_DIR.mkdir(exist_ok=True);U.KC=U.KlineCache();U._market_1m={};U._market_recompute_count=0
U.load_market_series();hset=U.load_setups()

def dtk(s):return datetime.strptime(str(s)[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).astimezone(UTC)
def find_setup(t):
    pool=hset if t["source"]=="HIST" else fset
    et=dtk(t["entry_time_kst"]);z=[s for s in pool if s["symbol"]==t["symbol"] and abs((s["entry"]-et).total_seconds())<=90]
    return min(z,key=lambda s:abs((s["entry"]-et).total_seconds())) if z else None

rows=[]
for i,t in enumerate(targets,1):
    st=find_setup(t)
    if st is None:
        print("NO SETUP",t);continue
    sim=(U.simulate_base(st) if t["source"]=="HIST" else M.get_sim(st,{}))
    e=st["entry_price"];stop=sim.exit_time
    one=M.api_1m(st["symbol"],stop-timedelta(minutes=5),stop+timedelta(hours=6,minutes=5))
    fl=stop.replace(second=0,microsecond=0);post=[b for b in one if fl+timedelta(minutes=1)<=b["ts"]<=fl+timedelta(hours=6)]
    lo=lt=trg=add=None
    for b in post:
        if lo is None or b["l"]<lo:lo=b["l"];lt=b["ts"]
        if b["ts"]>lt and b["h"]>=lo*1.015:trg=b["ts"];add=lo*1.015;break
    if trg is None:continue
    av=(e+add)/2;after=[b for b in post if b["ts"]>trg]
    event="STALL";evtmin=None;current=None
    for b in after:
        if b["l"]<=lo:
            event="LOW_BREAK";evtmin=(b["ts"]-trg).total_seconds()/60;current=fail_net(e,add,lo);break
        if b["h"]>=av:
            event="RECOVER";evtmin=(b["ts"]-trg).total_seconds()/60;current=avg_net(e,add);break
    if current is None:current=sim.net_pct
    r={"symbol":t["symbol"],"entry_time_kst":t["entry_time_kst"],"source":t["source"],"expected":t["expected"],
       "event":event,"event_min":evtmin,"current_policy_net":current}
    for N in TIMEOUTS:
        if event=="RECOVER" and evtmin is not None and evtmin<=N:
            r[f"T{N}_net"]=current;r[f"T{N}_action"]="RECOVER_BEFORE_TIMEOUT"
        elif event=="LOW_BREAK" and evtmin is not None and evtmin<=N:
            r[f"T{N}_net"]=current;r[f"T{N}_action"]="LOW_BREAK_BEFORE_TIMEOUT"
        else:
            # exit both at close of N-th minute after add
            z=[b for b in after if b["ts"]<=trg+timedelta(minutes=N)]
            if not z:
                r[f"T{N}_net"]=current;r[f"T{N}_action"]="NO_BAR"
            else:
                x=z[-1]["c"];r[f"T{N}_net"]=exit_both_net(e,add,x);r[f"T{N}_action"]="FULL_EXIT_TIMEOUT"
    rows.append(r)
    print(f"[{i}/{len(targets)}] {t['symbol']} {event} {evtmin}",flush=True)

wr(OUT,rows)
summ=[]
for N in TIMEOUTS:
    s=[r for r in rows if r["expected"]=="SUCCESS"];q=[r for r in rows if r["expected"]=="FAIL"]
    cur=sum(r["current_policy_net"] for r in rows);new=sum(r[f"T{N}_net"] for r in rows)
    succ_cut=sum(r[f"T{N}_action"]=="FULL_EXIT_TIMEOUT" for r in s)
    fail_saved=sum(r[f"T{N}_net"]>r["current_policy_net"] for r in q)
    summ.append({"timeout_min":N,"all_current_net":cur,"all_timeout_net":new,"delta":new-cur,
                 "success_timeout_cut":succ_cut,"fail_improved":fail_saved,
                 "MON_net":next((r[f"T{N}_net"] for r in q if r["symbol"].startswith("MON")),None),
                 "NEAR_net":next((r[f"T{N}_net"] for r in q if r["symbol"].startswith("NEAR")),None)})
wr(SUM,summ)
lines=["DCA POST-ADD TIMEOUT AUDIT"]+[str(x) for x in summ]
TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in (OUT,SUM,TXT):z.write(p,arcname=p.name)
print("\n".join(lines));print("DONE:",ZIP)
