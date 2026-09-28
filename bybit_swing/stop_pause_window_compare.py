#!/usr/bin/env python3
# STOP-pause window comparison: 30/45/60/90m on current clean engine.
# Reuses current unified replay and accepted candidate stream; no bot/live changes.
import importlib.util,sys,csv,zipfile
from pathlib import Path
from datetime import datetime,timedelta,timezone
R=Path("/root/hyejin-trader/bybit_swing"); P=R/"forward_full_0924_0925.py"; KST=timezone(timedelta(hours=9))
s=importlib.util.spec_from_file_location("SPW",str(P));M=importlib.util.module_from_spec(s);sys.modules["SPW"]=M;s.loader.exec_module(M)
M.SCAN=next(p for p in [R/"scan_FORWARD_20260923_TO_NOW_MKTREGIME_KST.csv",R/"scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.csv",R/"scan_rejected.csv"] if p.exists())
M.EVAL_START_KST=datetime(2026,9,23,tzinfo=KST);M.WARM_START_KST=datetime(2026,9,22,8,tzinfo=KST)
df,a,b,e=M.load_scan_window();M.configure_unified(e);M.U.load_market_series()
# Load clean final accepted cohort as baseline and all setups for counterfactual scheduling.
setups=sorted(M.U.load_setups(),key=lambda x:x["entry"])
cache={}
# Candidate gate metadata from latest regime trades.
Z=R/"MKT_REGIME_EXACT_RESULTS.zip"
import io
with zipfile.ZipFile(Z) as z:
 n=[n for n in z.namelist() if "PERF_B_N9_P9_N6" in n and n.endswith("_TRADES.csv")][0]
 rr=list(csv.DictReader(io.TextIOWrapper(z.open(n),encoding="utf-8-sig")))
gate={r["setup_id"]:r for r in rr}
windows=(30,45,60,90)
rows=[];daily=[]
for W in windows:
 openpos=[];entries=[];stop_exits=[];last_sym={}
 for st in setups:
  t=st["entry"]; sid=st["setup_id"];g=gate.get(sid)
  if not g or int(float(g.get("accepted") or 0))!=1:continue
  # Release completed positions.
  openpos=[x for x in openpos if x[1]>t]
  # Counterfactual pause only: >=2 STOP/LATE exits in preceding W minutes.
  recent=[x for x in stop_exits if t-timedelta(minutes=W)<=x<=t]
  if len(recent)>=2:
   rows.append({"window":W,"symbol":st["symbol"],"entry_time":t.astimezone(KST).strftime("%F %T"),"action":"BLOCK_STOP_PAUSE","result":"","net":0});continue
  sim=M.get_sim(st,cache)
  entries.append((st,sim));openpos.append((st["symbol"],sim.exit_time))
  if sim.result in ("STOP","LATE_FAILURE_EXIT"):stop_exits.append(sim.exit_time)
  rows.append({"window":W,"symbol":st["symbol"],"entry_time":t.astimezone(KST).strftime("%F %T"),"action":"ENTRY","result":sim.result,"net":sim.net_pct})
 for day in sorted(set(x[0]["entry"].astimezone(KST).strftime("%F") for x in entries)):
  z=[x for x in entries if x[0]["entry"].astimezone(KST).strftime("%F")==day]
  daily.append({"window":W,"date":day,"entries":len(z),"TP":sum(x[1].result=="TP20_FULL" for x in z),"STOP":sum(x[1].result=="STOP" for x in z),"net":sum(x[1].net_pct for x in z)})
 print("WINDOW",W,"entries",len(entries),"net",sum(x[1].net_pct for x in entries),flush=True)
def wr(p,rr):
 ks=[]
 for r in rr:
  for k in r:
   if k not in ks:ks.append(k)
 with p.open("w",encoding="utf-8-sig",newline="") as h:w=csv.DictWriter(h,fieldnames=ks);w.writeheader();w.writerows(rr)
wr(R/"STOP_PAUSE_WINDOW_COMPARE_TRADES.csv",rows);wr(R/"STOP_PAUSE_WINDOW_COMPARE_DAILY.csv",daily)
summary=[]
for W in windows:
 z=[x for x in daily if x["window"]==W]
 d27=next((x for x in z if x["date"]=="2026-09-27"),{})
 summary.append({"window":W,"entries":sum(x["entries"] for x in z),"net":sum(x["net"] for x in z),"day27_entries":d27.get("entries"),"day27_net":d27.get("net")})
wr(R/"STOP_PAUSE_WINDOW_COMPARE_SUMMARY.csv",summary)
out=R/"STOP_PAUSE_WINDOW_COMPARE_RESULTS.zip"
with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as z:
 for n in ("STOP_PAUSE_WINDOW_COMPARE_TRADES.csv","STOP_PAUSE_WINDOW_COMPARE_DAILY.csv","STOP_PAUSE_WINDOW_COMPARE_SUMMARY.csv"):z.write(R/n,arcname=n)
print(summary);print("DONE",out)
