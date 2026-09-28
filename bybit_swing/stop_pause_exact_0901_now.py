#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exact 09/01 -> latest STOP-pause window comparison (30/45/60/90).
Uses current clean kline cache / current unified engine.
Replays the portfolio chronologically from the candidate stream so blocked
entries free slots and later candidates may enter.
Keeps all other current rules unchanged.
Outputs daily + trades + summary ZIP.
"""
import importlib.util,sys,csv,io,zipfile,math
from pathlib import Path
from datetime import datetime,timedelta,timezone
from collections import deque
R=Path("/root/hyejin-trader/bybit_swing");KST=timezone(timedelta(hours=9));UTC=timezone.utc
U0=R/"unified_current_0901_0922.py";F0=R/"forward_full_0924_0925.py";REG=R/"MKT_REGIME_EXACT_RESULTS.zip"
OUT=R/"STOP_PAUSE_EXACT_0901_NOW_RESULTS.zip"
def mod(n,p):
 s=importlib.util.spec_from_file_location(n,str(p));m=importlib.util.module_from_spec(s);sys.modules[n]=m;s.loader.exec_module(m);return m
def f(v,d=0):
 try:return float(v)
 except:return d
def wr(p,rr):
 if not rr:p.write_text("",encoding="utf-8-sig");return
 ks=[]
 for r in rr:
  for k in r:
   if k not in ks:ks.append(k)
 with p.open("w",encoding="utf-8-sig",newline="") as h:w=csv.DictWriter(h,fieldnames=ks);w.writeheader();w.writerows(rr)

# Load the already-established +9/-6 + V22 gate decisions for every candidate.
with zipfile.ZipFile(REG) as z:
 n=[n for n in z.namelist() if "PERF_B_N9_P9_N6" in n and n.endswith("_TRADES.csv")][0]
 rr=list(csv.DictReader(io.TextIOWrapper(z.open(n),encoding="utf-8-sig")))
gate={r["setup_id"]:r for r in rr}

# Historical and forward setup/sim engines.
U=mod("USP",U0);U.START_KST=datetime(2026,9,1,tzinfo=KST);U.END_KST=datetime(2026,9,23,tzinfo=KST);U.START_UTC=U.START_KST.astimezone(UTC);U.END_UTC=U.END_KST.astimezone(UTC);U.EXPECTED_V25=-1;U.CACHE_DIR=R/".stop_pause_exact_hist";U.CACHE_DIR.mkdir(exist_ok=True);U.KC=U.KlineCache();U._market_1m={};U._market_recompute_count=0;U.load_market_series();hs=U.load_setups()
M=mod("MSP",F0);M.SCAN=next(p for p in [R/"scan_FORWARD_20260923_TO_NOW_MKTREGIME_KST.csv",R/"scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.csv",R/"scan_rejected.csv"] if p.exists());M.EVAL_START_KST=datetime(2026,9,23,tzinfo=KST);M.WARM_START_KST=datetime(2026,9,22,8,tzinfo=KST);df,a,b,end=M.load_scan_window();M.configure_unified(end);M.U.load_market_series();fs=M.U.load_setups()
setups=sorted(hs+fs,key=lambda s:s["entry"])
hc={};fc={}
def sim(st):
 return U.simulate_base(st) if st["entry"].astimezone(KST)<datetime(2026,9,23,tzinfo=KST) else M.get_sim(st,fc)

# Candidate stream is the established regime+V22 accepted/pre-portfolio stream available in gate file.
candidates=[]
seen=set()
for st in setups:
 sid=st["setup_id"]
 if sid in seen:continue
 g=gate.get(sid)
 if not g:continue
 seen.add(sid)
 # Keep rows that passed the strategy filters before portfolio constraints when possible.
 # Exact gate files encode accepted=1 for final admissible candidate; this comparison
 # reschedules STOP-pause and standard portfolio limits chronologically.
 if int(f(g.get("accepted"),0))!=1:continue
 candidates.append(st)

WINDOWS=(30,45,60,90)
alltr=[];alld=[]
for W in WINDOWS:
 openpos=[];entries15=deque();stop_exits=[];sym_last={};accepted=[]
 for st in candidates:
  t=st["entry"]
  # expire open positions and rolling entry timestamps
  openpos=[x for x in openpos if x["exit"]>t]
  while entries15 and entries15[0] < t-timedelta(minutes=15):entries15.popleft()
  # standard slot/cap
  if len(openpos)>=4:
   alltr.append({"window":W,"symbol":st["symbol"],"entry_time":t.astimezone(KST).strftime("%F %T"),"action":"BLOCK_SLOT4"});continue
  if len(entries15)>=2:
   alltr.append({"window":W,"symbol":st["symbol"],"entry_time":t.astimezone(KST).strftime("%F %T"),"action":"BLOCK_CAP15"});continue
  # same-symbol cooldown based on accepted previous trade
  prev=sym_last.get(st["symbol"])
  if prev:
   mins=(t-prev["exit"]).total_seconds()/60
   need=180 if prev["result"] in ("STOP","LATE_FAILURE_EXIT") else 90
   if mins<need:
    alltr.append({"window":W,"symbol":st["symbol"],"entry_time":t.astimezone(KST).strftime("%F %T"),"action":"BLOCK_COOLDOWN"});continue
  # requested variable: >=2 stop/late exits inside W min
  recent=[x for x in stop_exits if t-timedelta(minutes=W)<=x<=t]
  if len(recent)>=2:
   alltr.append({"window":W,"symbol":st["symbol"],"entry_time":t.astimezone(KST).strftime("%F %T"),"action":"BLOCK_STOP_PAUSE"});continue
  x=sim(st);accepted.append((st,x));entries15.append(t);openpos.append({"exit":x.exit_time});sym_last[st["symbol"]]={"exit":x.exit_time,"result":x.result}
  if x.result in ("STOP","LATE_FAILURE_EXIT"):stop_exits.append(x.exit_time)
  alltr.append({"window":W,"symbol":st["symbol"],"entry_time":t.astimezone(KST).strftime("%F %T"),"action":"ENTRY","result":x.result,"net":x.net_pct,"exit_time":x.exit_time.astimezone(KST).strftime("%F %T")})
 days=sorted(set(st["entry"].astimezone(KST).strftime("%F") for st,x in accepted))
 for day in days:
  z=[(st,x) for st,x in accepted if st["entry"].astimezone(KST).strftime("%F")==day]
  alld.append({"window":W,"date":day,"entries":len(z),"TP":sum(x.result=="TP20_FULL" for st,x in z),"STOP":sum(x.result=="STOP" for st,x in z),"net":sum(x.net_pct for st,x in z)})
 print("WINDOW",W,"entries",len(accepted),"net",sum(x.net_pct for st,x in accepted),flush=True)

summary=[]
for W in WINDOWS:
 z=[r for r in alld if r["window"]==W]
 def seg(a,b):
  q=[r for r in z if a<=r["date"]<=b];return sum(r["net"] for r in q)
 summary.append({"window":W,"entries":sum(r["entries"] for r in z),"net":sum(r["net"] for r in z),
                 "net_0901_0917":seg("2026-09-01","2026-09-17"),
                 "net_0918_0922":seg("2026-09-18","2026-09-22"),
                 "net_0923_now":seg("2026-09-23","2099-12-31"),
                 "day27_net":seg("2026-09-27","2026-09-27"),
                 "negative_days":sum(r["net"]<0 for r in z),
                 "worst_day":min((r["net"] for r in z),default=0)})
p1=R/"STOP_PAUSE_EXACT_0901_NOW_TRADES.csv";p2=R/"STOP_PAUSE_EXACT_0901_NOW_DAILY.csv";p3=R/"STOP_PAUSE_EXACT_0901_NOW_SUMMARY.csv"
wr(p1,alltr);wr(p2,alld);wr(p3,summary)
with zipfile.ZipFile(OUT,"w",zipfile.ZIP_DEFLATED) as z:
 for p in (p1,p2,p3):z.write(p,arcname=p.name)
print(summary);print("DONE",OUT)
