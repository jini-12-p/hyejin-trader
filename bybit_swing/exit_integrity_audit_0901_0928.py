#!/usr/bin/env python3
import csv,io,zipfile,math,importlib.util,sys
from pathlib import Path
from datetime import datetime,timedelta,timezone
from collections import Counter
R=Path("/root/hyejin-trader/bybit_swing"); Z=R/"MKT_REGIME_EXACT_RESULTS.zip"; U0=R/"unified_current_0901_0922.py"; F0=R/"forward_full_0924_0925.py"
KST=timezone(timedelta(hours=9)); UTC=timezone.utc
D=R/"EXIT_INTEGRITY_AUDIT_0901_0928_DETAIL.csv"; X=R/"EXIT_INTEGRITY_AUDIT_0901_0928_MISMATCH.csv"; S=R/"EXIT_INTEGRITY_AUDIT_0901_0928_SUMMARY.csv"; T=R/"EXIT_INTEGRITY_AUDIT_0901_0928_SUMMARY.txt"; O=R/"EXIT_INTEGRITY_AUDIT_0901_0928_RESULTS.zip"
def f(v,d=0):
 try:return float(v)
 except:return d
def mod(n,p):
 s=importlib.util.spec_from_file_location(n,str(p));m=importlib.util.module_from_spec(s);sys.modules[n]=m;s.loader.exec_module(m);return m
def wr(p,rr):
 if not rr:p.write_text("",encoding="utf-8-sig");return
 ks=[]
 for r in rr:
  for k in r:
   if k not in ks:ks.append(k)
 with p.open("w",encoding="utf-8-sig",newline="") as h:w=csv.DictWriter(h,fieldnames=ks);w.writeheader();w.writerows(rr)
with zipfile.ZipFile(Z) as z:
 n=[n for n in z.namelist() if "PERF_B_N9_P9_N6" in n and n.endswith("_TRADES.csv")][0]
 reg=list(csv.DictReader(io.TextIOWrapper(z.open(n),encoding="utf-8-sig")))
reg=[r for r in reg if int(f(r.get("accepted")))==1]
U=mod("UAI",U0);U.START_KST=datetime(2026,9,1,tzinfo=KST);U.END_KST=datetime(2026,9,23,tzinfo=KST);U.START_UTC=U.START_KST.astimezone(UTC);U.END_UTC=U.END_KST.astimezone(UTC);U.EXPECTED_V25=-1;U.CACHE_DIR=R/".exit_audit_cache";U.CACHE_DIR.mkdir(exist_ok=True);U.KC=U.KlineCache();U._market_1m={};U._market_recompute_count=0;U.load_market_series();hs={s["setup_id"]:s for s in U.load_setups()}
M=mod("MAI",F0);scan=next(p for p in [R/"scan_FORWARD_20260923_TO_NOW_MKTREGIME_KST.csv",R/"scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.csv",R/"scan_rejected.csv"] if p.exists());M.SCAN=scan;M.EVAL_START_KST=datetime(2026,9,23,tzinfo=KST);M.WARM_START_KST=datetime(2026,9,22,8,tzinfo=KST);df,a,b,e=M.load_scan_window();M.configure_unified(e);M.U.load_market_series();fs={s["setup_id"]:s for s in M.U.load_setups()};cache={}
def ppref(st,et):
 c=M.U.ReplayClient(st["symbol"],st["entry"]);c.set_now(et);b=c.last_confirmed(5);return None if b is None else (float(b["close"]),b["ts"])
def bounds(sym,et):
 q=M.api_1m(sym,et.replace(second=0,microsecond=0)-timedelta(minutes=1),et.replace(second=0,microsecond=0)+timedelta(minutes=2))
 q=[x for x in q if abs((x["ts"]-et.replace(second=0,microsecond=0)).total_seconds())<=60]
 return None if not q else (min(x["l"] for x in q),max(x["h"] for x in q))
rows=[]
for i,r in enumerate(reg,1):
 hist=r["entry_time_kst"][:10]<="2026-09-22";st=(hs if hist else fs).get(r["setup_id"]);issues=[]
 if not st: rows.append({"period":"HIST" if hist else "FWD",**r,"audit_status":"MISMATCH","issues":"SETUP_NOT_FOUND"});continue
 sim=U.simulate_base(st) if hist else M.get_sim(st,cache)
 if r["result"]!=sim.result:issues.append("RESULT")
 if abs(f(r["net_pct"])-sim.net_pct)>1e-5:issues.append("NET")
 fo=[]
 for j,x in enumerate(sim.fills):
  px=float(x.price);kind=str(x.kind);fo.append(f"{kind}:{x.qty_mult}@{px}")
  if j==len(sim.fills)-1:
   bd=bounds(st["symbol"],sim.exit_time)
   if bd and not(bd[0]*(1-1e-6)<=px<=bd[1]*(1+1e-6)):issues.append("FILL_OUTSIDE_MARKET")
  if kind=="TP20_FULL" and abs(px-st["entry_price"]*1.02)>st["entry_price"]*1e-6:issues.append("TP_PRICE")
  if kind=="PP12":
   q=ppref(st,sim.exit_time)
   if q and abs(px-q[0])>max(1e-10,abs(q[0])*1e-6):issues.append("PP_PRICE")
 issues=list(dict.fromkeys(issues))
 rows.append({"period":"HIST" if hist else "FWD","date":r["entry_time_kst"][:10],"symbol":r["symbol"],"entry_time_kst":r["entry_time_kst"],"setup_id":r["setup_id"],"entry_price":st["entry_price"],"recorded_result":r["result"],"recorded_net":r["net_pct"],"fresh_result":sim.result,"fresh_net":sim.net_pct,"fresh_exit_time_kst":sim.exit_time.astimezone(KST).strftime("%F %T"),"fills":" | ".join(fo),"audit_status":"PASS" if not issues else "MISMATCH","issues":";".join(issues)})
 if i%25==0 or issues:print(f"[{i}/{len(reg)}] {r['symbol']} {rows[-1]['audit_status']} {rows[-1]['issues']}",flush=True)
wr(D,rows);bad=[r for r in rows if r["audit_status"]=="MISMATCH"];wr(X,bad)
summ=[]
for p in ("HIST","FWD","ALL"):
 z=rows if p=="ALL" else [r for r in rows if r["period"]==p];b=[r for r in z if r["audit_status"]=="MISMATCH"];c=Counter(y for r in b for y in r["issues"].split(";") if y);summ.append({"period":p,"trades":len(z),"pass":len(z)-len(b),"mismatch":len(b),**dict(c)})
wr(S,summ);T.write_text("\n".join(["EXIT INTEGRITY AUDIT"]+[str(x) for x in summ]+[""]+[f"{r['date']} {r['symbol']} {r['issues']} {r['fills']}" for r in bad]),encoding="utf-8")
with zipfile.ZipFile(O,"w",zipfile.ZIP_DEFLATED) as z:
 for p in (D,X,S,T):z.write(p,arcname=p.name)
print("SUMMARY",summ);print("DONE",O)
