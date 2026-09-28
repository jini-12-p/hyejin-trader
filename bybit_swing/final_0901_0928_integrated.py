#!/usr/bin/env python3
# FINAL 0901-0928 INTEGRATED REPLAY
# Reuses already-tested scripts/results and only computes the final combination.
import csv,copy,importlib.util,sys,zipfile,math
from pathlib import Path
from datetime import datetime,timedelta,timezone
from collections import deque,Counter
ROOT=Path("/root/hyejin-trader/bybit_swing"); KST=timezone(timedelta(hours=9)); UTC=timezone.utc
REG=ROOT/"MKT_REGIME_PERF_B_N9_P9_N6_TRADES.csv"
REGZIP=ROOT/"MKT_REGIME_EXACT_RESULTS.zip"
HSTOP=ROOT/"DCA100_SEPARATION_DETAIL.csv"; HSTOPZIP=ROOT/"DCA100_SEPARATION_RESULTS.zip"
FWD=ROOT/"forward_full_0924_0925.py"
OUTD=ROOT/"FINAL_0901_0928_DAILY.csv"; OUTS=ROOT/"FINAL_0901_0928_SUMMARY.txt"; OUTX=ROOT/"FINAL_0901_0928_RESULTS.zip"; OUTSTOP=ROOT/"FINAL_0901_0928_STOP_DETAIL.csv"
def f(v,d=None):
 try:
  x=float(v);return d if math.isnan(x) else x
 except:return d
def read(p):
 with p.open(encoding="utf-8-sig",newline="") as h:return list(csv.DictReader(h))
def write(p,rr):
 if not rr:p.write_text("",encoding="utf-8-sig");return
 ks=[]
 for r in rr:
  for k in r:
   if k not in ks:ks.append(k)
 with p.open("w",encoding="utf-8-sig",newline="") as h:w=csv.DictWriter(h,fieldnames=ks,extrasaction="ignore");w.writeheader();w.writerows(rr)
def extract(z,p,name):
 if not p.exists() and z.exists():
  with zipfile.ZipFile(z) as q:
   if name in q.namelist():q.extract(name,ROOT)
if not REG.exists():
 if not REGZIP.exists():raise SystemExit("missing MKT_REGIME_EXACT_RESULTS.zip")
 with zipfile.ZipFile(REGZIP) as z:
  cand=[n for n in z.namelist() if "PERF_B_N9_P9_N6" in n and n.endswith("_TRADES.csv")]
  if not cand:raise SystemExit("missing PERF_B_N9_P9_N6 trades in regime zip")
  z.extract(cand[0],ROOT);(ROOT/cand[0]).replace(REG)
extract(HSTOPZIP,HSTOP,"DCA100_SEPARATION_DETAIL.csv")
if not HSTOP.exists():raise SystemExit("missing DCA100_SEPARATION_DETAIL.csv")
rows=read(REG)
acc=[r for r in rows if int(f(r.get("accepted"),0))==1]
# Historical stop-policy lookup already validated.
hist={}
for x in read(HSTOP):
 vr=f(x.get("STOP_prev1m_vol_ratio10"));sl=f(x.get("STOP_ema20_slope"))
 if vr is None or sl is None or not(vr<=2.61 and sl>=.00061):continue
 green=f(x.get("RB_swing_low_pct"),-999)>=-2.74314 and f(x.get("RB_rsi14_5m"),-999)>=43.89902
 safe=(not green and f(x.get("RB_low_to_trigger_min"),999)<12 and f(x.get("RB_prev3m_ret"),-999)>-1)
 key=(x["symbol"],str(x["entry_time_kst"])[:19])
 if green or safe:
  # validated avg-exit net from same fee model
  ep=f(x["entry_price"]); ap=ep*(1+f(x["RB_add_price_pct"])/100); av=(ep+ap)/2
  net=-(.055+.055*ap/ep+.055*(2*av/ep)); cls="GREEN" if green else "SAFE_GRAY"
 else:
  ep=f(x["entry_price"]); ap=ep*(1+f(x["RB_add_price_pct"])/100)
  net=(ap/ep-1)*100-(.055+.055*ap/ep);cls="RISK"
 hist[key]=(net,cls)
# For 9/23+ use lightweight current STOP path calculator imported from existing script,
# but only for accepted STOPs missing historical lookup.
spec=importlib.util.spec_from_file_location("FM",str(FWD));M=importlib.util.module_from_spec(spec);sys.modules["FM"]=M;spec.loader.exec_module(M)
def mean(a):
 z=[float(x) for x in a if x is not None];return sum(z)/len(z) if z else None
def ema(v,n):
 a=2/(n+1);e=float(v[0])
 for x in v[1:]:e=a*float(x)+(1-a)*e
 return e
def rsi(v,n=14):
 if len(v)<n+1:return None
 ds=[b-a for a,b in zip(v[-n-1:-1],v[-n:])];g=sum(max(x,0) for x in ds)/n;l=sum(max(-x,0) for x in ds)/n
 return 100 if l==0 else 100-100/(1+g/l)
def pct(a,b):return (a/b-1)*100
def five(one):
 d={}
 for b in one:
  t=b["ts"].replace(minute=b["ts"].minute//5*5,second=0,microsecond=0)
  if t not in d:d[t]={"ts":t,"o":b["o"],"h":b["h"],"l":b["l"],"c":b["c"],"v":b["v"]}
  else:x=d[t];x["h"]=max(x["h"],b["h"]);x["l"]=min(x["l"],b["l"]);x["c"]=b["c"];x["v"]+=b["v"]
 return [d[k] for k in sorted(d)]
def feat(one,cp):
 c=cp.replace(second=0,microsecond=0);p=[x for x in one if x["ts"]<c];q=[x for x in five(one) if x["ts"]+timedelta(minutes=5)<=c];o={"vr":None,"p3":None,"sl":None,"rsi":None}
 if p:
  m=mean([x["v"] for x in p[-11:-1]]);o["vr"]=p[-1]["v"]/m if m else None;z=p[-3:];o["p3"]=pct(z[-1]["c"],z[0]["o"]) if z else None
 if q:
  cs=[x["c"] for x in q];e=ema(cs[-80:],20);ep=ema(cs[-81:-1],20) if len(cs)>=2 else None;o["sl"]=(e/ep-1)*100 if e and ep else None;o["rsi"]=rsi(cs)
 return o
_fwd_meta = None
def get_fwd_meta():
 global _fwd_meta
 if _fwd_meta is not None:return _fwd_meta
 scan=next((q for q in [ROOT/"scan_FORWARD_20260923_TO_NOW_MKTREGIME_KST.csv",ROOT/"scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.csv",ROOT/"scan_rejected.csv"] if q.exists()),None)
 if scan is None:raise RuntimeError("no forward scan found")
 M.SCAN=scan;M.EVAL_START_KST=datetime(2026,9,23,tzinfo=KST);M.WARM_START_KST=datetime(2026,9,22,8,tzinfo=KST)
 df,a,b,end=M.load_scan_window();M.configure_unified(end);M.U.load_market_series()
 setups={s["setup_id"]:s for s in M.U.load_setups()};cache={};out={}
 for sid,st in setups.items():
  out[sid]={"entry_price":st["entry_price"],"st":st}
 _fwd_meta=(out,cache)
 return _fwd_meta

def policy(r):
 key=(r["symbol"],str(r["entry_time_kst"])[:19])
 if key in hist:return hist[key][0],hist[key][1],0
 if str(r["entry_time_kst"])[:10] < "2026-09-23":return f(r["net_pct"]), "NO_OBSERVER",0
 ent=datetime.strptime(str(r["entry_time_kst"])[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).astimezone(UTC)
 meta,cache=get_fwd_meta();sid=str(r["setup_id"])
 if sid not in meta:return f(r["net_pct"]),"NO_SETUP_META",0
 st=meta[sid]["st"];ep=f(meta[sid]["entry_price"])
 sim=M.get_sim(st,cache)
 stop=sim.exit_time
 one=M.api_1m(r["symbol"],ent-timedelta(minutes=90),stop+timedelta(hours=6,minutes=5));sf=feat(one,stop)
 obs=sf["vr"] is not None and sf["sl"] is not None and sf["vr"]<=2.61 and sf["sl"]>=.00061
 if not obs:return f(r["net_pct"]),"NO_OBSERVER",0
 fl=stop.replace(second=0,microsecond=0);post=[b for b in one if fl+timedelta(minutes=1)<=b["ts"]<=fl+timedelta(hours=6)]
 lo=lt=tt=ap=None
 for b in post:
  if lo is None or b["l"]<lo:lo=b["l"];lt=b["ts"]
  if b["ts"]>lt and b["h"]>=lo*1.015:tt=b["ts"];ap=lo*1.015;break
 if tt is None:return f(r["net_pct"]),"NO_TRIGGER",0
 ff=feat(one,tt);lp=pct(lo,ep);mins=(tt-lt).total_seconds()/60
 green=lp>=-2.74314 and ff["rsi"] is not None and ff["rsi"]>=43.89902
 safe=(not green and mins<12 and ff["p3"] is not None and ff["p3"]>-1)
 # FAST/SLOW shadow
 ef=ent.replace(second=0,microsecond=0);start=ef if ent.second==0 else ef+timedelta(minutes=1);sfloor=stop.replace(second=0,microsecond=0);pp=[b for b in one if start<=b["ts"]<sfloor]
 nh=near=None
 if pp:
  hi=-1e99;n=0;low=1e99;ilo=0
  for i,b in enumerate(pp):
   if b["h"]>hi:hi=b["h"];n+=1
   if b["l"]<low:low=b["l"];ilo=i
  nh=n/len(pp);near=len(pp)-(ilo+1)
 fast=(stop-ent).total_seconds()/60<=10 and sf["rsi"] is not None and sf["rsi"]<=70
 slow=nh is not None and nh<=.05 and near<=3
 fs=int(fast or slow)
 if green or safe:
  av=(ep+ap)/2
  for b in post:
   if b["ts"]<=tt:continue
   if b["l"]<=lo:
    net=((lo-ep)+(lo-ap))/ep*100-(.055+.055*ap/ep+.055*2*lo/ep);return net,"DCA_FAIL",fs
   if b["h"]>=av:
    net=-(.055+.055*ap/ep+.055*2*av/ep);return net,"DCA_RECOVER",fs
  return f(r["net_pct"]),"DCA_UNRESOLVED",fs
 net=pct(ap,ep)-(.055+.055*ap/ep);return net,"RISK",fs
detail=[];daily={}
for i,r in enumerate(acc,1):
 base=f(r["net_pct"]);new=base;cls="NON_STOP";fs=0
 if r.get("result")=="STOP":new,cls,fs=policy(r)
 d=str(r["entry_time_kst"])[:10];daily.setdefault(d,{"date":d,"entries":0,"base":0.0,"current":0.0,"fs":0.0,"tp":0,"stop":0,"observer":0,"dca_recover":0,"dca_fail":0,"fastslow":0})
 x=daily[d];x["entries"]+=1;x["base"]+=base;x["current"]+=new;x["fs"]+=(base if fs else new)
 x["tp"]+=r.get("result")=="TP20_FULL";x["stop"]+=r.get("result")=="STOP";x["observer"]+=cls not in ("NON_STOP","NO_OBSERVER","NO_ENTRY_PRICE");x["dca_recover"]+=cls=="DCA_RECOVER";x["dca_fail"]+=cls=="DCA_FAIL";x["fastslow"]+=fs
 detail.append({"symbol":r["symbol"],"entry_time_kst":r["entry_time_kst"],"base_result":r["result"],"base_net":base,"policy_class":cls,"policy_net":new,"fastslow_shadow":fs,"fastslow_shadow_net":base if fs else new})
 if i%25==0:print("[FINAL]",i,"/",len(acc),flush=True)
dd=list(daily.values())
for x in dd:
 for k in ("base","current","fs"):x[k]=round(x[k],6)
write(OUTD,dd);write(OUTSTOP,[x for x in detail if x["base_result"]=="STOP"])
base=sum(x["base"] for x in dd);cur=sum(x["current"] for x in dd);fsn=sum(x["fs"] for x in dd)
lines=["FINAL 0901-0928 INTEGRATED","Entry = PERF market regime N9 +9/-6 + V22 LOCK0","Exit = TP2/PP12/Final4+V27-1 + selective DCA","FAST/SLOW = shadow only",f"accepted={len(acc)}",f"base_exit_net={base:+.6f}",f"current_selective_dca_net={cur:+.6f}",f"fastslow_shadow_net={fsn:+.6f}",f"delta_dca={cur-base:+.6f}",f"delta_fastslow_vs_current={fsn-cur:+.6f}",f"STOPs={sum(x['stop'] for x in dd)} observers={sum(x['observer'] for x in dd)} DCArecover={sum(x['dca_recover'] for x in dd)} DCAfail={sum(x['dca_fail'] for x in dd)} FASTSLOW={sum(x['fastslow'] for x in dd)}"]
OUTS.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(OUTX,"w",zipfile.ZIP_DEFLATED) as z:
 for p in (OUTD,OUTSTOP,OUTS):z.write(p,arcname=p.name)
print("\n".join(lines));print("DONE:",OUTX)
