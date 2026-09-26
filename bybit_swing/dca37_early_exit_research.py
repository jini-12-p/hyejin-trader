#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DCA observer early-exit research (lightweight)
- Historical observer cohort from DCA100_SEPARATION_DETAIL.csv: 33
- New forward observer cohort from FORWARD_0924_0925_DCA7_DETAIL.csv: 4
- Pulls only ~9h of public Bybit 1m candles per observer trade.
- Searches causal FAST_CRASH and STALE_WEAK early-exit candidates.
No DB writes. No orders. No bot changes.
"""
from __future__ import annotations
import csv, json, math, time, urllib.parse, urllib.request, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT=Path("/root/hyejin-trader/bybit_swing")
SEP=ROOT/"DCA100_SEPARATION_DETAIL.csv"
SEPZIP=ROOT/"DCA100_SEPARATION_RESULTS.zip"
CUR=ROOT/"FORWARD_0924_0925_DCA7_DETAIL.csv"
TRADES=ROOT/"FORWARD_0924_0925_4WAY_V25_PLUS_MARKET_PLUS_V22Q_TRADES.csv"

OUT_PATH=ROOT/"DCA37_EARLY_EXIT_PATHS.csv"
OUT_FAST=ROOT/"DCA37_FAST_CRASH_RULES.csv"
OUT_STALE=ROOT/"DCA37_STALE_WEAK_RULES.csv"
OUT_SUM=ROOT/"DCA37_EARLY_EXIT_SUMMARY.txt"
OUT_ZIP=ROOT/"DCA37_EARLY_EXIT_RESULTS.zip"

KST=timezone(timedelta(hours=9)); UTC=timezone.utc
FEE=0.055
OBS_VOL=2.61; OBS_SLOPE=0.00061
GREEN_LOW=-2.74314; GREEN_RSI=43.89902

def f(v,d=None):
    try:
        x=float(v)
        return d if math.isnan(x) else x
    except: return d
def dt_kst(s): return datetime.strptime(str(s)[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
def pct(a,b): return (a/b-1)*100 if b else None
def one_net_from_pct(g):
    return g-(FEE+FEE*(1+g/100.0))
def mean(a):
    z=[float(x) for x in a if x is not None]
    return sum(z)/len(z) if z else None
def read_csv(p):
    with p.open(encoding="utf-8-sig",newline="") as h:return list(csv.DictReader(h))
def write_csv(p,rows):
    if not rows: p.write_text("",encoding="utf-8-sig"); return
    keys=[]; seen=set()
    for r in rows:
        for k in r:
            if k not in seen: seen.add(k); keys.append(k)
    with p.open("w",encoding="utf-8-sig",newline="") as h:
        w=csv.DictWriter(h,fieldnames=keys,extrasaction="ignore");w.writeheader();w.writerows(rows)

if not SEP.exists() and SEPZIP.exists():
    with zipfile.ZipFile(SEPZIP) as z:
        z.extract("DCA100_SEPARATION_DETAIL.csv",ROOT)
if not SEP.exists(): raise SystemExit("missing DCA100_SEPARATION_DETAIL.csv")
if not CUR.exists(): raise SystemExit("missing FORWARD_0924_0925_DCA7_DETAIL.csv")
if not TRADES.exists(): raise SystemExit("missing current 4WAY trades csv")

# Current entry-price lookup
tr=read_csv(TRADES)
entry_lookup={(x["symbol"],str(x["entry_time_kst"])[:19]):f(x["entry_price"]) for x in tr}

cohort=[]
for x in read_csv(SEP):
    vr=f(x.get("STOP_prev1m_vol_ratio10")); sl=f(x.get("STOP_ema20_slope"))
    if vr is None or sl is None or not(vr<=OBS_VOL and sl>=OBS_SLOPE): continue
    green=(f(x.get("RB_swing_low_pct"),-999)>=GREEN_LOW and f(x.get("RB_rsi14_5m"),-999)>=GREEN_RSI)
    safe=(not green and f(x.get("RB_low_to_trigger_min"),999)<12 and f(x.get("RB_prev3m_ret"),-999)>-1)
    allow=bool(green or safe)
    policy_net=None
    if not allow:
        policy_net=one_net_from_pct(f(x.get("RB_add_price_pct")))
    cohort.append({
        "symbol":x["symbol"],"split":x["split"],"source":"HIST",
        "entry_time_kst":x["entry_time_kst"],"stop_time_kst":x["stop_time_kst"],
        "trigger_time_kst":x["rebound_time_kst"],"entry_price":f(x["entry_price"]),
        "current_stop_net":f(x["current_stop_net_pct"]),
        "stop_vol":vr,"stop_slope":sl,
        "policy_class":"ALLOW" if allow else "RISK",
        "policy_net":policy_net,
        "eventual_recover":int(f(x.get("RB_SUCCESS_RECOVER6"),0)==1),
    })

for x in read_csv(CUR):
    if int(f(x.get("observer"),0))!=1: continue
    cls=x["class"]
    allow=cls in ("GREEN_DCA","SAFEGRAY_DCA")
    ep=entry_lookup.get((x["symbol"],str(x["entry_time_kst"])[:19]))
    if ep is None: raise SystemExit("entry price missing "+x["symbol"])
    cohort.append({
        "symbol":x["symbol"],"split":"FORWARD_0924_0925","source":"CURRENT",
        "entry_time_kst":x["entry_time_kst"],"stop_time_kst":x["stop_time_kst"],
        "trigger_time_kst":x["trigger_time_kst"],"entry_price":ep,
        "current_stop_net":f(x["current_stop_net"]),
        "stop_vol":f(x["stop_vol_ratio"]),"stop_slope":f(x["stop_ema20_slope"]),
        "policy_class":"ALLOW" if allow else "RISK",
        "policy_net":None if allow else f(x["policy_net"]),
        "eventual_recover":1 if allow and int(f(x.get("dca_success"),0))==1 else 0,
    })

print("cohort",len(cohort),"ALLOW",sum(x["policy_class"]=="ALLOW" for x in cohort),
      "RISK",sum(x["policy_class"]=="RISK" for x in cohort),flush=True)

def api_1m(sym,start,end):
    p={"category":"linear","symbol":sym,"interval":"1",
       "start":int(start.timestamp()*1000),"end":int(end.timestamp()*1000),"limit":1000}
    u="https://api.bybit.com/v5/market/kline?"+urllib.parse.urlencode(p); last=None
    for k in range(8):
        try:
            req=urllib.request.Request(u,headers={"User-Agent":"HJ-DCA37-EARLY/1.0"})
            with urllib.request.urlopen(req,timeout=25) as resp:j=json.loads(resp.read().decode())
            if int(j.get("retCode",-1))!=0:raise RuntimeError(j)
            z=[]
            for a in j.get("result",{}).get("list",[]):
                z.append({"t":datetime.fromtimestamp(int(a[0])/1000,tz=UTC),
                          "o":float(a[1]),"h":float(a[2]),"l":float(a[3]),"c":float(a[4]),"v":float(a[5])})
            return sorted(z,key=lambda q:q["t"])
        except Exception as e:
            last=e;time.sleep(min(6,.7*(k+1)))
    raise RuntimeError(last)

paths={}
flat=[]
errors=[]
checkpoints={1,2,3,4,5,6,7,8,10,12,15,20,30,45,60,75,90,120,150,180,210}
for i,x in enumerate(cohort,1):
    try:
        stop=dt_kst(x["stop_time_kst"]).astimezone(UTC)
        trg=dt_kst(x["trigger_time_kst"]).astimezone(UTC)
        one=api_1m(x["symbol"],stop-timedelta(minutes=15),min(stop+timedelta(hours=6,minutes=5),trg+timedelta(minutes=5)))
        floor=stop.replace(second=0,microsecond=0)
        pre=[b for b in one if b["t"]<floor+timedelta(minutes=1)]
        post=[b for b in one if floor+timedelta(minutes=1)<=b["t"] and b["t"]+timedelta(minutes=1)<trg]
        runlow=None; lowt=None; rows=[]
        hist=pre[:]
        for b in post:
            hist.append(b)
            elapsed=(b["t"]-floor).total_seconds()/60
            if runlow is None or b["l"]<runlow: runlow=b["l"]; lowt=b["t"]
            closep=pct(b["c"],x["entry_price"]); lowp=pct(runlow,x["entry_price"])
            rb=pct(b["c"],runlow)
            z=hist[-3:]; ret3=pct(z[-1]["c"],z[0]["o"]) if z else None
            vols=[q["v"] for q in hist[-11:-1]]; mv=mean(vols)
            vr=b["v"]/mv if mv not in (None,0) else None
            since=(b["t"]-lowt).total_seconds()/60 if lowt else 0
            net=one_net_from_pct(closep)
            r={"symbol":x["symbol"],"split":x["split"],"source":x["source"],
               "policy_class":x["policy_class"],"eventual_recover":x["eventual_recover"],
               "elapsed_min":elapsed,"time_kst":b["t"].astimezone(KST).strftime("%F %T"),
               "close_pct":round(closep,6),"running_low_pct":round(lowp,6),
               "rebound_from_low_close":round(rb,6),"since_low_min":since,
               "ret3":None if ret3 is None else round(ret3,6),
               "vol_ratio10":None if vr is None else round(vr,6),
               "exit_net":round(net,6),"policy_net":x["policy_net"],
               "current_stop_net":x["current_stop_net"],"stop_vol":x["stop_vol"],"stop_slope":x["stop_slope"]}
            rows.append(r)
            if int(round(elapsed)) in checkpoints: flat.append(r)
        paths[(x["symbol"],x["stop_time_kst"])]=rows
        print(f"[PATH] {i}/{len(cohort)} {x['symbol']} rows={len(rows)} trigger={x['trigger_time_kst']}",flush=True)
        time.sleep(.04)
    except Exception as e:
        errors.append({"symbol":x["symbol"],"stop_time":x["stop_time_kst"],"error":repr(e)})
        print("[ERR]",x["symbol"],repr(e),flush=True)

def first_hit(x,fn):
    rr=paths.get((x["symbol"],x["stop_time_kst"]),[])
    for r in rr:
        if fn(r,x): return r
    return None

def score_rule(kind,params,fn):
    hits=[]
    for x in cohort:
        h=first_hit(x,fn)
        if h: hits.append((x,h))
    def met(sub):
        z=[a for a in hits if sub(a[0])]
        protect=sum(a[0]["policy_class"]=="ALLOW" for a in z)
        risk=[a for a in z if a[0]["policy_class"]=="RISK"]
        ben=worse=0;delta=0
        for x,h in risk:
            if x["policy_net"] is None: continue
            d=h["exit_net"]-x["policy_net"];delta+=d
            if d>0.05:ben+=1
            elif d<-0.05:worse+=1
        return protect,len(risk),ben,worse,delta,[a[0]["symbol"] for a in z]
    prior=met(lambda x:x["source"]=="HIST")
    curr=met(lambda x:x["source"]=="CURRENT")
    allm=met(lambda x:True)
    return {"kind":kind,"params":json.dumps(params,sort_keys=True),
            "prior_protected_cut":prior[0],"prior_risk_hit":prior[1],"prior_benefit":prior[2],"prior_worse":prior[3],"prior_delta":round(prior[4],6),
            "current_protected_cut":curr[0],"current_risk_hit":curr[1],"current_benefit":curr[2],"current_worse":curr[3],"current_delta":round(curr[4],6),
            "all_protected_cut":allm[0],"all_risk_hit":allm[1],"all_benefit":allm[2],"all_worse":allm[3],"all_delta":round(allm[4],6),
            "hit_symbols":";".join(allm[5])}

fast=[]
for N in [3,4,5,6,7,8,10]:
  for C in [-2.5,-3,-3.5,-4,-4.5,-5,-5.5,-6]:
    fast.append(score_rule("FAST_CLOSE",{"N":N,"close":C},
      lambda r,x,N=N,C=C:r["elapsed_min"]<=N and r["close_pct"]<=C))
    for R3 in [-1,-1.5,-2,-2.5,-3]:
      fast.append(score_rule("FAST_CLOSE_R3",{"N":N,"close":C,"ret3":R3},
        lambda r,x,N=N,C=C,R3=R3:r["elapsed_min"]<=N and r["close_pct"]<=C and r["ret3"] is not None and r["ret3"]<=R3))
for N in [4,5,6,7,8,10]:
  for L in [-4,-5,-6,-7]:
    for B in [.2,.4,.6,.8]:
      for R3 in [-1,-1.5,-2,-2.5,-3]:
        fast.append(score_rule("FAST_LOW_RB_R3",{"N":N,"low":L,"rb":B,"ret3":R3},
          lambda r,x,N=N,L=L,B=B,R3=R3:r["elapsed_min"]<=N and r["running_low_pct"]<=L and r["rebound_from_low_close"]<=B and r["ret3"] is not None and r["ret3"]<=R3))

stale=[]
for N in [30,45,60,75,90,120]:
  for M in [5,10,15,20]:
    for B in [.1,.2,.3,.5]:
      for C in [-1.5,-2,-2.5,-3,-3.5]:
        stale.append(score_rule("STALE_RB_CLOSE",{"N":N,"since":M,"rb":B,"close":C},
          lambda r,x,N=N,M=M,B=B,C=C:r["elapsed_min"]>=N and r["since_low_min"]>=M and r["rebound_from_low_close"]<=B and r["close_pct"]<=C))
  for C in [-1.5,-2,-2.5,-3,-3.5]:
    for R3 in [-.2,-.5,-1,-1.5]:
      stale.append(score_rule("STALE_CLOSE_R3",{"N":N,"close":C,"ret3":R3},
        lambda r,x,N=N,C=C,R3=R3:r["elapsed_min"]>=N and r["close_pct"]<=C and r["ret3"] is not None and r["ret3"]<=R3))
  for SN in [-1,-1.25,-1.5,-1.75,-2,-2.5,-3]:
    stale.append(score_rule("TIME_STOPNET",{"N":N,"stop_net":SN},
      lambda r,x,N=N,SN=SN:r["elapsed_min"]>=N and x["current_stop_net"]<=SN))

def order(rows):
    return sorted(rows,key=lambda q:(q["all_protected_cut"],q["all_worse"],-q["all_benefit"],-q["all_delta"],-q["all_risk_hit"]))

write_csv(OUT_PATH,flat)
write_csv(OUT_FAST,order(fast))
write_csv(OUT_STALE,order(stale))

fast_stable=[q for q in order(fast) if q["all_protected_cut"]==0 and q["all_worse"]==0 and q["all_benefit"]>0]
stale_stable=[q for q in order(stale) if q["all_protected_cut"]==0 and q["all_worse"]==0 and q["all_benefit"]>0]
prior_fast=[q for q in order(fast) if q["prior_protected_cut"]==0 and q["prior_worse"]==0 and q["prior_benefit"]>0]
prior_stale=[q for q in order(stale) if q["prior_protected_cut"]==0 and q["prior_worse"]==0 and q["prior_benefit"]>0]

allow_times=[];risk_times=[]
for x in cohort:
    a=dt_kst(x["stop_time_kst"]);b=dt_kst(x["trigger_time_kst"]);m=(b-a).total_seconds()/60
    (allow_times if x["policy_class"]=="ALLOW" else risk_times).append((x["symbol"],m,x["current_stop_net"]))
lines=[
"DCA37 EARLY EXIT RESEARCH",
f"cohort={len(cohort)} ALLOW={sum(x['policy_class']=='ALLOW' for x in cohort)} RISK={sum(x['policy_class']=='RISK' for x in cohort)} errors={len(errors)}",
f"ALLOW trigger max={max(m for _,m,_ in allow_times):.1f}m median={sorted(m for _,m,_ in allow_times)[len(allow_times)//2]:.1f}m",
f"RISK trigger min/max={min(m for _,m,_ in risk_times):.1f}/{max(m for _,m,_ in risk_times):.1f}m",
"RISK trigger times="+", ".join(f"{s}:{m:.0f}m" for s,m,_ in sorted(risk_times,key=lambda z:z[1])),
"",
f"FAST stable(all): {len(fast_stable)}",
]
for q in fast_stable[:12]:
    lines.append(f"  {q['kind']} {q['params']} benefit={q['all_benefit']} riskhit={q['all_risk_hit']} delta={q['all_delta']:+.4f} hits={q['hit_symbols']}")
lines.append("")
lines.append(f"STALE stable(all): {len(stale_stable)}")
for q in stale_stable[:12]:
    lines.append(f"  {q['kind']} {q['params']} benefit={q['all_benefit']} riskhit={q['all_risk_hit']} delta={q['all_delta']:+.4f} hits={q['hit_symbols']}")
lines+=["",f"FAST prior-stable candidates={len(prior_fast)} (inspect current OOS columns in CSV)",
        f"STALE prior-stable candidates={len(prior_stale)} (inspect current OOS columns in CSV)"]
if errors:
    lines+=["","ERRORS:"]+[str(x) for x in errors]
OUT_SUM.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in (OUT_PATH,OUT_FAST,OUT_STALE,OUT_SUM):z.write(p,arcname=p.name)

print("\n".join(lines),flush=True)
print("DONE:",OUT_ZIP,flush=True)
