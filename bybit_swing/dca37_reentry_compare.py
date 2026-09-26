#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare current HOLD+DCA vs STOP-THEN-REENTER on the 37 observer trades.
Uses only 29 ALLOW trades' post-trigger public Bybit 1m candles.
No DB writes. No orders. No bot changes.

Scenarios:
A_CURRENT     : current observer policy (hold; GREEN/SAFE -> DCA100; RISK -> exit on +1.5 rebound)
B1_VAVG       : original STOP, then 1x reentry on GREEN/SAFE, exit at virtual old+DCA average
B2_VAVG       : same with 2x reentry
B1_TP1        : STOP, 1x reentry, +1.0% from reentry
B2_TP1        : STOP, 2x reentry, +1.0%
B1_TP15       : STOP, 1x reentry, +1.5%
B2_TP15       : STOP, 2x reentry, +1.5%

For reentry scenarios:
- RISK: no reentry; original STOP stands.
- After reentry: conservative old swing-low break is checked before target in each next 1m candle.
- If neither target nor old-low break occurs within 6h after trigger, exit at final available close.
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

OUT_DETAIL=ROOT/"DCA37_REENTRY_COMPARE_DETAIL.csv"
OUT_SUM=ROOT/"DCA37_REENTRY_COMPARE_SUMMARY.txt"
OUT_ZIP=ROOT/"DCA37_REENTRY_COMPARE_RESULTS.zip"

KST=timezone(timedelta(hours=9)); UTC=timezone.utc
FEE=.055
VOL=2.61; SLOPE=.00061; LOWMIN=-2.74314; RSIMIN=43.89902

def f(v,d=None):
    try:
        x=float(v)
        return d if math.isnan(x) else x
    except: return d
def dt(s): return datetime.strptime(str(s)[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
def readcsv(p):
    with p.open(encoding="utf-8-sig",newline="") as h:return list(csv.DictReader(h))
def writecsv(p,rows):
    keys=[]
    for r in rows:
        for k in r:
            if k not in keys:keys.append(k)
    with p.open("w",encoding="utf-8-sig",newline="") as h:
        w=csv.DictWriter(h,fieldnames=keys,extrasaction="ignore");w.writeheader();w.writerows(rows)

def one_exit_net(entry,exitp):
    gross=(exitp/entry-1)*100
    return gross-(FEE+FEE*exitp/entry)

def dca_avg_net(entry,addp):
    av=(entry+addp)/2
    return -(FEE+FEE*addp/entry+FEE*(2*av/entry))

def reentry_leg_net(entry,addp,exitp,q):
    gross=q*(exitp-addp)/entry*100
    fees=q*(FEE*addp/entry+FEE*exitp/entry)
    return gross-fees

def api(sym,start,end):
    p={"category":"linear","symbol":sym,"interval":"1",
       "start":int(start.timestamp()*1000),"end":int(end.timestamp()*1000),"limit":1000}
    u="https://api.bybit.com/v5/market/kline?"+urllib.parse.urlencode(p)
    last=None
    for k in range(8):
        try:
            req=urllib.request.Request(u,headers={"User-Agent":"HJ-REENTRY-COMPARE/1.0"})
            with urllib.request.urlopen(req,timeout=25) as resp:j=json.loads(resp.read().decode())
            if int(j.get("retCode",-1))!=0:raise RuntimeError(j)
            z=[]
            for a in j.get("result",{}).get("list",[]):
                z.append({"t":datetime.fromtimestamp(int(a[0])/1000,tz=UTC),
                          "o":float(a[1]),"h":float(a[2]),"l":float(a[3]),"c":float(a[4])})
            return sorted(z,key=lambda x:x["t"])
        except Exception as e:
            last=e;time.sleep(min(6,.7*(k+1)))
    raise RuntimeError(last)

def simulate_target(one,trigger,low,entry,addp,target,q):
    after=[b for b in one if b["t"]>trigger and b["t"]<=trigger+timedelta(hours=6)]
    for b in after:
        # conservative: adverse first
        if b["l"]<=low:
            return "LOW_BREAK", low, b["t"], reentry_leg_net(entry,addp,low,q)
        if b["h"]>=target:
            return "TARGET", target, b["t"], reentry_leg_net(entry,addp,target,q)
    if after:
        ex=after[-1]["c"]
        return "H6_CLOSE",ex,after[-1]["t"],reentry_leg_net(entry,addp,ex,q)
    return "NO_DATA",addp,trigger,0.0

if not SEP.exists() and SEPZIP.exists():
    with zipfile.ZipFile(SEPZIP) as z:z.extract("DCA100_SEPARATION_DETAIL.csv",ROOT)
if not SEP.exists():raise SystemExit("missing DCA100_SEPARATION_DETAIL.csv")
if not CUR.exists():raise SystemExit("missing FORWARD_0924_0925_DCA7_DETAIL.csv")
if not TRADES.exists():raise SystemExit("missing current 4WAY trades")

lookup={(x["symbol"],str(x["entry_time_kst"])[:19]):f(x["entry_price"]) for x in readcsv(TRADES)}

cohort=[]
for x in readcsv(SEP):
    vr=f(x.get("STOP_prev1m_vol_ratio10"));sl=f(x.get("STOP_ema20_slope"))
    if vr is None or sl is None or not(vr<=VOL and sl>=SLOPE):continue
    green=f(x.get("RB_swing_low_pct"),-999)>=LOWMIN and f(x.get("RB_rsi14_5m"),-999)>=RSIMIN
    safe=(not green and f(x.get("RB_low_to_trigger_min"),999)<12 and f(x.get("RB_prev3m_ret"),-999)>-1)
    allow=green or safe
    entry=f(x["entry_price"]);addp=entry*(1+f(x["RB_add_price_pct"])/100)
    low=entry*(1+f(x["RB_swing_low_pct"])/100)
    current=dca_avg_net(entry,addp) if allow else one_exit_net(entry,addp)
    cohort.append({"symbol":x["symbol"],"split":x["split"],"source":"HIST",
        "entry_time_kst":x["entry_time_kst"],"stop_time_kst":x["stop_time_kst"],
        "trigger_time_kst":x["rebound_time_kst"],"entry":entry,"addp":addp,"low":low,
        "stop_net":f(x["current_stop_net_pct"]),"allow":allow,"current":current})

for x in readcsv(CUR):
    if int(f(x.get("observer"),0))!=1:continue
    allow=x["class"] in ("GREEN_DCA","SAFEGRAY_DCA")
    entry=lookup.get((x["symbol"],str(x["entry_time_kst"])[:19]))
    if entry is None:raise SystemExit("entry missing "+x["symbol"])
    low=entry*(1+f(x["swing_low_pct"])/100)
    addp=low*1.015 if allow or x["class"]=="RISK_NO_DCA_EXIT_REBOUND" else None
    cohort.append({"symbol":x["symbol"],"split":"FORWARD_0924_0925","source":"CURRENT",
        "entry_time_kst":x["entry_time_kst"],"stop_time_kst":x["stop_time_kst"],
        "trigger_time_kst":x["trigger_time_kst"],"entry":entry,"addp":addp,"low":low,
        "stop_net":f(x["current_stop_net"]),"allow":allow,"current":f(x["policy_net"])})

print("cohort",len(cohort),"ALLOW",sum(x["allow"] for x in cohort),"RISK",sum(not x["allow"] for x in cohort),flush=True)

rows=[]
for i,x in enumerate(cohort,1):
    base=dict(x)
    # RISK: stop and no reentry in all B scenarios
    if not x["allow"]:
        for k in ("B1_VAVG","B2_VAVG","B1_TP1","B2_TP1","B1_TP15","B2_TP15"):
            base[k]=x["stop_net"]
            base[k+"_status"]="NO_REENTRY"
        rows.append(base);continue

    trg=dt(x["trigger_time_kst"]).astimezone(UTC)
    one=api(x["symbol"],trg-timedelta(minutes=5),trg+timedelta(hours=6,minutes=5))
    av=(x["entry"]+x["addp"])/2
    targets={"VAVG":av,"TP1":x["addp"]*1.01,"TP15":x["addp"]*1.015}
    for label,tp in targets.items():
        for q in (1,2):
            st,ex,et,leg=simulate_target(one,trg,x["low"],x["entry"],x["addp"],tp,q)
            key=f"B{q}_{label}"
            base[key]=x["stop_net"]+leg
            base[key+"_status"]=st
            base[key+"_exit_time_kst"]=et.astimezone(KST).strftime("%F %T")
    rows.append(base)
    print(f"[{i}/{len(cohort)}] {x['symbol']} done",flush=True)
    time.sleep(.04)

writecsv(OUT_DETAIL,rows)

scens=["current","B1_VAVG","B2_VAVG","B1_TP1","B2_TP1","B1_TP15","B2_TP15"]
splits=[]
for s in sorted(set(r["split"] for r in rows))+["ALL"]:
    z=rows if s=="ALL" else [r for r in rows if r["split"]==s]
    d={"split":s,"n":len(z)}
    for q in scens:d[q]=sum(float(r[q]) for r in z)
    splits.append(d)

lines=["DCA37 STOP-THEN-REENTER COMPARISON",""]
for d in splits:
    lines.append(f"[{d['split']}] n={d['n']}")
    for q in scens:
        lines.append(f"  {q:10s} {d[q]:+.6f}")
    lines.append("")

allow=[r for r in rows if r["allow"]]
for key in ("B1_VAVG","B2_VAVG","B1_TP1","B2_TP1","B1_TP15","B2_TP15"):
    stats={}
    for r in allow:
        st=r.get(key+"_status","")
        stats[st]=stats.get(st,0)+1
    lines.append(f"{key} status: "+", ".join(f"{k}={v}" for k,v in sorted(stats.items())))

OUT_SUM.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(OUT_DETAIL,arcname=OUT_DETAIL.name);z.write(OUT_SUM,arcname=OUT_SUM.name)
print("\n".join(lines),flush=True)
print("DONE:",OUT_ZIP,flush=True)
