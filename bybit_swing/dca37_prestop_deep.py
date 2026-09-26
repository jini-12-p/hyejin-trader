#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Deep PRE-STOP path research for the 37 observer trades.
TRAIN: historical 33 observer trades only.
OOS: new forward 4 observer trades (TUT/AXS allow, XCN/XAI risk).
Pulls only entry->STOP 1m candles (+90m warmup) from public Bybit.
No DB writes, no orders, no bot changes.
"""
from __future__ import annotations
import csv,json,math,time,urllib.parse,urllib.request,zipfile
from datetime import datetime,timedelta,timezone
from pathlib import Path

ROOT=Path("/root/hyejin-trader/bybit_swing")
SEP=ROOT/"DCA100_SEPARATION_DETAIL.csv"
SEPZIP=ROOT/"DCA100_SEPARATION_RESULTS.zip"
CUR=ROOT/"FORWARD_0924_0925_DCA7_DETAIL.csv"
TRADES=ROOT/"FORWARD_0924_0925_4WAY_V25_PLUS_MARKET_PLUS_V22Q_TRADES.csv"

OUT_DETAIL=ROOT/"DCA37_PRESTOP_DEEP_DETAIL.csv"
OUT_RULES=ROOT/"DCA37_PRESTOP_DEEP_RULES.csv"
OUT_SUM=ROOT/"DCA37_PRESTOP_DEEP_SUMMARY.txt"
OUT_ZIP=ROOT/"DCA37_PRESTOP_DEEP_RESULTS.zip"

KST=timezone(timedelta(hours=9)); UTC=timezone.utc
OBS_VOL=2.61; OBS_SLOPE=.00061
GREEN_LOW=-2.74314; GREEN_RSI=43.89902

def fv(v,d=None):
    try:
        x=float(v)
        return d if math.isnan(x) else x
    except:return d
def dt(s): return datetime.strptime(str(s)[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
def pct(a,b): return (a/b-1)*100 if b else None
def mean(v):
    z=[float(x) for x in v if x is not None and math.isfinite(float(x))]
    return sum(z)/len(z) if z else None
def stdev(v):
    z=[float(x) for x in v if x is not None and math.isfinite(float(x))]
    if len(z)<2:return 0.0
    m=sum(z)/len(z);return math.sqrt(sum((x-m)**2 for x in z)/(len(z)-1))
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
def readcsv(p):
    with p.open(encoding="utf-8-sig",newline="") as h:return list(csv.DictReader(h))
def writecsv(p,rows):
    if not rows:p.write_text("",encoding="utf-8-sig");return
    ks=[];seen=set()
    for r in rows:
        for k in r:
            if k not in seen:seen.add(k);ks.append(k)
    with p.open("w",encoding="utf-8-sig",newline="") as h:
        w=csv.DictWriter(h,fieldnames=ks,extrasaction="ignore");w.writeheader();w.writerows(rows)

if not SEP.exists() and SEPZIP.exists():
    with zipfile.ZipFile(SEPZIP) as z:z.extract("DCA100_SEPARATION_DETAIL.csv",ROOT)
if not SEP.exists():raise SystemExit("missing DCA100_SEPARATION_DETAIL.csv")
if not CUR.exists():raise SystemExit("missing FORWARD_0924_0925_DCA7_DETAIL.csv")
if not TRADES.exists():raise SystemExit("missing current trades csv")

lookup={(x["symbol"],str(x["entry_time_kst"])[:19]):fv(x["entry_price"]) for x in readcsv(TRADES)}

cohort=[]
for x in readcsv(SEP):
    vr=fv(x.get("STOP_prev1m_vol_ratio10"));sl=fv(x.get("STOP_ema20_slope"))
    if vr is None or sl is None or not(vr<=OBS_VOL and sl>=OBS_SLOPE):continue
    green=fv(x.get("RB_swing_low_pct"),-999)>=GREEN_LOW and fv(x.get("RB_rsi14_5m"),-999)>=GREEN_RSI
    safe=(not green and fv(x.get("RB_low_to_trigger_min"),999)<12 and fv(x.get("RB_prev3m_ret"),-999)>-1)
    cohort.append({"symbol":x["symbol"],"source":"TRAIN","split":x["split"],
        "entry_time_kst":x["entry_time_kst"],"stop_time_kst":x["stop_time_kst"],
        "entry_price":fv(x["entry_price"]),"label":"ALLOW" if green or safe else "RISK",
        "future_class":"GREEN" if green else "SAFE_GRAY" if safe else "RISK"})

for x in readcsv(CUR):
    if int(fv(x.get("observer"),0))!=1:continue
    allow=x["class"] in ("GREEN_DCA","SAFEGRAY_DCA")
    ep=lookup.get((x["symbol"],str(x["entry_time_kst"])[:19]))
    if ep is None:raise SystemExit("entry missing "+x["symbol"])
    cohort.append({"symbol":x["symbol"],"source":"OOS","split":"FORWARD_0924_0925",
        "entry_time_kst":x["entry_time_kst"],"stop_time_kst":x["stop_time_kst"],
        "entry_price":ep,"label":"ALLOW" if allow else "RISK","future_class":x["class"]})

print("cohort",len(cohort),"TRAIN",sum(x["source"]=="TRAIN" for x in cohort),
      "OOS",sum(x["source"]=="OOS" for x in cohort),flush=True)

def api1(sym,start,end):
    p={"category":"linear","symbol":sym,"interval":"1",
       "start":int(start.timestamp()*1000),"end":int(end.timestamp()*1000),"limit":1000}
    u="https://api.bybit.com/v5/market/kline?"+urllib.parse.urlencode(p);last=None
    for k in range(8):
        try:
            req=urllib.request.Request(u,headers={"User-Agent":"HJ-PRESTOP37/1.0"})
            with urllib.request.urlopen(req,timeout=25) as resp:j=json.loads(resp.read().decode())
            if int(j.get("retCode",-1))!=0:raise RuntimeError(j)
            out=[]
            for a in j.get("result",{}).get("list",[]):
                out.append({"t":datetime.fromtimestamp(int(a[0])/1000,tz=UTC),
                            "o":float(a[1]),"h":float(a[2]),"l":float(a[3]),
                            "c":float(a[4]),"v":float(a[5])})
            return sorted(out,key=lambda z:z["t"])
        except Exception as e:
            last=e;time.sleep(min(6,.7*(k+1)))
    raise RuntimeError(last)

def five(one):
    d={}
    for b in one:
        t=b["t"].replace(minute=b["t"].minute//5*5,second=0,microsecond=0)
        if t not in d:d[t]={"t":t,"o":b["o"],"h":b["h"],"l":b["l"],"c":b["c"],"v":b["v"]}
        else:
            x=d[t];x["h"]=max(x["h"],b["h"]);x["l"]=min(x["l"],b["l"]);x["c"]=b["c"];x["v"]+=b["v"]
    return [d[k] for k in sorted(d)]

def ret_window(path,n):
    z=path[-n:] if len(path)>=n else path
    return pct(z[-1]["c"],z[0]["o"]) if z else None
def max_window_drop(rets,n):
    if len(rets)<n:return None
    return min(rets[i+n-1]-rets[i-1] if i>0 else rets[n-1] for i in range(len(rets)-n+1))
def longest_streak(flags):
    best=cur=0
    for q in flags:
        cur=cur+1 if q else 0;best=max(best,cur)
    return best

def features(x):
    ent=dt(x["entry_time_kst"]).astimezone(UTC);stop=dt(x["stop_time_kst"]).astimezone(UTC)
    warm=api1(x["symbol"],ent-timedelta(minutes=90),stop+timedelta(minutes=1))
    ef=ent.replace(second=0,microsecond=0);sf=stop.replace(second=0,microsecond=0)
    path=[b for b in warm if b["t"]>=ef and b["t"]<sf]
    if not path:raise RuntimeError("empty path")
    entry=x["entry_price"]
    close_ret=[pct(b["c"],entry) for b in path]
    hi_ret=[pct(b["h"],entry) for b in path];lo_ret=[pct(b["l"],entry) for b in path]
    one_ret=[pct(b["c"],b["o"]) for b in path]
    mfe=max(hi_ret);mae=min(lo_ret)
    imfe=hi_ret.index(mfe);imae=lo_ret.index(mae)
    running_low=1e99;running_high=-1e99;new_lows=0;new_highs=0;max_rb=0;max_dd=0
    recovery05=0;was_below1=False
    for b,cr in zip(path,close_ret):
        if b["l"]<running_low:running_low=b["l"];new_lows+=1
        if b["h"]>running_high:running_high=b["h"];new_highs+=1
        max_rb=max(max_rb,pct(b["c"],running_low))
        max_dd=min(max_dd,pct(b["c"],running_high))
        if cr<=-1:was_below1=True
        if was_below1 and cr>=-.5:recovery05+=1;was_below1=False
    def first_cross(vals,th,le=True):
        for i,v in enumerate(vals,1):
            if (v<=th if le else v>=th):return i
        return 999
    def firstn(vals,n,fn):
        z=vals[:min(n,len(vals))]
        return fn(z) if z else None

    f5=[b for b in five(warm) if b["t"]+timedelta(minutes=5)<=sf]
    closes=[b["c"] for b in f5]
    e9=ema(closes[-60:],9) if closes else None;e20=ema(closes[-80:],20) if closes else None
    e20p=ema(closes[-81:-1],20) if len(closes)>=2 else None
    gap=(e9/e20-1)*100 if e9 and e20 else None
    slope=(e20/e20p-1)*100 if e20 and e20p else None
    rs=rsi(closes,14) if closes else None

    pre=[b for b in warm if b["t"]<sf]
    vr=None
    if pre:
        vv=[b["v"] for b in pre[-11:-1]]
        m=mean(vv);vr=pre[-1]["v"]/m if m else None

    duration=(stop-ent).total_seconds()/60
    out=dict(x)
    out.update({
      "duration":duration,"bars":len(path),
      "mfe":mfe,"mae":mae,"time_mfe":imfe+1,"time_mae":imae+1,
      "mfe3":firstn(hi_ret,3,max),"mfe5":firstn(hi_ret,5,max),"mfe10":firstn(hi_ret,10,max),
      "mae3":firstn(lo_ret,3,min),"mae5":firstn(lo_ret,5,min),"mae10":firstn(lo_ret,10,min),
      "last1":one_ret[-1],"last3":ret_window(path,3),"last5":ret_window(path,5),
      "max1m_drop":min(one_ret),"max3m_drop":max_window_drop(close_ret,3),"max5m_drop":max_window_drop(close_ret,5),
      "underwater_frac":sum(v<0 for v in close_ret)/len(close_ret),
      "deep_underwater_frac":sum(v<=-1 for v in close_ret)/len(close_ret),
      "downbar_frac":sum(v<0 for v in one_ret)/len(one_ret),
      "max_down_streak":longest_streak([v<0 for v in one_ret]),
      "max_up_streak":longest_streak([v>0 for v in one_ret]),
      "new_low_count":new_lows,"new_low_rate":new_lows/len(path),
      "new_high_count":new_highs,"new_high_rate":new_highs/len(path),
      "max_rebound_from_running_low":max_rb,"max_dd_from_running_high":max_dd,
      "recovery_after_below1_count":recovery05,
      "first_neg05":first_cross(close_ret,-.5,True),"first_neg1":first_cross(close_ret,-1,True),
      "first_neg15":first_cross(close_ret,-1.5,True),"first_neg2":first_cross(close_ret,-2,True),
      "first_pos025":first_cross(close_ret,.25,False),"first_pos05":first_cross(close_ret,.5,False),
      "avg_close_ret":mean(close_ret),"ret_std":stdev(one_ret),
      "stop_rsi5":rs,"stop_gap9_20":gap,"stop_ema20_slope":slope,"stop_vol_ratio10":vr,
      "mfe_mae_span":mfe+abs(mae),"mfe_to_absmae":mfe/abs(mae) if mae else 999,
      "mfe_then_stop_min":max(0,len(path)-(imfe+1)),
      "mae_near_stop":len(path)-(imae+1)
    })
    return out

details=[];errs=[]
for i,x in enumerate(cohort,1):
    try:
        r=features(x);details.append(r)
        print(f"[{i}/{len(cohort)}] {x['source']} {x['symbol']} {x['label']} mfe={r['mfe']:.3f} mae={r['mae']:.3f}",flush=True)
        time.sleep(.03)
    except Exception as e:
        errs.append({"symbol":x["symbol"],"source":x["source"],"error":repr(e)})
        print("[ERR]",x["symbol"],repr(e),flush=True)

writecsv(OUT_DETAIL,details)

# Fixed, interpretable primitive grid. TRAIN only.
grids={
"duration":[3,5,10,15,20,25,30],
"mfe":[0,.05,.1,.2,.3,.5,.75,1,1.5,2],
"mae":[-1,-1.5,-2,-2.5,-3],
"time_mfe":[2,3,5,10,15,20],
"time_mae":[2,3,5,10,15,20,25,30],
"mfe3":[0,.1,.25,.5,1],"mfe5":[0,.1,.25,.5,1],"mfe10":[0,.1,.25,.5,1],
"mae3":[-.5,-1,-1.5,-2,-2.5],"mae5":[-.5,-1,-1.5,-2,-2.5],"mae10":[-.5,-1,-1.5,-2,-2.5],
"last1":[-.25,-.5,-.75,-1,-1.25],"last3":[-.5,-1,-1.5,-2,-2.5],"last5":[-.5,-1,-1.5,-2,-2.5],
"max1m_drop":[-.25,-.5,-.75,-1,-1.25,-1.5],"max3m_drop":[-.5,-1,-1.5,-2,-2.5,-3],
"max5m_drop":[-.75,-1.25,-1.75,-2.5,-3.5],
"underwater_frac":[.5,.6,.7,.8,.9,1],"deep_underwater_frac":[.25,.5,.6,.7,.8,.9],
"downbar_frac":[.5,.6,.7,.8],"max_down_streak":[2,3,4,5,6],
"new_low_count":[2,3,4,5,6,8,10],"new_low_rate":[.1,.2,.3,.4,.5],
"new_high_count":[1,2,3,4,5,6],"new_high_rate":[.05,.1,.2,.3,.4],
"max_rebound_from_running_low":[.25,.5,.75,1,1.5,2],
"max_dd_from_running_high":[-.5,-1,-1.5,-2,-2.5,-3],
"recovery_after_below1_count":[0,1,2,3],
"first_neg1":[2,3,5,10,15,20],"first_neg2":[2,3,5,10,15,20],
"first_pos05":[2,3,5,10,15,20,999],
"avg_close_ret":[-.25,-.5,-.75,-1,-1.25],"ret_std":[.2,.4,.6,.8,1,1.5],
"stop_rsi5":[55,60,65,67,70],"stop_gap9_20":[.3,.5,.7,.9,1.2,1.5],
"stop_ema20_slope":[.005,.01,.02,.05,.075,.1],"stop_vol_ratio10":[.25,.5,.75,1,1.5,2,2.5],
"mfe_to_absmae":[.02,.05,.1,.2,.3,.5,1],"mfe_then_stop_min":[1,2,3,5,10,15,20],
"mae_near_stop":[0,1,2,3,5,10]
}

train=[r for r in details if r["source"]=="TRAIN"];oos=[r for r in details if r["source"]=="OOS"]
def cond(r,c):
    v=fv(r.get(c["f"]))
    if v is None:return False
    return v<=c["t"] if c["op"]=="<=" else v>=c["t"]
def eval_rule(conds):
    def one(ds):
        hit=[r for r in ds if all(cond(r,c) for c in conds)]
        return (sum(r["label"]=="ALLOW" for r in hit),sum(r["label"]=="RISK" for r in hit),
                ";".join(r["symbol"] for r in hit))
    a=one(train);b=one(oos);c=one(details)
    return a,b,c

prims=[]
for feat,ths in grids.items():
    for th in ths:
        for op in ("<=",">="):
            c={"f":feat,"op":op,"t":th}
            a,b,allv=eval_rule([c])
            if a[1]>0:
                prims.append((c,a,b,allv))

rules=[]
def add_rule(kind,conds):
    a,b,c=eval_rule(conds)
    if a[0]==0 and a[1]>0:
        rules.append({"kind":kind,"rule":" AND ".join(f"{q['f']}{q['op']}{q['t']}" for q in conds),
          "train_allow_cut":a[0],"train_risk_hit":a[1],"train_hits":a[2],
          "oos_allow_cut":b[0],"oos_risk_hit":b[1],"oos_hits":b[2],
          "all_allow_cut":c[0],"all_risk_hit":c[1],"all_hits":c[2]})

for c,a,b,allv in prims:add_rule("SINGLE",[c])
for i in range(len(prims)):
    c1=prims[i][0]
    for j in range(i+1,len(prims)):
        c2=prims[j][0]
        if c1["f"]==c2["f"]:continue
        add_rule("PAIR",[c1,c2])

# Prespecified archetypes from checkpoint analysis (coarse thresholds, not fitted to OOS).
archetypes=[
("DEAD_FLAT",[{"f":"mfe","op":"<=","t":.2},{"f":"stop_ema20_slope","op":"<=","t":.01}]),
("HOT_BUT_FLAT",[{"f":"stop_rsi5","op":">=","t":66},{"f":"stop_ema20_slope","op":"<=","t":.075}]),
("FLASH_REVERSAL",[{"f":"duration","op":"<=","t":3},{"f":"mfe","op":">=","t":1.5},{"f":"last1","op":"<=","t":-1.0}])
]
for name,cs in archetypes:
    a,b,c=eval_rule(cs)
    rules.append({"kind":"ARCHETYPE_"+name,"rule":" AND ".join(f"{q['f']}{q['op']}{q['t']}" for q in cs),
      "train_allow_cut":a[0],"train_risk_hit":a[1],"train_hits":a[2],
      "oos_allow_cut":b[0],"oos_risk_hit":b[1],"oos_hits":b[2],
      "all_allow_cut":c[0],"all_risk_hit":c[1],"all_hits":c[2]})

rules=sorted(rules,key=lambda r:(r["train_allow_cut"],r["oos_allow_cut"],-r["oos_risk_hit"],-r["train_risk_hit"],-r["all_risk_hit"],r["kind"]))
writecsv(OUT_RULES,rules)

stable=[r for r in rules if r["train_allow_cut"]==0 and r["oos_allow_cut"]==0 and r["oos_risk_hit"]>0]
trainonly=[r for r in rules if r["train_allow_cut"]==0 and r["train_risk_hit"]>=2]

lines=[
"DCA37 PRE-STOP DEEP RESEARCH",
f"rows={len(details)} errors={len(errs)}",
f"TRAIN allow/risk={sum(r['label']=='ALLOW' for r in train)}/{sum(r['label']=='RISK' for r in train)}",
f"OOS allow/risk={sum(r['label']=='ALLOW' for r in oos)}/{sum(r['label']=='RISK' for r in oos)}",
"",
"ARCHETYPES:"
]
for r in [q for q in rules if q["kind"].startswith("ARCHETYPE")]:
    lines.append(f"{r['kind']}: train A/R={r['train_allow_cut']}/{r['train_risk_hit']} ({r['train_hits']}) | OOS A/R={r['oos_allow_cut']}/{r['oos_risk_hit']} ({r['oos_hits']})")
lines+=["",f"STABLE rules (train 0 allow cut + OOS 0 allow cut + catches OOS risk): {len(stable)}"]
for r in stable[:30]:
    lines.append(f"{r['kind']} {r['rule']} | train risk={r['train_risk_hit']} [{r['train_hits']}] | OOS risk={r['oos_risk_hit']} [{r['oos_hits']}]")
lines+=["",f"TRAIN zero-allow rules catching >=2 risk: {len(trainonly)}"]
for r in trainonly[:25]:
    lines.append(f"{r['kind']} {r['rule']} | train risk={r['train_risk_hit']} | OOS A/R={r['oos_allow_cut']}/{r['oos_risk_hit']} [{r['oos_hits']}]")

# Basic feature medians
focus=["duration","mfe","mae","time_mfe","time_mae","underwater_frac","deep_underwater_frac","downbar_frac",
       "new_low_rate","max_down_streak","max1m_drop","max3m_drop","last1","last3","last5",
       "max_rebound_from_running_low","max_dd_from_running_high","recovery_after_below1_count",
       "stop_rsi5","stop_gap9_20","stop_ema20_slope","mfe_to_absmae","mfe_then_stop_min"]
lines+=["","TRAIN medians ALLOW vs RISK:"]
for ftr in focus:
    aa=sorted(fv(r.get(ftr)) for r in train if fv(r.get(ftr)) is not None)
    rr=sorted(fv(r.get(ftr)) for r in train if fv(r.get(ftr)) is not None and r["label"]=="RISK")
    al=sorted(fv(r.get(ftr)) for r in train if fv(r.get(ftr)) is not None and r["label"]=="ALLOW")
    def med(z):return z[len(z)//2] if z else None
    lines.append(f"{ftr}: ALLOW={med(al)} RISK={med(rr)}")

if errs:lines+=["","ERRORS:"]+[str(e) for e in errs]
OUT_SUM.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in (OUT_DETAIL,OUT_RULES,OUT_SUM):z.write(p,arcname=p.name)
print("\n".join(lines),flush=True)
print("DONE:",OUT_ZIP,flush=True)
