#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DCA37 PRE-STOP DEEP v2

Fixes:
1) EXCLUDES the partial 1m candle containing the entry timestamp.
   Only fully post-entry 1m candles are used for path/MFE/MAE features.
2) Economic target:
   BAD = waiting past the original STOP made the final policy PnL worse than
         taking the original STOP by more than 0.05%p.
   GOOD = waiting/DCA was better or not materially worse.
   Thus PENGU-like RISK classifications that benefited from waiting are GOOD.

TRAIN = historical observer cohort (33)
OOS   = new forward observer cohort (4)

No DB writes. No orders. No bot changes.
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

OUT_DETAIL=ROOT/"DCA37_PRESTOP_DEEP_V2_DETAIL.csv"
OUT_RULES=ROOT/"DCA37_PRESTOP_DEEP_V2_RULES.csv"
OUT_SUM=ROOT/"DCA37_PRESTOP_DEEP_V2_SUMMARY.txt"
OUT_ZIP=ROOT/"DCA37_PRESTOP_DEEP_V2_RESULTS.zip"

KST=timezone(timedelta(hours=9)); UTC=timezone.utc
FEE=.055
OBS_VOL=2.61; OBS_SLOPE=.00061
GREEN_LOW=-2.74314; GREEN_RSI=43.89902
BAD_EPS=-0.05

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
def one_exit_net(entry,exitp):
    return pct(exitp,entry)-(FEE+FEE*exitp/entry)
def dca_avg_net(entry,addp):
    av=(entry+addp)/2
    return -(FEE+FEE*addp/entry+FEE*(2*av/entry))

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
    entry=fv(x["entry_price"])
    addp=entry*(1+fv(x["RB_add_price_pct"])/100)
    stop_net=fv(x["current_stop_net_pct"])
    policy=dca_avg_net(entry,addp) if (green or safe) else one_exit_net(entry,addp)
    delta=policy-stop_net
    cohort.append({
        "symbol":x["symbol"],"source":"TRAIN","split":x["split"],
        "entry_time_kst":x["entry_time_kst"],"stop_time_kst":x["stop_time_kst"],
        "entry_price":entry,"original_stop_net":stop_net,"policy_net":policy,
        "wait_delta_vs_stop":delta,
        "econ_label":"BAD" if delta<BAD_EPS else "GOOD",
        "future_class":"GREEN" if green else "SAFE_GRAY" if safe else "RISK"
    })

for x in readcsv(CUR):
    if int(fv(x.get("observer"),0))!=1:continue
    ep=lookup.get((x["symbol"],str(x["entry_time_kst"])[:19]))
    if ep is None:raise SystemExit("entry missing "+x["symbol"])
    stop_net=fv(x["current_stop_net"]);policy=fv(x["policy_net"]);delta=policy-stop_net
    cohort.append({
        "symbol":x["symbol"],"source":"OOS","split":"FORWARD_0924_0925",
        "entry_time_kst":x["entry_time_kst"],"stop_time_kst":x["stop_time_kst"],
        "entry_price":ep,"original_stop_net":stop_net,"policy_net":policy,
        "wait_delta_vs_stop":delta,
        "econ_label":"BAD" if delta<BAD_EPS else "GOOD",
        "future_class":x["class"]
    })

print("cohort",len(cohort),
      "TRAIN G/B",sum(x["source"]=="TRAIN" and x["econ_label"]=="GOOD" for x in cohort),
      sum(x["source"]=="TRAIN" and x["econ_label"]=="BAD" for x in cohort),
      "OOS G/B",sum(x["source"]=="OOS" and x["econ_label"]=="GOOD" for x in cohort),
      sum(x["source"]=="OOS" and x["econ_label"]=="BAD" for x in cohort),flush=True)

def api1(sym,start,end):
    p={"category":"linear","symbol":sym,"interval":"1",
       "start":int(start.timestamp()*1000),"end":int(end.timestamp()*1000),"limit":1000}
    u="https://api.bybit.com/v5/market/kline?"+urllib.parse.urlencode(p);last=None
    for k in range(8):
        try:
            req=urllib.request.Request(u,headers={"User-Agent":"HJ-PRESTOP37-V2/1.0"})
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
def longest(flags):
    best=cur=0
    for q in flags:
        cur=cur+1 if q else 0;best=max(best,cur)
    return best
def rolling_return(path,n):
    vals=[]
    for i in range(len(path)-n+1):
        vals.append(pct(path[i+n-1]["c"],path[i]["o"]))
    return min(vals) if vals else None

def features(x):
    ent=dt(x["entry_time_kst"]).astimezone(UTC);stop=dt(x["stop_time_kst"]).astimezone(UTC)
    warm=api1(x["symbol"],ent-timedelta(minutes=90),stop+timedelta(minutes=1))

    ef=ent.replace(second=0,microsecond=0)
    # IMPORTANT: exclude partial entry candle when entry is inside the minute.
    start_full=ef if ent.second==0 and ent.microsecond==0 else ef+timedelta(minutes=1)
    sf=stop.replace(second=0,microsecond=0)
    path=[b for b in warm if b["t"]>=start_full and b["t"]<sf]
    if not path:raise RuntimeError("no fully post-entry bars")

    entry=x["entry_price"]
    close_ret=[pct(b["c"],entry) for b in path]
    hi_ret=[pct(b["h"],entry) for b in path];lo_ret=[pct(b["l"],entry) for b in path]
    one_ret=[pct(b["c"],b["o"]) for b in path]
    mfe=max(hi_ret);mae=min(lo_ret);imfe=hi_ret.index(mfe);imae=lo_ret.index(mae)

    running_low=1e99;running_high=-1e99;new_lows=0;new_highs=0;max_rb=0;max_dd=0
    for b in path:
        if b["l"]<running_low:running_low=b["l"];new_lows+=1
        if b["h"]>running_high:running_high=b["h"];new_highs+=1
        max_rb=max(max_rb,pct(b["c"],running_low))
        max_dd=min(max_dd,pct(b["c"],running_high))

    f5=[b for b in five(warm) if b["t"]+timedelta(minutes=5)<=sf]
    closes=[b["c"] for b in f5]
    e9=ema(closes[-60:],9) if closes else None;e20=ema(closes[-80:],20) if closes else None
    e20p=ema(closes[-81:-1],20) if len(closes)>=2 else None
    gap=(e9/e20-1)*100 if e9 and e20 else None
    slope=(e20/e20p-1)*100 if e20 and e20p else None
    rs=rsi(closes,14) if closes else None
    pre=[b for b in warm if b["t"]<sf]
    vv=[b["v"] for b in pre[-11:-1]] if pre else [];mv=mean(vv)
    vr=pre[-1]["v"]/mv if pre and mv else None

    out=dict(x)
    out.update({
      "duration_exact":(stop-ent).total_seconds()/60,
      "full_bars":len(path),
      "mfe":mfe,"mae":mae,"time_mfe_bar":imfe+1,"time_mae_bar":imae+1,
      "mfe3":max(hi_ret[:3]),"mfe5":max(hi_ret[:5]),"mfe10":max(hi_ret[:10]),
      "mae3":min(lo_ret[:3]),"mae5":min(lo_ret[:5]),"mae10":min(lo_ret[:10]),
      "last1":one_ret[-1],"last3":ret_window(path,3),"last5":ret_window(path,5),
      "max1m_drop":min(one_ret),"max3m_drop":rolling_return(path,3),"max5m_drop":rolling_return(path,5),
      "underwater_frac":sum(v<0 for v in close_ret)/len(close_ret),
      "deep_underwater_frac":sum(v<=-1 for v in close_ret)/len(close_ret),
      "downbar_frac":sum(v<0 for v in one_ret)/len(one_ret),
      "max_down_streak":longest([v<0 for v in one_ret]),
      "new_low_count":new_lows,"new_low_rate":new_lows/len(path),
      "new_high_count":new_highs,"new_high_rate":new_highs/len(path),
      "max_rebound_from_running_low":max_rb,"max_dd_from_running_high":max_dd,
      "avg_close_ret":mean(close_ret),"ret_std":stdev(one_ret),
      "stop_rsi5":rs,"stop_gap9_20":gap,"stop_ema20_slope":slope,"stop_vol_ratio10":vr,
      "mfe_to_absmae":mfe/abs(mae) if mae else 999,
      "mfe_then_stop_bars":len(path)-(imfe+1),
      "mae_near_stop_bars":len(path)-(imae+1),
      "tp2_path_anomaly":int(mfe>=2.0)
    })
    return out

details=[];errs=[]
for i,x in enumerate(cohort,1):
    try:
        r=features(x);details.append(r)
        print(f"[{i}/{len(cohort)}] {x['source']} {x['symbol']} {x['econ_label']} "
              f"delta={x['wait_delta_vs_stop']:+.3f} mfe={r['mfe']:.3f}",flush=True)
        time.sleep(.03)
    except Exception as e:
        errs.append({"symbol":x["symbol"],"source":x["source"],"error":repr(e)})
        print("[ERR]",x["symbol"],repr(e),flush=True)
writecsv(OUT_DETAIL,details)

# Fixed coarse grid. Thresholds are not learned from OOS.
grids={
"duration_exact":[3,5,10,15,20,25,30],
"full_bars":[2,3,5,8,10,15,20,25,30],
"mfe":[0,.05,.1,.2,.3,.5,.75,1,1.5],
"mae":[-1,-1.5,-2,-2.5,-3],
"time_mfe_bar":[1,2,3,5,10,15],"time_mae_bar":[1,2,3,5,10,15,20,25],
"mfe3":[0,.1,.25,.5,1],"mfe5":[0,.1,.25,.5,1],"mfe10":[0,.1,.25,.5,1],
"mae3":[-.5,-1,-1.5,-2,-2.5],"mae5":[-.5,-1,-1.5,-2,-2.5],"mae10":[-.5,-1,-1.5,-2,-2.5],
"last1":[-.25,-.5,-.75,-1,-1.25],"last3":[-.5,-1,-1.5,-2,-2.5],"last5":[-.5,-1,-1.5,-2,-2.5],
"max1m_drop":[-.25,-.5,-.75,-1,-1.25,-1.5],
"max3m_drop":[-.5,-1,-1.5,-2,-2.5,-3],"max5m_drop":[-.75,-1.25,-1.75,-2.5,-3.5],
"underwater_frac":[.5,.6,.7,.8,.9,1],"deep_underwater_frac":[.25,.5,.6,.7,.8,.9],
"downbar_frac":[.5,.6,.7,.8],"max_down_streak":[2,3,4,5,6],
"new_low_count":[2,3,4,5,6,8,10],"new_low_rate":[.1,.2,.3,.4,.5],
"new_high_count":[1,2,3,4,5,6],"new_high_rate":[.05,.1,.2,.3,.4],
"max_rebound_from_running_low":[.25,.5,.75,1,1.5,2],
"max_dd_from_running_high":[-.5,-1,-1.5,-2,-2.5,-3],
"avg_close_ret":[-.25,-.5,-.75,-1,-1.25],"ret_std":[.2,.4,.6,.8,1,1.5],
"stop_rsi5":[55,60,65,67,70],"stop_gap9_20":[.3,.5,.7,.9,1.2,1.5],
"stop_ema20_slope":[.005,.01,.02,.05,.075,.1],"stop_vol_ratio10":[.25,.5,.75,1,1.5,2,2.5],
"mfe_to_absmae":[.02,.05,.1,.2,.3,.5,1],
"mfe_then_stop_bars":[1,2,3,5,10,15,20],"mae_near_stop_bars":[0,1,2,3,5,10]
}

train=[r for r in details if r["source"]=="TRAIN"];oos=[r for r in details if r["source"]=="OOS"]
def hit(r,c):
    v=fv(r.get(c["f"]))
    return False if v is None else (v<=c["t"] if c["op"]=="<=" else v>=c["t"])
def ev(conds,ds):
    z=[r for r in ds if all(hit(r,c) for c in conds)]
    return (sum(r["econ_label"]=="GOOD" for r in z),
            sum(r["econ_label"]=="BAD" for r in z),
            sum(-r["wait_delta_vs_stop"] for r in z if r["econ_label"]=="BAD"),
            ";".join(r["symbol"] for r in z))

prim=[]
for ftr,ths in grids.items():
    for th in ths:
        for op in ("<=",">="):
            c={"f":ftr,"op":op,"t":th}
            a=ev([c],train)
            if a[1]>0:prim.append(c)

rules=[]
def add(kind,cs):
    a=ev(cs,train);b=ev(cs,oos);c=ev(cs,details)
    if a[0]==0 and a[1]>0:
        rules.append({"kind":kind,"rule":" AND ".join(f"{q['f']}{q['op']}{q['t']}" for q in cs),
          "train_good_cut":a[0],"train_bad_hit":a[1],"train_saving":round(a[2],6),"train_hits":a[3],
          "oos_good_cut":b[0],"oos_bad_hit":b[1],"oos_saving":round(b[2],6),"oos_hits":b[3],
          "all_good_cut":c[0],"all_bad_hit":c[1],"all_saving":round(c[2],6),"all_hits":c[3]})
for c in prim:add("SINGLE",[c])
for i,c1 in enumerate(prim):
    for c2 in prim[i+1:]:
        if c1["f"]==c2["f"]:continue
        add("PAIR",[c1,c2])

rules=sorted(rules,key=lambda r:(r["oos_good_cut"],-r["oos_bad_hit"],-r["train_bad_hit"],-r["all_saving"]))
writecsv(OUT_RULES,rules)

stable=[r for r in rules if r["train_good_cut"]==0 and r["oos_good_cut"]==0 and r["oos_bad_hit"]>0]
train2=[r for r in rules if r["train_good_cut"]==0 and r["train_bad_hit"]>=2]

lines=[
"DCA37 PRE-STOP DEEP V2",
"PARTIAL ENTRY CANDLE EXCLUDED",
"TARGET = ECONOMIC HARM OF WAITING PAST ORIGINAL STOP",
f"rows={len(details)} errors={len(errs)}",
f"TRAIN GOOD/BAD={sum(r['econ_label']=='GOOD' for r in train)}/{sum(r['econ_label']=='BAD' for r in train)}",
f"OOS GOOD/BAD={sum(r['econ_label']=='GOOD' for r in oos)}/{sum(r['econ_label']=='BAD' for r in oos)}",
"",
"BAD trades and preventable loss if original STOP had been kept:"
]
for r in details:
    if r["econ_label"]=="BAD":
        lines.append(f"  {r['source']} {r['symbol']}: wait_delta={r['wait_delta_vs_stop']:+.6f}, preventable={-r['wait_delta_vs_stop']:+.6f}")
lines+=["",f"TP2-path anomaly after partial-entry exclusion (MFE>=2 among STOP cohort): {sum(r['tp2_path_anomaly'] for r in details)}"]
for r in details:
    if r["tp2_path_anomaly"]:
        lines.append(f"  {r['source']} {r['symbol']} MFE={r['mfe']:.4f}")
lines+=["",f"Stable single/pair rules catching OOS BAD with 0 GOOD cuts in TRAIN+OOS: {len(stable)}"]
for r in stable[:30]:
    lines.append(f"  {r['kind']} {r['rule']} | train BAD={r['train_bad_hit']} [{r['train_hits']}] "
                 f"| OOS BAD={r['oos_bad_hit']} [{r['oos_hits']}] | saving={r['all_saving']:+.4f}")
lines+=["",f"TRAIN rules: 0 GOOD cuts and >=2 BAD hits: {len(train2)}"]
for r in train2[:25]:
    lines.append(f"  {r['kind']} {r['rule']} | train BAD={r['train_bad_hit']} [{r['train_hits']}] "
                 f"| OOS G/B={r['oos_good_cut']}/{r['oos_bad_hit']} [{r['oos_hits']}]")
if errs:lines+=["","ERRORS:"]+[str(e) for e in errs]
OUT_SUM.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in (OUT_DETAIL,OUT_RULES,OUT_SUM):z.write(p,arcname=p.name)

print("\n".join(lines),flush=True)
print("DONE:",OUT_ZIP,flush=True)
