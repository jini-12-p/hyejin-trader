#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MON/NEAR DCA failure separation audit.
Uses FINAL_0901_0928_STOP_DETAIL.csv plus historical DCA separation data.
Focus: values known AT the +1.5% rebound / DCA decision time.
No orders, no bot changes.
"""
import csv, zipfile, math
from pathlib import Path

ROOT=Path("/root/hyejin-trader/bybit_swing")
FINALZIP=ROOT/"FINAL_0901_0928_RESULTS.zip"
SEPZIP=ROOT/"DCA100_SEPARATION_RESULTS.zip"
OUT=ROOT/"MON_NEAR_DCA_SEPARATION.csv"
SUM=ROOT/"MON_NEAR_DCA_SEPARATION.txt"
ZIP=ROOT/"MON_NEAR_DCA_SEPARATION_RESULTS.zip"

def f(v,d=None):
    try:
        x=float(v); return d if math.isnan(x) else x
    except:return d
def rz(zp,needle):
    with zipfile.ZipFile(zp) as z:
        names=[n for n in z.namelist() if needle in n]
        if not names:return []
        import io
        with z.open(names[0]) as raw:return list(csv.DictReader(io.TextIOWrapper(raw,encoding="utf-8-sig")))
def wr(p,rows):
    if not rows:p.write_text("",encoding="utf-8-sig");return
    ks=[]
    for r in rows:
        for k in r:
            if k not in ks:ks.append(k)
    with p.open("w",encoding="utf-8-sig",newline="") as h:
        w=csv.DictWriter(h,fieldnames=ks,extrasaction="ignore");w.writeheader();w.writerows(rows)

final=rz(FINALZIP,"FINAL_0901_0928_STOP_DETAIL.csv")
sep=rz(SEPZIP,"DCA100_SEPARATION_DETAIL.csv") if SEPZIP.exists() else []

rows=[]
# Historical successful/failed allowed-DCA rows
for x in sep:
    vr=f(x.get("STOP_prev1m_vol_ratio10")); sl=f(x.get("STOP_ema20_slope"))
    if vr is None or sl is None or not(vr<=2.61 and sl>=.00061):continue
    low=f(x.get("RB_swing_low_pct")); rr=f(x.get("RB_rsi14_5m")); mins=f(x.get("RB_low_to_trigger_min")); p3=f(x.get("RB_prev3m_ret"))
    green=low is not None and rr is not None and low>=-2.74314 and rr>=43.89902
    safe=(not green and mins is not None and p3 is not None and mins<12 and p3>-1)
    if not(green or safe):continue
    rows.append({
      "symbol":x.get("symbol"),"entry_time_kst":x.get("entry_time_kst"),"source":"HIST",
      "result":"SUCCESS" if int(f(x.get("RB_SUCCESS_RECOVER6"),0))==1 else "FAIL",
      "class":"GREEN" if green else "SAFE_GRAY",
      "stop_vol_ratio":vr,"stop_ema20_slope":sl,
      "swing_low_pct":low,"rebound_rsi5":rr,"low_to_trigger_min":mins,"rebound_prev3m":p3,
      "add_price_pct":f(x.get("RB_add_price_pct")),
      "new_avg_pct":f(x.get("RB_new_avg_pct")),
      "mfe_after_add":f(x.get("RB_mfe_after_add_pct")),
      "mae_after_add":f(x.get("RB_mae_after_add_pct")),
    })

# Latest final STOP detail: focus on DCA recover/fail rows.
for x in final:
    cls=str(x.get("policy_class") or "")
    if cls not in ("DCA_RECOVER","DCA_FAIL"):continue
    # This compact final output may not contain all trigger features, but retains outcome.
    rows.append({
      "symbol":x.get("symbol"),"entry_time_kst":x.get("entry_time_kst"),"source":"LATEST",
      "result":"SUCCESS" if cls=="DCA_RECOVER" else "FAIL",
      "class":cls,
      "stop_vol_ratio":f(x.get("stop_vol_ratio")),
      "stop_ema20_slope":f(x.get("stop_ema20_slope")),
      "swing_low_pct":f(x.get("swing_low_pct")),
      "rebound_rsi5":f(x.get("rebound_rsi5")),
      "low_to_trigger_min":f(x.get("low_to_trigger_min")),
      "rebound_prev3m":f(x.get("rebound_prev3m")),
      "add_price_pct":f(x.get("add_price_pct")),
      "new_avg_pct":f(x.get("new_avg_pct")),
      "mfe_after_add":f(x.get("mfe_after_add")),
      "mae_after_add":f(x.get("mae_after_add")),
    })

# Always report MON/NEAR from final.
fails=[x for x in final if x.get("symbol") in ("MONUSDT","MON","NEARUSDT","NEAR")]
def vals(key, result=None):
    z=[f(r.get(key)) for r in rows if (result is None or r["result"]==result) and f(r.get(key)) is not None]
    return sorted(z)
def med(z):return z[len(z)//2] if z else None
def rng(z):return (min(z),max(z)) if z else (None,None)

features=["stop_vol_ratio","stop_ema20_slope","swing_low_pct","rebound_rsi5","low_to_trigger_min","rebound_prev3m","add_price_pct","new_avg_pct","mfe_after_add","mae_after_add"]
lines=[
"MON/NEAR DCA FAILURE SEPARATION",
f"allowed cohort rows={len(rows)} success={sum(r['result']=='SUCCESS' for r in rows)} fail={sum(r['result']=='FAIL' for r in rows)}",
"",
"[MON/NEAR final rows]"
]
for x in fails:lines.append(str(x))
lines+=["","[SUCCESS distribution]"]
for k in features:
    z=vals(k,"SUCCESS"); lines.append(f"{k}: n={len(z)} median={med(z)} range={rng(z)}")
lines+=["","[FAIL distribution]"]
for k in features:
    z=vals(k,"FAIL"); lines.append(f"{k}: n={len(z)} median={med(z)} range={rng(z)}")

# Search simple thresholds only when feature is present in both success and fail.
cands=[]
for k in features:
    s=vals(k,"SUCCESS"); q=vals(k,"FAIL")
    if not s or not q:continue
    thresholds=sorted(set(s+q))
    for t in thresholds:
        for op in ("<=",">="):
            sh=sum((v<=t if op=="<=" else v>=t) for v in s)
            fh=sum((v<=t if op=="<=" else v>=t) for v in q)
            if sh==0 and fh>0:cands.append((fh,k,op,t))
cands.sort(reverse=True)
lines+=["","[ZERO-SUCCESS-CUT simple candidates]"]
for a in cands[:30]:lines.append(f"fail_hit={a[0]} {a[1]} {a[2]} {a[3]}")

wr(OUT,rows);SUM.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(OUT,arcname=OUT.name);z.write(SUM,arcname=SUM.name)
print("\n".join(lines));print("DONE:",ZIP)
