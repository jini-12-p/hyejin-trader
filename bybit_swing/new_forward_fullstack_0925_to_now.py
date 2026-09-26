#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NEW FORWARD FULL-STACK validation
Window starts immediately after the prior source ended:
  2026-09-25 21:31:40 KST -> NOW

Stages:
  BASE
    V25 + SAFE/RELAX + existing MKT100 + portfolio scheduler
    TP2.0 / PP12 / Final4 + V27-1
  MARKET
    BASE + V25 market guard (rolling 2h V25 >=8 and abs4h avg >=0.40)
  MARKET_V22
    MARKET + V22 quality OR (OVEREXT / WEAK_REACCEL)
  FINAL_DCA
    MARKET_V22 + selective observer -> GREEN/SAFE_GRAY DCA100 / RISK rebound exit
  FINAL_DCA_FASTSLOW
    FINAL_DCA + FAST/SLOW original-STOP protection (research shadow)
      FAST: observer STOP and entry->STOP <=10m and stop RSI5 <=70
      SLOW: observer STOP and post-entry new-high rate <=5%
            and path low occurs within last 3 full 1m bars before STOP

Important:
- Raw scan is built through NOW.
- BASE/MARKET/MARKET_V22 "latest" evaluation ends source_end -3h.
- Apples-to-apples FULL-STACK evaluation ends source_end -9h,
  leaving up to 3h for base exit + 6h after STOP for DCA research.
- FAST/SLOW remains SHADOW research, not a live-bot change.
- No DB writes. No orders. No bot changes.
"""
from __future__ import annotations

import copy, csv, gzip, hashlib, importlib.util, io, json, math, os, sys, time
import urllib.parse, urllib.request, zipfile
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT=Path("/root/hyejin-trader/bybit_swing")
BASE_SCRIPT=ROOT/"forward_full_0924_0925.py"

KST=timezone(timedelta(hours=9))
UTC=timezone.utc
START_KST=datetime(2026,9,25,21,31,40,tzinfo=KST)
WARM_KST=START_KST-timedelta(hours=8)

SCAN=ROOT/"scan_FORWARD_20260925_213140_to_NOW_KST.csv"
SCANZIP=ROOT/"scan_FORWARD_20260925_213140_to_NOW_KST.zip"

PREFIX="FORWARD_0925_213140_TO_NOW"
OUT_SUM=ROOT/f"{PREFIX}_FULLSTACK_SUMMARY.csv"
OUT_DAILY=ROOT/f"{PREFIX}_FULLSTACK_DAILY.csv"
OUT_STOPS=ROOT/f"{PREFIX}_STOP_POLICY_DETAIL.csv"
OUT_TXT=ROOT/f"{PREFIX}_FULLSTACK_SUMMARY.txt"
OUT_ZIP=ROOT/f"{PREFIX}_FULLSTACK_RESULTS.zip"

OBS_VOL_MAX=2.61
OBS_SLOPE_MIN=0.00061
GREEN_LOW_MIN=-2.74314
GREEN_RSI_MIN=43.89902
SAFEGRAY_MAX_MIN=12.0
SAFEGRAY_PREV3_MIN=-1.0
REBOUND_PCT=1.5

FAST_MAX_MIN=10.0
FAST_RSI_MAX=70.0
SLOW_NEWHIGH_RATE_MAX=0.05
SLOW_MAE_NEAR_STOP_MAX_BARS=3

def load_module(name,path):
    spec=importlib.util.spec_from_file_location(name,str(path))
    if spec is None or spec.loader is None: raise RuntimeError(f"cannot load {path}")
    m=importlib.util.module_from_spec(spec);sys.modules[name]=m;spec.loader.exec_module(m)
    return m

if not BASE_SCRIPT.exists(): raise SystemExit(f"missing {BASE_SCRIPT}")
M=load_module("FWD_NEW_STACK_BASE",BASE_SCRIPT)
U=M.U

def fv(v,d=None):
    try:
        if v is None or str(v).strip()=="": return d
        x=float(v)
        return d if math.isnan(x) else x
    except: return d

def write_csv(path,rows):
    if not rows:
        path.write_text("",encoding="utf-8-sig");return
    keys=[];seen=set()
    for r in rows:
        for k in r:
            if k not in seen:seen.add(k);keys.append(k)
    with path.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=keys,extrasaction="ignore");w.writeheader();w.writerows(rows)

def is_scan_source(p):
    n=p.name.lower()
    return p.is_file() and (
        p.suffix.lower() in (".csv",".zip") or n.endswith(".csv.gz")
    ) and ("scan" in n)

def source_candidates():
    cutoff_epoch=(START_KST-timedelta(hours=3)).timestamp()
    dates=("20260925","20260926","20260927")
    out=[]
    def maybe(p):
        try:
            if not is_scan_source(p): return
            if p.resolve() in (SCAN.resolve(),SCANZIP.resolve()): return
            n=p.name
            if p.name=="scan_rejected.csv" or any(d in n for d in dates) or p.stat().st_mtime>=cutoff_epoch:
                out.append(p)
        except Exception:
            pass

    arc=ROOT/"scan_archive"
    if arc.exists():
        for p in arc.rglob("*"): maybe(p)
    for p in ROOT.glob("scan*"): maybe(p)

    # de-dupe + smaller/older first; current scan_rejected last
    uniq=list(dict.fromkeys(out))
    uniq.sort(key=lambda p:(p.name=="scan_rejected.csv", p.stat().st_mtime, str(p)))
    return uniq

def build_scan():
    sources=source_candidates()
    print(f"[SCAN] candidate source files={len(sources)}",flush=True)
    fields=[];fieldset=set();rows=[];seen=set()
    start_txt=START_KST.strftime("%Y-%m-%d %H:%M:%S")

    def consume_text_stream(fh,label):
        nonlocal fields,rows
        rd=csv.DictReader(fh)
        if not rd.fieldnames:return
        for fld in rd.fieldnames:
            if fld not in fieldset:
                fieldset.add(fld);fields.append(fld)
        tk=next((k for k in ("time_kst","timestamp_kst","datetime_kst","time","timestamp") if k in rd.fieldnames),rd.fieldnames[0])
        for r in rd:
            ts=str(r.get(tk,"")).strip().strip('"')
            if not ts or ts<start_txt:continue
            blob=json.dumps(r,sort_keys=True,ensure_ascii=False,separators=(",",":")).encode("utf-8","replace")
            h=hashlib.sha1(blob).digest()
            if h in seen:continue
            seen.add(h)
            r["__TS__"]=ts
            rows.append(r)

    for i,p in enumerate(sources,1):
        try:
            n=p.name.lower()
            if n.endswith(".csv.gz"):
                with gzip.open(p,"rt",encoding="utf-8-sig",errors="replace",newline="") as fh:
                    consume_text_stream(fh,p.name)
            elif p.suffix.lower()==".zip":
                with zipfile.ZipFile(p) as z:
                    for nm in z.namelist():
                        if not nm.lower().endswith(".csv"):continue
                        with z.open(nm) as raw:
                            with io.TextIOWrapper(raw,encoding="utf-8-sig",errors="replace",newline="") as fh:
                                consume_text_stream(fh,p.name+"::"+nm)
            elif p.suffix.lower()==".csv":
                with p.open("r",encoding="utf-8-sig",errors="replace",newline="") as fh:
                    consume_text_stream(fh,p.name)
            if i%20==0 or i==len(sources):
                print(f"[SCAN] {i}/{len(sources)} rows_after_cutoff={len(rows)}",flush=True)
        except Exception as e:
            print("[SCAN WARN]",p.name,repr(e),flush=True)

    if not rows: raise SystemExit("NO SCAN DATA AFTER prior source end")

    rows.sort(key=lambda r:r["__TS__"])
    first,last=rows[0]["__TS__"],rows[-1]["__TS__"]
    for r in rows:r.pop("__TS__",None)

    with SCAN.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore");w.writeheader();w.writerows(rows)
    with zipfile.ZipFile(SCANZIP,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(SCAN,arcname=SCAN.name)

    print("=== NEW SCAN BUILT ===",flush=True)
    print("first =",first,flush=True);print("last  =",last,flush=True)
    print("rows  =",len(rows),flush=True)
    return first,last

def setup_module(scan_path):
    M.SCAN=scan_path
    M.EVAL_START_KST=START_KST
    M.WARM_START_KST=WARM_KST

def get_sim(st,cache):
    sid=st["setup_id"]
    if sid not in cache:cache[sid]=U.simulate_base(st)
    return cache[sid]

def run_entry(name,setups,controls,rmeta,qmeta,warm,cache,end_kst,use_market,use_v22):
    sched=copy.deepcopy(warm);rows=[];acc=[]
    lo=START_KST.astimezone(UTC);hi=end_kst.astimezone(UTC)
    es=[s for s in setups if lo<=s["entry"]<=hi]
    for i,st in enumerate(es,1):
        sid=st["setup_id"];rm=rmeta[sid];qm=qmeta.get(sid);ef=U.entry_filter(st,controls)
        row=M.base_row(st,ef,rm,qm,name)
        if not ef["pass"]:rows.append(row);continue
        if use_market and rm["risk"]:
            row["block_reason"]="V25_MARKET_GUARD_2H8_ABS4H04";rows.append(row);continue
        if use_v22 and qm and qm["v22_quality_or"]:
            row["block_reason"]="V22_QUALITY_OR";rows.append(row);continue
        ok,why=sched.can_open(st["entry"],st["symbol"])
        if not ok:row["block_reason"]=why;rows.append(row);continue
        sim=get_sim(st,cache);M.attach_sim(row,sim)
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        rows.append(row);acc.append(row)
        if i%25==0 or i==len(es):
            print(f"[{name}] {i}/{len(es)} accepted={len(acc)}",flush=True)
    return rows,acc

def five_from_one(one):
    d={}
    for b in one:
        t=b["ts"].replace(minute=(b["ts"].minute//5)*5,second=0,microsecond=0)
        if t not in d:d[t]={"ts":t,"o":b["o"],"h":b["h"],"l":b["l"],"c":b["c"],"v":b["v"]}
        else:
            x=d[t];x["h"]=max(x["h"],b["h"]);x["l"]=min(x["l"],b["l"]);x["c"]=b["c"];x["v"]+=b["v"]
    return [d[k] for k in sorted(d)]

def mean_num(xs):
    z=[float(x) for x in xs if x is not None and math.isfinite(float(x))]
    return sum(z)/len(z) if z else None

def ema_num(vals,n):
    if not vals:return None
    a=2/(n+1);e=float(vals[0])
    for x in vals[1:]:e=a*float(x)+(1-a)*e
    return e

def rsi_num(vals,n=14):
    if len(vals)<n+1:return None
    ds=[b-a for a,b in zip(vals[-n-1:-1],vals[-n:])]
    g=sum(max(x,0) for x in ds)/n;l=sum(max(-x,0) for x in ds)/n
    return 100.0 if l==0 else 100-100/(1+g/l)

def pct(a,b):return (a/b-1)*100 if b else None

def features_before(one,checkpoint):
    cp=checkpoint.replace(second=0,microsecond=0)
    p1=[b for b in one if b["ts"]<cp]
    f5=[b for b in five_from_one(one) if b["ts"]+timedelta(minutes=5)<=cp]
    out={"volratio":None,"prev3":None,"slope":None,"rsi5":None}
    if p1:
        vv=[b["v"] for b in p1[-11:-1]];mv=mean_num(vv)
        out["volratio"]=p1[-1]["v"]/mv if mv not in (None,0) else None
        z=p1[-3:];out["prev3"]=pct(z[-1]["c"],z[0]["o"]) if z else None
    if f5:
        c=[b["c"] for b in f5]
        e=ema_num(c[-80:],20);ep=ema_num(c[-81:-1],20) if len(c)>=2 else None
        out["slope"]=(e/ep-1)*100 if e and ep else None
        out["rsi5"]=rsi_num(c,14)
    return out

def prestop_features(one,entry_ts,stop_ts,entry_price):
    ef=entry_ts.replace(second=0,microsecond=0)
    start=ef if entry_ts.second==0 and entry_ts.microsecond==0 else ef+timedelta(minutes=1)
    sf=stop_ts.replace(second=0,microsecond=0)
    path=[b for b in one if start<=b["ts"]<sf]
    if not path:return {"duration":(stop_ts-entry_ts).total_seconds()/60,"new_high_rate":None,"mae_near_stop":None}
    running=-1e300;nh=0;lo=1e300;ilo=0
    for i,b in enumerate(path):
        if b["h"]>running:running=b["h"];nh+=1
        if b["l"]<lo:lo=b["l"];ilo=i
    return {
        "duration":(stop_ts-entry_ts).total_seconds()/60,
        "new_high_rate":nh/len(path),
        "mae_near_stop":len(path)-(ilo+1),
        "path_bars":len(path),
    }

def one_exit_net(entry,exitp):
    return pct(exitp,entry)-(0.055+0.055*exitp/entry)

def dca_avg_net(entry,addp):
    av=(entry+addp)/2
    return -(0.055+0.055*addp/entry+0.055*(2*av/entry))

def dca_fail_net(entry,addp,exitp):
    gross=((exitp-entry)+(exitp-addp))/entry*100
    fees=0.055+0.055*addp/entry+0.055*(2*exitp/entry)
    return gross-fees

def make_sim(base,result,exit_time,net):
    s=copy.copy(base)
    s.result=result;s.exit_time=exit_time;s.net_pct=net
    return s

stop_cache={}
def stop_policy(st,base_sim,data_end_kst):
    sid=st["setup_id"]
    if sid in stop_cache:return stop_cache[sid]

    entry=float(st["entry_price"]);entry_ts=st["entry"];stop=base_sim.exit_time
    end=stop+timedelta(hours=6,minutes=5)
    # full-stack range is selected so this should be inside source availability.
    one=M.api_1m(st["symbol"],entry_ts-timedelta(minutes=90),end)
    sf=features_before(one,stop)
    obs=bool(sf["volratio"] is not None and sf["slope"] is not None and
             sf["volratio"]<=OBS_VOL_MAX and sf["slope"]>=OBS_SLOPE_MIN)

    pre=prestop_features(one,entry_ts,stop,entry)
    fast=bool(obs and pre["duration"]<=FAST_MAX_MIN and sf["rsi5"] is not None and sf["rsi5"]<=FAST_RSI_MAX)
    slow=bool(obs and pre["new_high_rate"] is not None and pre["mae_near_stop"] is not None and
              pre["new_high_rate"]<=SLOW_NEWHIGH_RATE_MAX and pre["mae_near_stop"]<=SLOW_MAE_NEAR_STOP_MAX_BARS)

    out={
        "observer":int(obs),"stop_vol_ratio":sf["volratio"],"stop_ema20_slope":sf["slope"],
        "stop_rsi5":sf["rsi5"],"pre_duration_min":pre["duration"],
        "pre_new_high_rate":pre["new_high_rate"],"pre_mae_near_stop_bars":pre["mae_near_stop"],
        "fast":int(fast),"slow":int(slow),"fs_hit":int(fast or slow),
        "base_stop_net":base_sim.net_pct,
        "dca_class":"NOT_OBSERVER","dca_net":base_sim.net_pct,"dca_exit":stop,
        "dca_result":"STOP",
    }
    if not obs:
        stop_cache[sid]=out;return out

    fl=stop.replace(second=0,microsecond=0)
    post=[b for b in one if fl+timedelta(minutes=1)<=b["ts"]<=fl+timedelta(hours=6)]
    low=None;lowts=None;trg=None;addp=None
    for b in post:
        if low is None or b["l"]<low:low=b["l"];lowts=b["ts"]
        if b["ts"]<=lowts:continue
        p=low*(1+REBOUND_PCT/100)
        if b["h"]>=p:trg=b["ts"];addp=p;break

    if trg is None:
        out.update({"dca_class":"NO_TRIGGER_6H","dca_result":"STOP_FALLBACK"})
        stop_cache[sid]=out;return out

    tf=features_before(one,trg)
    lp=pct(low,entry);mins=(trg-lowts).total_seconds()/60
    green=bool(lp>=GREEN_LOW_MIN and tf["rsi5"] is not None and tf["rsi5"]>=GREEN_RSI_MIN)
    safe=bool((not green) and mins<SAFEGRAY_MAX_MIN and tf["prev3"] is not None and tf["prev3"]>SAFEGRAY_PREV3_MIN)
    out.update({
        "trigger_time_kst":trg.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
        "swing_low_pct":lp,"low_to_trigger_min":mins,"rebound_rsi5":tf["rsi5"],
        "rebound_prev3":tf["prev3"],
    })

    if not (green or safe):
        net=one_exit_net(entry,addp)
        out.update({"dca_class":"RISK_NO_DCA","dca_net":net,"dca_exit":trg,"dca_result":"RISK_REBOUND_EXIT"})
        stop_cache[sid]=out;return out

    cls="GREEN_DCA" if green else "SAFEGRAY_DCA"
    av=(entry+addp)/2
    after=[b for b in post if b["ts"]>trg]
    for b in after:
        if b["l"]<=low:
            net=dca_fail_net(entry,addp,low)
            out.update({"dca_class":cls,"dca_net":net,"dca_exit":b["ts"],"dca_result":"DCA_FAIL_LOW_BREAK"})
            stop_cache[sid]=out;return out
        if b["h"]>=av:
            net=dca_avg_net(entry,addp)
            out.update({"dca_class":cls,"dca_net":net,"dca_exit":b["ts"],"dca_result":"DCA_RECOVER_AVG"})
            stop_cache[sid]=out;return out

    out.update({"dca_class":cls+"_UNRESOLVED","dca_result":"STOP_FALLBACK"})
    stop_cache[sid]=out;return out

def run_final(name,setups,controls,rmeta,qmeta,warm,cache,end_kst,use_fs):
    sched=copy.deepcopy(warm);rows=[];acc=[]
    lo=START_KST.astimezone(UTC);hi=end_kst.astimezone(UTC)
    es=[s for s in setups if lo<=s["entry"]<=hi]
    for i,st in enumerate(es,1):
        sid=st["setup_id"];rm=rmeta[sid];qm=qmeta.get(sid);ef=U.entry_filter(st,controls)
        row=M.base_row(st,ef,rm,qm,name)
        if not ef["pass"]:rows.append(row);continue
        if rm["risk"]:row["block_reason"]="V25_MARKET_GUARD_2H8_ABS4H04";rows.append(row);continue
        if qm and qm["v22_quality_or"]:row["block_reason"]="V22_QUALITY_OR";rows.append(row);continue
        ok,why=sched.can_open(st["entry"],st["symbol"])
        if not ok:row["block_reason"]=why;rows.append(row);continue

        base=get_sim(st,cache);sim=base;pol=None
        if base.result=="STOP":
            pol=stop_policy(st,base,end_kst)
            if use_fs and pol["fs_hit"]:
                sim=base
                pol["applied"]="ORIGINAL_STOP_FASTSLOW"
            else:
                sim=make_sim(base,pol["dca_result"],pol["dca_exit"],pol["dca_net"])
                pol["applied"]="SELECTIVE_DCA_POLICY"

        M.attach_sim(row,sim)
        row["base_result"]=base.result;row["base_net_pct"]=round(base.net_pct,6)
        if pol:
            for k,v in pol.items():
                row["stop_"+k]=v
        sched.add(st["entry"],st["symbol"],sim,base.result in ("STOP","LATE_FAILURE_EXIT"))
        rows.append(row);acc.append(row)

        if i%20==0 or i==len(es):
            print(f"[{name}] {i}/{len(es)} accepted={len(acc)}",flush=True)
    return rows,acc

def counts(acc):
    c=Counter(str(r["result"]) for r in acc)
    return dict(c)

def net(acc):return sum(float(r["net_pct"]) for r in acc)

def stat(name,acc,period,end_kst):
    c=counts(acc)
    return {
        "scenario":name,"period":period,"eval_end_kst":end_kst.isoformat(),
        "entries":len(acc),"net_pct":round(net(acc),6),
        "TP20":c.get("TP20_FULL",0),
        "STOP":c.get("STOP",0),
        "PP12":c.get("PROFIT_PROTECT_EXIT",0),
        "TIME":c.get("TIME_EXIT",0),
        "DCA_RECOVER":c.get("DCA_RECOVER_AVG",0),
        "DCA_FAIL":c.get("DCA_FAIL_LOW_BREAK",0),
        "RISK_REBOUND":c.get("RISK_REBOUND_EXIT",0),
    }

def main():
    build_scan()
    setup_module(SCAN)

    df,data_first,data_end,latest_end=M.load_scan_window()
    full_end=data_end-timedelta(hours=9)
    print("source =",data_first.isoformat(),"~",data_end.isoformat(),flush=True)
    print("latest entry evaluation end (-3h) =",latest_end.isoformat(),flush=True)
    print("full-stack complete end (-9h)    =",full_end.isoformat(),flush=True)
    if latest_end<=START_KST:raise SystemExit("not enough data for latest entry replay")
    if full_end<=START_KST:raise SystemExit("not enough data yet for 9h full-stack horizon")

    qmeta=M.load_exact_watch_features(df)
    print("exact WATCH setups =",len(qmeta),"quality flags =",sum(x["v22_quality_or"] for x in qmeta.values()),flush=True)

    # Configure through latest_end first; market/setup range is enough for both replays.
    M.configure_unified(latest_end)
    U.load_market_series()
    setups=U.load_setups();controls=U.load_control_proxy()
    rmeta=M.build_risk_meta(setups)
    cache={}
    warm=M.warm_base(setups,controls,cache)

    summary=[];daily=[]

    # Latest entry-layer replay to source_end-3h.
    latest_runs={}
    for name,um,uv in [
        ("BASE_LATEST",False,False),
        ("MARKET_LATEST",True,False),
        ("MARKET_V22_LATEST",True,True),
    ]:
        rows,acc=run_entry(name,setups,controls,rmeta,qmeta,warm,cache,latest_end,um,uv)
        latest_runs[name]=(rows,acc)
        write_csv(ROOT/f"{PREFIX}_{name}_TRADES.csv",rows)
        summary.append(stat(name,acc,"LATEST_3H_HORIZON",latest_end))

    # Apples-to-apples full-stack complete window.
    full_runs={}
    for name,um,uv in [
        ("BASE_FULL",False,False),
        ("MARKET_FULL",True,False),
        ("MARKET_V22_FULL",True,True),
    ]:
        rows,acc=run_entry(name,setups,controls,rmeta,qmeta,warm,cache,full_end,um,uv)
        full_runs[name]=(rows,acc)
        write_csv(ROOT/f"{PREFIX}_{name}_TRADES.csv",rows)
        summary.append(stat(name,acc,"FULLSTACK_9H_HORIZON",full_end))

    rows_dca,acc_dca=run_final("FINAL_DCA_FULL",setups,controls,rmeta,qmeta,warm,cache,full_end,False)
    rows_fs,acc_fs=run_final("FINAL_DCA_FASTSLOW_FULL",setups,controls,rmeta,qmeta,warm,cache,full_end,True)
    full_runs["FINAL_DCA_FULL"]=(rows_dca,acc_dca)
    full_runs["FINAL_DCA_FASTSLOW_FULL"]=(rows_fs,acc_fs)
    write_csv(ROOT/f"{PREFIX}_FINAL_DCA_FULL_TRADES.csv",rows_dca)
    write_csv(ROOT/f"{PREFIX}_FINAL_DCA_FASTSLOW_FULL_TRADES.csv",rows_fs)
    summary.append(stat("FINAL_DCA_FULL",acc_dca,"FULLSTACK_9H_HORIZON",full_end))
    summary.append(stat("FINAL_DCA_FASTSLOW_FULL",acc_fs,"FULLSTACK_9H_HORIZON",full_end))

    # STOP policy detail from cached STOPs.
    stoprows=[]
    for sid,p in stop_cache.items():
        q={"setup_id":sid,**p}
        dca_delta=fv(p.get("dca_net"),0)-fv(p.get("base_stop_net"),0)
        q["dca_delta_vs_original_stop"]=round(dca_delta,6)
        q["fastslow_economic_label"]="BAD_WAIT" if dca_delta<-0.05 else "GOOD_WAIT"
        stoprows.append(q)
    write_csv(OUT_STOPS,stoprows)

    # Daily, same FULL window only.
    dates=sorted({str(r["entry_time_kst"])[:10] for _,acc in full_runs.values() for r in acc})
    for d in dates:
        row={"date":d}
        for nm,(rr,acc) in full_runs.items():
            z=[r for r in acc if str(r["entry_time_kst"]).startswith(d)]
            row[nm+"_entries"]=len(z);row[nm+"_net"]=round(net(z),6)
        daily.append(row)

    write_csv(OUT_SUM,summary);write_csv(OUT_DAILY,daily)

    fs_hits=[x for x in stoprows if int(x.get("fs_hit",0))==1]
    fs_bad=[x for x in fs_hits if x["fastslow_economic_label"]=="BAD_WAIT"]
    fs_good=[x for x in fs_hits if x["fastslow_economic_label"]=="GOOD_WAIT"]

    lines=[
        "NEW FORWARD FULL-STACK VALIDATION",
        f"source={data_first.isoformat()} ~ {data_end.isoformat()}",
        f"start={START_KST.isoformat()}",
        f"latest_entry_eval_end={latest_end.isoformat()} (-3h)",
        f"fullstack_eval_end={full_end.isoformat()} (-9h)",
        "",
        "STAGES:",
        "BASE = V25+SAFE/RELAX+existing MKT100+portfolio + TP2/PP12/Final4/V27-1",
        "MARKET = BASE + V25 market protection",
        "MARKET_V22 = MARKET + V22 quality filter",
        "FINAL_DCA = MARKET_V22 + selective DCA/RISK rebound policy",
        "FINAL_DCA_FASTSLOW = FINAL_DCA + FAST/SLOW original-STOP shadow protection",
        "",
    ]
    for s in summary:
        lines.append(f"{s['scenario']}: {s['period']} entries={s['entries']} net={s['net_pct']:+.6f} "
                     f"TP={s['TP20']} STOP={s['STOP']} DCA_REC={s['DCA_RECOVER']} "
                     f"DCA_FAIL={s['DCA_FAIL']} RISK={s['RISK_REBOUND']}")
    lines += [
        "",
        f"STOPs evaluated in full-stack cache={len(stoprows)}",
        f"FAST/SLOW hits={len(fs_hits)} BAD_WAIT caught={len(fs_bad)} GOOD_WAIT overcuts={len(fs_good)}",
        "FAST/SLOW hit symbols="+",".join(str(x.get("setup_id")) for x in fs_hits),
        "",
        "DAILY FULLSTACK:"
    ]
    for r in daily:
        lines.append(str(r))
    OUT_TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")

    files=[OUT_SUM,OUT_DAILY,OUT_STOPS,OUT_TXT,SCANZIP]
    for nm in [
        "BASE_LATEST","MARKET_LATEST","MARKET_V22_LATEST",
        "BASE_FULL","MARKET_FULL","MARKET_V22_FULL",
        "FINAL_DCA_FULL","FINAL_DCA_FASTSLOW_FULL"
    ]:
        p=ROOT/f"{PREFIX}_{nm}_TRADES.csv"
        if p.exists():files.append(p)

    with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
        for p in files:
            if p.exists():z.write(p,arcname=p.name)

    print("\n".join(lines),flush=True)
    print("DONE:",OUT_ZIP,flush=True)
    print("RAW :",SCANZIP,flush=True)

if __name__=="__main__":
    main()
