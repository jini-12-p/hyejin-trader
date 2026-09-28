#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MARKET PERFORMANCE-REGIME exact causal portfolio replay + V22 LOCK0

Compares:
  BASE                    : no new V25 market guard, no V22 quality
  V22_ONLY                : market guard OFF, V22 quality LOCK0
  MARKET_ALWAYS_ON_V22    : current V25 market guard always active + V22 LOCK0

Performance-regime variants:
  market guard condition itself stays:
      rolling 2h V25 confirmed >= 8
      AND mean(abs(BTC4h),abs(ETH4h)) >= 0.40%
  but it blocks only while regime_state == ON.

Regime state uses ONLY COMPLETED shadow outcomes:
  start ON
  ON  -> OFF when sum(last N completed market-shadow trades) >= OFF threshold
  OFF -> ON  when sum(last N completed market-shadow trades) <= ON threshold

Primary:
  N=9, OFF at +8%p, re-ON at -5%p

Robustness:
  N=7/8/9, OFF +8/+10, re-ON -5/-6/-7.

Two independent shadow references:
  BASE_REF = market-condition trades actually accepted by BASE (no market, no V22)
  V22_REF  = market-condition trades actually accepted by V22_ONLY

The shadow reference is independent of the tested market ON/OFF state, avoiding
self-referential/future-leaking regime labels.

Periods reported:
  09/01-17
  09/18-22
  09/23-latest complete (-3h)

Forward raw scan is rebuilt with SQLite streaming de-duplication (no OOM).

No DB writes. No orders. No live bot changes.
"""
from __future__ import annotations

import copy,csv,gzip,hashlib,importlib.util,io,json,math,sqlite3,sys,time,zipfile
from collections import Counter,deque
from datetime import datetime,timedelta,timezone
from pathlib import Path

ROOT=Path("/root/hyejin-trader/bybit_swing")
U_PATH=ROOT/"unified_current_0901_0922.py"
FWD_BASE=ROOT/"forward_full_0924_0925.py"

KST=timezone(timedelta(hours=9)); UTC=timezone.utc
HIST_START=datetime(2026,9,1,0,0,0,tzinfo=KST)
HIST_END=datetime(2026,9,23,0,0,0,tzinfo=KST)
FWD_START=datetime(2026,9,23,0,0,0,tzinfo=KST)
FWD_WARM=datetime(2026,9,22,8,0,0,tzinfo=KST)

SCAN=ROOT/"scan_FORWARD_20260923_TO_NOW_MKTREGIME_KST.csv"
TMPDB=ROOT/".mktregime_scan_tmp.sqlite"

OUT_SUM=ROOT/"MKT_REGIME_EXACT_SUMMARY.csv"
OUT_DAY=ROOT/"MKT_REGIME_EXACT_DAILY.csv"
OUT_TRANS=ROOT/"MKT_REGIME_TRANSITIONS.csv"
OUT_BLOCKS=ROOT/"MKT_REGIME_BLOCKS.csv"
OUT_REF=ROOT/"MKT_REGIME_SHADOW_REFERENCE.csv"
OUT_TXT=ROOT/"MKT_REGIME_EXACT_SUMMARY.txt"
OUT_ZIP=ROOT/"MKT_REGIME_EXACT_RESULTS.zip"

V22_HIST=ROOT/"V22_QUALITY_RESCHEDULE_TRADES.csv"
BASE_CACHE=ROOT/"UNIFIED_CURRENT_0901_0922_TRADES.csv"

# label, type, ref_mode, N, off_threshold, on_threshold
SCENARIOS=[
    ("BASE","BASE",None,None,None,None),
    ("V22_ONLY","V22",None,None,None,None),
    ("MARKET_ALWAYS_ON_V22","ALWAYS","BASE_REF",None,None,None),

    ("PERF_B_N9_P8_N5","PERF","BASE_REF",9,8.0,-5.0),
    ("PERF_B_N7_P8_N5","PERF","BASE_REF",7,8.0,-5.0),
    ("PERF_B_N8_P8_N5","PERF","BASE_REF",8,8.0,-5.0),
    ("PERF_B_N9_P8_N6","PERF","BASE_REF",9,8.0,-6.0),
    ("PERF_B_N9_P8_N7","PERF","BASE_REF",9,8.0,-7.0),
    ("PERF_B_N9_P10_N5","PERF","BASE_REF",9,10.0,-5.0),
    ("PERF_B_N9_P9_N6","PERF","BASE_REF",9,9.0,-6.0),

    ("PERF_V_N9_P8_N5","PERF","V22_REF",9,8.0,-5.0),
]

def load_module(name,path):
    spec=importlib.util.spec_from_file_location(name,str(path))
    if spec is None or spec.loader is None:raise RuntimeError(f"cannot load {path}")
    m=importlib.util.module_from_spec(spec);sys.modules[name]=m;spec.loader.exec_module(m);return m

def fv(v,d=None):
    try:
        if v is None or str(v).strip()=="":return d
        x=float(v)
        return d if math.isnan(x) else x
    except:return d

def dt_utc(v):
    if not v:return None
    try:
        d=datetime.fromisoformat(str(v).replace("Z","+00:00"))
        if d.tzinfo is None:d=d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except:return None

def write_csv(p,rows):
    if not rows:p.write_text("",encoding="utf-8-sig");return
    keys=[];seen=set()
    for r in rows:
        for k in r:
            if k not in seen:seen.add(k);keys.append(k)
    with p.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=keys,extrasaction="ignore");w.writeheader();w.writerows(rows)

# -------------------------------------------------------------------
# Forward scan, streaming via SQLite to avoid memory blow-up
# -------------------------------------------------------------------
def source_candidates():
    toks=("20260922","20260923","20260924","20260925","20260926","20260927")
    out=[]
    def maybe(p):
        try:
            if not p.is_file():return
            n=p.name.lower()
            if "scan" not in n:return
            if not (p.suffix.lower() in (".csv",".zip") or n.endswith(".csv.gz")):return
            if p.resolve()==SCAN.resolve():return
            if p.name=="scan_rejected.csv" or any(t in p.name for t in toks):
                out.append(p)
        except:pass
    arc=ROOT/"scan_archive"
    if arc.exists():
        for p in arc.rglob("*"):maybe(p)
    for p in ROOT.glob("scan*"):maybe(p)
    return list(dict.fromkeys(out))

def build_scan():
    if TMPDB.exists():TMPDB.unlink()
    con=sqlite3.connect(TMPDB)
    con.execute("PRAGMA journal_mode=OFF");con.execute("PRAGMA synchronous=OFF")
    con.execute("CREATE TABLE rows(h TEXT PRIMARY KEY,ts TEXT NOT NULL,payload TEXT NOT NULL)")
    fields=[];fieldset=set();start_txt=FWD_START.strftime("%Y-%m-%d %H:%M:%S")
    sources=source_candidates()
    print("[SCAN] sources",len(sources),flush=True)

    def consume(fh):
        rd=csv.DictReader(fh)
        if not rd.fieldnames:return
        for fld in rd.fieldnames:
            if fld not in fieldset:fieldset.add(fld);fields.append(fld)
        tk=next((k for k in ("time_kst","timestamp_kst","datetime_kst","time","timestamp") if k in rd.fieldnames),rd.fieldnames[0])
        batch=[]
        for r in rd:
            ts=str(r.get(tk,"")).strip().strip('"')
            if not ts or ts<start_txt:continue
            payload=json.dumps(r,sort_keys=True,ensure_ascii=False,separators=(",",":"))
            h=hashlib.sha1(payload.encode("utf-8","replace")).hexdigest()
            batch.append((h,ts,payload))
            if len(batch)>=2000:
                con.executemany("INSERT OR IGNORE INTO rows VALUES(?,?,?)",batch);batch=[]
        if batch:con.executemany("INSERT OR IGNORE INTO rows VALUES(?,?,?)",batch)

    for i,p in enumerate(sources,1):
        try:
            n=p.name.lower()
            if n.endswith(".csv.gz"):
                with gzip.open(p,"rt",encoding="utf-8-sig",errors="replace",newline="") as fh:consume(fh)
            elif p.suffix.lower()==".zip":
                with zipfile.ZipFile(p) as z:
                    for nm in z.namelist():
                        if nm.lower().endswith(".csv"):
                            with z.open(nm) as raw:
                                with io.TextIOWrapper(raw,encoding="utf-8-sig",errors="replace",newline="") as fh:consume(fh)
            else:
                with p.open("r",encoding="utf-8-sig",errors="replace",newline="") as fh:consume(fh)
            if i%20==0 or i==len(sources):
                con.commit()
                print(f"[SCAN] {i}/{len(sources)} unique={con.execute('SELECT COUNT(*) FROM rows').fetchone()[0]}",flush=True)
        except Exception as e:
            print("[SCAN WARN]",p.name,repr(e),flush=True)
    con.commit()

    first=last=None;count=0;prev=None;maxgap=(0,None,None)
    with SCAN.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore");w.writeheader()
        for ts,payload in con.execute("SELECT ts,payload FROM rows ORDER BY ts"):
            w.writerow(json.loads(payload));count+=1
            if first is None:first=ts
            last=ts
            try:
                d=datetime.strptime(ts[:19],"%Y-%m-%d %H:%M:%S")
                if prev:
                    g=(d-prev).total_seconds()/60
                    if g>maxgap[0]:maxgap=(g,prev,d)
                prev=d
            except:pass
    con.close();TMPDB.unlink(missing_ok=True)
    print("SCAN",first,"~",last,"rows",count,"maxgap",maxgap[0],flush=True)
    return first,last,count,maxgap

# -------------------------------------------------------------------
# Performance switch
# -------------------------------------------------------------------
class PerfSwitch:
    def __init__(self,scenario,events,n,off_thr,on_thr):
        self.scenario=scenario
        self.events=sorted(events,key=lambda x:(x["exit_time"],x["setup_id"]))
        self.i=0;self.n=n;self.off_thr=off_thr;self.on_thr=on_thr
        self.q=deque(maxlen=n);self.state=True # ON
        self.transitions=[]

    def advance(self,now):
        while self.i<len(self.events) and self.events[self.i]["exit_time"]<=now:
            e=self.events[self.i];self.i+=1
            self.q.append(float(e["net_pct"]))
            if len(self.q)<self.n:continue
            s=sum(self.q);old=self.state
            if self.state and s>=self.off_thr:self.state=False
            elif (not self.state) and s<=self.on_thr:self.state=True
            if self.state!=old:
                self.transitions.append({
                    "scenario":self.scenario,
                    "time_kst":e["exit_time"].astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
                    "new_state":"ON" if self.state else "OFF",
                    "rolling_n":self.n,"rolling_sum":round(s,6),
                    "trigger_setup_id":e["setup_id"],"trigger_symbol":e["symbol"],
                    "trigger_net":e["net_pct"]
                })

    def snapshot(self,now):
        self.advance(now)
        return self.state,len(self.q),(sum(self.q) if self.q else None)

# -------------------------------------------------------------------
# Generic helpers
# -------------------------------------------------------------------
def period_of(kst):
    d=kst[:10]
    if d<="2026-09-17":return "0901_0917"
    if d<="2026-09-22":return "0918_0922"
    return "0923_NOW"

def summarize(name,rows,period):
    z=[r for r in rows if r["period"]==period and int(r.get("accepted",0))==1]
    c=Counter(str(r.get("result") or "") for r in z)
    allp=[r for r in rows if r["period"]==period]
    return {
        "period":period,"scenario":name,"entries":len(z),
        "net_pct":round(sum(float(r["net_pct"]) for r in z),6),
        "TP":c.get("TP20_FULL",0),"STOP":c.get("STOP",0),
        "PP12":c.get("PROFIT_PROTECT_EXIT",0),"TIME":c.get("TIME_EXIT",0),
        "LATE":c.get("LATE_FAILURE_EXIT",0),
        "market_blocks":sum(r.get("block_reason")=="MARKET_GUARD_ACTIVE" for r in allp),
        "market_block_would_net":round(sum(fv(r.get("would_net"),0) for r in allp if r.get("block_reason")=="MARKET_GUARD_ACTIVE"),6),
        "v22_blocks":sum(r.get("block_reason")=="V22_QUALITY_OR" for r in allp),
        "regime_on_candidates":sum(int(r.get("regime_on",0)) for r in allp),
        "regime_off_candidates":sum(int(r.get("regime_off",0)) for r in allp),
    }

# -------------------------------------------------------------------
# HIST context
# -------------------------------------------------------------------
def prepare_hist():
    U=load_module("U_MKTREG_HIST",U_PATH)
    U.START_KST=HIST_START;U.END_KST=HIST_END
    U.START_UTC=HIST_START.astimezone(UTC);U.END_UTC=HIST_END.astimezone(UTC)
    U.EXPECTED_V25=-1;U.CACHE_DIR=ROOT/".mktreg_hist_cache";U.CACHE_DIR.mkdir(exist_ok=True)
    U.KC=U.KlineCache();U._market_1m={};U._market_recompute_count=0
    U.load_market_series()
    setups=U.load_setups();controls=U.load_control_proxy()
    setups=[s for s in setups if U.START_UTC<=s["entry"]<U.END_UTC]

    import pandas as pd
    if not V22_HIST.exists():raise SystemExit(f"missing {V22_HIST}")
    df=pd.read_csv(V22_HIST,low_memory=False)
    df=df[df["dataset"].astype(str)=="HIST"]
    v22={}
    for r in df.to_dict("records"):
        sid=str(r.get("setup_id") or "")
        if sid and sid not in v22:
            v22[sid]=bool(int(fv(r.get("overext"),0) or 0) or int(fv(r.get("weak_reaccel"),0) or 0))

    q=deque();market={}
    for st in setups:
        t=st["entry"]
        while q and q[0]<t-timedelta(hours=2):q.popleft()
        q.append(t)
        d=st["details"];U.fill_missing_market(d,t)
        b=U.first_f(d,"btc_4h_change_pct","btc_4h");e=U.first_f(d,"eth_4h_change_pct","eth_4h")
        av=None if b is None or e is None else (abs(b)+abs(e))/2
        market[st["setup_id"]]=bool(len(q)>=8 and av is not None and av>=.40)

    cache={}
    if BASE_CACHE.exists():
        bdf=pd.read_csv(BASE_CACHE,low_memory=False)
        for r in bdf.to_dict("records"):
            if int(fv(r.get("accepted"),0) or 0)!=1:continue
            et=dt_utc(r.get("exit_ts_utc"))
            if et is None:
                try:et=datetime.strptime(str(r.get("exit_time_kst"))[:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).astimezone(UTC)
                except:continue
            ep=fv(r.get("entry_price"),0) or 0
            cache[str(r["setup_id"])]=U.SimResult(
                result=str(r.get("result") or ""),exit_time=et,
                terminal_price=fv(r.get("terminal_price"),ep) or ep,fills=[],
                gross_pct=fv(r.get("gross_pct"),0) or 0,fee_pct=fv(r.get("fee_pct"),0) or 0,
                net_pct=fv(r.get("net_pct"),0) or 0,mfe_pct=fv(r.get("mfe_pct"),0) or 0,
                mae_pct=fv(r.get("mae_pct"),0) or 0,stop_stage=str(r.get("stop_stage") or ""),
                detail="CACHE",data_error=str(r.get("data_error") or "")
            )
    def getsim(st):
        sid=st["setup_id"]
        if sid not in cache:cache[sid]=U.simulate_base(st)
        return cache[sid]
    return U,setups,controls,v22,market,getsim

# -------------------------------------------------------------------
# FORWARD context
# -------------------------------------------------------------------
def prepare_forward():
    M=load_module("M_MKTREG_FWD",FWD_BASE);U=M.U
    M.SCAN=SCAN;M.EVAL_START_KST=FWD_START;M.WARM_START_KST=FWD_WARM
    df,data_first,data_end,eval_end=M.load_scan_window()
    if eval_end<=FWD_START:raise SystemExit("not enough forward horizon")
    qmeta=M.load_exact_watch_features(df)
    M.configure_unified(eval_end);U.load_market_series()
    setups=U.load_setups();controls=U.load_control_proxy();rmeta=M.build_risk_meta(setups)
    cache={};warm=M.warm_base(setups,controls,cache)
    lo=FWD_START.astimezone(UTC);hi=eval_end.astimezone(UTC)
    setups=[s for s in setups if lo<=s["entry"]<=hi]
    def getsim(st):return M.get_sim(st,cache)
    v22={s["setup_id"]:bool(qmeta.get(s["setup_id"]) and qmeta[s["setup_id"]]["v22_quality_or"]) for s in setups}
    market={s["setup_id"]:bool(rmeta[s["setup_id"]]["risk"]) for s in setups}
    return M,U,setups,controls,v22,market,getsim,warm,data_first,data_end,eval_end,qmeta

# -------------------------------------------------------------------
# Reference streams independent of tested market state
# -------------------------------------------------------------------
def build_ref(U,setups,controls,v22,market,getsim,warm,use_v22,dataset):
    sched=copy.deepcopy(warm) if warm is not None else U.Scheduler()
    events=[];rows=[]
    for st in setups:
        sid=st["setup_id"];ef=U.entry_filter(st,controls)
        if not ef["pass"]:continue
        if use_v22 and v22.get(sid,False):continue
        ok,why=sched.can_open(st["entry"],st["symbol"])
        if not ok:continue
        sim=getsim(st)
        sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        if market.get(sid,False):
            e={"dataset":dataset,"ref_mode":"V22_REF" if use_v22 else "BASE_REF",
               "setup_id":sid,"symbol":st["symbol"],"entry_time":st["entry"],
               "exit_time":sim.exit_time,"net_pct":float(sim.net_pct)}
            events.append(e);rows.append({
                "dataset":dataset,"ref_mode":e["ref_mode"],"setup_id":sid,"symbol":st["symbol"],
                "entry_time_kst":st["entry"].astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "exit_time_kst":sim.exit_time.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
                "net_pct":round(sim.net_pct,6)
            })
    return events,rows

# -------------------------------------------------------------------
# Scenario replay
# -------------------------------------------------------------------
def replay_dataset(U,setups,controls,v22,market,getsim,warm,scenario,kind,switch):
    sched=copy.deepcopy(warm) if warm is not None else U.Scheduler()
    rows=[]
    for st in setups:
        sid=st["setup_id"];t=st["entry"];ef=U.entry_filter(st,controls)
        reg_on=True;reg_n=0;reg_sum=None
        if switch is not None:
            reg_on,reg_n,reg_sum=switch.snapshot(t)

        row={"scenario":scenario,"setup_id":sid,"symbol":st["symbol"],
             "entry_time_kst":t.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S"),
             "period":period_of(t.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")),
             "accepted":0,"block_reason":ef["reason"],
             "market_shadow":int(market.get(sid,False)),"v22q":int(v22.get(sid,False)),
             "regime_on":int(bool(reg_on)),"regime_off":int(not reg_on),
             "regime_shadow_n":reg_n,"regime_shadow_sum":"" if reg_sum is None else round(reg_sum,6),
             "result":"","net_pct":"","would_net":""}
        if not ef["pass"]:rows.append(row);continue

        # market guard before V22/scheduler, matching current 4-way replay
        block_market=False
        if kind=="ALWAYS" and market.get(sid,False):block_market=True
        elif kind=="PERF" and reg_on and market.get(sid,False):block_market=True
        if block_market:
            sim=getsim(st)
            row["block_reason"]="MARKET_GUARD_ACTIVE";row["would_net"]=round(sim.net_pct,6)
            rows.append(row);continue

        if kind!="BASE" and v22.get(sid,False):
            sim=getsim(st)
            row["block_reason"]="V22_QUALITY_OR";row["would_net"]=round(sim.net_pct,6)
            rows.append(row);continue

        ok,why=sched.can_open(t,st["symbol"])
        if not ok:row["block_reason"]=why;rows.append(row);continue
        sim=getsim(st)
        row.update({"accepted":1,"block_reason":"","result":sim.result,"net_pct":round(sim.net_pct,6)})
        sched.add(t,st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
        rows.append(row)
    return rows

def main():
    first,last,n,maxgap=build_scan()
    UH,hset,hctl,hv22,hmarket,hget=prepare_hist()
    M,UF,fset,fctl,fv22,fmarket,fget,fwarm,data_first,data_end,eval_end,qmeta=prepare_forward()

    # Independent reference streams
    hb,hbr=build_ref(UH,hset,hctl,hv22,hmarket,hget,None,False,"HIST")
    hv,hvr=build_ref(UH,hset,hctl,hv22,hmarket,hget,None,True,"HIST")
    fb,fbr=build_ref(UF,fset,fctl,fv22,fmarket,fget,fwarm,False,"FORWARD")
    fv_,fvr=build_ref(UF,fset,fctl,fv22,fmarket,fget,fwarm,True,"FORWARD")
    ref_events={"BASE_REF":sorted(hb+fb,key=lambda x:x["exit_time"]),
                "V22_REF":sorted(hv+fv_,key=lambda x:x["exit_time"])}
    ref_rows=hbr+hvr+fbr+fvr
    write_csv(OUT_REF,ref_rows)

    allrows={};transitions=[]

    for label,kind,ref_mode,N,off_thr,on_thr in SCENARIOS:
        print("RUN",label,flush=True)
        switch=None
        if kind=="PERF":
            switch=PerfSwitch(label,ref_events[ref_mode],N,off_thr,on_thr)

        # Historical replay first; same switch continues into forward
        hr=replay_dataset(UH,hset,hctl,hv22,hmarket,hget,None,label,kind,switch)
        fr=replay_dataset(UF,fset,fctl,fv22,fmarket,fget,fwarm,label,kind,switch)
        allrows[label]=hr+fr
        if switch is not None:transitions.extend(switch.transitions)

        p=ROOT/f"MKT_REGIME_{label}_TRADES.csv"
        write_csv(p,allrows[label])

    # Summary
    periods=("0901_0917","0918_0922","0923_NOW")
    summary=[]
    for period in periods:
        for label,_,_,_,_,_ in SCENARIOS:
            summary.append(summarize(label,allrows[label],period))

    # Delta vs V22_ONLY in each period
    for r in summary:
        base=next(x for x in summary if x["period"]==r["period"] and x["scenario"]=="V22_ONLY")
        r["delta_vs_v22_only"]=round(r["net_pct"]-base["net_pct"],6)
        always=next(x for x in summary if x["period"]==r["period"] and x["scenario"]=="MARKET_ALWAYS_ON_V22")
        r["delta_vs_market_always_on"]=round(r["net_pct"]-always["net_pct"],6)

    write_csv(OUT_SUM,summary);write_csv(OUT_TRANS,transitions)

    # Blocks audit
    blocks=[]
    for label,rows in allrows.items():
        for r in rows:
            if r.get("block_reason") in ("MARKET_GUARD_ACTIVE","V22_QUALITY_OR"):
                blocks.append(dict(r))
    write_csv(OUT_BLOCKS,blocks)

    # Daily
    dates=sorted(set(r["entry_time_kst"][:10] for rows in allrows.values() for r in rows))
    daily=[]
    for d in dates:
        row={"date":d}
        for label,_,_,_,_,_ in SCENARIOS:
            z=[r for r in allrows[label] if r["entry_time_kst"].startswith(d) and int(r.get("accepted",0))==1]
            row[label+"_entries"]=len(z)
            row[label+"_net"]=round(sum(float(r["net_pct"]) for r in z),6)
        daily.append(row)
    write_csv(OUT_DAY,daily)

    lines=[
        "MARKET PERFORMANCE-REGIME EXACT PORTFOLIO REPLAY + V22 LOCK0",
        f"forward_source={data_first.isoformat()} ~ {data_end.isoformat()}",
        f"forward_eval_end={eval_end.isoformat()} (-3h)",
        f"raw_scan_max_gap_min={maxgap[0]:.1f}",
        f"forward_exact_watch={len(qmeta)}",
        "",
        "BASE_REF shadow events="+str(len(ref_events["BASE_REF"])),
        "V22_REF shadow events="+str(len(ref_events["V22_REF"])),
        "",
    ]
    for p in periods:
        lines.append("["+p+"]")
        for label,_,_,_,_,_ in SCENARIOS:
            r=next(x for x in summary if x["period"]==p and x["scenario"]==label)
            lines.append(
                f"{label}: entries={r['entries']} NET={r['net_pct']:+.6f} "
                f"dV22={r['delta_vs_v22_only']:+.6f} dAlways={r['delta_vs_market_always_on']:+.6f} "
                f"TP={r['TP']} STOP={r['STOP']} MKTblocks={r['market_blocks']} "
                f"MKTwould={r['market_block_would_net']:+.6f}"
            )
        lines.append("")

    lines.append("[TRANSITIONS PRIMARY PERF_B_N9_P8_N5]")
    for t in transitions:
        if t["scenario"]=="PERF_B_N9_P8_N5":
            lines.append(f"{t['time_kst']} -> {t['new_state']} sum9={t['rolling_sum']:+.6f} by {t['trigger_symbol']}")
    OUT_TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")

    files=[OUT_SUM,OUT_DAY,OUT_TRANS,OUT_BLOCKS,OUT_REF,OUT_TXT]
    for label,_,_,_,_,_ in SCENARIOS:
        files.append(ROOT/f"MKT_REGIME_{label}_TRADES.csv")
    with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
        for p in files:
            if p.exists():z.write(p,arcname=p.name)

    print("\n".join(lines),flush=True)
    print("DONE:",OUT_ZIP,flush=True)

if __name__=="__main__":
    main()
