#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V22 SLOT-LOCK exact causal replay

Goal
----
Market guard is SHADOW ONLY (never blocks).
Compare:
  BASE          : V25 + SAFE/RELAX + existing MKT100 + portfolio scheduler
  V22_LOCK0     : BASE + V22 quality block, immediate backfill allowed
  V22_LOCK15    : V22 quality block + reserve one slot for 15m
  V22_LOCK30
  V22_LOCK45
  V22_LOCK60
  V22_LOCK90

A virtual lock is created ONLY when:
  1) entry filter passes,
  2) portfolio scheduler says the trade could open at that moment,
  3) V22 quality blocks it.
Thus a V22 flag on a trade that could not have opened anyway does not reserve a slot.

Market shadow:
  rolling 2h V25 >= 8 AND mean(abs(BTC4h),abs(ETH4h)) >= 0.40%
is tagged and reported, but NEVER blocks any scenario.

Periods:
  TRAIN-A  2026-09-01~09-17
  TRAIN-B  2026-09-18~09-22
  FORWARD  2026-09-23~latest complete 3h horizon
  NOTE: any raw scan gaps are reported.

No DB writes. No orders. No live bot changes.
"""
from __future__ import annotations

import copy,csv,gzip,hashlib,importlib.util,io,json,math,os,sqlite3,sys,time,zipfile
from collections import Counter,deque
from datetime import datetime,timedelta,timezone
from pathlib import Path

ROOT=Path("/root/hyejin-trader/bybit_swing")
U_PATH=ROOT/"unified_current_0901_0922.py"
FWD_BASE=ROOT/"forward_full_0924_0925.py"

KST=timezone(timedelta(hours=9));UTC=timezone.utc
HIST_START=datetime(2026,9,1,0,0,0,tzinfo=KST)
HIST_END=datetime(2026,9,23,0,0,0,tzinfo=KST)
FWD_START=datetime(2026,9,23,0,0,0,tzinfo=KST)
FWD_WARM=datetime(2026,9,22,8,0,0,tzinfo=KST)

HOLDS=[None,0,15,30,45,60,90] # None = BASE no V22
NAMES={None:"BASE",0:"V22_LOCK0",15:"V22_LOCK15",30:"V22_LOCK30",45:"V22_LOCK45",60:"V22_LOCK60",90:"V22_LOCK90"}

SCAN=ROOT/"scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.csv"
SCANZIP=ROOT/"scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.zip"
TMPDB=ROOT/".v22lock_scan_tmp.sqlite"

OUT_SUM=ROOT/"V22_SLOT_LOCK_SUMMARY.csv"
OUT_DAY=ROOT/"V22_SLOT_LOCK_DAILY.csv"
OUT_BLOCK=ROOT/"V22_SLOT_LOCK_DIRECT_BLOCKS.csv"
OUT_TXT=ROOT/"V22_SLOT_LOCK_SUMMARY.txt"
OUT_ZIP=ROOT/"V22_SLOT_LOCK_RESULTS.zip"

V22_HIST=ROOT/"V22_QUALITY_RESCHEDULE_TRADES.csv"
BASE_CACHE=ROOT/"UNIFIED_CURRENT_0901_0922_TRADES.csv"

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

# ---------------------------------------------------------
# Build forward scan with SQLite streaming dedupe: no OOM.
# ---------------------------------------------------------
def source_candidates():
    tokens=("20260922","20260923","20260924","20260925","20260926","20260927")
    out=[]
    def maybe(p):
        try:
            if not p.is_file():return
            n=p.name.lower()
            if "scan" not in n:return
            if not (p.suffix.lower() in (".csv",".zip") or n.endswith(".csv.gz")):return
            if p.resolve() in (SCAN.resolve(),SCANZIP.resolve()):return
            if p.name=="scan_rejected.csv" or any(t in p.name for t in tokens):
                out.append(p)
        except:pass
    arc=ROOT/"scan_archive"
    if arc.exists():
        for p in arc.rglob("*"):maybe(p)
    for p in ROOT.glob("scan*"):maybe(p)
    return list(dict.fromkeys(out))

def build_forward_scan():
    if TMPDB.exists():TMPDB.unlink()
    con=sqlite3.connect(TMPDB)
    con.execute("PRAGMA journal_mode=OFF")
    con.execute("PRAGMA synchronous=OFF")
    con.execute("CREATE TABLE rows(h TEXT PRIMARY KEY, ts TEXT NOT NULL, payload TEXT NOT NULL)")
    fields=[];fieldset=set();start_txt=FWD_START.strftime("%Y-%m-%d %H:%M:%S")
    sources=source_candidates()
    print("[SCAN] sources",len(sources),flush=True)

    def consume(fh):
        rd=csv.DictReader(fh)
        if not rd.fieldnames:return 0
        for fld in rd.fieldnames:
            if fld not in fieldset:fieldset.add(fld);fields.append(fld)
        tk=next((k for k in ("time_kst","timestamp_kst","datetime_kst","time","timestamp") if k in rd.fieldnames),rd.fieldnames[0])
        batch=[];n=0
        for r in rd:
            ts=str(r.get(tk,"")).strip().strip('"')
            if not ts or ts<start_txt:continue
            payload=json.dumps(r,sort_keys=True,ensure_ascii=False,separators=(",",":"))
            h=hashlib.sha1(payload.encode("utf-8","replace")).hexdigest()
            batch.append((h,ts,payload));n+=1
            if len(batch)>=2000:
                con.executemany("INSERT OR IGNORE INTO rows VALUES(?,?,?)",batch);batch=[]
        if batch:con.executemany("INSERT OR IGNORE INTO rows VALUES(?,?,?)",batch)
        return n

    for i,p in enumerate(sources,1):
        try:
            nl=p.name.lower()
            if nl.endswith(".csv.gz"):
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
                cnt=con.execute("SELECT COUNT(*) FROM rows").fetchone()[0]
                print(f"[SCAN] {i}/{len(sources)} unique={cnt}",flush=True)
        except Exception as e:
            print("[SCAN WARN]",p.name,repr(e),flush=True)
    con.commit()

    cur=con.execute("SELECT ts,payload FROM rows ORDER BY ts")
    count=0;first=None;last=None;maxgap=(0,None,None);prev_dt=None
    with SCAN.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction="ignore");w.writeheader()
        for ts,payload in cur:
            r=json.loads(payload);w.writerow(r);count+=1
            if first is None:first=ts
            last=ts
            try:
                d=datetime.strptime(ts[:19],"%Y-%m-%d %H:%M:%S")
                if prev_dt is not None:
                    gap=(d-prev_dt).total_seconds()/60
                    if gap>maxgap[0]:maxgap=(gap,prev_dt,d)
                prev_dt=d
            except:pass
    con.close();TMPDB.unlink(missing_ok=True)
    with zipfile.ZipFile(SCANZIP,"w",zipfile.ZIP_DEFLATED) as z:z.write(SCAN,arcname=SCAN.name)
    print("SCAN first/last",first,last,"rows",count,flush=True)
    print("SCAN largest timestamp gap(min)",maxgap[0],maxgap[1],maxgap[2],flush=True)
    return first,last,count,maxgap

# ---------------------------------------------------------
# Common replay helper
# ---------------------------------------------------------
def add_virtual_lock(sched,entry,sid,hold):
    if not hold:return
    expiry=entry+timedelta(minutes=hold)
    # open_trades is pruned causally by Scheduler.can_open().
    sched.state.open_trades.append((expiry,f"__V22LOCK_{sid}"))

def stats(period,scenario,rows):
    acc=[r for r in rows if int(r.get("accepted",0))==1]
    c=Counter(str(r.get("result") or "") for r in acc)
    direct=[r for r in rows if r.get("block_reason")=="V22_QUALITY_OR"]
    market=[r for r in acc if int(r.get("market_shadow",0))==1]
    return {
        "period":period,"scenario":scenario,"entries":len(acc),
        "net_pct":round(sum(float(r["net_pct"]) for r in acc),6),
        "TP":c.get("TP20_FULL",0),"STOP":c.get("STOP",0),
        "PP12":c.get("PROFIT_PROTECT_EXIT",0),"LATE":c.get("LATE_FAILURE_EXIT",0),
        "TIME":c.get("TIME_EXIT",0),"FLAT":c.get("FLAT_EXIT_75M",0),
        "v22_direct_blocks":len(direct),
        "v22_direct_would_net":round(sum(fv(r.get("would_net"),0) for r in direct),6),
        "market_shadow_entries":len(market),
        "market_shadow_net":round(sum(float(r["net_pct"]) for r in market),6),
        "virtual_locks":sum(int(r.get("virtual_lock_created",0)) for r in rows),
        "slot4_blocks":sum(r.get("block_reason")=="SLOT4" for r in rows),
    }

# ---------------------------------------------------------
# HIST 9/1-22
# ---------------------------------------------------------
def hist_replay():
    U=load_module("U_V22LOCK_HIST",U_PATH)
    U.START_KST=HIST_START;U.END_KST=HIST_END
    U.START_UTC=HIST_START.astimezone(UTC);U.END_UTC=HIST_END.astimezone(UTC)
    U.EXPECTED_V25=-1
    U.CACHE_DIR=ROOT/".v22lock_hist_cache";U.CACHE_DIR.mkdir(exist_ok=True)
    U.KC=U.KlineCache();U._market_1m={};U._market_recompute_count=0
    print("[HIST] market",flush=True);U.load_market_series()
    setups=U.load_setups();controls=U.load_control_proxy()
    setups=[s for s in setups if U.START_UTC<=s["entry"]<U.END_UTC]

    # exact historical V22 flags already recovered previously
    if not V22_HIST.exists():raise SystemExit(f"missing {V22_HIST}")
    import pandas as pd
    vdf=pd.read_csv(V22_HIST,low_memory=False)
    vdf=vdf[vdf["dataset"].astype(str)=="HIST"]
    v22={}
    for r in vdf.to_dict("records"):
        sid=str(r.get("setup_id") or "")
        if sid and sid not in v22:
            v22[sid]=bool(int(fv(r.get("overext"),0) or 0) or int(fv(r.get("weak_reaccel"),0) or 0))

    # market shadow
    q=deque();market={}
    for st in setups:
        t=st["entry"]
        while q and q[0]<t-timedelta(hours=2):q.popleft()
        q.append(t)
        d=st["details"];U.fill_missing_market(d,t)
        b=U.first_f(d,"btc_4h_change_pct","btc_4h");e=U.first_f(d,"eth_4h_change_pct","eth_4h")
        av=None if b is None or e is None else (abs(b)+abs(e))/2
        market[st["setup_id"]]=bool(len(q)>=8 and av is not None and av>=.40)

    # base cache from existing accepted historical trades
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

    allruns={}
    blocks=[]
    for hold in HOLDS:
        name=NAMES[hold];sched=U.Scheduler();rows=[]
        for i,st in enumerate(setups,1):
            sid=st["setup_id"];ef=U.entry_filter(st,controls)
            row={"dataset":"HIST","scenario":name,"setup_id":sid,"symbol":st["symbol"],
                 "entry_time_kst":U.kst_stamp(st["entry"]),"accepted":0,
                 "block_reason":ef["reason"],"v22q":int(v22.get(sid,False)),
                 "market_shadow":int(market.get(sid,False)),"virtual_lock_created":0,
                 "result":"","net_pct":"","exit_time_kst":"","would_net":""}
            if not ef["pass"]:rows.append(row);continue
            ok,why=sched.can_open(st["entry"],st["symbol"])
            if not ok:
                row["block_reason"]=why;rows.append(row);continue
            if hold is not None and v22.get(sid,False):
                sim=getsim(st) # counterfactual only
                row["block_reason"]="V22_QUALITY_OR";row["would_net"]=round(sim.net_pct,6)
                if hold>0:
                    add_virtual_lock(sched,st["entry"],sid,hold);row["virtual_lock_created"]=1
                rows.append(row);blocks.append(dict(row));continue
            sim=getsim(st)
            row.update({"accepted":1,"block_reason":"","result":sim.result,
                        "net_pct":round(sim.net_pct,6),"exit_time_kst":U.kst_stamp(sim.exit_time)})
            sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
            rows.append(row)
        allruns[name]=rows
        print("[HIST]",name,"accepted",sum(r["accepted"] for r in rows),flush=True)
    return allruns,blocks

# ---------------------------------------------------------
# FORWARD 9/23-now
# ---------------------------------------------------------
def forward_replay():
    M=load_module("M_V22LOCK_FWD",FWD_BASE);U=M.U
    M.SCAN=SCAN;M.EVAL_START_KST=FWD_START;M.WARM_START_KST=FWD_WARM
    df,data_first,data_end,eval_end=M.load_scan_window()
    if eval_end<=FWD_START:raise SystemExit("not enough forward horizon")
    qmeta=M.load_exact_watch_features(df)
    M.configure_unified(eval_end)
    U.load_market_series();setups=U.load_setups();controls=U.load_control_proxy()
    rmeta=M.build_risk_meta(setups);cache={}
    warm=M.warm_base(setups,controls,cache)
    lo=FWD_START.astimezone(UTC);hi=eval_end.astimezone(UTC)
    es=[s for s in setups if lo<=s["entry"]<=hi]

    def getsim(st):
        return M.get_sim(st,cache)

    allruns={};blocks=[]
    for hold in HOLDS:
        name=NAMES[hold];sched=copy.deepcopy(warm);rows=[]
        for st in es:
            sid=st["setup_id"];ef=U.entry_filter(st,controls);qm=qmeta.get(sid);rm=rmeta[sid]
            row={"dataset":"FORWARD","scenario":name,"setup_id":sid,"symbol":st["symbol"],
                 "entry_time_kst":U.kst_stamp(st["entry"]),"accepted":0,
                 "block_reason":ef["reason"],
                 "v22q":int(bool(qm and qm["v22_quality_or"])),
                 "exact_watch":int(bool(qm)),
                 "market_shadow":int(bool(rm["risk"])),"virtual_lock_created":0,
                 "result":"","net_pct":"","exit_time_kst":"","would_net":""}
            if not ef["pass"]:rows.append(row);continue
            ok,why=sched.can_open(st["entry"],st["symbol"])
            if not ok:
                row["block_reason"]=why;rows.append(row);continue
            if hold is not None and qm and qm["v22_quality_or"]:
                sim=getsim(st)
                row["block_reason"]="V22_QUALITY_OR";row["would_net"]=round(sim.net_pct,6)
                if hold>0:
                    add_virtual_lock(sched,st["entry"],sid,hold);row["virtual_lock_created"]=1
                rows.append(row);blocks.append(dict(row));continue
            sim=getsim(st)
            row.update({"accepted":1,"block_reason":"","result":sim.result,
                        "net_pct":round(sim.net_pct,6),"exit_time_kst":U.kst_stamp(sim.exit_time)})
            sched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
            rows.append(row)
        allruns[name]=rows
        print("[FWD]",name,"accepted",sum(r["accepted"] for r in rows),flush=True)
    return allruns,blocks,data_first,data_end,eval_end,qmeta

def main():
    first,last,n,maxgap=build_forward_scan()
    hist,hblocks=hist_replay()
    fwd,fblocks,data_first,data_end,eval_end,qmeta=forward_replay()

    summary=[];daily=[]
    for name in NAMES.values():
        hr=hist[name]
        a=[r for r in hr if "2026-09-01"<=r["entry_time_kst"][:10]<="2026-09-17"]
        b=[r for r in hr if "2026-09-18"<=r["entry_time_kst"][:10]<="2026-09-22"]
        summary.append(stats("0901_0917",name,a))
        summary.append(stats("0918_0922",name,b))
        summary.append(stats("0923_NOW",name,fwd[name]))

    # daily rows
    dates=sorted(set(r["entry_time_kst"][:10] for rr in list(hist.values())+list(fwd.values()) for r in rr))
    for d in dates:
        row={"date":d}
        source=hist if d<"2026-09-23" else fwd
        for name in NAMES.values():
            z=[r for r in source[name] if r["entry_time_kst"].startswith(d)]
            s=stats(d,name,z)
            row[name+"_entries"]=s["entries"];row[name+"_net"]=s["net_pct"]
        daily.append(row)

    write_csv(OUT_SUM,summary);write_csv(OUT_DAY,daily);write_csv(OUT_BLOCK,hblocks+fblocks)

    lines=[
        "V22 SLOT-LOCK EXACT CAUSAL REPLAY",
        "Market guard = SHADOW ONLY; never blocks",
        f"forward_source={data_first.isoformat()} ~ {data_end.isoformat()}",
        f"forward_eval_end={eval_end.isoformat()} (-3h outcome horizon)",
        f"raw_scan_largest_gap_min={maxgap[0]:.1f} {maxgap[1]} -> {maxgap[2]}",
        f"exact_forward_watch_setups={len(qmeta)}",
        "",
    ]
    for period in ("0901_0917","0918_0922","0923_NOW"):
        lines.append("["+period+"]")
        for name in NAMES.values():
            s=next(x for x in summary if x["period"]==period and x["scenario"]==name)
            lines.append(
                f"{name}: entries={s['entries']} NET={s['net_pct']:+.6f} "
                f"TP={s['TP']} STOP={s['STOP']} V22blocks={s['v22_direct_blocks']} "
                f"wouldNET={s['v22_direct_would_net']:+.6f} locks={s['virtual_locks']} "
                f"MKTshadow={s['market_shadow_entries']}/{s['market_shadow_net']:+.6f}"
            )
        lines.append("")
    OUT_TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")

    # keep per-trade files for exact audit
    audit=[]
    for name,rr in hist.items():
        p=ROOT/f"V22_SLOT_LOCK_HIST_{name}_TRADES.csv";write_csv(p,rr);audit.append(p)
    for name,rr in fwd.items():
        p=ROOT/f"V22_SLOT_LOCK_FWD_{name}_TRADES.csv";write_csv(p,rr);audit.append(p)

    with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
        for p in [OUT_SUM,OUT_DAY,OUT_BLOCK,OUT_TXT,SCANZIP]+audit:
            if p.exists():z.write(p,arcname=p.name)
    print("\n".join(lines),flush=True)
    print("DONE:",OUT_ZIP,flush=True)

if __name__=="__main__":
    main()
