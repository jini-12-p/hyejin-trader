#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit ONLY the 7 EXIT_RECHECK mismatches.
Reconstruct each setup from the latest forward engine and export:
- old/new result/net
- reconstructed entry price
- fresh base result/net/exit time
- 1m path first-hit times for +1.2% MFE and +2.0% TP
- PP12 trigger check after MFE +1.2%
No orders / DB writes / bot changes.
"""
import csv, math, importlib.util, sys, zipfile
from pathlib import Path
from datetime import datetime,timedelta,timezone

ROOT=Path("/root/hyejin-trader/bybit_swing")
MIS=ROOT/"EXIT_RECHECK_0901_0928_MISMATCH.csv"
FWD=ROOT/"forward_full_0924_0925.py"
OUT=ROOT/"EXIT_AUDIT_7_DETAIL.csv"
TXT=ROOT/"EXIT_AUDIT_7_SUMMARY.txt"
ZIP=ROOT/"EXIT_AUDIT_7_RESULTS.zip"
KST=timezone(timedelta(hours=9));UTC=timezone.utc

def f(v,d=None):
    try:
        x=float(v);return d if math.isnan(x) else x
    except:return d
def pct(a,b):return (a/b-1)*100 if b else None
def writecsv(p,rows):
    if not rows:p.write_text("",encoding="utf-8-sig");return
    ks=[]
    for r in rows:
        for k in r:
            if k not in ks:ks.append(k)
    with p.open("w",encoding="utf-8-sig",newline="") as h:
        w=csv.DictWriter(h,fieldnames=ks,extrasaction="ignore");w.writeheader();w.writerows(rows)

if not MIS.exists():raise SystemExit("missing EXIT_RECHECK_0901_0928_MISMATCH.csv")
with MIS.open(encoding="utf-8-sig",newline="") as h:mis=list(csv.DictReader(h))

spec=importlib.util.spec_from_file_location("MEXITAUDIT",str(FWD))
M=importlib.util.module_from_spec(spec);sys.modules["MEXITAUDIT"]=M;spec.loader.exec_module(M)
scan=next((p for p in [
    ROOT/"scan_FORWARD_20260923_TO_NOW_MKTREGIME_KST.csv",
    ROOT/"scan_FORWARD_20260923_TO_NOW_V22LOCK_KST.csv",
    ROOT/"scan_rejected.csv"] if p.exists()),None)
if scan is None:raise SystemExit("no forward scan found")
M.SCAN=scan;M.EVAL_START_KST=datetime(2026,9,23,tzinfo=KST);M.WARM_START_KST=datetime(2026,9,22,8,tzinfo=KST)
df,a,b,end=M.load_scan_window();M.configure_unified(end);M.U.load_market_series()
setups=M.U.load_setups()
byid={s["setup_id"]:s for s in setups}

def first_hit(one,start,entry,level):
    target=entry*(1+level/100)
    for b in one:
        if b["ts"]>=start and b["h"]>=target:return b["ts"]
    return None

rows=[];cache={}
for i,x in enumerate(mis,1):
    sid=x["setup_id"];st=byid.get(sid)
    if st is None:
        # fallback symbol/time nearest
        t=datetime.strptime(x["entry_time_kst"][:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).astimezone(UTC)
        z=[s for s in setups if s["symbol"]==x["symbol"] and abs((s["entry"]-t).total_seconds())<=120]
        st=min(z,key=lambda s:abs((s["entry"]-t).total_seconds())) if z else None
    if st is None:
        rows.append({**x,"audit_error":"SETUP_NOT_FOUND"});continue

    sim=M.get_sim(st,cache)
    ep=float(st["entry_price"]);ent=st["entry"]
    horizon=max(sim.exit_time,ent+timedelta(hours=3))+timedelta(minutes=5)
    one=M.api_1m(st["symbol"],ent-timedelta(minutes=2),horizon)

    mfe12=first_hit(one,ent,ep,1.2)
    tp20=first_hit(one,ent,ep,2.0)

    # PP12 definition: after MFE >= +1.2%, confirmed 5m close <= entry -1.15%.
    pp_time=None;pp_close=None
    if mfe12:
        bars5={}
        for q in one:
            t=q["ts"].replace(minute=q["ts"].minute//5*5,second=0,microsecond=0)
            if t not in bars5:bars5[t]={"ts":t,"c":q["c"]}
            else:bars5[t]["c"]=q["c"]
        for t in sorted(bars5):
            close_time=t+timedelta(minutes=5)
            if close_time>=mfe12 and close_time>=ent and bars5[t]["c"]<=ep*(1-.0115):
                pp_time=close_time;pp_close=bars5[t]["c"];break

    def ks(t):return "" if t is None else t.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")
    chronology=[]
    for nm,t in (("MFE12",mfe12),("TP20",tp20),("PP12",pp_time),("FRESH_EXIT",sim.exit_time)):
        if t:chronology.append((t,nm))
    chronology=" > ".join(nm for _,nm in sorted(chronology))

    rows.append({
        **x,
        "matched_setup_id":st["setup_id"],
        "entry_price":ep,
        "fresh_result":sim.result,
        "fresh_net":round(sim.net_pct,6),
        "fresh_exit_time_kst":ks(sim.exit_time),
        "mfe12_time_kst":ks(mfe12),
        "tp20_time_kst":ks(tp20),
        "pp12_time_kst":ks(pp_time),
        "pp12_close":pp_close,
        "chronology":chronology,
        "old_matches_fresh":int(str(x["old_result"])==str(sim.result) and abs(f(x["old_net"],0)-sim.net_pct)<1e-5),
        "new_matches_fresh":int(str(x["new_result"])==str(sim.result) and abs(f(x["new_net"],0)-sim.net_pct)<1e-5),
        "audit_error":""
    })
    print(f"[{i}/{len(mis)}] {x['symbol']} old={x['old_result']} new={x['new_result']} fresh={sim.result} {chronology}",flush=True)

writecsv(OUT,rows)
lines=["EXIT AUDIT 7 MISMATCHES",
       f"old_matches_fresh={sum(r.get('old_matches_fresh',0) for r in rows)}/{len(rows)}",
       f"new_matches_fresh={sum(r.get('new_matches_fresh',0) for r in rows)}/{len(rows)}",""]
for r in rows:
    lines.append(f"{r.get('symbol')} {r.get('entry_time_kst')} | old {r.get('old_result')} {r.get('old_net')} | new {r.get('new_result')} {r.get('new_net')} | fresh {r.get('fresh_result')} {r.get('fresh_net')} | {r.get('chronology')}")
TXT.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(OUT,arcname=OUT.name);z.write(TXT,arcname=TXT.name)
print("\n".join(lines));print("DONE:",ZIP)
