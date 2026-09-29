#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
9/28 CLOSED-DAY EXACT FULL INTEGRATION

Replays 2026-09-23 -> 2026-09-29 00:00 KST for causal state,
but reports 2026-09-28 only.

ENTRY:
 V25 -> SAFE/RELAX -> existing MKT100
 -> performance market regime N=9, OFF +9 / ON -6 (BASE_REF)
 -> V22 quality LOCK0
 -> portfolio scheduler with STOP_PAUSE_WINDOW=45m

EXIT:
 current clean TP2 / PP12 / Final4 + V27-1

STOP:
 observer vol<=2.61 and ema20 slope>=.00061
 -> new low +1.5% rebound
 -> GREEN / SAFE_GRAY only add 100%
 -> new-average recover
 -> if no recover within 45m after add: full DCA45 exit
 -> RISK: no add, exit original at rebound trigger

No live bot/DB changes.
"""
from __future__ import annotations
import csv,gzip,hashlib,importlib.util,io,json,math,sqlite3,sys,zipfile,copy
from pathlib import Path
from datetime import datetime,timedelta,timezone
from collections import deque,Counter

R=Path("/root/hyejin-trader/bybit_swing")
F0=R/"forward_full_0924_0925.py"
REGZIP=R/"MKT_REGIME_EXACT_RESULTS.zip"
FULL28=R/"SCAN_FULL_20260928_0000_2359_KST.csv"
SCAN=R/"SCAN_COMBINED_0923_0928_FINAL.csv"
DB=R/".scan_final28.sqlite"

KST=timezone(timedelta(hours=9));UTC=timezone.utc
START=datetime(2026,9,23,0,0,tzinfo=KST)
END=datetime(2026,9,29,0,0,tzinfo=KST)
WARM=datetime(2026,9,22,8,0,tzinfo=KST)
DAY="2026-09-28"

OUT_TR=R/"FINAL_EXACT_20260928_TRADES.csv"
OUT_BL=R/"FINAL_EXACT_20260928_BLOCKS.csv"
OUT_SUM=R/"FINAL_EXACT_20260928_SUMMARY.txt"
OUT_ZIP=R/"FINAL_EXACT_20260928_RESULTS.zip"

FEE=.055

def fv(v,d=None):
    try:
        x=float(v);return d if math.isnan(x) else x
    except:return d
def pct(a,b):return (a/b-1)*100 if b else None
def mean(a):
    z=[float(x) for x in a if x is not None and math.isfinite(float(x))]
    return sum(z)/len(z) if z else None
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
def writecsv(p,rr):
    if not rr:p.write_text("",encoding="utf-8-sig");return
    ks=[]
    for r in rr:
        for k in r:
            if k not in ks:ks.append(k)
    with p.open("w",encoding="utf-8-sig",newline="") as h:
        w=csv.DictWriter(h,fieldnames=ks,extrasaction="ignore");w.writeheader();w.writerows(rr)
def loadmod(n,p):
    s=importlib.util.spec_from_file_location(n,str(p));m=importlib.util.module_from_spec(s);sys.modules[n]=m;s.loader.exec_module(m);return m

# -------- Merge 9/23-9/28 scan sources, including the closed-day 9/28 file --------
DB.unlink(missing_ok=True);con=sqlite3.connect(DB)
con.execute("PRAGMA journal_mode=OFF");con.execute("PRAGMA synchronous=OFF")
con.execute("CREATE TABLE x(h TEXT PRIMARY KEY,ts TEXT,p TEXT)")
fields=[];fieldset=set()

def eat(fh):
    rd=csv.DictReader(fh)
    if not rd.fieldnames:return
    for k in rd.fieldnames:
        if k not in fieldset:fieldset.add(k);fields.append(k)
    tk=next((k for k in ("time_kst","timestamp_kst","datetime_kst","time","timestamp") if k in rd.fieldnames),None)
    if not tk:return
    batch=[]
    for r in rd:
        ts=str(r.get(tk,"")).strip().strip('"')
        if not ("2026-09-23 00:00:00"<=ts<"2026-09-29 00:00:00"):continue
        s=json.dumps(r,sort_keys=True,ensure_ascii=False,separators=(",",":"))
        batch.append((hashlib.sha1(s.encode()).hexdigest(),ts,s))
        if len(batch)>=3000:
            con.executemany("INSERT OR IGNORE INTO x VALUES(?,?,?)",batch);batch=[]
    if batch:con.executemany("INSERT OR IGNORE INTO x VALUES(?,?,?)",batch)

src=[]
for b in (R,R/"scan_archive"):
    if b.exists():
        for p in b.rglob("*"):
            n=p.name.lower()
            if p.is_file() and "scan" in n and (p.suffix.lower() in (".csv",".zip") or n.endswith(".csv.gz")) and p.resolve()!=SCAN.resolve():
                src.append(p)
src=list(dict.fromkeys(src))
print("[MERGE] sources",len(src),flush=True)
for i,p in enumerate(src,1):
    try:
        if p.name.lower().endswith(".csv.gz"):
            with gzip.open(p,"rt",encoding="utf-8-sig",errors="replace",newline="") as h:eat(h)
        elif p.suffix.lower()==".zip":
            with zipfile.ZipFile(p) as z:
                for n in z.namelist():
                    if n.lower().endswith(".csv"):
                        with z.open(n) as q:
                            with io.TextIOWrapper(q,encoding="utf-8-sig",errors="replace",newline="") as h:eat(h)
        else:
            with p.open("r",encoding="utf-8-sig",errors="replace",newline="") as h:eat(h)
        if i%20==0 or i==len(src):
            con.commit();print("[MERGE]",i,"/",len(src),con.execute("SELECT COUNT(*) FROM x").fetchone()[0],flush=True)
    except Exception as e:print("[WARN]",p.name,repr(e),flush=True)
con.commit()
rr=list(con.execute("SELECT ts,p FROM x ORDER BY ts"));con.close();DB.unlink(missing_ok=True)
if not rr:raise SystemExit("NO COMBINED SCAN")
with SCAN.open("w",encoding="utf-8-sig",newline="") as h:
    w=csv.DictWriter(h,fieldnames=fields,extrasaction="ignore");w.writeheader()
    for ts,s in rr:w.writerow(json.loads(s))
print("[MERGE] first",rr[0][0],"last",rr[-1][0],flush=True)

# -------- Current forward engine --------
M=loadmod("MFINAL28",F0);U=M.U
M.SCAN=SCAN;M.EVAL_START_KST=START;M.WARM_START_KST=WARM
df,dfirst,dend,_=M.load_scan_window()
M.configure_unified(END)
U.load_market_series()
setups=U.load_setups();controls=U.load_control_proxy()
qmeta=M.load_exact_watch_features(df);rmeta=M.build_risk_meta(setups)
cache={}
warm=M.warm_base(setups,controls,cache)

lo=START.astimezone(UTC);hi=END.astimezone(UTC)
evalset=[s for s in setups if lo<=s["entry"]<hi]

# -------- Historical BASE_REF market-shadow outcomes to seed regime --------
hist_ref=[]
if not REGZIP.exists():raise SystemExit("missing MKT_REGIME_EXACT_RESULTS.zip")
with zipfile.ZipFile(REGZIP) as z:
    ns=[n for n in z.namelist() if n.endswith("MKT_REGIME_SHADOW_REFERENCE.csv") or "SHADOW_REFERENCE" in n]
    if ns:
        with z.open(ns[0]) as raw:
            for r in csv.DictReader(io.TextIOWrapper(raw,encoding="utf-8-sig")):
                if r.get("dataset")=="HIST" and r.get("ref_mode")=="BASE_REF":
                    et=datetime.strptime(r["exit_time_kst"][:19],"%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).astimezone(UTC)
                    hist_ref.append({"exit":et,"net":float(r["net_pct"]),"sid":r["setup_id"],"sym":r["symbol"]})
if not hist_ref:raise SystemExit("no historical BASE_REF events")

# Forward BASE_REF uses original 30m pause, preserving the validated +9/-6 reference definition.
U.STOP_PAUSE_WINDOW_MIN=30
refsched=copy.deepcopy(warm);fref=[]
for st in evalset:
    ef=U.entry_filter(st,controls)
    if not ef["pass"]:continue
    ok,why=refsched.can_open(st["entry"],st["symbol"])
    if not ok:continue
    sim=M.get_sim(st,cache)
    refsched.add(st["entry"],st["symbol"],sim,sim.result in ("STOP","LATE_FAILURE_EXIT"))
    if rmeta[st["setup_id"]]["risk"]:
        fref.append({"exit":sim.exit_time,"net":float(sim.net_pct),"sid":st["setup_id"],"sym":st["symbol"]})

events=sorted(hist_ref+fref,key=lambda x:(x["exit"],x["sid"]))

class Switch:
    def __init__(self):
        self.i=0;self.q=deque(maxlen=9);self.on=True;self.trans=[]
    def state(self,t):
        while self.i<len(events) and events[self.i]["exit"]<=t:
            x=events[self.i];self.i+=1;self.q.append(x["net"])
            if len(self.q)<9:continue
            s=sum(self.q);old=self.on
            if self.on and s>=9:self.on=False
            elif (not self.on) and s<=-6:self.on=True
            if old!=self.on:
                self.trans.append((x["exit"],self.on,s,x["sym"]))
        return self.on,(sum(self.q) if self.q else None)
sw=Switch()

# -------- DCA helpers --------
def five(one):
    d={}
    for b in one:
        t=b["ts"].replace(minute=b["ts"].minute//5*5,second=0,microsecond=0)
        if t not in d:d[t]={"ts":t,"o":b["o"],"h":b["h"],"l":b["l"],"c":b["c"],"v":b["v"]}
        else:
            x=d[t];x["h"]=max(x["h"],b["h"]);x["l"]=min(x["l"],b["l"]);x["c"]=b["c"];x["v"]+=b["v"]
    return [d[k] for k in sorted(d)]
def feat(one,cp):
    c=cp.replace(second=0,microsecond=0)
    p=[x for x in one if x["ts"]<c];q=[x for x in five(one) if x["ts"]+timedelta(minutes=5)<=c]
    o={"vr":None,"p3":None,"sl":None,"rsi":None}
    if p:
        mv=mean([x["v"] for x in p[-11:-1]]);o["vr"]=p[-1]["v"]/mv if mv else None
        z=p[-3:];o["p3"]=pct(z[-1]["c"],z[0]["o"]) if z else None
    if q:
        cs=[x["c"] for x in q];e=ema(cs[-80:],20);ep=ema(cs[-81:-1],20) if len(cs)>=2 else None
        o["sl"]=(e/ep-1)*100 if e and ep else None;o["rsi"]=rsi(cs)
    return o
def one_net(e,x):return pct(x,e)-(FEE+FEE*x/e)
def two_net(e,a,x):return ((x-e)+(x-a))/e*100-(FEE+FEE*a/e+FEE*2*x/e)
def simcopy(base,res,et,net):
    s=copy.copy(base);s.result=res;s.exit_time=et;s.net_pct=net;return s

def stop_policy(st,base):
    e=float(st["entry_price"]);stop=base.exit_time
    one=M.api_1m(st["symbol"],st["entry"]-timedelta(minutes=90),stop+timedelta(hours=6,minutes=5))
    sf=feat(one,stop)
    obs=sf["vr"] is not None and sf["sl"] is not None and sf["vr"]<=2.61 and sf["sl"]>=.00061
    if not obs:return base,"NO_OBSERVER"
    fl=stop.replace(second=0,microsecond=0);post=[b for b in one if fl+timedelta(minutes=1)<=b["ts"]<=fl+timedelta(hours=6)]
    low=lt=trg=add=None
    for b in post:
        if low is None or b["l"]<low:low=b["l"];lt=b["ts"]
        if b["ts"]>lt and b["h"]>=low*1.015:
            trg=b["ts"];add=low*1.015;break
    if trg is None:return base,"NO_TRIGGER"
    ff=feat(one,trg);lp=pct(low,e);mins=(trg-lt).total_seconds()/60
    green=lp>=-2.74314 and ff["rsi"] is not None and ff["rsi"]>=43.89902
    safe=(not green and mins<12 and ff["p3"] is not None and ff["p3"]>-1)
    if not(green or safe):
        return simcopy(base,"RISK_REBOUND_EXIT",trg,one_net(e,add)),"RISK"
    av=(e+add)/2
    timeout=trg+timedelta(minutes=45)
    after=[b for b in post if b["ts"]>trg]
    for b in after:
        if b["ts"]>timeout:break
        if b["l"]<=low:
            return simcopy(base,"DCA_FAIL_LOW_BREAK",b["ts"],two_net(e,add,low)),"DCA_FAIL"
        if b["h"]>=av:
            return simcopy(base,"DCA_RECOVER_AVG",b["ts"],two_net(e,add,av)),"DCA_RECOVER"
    z=[b for b in after if b["ts"]<=timeout]
    if z:
        b=z[-1]
        return simcopy(base,"DCA_45M_EXIT",b["ts"],two_net(e,add,b["c"])),"DCA_45M_EXIT"
    return base,"DCA_NO_DATA"

# -------- Final exact replay with pause45 --------
U.STOP_PAUSE_WINDOW_MIN=45
sched=copy.deepcopy(warm)
out=[];blocks=[]
for st in evalset:
    sid=st["setup_id"];ef=U.entry_filter(st,controls);ron,rsum=sw.state(st["entry"])
    row={"setup_id":sid,"symbol":st["symbol"],"entry_time_kst":U.kst_stamp(st["entry"]),
         "market_regime_on":int(ron),"market_shadow_sum9":rsum,
         "market_condition":int(rmeta[sid]["risk"]),
         "v22q":int(bool(qmeta.get(sid) and qmeta[sid]["v22_quality_or"]))}
    if not ef["pass"]:
        row["action"]="BLOCK_"+ef["reason"];blocks.append(row);continue
    if ron and rmeta[sid]["risk"]:
        row["action"]="BLOCK_MARKET";blocks.append(row);continue
    if qmeta.get(sid) and qmeta[sid]["v22_quality_or"]:
        row["action"]="BLOCK_V22";blocks.append(row);continue
    ok,why=sched.can_open(st["entry"],st["symbol"])
    if not ok:
        row["action"]="BLOCK_"+why;blocks.append(row);continue
    base=M.get_sim(st,cache);final=base;pc="BASE"
    if base.result=="STOP":
        final,pc=stop_policy(st,base)
    sched.add(st["entry"],st["symbol"],final,base.result in ("STOP","LATE_FAILURE_EXIT"))
    row.update({"action":"ENTRY","base_result":base.result,"base_net":base.net_pct,
                "final_result":final.result,"final_net":final.net_pct,
                "exit_time_kst":U.kst_stamp(final.exit_time),"policy_class":pc})
    out.append(row)

day=[r for r in out if r["entry_time_kst"].startswith(DAY)]
dayblocks=[r for r in blocks if r["entry_time_kst"].startswith(DAY)]
net=sum(float(r["final_net"]) for r in day);base=sum(float(r["base_net"]) for r in day)
c=Counter(r["final_result"] for r in day);bc=Counter(r["action"] for r in dayblocks)

writecsv(OUT_TR,day);writecsv(OUT_BL,dayblocks)
lines=[
"9/28 CLOSED-DAY EXACT FULL INTEGRATION",
f"SCAN_FIRST={dfirst.isoformat()}",
f"SCAN_LAST={dend.isoformat()}",
f"FINAL_ENTRIES={len(day)}",
f"BLOCKS={len(dayblocks)}",
f"BASE_EXIT_NET={base:+.6f}%p",
f"FINAL_NET={net:+.6f}%p",
f"TP20={c['TP20_FULL']}",
f"PP12={c['PROFIT_PROTECT_EXIT']}",
f"STOP={c['STOP']}",
f"RISK_REBOUND={c['RISK_REBOUND_EXIT']}",
f"DCA_RECOVER={c['DCA_RECOVER_AVG']}",
f"DCA_FAIL={c['DCA_FAIL_LOW_BREAK']}",
f"DCA45={c['DCA_45M_EXIT']}",
"BLOCK_COUNTS="+str(dict(bc)),
"",
"REGIME_TRANSITIONS_9/28:"
]
for t,on,s,sym in sw.trans:
    if t.astimezone(KST).strftime("%F")==DAY:
        lines.append(f"{t.astimezone(KST)} -> {'ON' if on else 'OFF'} sum9={s:+.6f} by {sym}")
OUT_SUM.write_text("\n".join(lines)+"\n",encoding="utf-8")
with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in (OUT_TR,OUT_BL,OUT_SUM):z.write(p,arcname=p.name)
print("\n".join(lines));print("DONE:",OUT_ZIP)
