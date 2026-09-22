#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
A/B/C comparison for the deep cycle candidate (DD2_RB10).

READ ONLY:
- no orders
- no DB writes

Candidate entry:
- original core size = 1.0
- after price has touched <= -2.0% from original entry,
  track the observed swing low
- when price rebounds +1.0% from that swing low, add 0.5
- max one add
- new average is recalculated immediately
- TP = new average +2.0%
- if price recovers to new average before the baseline stop, remove the added 0.5
- if the pre-add swing low breaks before the baseline stop, remove the added 0.5

A = CURRENT_STOP
    Existing baseline stop remains a full exit (same as prior V2 study).

B = ADD_ONLY_AT_BASE_STOP
    If the existing baseline loss signal arrives while the added 0.5 is still open:
      - close only the added 0.5 at the baseline stop price
      - keep the original core 1.0
      - after that, core exits at new-average +2.0% TP
        or original-entry -3.0% hard disaster, whichever comes first
      - no second add
    If added 0.5 had already been removed, the baseline stop stays a full exit.

C = REBASE_STOP
    At the existing baseline loss signal, if a cycle add had occurred:
      - take the loss percentage of that baseline exit signal
      - apply the same percentage from the NEW average
      - never move the stop below original-entry -3.0% hard disaster
      - continue managing any still-open added 0.5:
          * new-average recovery => remove added 0.5
          * swing-low rebreak => remove added 0.5
      - core TP remains new-average +2.0%
      - no second add
    This is a direct "same stop percentage, new average reference" comparison,
    not a re-run of every internal 4-stage stop checkpoint.

Post-baseline rescue horizon:
- until 180 minutes from original entry
- if neither TP nor hard/rebased stop is hit by then, close at the last 1m close

Fees:
- taker 0.055% on each modeled fill
- summary reports both gross and after-all-modeled-fees
"""

from __future__ import annotations

import csv, io, json, sqlite3, time, urllib.parse, urllib.request, zipfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

KST = timezone(timedelta(hours=9))
API = "https://api.bybit.com"
OUT = Path("CYCLE_STOP_ABC_LATEST.zip")

ADD_FRAC = 0.50
ARM_DD_PCT = -2.0
REBOUND_PCT = 1.0
TP_PCT = 2.0
HARD_STOP_PCT = -3.0
FEE_PCT = 0.055
MAX_MINUTES = 180

def f(v, default=None):
    try:
        if v is None or str(v).strip() == "":
            return default
        return float(v)
    except Exception:
        return default

def dt_kst(v):
    if not v:
        return None
    s = str(v).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=KST)
        except Exception:
            pass
    try:
        d = datetime.fromisoformat(s.replace("Z","+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=KST)
        return d.astimezone(KST)
    except Exception:
        return None

def dt_utc(v):
    if not v:
        return None
    try:
        d = datetime.fromisoformat(str(v).replace("Z","+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except Exception:
        return None

def pp(price, entry):
    return (price / entry - 1.0) * 100.0 if entry else 0.0

def csv_text(rows):
    if not rows:
        return ""
    s = io.StringIO()
    w = csv.DictWriter(s, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
    return s.getvalue()

def find_exact():
    direct=[]
    for pat in ("SPLIT_EXACT_DETAIL.csv","**/SPLIT_EXACT_DETAIL.csv"):
        direct += list(Path(".").glob(pat))
    direct=[p for p in direct if p.is_file()]
    if direct:
        p=max(direct,key=lambda x:x.stat().st_mtime)
        print("INPUT EXACT CSV =",p)
        return list(csv.DictReader(p.open("r",encoding="utf-8-sig",newline="")))
    zips=sorted(Path(".").glob("SPLIT_ENTRY_EXACT_V2_*.zip"),
                key=lambda x:x.stat().st_mtime,reverse=True)
    if not zips:
        raise RuntimeError("SPLIT_EXACT_DETAIL.csv / SPLIT_ENTRY_EXACT_V2_*.zip not found")
    zp=zips[0]
    print("INPUT EXACT ZIP =",zp)
    with zipfile.ZipFile(zp) as z:
        names=[n for n in z.namelist() if n.endswith("SPLIT_EXACT_DETAIL.csv")]
        if not names:
            raise RuntimeError("SPLIT_EXACT_DETAIL.csv not in exact zip")
        return list(csv.DictReader(io.StringIO(z.read(names[0]).decode("utf-8-sig"))))

def find_recovery_detail():
    zp=Path("RECOVERY_CONFIRM_LATEST.zip")
    if not zp.exists():
        zips=sorted(Path(".").glob("RECOVERY_CONFIRM*.zip"),
                    key=lambda x:x.stat().st_mtime,reverse=True)
        if not zips:
            raise RuntimeError("RECOVERY_CONFIRM_LATEST.zip not found")
        zp=zips[0]
    print("INPUT RECOVERY ZIP =",zp)
    with zipfile.ZipFile(zp) as z:
        names=[n for n in z.namelist() if n.endswith("DETAIL.csv")]
        if not names:
            raise RuntimeError("DETAIL.csv not in recovery zip")
        return list(csv.DictReader(io.StringIO(z.read(names[0]).decode("utf-8-sig"))))

raw=find_exact()
trades={}
for r in raw:
    if str(r.get("mode"))!="A_FULL100":
        continue
    tid=str(r.get("trade_id") or "").strip()
    if not tid or tid in trades:
        continue
    entry=f(r.get("entry_price"))
    base=f(r.get("total_pct"))
    ep=f(r.get("common_exit_price"))
    if entry is None or base is None or ep is None:
        continue
    trades[tid]={
        "period":str(r.get("period") or ""),
        "trade_id":tid,
        "entry_time_kst":str(r.get("entry_time_kst") or ""),
        "symbol":str(r.get("symbol") or ""),
        "entry_price":entry,
        "baseline_pct":base,
        "common_result":str(r.get("common_result") or ""),
        "common_exit_price":ep,
    }

recovery=find_recovery_detail()
v2={}
trigger_ids=[]
for r in recovery:
    if str(r.get("variant"))!="DD2_RB10":
        continue
    tid=str(r.get("trade_id") or "")
    v2[tid]=r
    if int(float(r.get("added") or 0))==1:
        trigger_ids.append(tid)

trigger_ids=[tid for tid in trigger_ids if tid in trades]
print("DD2_RB10 ADDED TRADES =",len(trigger_ids))
if len(trigger_ids)<20:
    raise RuntimeError("too few DD2_RB10 added trades; wrong recovery file?")

# Optional old exact timing table.
old_times={}
old_files=list(Path(".").glob("**/P_SAFE_SAFEA_0901_0917_trades.csv"))
if old_files:
    op=max(old_files,key=lambda x:x.stat().st_mtime)
    print("OLD TIMING =",op)
    with op.open("r",encoding="utf-8-sig",newline="") as fh:
        for r in csv.DictReader(fh):
            tid=str(r.get("trade_id") or "")
            if tid:
                old_times[tid]={
                    "ts":dt_kst(r.get("baseline_result_time_kst")),
                    "price":f(r.get("baseline_result_price")),
                }

DB=Path("bybit_swing_bot.db")
con=None
if DB.exists():
    try:
        con=sqlite3.connect(f"file:{DB.resolve()}?mode=ro",uri=True)
        con.row_factory=sqlite3.Row
        print("DB TIMING = ON")
    except Exception as e:
        print("DB TIMING = OFF",repr(e))

def api_1m(symbol,start,end):
    params={
        "category":"linear","symbol":symbol,"interval":"1",
        "start":int(start.timestamp()*1000),
        "end":int(end.timestamp()*1000),
        "limit":1000,
    }
    url=API+"/v5/market/kline?"+urllib.parse.urlencode(params)
    last=None
    for k in range(8):
        try:
            req=urllib.request.Request(url,headers={"User-Agent":"HJ-CYCLE-ABC/1.0"})
            with urllib.request.urlopen(req,timeout=25) as resp:
                j=json.loads(resp.read().decode("utf-8"))
            if int(j.get("retCode",-1))!=0:
                raise RuntimeError(f"{j.get('retCode')} {j.get('retMsg')}")
            out=[]
            for z in j.get("result",{}).get("list",[]):
                try:
                    out.append({
                        "ts":datetime.fromtimestamp(int(z[0])/1000,tz=timezone.utc),
                        "o":float(z[1]),"h":float(z[2]),"l":float(z[3]),"c":float(z[4]),
                    })
                except Exception:
                    pass
            out.sort(key=lambda x:x["ts"])
            return out
        except Exception as e:
            last=e
            time.sleep(min(8,0.8*(k+1)))
    raise RuntimeError(last)

def first_touch(bars,entry,exit_price):
    if exit_price>=entry:
        for b in bars:
            if b["h"]>=exit_price:
                return b["ts"]
    else:
        for b in bars:
            if b["l"]<=exit_price:
                return b["ts"]
    return None

def db_match_time(t):
    if con is None:
        return None,"NO_DB"
    try:
        rows=con.execute("""
            SELECT variant,result_ts,result_price,result
            FROM research_shadow_reviews
            WHERE setup_id=? AND completed=1
              AND result_ts IS NOT NULL AND result_price IS NOT NULL
        """,(t["trade_id"],)).fetchall()
    except Exception:
        return None,"DB_QUERY_FAIL"
    scored=[]
    for r in rows:
        rp=f(r["result_price"]); ts=dt_utc(r["result_ts"])
        if rp is None or ts is None:
            continue
        d=abs(pp(rp,t["entry_price"])-pp(t["common_exit_price"],t["entry_price"]))
        scored.append((d,ts,str(r["variant"] or ""),str(r["result"] or "")))
    if not scored:
        return None,"DB_NO_MATCH"
    scored.sort(key=lambda x:x[0])
    d,ts,var,res=scored[0]
    if d<=0.08:
        return ts,f"DB_MATCH:{var}:{res}:{d:.4f}pp"
    return None,f"DB_NO_CLOSE:{d:.4f}pp"

def resolve_cutoff(t,bars):
    entry=t["entry_price"]; ep=t["common_exit_price"]
    if t["common_result"]=="TP20":
        x=first_touch(bars,entry,ep)
        if x:
            return x,"TP20_TOUCH",True
    o=old_times.get(t["trade_id"])
    if o and o.get("ts") and o.get("price") is not None:
        if abs(pp(o["price"],entry)-pp(ep,entry))<=0.08:
            return o["ts"].astimezone(timezone.utc),"OLD_EXACT",True
    x,src=db_match_time(t)
    if x:
        return x,src,True
    x=first_touch(bars,entry,ep)
    if x:
        return x,"FALLBACK_TOUCH",False
    return bars[-1]["ts"],"APPROX_END",False

def fee_pp(qfrac,price,entry):
    return qfrac*(price/entry)*FEE_PCT

def full_fee(entry, fills):
    # fills: [(qty_fraction, price), ...], includes exits only.
    total=fee_pp(1.0,entry,entry)  # original core entry
    for q,p in fills:
        total += fee_pp(q,p,entry)
    return total

def process_to_cutoff(t,bars,cutoff):
    """Recreate DD2_RB10 cycle state up to but not including the baseline cutoff minute."""
    entry=t["entry_price"]
    start=(dt_kst(t["entry_time_kst"]).astimezone(timezone.utc)
           .replace(second=0,microsecond=0)+timedelta(minutes=1))
    cf=cutoff.replace(second=0,microsecond=0)
    pre=[b for b in bars if b["ts"]>=start and b["ts"]<cf]

    armed=False; swing_low=None; swing_i=None
    added=False; add_open=False; add_price=None; add_i=None
    avg=entry; new_tp=None
    realized=0.0
    fills=[]  # extra fills only so far: add entry/exit
    false_break=0; recovered=0

    for i,b in enumerate(pre):
        if not added:
            if not armed:
                if b["l"]<=entry*(1+ARM_DD_PCT/100):
                    armed=True; swing_low=b["l"]; swing_i=i
                continue
            if b["l"]<swing_low:
                swing_low=b["l"]; swing_i=i
            if i<=swing_i:
                continue
            trigger=swing_low*(1+REBOUND_PCT/100)
            if b["h"]>=trigger:
                add_price=trigger
                avg=(entry+ADD_FRAC*add_price)/(1+ADD_FRAC)
                new_tp=avg*(1+TP_PCT/100)
                fills.append((ADD_FRAC,add_price))
                added=True; add_open=True; add_i=i
                continue
        else:
            if add_open and i>add_i and b["l"]<=swing_low:
                # Added fraction removed at swing-low.
                realized += ADD_FRAC*(swing_low-avg)/entry*100
                fills.append((ADD_FRAC,swing_low))
                add_open=False; false_break=1
                continue
            if add_open and i>add_i and b["h"]>=avg:
                fills.append((ADD_FRAC,avg))
                add_open=False; recovered=1
                if b["h"]>=new_tp:
                    gross=realized+(new_tp-avg)/entry*100
                    fills.append((1.0,new_tp))
                    return {
                        "done":True,"gross":gross,"fills":fills,"exit":"TP_BEFORE_BASELINE",
                        "exit_price":new_tp,"added":added,"add_open":False,"avg":avg,
                        "new_tp":new_tp,"swing_low":swing_low,"add_price":add_price,
                        "false_break":false_break,"recovered":recovered,
                    }
            if (not add_open) and b["h"]>=new_tp:
                gross=realized+(new_tp-avg)/entry*100
                fills.append((1.0,new_tp))
                return {
                    "done":True,"gross":gross,"fills":fills,"exit":"TP_BEFORE_BASELINE",
                    "exit_price":new_tp,"added":added,"add_open":False,"avg":avg,
                    "new_tp":new_tp,"swing_low":swing_low,"add_price":add_price,
                    "false_break":false_break,"recovered":recovered,
                }

    return {
        "done":False,"gross":None,"fills":fills,"exit":"",
        "added":added,"add_open":add_open,"avg":avg,"new_tp":new_tp,
        "swing_low":swing_low,"add_price":add_price,
        "realized":realized,"false_break":false_break,"recovered":recovered,
    }

def settle_baseline_A(t,state,final_bar):
    """Current V2 behavior at baseline cutoff."""
    entry=t["entry_price"]; base=t["common_exit_price"]
    avg=state["avg"]; realized=state.get("realized",0.0)
    fills=list(state["fills"]); add_open=state["add_open"]

    # Original TP20 cutoff after a prior cycle: lower new TP must be crossed by this rally.
    if t["common_result"]=="TP20" and state["added"]:
        if add_open:
            if final_bar and final_bar["l"]<=state["swing_low"]:
                realized += ADD_FRAC*(state["swing_low"]-avg)/entry*100
                fills.append((ADD_FRAC,state["swing_low"]))
                add_open=False
            else:
                fills.append((ADD_FRAC,avg))
                add_open=False
        px=state["new_tp"]
        gross=realized+(px-avg)/entry*100
        fills.append((1.0,px))
        return gross,fills,px,"A_TP_CYCLE"

    if not state["added"]:
        gross=t["baseline_pct"]
        fills.append((1.0,base))
        return gross,fills,base,"A_BASE_NO_ADD"

    if add_open:
        realized += ADD_FRAC*(base-avg)/entry*100
        fills.append((ADD_FRAC,base))
        add_open=False
    gross=realized+(base-avg)/entry*100
    fills.append((1.0,base))
    return gross,fills,base,"A_BASE_FULL"

def post_bars_after(bars,cutoff):
    cf=cutoff.replace(second=0,microsecond=0)
    # Start on NEXT full minute; no optimistic same-minute ordering after the baseline signal.
    return [b for b in bars if b["ts"]>cf]

def settle_B(t,state,bars,cutoff):
    # If already done before baseline, caller won't enter.
    entry=t["entry_price"]; base=t["common_exit_price"]
    if t["common_result"]!="BASELINE_LOSS" or not state["added"] or not state["add_open"]:
        return settle_baseline_A(t,state,next((b for b in bars if b["ts"]==cutoff.replace(second=0,microsecond=0)),None))

    hard=entry*(1+HARD_STOP_PCT/100)
    # If baseline stop itself is already at/below the hard floor, don't rescue it.
    if base<=hard:
        return settle_baseline_A(t,state,next((b for b in bars if b["ts"]==cutoff.replace(second=0,microsecond=0)),None))

    avg=state["avg"]; realized=state.get("realized",0.0)
    fills=list(state["fills"])
    # At baseline stop, remove only the added 0.5.
    realized += ADD_FRAC*(base-avg)/entry*100
    fills.append((ADD_FRAC,base))

    for b in post_bars_after(bars,cutoff):
        hit_hard=b["l"]<=hard
        hit_tp=b["h"]>=state["new_tp"]
        if hit_hard:
            px=hard
            gross=realized+(px-avg)/entry*100
            fills.append((1.0,px))
            return gross,fills,px,"B_HARD3"
        if hit_tp:
            px=state["new_tp"]
            gross=realized+(px-avg)/entry*100
            fills.append((1.0,px))
            return gross,fills,px,"B_TP_AFTER_ADD_ONLY_CUT"

    post=post_bars_after(bars,cutoff)
    px=(post[-1]["c"] if post else base)
    gross=realized+(px-avg)/entry*100
    fills.append((1.0,px))
    return gross,fills,px,"B_180M_TIMEOUT"

def settle_C(t,state,bars,cutoff):
    entry=t["entry_price"]; base=t["common_exit_price"]
    if t["common_result"]!="BASELINE_LOSS" or not state["added"]:
        return settle_baseline_A(t,state,next((b for b in bars if b["ts"]==cutoff.replace(second=0,microsecond=0)),None))

    loss_pct=pp(base,entry)
    if loss_pct>=0:
        return settle_baseline_A(t,state,next((b for b in bars if b["ts"]==cutoff.replace(second=0,microsecond=0)),None))

    avg=state["avg"]; realized=state.get("realized",0.0)
    fills=list(state["fills"]); add_open=state["add_open"]
    hard=entry*(1+HARD_STOP_PCT/100)
    rebased=avg*(1+loss_pct/100)
    stop=max(rebased,hard)

    # If baseline signal was already at/below hard disaster, no extra room.
    if base<=hard:
        return settle_baseline_A(t,state,next((b for b in bars if b["ts"]==cutoff.replace(second=0,microsecond=0)),None))

    for b in post_bars_after(bars,cutoff):
        # Full stop first on same-minute ambiguity.
        if b["l"]<=stop:
            qty=1.0+(ADD_FRAC if add_open else 0.0)
            if add_open:
                realized += ADD_FRAC*(stop-avg)/entry*100
                fills.append((ADD_FRAC,stop))
                add_open=False
            gross=realized+(stop-avg)/entry*100
            fills.append((1.0,stop))
            return gross,fills,stop,"C_REBASED_STOP"

        # Cycle management continues while added fraction is open.
        if add_open and b["l"]<=state["swing_low"]:
            realized += ADD_FRAC*(state["swing_low"]-avg)/entry*100
            fills.append((ADD_FRAC,state["swing_low"]))
            add_open=False
            # Same-minute rebound ignored after false break.
            continue

        if add_open and b["h"]>=avg:
            fills.append((ADD_FRAC,avg))
            add_open=False
            if b["h"]>=state["new_tp"]:
                px=state["new_tp"]
                gross=realized+(px-avg)/entry*100
                fills.append((1.0,px))
                return gross,fills,px,"C_TP_AFTER_RECOVERY"

        if (not add_open) and b["h"]>=state["new_tp"]:
            px=state["new_tp"]
            gross=realized+(px-avg)/entry*100
            fills.append((1.0,px))
            return gross,fills,px,"C_TP"

    post=post_bars_after(bars,cutoff)
    px=(post[-1]["c"] if post else base)
    if add_open:
        realized += ADD_FRAC*(px-avg)/entry*100
        fills.append((ADD_FRAC,px))
        add_open=False
    gross=realized+(px-avg)/entry*100
    fills.append((1.0,px))
    return gross,fills,px,"C_180M_TIMEOUT"

def calc_net(entry,gross,fills):
    return gross-full_fee(entry,fills)

details=[]
errors=[]
audit_diffs=[]

for n,tid in enumerate(trigger_ids,1):
    t=trades[tid]
    try:
        ek=dt_kst(t["entry_time_kst"])
        start=ek.astimezone(timezone.utc).replace(second=0,microsecond=0)
        end=start+timedelta(minutes=MAX_MINUTES+2)
        bars=api_1m(t["symbol"],start,end)
        if not bars:
            raise RuntimeError("no bars")
        cutoff,tsrc,strict=resolve_cutoff(t,bars)
        if cutoff>end:
            cutoff=end; strict=False; tsrc+=":CLAMP180"
        final_bar=next((b for b in bars if b["ts"]==cutoff.replace(second=0,microsecond=0)),None)
        state=process_to_cutoff(t,bars,cutoff)

        # If our reconstruction exits before baseline, A/B/C are identical.
        if state["done"]:
            grossA=state["gross"]; fillsA=list(state["fills"]); pxA=state["exit_price"]; reasonA=state["exit"]
            grossB,fillsB,pxB,reasonB=grossA,list(fillsA),pxA,reasonA
            grossC,fillsC,pxC,reasonC=grossA,list(fillsA),pxA,reasonA
        else:
            grossA,fillsA,pxA,reasonA=settle_baseline_A(t,state,final_bar)
            grossB,fillsB,pxB,reasonB=settle_B(t,state,bars,cutoff)
            grossC,fillsC,pxC,reasonC=settle_C(t,state,bars,cutoff)

        # Audit against previous V2 incremental-net row.
        prior=v2.get(tid,{})
        prior_net=f(prior.get("net_pct"))
        # Prior V2 net excluded the ordinary core entry/exit fees.
        # Reconstruct comparable A incremental net: gross - cycle-only fees.
        cycle_feeA=0.0
        # all fills except original core entry; identify extra add fills approximately from state/fills
        # easier: previous prior_net is only an audit reference, not used in final summary.
        if prior_net is not None:
            # all-fee net differs by ordinary core fees; compare gross only to prior gross if available.
            pg=f(prior.get("gross_pct"))
            if pg is not None:
                audit_diffs.append(abs(grossA-pg))

        vals=[
            ("A_CURRENT_STOP",grossA,fillsA,pxA,reasonA),
            ("B_ADD_ONLY_AT_BASE_STOP",grossB,fillsB,pxB,reasonB),
            ("C_REBASE_STOP",grossC,fillsC,pxC,reasonC),
        ]
        for var,gross,fills,px,reason in vals:
            net=calc_net(t["entry_price"],gross,fills)
            details.append({
                "period":t["period"],"date":t["entry_time_kst"][:10],
                "trade_id":tid,"symbol":t["symbol"],"entry_time_kst":t["entry_time_kst"],
                "variant":var,"baseline_result":t["common_result"],
                "baseline_pct":round(t["baseline_pct"],6),
                "cycle_add_price_pct":round(pp(state["add_price"],t["entry_price"]),4) if state.get("add_price") else "",
                "cycle_new_avg_pct":round(pp(state["avg"],t["entry_price"]),4) if state.get("added") else "",
                "cycle_swing_low_pct":round(pp(state["swing_low"],t["entry_price"]),4) if state.get("swing_low") else "",
                "add_open_at_baseline":int(bool(state.get("add_open"))),
                "gross_pct":round(gross,6),
                "all_modeled_fee_pct":round(full_fee(t["entry_price"],fills),6),
                "net_after_all_fee_pct":round(net,6),
                "exit_price_pct":round(pp(px,t["entry_price"]),4),
                "exit_reason":reason,
                "timing_source":tsrc,"strict_timing":int(strict),
            })

        if n%10==0:
            print(f"{n}/{len(trigger_ids)} complete")
        time.sleep(0.10)

    except Exception as e:
        errors.append({"trade_id":tid,"symbol":t["symbol"],"error":repr(e)})

def summarize(rows,period="ALL",strict_only=False):
    out=[]
    variants=["A_CURRENT_STOP","B_ADD_ONLY_AT_BASE_STOP","C_REBASE_STOP"]
    for v in variants:
        z=[r for r in rows if r["variant"]==v]
        if period!="ALL":
            z=[r for r in z if r["period"]==period]
        if strict_only:
            z=[r for r in z if int(r["strict_timing"])==1]
        if not z:
            continue
        z=sorted(z,key=lambda x:x["entry_time_kst"])
        gross=sum(float(r["gross_pct"]) for r in z)
        net=sum(float(r["net_after_all_fee_pct"]) for r in z)
        base=sum(float(r["baseline_pct"]) for r in z)
        eq=peak=mdd=0.0
        for r in z:
            eq+=float(r["net_after_all_fee_pct"])
            peak=max(peak,eq); mdd=min(mdd,eq-peak)
        out.append({
            "scope":"STRICT" if strict_only else "ALL_TIMING",
            "period":period,"variant":v,"trades":len(z),
            "baseline_gross_sum":round(base,4),
            "strategy_gross_sum":round(gross,4),
            "strategy_net_all_fees":round(net,4),
            "gross_delta_vs_A":"",
            "net_delta_vs_A":"",
            "mdd_net_pctp":round(mdd,4),
            "tp_exits":sum("TP" in str(r["exit_reason"]) for r in z),
            "hard_or_stop_exits":sum(("STOP" in str(r["exit_reason"]) or "HARD" in str(r["exit_reason"])) for r in z),
            "timeouts":sum("TIMEOUT" in str(r["exit_reason"]) for r in z),
        })
    amap={r["variant"]:r for r in out}
    if "A_CURRENT_STOP" in amap:
        a=amap["A_CURRENT_STOP"]
        for r in out:
            r["gross_delta_vs_A"]=round(r["strategy_gross_sum"]-a["strategy_gross_sum"],4)
            r["net_delta_vs_A"]=round(r["strategy_net_all_fees"]-a["strategy_net_all_fees"],4)
    return out

summary=[]
for strict in (False,True):
    for per in ("ALL","0901_0917","0918_0921"):
        summary+=summarize(details,per,strict)

daily=[]
for v in ("A_CURRENT_STOP","B_ADD_ONLY_AT_BASE_STOP","C_REBASE_STOP"):
    dates=sorted(set(r["date"] for r in details if r["variant"]==v))
    for d in dates:
        z=[r for r in details if r["variant"]==v and r["date"]==d]
        daily.append({
            "date":d,"variant":v,"trades":len(z),
            "gross":round(sum(float(r["gross_pct"]) for r in z),4),
            "net_all_fees":round(sum(float(r["net_after_all_fee_pct"]) for r in z),4),
        })

audit={
    "triggered_trades":len(trigger_ids),
    "completed_trades":len(set(r["trade_id"] for r in details)),
    "errors":len(errors),
    "A_vs_prior_V2_gross_max_abs_diff":round(max(audit_diffs),6) if audit_diffs else None,
    "A_vs_prior_V2_gross_mean_abs_diff":round(sum(audit_diffs)/len(audit_diffs),6) if audit_diffs else None,
}

readme=f"""Cycle Stop A/B/C comparison

Candidate:
-2.0% arm -> observed swing-low +1.0% rebound -> add 50%, max 1 cycle
new average +2.0% TP

A_CURRENT_STOP:
baseline stop remains full exit.

B_ADD_ONLY_AT_BASE_STOP:
if baseline loss stop arrives while added 50% is still open and baseline stop is above original -3% hard floor,
close only added 50%; keep original core until new-average +2% TP or original -3% hard floor.
No second add. 180m research timeout.

C_REBASE_STOP:
at baseline loss signal after a cycle add, take that signal's loss percentage and apply it from the new average.
Stop is capped at original-entry -3% hard floor.
No second add. 180m research timeout.
This is a direct rebase comparison, not a full reconstruction of every internal 4-stage checkpoint.

Fees:
0.055% taker on every modeled fill, including original core entry and final exit.

AUDIT:
{json.dumps(audit,ensure_ascii=False,indent=2)}
"""

with zipfile.ZipFile(OUT,"w",zipfile.ZIP_DEFLATED) as z:
    z.writestr("ABC_SUMMARY.csv",csv_text(summary).encode("utf-8-sig"))
    z.writestr("ABC_DAILY.csv",csv_text(daily).encode("utf-8-sig"))
    z.writestr("ABC_DETAIL.csv",csv_text(details).encode("utf-8-sig"))
    z.writestr("README.txt",readme.encode("utf-8"))
    if errors:
        z.writestr("ERRORS.csv",csv_text(errors).encode("utf-8-sig"))

print()
print("========================================")
print(" CYCLE STOP A/B/C COMPLETE")
print("========================================")
print("TRIGGERED =",len(trigger_ids))
print("ERRORS =",len(errors))
print("AUDIT =",audit)
print("ZIP =",OUT.resolve())
print()
for r in summary:
    if r["scope"]=="ALL_TIMING" and r["period"] in ("ALL","0901_0917","0918_0921"):
        print(r["period"],r["variant"],
              "GROSS",r["strategy_gross_sum"],
              "NET",r["strategy_net_all_fees"],
              "DELTA_A",r["net_delta_vs_A"],
              "MDD",r["mdd_net_pctp"])
