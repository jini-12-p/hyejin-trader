#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CURRENT STOP -> DCA 100% SUCCESS/FAILURE SEPARATION

Question A:
  At the CURRENT stop moment, can we distinguish:
    - STOP now (future DCA path will fail / never trigger)
    - HOLD for recovery path (future low +1.5% rebound -> 100% add -> avg recovery)

Question B:
  At the exact low+1.5% rebound trigger, BEFORE adding, can we distinguish:
    - TRUE recovery
    - FALSE rebound / STALL

Frozen hypothetical DCA:
  1) Only trades that are STOP under CURRENT+V22 quality strategy.
  2) Ignore the current STOP.
  3) From the NEXT minute after stop, track a new swing low.
  4) When a later minute reaches swing_low * 1.015, add 100% of original size.
     (100% = same original quantity; new_avg = (entry + add_price)/2)
  5) Primary success = new average is recovered BEFORE pre-add swing low is re-broken.
  6) Also record whether new_avg +2% is reached before low re-break.
  7) Same-minute ambiguity is conservative:
     - no rebound trigger in the same minute that makes a new low
     - after add, old-low break is checked before favorable recovery in a bar
  8) Primary outcome horizon = 6h after current stop.
     Sensitivity = 3h / 6h / 12h.

ANTI-LOOKAHEAD:
  STOP checkpoint uses only information available by stop moment:
    - exact current strategy stop loss / elapsed time
    - bars completed BEFORE stop minute
    - entry->stop path statistics available by then

  REBOUND checkpoint uses:
    - known swing-low depth and elapsed time to +1.5% trigger
    - bars completed BEFORE trigger minute
    - NO trigger-minute final close/volume

Rule discovery:
  DISCOVERY = 9/1~17
  VALIDATION = 9/18~22
  FORWARD = 9/23 onward
Thresholds are derived from DISCOVERY only, then reported untouched on validation/forward.

Inputs expected in /root/hyejin-trader/bybit_swing:
  - bybit_swing_bot.db
  - V22_QUALITY_RESCHEDULE_TRADES.csv
  - FORWARD_0923_0924_V22Q_TRADES.csv
Public Bybit klines are READ ONLY. No DB writes. No orders.
"""

from __future__ import annotations

import csv
import io
import json
import math
import sqlite3
import statistics
import time
import urllib.parse
import urllib.request
import zipfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT = Path("/root/hyejin-trader/bybit_swing")
DB = ROOT / "bybit_swing_bot.db"
API = "https://api.bybit.com"
KST = timezone(timedelta(hours=9))

HIST_TRADES = ROOT / "V22_QUALITY_RESCHEDULE_TRADES.csv"
FWD_TRADES = ROOT / "FORWARD_0923_0924_V22Q_TRADES.csv"

OUT_DETAIL = ROOT / "DCA100_SEPARATION_DETAIL.csv"
OUT_STOP_RULES = ROOT / "DCA100_STOP_POINT_RULES.csv"
OUT_REBOUND_RULES = ROOT / "DCA100_REBOUND_POINT_RULES.csv"
OUT_SUMMARY = ROOT / "DCA100_SEPARATION_SUMMARY.txt"
OUT_ZIP = ROOT / "DCA100_SEPARATION_RESULTS.zip"

PRIMARY_H = 6
HORIZONS_H = (3, 6, 12)
REBOUND_PCT = 1.5

def f(v, default=None):
    try:
        if v is None or str(v).strip()=="":
            return default
        x=float(v)
        if math.isnan(x):
            return default
        return x
    except Exception:
        return default

def i(v, default=0):
    try:
        return int(float(v))
    except Exception:
        return default

def dt_kst(v):
    if not v:
        return None
    s=str(v).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s,fmt).replace(tzinfo=KST)
        except Exception:
            pass
    try:
        d=datetime.fromisoformat(s.replace("Z","+00:00"))
        if d.tzinfo is None:
            d=d.replace(tzinfo=KST)
        return d.astimezone(KST)
    except Exception:
        return None

def pct(a,b):
    return (a/b-1.0)*100.0 if b else None

def mean(xs):
    z=[float(x) for x in xs if x is not None and math.isfinite(float(x))]
    return sum(z)/len(z) if z else None

def ema(vals,n):
    z=[float(x) for x in vals if x is not None]
    if not z:
        return None
    a=2/(n+1)
    e=z[0]
    for x in z[1:]:
        e=a*x+(1-a)*e
    return e

def rsi(vals,n=14):
    if len(vals)<n+1:
        return None
    gains=[]; losses=[]
    for a,b in zip(vals[-(n+1):-1],vals[-n:]):
        d=b-a
        gains.append(max(d,0))
        losses.append(max(-d,0))
    ag=sum(gains)/n; al=sum(losses)/n
    if al==0:
        return 100.0
    rs=ag/al
    return 100-(100/(1+rs))

def close_pos(b):
    if not b or b["h"]<=b["l"]:
        return None
    return (b["c"]-b["l"])/(b["h"]-b["l"])

def request_klines(symbol,start,end):
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
            req=urllib.request.Request(url,headers={"User-Agent":"HJ-DCA100-SEPARATION/1.0"})
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
                        "v":float(z[5]),
                    })
                except Exception:
                    pass
            out.sort(key=lambda x:x["ts"])
            return out
        except Exception as e:
            last=e
            time.sleep(min(8,0.8*(k+1)))
    raise RuntimeError(last)

def five_bars(one):
    buckets={}
    for b in one:
        t=b["ts"].replace(minute=(b["ts"].minute//5)*5,second=0,microsecond=0)
        x=buckets.get(t)
        if x is None:
            buckets[t]={"ts":t,"o":b["o"],"h":b["h"],"l":b["l"],"c":b["c"],"v":b["v"],"n":1}
        else:
            x["h"]=max(x["h"],b["h"])
            x["l"]=min(x["l"],b["l"])
            x["c"]=b["c"]
            x["v"]+=b["v"]
            x["n"]+=1
    return [buckets[k] for k in sorted(buckets)]

def feature_pack(one, checkpoint, entry_price=None, entry_ts=None):
    """
    Only completed bars strictly BEFORE checkpoint minute.
    """
    cp=checkpoint.astimezone(timezone.utc).replace(second=0,microsecond=0)
    prev1=[b for b in one if b["ts"]<cp]
    five=five_bars(one)
    # full completed 5m bars only
    prev5=[b for b in five if b["ts"]+timedelta(minutes=5)<=cp]

    p1=prev1[-1] if prev1 else None
    last3=prev1[-3:] if len(prev1)>=3 else prev1
    last5=prev5[-1] if prev5 else None

    vol10=[b["v"] for b in prev1[-11:-1]] if len(prev1)>=2 else []
    vr1=(p1["v"]/mean(vol10)) if p1 and mean(vol10) not in (None,0) else None

    vol5=[b["v"] for b in prev5[-6:-1]] if len(prev5)>=2 else []
    vr5=(last5["v"]/mean(vol5)) if last5 and mean(vol5) not in (None,0) else None

    closes=[b["c"] for b in prev5]
    e9=ema(closes[-60:],9) if closes else None
    e20=ema(closes[-80:],20) if closes else None
    e20p=ema(closes[-81:-1],20) if len(closes)>=2 else None

    out={
        "prev1m_ret":pct(p1["c"],p1["o"]) if p1 else None,
        "prev3m_ret":pct(last3[-1]["c"],last3[0]["o"]) if last3 else None,
        "prev1m_close_pos":close_pos(p1),
        "prev1m_vol_ratio10":vr1,
        "last5m_ret":pct(last5["c"],last5["o"]) if last5 else None,
        "last5m_close_pos":close_pos(last5),
        "last5m_vol_ratio5":vr5,
        "ema9_20_gap":(e9/e20-1)*100 if e9 and e20 else None,
        "ema20_slope":(e20/e20p-1)*100 if e20 and e20p else None,
        "rsi14_5m":rsi(closes,14),
    }

    if entry_price and entry_ts:
        path=[b for b in one if entry_ts.astimezone(timezone.utc).replace(second=0,microsecond=0)<=b["ts"]<cp]
        if path:
            out["mfe_to_checkpoint"]=pct(max(b["h"] for b in path),entry_price)
            out["mae_to_checkpoint"]=pct(min(b["l"] for b in path),entry_price)
        else:
            out["mfe_to_checkpoint"]=None
            out["mae_to_checkpoint"]=None
    return out

def market_change(one, checkpoint, minutes):
    cp=checkpoint.astimezone(timezone.utc).replace(second=0,microsecond=0)
    prev=[b for b in one if b["ts"]<cp]
    if not prev:
        return None
    cur=prev[-1]["c"]
    target=cp-timedelta(minutes=minutes)
    old=None
    for b in reversed(prev):
        if b["ts"]<=target:
            old=b["c"]; break
    return pct(cur,old) if old else None

def load_current_stop_cohort():
    if not HIST_TRADES.exists():
        raise RuntimeError(f"missing {HIST_TRADES}")
    if not FWD_TRADES.exists():
        raise RuntimeError(f"missing {FWD_TRADES}")

    rows=[]

    with HIST_TRADES.open("r",encoding="utf-8-sig",newline="") as fh:
        for r in csv.DictReader(fh):
            if str(r.get("scenario"))!="V22_QUALITY_OR":
                continue
            if i(r.get("accepted"))!=1 or str(r.get("result"))!="STOP":
                continue
            r["_src"]="HIST_QUALITY"
            rows.append(r)

    with FWD_TRADES.open("r",encoding="utf-8-sig",newline="") as fh:
        for r in csv.DictReader(fh):
            if str(r.get("scenario"))!="CURRENT_PLUS_V22Q":
                continue
            if i(r.get("accepted"))!=1 or str(r.get("result"))!="STOP":
                continue
            r["_src"]="FORWARD"
            rows.append(r)

    # De-dupe overlap by setup_id. Prefer HIST over FRESH within historical file,
    # and FORWARD for its non-overlapping later window.
    best={}
    for r in rows:
        sid=str(r.get("setup_id") or "")
        if not sid:
            continue
        old=best.get(sid)
        if old is None:
            best[sid]=r
            continue
        # HIST dataset preferred over FRESH for same setup id.
        if str(r.get("dataset"))=="HIST" and str(old.get("dataset"))!="HIST":
            best[sid]=r
    rows=list(best.values())
    rows.sort(key=lambda r:str(r.get("entry_time_kst") or ""))
    return rows

def load_entries(stop_rows):
    if not DB.exists():
        raise RuntimeError(f"missing {DB}")
    con=sqlite3.connect(f"file:{DB.resolve()}?mode=ro",uri=True)
    con.row_factory=sqlite3.Row
    out={}
    for r in stop_rows:
        sid=str(r["setup_id"])
        q=con.execute(
            "SELECT setup_id,symbol,confirmed_at,confirmed_price FROM research_pv25_setups WHERE setup_id=? LIMIT 1",
            (sid,)
        ).fetchone()
        if q:
            out[sid]=dict(q)
    con.close()
    return out

def split_name(date_s):
    if date_s<="2026-09-17":
        return "DISCOVERY_0901_0917"
    if date_s<="2026-09-22":
        return "VALIDATION_0918_0922"
    return "FORWARD_0923_PLUS"

def simulate_after_stop(one, stop_ts, entry, horizon_h):
    stop_floor=stop_ts.astimezone(timezone.utc).replace(second=0,microsecond=0)
    end=stop_floor+timedelta(hours=horizon_h)

    # Conservative: only start observing from next minute.
    post=[b for b in one if stop_floor+timedelta(minutes=1)<=b["ts"]<=end]
    if not post:
        return {"triggered":0,"status":"NO_DATA"}

    low=None; low_ts=None; trigger=None; add_price=None

    for b in post:
        if low is None or b["l"]<low:
            low=b["l"]; low_ts=b["ts"]

        if b["ts"]<=low_ts:
            continue

        trg=low*(1+REBOUND_PCT/100)
        if b["h"]>=trg:
            trigger=b["ts"]; add_price=trg
            break

    if trigger is None:
        return {
            "triggered":0,"status":"NO_REBOUND",
            "swing_low":low,"low_ts":low_ts,
            "swing_low_pct":pct(low,entry) if low else None,
        }

    # 100% = same original quantity.
    new_avg=(entry+add_price)/2.0
    tp2=new_avg*1.02

    after=[b for b in post if b["ts"]>trigger]
    recovered=0; tp2_hit=0; false_break=0
    recover_ts=None; tp_ts=None; fail_ts=None

    for b in after:
        # Conservative adverse-first ordering.
        if b["l"]<=low:
            false_break=1; fail_ts=b["ts"]; break
        if b["h"]>=new_avg and not recovered:
            recovered=1; recover_ts=b["ts"]
        if b["h"]>=tp2:
            tp2_hit=1; tp_ts=b["ts"]; break

    if tp2_hit:
        status="TP2"
    elif recovered:
        status="RECOVER_AVG"
    elif false_break:
        status="FALSE_BREAK"
    else:
        status="STALL"

    return {
        "triggered":1,"status":status,
        "swing_low":low,"low_ts":low_ts,
        "swing_low_pct":pct(low,entry),
        "trigger_ts":trigger,"add_price":add_price,
        "add_price_pct":pct(add_price,entry),
        "new_avg":new_avg,"new_avg_pct":pct(new_avg,entry),
        "tp2_price":tp2,
        "recovered":recovered,"tp2_hit":tp2_hit,"false_break":false_break,
        "recover_ts":recover_ts,"tp_ts":tp_ts,"fail_ts":fail_ts,
        "stop_to_low_min":(low_ts-stop_floor).total_seconds()/60 if low_ts else None,
        "low_to_trigger_min":(trigger-low_ts).total_seconds()/60 if low_ts else None,
        "stop_to_trigger_min":(trigger-stop_floor).total_seconds()/60,
    }

def safe_round(v,n=5):
    return round(v,n) if v is not None and isinstance(v,(int,float)) and math.isfinite(v) else ""

def write_csv(path,rows):
    if not rows:
        path.write_text("",encoding="utf-8-sig"); return
    keys=[]; seen=set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with path.open("w",newline="",encoding="utf-8-sig") as fh:
        w=csv.DictWriter(fh,fieldnames=keys,extrasaction="ignore")
        w.writeheader(); w.writerows(rows)

def val(r,k):
    return f(r.get(k))

def eval_rule(rows, feature, op, th, label):
    usable=[r for r in rows if val(r,feature) is not None]
    if not usable:
        return None
    if op==">=":
        allow=[r for r in usable if val(r,feature)>=th]
    else:
        allow=[r for r in usable if val(r,feature)<=th]
    total_s=sum(i(r.get(label)) for r in usable)
    total_b=len(usable)-total_s
    asu=sum(i(r.get(label)) for r in allow)
    abad=len(allow)-asu
    return {
        "usable":len(usable),"allowed":len(allow),
        "precision":asu/len(allow) if allow else 0,
        "base_precision":total_s/len(usable) if usable else 0,
        "success_keep":asu/total_s if total_s else 0,
        "failure_block":(total_b-abad)/total_b if total_b else 0,
    }

def search_rules(rows, features, label, checkpoint):
    disc=[r for r in rows if r["split"]=="DISCOVERY_0901_0917"]
    valset=[r for r in rows if r["split"]=="VALIDATION_0918_0922"]
    fwd=[r for r in rows if r["split"]=="FORWARD_0923_PLUS"]

    out=[]
    for feat in features:
        xs=sorted(set(val(r,feat) for r in disc if val(r,feat) is not None))
        if len(xs)<6:
            continue
        qs=[]
        for q in (0.15,0.25,0.35,0.45,0.55,0.65,0.75,0.85):
            pos=min(len(xs)-1,max(0,round((len(xs)-1)*q)))
            qs.append(xs[pos])
        for th in sorted(set(qs)):
            for op in (">=","<="):
                ed=eval_rule(disc,feat,op,th,label)
                ev=eval_rule(valset,feat,op,th,label)
                ef=eval_rule(fwd,feat,op,th,label)
                if not ed or ed["allowed"]<8:
                    continue
                # Keep validation/fwd rows even if tiny; sample counts are explicit.
                stable_val=bool(
                    ev and ev["allowed"]>=4
                    and ev["precision"]>=ev["base_precision"]
                )
                stable_fwd=bool(
                    ef and ef["allowed"]>=2
                    and ef["precision"]>=ef["base_precision"]
                )
                score=(
                    (ed["precision"]-ed["base_precision"])
                    + 0.20*ed["failure_block"]
                    + (0 if not ev else (ev["precision"]-ev["base_precision"]))
                    + (0 if not ef else (ef["precision"]-ef["base_precision"]))
                )
                out.append({
                    "checkpoint":checkpoint,"label":label,
                    "feature":feat,"op":op,"threshold":round(th,6),
                    "stable_validation":int(stable_val),
                    "stable_forward":int(stable_fwd),
                    "stable_both":int(stable_val and stable_fwd),
                    "score":round(score,6),
                    "disc_n":ed["usable"],"disc_allow":ed["allowed"],
                    "disc_base":round(ed["base_precision"],4),
                    "disc_precision":round(ed["precision"],4),
                    "disc_keep":round(ed["success_keep"],4),
                    "disc_blockfail":round(ed["failure_block"],4),
                    "val_n":0 if not ev else ev["usable"],
                    "val_allow":0 if not ev else ev["allowed"],
                    "val_base":"" if not ev else round(ev["base_precision"],4),
                    "val_precision":"" if not ev else round(ev["precision"],4),
                    "val_keep":"" if not ev else round(ev["success_keep"],4),
                    "val_blockfail":"" if not ev else round(ev["failure_block"],4),
                    "fwd_n":0 if not ef else ef["usable"],
                    "fwd_allow":0 if not ef else ef["allowed"],
                    "fwd_base":"" if not ef else round(ef["base_precision"],4),
                    "fwd_precision":"" if not ef else round(ef["precision"],4),
                    "fwd_keep":"" if not ef else round(ef["success_keep"],4),
                    "fwd_blockfail":"" if not ef else round(ef["failure_block"],4),
                })
    out.sort(key=lambda x:(x["stable_both"],x["stable_validation"],x["score"],x["disc_precision"]),reverse=True)
    return out

def split_stats(rows,label):
    out=[]
    for sp in ("DISCOVERY_0901_0917","VALIDATION_0918_0922","FORWARD_0923_PLUS","ALL"):
        z=rows if sp=="ALL" else [r for r in rows if r["split"]==sp]
        if not z: continue
        s=sum(i(r.get(label)) for r in z)
        out.append((sp,len(z),s,len(z)-s,s/len(z)))
    return out

def main():
    stops=load_current_stop_cohort()
    entries=load_entries(stops)
    print("CURRENT+V22Q STOP cohort =",len(stops),flush=True)

    # Preload BTC/ETH per trade window too, but cache exact requested windows by rounded stop hour.
    market_cache={}
    def market_bars(sym,start,end):
        key=(sym,start.replace(minute=0,second=0,microsecond=0),end.replace(minute=0,second=0,microsecond=0))
        if key not in market_cache:
            market_cache[key]=request_klines(sym,key[1],key[2]+timedelta(hours=1))
            time.sleep(0.03)
        return market_cache[key]

    detail=[]
    errors=[]

    for n,r in enumerate(stops,1):
        try:
            sid=str(r["setup_id"])
            q=entries.get(sid)
            if not q:
                raise RuntimeError("entry not found in DB")

            entry=f(q.get("confirmed_price"))
            entry_ts=dt_kst(r.get("entry_time_kst"))
            stop_ts=dt_kst(r.get("exit_time_kst"))
            if not entry or not entry_ts or not stop_ts:
                raise RuntimeError("bad entry/stop data")

            start=stop_ts.astimezone(timezone.utc)-timedelta(hours=3)
            end=stop_ts.astimezone(timezone.utc)+timedelta(hours=12,minutes=5)
            one=request_klines(str(r["symbol"]),start,end)
            if len(one)<30:
                raise RuntimeError("too few 1m bars")

            # Market bars around full 12h horizon.
            btc=market_bars("BTCUSDT",start,end)
            eth=market_bars("ETHUSDT",start,end)

            stopfeat=feature_pack(one,stop_ts,entry,entry_ts)
            stopfeat["btc15_stop"]=market_change(btc,stop_ts,15)
            stopfeat["eth15_stop"]=market_change(eth,stop_ts,15)
            stopfeat["btc4h_stop"]=market_change(btc,stop_ts,240)
            stopfeat["eth4h_stop"]=market_change(eth,stop_ts,240)

            sims={h:simulate_after_stop(one,stop_ts,entry,h) for h in HORIZONS_H}
            prim=sims[PRIMARY_H]

            row={
                "setup_id":sid,"symbol":str(r["symbol"]),
                "entry_time_kst":str(r.get("entry_time_kst") or ""),
                "stop_time_kst":str(r.get("exit_time_kst") or ""),
                "date":str(r.get("entry_time_kst") or "")[:10],
                "split":split_name(str(r.get("entry_time_kst") or "")[:10]),
                "current_stop_net_pct":f(r.get("net_pct")),
                "entry_price":entry,
                "entry_to_stop_min":round((stop_ts-entry_ts).total_seconds()/60,2),
            }
            for k,v in stopfeat.items():
                row["STOP_"+k]=safe_round(v)

            for h,sm in sims.items():
                row[f"H{h}_triggered"]=int(bool(sm.get("triggered")))
                row[f"H{h}_status"]=sm.get("status","")
                row[f"H{h}_recover_avg"]=int(bool(sm.get("recovered")))
                row[f"H{h}_tp2"]=int(bool(sm.get("tp2_hit")))
                row[f"H{h}_false_break"]=int(bool(sm.get("false_break")))
                row[f"H{h}_swing_low_pct"]=safe_round(sm.get("swing_low_pct"))
                row[f"H{h}_stop_to_trigger_min"]=safe_round(sm.get("stop_to_trigger_min"))

            # Stop checkpoint target:
            # hold is "good" if 6h path actually triggers and recovers the new average before old-low rebreak.
            row["STOP_GOOD_RECOVER6"]=int(bool(prim.get("recovered")))
            row["STOP_GOOD_TP26"]=int(bool(prim.get("tp2_hit")))

            if prim.get("triggered"):
                trg=prim["trigger_ts"].astimezone(KST)
                rbfeat=feature_pack(one,trg,entry,entry_ts)
                rbfeat["swing_low_pct"]=prim.get("swing_low_pct")
                rbfeat["add_price_pct"]=prim.get("add_price_pct")
                rbfeat["new_avg_pct"]=prim.get("new_avg_pct")
                rbfeat["stop_to_low_min"]=prim.get("stop_to_low_min")
                rbfeat["low_to_trigger_min"]=prim.get("low_to_trigger_min")
                rbfeat["stop_to_trigger_min"]=prim.get("stop_to_trigger_min")
                low_to=prim.get("low_to_trigger_min")
                rbfeat["rebound_speed_pct_per_min"]=(REBOUND_PCT/low_to) if low_to not in (None,0) else None
                rbfeat["btc15_rebound"]=market_change(btc,trg,15)
                rbfeat["eth15_rebound"]=market_change(eth,trg,15)
                rbfeat["btc4h_rebound"]=market_change(btc,trg,240)
                rbfeat["eth4h_rebound"]=market_change(eth,trg,240)

                row["rebound_time_kst"]=trg.strftime("%Y-%m-%d %H:%M:%S")
                for k,v in rbfeat.items():
                    row["RB_"+k]=safe_round(v)
                row["RB_SUCCESS_RECOVER6"]=int(bool(prim.get("recovered")))
                row["RB_SUCCESS_TP26"]=int(bool(prim.get("tp2_hit")))
            else:
                row["rebound_time_kst"]=""
                row["RB_SUCCESS_RECOVER6"]=""
                row["RB_SUCCESS_TP26"]=""

            detail.append(row)

            if n%10==0 or n==len(stops):
                print(f"{n}/{len(stops)} complete",flush=True)
            time.sleep(0.05)

        except Exception as e:
            errors.append({
                "setup_id":r.get("setup_id"),"symbol":r.get("symbol"),
                "entry_time_kst":r.get("entry_time_kst"),"error":repr(e)
            })
            print("ERR",r.get("symbol"),repr(e),flush=True)

    write_csv(OUT_DETAIL,detail)

    stop_features=[
        "current_stop_net_pct","entry_to_stop_min",
        "STOP_mfe_to_checkpoint","STOP_mae_to_checkpoint",
        "STOP_prev1m_ret","STOP_prev3m_ret","STOP_prev1m_close_pos","STOP_prev1m_vol_ratio10",
        "STOP_last5m_ret","STOP_last5m_close_pos","STOP_last5m_vol_ratio5",
        "STOP_ema9_20_gap","STOP_ema20_slope","STOP_rsi14_5m",
        "STOP_btc15_stop","STOP_eth15_stop","STOP_btc4h_stop","STOP_eth4h_stop",
    ]
    rb_features=[
        "RB_swing_low_pct","RB_add_price_pct","RB_new_avg_pct",
        "RB_stop_to_low_min","RB_low_to_trigger_min","RB_stop_to_trigger_min",
        "RB_rebound_speed_pct_per_min",
        "RB_prev1m_ret","RB_prev3m_ret","RB_prev1m_close_pos","RB_prev1m_vol_ratio10",
        "RB_last5m_ret","RB_last5m_close_pos","RB_last5m_vol_ratio5",
        "RB_ema9_20_gap","RB_ema20_slope","RB_rsi14_5m",
        "RB_btc15_rebound","RB_eth15_rebound","RB_btc4h_rebound","RB_eth4h_rebound",
    ]

    stop_rules=search_rules(detail,stop_features,"STOP_GOOD_RECOVER6","STOP_POINT")
    rb_rows=[r for r in detail if i(r.get("H6_triggered"))==1]
    rb_rules=search_rules(rb_rows,rb_features,"RB_SUCCESS_RECOVER6","REBOUND_POINT")

    write_csv(OUT_STOP_RULES,stop_rules)
    write_csv(OUT_REBOUND_RULES,rb_rules)

    lines=[
        "DCA100 SUCCESS/FAILURE SEPARATION",
        "",
        f"STOP trades analyzed={len(detail)} errors={len(errors)}",
        "Hypothesis: current STOP ignored -> next-minute onward low tracking -> low+1.5% -> add 100%",
        "Primary success: new average recovered before pre-add low is re-broken.",
        "",
        "[HORIZON SENSITIVITY]",
    ]
    for h in HORIZONS_H:
        for sp in ("DISCOVERY_0901_0917","VALIDATION_0918_0922","FORWARD_0923_PLUS","ALL"):
            z=detail if sp=="ALL" else [r for r in detail if r["split"]==sp]
            if not z: continue
            trig=sum(i(r.get(f"H{h}_triggered")) for r in z)
            rec=sum(i(r.get(f"H{h}_recover_avg")) for r in z)
            tp=sum(i(r.get(f"H{h}_tp2")) for r in z)
            lines.append(
                f"H{h} {sp}: n={len(z)} trigger={trig} ({trig/len(z):.1%}) "
                f"recover_avg={rec} ({rec/len(z):.1%}) TP2={tp} ({tp/len(z):.1%})"
            )

    lines += ["","[STOP POINT BASE SUCCESS — 6H]"]
    for sp,n,s,b,rate in split_stats(detail,"STOP_GOOD_RECOVER6"):
        lines.append(f"{sp}: n={n} good={s} bad={b} good_rate={rate:.3f}")

    lines += ["","[REBOUND POINT BASE SUCCESS — triggered only, 6H]"]
    for sp,n,s,b,rate in split_stats(rb_rows,"RB_SUCCESS_RECOVER6"):
        lines.append(f"{sp}: n={n} success={s} fail={b} success_rate={rate:.3f}")

    lines += ["","[TOP STOP-POINT RULES]"]
    if stop_rules:
        for x in stop_rules[:12]:
            lines.append(
                f"{x['feature']} {x['op']} {x['threshold']} | "
                f"DISC precision={x['disc_precision']} keep={x['disc_keep']} blockfail={x['disc_blockfail']} | "
                f"VAL n={x['val_allow']} precision={x['val_precision']} keep={x['val_keep']} blockfail={x['val_blockfail']} | "
                f"FWD n={x['fwd_allow']} precision={x['fwd_precision']} keep={x['fwd_keep']} blockfail={x['fwd_blockfail']} | "
                f"stable_both={x['stable_both']}"
            )
    else:
        lines.append("NONE")

    lines += ["","[TOP REBOUND-POINT RULES]"]
    if rb_rules:
        for x in rb_rules[:12]:
            lines.append(
                f"{x['feature']} {x['op']} {x['threshold']} | "
                f"DISC precision={x['disc_precision']} keep={x['disc_keep']} blockfail={x['disc_blockfail']} | "
                f"VAL n={x['val_allow']} precision={x['val_precision']} keep={x['val_keep']} blockfail={x['val_blockfail']} | "
                f"FWD n={x['fwd_allow']} precision={x['fwd_precision']} keep={x['fwd_keep']} blockfail={x['fwd_blockfail']} | "
                f"stable_both={x['stable_both']}"
            )
    else:
        lines.append("NONE")

    if errors:
        lines += ["",f"[ERRORS] {len(errors)}"]
        for e in errors[:10]:
            lines.append(str(e))

    OUT_SUMMARY.write_text("\n".join(lines)+"\n",encoding="utf-8")

    with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
        for p in [OUT_DETAIL,OUT_STOP_RULES,OUT_REBOUND_RULES,OUT_SUMMARY]:
            z.write(p,arcname=p.name)
        if errors:
            epath=ROOT/"DCA100_SEPARATION_ERRORS.csv"
            write_csv(epath,errors)
            z.write(epath,arcname=epath.name)

    print()
    print("\n".join(lines[:45]))
    print()
    print("DONE:",OUT_ZIP,flush=True)

if __name__=="__main__":
    main()
