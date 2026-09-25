#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DCA100 observer cohort: PRE-REBOUND early-stop research

Frozen upstream rules:
- Observer at current STOP:
    STOP_prev1m_vol_ratio10 <= 2.61
    STOP_ema20_slope >= 0.00061
- Green at low +1.5% rebound:
    swing low >= -2.74314% from original entry
    5m RSI14 >= 43.89902
- DCA size if green: 100% of original (1:1)

Purpose:
Find whether an EARLY RED rule can cut future failures BEFORE the +1.5%
rebound trigger, without cutting future green/recovery trades.

Important:
A deployable pre-trigger rule MUST be tested on ALL observer trades, not only
the 18 later non-green trades, because before +1.5% we do not yet know who
will become green.

Discovery: 2026-09-01..17
Validation: 2026-09-18..22
Forward: 2026-09-23+

Input:
  DCA100_SEPARATION_DETAIL.csv in current directory
Output:
  DCA100_GRAY_MIDSTOP_RESULTS.zip
"""
from __future__ import annotations

import csv, io, json, math, time, urllib.parse, urllib.request, zipfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

ROOT = Path("/root/hyejin-trader/bybit_swing")
IN = ROOT / "DCA100_SEPARATION_DETAIL.csv"
OUT_ZIP = ROOT / "DCA100_GRAY_MIDSTOP_RESULTS.zip"
KST = timezone(timedelta(hours=9))
API = "https://api.bybit.com"

OBS_VOL_MAX = 2.61
OBS_SLOPE_MIN = 0.00061
GREEN_LOW_MIN = -2.74314
GREEN_RSI_MIN = 43.89902
RB_TRIGGER = 1.5
MAX_HOURS = 6

def f(v, d=None):
    try:
        if v is None or str(v).strip()=="":
            return d
        x=float(v)
        if math.isnan(x): return d
        return x
    except Exception:
        return d

def i(v, d=0):
    try: return int(float(v))
    except Exception: return d

def dt_kst(s):
    if not s: return None
    s=str(s).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M:%S.%f"):
        try: return datetime.strptime(s,fmt).replace(tzinfo=KST)
        except Exception: pass
    try:
        d=datetime.fromisoformat(s.replace("Z","+00:00"))
        if d.tzinfo is None: d=d.replace(tzinfo=KST)
        return d.astimezone(KST)
    except Exception:
        return None

def pct(a,b):
    return (a/b-1)*100 if b else None

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
            req=urllib.request.Request(url,headers={"User-Agent":"HJ-DCA100-MIDSTOP/1.0"})
            with urllib.request.urlopen(req,timeout=25) as resp:
                j=json.loads(resp.read().decode())
            if int(j.get("retCode",-1))!=0:
                raise RuntimeError(f"{j.get('retCode')} {j.get('retMsg')}")
            out=[]
            for z in j.get("result",{}).get("list",[]):
                try:
                    out.append({
                        "ts":datetime.fromtimestamp(int(z[0])/1000,tz=timezone.utc),
                        "o":float(z[1]),"h":float(z[2]),"l":float(z[3]),
                        "c":float(z[4]),"v":float(z[5]),
                    })
                except Exception:
                    pass
            out.sort(key=lambda x:x["ts"])
            return out
        except Exception as e:
            last=e
            time.sleep(min(8,0.8*(k+1)))
    raise RuntimeError(last)

def ema(vals,n):
    if not vals: return None
    a=2/(n+1)
    e=float(vals[0])
    for x in vals[1:]:
        e=a*float(x)+(1-a)*e
    return e

def rsi(vals,n=14):
    if len(vals)<n+1: return None
    gains=[]; losses=[]
    for a,b in zip(vals[-(n+1):-1],vals[-n:]):
        d=b-a
        gains.append(max(d,0)); losses.append(max(-d,0))
    ag=sum(gains)/n; al=sum(losses)/n
    if al==0: return 100.0
    rs=ag/al
    return 100-(100/(1+rs))

def five_bars(one):
    d={}
    for b in one:
        t=b["ts"].replace(minute=(b["ts"].minute//5)*5,second=0,microsecond=0)
        x=d.get(t)
        if x is None:
            d[t]={"ts":t,"o":b["o"],"h":b["h"],"l":b["l"],"c":b["c"],"v":b["v"],"n":1}
        else:
            x["h"]=max(x["h"],b["h"])
            x["l"]=min(x["l"],b["l"])
            x["c"]=b["c"]; x["v"]+=b["v"]; x["n"]+=1
    return [d[k] for k in sorted(d)]

def completed5_features(one, cp):
    cp=cp.replace(second=0,microsecond=0)
    f5=[b for b in five_bars(one) if b["ts"]+timedelta(minutes=5)<=cp]
    if not f5: return {}
    last=f5[-1]
    closes=[b["c"] for b in f5]
    e9=ema(closes[-60:],9)
    e20=ema(closes[-80:],20)
    e20p=ema(closes[-81:-1],20) if len(closes)>=2 else None
    return {
        "rsi5":rsi(closes,14),
        "f5ret":pct(last["c"],last["o"]),
        "ema_gap":(e9/e20-1)*100 if e9 and e20 else None,
        "ema20_slope":(e20/e20p-1)*100 if e20 and e20p else None,
    }

def split_name(date_s):
    if date_s<="2026-09-17": return "DISCOVERY"
    if date_s<="2026-09-22": return "VALIDATION"
    return "FORWARD"

def simple_net_from_exit(entry,exitp):
    # approximate round-trip taker fee in % of original notional
    gross=(exitp/entry-1)*100
    fee=0.055 + 0.055*(exitp/entry)
    return gross-fee

def load_cohort():
    if not IN.exists():
        raise RuntimeError(f"missing {IN}")
    with IN.open("r",encoding="utf-8-sig",newline="") as fh:
        rows=list(csv.DictReader(fh))
    out=[]
    for r in rows:
        vol=f(r.get("STOP_prev1m_vol_ratio10"))
        sl=f(r.get("STOP_ema20_slope"))
        if vol is None or sl is None: continue
        if vol<=OBS_VOL_MAX and sl>=OBS_SLOPE_MIN:
            r["split2"]=split_name(str(r.get("date") or ""))
            r["eventual_recover"]=i(r.get("STOP_GOOD_RECOVER6"))
            low=f(r.get("RB_swing_low_pct"))
            rr=f(r.get("RB_rsi14_5m"))
            r["future_green"]=int(low is not None and rr is not None and low>=GREEN_LOW_MIN and rr>=GREEN_RSI_MIN)
            out.append(r)
    return out

def build_snapshots(r):
    symbol=str(r["symbol"])
    entry=f(r["entry_price"])
    stop=dt_kst(r["stop_time_kst"])
    if not entry or not stop:
        raise RuntimeError("bad entry/stop")
    start=stop.astimezone(timezone.utc)-timedelta(hours=3)
    end=stop.astimezone(timezone.utc)+timedelta(hours=MAX_HOURS,minutes=5)
    one=request_klines(symbol,start,end)
    stop_floor=stop.astimezone(timezone.utc).replace(second=0,microsecond=0)
    post=[b for b in one if b["ts"]>=stop_floor+timedelta(minutes=1)
                         and b["ts"]<=stop_floor+timedelta(hours=MAX_HOURS)]
    if not post: raise RuntimeError("no post bars")

    low=None; low_ts=None
    snaps=[]
    trigger=None
    trigger_price=None

    for idx,b in enumerate(post):
        made_new_low=False
        if low is None or b["l"]<low:
            low=b["l"]; low_ts=b["ts"]; made_new_low=True

        # If this minute itself reaches +1.5% after a prior-minute low,
        # trigger occurs intrabar; do NOT use this bar's close for an early-stop rule.
        if (not made_new_low) and b["ts"]>low_ts and b["h"]>=low*(1+RB_TRIGGER/100):
            trigger=b["ts"]; trigger_price=low*(1+RB_TRIGGER/100)
            break

        # Snapshot is at this 1m bar CLOSE, only if no rebound trigger occurred.
        hist=[x for x in post[:idx+1]]
        prev3=hist[-3:] if hist else []
        cp=b["ts"]+timedelta(minutes=1)
        f5=completed5_features(one,cp)
        snap={
            "ts":b["ts"],
            "elapsed":(b["ts"]-stop_floor).total_seconds()/60,
            "since_low":(b["ts"]-low_ts).total_seconds()/60 if low_ts else 0,
            "low_pct":pct(low,entry),
            "close_pct":pct(b["c"],entry),
            "rb_from_low_close":pct(b["c"],low),
            "ret1":pct(b["c"],b["o"]),
            "ret3":pct(prev3[-1]["c"],prev3[0]["o"]) if prev3 else None,
            "rsi5":f5.get("rsi5"),
            "f5ret":f5.get("f5ret"),
            "ema_gap":f5.get("ema_gap"),
            "ema20_slope":f5.get("ema20_slope"),
            "exit_net":simple_net_from_exit(entry,b["c"]),
        }
        snaps.append(snap)

    return snaps, trigger, trigger_price, low

# Conservative, pre-specified rule family grids.
ELAPSED=[5,8,10,12,15,20,30,40,60]
SINCE_LOW=[3,5,8,10,12,15,20]
LOW_DD=[-2.5,-3.0,-3.5,-4.0,-4.5,-5.0,-6.0]
CLOSE_DD=[-2.0,-2.5,-3.0,-3.5,-4.0,-5.0]
RB_CLOSE=[0.2,0.4,0.6,0.8,1.0]
RET3=[-0.3,-0.5,-0.8,-1.0,-1.5]
RSI=[25,30,35,40,45]
GAP=[-0.1,-0.2,-0.3,-0.5,-0.8]

def rule_defs():
    rules=[]
    # A: stale low, weak rebound
    for L in SINCE_LOW:
        for R in RB_CLOSE:
            rules.append(("STALE_RB",{"since_low_min":L,"rb_max":R}))
    # B: elapsed + deep drawdown
    for E in ELAPSED:
        for D in LOW_DD:
            rules.append(("TIME_DD",{"elapsed_min":E,"low_pct_max":D}))
    # C: deep + weak short momentum
    for D in LOW_DD:
        for R in RET3:
            rules.append(("DD_RET3",{"low_pct_max":D,"ret3_max":R}))
    # D: stale + weak 3m
    for L in SINCE_LOW:
        for R in RET3:
            rules.append(("STALE_RET3",{"since_low_min":L,"ret3_max":R}))
    # E: deep close + weak RSI
    for D in CLOSE_DD:
        for R in RSI:
            rules.append(("CLOSE_RSI",{"close_pct_max":D,"rsi5_max":R}))
    # F: structure weakness
    for R in RSI:
        for G in GAP:
            rules.append(("RSI_GAP",{"rsi5_max":R,"ema_gap_max":G}))
    return rules

def fires(s,kind,p):
    try:
        if kind=="STALE_RB":
            return s["since_low"]>=p["since_low_min"] and s["rb_from_low_close"]<=p["rb_max"]
        if kind=="TIME_DD":
            return s["elapsed"]>=p["elapsed_min"] and s["low_pct"]<=p["low_pct_max"]
        if kind=="DD_RET3":
            return s["low_pct"]<=p["low_pct_max"] and s["ret3"] is not None and s["ret3"]<=p["ret3_max"]
        if kind=="STALE_RET3":
            return s["since_low"]>=p["since_low_min"] and s["ret3"] is not None and s["ret3"]<=p["ret3_max"]
        if kind=="CLOSE_RSI":
            return s["close_pct"]<=p["close_pct_max"] and s["rsi5"] is not None and s["rsi5"]<=p["rsi5_max"]
        if kind=="RSI_GAP":
            return s["rsi5"] is not None and s["ema_gap"] is not None and s["rsi5"]<=p["rsi5_max"] and s["ema_gap"]<=p["ema_gap_max"]
    except Exception:
        return False
    return False

def first_fire(snaps,kind,p):
    for s in snaps:
        if fires(s,kind,p):
            return s
    return None

def evaluate(rule, trades):
    kind,p=rule
    out=[]
    for t in trades:
        hit=first_fire(t["snaps"],kind,p)
        if hit:
            out.append((t,hit))
    n=len(out)
    fails=sum(1 for t,h in out if t["fail"])
    recovers=sum(1 for t,h in out if t["recover"])
    greens=sum(1 for t,h in out if t["green"])
    return {
        "n":n,"fails":fails,"recovers":recovers,"greens":greens,
        "fail_precision":fails/n if n else None,
        "fail_capture":fails/sum(t["fail"] for t in trades) if sum(t["fail"] for t in trades) else None,
        "avg_exit_net":sum(h["exit_net"] for t,h in out)/n if n else None,
        "hits":out,
    }

def csv_text(rows):
    if not rows: return ""
    s=io.StringIO()
    keys=[]
    seen=set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    w=csv.DictWriter(s,fieldnames=keys)
    w.writeheader(); w.writerows(rows)
    return s.getvalue()

def main():
    cohort=load_cohort()
    print("observer cohort",len(cohort),flush=True)
    trades=[]
    errors=[]
    for n,r in enumerate(cohort,1):
        try:
            snaps,trig,trigp,low=build_snapshots(r)
            trades.append({
                "setup_id":r["setup_id"],"symbol":r["symbol"],
                "split":r["split2"],"date":r["date"],
                "stop_net":f(r["current_stop_net_pct"]),
                "recover":i(r["eventual_recover"]),
                "fail":1-i(r["eventual_recover"]),
                "green":i(r["future_green"]),
                "snaps":snaps,
                "trigger":trig,
                "trigger_price":trigp,
            })
            if n%5==0 or n==len(cohort):
                print(f"{n}/{len(cohort)}",flush=True)
            time.sleep(0.03)
        except Exception as e:
            errors.append({"setup_id":r.get("setup_id"),"symbol":r.get("symbol"),"error":repr(e)})
            print("ERR",r.get("symbol"),repr(e),flush=True)

    disc=[t for t in trades if t["split"]=="DISCOVERY"]
    val=[t for t in trades if t["split"]=="VALIDATION"]
    fwd=[t for t in trades if t["split"]=="FORWARD"]

    rule_rows=[]
    candidates=[]
    for rule in rule_defs():
        ed=evaluate(rule,disc)
        if ed["n"]<2 or ed["fails"]<1:
            continue

        ev=evaluate(rule,val)
        ef=evaluate(rule,fwd)

        # Discovery-first candidate screen. Do not use val/fwd to create threshold.
        if ed["fail_precision"] is None or ed["fail_precision"]<0.60:
            continue
        if ed["fail_capture"] is None or ed["fail_capture"]<1/3:
            continue

        # Record all discovery candidates; stability columns are descriptive.
        stable_val = (
            ev["n"]==0 or
            (
                ev["fail_precision"] is not None and ev["fail_precision"]>=0.60
                and ev["greens"]==0
            )
        )
        stable_fwd = (ef["greens"]==0 and ef["recovers"]==0)

        kind,p=rule
        row={
            "kind":kind,
            "params":json.dumps(p,sort_keys=True),
            "disc_n":ed["n"],"disc_fail":ed["fails"],"disc_recover_cut":ed["recovers"],"disc_green_cut":ed["greens"],
            "disc_fail_precision":round(ed["fail_precision"],4),
            "disc_fail_capture":round(ed["fail_capture"],4),
            "val_n":ev["n"],"val_fail":ev["fails"],"val_recover_cut":ev["recovers"],"val_green_cut":ev["greens"],
            "val_fail_precision":"" if ev["fail_precision"] is None else round(ev["fail_precision"],4),
            "fwd_n":ef["n"],"fwd_fail":ef["fails"],"fwd_recover_cut":ef["recovers"],"fwd_green_cut":ef["greens"],
            "stable_val":int(stable_val),"stable_fwd":int(stable_fwd),
            "stable_both":int(stable_val and stable_fwd),
        }
        # rank: stable, fewer green/recover cuts, more failure capture
        score=(
            10*row["stable_both"]
            + 4*ed["fail_capture"]
            + 2*(ev["fail_capture"] or 0)
            - 3*(ed["greens"]+ev["greens"]+ef["greens"])
            - 1.5*(ed["recovers"]+ev["recovers"]+ef["recovers"])
        )
        row["score"]=round(score,4)
        rule_rows.append(row)
        candidates.append((score,rule,ed,ev,ef,row))

    candidates.sort(key=lambda x:x[0],reverse=True)
    rule_rows.sort(key=lambda x:x["score"],reverse=True)

    hit_rows=[]
    for rank,x in enumerate(candidates[:30],1):
        score,rule,ed,ev,ef,row=x
        kind,p=rule
        for split_name,evx in [("DISCOVERY",ed),("VALIDATION",ev),("FORWARD",ef)]:
            for t,h in evx["hits"]:
                hit_rows.append({
                    "rank":rank,"kind":kind,"params":json.dumps(p,sort_keys=True),
                    "split":split_name,"symbol":t["symbol"],"date":t["date"],
                    "fail":t["fail"],"recover":t["recover"],"green":t["green"],
                    "fire_time_utc":h["ts"].isoformat(),
                    "elapsed_min":round(h["elapsed"],2),
                    "since_low_min":round(h["since_low"],2),
                    "low_pct":round(h["low_pct"],4),
                    "close_pct":round(h["close_pct"],4),
                    "rb_from_low_close":round(h["rb_from_low_close"],4),
                    "ret3":"" if h["ret3"] is None else round(h["ret3"],4),
                    "rsi5":"" if h["rsi5"] is None else round(h["rsi5"],4),
                    "ema_gap":"" if h["ema_gap"] is None else round(h["ema_gap"],4),
                    "exit_net_est":round(h["exit_net"],4),
                    "current_stop_net":t["stop_net"],
                })

    lines=[]
    lines.append("DCA100 GRAY MID-STOP RESEARCH")
    lines.append(f"Observer cohort analyzed={len(trades)} errors={len(errors)}")
    for sp,z in [("DISCOVERY",disc),("VALIDATION",val),("FORWARD",fwd),("ALL",trades)]:
        if not z: continue
        lines.append(
            f"{sp}: n={len(z)} green={sum(t['green'] for t in z)} "
            f"eventual_recover={sum(t['recover'] for t in z)} fail={sum(t['fail'] for t in z)}"
        )
    lines.append("")
    lines.append("IMPORTANT: pre-trigger rule tested on ALL observer trades, not only later non-green trades.")
    lines.append("Top discovery-derived candidates:")
    for rank,x in enumerate(candidates[:20],1):
        score,rule,ed,ev,ef,row=x
        lines.append(
            f"{rank}. {row['kind']} {row['params']} | "
            f"D {row['disc_fail']}/{row['disc_n']} fail, green_cut={row['disc_green_cut']} | "
            f"V {row['val_fail']}/{row['val_n']} fail, green_cut={row['val_green_cut']} | "
            f"F cuts={row['fwd_n']} recover_cut={row['fwd_recover_cut']} green_cut={row['fwd_green_cut']} | "
            f"stable_both={row['stable_both']}"
        )

    summary="\n".join(lines)+"\n"
    (ROOT/"DCA100_GRAY_MIDSTOP_SUMMARY.txt").write_text(summary,encoding="utf-8")
    (ROOT/"DCA100_GRAY_MIDSTOP_RULES.csv").write_text(csv_text(rule_rows),encoding="utf-8-sig")
    (ROOT/"DCA100_GRAY_MIDSTOP_HITS.csv").write_text(csv_text(hit_rows),encoding="utf-8-sig")

    with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
        for fn in ["DCA100_GRAY_MIDSTOP_SUMMARY.txt","DCA100_GRAY_MIDSTOP_RULES.csv","DCA100_GRAY_MIDSTOP_HITS.csv"]:
            z.write(ROOT/fn,arcname=fn)
        if errors:
            z.writestr("ERRORS.csv",csv_text(errors).encode("utf-8-sig"))

    print(summary)
    print("DONE",OUT_ZIP,flush=True)

if __name__=="__main__":
    main()
