#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recovery-confirm cycle research (READ ONLY)
- No orders
- No DB writes
- Input: SPLIT_EXACT_DETAIL.csv or SPLIT_ENTRY_EXACT_V2_*.zip
- Output: RECOVERY_CONFIRM_LATEST.zip only

Variants:
1) DD1.0 -> low +0.3% rebound (previous comparator)
2) DD1.5 -> low +0.5% rebound
3) DD1.0 -> recover to original entry -0.5%
4) DD1.5 -> recover to original entry -0.5%
5) DD2.0 -> recover to original entry -0.5%
6) DD1.5 -> low +1.0% rebound
7) DD2.0 -> low +1.0% rebound
8) DD2.5 -> low +1.0% rebound

Common:
- add 50% of original
- max 1 cycle
- new average immediately after add
- TP = new average +2.0%
- if new average is recovered: remove added 50%
- if pre-add swing low breaks: remove added 50% at swing-low level
- core/base stop is NOT moved farther down
- extra taker fee = 0.055% each side on cycle add / cycle-add exit
- 1-minute same-bar ambiguity handled conservatively
"""
from __future__ import annotations

import csv, io, json, sqlite3, time, urllib.parse, urllib.request, zipfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

KST = timezone(timedelta(hours=9))
API = "https://api.bybit.com"
OUT = Path("RECOVERY_CONFIRM_LATEST.zip")

ADD_FRAC = 0.50
TP_PCT = 2.00
FEE_PCT = 0.055
MAX_MINUTES = 180

VARIANTS = [
    # 기존 비교군
    ("DD1_RB03",       "from_low", -1.0,  0.3),
    ("DD15_RB05",      "from_low", -1.5,  0.5),

    # 원진입가 -0.5%까지 회복 확인형
    ("DD1_TO_M05",     "to_level", -1.0, -0.5),
    ("DD15_TO_M05",    "to_level", -1.5, -0.5),
    ("DD2_TO_M05",     "to_level", -2.0, -0.5),

    # 깊게 눌린 뒤 저점에서 +1.0% 반등 확인형
    ("DD15_RB10",      "from_low", -1.5,  1.0),
    ("DD2_RB10",       "from_low", -2.0,  1.0),
    ("DD25_RB10",      "from_low", -2.5,  1.0),
]

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

def find_latest_exact():
    direct = []
    for pat in ("SPLIT_EXACT_DETAIL.csv", "**/SPLIT_EXACT_DETAIL.csv"):
        direct += list(Path(".").glob(pat))
    direct = [p for p in direct if p.is_file()]
    if direct:
        p = max(direct, key=lambda x:x.stat().st_mtime)
        print("INPUT CSV =", p)
        return list(csv.DictReader(p.open("r",encoding="utf-8-sig",newline="")))
    zips = sorted(Path(".").glob("SPLIT_ENTRY_EXACT_V2_*.zip"),
                  key=lambda x:x.stat().st_mtime, reverse=True)
    if not zips:
        raise RuntimeError("SPLIT_EXACT_DETAIL.csv / SPLIT_ENTRY_EXACT_V2_*.zip not found")
    zp = zips[0]
    print("INPUT ZIP =", zp)
    with zipfile.ZipFile(zp) as z:
        names = [n for n in z.namelist() if n.endswith("SPLIT_EXACT_DETAIL.csv")]
        if not names:
            raise RuntimeError("SPLIT_EXACT_DETAIL.csv not inside ZIP")
        return list(csv.DictReader(io.StringIO(z.read(names[0]).decode("utf-8-sig"))))

raw = find_latest_exact()
trades, seen = [], set()
for r in raw:
    if str(r.get("mode")) != "A_FULL100":
        continue
    tid = str(r.get("trade_id") or "").strip()
    if not tid or tid in seen:
        continue
    entry = f(r.get("entry_price"))
    baseline = f(r.get("total_pct"))
    ep = f(r.get("common_exit_price"))
    if entry is None or baseline is None or ep is None:
        continue
    seen.add(tid)
    trades.append({
        "period":str(r.get("period") or ""),
        "trade_id":tid,
        "entry_time_kst":str(r.get("entry_time_kst") or ""),
        "symbol":str(r.get("symbol") or ""),
        "entry_price":entry,
        "baseline_pct":baseline,
        "common_result":str(r.get("common_result") or ""),
        "common_exit_price":ep,
    })
trades.sort(key=lambda x:x["entry_time_kst"])
print("EXACT TRADES =", len(trades))
if len(trades) < 500:
    raise RuntimeError(f"Only {len(trades)} trades; expected the 586-trade exact set")

# Old-period baseline exit times, if the prior exact trade table is present.
old_times = {}
old_files = list(Path(".").glob("**/P_SAFE_SAFEA_0901_0917_trades.csv"))
if old_files:
    op = max(old_files, key=lambda x:x.stat().st_mtime)
    print("OLD TIMING =", op)
    with op.open("r",encoding="utf-8-sig",newline="") as fh:
        for r in csv.DictReader(fh):
            tid = str(r.get("trade_id") or "")
            if tid:
                old_times[tid] = {
                    "ts":dt_kst(r.get("baseline_result_time_kst")),
                    "price":f(r.get("baseline_result_price")),
                    "result":str(r.get("baseline_result") or ""),
                }

DB = Path("bybit_swing_bot.db")
con = None
if DB.exists():
    try:
        con = sqlite3.connect(f"file:{DB.resolve()}?mode=ro", uri=True)
        con.row_factory = sqlite3.Row
        print("DB TIMING = ON")
    except Exception as e:
        print("DB TIMING = OFF", repr(e))

def api_1m(symbol, start, end):
    params = {
        "category":"linear", "symbol":symbol, "interval":"1",
        "start":int(start.timestamp()*1000),
        "end":int(end.timestamp()*1000),
        "limit":1000,
    }
    url = API + "/v5/market/kline?" + urllib.parse.urlencode(params)
    last = None
    for k in range(8):
        try:
            req = urllib.request.Request(url, headers={"User-Agent":"HJ-RECOVERY-CHECK/1.0"})
            with urllib.request.urlopen(req,timeout=25) as resp:
                j=json.loads(resp.read().decode("utf-8"))
            if int(j.get("retCode",-1)) != 0:
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

def first_touch(bars, entry, exit_price):
    if exit_price >= entry:
        for b in bars:
            if b["h"] >= exit_price:
                return b["ts"]
    else:
        for b in bars:
            if b["l"] <= exit_price:
                return b["ts"]
    return None

def db_match_time(t):
    if con is None:
        return None, "NO_DB"
    try:
        rows = con.execute("""
            SELECT variant,result_ts,result_price,result
            FROM research_shadow_reviews
            WHERE setup_id=? AND completed=1
              AND result_ts IS NOT NULL AND result_price IS NOT NULL
        """,(t["trade_id"],)).fetchall()
    except Exception:
        return None, "DB_QUERY_FAIL"
    if not rows:
        return None, "DB_NO_MATCH"
    entry=t["entry_price"]; target=t["common_exit_price"]
    scored=[]
    for r in rows:
        rp=f(r["result_price"])
        ts=dt_utc(r["result_ts"])
        if rp is None or ts is None:
            continue
        d=abs(pp(rp,entry)-pp(target,entry))
        scored.append((d,ts,str(r["variant"] or ""),str(r["result"] or "")))
    if not scored:
        return None, "DB_BAD"
    scored.sort(key=lambda x:x[0])
    d,ts,var,res=scored[0]
    if d <= 0.08:
        return ts, f"DB_MATCH:{var}:{res}:{d:.4f}pp"
    return None, f"DB_NO_CLOSE:{d:.4f}pp"

def resolve_cutoff(t,bars):
    entry=t["entry_price"]; exit_price=t["common_exit_price"]
    # TP20 is exact by first price touch.
    if t["common_result"] == "TP20":
        x=first_touch(bars,entry,exit_price)
        if x:
            return x,"TP20_TOUCH",True

    # Old-period exact baseline time if exit price matches the old result price.
    o=old_times.get(t["trade_id"])
    if o and o.get("ts") and o.get("price"):
        if abs(pp(o["price"],entry)-pp(exit_price,entry)) <= 0.08:
            return o["ts"].astimezone(timezone.utc),"OLD_EXACT",True

    # Recent / other: DB result with matching exit price.
    x,src=db_match_time(t)
    if x:
        return x,src,True

    # Fallback to first touch of the known exact exit price.
    x=first_touch(bars,entry,exit_price)
    if x:
        return x,"FALLBACK_TOUCH",False

    # Last resort: 180m boundary.
    return bars[-1]["ts"],"APPROX_END",False

def extra_fee(qfrac, price, entry):
    # fee expressed in percentage points of original 1.0*entry notional
    return qfrac * (price/entry) * FEE_PCT

def simulate(t,bars,cutoff,name,kind,arm_dd,trigger_value):
    entry=t["entry_price"]; base_exit=t["common_exit_price"]
    entry_dt=dt_kst(t["entry_time_kst"])
    start=(entry_dt.astimezone(timezone.utc).replace(second=0,microsecond=0)
           + timedelta(minutes=1))
    cutoff_floor=cutoff.replace(second=0,microsecond=0)
    pre=[b for b in bars if b["ts"]>=start and b["ts"]<cutoff_floor]
    final_bar=next((b for b in bars if b["ts"]==cutoff_floor),None)

    armed=False
    swing_low=None
    swing_low_i=None

    added=False
    add_open=False
    add_price=None
    avg=entry
    new_tp=None
    add_i=None

    realized=0.0
    fee=0.0
    false_break=0
    recovered=0
    exit_reason="BASELINE"
    exit_price=base_exit

    def finish_core(px):
        return realized + (px-avg)/entry*100.0

    for i,b in enumerate(pre):
        if not added:
            arm_price=entry*(1+arm_dd/100.0)
            if not armed:
                if b["l"] <= arm_price:
                    armed=True; swing_low=b["l"]; swing_low_i=i
                continue

            if b["l"] < swing_low:
                swing_low=b["l"]; swing_low_i=i

            # Never add in the same minute that produced the current swing low.
            if i <= swing_low_i:
                continue

            if kind=="from_low":
                trigger=swing_low*(1+trigger_value/100.0)
            else:
                trigger=entry*(1+trigger_value/100.0)

            if b["h"] >= trigger:
                add_price=trigger
                avg=(entry + ADD_FRAC*add_price)/(1.0+ADD_FRAC)
                new_tp=avg*(1+TP_PCT/100.0)
                fee += extra_fee(ADD_FRAC,add_price,entry)
                added=True; add_open=True; add_i=i
                # Same-minute favorable follow-through is deliberately ignored.
                continue

        else:
            # After add: prior swing-low rebreak is handled first (conservative).
            if add_open and i > add_i and b["l"] <= swing_low:
                realized += ADD_FRAC*(swing_low-avg)/entry*100.0
                fee += extra_fee(ADD_FRAC,swing_low,entry)
                add_open=False; false_break=1
                # Same-minute rebound after a false break is ignored.
                continue

            if add_open and i > add_i and b["h"] >= avg:
                fee += extra_fee(ADD_FRAC,avg,entry)
                add_open=False; recovered=1
                if b["h"] >= new_tp:
                    gross=finish_core(new_tp)
                    return gross,fee,"TP20_AFTER_CYCLE",new_tp,added,recovered,false_break,avg,add_price,swing_low

            if (not add_open) and b["h"] >= new_tp:
                gross=finish_core(new_tp)
                return gross,fee,"TP20_AFTER_CYCLE",new_tp,added,recovered,false_break,avg,add_price,swing_low

    # Exact baseline TP20 cutoff:
    # original +2% was reached. If cycle was added earlier, the lower new TP
    # is necessarily crossed on that rally. Deal with any same-minute low
    # conservatively first, then close core at the cycle TP.
    if t["common_result"]=="TP20" and added:
        if add_open:
            if final_bar and final_bar["l"] <= swing_low:
                realized += ADD_FRAC*(swing_low-avg)/entry*100.0
                fee += extra_fee(ADD_FRAC,swing_low,entry)
                add_open=False; false_break=1
            else:
                fee += extra_fee(ADD_FRAC,avg,entry)
                add_open=False; recovered=1
        gross=finish_core(new_tp)
        return gross,fee,"TP20_AFTER_CYCLE_AT_BASE_TP",new_tp,added,recovered,false_break,avg,add_price,swing_low

    # Baseline exit now. Do not push the core stop farther down.
    if not added:
        return t["baseline_pct"],0.0,"BASELINE_NO_ADD",base_exit,False,0,0,entry,None,None

    if add_open:
        # Added 0.5 is still open at baseline exit -> close it together here.
        realized += ADD_FRAC*(base_exit-avg)/entry*100.0
        fee += extra_fee(ADD_FRAC,base_exit,entry)
        add_open=False

    gross=finish_core(base_exit)
    return gross,fee,"BASELINE_AFTER_CYCLE",base_exit,added,recovered,false_break,avg,add_price,swing_low

detail=[]
errors=[]
timing_counts=defaultdict(int)

for n,t in enumerate(trades,1):
    try:
        ek=dt_kst(t["entry_time_kst"])
        if not ek:
            raise RuntimeError("bad entry time")
        start=ek.astimezone(timezone.utc).replace(second=0,microsecond=0)
        end=start+timedelta(minutes=MAX_MINUTES+2)
        bars=api_1m(t["symbol"],start,end)
        if not bars:
            raise RuntimeError("no bars")
        cutoff,tsrc,strict=resolve_cutoff(t,bars)
        if cutoff > end:
            cutoff=end
            strict=False
            tsrc += ":CLAMP180"
        timing_counts[tsrc]+=1

        # Baseline row.
        detail.append({
            "period":t["period"],"date":t["entry_time_kst"][:10],
            "trade_id":t["trade_id"],"symbol":t["symbol"],"entry_time_kst":t["entry_time_kst"],
            "variant":"BASE_FULL100","baseline_pct":round(t["baseline_pct"],6),
            "gross_pct":round(t["baseline_pct"],6),"extra_fee_pct":0.0,
            "net_pct":round(t["baseline_pct"],6),"net_improvement":0.0,
            "added":0,"recovered":0,"false_break":0,
            "add_price_pct":"","new_avg_pct":"","swing_low_pct":"",
            "exit_reason":t["common_result"],"timing_source":tsrc,"strict_timing":int(strict),
        })

        for name,kind,arm_dd,trig in VARIANTS:
            gross,fee,reason,ep,added,recovered,false_break,avg,ap,low = simulate(
                t,bars,cutoff,name,kind,arm_dd,trig
            )
            net=gross-fee
            detail.append({
                "period":t["period"],"date":t["entry_time_kst"][:10],
                "trade_id":t["trade_id"],"symbol":t["symbol"],"entry_time_kst":t["entry_time_kst"],
                "variant":name,"baseline_pct":round(t["baseline_pct"],6),
                "gross_pct":round(gross,6),"extra_fee_pct":round(fee,6),
                "net_pct":round(net,6),"net_improvement":round(net-t["baseline_pct"],6),
                "added":int(bool(added)),"recovered":int(recovered),"false_break":int(false_break),
                "add_price_pct":round(pp(ap,entry),4) if ap else "",
                "new_avg_pct":round(pp(avg,entry),4) if added else "",
                "swing_low_pct":round(pp(low,entry),4) if low else "",
                "exit_reason":reason,"timing_source":tsrc,"strict_timing":int(strict),
            })

        if n%25==0:
            print(f"{n}/{len(trades)} complete")
        time.sleep(0.10)

    except Exception as e:
        errors.append({
            "trade_id":t["trade_id"],"symbol":t["symbol"],
            "entry_time_kst":t["entry_time_kst"],"error":repr(e)
        })

def summarize(rows,scope,period,strict_only):
    out=[]
    names=["BASE_FULL100"]+[v[0] for v in VARIANTS]
    for name in names:
        z=[r for r in rows if r["variant"]==name]
        if period!="ALL":
            z=[r for r in z if r["period"]==period]
        if strict_only:
            z=[r for r in z if int(r["strict_timing"])==1]
        if not z:
            continue
        z=sorted(z,key=lambda x:x["entry_time_kst"])
        base=sum(float(r["baseline_pct"]) for r in z)
        net=sum(float(r["net_pct"]) for r in z)
        gross=sum(float(r["gross_pct"]) for r in z)
        fees=sum(float(r["extra_fee_pct"]) for r in z)
        eq=peak=mdd=0.0
        for r in z:
            eq += float(r["net_pct"])
            peak=max(peak,eq); mdd=min(mdd,eq-peak)
        out.append({
            "scope":scope,"period":period,"variant":name,"trades":len(z),
            "baseline_net":round(base,4),"gross_net":round(gross,4),
            "extra_fee":round(fees,4),"net_after_extra_fee":round(net,4),
            "net_improvement":round(net-base,4),
            "adds":sum(int(r["added"]) for r in z),
            "recovered":sum(int(r["recovered"]) for r in z),
            "false_breaks":sum(int(r["false_break"]) for r in z),
            "mdd_pctp":round(mdd,4),
            "improved_trades":sum(float(r["net_improvement"])>1e-9 for r in z),
            "worsened_trades":sum(float(r["net_improvement"])<-1e-9 for r in z),
        })
    return out

summary=[]
for strict_only,scope in ((False,"ALL_TIMING"),(True,"STRICT_TIMING")):
    for per in ("ALL","0901_0917","0918_0921"):
        summary += summarize(detail,scope,per,strict_only)

daily=[]
for name in ["BASE_FULL100"]+[v[0] for v in VARIANTS]:
    dates=sorted(set(r["date"] for r in detail if r["variant"]==name))
    for d in dates:
        z=[r for r in detail if r["variant"]==name and r["date"]==d]
        if not z: continue
        daily.append({
            "date":d,"variant":name,"trades":len(z),
            "baseline_net":round(sum(float(r["baseline_pct"]) for r in z),4),
            "net_after_extra_fee":round(sum(float(r["net_pct"]) for r in z),4),
            "improvement":round(sum(float(r["net_improvement"]) for r in z),4),
            "adds":sum(int(r["added"]) for r in z),
            "false_breaks":sum(int(r["false_break"]) for r in z),
        })

readme=f"""RECOVERY CONFIRM RESEARCH
Trades expected: 586
Completed unique: {len(set(r['trade_id'] for r in detail))}
Errors: {len(errors)}

Variants:
DD1_RB03    = hit -1.0%, then +0.3% from observed swing low
DD15_RB05   = hit -1.5%, then +0.5% from observed swing low
DD1_TO_M05  = hit -1.0%, then recover to original entry -0.5%
DD15_TO_M05 = hit -1.5%, then recover to original entry -0.5%
DD2_TO_M05  = hit -2.0%, then recover to original entry -0.5%
DD15_RB10     = hit -1.5%, then +1.0% from observed swing low
DD2_RB10      = hit -2.0%, then +1.0% from observed swing low
DD25_RB10     = hit -2.5%, then +1.0% from observed swing low

Common:
- cycle add = 50% of original
- max 1 cycle
- new TP = new average +2.0%
- add removed at new average
- false rebound: added 50% removed on pre-add swing-low break
- original core stop is not moved lower
- extra cycle fees: taker 0.055% each side
- same-minute add after a new low is prohibited
- timing ambiguity is handled conservatively
- ALL_TIMING includes fallback timing; STRICT_TIMING only exact/price-matched timing.

Timing sources:
{dict(timing_counts)}
"""

with zipfile.ZipFile(OUT,"w",zipfile.ZIP_DEFLATED) as z:
    z.writestr("SUMMARY.csv",csv_text(summary).encode("utf-8-sig"))
    z.writestr("DAILY.csv",csv_text(daily).encode("utf-8-sig"))
    z.writestr("DETAIL.csv",csv_text(detail).encode("utf-8-sig"))
    if errors:
        z.writestr("ERRORS.csv",csv_text(errors).encode("utf-8-sig"))
    z.writestr("README.txt",readme.encode("utf-8"))

print()
print("====================================")
print("RECOVERY CONFIRM COMPLETE")
print("====================================")
print("TRADES =",len(trades))
print("ERRORS =",len(errors))
print("ZIP =",OUT.resolve())
print()
print("=== ALL_TIMING / ALL ===")
for r in summary:
    if r["scope"]=="ALL_TIMING" and r["period"]=="ALL":
        print(r["variant"],
              "| NET",r["net_after_extra_fee"],
              "| Δ",r["net_improvement"],
              "| adds",r["adds"],
              "| false",r["false_breaks"],
              "| MDD",r["mdd_pctp"])
print()
print("=== STRICT_TIMING / ALL ===")
for r in summary:
    if r["scope"]=="STRICT_TIMING" and r["period"]=="ALL":
        print(r["variant"],
              "| NET",r["net_after_extra_fee"],
              "| Δ",r["net_improvement"],
              "| adds",r["adds"],
              "| false",r["false_breaks"],
              "| MDD",r["mdd_pctp"])
