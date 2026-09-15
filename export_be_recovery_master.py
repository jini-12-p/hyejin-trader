#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BE recovery master exporter.

READ-ONLY against SQLite DB + Bybit public 1m klines.
No orders, no DB writes.

Input: BE_CANDIDATES_ALL_*.csv generated from scan CSV history.
- dedupes variant copies into one independent BE event (same time/symbol/BE price)
- chooses a representative shadow with priority P_V27_1 > P_V26 > P_V27_4_1 > non-ghost > ghost
- resolves exact entry/opened_at from research_shadow_reviews when possible
- fetches 1m Bybit klines from BE anchor through +180m
- records early path (1/3/5/10/15/30/60/120/180m), TP1/TP2 recovery, drawdown timing,
  and compact first-15-full-minute path for offline rule search.
"""
from __future__ import annotations

import csv
import json
import sqlite3
import time
import urllib.parse
import urllib.request
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

KST = timezone(timedelta(hours=9))
API_BASE = "https://api.bybit.com"
TP1_PCT = 1.5
TP2_PCT = 3.0
RECOVERY_MINUTES = 180

DB_CANDIDATES = [
    Path("/root/hyejin-trader/bybit_swing/bybit_swing_bot.db"),
    Path.cwd()/"bybit_swing"/"bybit_swing_bot.db",
    Path.cwd()/"bybit_swing_bot.db",
]

REP_PRIORITY = [
    "P_V27_1",
    "P_V26",
    "P_V27_4_1",
    "P_V27_1R",
    "P_V27_4_3",
    "P_V27_4_2",
    "P_V27_3",
    "P_V27_4",
]


def fnum(v, default=None):
    try:
        if v is None or str(v).strip()=="":
            return default
        return float(v)
    except Exception:
        return default


def dtp(v):
    if not v:
        return None
    s=str(v).strip()
    # candidate CSV uses KST naive text
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s,fmt).replace(tzinfo=KST).astimezone(timezone.utc)
        except Exception:
            pass
    try:
        d=datetime.fromisoformat(s.replace("Z","+00:00"))
        if d.tzinfo is None:
            d=d.replace(tzinfo=KST)
        return d.astimezone(timezone.utc)
    except Exception:
        return None


def dtp_utc(v):
    if not v:
        return None
    try:
        d=datetime.fromisoformat(str(v).replace("Z","+00:00"))
        if d.tzinfo is None:
            d=d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except Exception:
        return None


def iso(d):
    return d.astimezone(timezone.utc).isoformat() if d else ""


def kst(d):
    return d.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S") if d else ""


def locate_db():
    for p in DB_CANDIDATES:
        if p.exists():
            return p
    root=Path("/root/hyejin-trader")
    if root.exists():
        for p in root.rglob("bybit_swing_bot.db"):
            return p
    return None


def locate_input():
    pats=[
        Path.cwd()/"bybit_swing",
        Path.cwd(),
        Path("/root/hyejin-trader/bybit_swing"),
        Path("/root/hyejin-trader"),
    ]
    hits=[]
    for base in pats:
        if base.exists():
            hits += list(base.glob("BE_CANDIDATES_ALL_*_KST.csv"))
    if not hits:
        raise SystemExit("BE_CANDIDATES_ALL_*_KST.csv not found")
    # newest filename / mtime
    hits=sorted(set(hits), key=lambda p:(p.stat().st_mtime,p.name), reverse=True)
    return hits[0]


def http_json(url, tries=5):
    err=None
    for i in range(tries):
        try:
            req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 BERecoveryAudit/1.0"})
            with urllib.request.urlopen(req,timeout=25) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            err=e
            time.sleep(1.0*(i+1))
    raise RuntimeError(err)


def get_1m(symbol,start_dt,end_dt):
    q=urllib.parse.urlencode({
        "category":"linear","symbol":symbol,"interval":"1",
        "start":int(start_dt.timestamp()*1000),"end":int(end_dt.timestamp()*1000),"limit":1000,
    })
    d=http_json(API_BASE+"/v5/market/kline?"+q)
    if int(d.get("retCode",-1))!=0:
        raise RuntimeError(f"Bybit {d.get('retCode')} {d.get('retMsg')}")
    out=[]
    for x in ((d.get("result") or {}).get("list") or []):
        if len(x)<6:
            continue
        out.append({
            "ts":datetime.fromtimestamp(int(x[0])/1000,tz=timezone.utc),
            "open":float(x[1]),"high":float(x[2]),"low":float(x[3]),
            "close":float(x[4]),"volume":float(x[5] or 0),
        })
    out.sort(key=lambda z:z["ts"])
    return out


def choose_rep(grp):
    for p in REP_PRIORITY:
        for r in grp:
            if r.get("strategy")==p:
                return r
    for r in grp:
        if "GHOST" not in str(r.get("strategy") or ""):
            return r
    return grp[0]


def event_groups(rows):
    # Exact same BE timestamp + symbol + result price = same copied event across variants.
    d=defaultdict(list)
    for r in rows:
        key=(str(r.get("time_kst") or ""),str(r.get("symbol") or ""),str(r.get("price") or ""))
        d[key].append(r)
    return [d[k] for k in sorted(d)]


def lookup_shadow(con, grp, rep):
    # Try representative first, then every copy in the group.
    order=[rep]+[r for r in grp if r is not rep]
    for r in order:
        sid=str(r.get("shadow_id") or "").strip()
        if not sid:
            continue
        x=con.execute("SELECT * FROM research_shadow_reviews WHERE shadow_id=? LIMIT 1",(sid,)).fetchone()
        if x:
            return x, sid
    return None, ""


def first_hit(bars, predicate):
    for b in bars:
        if predicate(b):
            return b["ts"]
    return None


def pct(v,entry):
    return (v/entry-1)*100 if entry else None


def window_metrics(bars, anchor_floor, entry, n):
    # Use only full minutes AFTER the BE anchor minute to avoid within-minute order ambiguity.
    start=anchor_floor+timedelta(minutes=1)
    end=start+timedelta(minutes=n)
    sel=[b for b in bars if b["ts"]>=start and b["ts"]<end]
    if not sel:
        return {}
    hi=max(b["high"] for b in sel); lo=min(b["low"] for b in sel); cl=sel[-1]["close"]
    return {
        f"post{n}_high_pct":pct(hi,entry),
        f"post{n}_low_pct":pct(lo,entry),
        f"post{n}_close_pct":pct(cl,entry),
        f"post{n}_range_pct":((hi/lo-1)*100 if lo else None),
    }


def minute_path_json(bars,anchor_floor,entry,count=15):
    start=anchor_floor+timedelta(minutes=1)
    sel=[b for b in bars if b["ts"]>=start][:count]
    out=[]
    for i,b in enumerate(sel,1):
        out.append({
            "m":i,
            "t":kst(b["ts"]),
            "o":round(pct(b["open"],entry),4),
            "h":round(pct(b["high"],entry),4),
            "l":round(pct(b["low"],entry),4),
            "c":round(pct(b["close"],entry),4),
            "v":b["volume"],
        })
    return json.dumps(out,ensure_ascii=False,separators=(",",":"))


def threshold_times(bars,anchor_floor,entry):
    start=anchor_floor+timedelta(minutes=1)
    sel=[b for b in bars if b["ts"]>=start]
    out={}
    for level in (0.25,0.5,0.75,1.0,1.5,2.0,3.0):
        t=first_hit(sel,lambda b,lv=level: pct(b["high"],entry)>=lv)
        out[f"mins_to_up_{str(level).replace('.','p')}"]=((t-start).total_seconds()/60 if t else None)
    for level in (-0.5,-1.0,-1.5,-2.0,-2.5,-3.0,-3.5,-4.0,-4.5,-5.0):
        t=first_hit(sel,lambda b,lv=level: pct(b["low"],entry)<=lv)
        out[f"mins_to_down_{str(abs(level)).replace('.','p')}"]=((t-start).total_seconds()/60 if t else None)
    return out


def classify_and_path(bars,anchor,entry):
    floor=anchor.replace(second=0,microsecond=0)
    start=floor+timedelta(minutes=1)
    end=floor+timedelta(minutes=RECOVERY_MINUTES+1)
    sel=[b for b in bars if b["ts"]>=start and b["ts"]<end]
    if not sel:
        return {"classification":"NO_KLINE_DATA"}
    tp1=entry*(1+TP1_PCT/100); tp2=entry*(1+TP2_PCT/100)
    t1=first_hit(sel,lambda b:b["high"]>=tp1)
    t2=first_hit(sel,lambda b:b["high"]>=tp2)
    hi=max(b["high"] for b in sel); lo=min(b["low"] for b in sel)
    if t2:
        cl="TP2_RECOVERED"
    elif t1:
        cl="TP1_RETOUCH_ONLY"
    elif pct(hi,entry)>=1.0:
        cl="REBOUND_1P_ONLY"
    elif pct(hi,entry)>=0.5:
        cl="REBOUND_0P5_ONLY"
    else:
        cl="WEAK_OR_FAILED"
    out={
        "classification":cl,
        "first_tp1_after_be_utc":iso(t1),"first_tp1_after_be_kst":kst(t1),
        "first_tp2_after_be_utc":iso(t2),"first_tp2_after_be_kst":kst(t2),
        "mins_to_tp1_after_be":((t1-start).total_seconds()/60 if t1 else None),
        "mins_to_tp2_after_be":((t2-start).total_seconds()/60 if t2 else None),
        "mfe_180m_pct":pct(hi,entry),"mae_180m_pct":pct(lo,entry),
    }
    # Drawdown before TP2: exclude the TP2 hit bar for conservative ordering, and include separately.
    if t2:
        before_excl=[b for b in sel if b["ts"]<t2]
        before_incl=[b for b in sel if b["ts"]<=t2]
        out["pre_tp2_low_excl_hitbar_pct"]=pct(min(b["low"] for b in before_excl),entry) if before_excl else None
        out["pre_tp2_low_incl_hitbar_pct"]=pct(min(b["low"] for b in before_incl),entry) if before_incl else None
    else:
        out["pre_tp2_low_excl_hitbar_pct"]=""
        out["pre_tp2_low_incl_hitbar_pct"]=""
    for n in (1,3,5,10,15,30,60,120,180):
        out.update(window_metrics(bars,floor,entry,n))
    out.update(threshold_times(bars,floor,entry))
    out["post15_path_json"]=minute_path_json(bars,floor,entry,15)
    return out


def main():
    inp=locate_input()
    db=locate_db()
    print("INPUT =",inp)
    print("DB =",db)
    with inp.open("r",encoding="utf-8-sig",newline="") as f:
        raw=list(csv.DictReader(f))
    grps=event_groups(raw)
    print("RAW ROWS =",len(raw))
    print("INDEPENDENT BE EVENTS =",len(grps))
    print("GHOST-ONLY EVENTS =",sum(all("GHOST" in str(x.get("strategy") or "") for x in g) for g in grps))

    con=None
    if db:
        con=sqlite3.connect(str(db)); con.row_factory=sqlite3.Row

    out=[]; api_errors=0; db_found=0
    for i,grp in enumerate(grps,1):
        rep=choose_rep(grp)
        anchor=dtp(rep.get("time_kst"))
        ghost_only=all("GHOST" in str(x.get("strategy") or "") for x in grp)
        row=None; resolved_sid=""
        if con:
            row,resolved_sid=lookup_shadow(con,grp,rep)
        entry=fnum(row["entry_price"] if row else None)
        opened=dtp_utc(row["opened_at"] if row else None)
        if row:
            db_found+=1
        # Conservative fallback only when DB row missing: research BE records use +0.1% gross accounting.
        # Flag it so offline analysis can exclude fallbacks if desired.
        entry_source="DB_EXACT" if entry else "DERIVED_BE_0P1"
        if not entry:
            be_price=fnum(rep.get("price"))
            entry=(be_price/1.001) if be_price else None

        has_pv271=any(str(x.get("strategy") or "")=="P_V27_1" for x in grp)
        has_pv26=any(str(x.get("strategy") or "")=="P_V26" for x in grp)
        has_pv2741=any(str(x.get("strategy") or "")=="P_V27_4_1" for x in grp)
        rec={
            "event_no":i,
            "be_time_kst":rep.get("time_kst",""),
            "be_time_utc":iso(anchor),
            "symbol":rep.get("symbol",""),
            "be_exit_price":fnum(rep.get("price")),
            "entry_price":entry,
            "entry_price_source":entry_source,
            "opened_at_utc":iso(opened),
            "opened_at_kst":kst(opened),
            "representative_strategy":rep.get("strategy",""),
            "representative_shadow_id":rep.get("shadow_id",""),
            "resolved_shadow_id":resolved_sid,
            "db_row_found":bool(row),
            "ghost_only_event":ghost_only,
            "has_pv271":has_pv271,
            "has_pv26":has_pv26,
            "has_pv2741":has_pv2741,
            "primary_pv271_event":bool(has_pv271 and not ghost_only),
            "variant_copy_count":len(grp),
            "variant_copies":"|".join(sorted(str(x.get("strategy") or "") for x in grp)),
            "source_files":"|".join(sorted(set(str(x.get("source_file") or "") for x in grp))),
            "candidate_mfe_pct":rep.get("mfe_pct",""),
            "candidate_mae_pct":rep.get("mae_pct",""),
        }
        try:
            if not anchor or not entry:
                raise RuntimeError("missing anchor/entry")
            start=anchor.replace(second=0,microsecond=0)-timedelta(minutes=1)
            end=anchor.replace(second=0,microsecond=0)+timedelta(minutes=RECOVERY_MINUTES+2)
            bars=get_1m(str(rep.get("symbol") or ""),start,end)
            rec.update(classify_and_path(bars,anchor,entry))
            rec["api_error"]=""
            time.sleep(0.08)
        except Exception as e:
            api_errors+=1
            rec["classification"]="API_ERROR"
            rec["api_error"]=f"{type(e).__name__}: {e}"
        out.append(rec)
        print(f"{i}/{len(grps)} {rec['symbol']} {rec['classification']} db={rec['db_row_found']} ghost={rec['ghost_only_event']}")

    if con:
        con.close()

    stamp=datetime.now(KST).strftime("%Y%m%d_%H%M_KST")
    outdir=Path.cwd()/f"BE_RECOVERY_MASTER_{stamp}"
    outdir.mkdir(exist_ok=True)
    csvp=outdir/f"BE_RECOVERY_MASTER_{stamp}.csv"
    fields=[]; seen=set()
    for r in out:
        for k in r:
            if k not in seen:
                seen.add(k); fields.append(k)
    with csvp.open("w",encoding="utf-8-sig",newline="") as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(out)

    cls=Counter(str(r.get("classification") or "") for r in out)
    primary=[r for r in out if r.get("primary_pv271_event")]
    cls_primary=Counter(str(r.get("classification") or "") for r in primary)
    summary=outdir/f"BE_RECOVERY_SUMMARY_{stamp}.txt"
    with summary.open("w",encoding="utf-8") as f:
        f.write(f"INPUT={inp}\n")
        f.write(f"DB={db}\n")
        f.write(f"RAW_ROWS={len(raw)}\n")
        f.write(f"INDEPENDENT_EVENTS={len(out)}\n")
        f.write(f"GHOST_ONLY_EVENTS={sum(bool(r.get('ghost_only_event')) for r in out)}\n")
        f.write(f"PRIMARY_PV271_EVENTS={len(primary)}\n")
        f.write(f"DB_EXACT_EVENTS={sum(r.get('entry_price_source')=='DB_EXACT' for r in out)}\n")
        f.write(f"API_ERRORS={api_errors}\n")
        f.write("CLASSIFICATION_ALL="+json.dumps(dict(cls),ensure_ascii=False,sort_keys=True)+"\n")
        f.write("CLASSIFICATION_PRIMARY_PV271="+json.dumps(dict(cls_primary),ensure_ascii=False,sort_keys=True)+"\n")
        f.write("NOTE=Early windows use full 1m bars starting from the minute AFTER the BE anchor minute to avoid within-minute order ambiguity.\n")
        f.write("NOTE=Ghost-only events are retained and flagged; use non-ghost/control subset for primary conclusions.\n")

    zipp=Path.cwd()/f"BE_RECOVERY_MASTER_{stamp}.zip"
    with zipfile.ZipFile(zipp,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(csvp,csvp.name); z.write(summary,summary.name)

    print("\n=== COMPLETE ===")
    print("EVENTS =",len(out))
    print("GHOST ONLY =",sum(bool(r.get('ghost_only_event')) for r in out))
    print("PRIMARY P_V27_1 EVENTS =",len(primary))
    print("DB EXACT =",sum(r.get('entry_price_source')=='DB_EXACT' for r in out))
    print("API ERRORS =",api_errors)
    print("CLASSIFICATION ALL =",dict(cls))
    print("CLASSIFICATION PRIMARY P_V27_1 =",dict(cls_primary))
    print("CSV =",csvp)
    print("ZIP =",zipp)


if __name__=="__main__":
    main()
