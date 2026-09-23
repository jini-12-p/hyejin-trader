#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V22 ROOT AUDIT
--------------
Purpose:
Determine whether the post-2026-09-18 collapse starts at:
A) V22 candidate generation,
B) V22->V25 confirmation,
C) only after V25 confirmation / downstream market behavior.

Authoritative sources:
- SQLite research_pv25_setups:
  each row is created when a P_V22 candidate starts a V25 WATCH.
  status CONFIRMED = V25 confirmed.
  status DROPPED   = V22 candidate failed V25 confirmation window/adverse rule.
- scan CSV RESEARCH_P_CONFIRM_WATCH rows:
  candidate-time V22 telemetry, when available.
- UNIFIED_CURRENT_0901_0922_TRADES.csv + FRESH_BASE_TRADES.csv:
  current exact BASE accepted outcome mapping.

No orders. No DB writes.
"""
from __future__ import annotations

import csv
import glob
import json
import math
import os
import sqlite3
import statistics
import zipfile
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path("/root/hyejin-trader/bybit_swing")
DB = ROOT / "bybit_swing_bot.db"
KST = timezone(timedelta(hours=9))
UTC = timezone.utc

START_KST = datetime(2026,9,1,0,0,0,tzinfo=KST)
END_KST = datetime(2026,9,24,0,0,0,tzinfo=KST)

OUT_DAILY = ROOT / "V22_ROOT_AUDIT_DAILY.csv"
OUT_PERIOD = ROOT / "V22_ROOT_AUDIT_PERIOD.csv"
OUT_FEATURE = ROOT / "V22_ROOT_AUDIT_FEATURE_SHIFT.csv"
OUT_OUTCOME = ROOT / "V22_ROOT_AUDIT_OUTCOME.csv"
OUT_SETUPS = ROOT / "V22_ROOT_AUDIT_SETUPS.csv"
OUT_SUMMARY = ROOT / "V22_ROOT_AUDIT_SUMMARY.txt"
OUT_ZIP = ROOT / "V22_ROOT_AUDIT_RESULTS.zip"

FEATURES = [
    "p_v2_score",
    "p_v21_persistence_score",
    "p_v2_signal_pass_count",
    "p_v2_structure_score",
    "p_v22_heat_count",
    "p_v22_structure_weak_count",
    "rsi",
    "rsi_delta",
    "ema20_slope_pct",
    "ema9_ema20_gap_pct",
    "live_candle_gain_pct",
    "pullback_from_high_pct",
    "rebound_from_low_pct",
    "one_hour_signed_move_pct",
    "btc_15m_change_pct",
    "eth_15m_change_pct",
    "btc_4h_change_pct",
    "eth_4h_change_pct",
]

def dt(v):
    if v in (None,""):
        return None
    try:
        d = datetime.fromisoformat(str(v).replace("Z","+00:00"))
        if d.tzinfo is None:
            d=d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except Exception:
        return None

def fk(d):
    return d.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S") if d else ""

def datek(d):
    return d.astimezone(KST).strftime("%Y-%m-%d") if d else ""

def fv(v):
    try:
        if v is None or str(v).strip()=="":
            return None
        x=float(v)
        if math.isnan(x):
            return None
        return x
    except Exception:
        return None

def iv(v):
    x=fv(v)
    return None if x is None else int(x)

def truth(v):
    s=str(v or "").strip().lower()
    return s in {"1","true","t","yes","y"}

def median(xs):
    z=[float(x) for x in xs if x is not None]
    return statistics.median(z) if z else None

def mean(xs):
    z=[float(x) for x in xs if x is not None]
    return sum(z)/len(z) if z else None

def write_csv(path, rows):
    if not rows:
        path.write_text("",encoding="utf-8-sig")
        return
    keys=[]; seen=set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with path.open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=keys,extrasaction="ignore")
        w.writeheader(); w.writerows(rows)

def period_of(kdate):
    if "2026-09-01" <= kdate <= "2026-09-17": return "0901_0917"
    if "2026-09-18" <= kdate <= "2026-09-22": return "0918_0922"
    if kdate == "2026-09-23": return "0923"
    return "OTHER"

if not DB.exists():
    raise SystemExit(f"DB missing: {DB}")

# 1) Authoritative V22 candidate -> V25 setup table
con=sqlite3.connect(DB)
con.row_factory=sqlite3.Row
rows=con.execute("""
SELECT id,setup_id,symbol,first_seen_at,last_seen_at,trigger_price,lowest_price,last_price,
       snapshot_json,status,last_5m_bucket,confirmed_at,confirmed_price,expires_at,note
FROM research_pv25_setups
ORDER BY first_seen_at
""").fetchall()
con.close()

setups=[]
for r in rows:
    first=dt(r["first_seen_at"])
    if not first:
        continue
    if not (START_KST.astimezone(UTC) <= first < END_KST.astimezone(UTC)):
        continue
    conf=dt(r["confirmed_at"])
    status=str(r["status"] or "").upper()
    trigger=fv(r["trigger_price"])
    low=fv(r["lowest_price"])
    adverse=None
    if trigger and low:
        adverse=(low/trigger-1)*100
    latency=None
    if conf:
        latency=(conf-first).total_seconds()/60
    setups.append({
        "setup_id":str(r["setup_id"]),
        "symbol":str(r["symbol"]),
        "first_utc":first,
        "first_kst":fk(first),
        "date":datek(first),
        "period":period_of(datek(first)),
        "status":status,
        "confirmed_utc":conf,
        "confirmed_kst":fk(conf),
        "confirm_latency_min":latency,
        "trigger_price":trigger,
        "lowest_price":low,
        "adverse_before_finish_pct":adverse,
        "note":str(r["note"] or ""),
        "db_snapshot":str(r["snapshot_json"] or ""),
    })

# 2) Candidate-time telemetry from RESEARCH_P_CONFIRM_WATCH scan rows.
# Search all likely local scan/archive CSVs and keep first WATCH row for each setup_id.
patterns=[
    str(ROOT/"scan_rejected.csv"),
    str(ROOT/"scan_rejected_*.csv"),
    str(ROOT/"scan_*.csv"),
    str(ROOT/"scan_archive"/"**"/"*.csv"),
]
files=[]
for pat in patterns:
    files += glob.glob(pat,recursive=True)
files=sorted(set(f for f in files if os.path.isfile(f)))

watch={}
for fi,fn in enumerate(files,1):
    try:
        with open(fn,"r",encoding="utf-8-sig",errors="replace",newline="") as f:
            rd=csv.DictReader(f)
            if not rd.fieldnames:
                continue
            if "result" not in rd.fieldnames:
                continue
            for rr in rd:
                if str(rr.get("result") or "") != "RESEARCH_P_CONFIRM_WATCH":
                    continue
                sid=str(rr.get("p_v25_setup_id") or "")
                if not sid or sid in watch:
                    continue
                watch[sid]=dict(rr)
    except Exception:
        pass

# Fallback telemetry from final DB snapshot only if WATCH row unavailable.
# Mark source so candidate-time vs later-watch data never gets confused.
for s in setups:
    sid=s["setup_id"]
    snap={}
    source=""
    if sid in watch:
        snap=watch[sid]
        source="WATCH_SCAN"
    else:
        try:
            snap=json.loads(s["db_snapshot"] or "{}")
            source="DB_FINAL_SNAPSHOT"
        except Exception:
            snap={}
            source="NONE"
    s["feature_source"]=source
    for k in FEATURES:
        s[k]=fv(snap.get(k))
    s["v22_structure_incomplete"]=truth(snap.get("p_v22_structure_incomplete"))
    s["v22_late_extension"]=truth(snap.get("p_v22_late_extension"))

# 3) Current BASE outcomes map.
outcome={}
def ingest_outcome_csv(path, fresh=False):
    if not path.exists():
        return
    with path.open("r",encoding="utf-8-sig",errors="replace",newline="") as f:
        rd=csv.DictReader(f)
        for r in rd:
            sid=str(r.get("setup_id") or "")
            if not sid:
                continue
            acc=iv(r.get("accepted")) or 0
            # For fresh file, override overlap after its evaluation start.
            if fresh:
                t=str(r.get("entry_time_kst") or "")
                if t and t < "2026-09-22 14:18:34":
                    continue
            outcome[sid]={
                "accepted":acc,
                "result":str(r.get("result") or ""),
                "net_pct":fv(r.get("net_pct")),
                "mfe_pct":fv(r.get("mfe_pct")),
                "mae_pct":fv(r.get("mae_pct")),
                "block_reason":str(r.get("block_reason") or ""),
                "entry_time_kst":str(r.get("entry_time_kst") or ""),
            }

ingest_outcome_csv(ROOT/"UNIFIED_CURRENT_0901_0922_TRADES.csv")
ingest_outcome_csv(ROOT/"FRESH_BASE_TRADES.csv",fresh=True)

for s in setups:
    o=outcome.get(s["setup_id"],{})
    s["base_accepted"]=int(o.get("accepted",0) or 0)
    s["base_result"]=str(o.get("result") or "")
    s["base_net_pct"]=o.get("net_pct")
    s["base_mfe_pct"]=o.get("mfe_pct")
    s["base_mae_pct"]=o.get("mae_pct")
    s["base_block_reason"]=str(o.get("block_reason") or "")

# 4) Rolling V22 density based on first_seen.
q=deque()
for s in setups:
    t=s["first_utc"]
    while q and q[0] < t-timedelta(hours=2):
        q.popleft()
    q.append(t)
    s["v22_2h_count"]=len(q)

# Rolling V25 confirmed density based on confirmed_at.
conf_order=sorted([s for s in setups if s["confirmed_utc"]],key=lambda x:x["confirmed_utc"])
q=deque()
for s in conf_order:
    t=s["confirmed_utc"]
    while q and q[0] < t-timedelta(hours=2):
        q.popleft()
    q.append(t)
    s["v25_2h_count_at_confirm"]=len(q)
for s in setups:
    s.setdefault("v25_2h_count_at_confirm",None)

# 5) Daily stats
daily=[]
dates=[f"2026-09-{d:02d}" for d in range(1,24)]
for d in dates:
    z=[x for x in setups if x["date"]==d]
    mature=[x for x in z if x["status"]!="WATCH"]
    conf=[x for x in mature if x["status"]=="CONFIRMED"]
    drop=[x for x in mature if x["status"]=="DROPPED"]
    acc=[x for x in conf if x["base_accepted"]==1]
    daily.append({
        "date":d,
        "v22_candidates":len(z),
        "mature_candidates":len(mature),
        "v25_confirmed":len(conf),
        "v22_to_v25_rate_pct":round(100*len(conf)/len(mature),2) if mature else "",
        "dropped":len(drop),
        "watch_open":sum(x["status"]=="WATCH" for x in z),
        "confirm_latency_med_min":round(median([x["confirm_latency_min"] for x in conf]),3) if conf else "",
        "v22_2h_med":round(median([x["v22_2h_count"] for x in z]),3) if z else "",
        "v25_2h_med":round(median([x["v25_2h_count_at_confirm"] for x in conf]),3) if conf else "",
        "base_accepted":len(acc),
        "tp":sum(x["base_result"]=="TP20_FULL" for x in acc),
        "stop":sum(x["base_result"]=="STOP" for x in acc),
        "pp12":sum(x["base_result"]=="PROFIT_PROTECT_EXIT" for x in acc),
        "late":sum(x["base_result"]=="LATE_FAILURE_EXIT" for x in acc),
        "base_net_pct":round(sum(x["base_net_pct"] or 0 for x in acc),6),
    })

# 6) Period stats
periods=["0901_0917","0918_0922","0923"]
period_rows=[]
for p in periods:
    z=[x for x in setups if x["period"]==p]
    mature=[x for x in z if x["status"]!="WATCH"]
    conf=[x for x in mature if x["status"]=="CONFIRMED"]
    drop=[x for x in mature if x["status"]=="DROPPED"]
    acc=[x for x in conf if x["base_accepted"]==1]
    ndays=17 if p=="0901_0917" else (5 if p=="0918_0922" else 1)
    period_rows.append({
        "period":p,
        "days":ndays,
        "v22_candidates":len(z),
        "v22_per_day":round(len(z)/ndays,3),
        "mature_candidates":len(mature),
        "v25_confirmed":len(conf),
        "v25_per_day":round(len(conf)/ndays,3),
        "v22_to_v25_rate_pct":round(100*len(conf)/len(mature),3) if mature else "",
        "dropped":len(drop),
        "drop_rate_pct":round(100*len(drop)/len(mature),3) if mature else "",
        "confirm_latency_med_min":round(median([x["confirm_latency_min"] for x in conf]),3) if conf else "",
        "v22_2h_med":round(median([x["v22_2h_count"] for x in z]),3) if z else "",
        "v22_2h_mean":round(mean([x["v22_2h_count"] for x in z]),3) if z else "",
        "v25_2h_med":round(median([x["v25_2h_count_at_confirm"] for x in conf]),3) if conf else "",
        "base_accepted":len(acc),
        "tp_rate_pct":round(100*sum(x["base_result"]=="TP20_FULL" for x in acc)/len(acc),3) if acc else "",
        "stop_rate_pct":round(100*sum(x["base_result"]=="STOP" for x in acc)/len(acc),3) if acc else "",
        "pp12_rate_pct":round(100*sum(x["base_result"]=="PROFIT_PROTECT_EXIT" for x in acc)/len(acc),3) if acc else "",
        "base_net_pct":round(sum(x["base_net_pct"] or 0 for x in acc),6),
        "net_per_accepted":round(sum(x["base_net_pct"] or 0 for x in acc)/len(acc),6) if acc else "",
        "watch_scan_coverage_pct":round(100*sum(x["feature_source"]=="WATCH_SCAN" for x in z)/len(z),2) if z else "",
    })

# 7) Feature shift, candidate-time WATCH rows only to avoid timing contamination.
feature_rows=[]
for feat in FEATURES:
    for p in periods:
        z=[x for x in setups if x["period"]==p and x["feature_source"]=="WATCH_SCAN" and x.get(feat) is not None]
        conf=[x for x in z if x["status"]=="CONFIRMED"]
        drop=[x for x in z if x["status"]=="DROPPED"]
        feature_rows.append({
            "feature":feat,
            "period":p,
            "n_all":len(z),
            "median_all":round(median([x[feat] for x in z]),6) if z else "",
            "median_confirmed":round(median([x[feat] for x in conf]),6) if conf else "",
            "median_dropped":round(median([x[feat] for x in drop]),6) if drop else "",
        })

# 8) Outcome by V22 density buckets for accepted current BASE.
out_rows=[]
buckets=[("V22_2H_1_4",1,4),("V22_2H_5_7",5,7),("V22_2H_8_11",8,11),("V22_2H_12_PLUS",12,999)]
for p in periods:
    for name,lo,hi in buckets:
        z=[x for x in setups if x["period"]==p and x["base_accepted"]==1 and lo <= (x["v22_2h_count"] or 0) <= hi]
        if not z:
            continue
        out_rows.append({
            "period":p,
            "bucket":name,
            "n":len(z),
            "net_pct":round(sum(x["base_net_pct"] or 0 for x in z),6),
            "avg_net":round(sum(x["base_net_pct"] or 0 for x in z)/len(z),6),
            "tp_rate_pct":round(100*sum(x["base_result"]=="TP20_FULL" for x in z)/len(z),3),
            "stop_rate_pct":round(100*sum(x["base_result"]=="STOP" for x in z)/len(z),3),
            "pp12_rate_pct":round(100*sum(x["base_result"]=="PROFIT_PROTECT_EXIT" for x in z)/len(z),3),
        })

# Export setup rows in compact form
compact=[]
for x in setups:
    d={k:v for k,v in x.items() if k not in ("first_utc","confirmed_utc","db_snapshot")}
    compact.append(d)

write_csv(OUT_DAILY,daily)
write_csv(OUT_PERIOD,period_rows)
write_csv(OUT_FEATURE,feature_rows)
write_csv(OUT_OUTCOME,out_rows)
write_csv(OUT_SETUPS,compact)

# Diagnostic conclusion rule
pm={r["period"]:r for r in period_rows}
a=pm.get("0901_0917",{})
b=pm.get("0918_0922",{})
cand_ratio=(b.get("v22_per_day",0)/a.get("v22_per_day",1)) if a.get("v22_per_day") else None
conv_delta=(float(b.get("v22_to_v25_rate_pct") or 0)-float(a.get("v22_to_v25_rate_pct") or 0))

if cand_ratio is not None and cand_ratio >= 1.6 and abs(conv_delta) < 10:
    diagnosis="V22_CANDIDATE_FLOOD_PRIMARY"
elif cand_ratio is not None and cand_ratio < 1.3 and conv_delta >= 10:
    diagnosis="V25_CONFIRMATION_RATE_JUMP_PRIMARY"
elif cand_ratio is not None and cand_ratio >= 1.4 and conv_delta >= 8:
    diagnosis="BOTH_V22_FLOOD_AND_V25_CONFIRM_JUMP"
else:
    diagnosis="NO_SINGLE_UPSTREAM_COUNT_SHIFT__CHECK_QUALITY_AND_MARKET"

drop_notes=Counter(x["note"] for x in setups if x["status"]=="DROPPED")
lines=[
    "V22 ROOT AUDIT",
    f"DB={DB}",
    "",
    "[V22 / V25 DEFINITIONS]",
    "V22 candidate => research_pv25_setups WATCH row",
    "V25 confirmed => same row status=CONFIRMED",
    "V25 confirm window=5~20m; confirmed closed 5m bullish + close > prior 5m high",
    "DROP if >20m or pre-confirm adverse <= -1.5%",
    "",
    "[PERIOD]",
]
for r in period_rows:
    lines.append(
        f"{r['period']}: V22={r['v22_candidates']} ({r['v22_per_day']}/day), "
        f"V25={r['v25_confirmed']} ({r['v25_per_day']}/day), "
        f"convert={r['v22_to_v25_rate_pct']}%, drop={r['drop_rate_pct']}%, "
        f"latency_med={r['confirm_latency_med_min']}m, "
        f"accepted={r['base_accepted']} TP={r['tp_rate_pct']}% STOP={r['stop_rate_pct']}% "
        f"NET={r['base_net_pct']} avg={r['net_per_accepted']}"
    )
lines += [
    "",
    "[SHIFT]",
    f"V22 per-day ratio post/pre={cand_ratio:.3f}" if cand_ratio is not None else "V22 ratio=N/A",
    f"V22->V25 conversion delta post-pre={conv_delta:+.3f}pp",
    f"diagnosis={diagnosis}",
    "",
    "[DROP NOTES]",
    str(dict(drop_notes)),
    "",
    f"candidate-time WATCH telemetry found={len(watch)} unique setup_ids",
    "Feature-shift conclusions should use WATCH_SCAN rows only; DB_FINAL_SNAPSHOT is later in the watch and is exported only for reference.",
    "",
    "[FILES]",
    OUT_DAILY.name,OUT_PERIOD.name,OUT_FEATURE.name,OUT_OUTCOME.name,OUT_SETUPS.name
]
OUT_SUMMARY.write_text("\n".join(lines)+"\n",encoding="utf-8")
print("\n".join(lines),flush=True)

with zipfile.ZipFile(OUT_ZIP,"w",zipfile.ZIP_DEFLATED) as z:
    for p in [OUT_DAILY,OUT_PERIOD,OUT_FEATURE,OUT_OUTCOME,OUT_SETUPS,OUT_SUMMARY]:
        z.write(p,arcname=p.name)
print("DONE:",OUT_ZIP,flush=True)
