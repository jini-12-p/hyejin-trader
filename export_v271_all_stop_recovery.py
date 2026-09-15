#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
All-history P_V27_1 STOP recovery exporter.

READ-ONLY against SQLite DB + Bybit public 1m klines.
No orders, no DB writes.

For every actual P_V27_1 STOP:
- exact source P_V26 row via p_v271_source_shadow_id
- V25-confirmation flags inherited by P_V26
- V27-1 stop type/stage reason
- 180m recovery classification from BOTH first staged-signal anchor and final result anchor
- compact 1m path metrics around each stop anchor
- source entry snapshot JSON for offline analysis
"""
from __future__ import annotations
import csv, json, sqlite3, time, urllib.parse, urllib.request, zipfile
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

KST = timezone(timedelta(hours=9))
API_BASE = "https://api.bybit.com"
TP1_PCT = 1.5
TP2_PCT = 3.0
RECOVERY_MINUTES = 180
PRE_MINUTES = 10
DB_CANDIDATES = [
    Path("/root/hyejin-trader/bybit_swing/bybit_swing_bot.db"),
    Path.cwd()/"bybit_swing"/"bybit_swing_bot.db",
    Path.cwd()/"bybit_swing_bot.db",
]

def jf(v):
    try:
        x=json.loads(v or "{}")
        return x if isinstance(x,dict) else {}
    except Exception:
        return {}

def fnum(v, default=0.0):
    try: return float(v)
    except Exception: return float(default)

def dtp(v):
    if not v: return None
    try:
        d=datetime.fromisoformat(str(v).replace("Z","+00:00"))
        if d.tzinfo is None: d=d.replace(tzinfo=timezone.utc)
        return d.astimezone(timezone.utc)
    except Exception: return None

def iso(d): return d.astimezone(timezone.utc).isoformat() if d else ""
def kst(d): return d.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S") if d else ""

def locate_db():
    for p in DB_CANDIDATES:
        if p.exists(): return p
    root=Path("/root/hyejin-trader")
    if root.exists():
        for p in root.rglob("bybit_swing_bot.db"):
            return p
    raise SystemExit("DB not found")

def http_json(url, tries=5):
    err=None
    for i in range(tries):
        try:
            req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 PV271AllStopAudit/1.0"})
            with urllib.request.urlopen(req,timeout=25) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            err=e; time.sleep(1.0*(i+1))
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
        if len(x)<6: continue
        out.append({"ts":datetime.fromtimestamp(int(x[0])/1000,tz=timezone.utc),
                    "open":float(x[1]),"high":float(x[2]),"low":float(x[3]),"close":float(x[4]),"volume":float(x[5] or 0)})
    out.sort(key=lambda z:z["ts"])
    return out

def classify(bars,anchor,entry,tp1,tp2):
    if not anchor: return {"classification":"NO_ANCHOR"}
    end=anchor+timedelta(minutes=RECOVERY_MINUTES)
    sel=[b for b in bars if b["ts"]>=anchor.replace(second=0,microsecond=0) and b["ts"]<=end]
    if not sel: return {"classification":"NO_KLINE_DATA"}
    hi=max(b["high"] for b in sel); lo=min(b["low"] for b in sel)
    fe=next((b["ts"] for b in sel if b["high"]>=entry),None)
    f1=next((b["ts"] for b in sel if b["high"]>=tp1),None)
    f2=next((b["ts"] for b in sel if b["high"]>=tp2),None)
    cl="FALSE_STOP_TP2" if f2 else "FALSE_STOP_TP1" if f1 else "RECOVERED_ENTRY_ONLY" if fe else "VALID_STOP"
    return {
        "classification":cl,"highest_180m":hi,"lowest_180m":lo,
        "max_up_from_entry_pct":(hi/entry-1)*100 if entry else None,
        "max_down_from_entry_pct":(lo/entry-1)*100 if entry else None,
        "first_entry_recovery_utc":iso(fe),"first_tp1_utc":iso(f1),"first_tp2_utc":iso(f2),
        "mins_to_entry":((fe-anchor).total_seconds()/60 if fe else None),
        "mins_to_tp1":((f1-anchor).total_seconds()/60 if f1 else None),
        "mins_to_tp2":((f2-anchor).total_seconds()/60 if f2 else None),
    }

def path_metrics(bars,anchor,entry):
    if not anchor: return {}
    floor=anchor.replace(second=0,microsecond=0)
    pre=[b for b in bars if b["ts"]<floor][-5:]
    post=[b for b in bars if b["ts"]>=floor]
    out={}
    if pre:
        last=pre[-1]
        out.update({
            "pre_last_close_pct":(last["close"]/entry-1)*100,
            "pre_last_low_pct":(last["low"]/entry-1)*100,
            "pre_last_high_pct":(last["high"]/entry-1)*100,
            "pre_last_body_pct":((last["close"]/last["open"]-1)*100 if last["open"] else None),
            "pre5_low_pct":(min(x["low"] for x in pre)/entry-1)*100,
            "pre5_high_pct":(max(x["high"] for x in pre)/entry-1)*100,
        })
        if len(pre)>=2 and pre[-2]["close"]:
            out["pre1_close_change_pct"]=(pre[-1]["close"]/pre[-2]["close"]-1)*100
        if len(pre)>=4 and pre[-4]["close"]:
            out["pre3_close_change_pct"]=(pre[-1]["close"]/pre[-4]["close"]-1)*100
        if len(pre)>=5 and pre[0]["close"]:
            out["pre4_close_change_pct"]=(pre[-1]["close"]/pre[0]["close"]-1)*100
        vols=[x["volume"] for x in pre if x["volume"] is not None]
        if vols:
            out["pre5_volume_mean"]=sum(vols)/len(vols)
            out["pre_last_volume_ratio_to_pre5"]=(last["volume"]/(sum(vols)/len(vols))) if sum(vols)>0 else None
    for n in (1,3,5):
        p=post[:n]
        if p:
            hi=max(x["high"] for x in p); lo=min(x["low"] for x in p); cl=p[-1]["close"]
            out[f"post{n}_high_pct"]=(hi/entry-1)*100
            out[f"post{n}_low_pct"]=(lo/entry-1)*100
            out[f"post{n}_close_pct"]=(cl/entry-1)*100
    return out

def stage_anchors(row):
    det=jf(row["result_details"]); snap=jf(row["snapshot_json"])
    stage=snap.get("v271_stop_stage") if isinstance(snap.get("v271_stop_stage"),dict) else {}
    result_anchor=dtp(row["result_ts"])
    stop_type=str(det.get("stop_type") or "")
    started=dtp(stage.get("started_at"))
    staged=(stop_type=="V27_1_STAGED_STOP" or "v271_stage_reason" in det)
    signal_anchor=started if staged and started else result_anchor
    return signal_anchor,result_anchor,stop_type,det,stage

def main():
    db=locate_db(); print("DB=",db)
    con=sqlite3.connect(str(db)); con.row_factory=sqlite3.Row
    stops=con.execute("SELECT * FROM research_shadow_reviews WHERE variant='P_V27_1' AND result='STOP' ORDER BY result_ts,id").fetchall()
    now=datetime.now(timezone.utc)
    rows=[]; cache={}; errors=0
    for i,y in enumerate(stops,1):
        ys=jf(y["snapshot_json"]); sid=str(ys.get("p_v271_source_shadow_id") or "")
        x=con.execute("SELECT * FROM research_shadow_reviews WHERE variant='P_V26' AND shadow_id=? LIMIT 1",(sid,)).fetchone()
        xs=jf(x["snapshot_json"] if x else "{}")
        signal_anchor,result_anchor,stop_type,det,stage=stage_anchors(y)
        entry=fnum(y["entry_price"]); tp1=entry*(1+TP1_PCT/100); tp2=entry*(1+TP2_PCT/100)
        rec={
            "no":i,"symbol":y["symbol"],"v271_shadow_id":y["shadow_id"],"source_v26_shadow_id":sid,
            "source_found":bool(x),"entry_utc":y["opened_at"],"entry_kst":kst(dtp(y["opened_at"])),"entry_price":entry,
            "v25_5m_bullish":xs.get("p_v25_5m_bullish"),"v25_prev_high_break":xs.get("p_v25_prev_high_break"),
            "v25_closed_5m_price":xs.get("p_v25_closed_5m_price"),"v26_block_reason":xs.get("p_v26_block_reason"),
            "stop_type":stop_type,"stage_reason":det.get("v271_stage_reason","") ,"v271_total_gross_pct":det.get("v271_total_gross_pct",""),
            "v271_mfe_pct":y["mfe_pct"],"v271_mae_pct":y["mae_pct"],"result_price":y["result_price"],
            "signal_anchor_utc":iso(signal_anchor),"signal_anchor_kst":kst(signal_anchor),
            "result_anchor_utc":iso(result_anchor),"result_anchor_kst":kst(result_anchor),
            "stage_signal_price":stage.get("signal_price",""),
            "source_entry_snapshot_json":x["snapshot_json"] if x else "",
            "v271_result_details":y["result_details"],
        }
        # useful entry features copied out
        for key in ["rsi","live_candle_gain_pct","live_body_pct","pullback_from_high_pct","new_score_volume","volume_ratio","rsi_delta",
                    "ema20_slope_prev1_pct","ema20_slope_pct","ema9_slope_pct","ema9_ema20_gap_pct","one_hour_signed_move_pct",
                    "btc_15m_change_pct","eth_15m_change_pct","btc_eth_both_down_15m"]:
            rec["entry_"+key]=xs.get(key,"")
        try:
            if not signal_anchor or not result_anchor:
                raise RuntimeError("missing stop anchor")
            start=min(signal_anchor,result_anchor)-timedelta(minutes=PRE_MINUTES)
            end=max(signal_anchor,result_anchor)+timedelta(minutes=RECOVERY_MINUTES)
            ck=(str(y["symbol"]),int(start.timestamp()//60),int(end.timestamp()//60))
            if ck not in cache:
                cache[ck]=get_1m(str(y["symbol"]),start,end); time.sleep(0.08)
            bars=cache[ck]
            cs=classify(bars,signal_anchor,entry,tp1,tp2)
            cr=classify(bars,result_anchor,entry,tp1,tp2)
            for k,v in cs.items(): rec["signal_"+k]=v
            for k,v in cr.items(): rec["result_"+k]=v
            for k,v in path_metrics(bars,signal_anchor,entry).items(): rec["signal_"+k]=v
            for k,v in path_metrics(bars,result_anchor,entry).items(): rec["result_"+k]=v
            rec["anchor_class_same"]=(cs.get("classification")==cr.get("classification"))
            rec["api_error"]=""
        except Exception as e:
            errors+=1; rec["api_error"]=f"{type(e).__name__}: {e}"
            rec["signal_classification"]="API_ERROR"; rec["result_classification"]="API_ERROR"
        rows.append(rec)
        if i%10==0 or i==len(stops):
            print(f"{i}/{len(stops)} done, errors={errors}")
    con.close()
    stamp=datetime.now(KST).strftime("%Y%m%d_%H%M_KST")
    outdir=Path.cwd()/f"PV271_ALL_STOP_RECOVERY_{stamp}"; outdir.mkdir(exist_ok=True)
    csvp=outdir/f"PV271_ALL_STOP_RECOVERY_{stamp}.csv"
    keys=[]; seen=set()
    for r in rows:
        for k in r:
            if k not in seen: seen.add(k); keys.append(k)
    with csvp.open("w",newline="",encoding="utf-8-sig") as f:
        w=csv.DictWriter(f,fieldnames=keys); w.writeheader(); w.writerows(rows)
    sig=Counter(str(r.get("signal_classification") or "") for r in rows)
    res=Counter(str(r.get("result_classification") or "") for r in rows)
    st=Counter(str(r.get("stop_type") or "") for r in rows)
    summ=outdir/f"PV271_ALL_STOP_SUMMARY_{stamp}.txt"
    with summ.open("w",encoding="utf-8") as f:
        f.write(f"DB={db}\nSTOP_ROWS={len(rows)}\nAPI_ERRORS={errors}\n")
        f.write("STOP_TYPES="+json.dumps(dict(st),ensure_ascii=False,sort_keys=True)+"\n")
        f.write("SIGNAL_CLASS="+json.dumps(dict(sig),ensure_ascii=False,sort_keys=True)+"\n")
        f.write("RESULT_CLASS="+json.dumps(dict(res),ensure_ascii=False,sort_keys=True)+"\n")
        f.write("NOTE=P_V27_1 actual STOPs only. Recovery uses public Bybit 1m bars for 180m after each stop anchor.\n")
        f.write("NOTE=P_V26 source is exact by p_v271_source_shadow_id; P_V26 entries are V25 confirmed-5m-break entries that passed V26 guards.\n")
    zipp=Path.cwd()/f"PV271_ALL_STOP_RECOVERY_{stamp}.zip"
    with zipfile.ZipFile(zipp,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(csvp,csvp.name); z.write(summ,summ.name)
    print("\n=== COMPLETE ===")
    print("STOP ROWS =",len(rows)); print("API ERRORS=",errors)
    print("STOP TYPES=",dict(st)); print("SIGNAL CLASS=",dict(sig)); print("RESULT CLASS=",dict(res))
    print("ZIP =",zipp)

if __name__=="__main__": main()
