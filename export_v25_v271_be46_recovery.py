#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V25 동일진입 + V27-1 동일손절 exact-pair BE 46건의 BE 이후 180분 경로를 재생하고,
BE 잔량 25% Recovery 조합을 전수 비교한다.

READ ONLY:
- SQLite DB write 없음
- 주문 없음
- Bybit public 1m kline 조회만 사용

입력:
bybit_swing/V25_V271_UNIFIED_ENTRY_MASTER_*_KST.csv

핵심 cohort:
has_v25_anchor=1
has_v271_exact_pair=1
v271_result=BE_EXIT

기존 BE gross 기준:
TP1 +1.5%에서 원포지션 50% 익절 = +0.75%p
남은 50%를 BE +0.1%에서 종료 = +0.05%p
합계 = +0.80%p

Recovery 후보:
TP1 50% 익절 + BE에서 원포지션 25% 보호청산(+0.1%)
마지막 원포지션 25%만
 target / hard-stop / timeout 조합으로 관리.
"""
from __future__ import annotations

import csv, json, time, urllib.parse, urllib.request, zipfile, statistics
from pathlib import Path
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))
API = "https://api.bybit.com"
RECOVERY_MINUTES = 180
BASELINE_GROSS = 0.80
LOCKED_BE_PARTIAL_GROSS = 0.75 + 0.25*0.10  # 0.775

TARGETS = [0.50,0.75,1.00,1.25,1.50,2.00,3.00]
STOPS   = [-1.50,-2.00,-2.50,-3.00,-3.50,-4.00,-4.50,-5.00]
TIMEOUTS= [15,30,60,90,120,180]

def f(v, default=None):
    try:
        if v is None or str(v).strip()=="":
            return default
        return float(v)
    except:
        return default

def b(v):
    return str(v).strip().lower() in ("1","true","yes","y")

def dt(v):
    if not v: return None
    try:
        x=datetime.fromisoformat(str(v).replace("Z","+00:00"))
        if x.tzinfo is None: x=x.replace(tzinfo=timezone.utc)
        return x.astimezone(timezone.utc)
    except:
        return None

def iso(x): return x.astimezone(timezone.utc).isoformat() if x else ""
def kst(x): return x.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S") if x else ""
def pct(price, entry): return (price/entry-1)*100 if entry else None

def latest_master():
    bases=[Path.cwd()/"bybit_swing", Path("/root/hyejin-trader/bybit_swing")]
    hits=[]
    for base in bases:
        if base.exists():
            hits += list(base.glob("V25_V271_UNIFIED_ENTRY_MASTER_*_KST.csv"))
    if not hits:
        raise SystemExit("V25_V271_UNIFIED_ENTRY_MASTER_*_KST.csv not found")
    return sorted(set(hits), key=lambda p:(p.stat().st_mtime,p.name), reverse=True)[0]

def http_json(url, tries=6):
    err=None
    for i in range(tries):
        try:
            req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 HJ-BE-Audit/1.0"})
            with urllib.request.urlopen(req,timeout=25) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            err=e
            time.sleep(1.2*(i+1))
    raise RuntimeError(err)

def klines(symbol,start,end):
    q=urllib.parse.urlencode({
        "category":"linear","symbol":symbol,"interval":"1",
        "start":int(start.timestamp()*1000),"end":int(end.timestamp()*1000),"limit":1000
    })
    d=http_json(API+"/v5/market/kline?"+q)
    if int(d.get("retCode",-1))!=0:
        raise RuntimeError(f"Bybit {d.get('retCode')} {d.get('retMsg')}")
    out=[]
    for x in ((d.get("result") or {}).get("list") or []):
        if len(x)<6: continue
        out.append({
            "ts":datetime.fromtimestamp(int(x[0])/1000,tz=timezone.utc),
            "open":float(x[1]),"high":float(x[2]),"low":float(x[3]),
            "close":float(x[4]),"volume":float(x[5] or 0),
        })
    out.sort(key=lambda z:z["ts"])
    return out

def full_post_bars(allbars, anchor):
    floor=anchor.replace(second=0,microsecond=0)
    start=floor+timedelta(minutes=1)
    end=start+timedelta(minutes=RECOVERY_MINUTES)
    return [x for x in allbars if start <= x["ts"] < end], start

def first_hit(sel, fn):
    for i,x in enumerate(sel):
        if fn(x): return i,x
    return None,None

def window(sel,entry,n):
    x=sel[:n]
    if not x: return {}
    return {
        f"m{n}_high_pct":pct(max(r["high"] for r in x),entry),
        f"m{n}_low_pct":pct(min(r["low"] for r in x),entry),
        f"m{n}_close_pct":pct(x[-1]["close"],entry)
    }

def path_json(sel,entry,n=20):
    a=[]
    for i,r in enumerate(sel[:n],1):
        a.append({
            "m":i,"t":kst(r["ts"]),
            "o":round(pct(r["open"],entry),4),
            "h":round(pct(r["high"],entry),4),
            "l":round(pct(r["low"],entry),4),
            "c":round(pct(r["close"],entry),4),
            "v":r["volume"],
        })
    return json.dumps(a,ensure_ascii=False,separators=(",",":"))

def event_metrics(sel,start,entry):
    hi=max((r["high"] for r in sel),default=entry)
    lo=min((r["low"] for r in sel),default=entry)
    out={"mfe_180_pct":pct(hi,entry),"mae_180_pct":pct(lo,entry)}
    for n in (1,3,5,10,15,30,60,90,120,180):
        out.update(window(sel,entry,n))
    for up in (0.25,0.5,0.75,1.0,1.25,1.5,2.0,3.0):
        i,r=first_hit(sel,lambda z,u=up:pct(z["high"],entry)>=u)
        out[f"mins_to_up_{str(up).replace('.','p')}"]=i if r else ""
    for dn in (-0.5,-1,-1.5,-2,-2.5,-3,-3.5,-4,-4.5,-5):
        i,r=first_hit(sel,lambda z,d=dn:pct(z["low"],entry)<=d)
        out[f"mins_to_down_{str(abs(dn)).replace('.','p')}"]=i if r else ""
    i2,r2=first_hit(sel,lambda z:pct(z["high"],entry)>=3.0)
    out["tp2_recovered"]=bool(r2)
    out["mins_to_tp2"]=i2 if r2 else ""
    if r2:
        prior=sel[:i2]  # TP2 hitbar 제외
        out["pre_tp2_low_pct"]=pct(min((z["low"] for z in prior),default=entry),entry)
    else:
        out["pre_tp2_low_pct"]=""
    out["path20_json"]=path_json(sel,entry,20)
    return out

def simulate(sel,entry,target,stop,timeout):
    # 다음 완성 1분봉부터 시작. 같은 1분봉에 target/stop 동시 발생 시 보수적으로 stop 우선.
    lim=min(timeout,len(sel))
    if lim<=0:
        return {"exit_reason":"NO_DATA","exit_pct":0.10,"hold_min":0}
    for i,r in enumerate(sel[:lim],1):
        hit_stop=pct(r["low"],entry)<=stop
        hit_target=pct(r["high"],entry)>=target
        if hit_stop:
            ep=stop
            return {"exit_reason":"STOP","exit_pct":ep,"hold_min":i}
        if hit_target:
            ep=target
            return {"exit_reason":"TARGET","exit_pct":ep,"hold_min":i}
    ep=pct(sel[lim-1]["close"],entry)
    return {"exit_reason":"TIME","exit_pct":ep,"hold_min":lim}

def gross_from_exit(exit_pct):
    return LOCKED_BE_PARTIAL_GROSS + 0.25*exit_pct

def split_labels(rows):
    # chronological 60/20/20 split. Last 20% is untouched holdout.
    n=len(rows)
    d=max(1,int(n*0.60))
    v=max(d+1,int(n*0.80))
    for i,r in enumerate(rows):
        r["split"]="DISCOVERY" if i<d else ("VALIDATION" if i<v else "HOLDOUT")

def summarize_results(simrows):
    if not simrows:
        return {}
    gs=[r["gross"] for r in simrows]
    holds=[r["hold_min"] for r in simrows]
    return {
        "n":len(simrows),
        "sum_gross":sum(gs),
        "mean_gross":sum(gs)/len(gs),
        "delta_vs_baseline_sum":sum(g-BASELINE_GROSS for g in gs),
        "better_than_baseline":sum(g>BASELINE_GROSS+1e-12 for g in gs),
        "equal_baseline":sum(abs(g-BASELINE_GROSS)<=1e-12 for g in gs),
        "worse_than_baseline":sum(g<BASELINE_GROSS-1e-12 for g in gs),
        "negative_trade_count":sum(g<0 for g in gs),
        "median_hold_min":statistics.median(holds),
        "mean_hold_min":sum(holds)/len(holds),
        "target_count":sum(r["exit_reason"]=="TARGET" for r in simrows),
        "stop_count":sum(r["exit_reason"]=="STOP" for r in simrows),
        "time_count":sum(r["exit_reason"]=="TIME" for r in simrows),
        "tp2_path_but_not_target":sum(bool(r["tp2_recovered"]) and r["exit_reason"]!="TARGET" for r in simrows),
    }

def main():
    inp=latest_master()
    with inp.open("r",encoding="utf-8-sig",newline="") as fh:
        raw=list(csv.DictReader(fh))
    cohort=[r for r in raw if b(r.get("has_v25_anchor")) and b(r.get("has_v271_exact_pair")) and str(r.get("v271_result"))=="BE_EXIT"]
    cohort.sort(key=lambda r:dt(r.get("v271_result_ts")) or datetime.min.replace(tzinfo=timezone.utc))
    split_labels(cohort)

    print("INPUT =",inp)
    print("EXACT V25+V271 BE =",len(cohort))

    events=[]
    api_err=0
    for i,r in enumerate(cohort,1):
        anchor=dt(r.get("v271_result_ts"))
        entry=f(r.get("entry_price"))
        symbol=r.get("symbol")
        rec={
            "event_no":i,"cluster_id":r.get("cluster_id"),"symbol":symbol,
            "opened_at":r.get("opened_at"),"entry_price":entry,
            "be_time_utc":iso(anchor),"be_time_kst":kst(anchor),
            "be_price":f(r.get("v271_result_price")),
            "split":r.get("split"),"anchor_variant":r.get("anchor_variant"),
            "variants":r.get("variants"),"baseline_gross":BASELINE_GROSS,
        }
        try:
            floor=anchor.replace(second=0,microsecond=0)
            bars=klines(symbol,floor-timedelta(minutes=1),floor+timedelta(minutes=RECOVERY_MINUTES+2))
            sel,start=full_post_bars(bars,anchor)
            if not sel: raise RuntimeError("no post-BE full-minute bars")
            rec.update(event_metrics(sel,start,entry))
            rec["_sel"]=sel
            rec["api_error"]=""
            time.sleep(0.08)
        except Exception as e:
            api_err+=1
            rec["api_error"]=f"{type(e).__name__}: {e}"
            rec["_sel"]=[]
        events.append(rec)
        print(f"{i}/{len(cohort)} {symbol} split={r.get('split')} err={bool(rec['api_error'])}")

    ok=[r for r in events if not r.get("api_error")]
    print("API OK =",len(ok),"ERRORS =",api_err)

    grid=[]
    detail=[]
    for target in TARGETS:
        for stop in STOPS:
            for timeout in TIMEOUTS:
                per=[]
                for e in ok:
                    s=simulate(e["_sel"],e["entry_price"],target,stop,timeout)
                    g=gross_from_exit(s["exit_pct"])
                    rr={
                        "event_no":e["event_no"],"cluster_id":e["cluster_id"],"symbol":e["symbol"],
                        "split":e["split"],"target_pct":target,"stop_pct":stop,"timeout_min":timeout,
                        "exit_reason":s["exit_reason"],"exit_pct":s["exit_pct"],"hold_min":s["hold_min"],
                        "gross":g,"delta_vs_baseline":g-BASELINE_GROSS,
                        "tp2_recovered":e.get("tp2_recovered",False),
                    }
                    per.append(rr)
                row={"target_pct":target,"stop_pct":stop,"timeout_min":timeout}
                for split in ("ALL","DISCOVERY","VALIDATION","HOLDOUT"):
                    sub=per if split=="ALL" else [x for x in per if x["split"]==split]
                    sm=summarize_results(sub)
                    for k,v in sm.items(): row[f"{split.lower()}_{k}"]=v
                grid.append(row)

    # discovery only로 후보 선택. validation/holdout은 선택에 사용하지 않음.
    ranked=sorted(
        grid,
        key=lambda r:(
            f(r.get("discovery_delta_vs_baseline_sum"),-999999),
            -f(r.get("discovery_negative_trade_count"),9999),
            -f(r.get("discovery_mean_hold_min"),9999)
        ),
        reverse=True
    )
    top=ranked[:25]

    # top 25 세부 이벤트만 저장
    topkeys={(x["target_pct"],x["stop_pct"],x["timeout_min"]) for x in top}
    for target,stop,timeout in topkeys:
        for e in ok:
            s=simulate(e["_sel"],e["entry_price"],target,stop,timeout)
            g=gross_from_exit(s["exit_pct"])
            detail.append({
                "event_no":e["event_no"],"cluster_id":e["cluster_id"],"symbol":e["symbol"],"split":e["split"],
                "target_pct":target,"stop_pct":stop,"timeout_min":timeout,
                "exit_reason":s["exit_reason"],"exit_pct":s["exit_pct"],"hold_min":s["hold_min"],
                "gross":g,"delta_vs_baseline":g-BASELINE_GROSS,
                "tp2_recovered":e.get("tp2_recovered",False),
                "be_time_kst":e["be_time_kst"],
            })

    stamp=datetime.now(KST).strftime("%Y%m%d_%H%M_KST")
    outdir=Path.cwd()/f"V25_V271_BE46_RECOVERY_{stamp}"
    outdir.mkdir(exist_ok=True)

    # remove non-serializable private bars before CSV
    event_rows=[]
    for r in events:
        x={k:v for k,v in r.items() if k!="_sel"}
        event_rows.append(x)

    def write(path,rows):
        fields=[]
        seen=set()
        for r in rows:
            for k in r:
                if k not in seen: seen.add(k); fields.append(k)
        with path.open("w",encoding="utf-8-sig",newline="") as fh:
            w=csv.DictWriter(fh,fieldnames=fields); w.writeheader(); w.writerows(rows)

    p_events=outdir/f"BE46_EVENTS_{stamp}.csv"
    p_grid=outdir/f"BE46_GRID_{stamp}.csv"
    p_top=outdir/f"BE46_TOP25_{stamp}.csv"
    p_detail=outdir/f"BE46_TOP25_DETAIL_{stamp}.csv"
    write(p_events,event_rows); write(p_grid,grid); write(p_top,top); write(p_detail,detail)

    p_sum=outdir/f"BE46_SUMMARY_{stamp}.txt"
    with p_sum.open("w",encoding="utf-8") as fh:
        fh.write(f"INPUT={inp}\n")
        fh.write(f"COHORT={len(cohort)}\nAPI_OK={len(ok)}\nAPI_ERRORS={api_err}\n")
        fh.write(f"BASELINE_GROSS_PER_BE={BASELINE_GROSS:.4f}\n")
        fh.write("SPLIT_COUNTS="+json.dumps({s:sum(r.get('split')==s for r in ok) for s in ('DISCOVERY','VALIDATION','HOLDOUT')})+"\n")
        fh.write("CLASSIFICATION_TP2_RECOVERED="+str(sum(bool(r.get("tp2_recovered")) for r in ok))+"\n")
        fh.write("\n=== TOP 10 BY DISCOVERY ONLY ===\n")
        for n,r in enumerate(top[:10],1):
            fh.write(
                f"{n}. T={r['target_pct']} S={r['stop_pct']} TIME={r['timeout_min']} | "
                f"D_delta={r.get('discovery_delta_vs_baseline_sum',0):+.4f} "
                f"V_delta={r.get('validation_delta_vs_baseline_sum',0):+.4f} "
                f"H_delta={r.get('holdout_delta_vs_baseline_sum',0):+.4f} "
                f"ALL_delta={r.get('all_delta_vs_baseline_sum',0):+.4f} "
                f"H_neg={r.get('holdout_negative_trade_count',0)} "
                f"H_hold={r.get('holdout_mean_hold_min',0):.1f}\n"
            )

    zp=Path.cwd()/f"V25_V271_BE46_RECOVERY_{stamp}.zip"
    with zipfile.ZipFile(zp,"w",zipfile.ZIP_DEFLATED) as z:
        for p in (p_events,p_grid,p_top,p_detail,p_sum):
            z.write(p,p.name)

    print("\n=== COMPLETE ===")
    print("COHORT =",len(cohort))
    print("API OK =",len(ok),"ERRORS =",api_err)
    print("TP2 RECOVERED WITHIN 180M =",sum(bool(r.get("tp2_recovered")) for r in ok))
    print("SPLITS =",{s:sum(r.get('split')==s for r in ok) for s in ('DISCOVERY','VALIDATION','HOLDOUT')})
    print("\n=== TOP 10 DISCOVERY-SELECTED ===")
    for n,r in enumerate(top[:10],1):
        print(
            n, "T",r["target_pct"],"S",r["stop_pct"],"TIME",r["timeout_min"],
            "| D",round(f(r.get("discovery_delta_vs_baseline_sum"),0),4),
            "V",round(f(r.get("validation_delta_vs_baseline_sum"),0),4),
            "H",round(f(r.get("holdout_delta_vs_baseline_sum"),0),4),
            "ALL",round(f(r.get("all_delta_vs_baseline_sum"),0),4),
        )
    print("ZIP =",zp)

if __name__=="__main__":
    main()
