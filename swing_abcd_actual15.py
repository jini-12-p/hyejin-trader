#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S형 A~D actual 15m comparison using the cache already downloaded by
swing_actual15_recheck.py.

A: 1H SR lookback 18h (existing baseline)
B: 1H SR lookback 36h
C: 1H SR lookback 72h
D: 4H major SR (18 completed 4H bars = 72h) + 1H refinement (36h) + 15m rebound

Common:
- Entry dates 2026-09-01~09-28 KST
- Max 4 simultaneous positions
- 27 USDT margin x 5 leverage = 135 USDT notional/slot
- No averaging down
- Max hold 24h, 2h symbol cooldown after exit
- Same-bar STOP and target => STOP first (conservative)
- Fee scenario: 0.055% each side

This is research/backtest only. It does not place orders.
"""
from __future__ import annotations
import os, sys, zipfile
from pathlib import Path
import pandas as pd
import numpy as np

# The original script must be in /root/hyejin-trader (same directory on VPS).
try:
    import swing_actual15_recheck as base
except Exception as e:
    raise SystemExit(f"Cannot import swing_actual15_recheck.py: {e}")

OUT = Path("/root/hyejin-trader/swing_abcd_actual15")
OUT.mkdir(parents=True, exist_ok=True)

# ---------- Helpers ----------
def load_cached_bars():
    status_path = base.OUT / "S_ACTUAL15_FETCH_STATUS.csv"
    if not status_path.exists():
        raise FileNotFoundError(f"Missing {status_path}. Run swing_actual15_recheck.py once first.")
    status = pd.read_csv(status_path).fillna("").to_dict("records")
    b15 = base.load_bars(status)
    if len(b15) < 5:
        raise RuntimeError("Too few cached symbols")
    return b15


def max_loss_streak(T: pd.DataFrame) -> int:
    if T.empty: return 0
    z=T.sort_values(["time","symbol"])
    best=cur=0
    for v in z.ret_pct:
        if v < 0:
            cur += 1; best=max(best,cur)
        else:
            cur = 0
    return best


def add_metrics(name, T):
    s,d = base.summarize(T)
    r=s.iloc[0].to_dict()
    r["variant"] = name
    r["max_loss_streak"] = max_loss_streak(T)
    r["stop_count"] = int((T.exit_reason=="STOP").sum()) if len(T) else 0
    r["target_count"] = int((T.exit_reason=="R1").sum()) if len(T) else 0
    r["time_count"] = int((T.exit_reason=="TIME").sum()) if len(T) else 0
    r["end_count"] = int((T.exit_reason=="END").sum()) if len(T) else 0
    if len(T):
        r["avg_target_dist_pct"] = float(((T.r1/T.entry-1)*100).mean())
        r["median_target_dist_pct"] = float(((T.r1/T.entry-1)*100).median())
    else:
        r["avg_target_dist_pct"] = np.nan
        r["median_target_dist_pct"] = np.nan
    # daily positive / negative days based on fee scenario
    if len(d):
        r["positive_days_fee"] = int((d.net_fee_scenario_usdt>0).sum())
        r["negative_days_fee"] = int((d.net_fee_scenario_usdt<0).sum())
        r["worst_day_fee_usdt"] = float(d.net_fee_scenario_usdt.min())
        r["best_day_fee_usdt"] = float(d.net_fee_scenario_usdt.max())
    else:
        r["positive_days_fee"] = r["negative_days_fee"] = 0
        r["worst_day_fee_usdt"] = r["best_day_fee_usdt"] = np.nan
    return r,d


# ---------- D: 4H major SR + 1H refine ----------
def cluster(vals, tol):
    arr=np.sort(np.asarray(vals,float))
    if len(arr)==0:return []
    groups=[]
    for v in arr:
        if groups and abs(v-np.median(groups[-1]))/np.median(groups[-1])<=tol:
            groups[-1].append(v)
        else:
            groups.append([v])
    return [(float(np.median(g)),len(g)) for g in groups if len(g)>=2]


def make_states_D(b1,b4, q_lookback=18, h_lookback=36):
    """For each hour, find major 4H support/resistance then refine support with 1H lows."""
    states={}
    for sym,h in b1.items():
        q=b4.get(sym)
        if q is None or len(q)<q_lookback:
            states[sym]={}; continue
        d={}
        # each 1H completion point
        for k in range(1,len(h)+1):
            hour=(h.index[k-1]+pd.Timedelta(hours=1)).floor("h")
            # completed 4H bars only
            qc=q[(q.index+pd.Timedelta(hours=4))<=hour]
            if len(qc)<q_lookback:
                continue
            qhist=qc.tail(q_lookback)
            if (qhist.n>=16).mean()<.5:
                continue
            # enough fully/mostly formed 1H context as well
            hc=h[(h.index+pd.Timedelta(hours=1))<=hour]
            if len(hc)<h_lookback:
                continue
            hhist=hc.tail(h_lookback)
            if (hhist.n>=4).mean()<.5:
                continue
            ref=float(hhist.close.iloc[-1])

            # Wider zone tolerance on 4H, same narrow tolerance on 1H.
            qs=cluster(qhist.low.values, .010)
            qr=cluster(qhist.high.values, .010)
            if not qs or not qr:
                continue
            # closest major support not materially above current price
            qs=sorted([(lv,n) for lv,n in qs if lv<=ref*1.015], reverse=True)
            qr=sorted([(lv,n) for lv,n in qr if lv>=ref*1.005])
            if not qs or not qr:
                continue
            major_s, ns4 = qs[0]
            target, nr4 = qr[0]
            if target <= major_s:
                continue

            # 1H detailed support must sit close to the major 4H support zone.
            hs=cluster(hhist.low.values, .006)
            near=[(lv,n) for lv,n in hs if abs(lv-major_s)/major_s <= .012]
            if not near:
                continue
            fine_s, ns1=min(near, key=lambda x:abs(x[0]-major_s))

            # stop is below the MAJOR 4H support; entry trigger uses refined 1H support.
            stop=major_s*.994
            # skip nonsensical states where refined support is already below stop
            if fine_s <= stop:
                continue

            # same simple 4H 3-bar downtrend guard as A/B/C.
            trend_ok=True
            if len(qc)>=3:
                z=qc.tail(3)
                down=(z.close.iloc[2]<z.close.iloc[1]<z.close.iloc[0] and
                      z.high.iloc[2]<z.high.iloc[1]<z.high.iloc[0] and
                      z.low.iloc[2]<z.low.iloc[1]<z.low.iloc[0])
                trend_ok=not down
            if trend_ok:
                d[hour]={
                    "fine_s":float(fine_s),"major_s":float(major_s),"stop":float(stop),
                    "r1":float(target),"n_s1":int(ns1),"n_s4":int(ns4),"n_r4":int(nr4)
                }
        states[sym]=d
    return states


def make_signals_D(b15, states):
    out=[]
    for sym,h in b15.items():
        st=states.get(sym,{})
        if not st: continue
        prev=None
        hh=h[(h.index>=base.FETCH_START)&(h.index<=base.ENTRY_END)]
        for t,row in hh.iterrows():
            if prev is None:
                prev=row; continue
            hour=t.floor("h")
            ss=st.get(hour)
            if ss is None:
                prev=row; continue
            s1=ss["fine_s"]; stop=ss["stop"]; r1=ss["r1"]
            cl,lo,op=float(row.close),float(row.low),float(row.open)
            # same 15m rebound trigger used in A/B/C, but around refined 1H support.
            if not (lo<=s1*1.004 and cl>=s1*.998 and cl>op and cl>float(prev.close)):
                prev=row; continue
            reward=(r1-cl)/cl
            risk=(cl-stop)/cl
            if reward>=.01 and risk>0 and reward/risk>=1.15:
                quality=reward/risk + .1*(ss["n_s1"]+ss["n_s4"]+ss["n_r4"]-6)
                out.append((t,sym,cl,s1,ss["major_s"],stop,r1,reward/risk,quality))
            prev=row
    return pd.DataFrame(out,columns=["time","symbol","entry","s1","major_s","stop","r1","rr","quality"])


def simulate_D(b15,sig):
    sig=sig[(sig.time>=base.ENTRY_START)&(sig.time<=base.ENTRY_END)].copy()
    by={t:g.sort_values("quality",ascending=False) for t,g in sig.groupby("time")}
    times=pd.date_range(base.FETCH_START,base.FETCH_END,freq="15min")
    pos={}; cool={}; tr=[]
    for t in times:
        for sym,p in list(pos.items()):
            h=b15.get(sym)
            if h is None or t not in h.index: continue
            r=h.loc[t]; lo,hi,cl=float(r.low),float(r.high),float(r.close)
            reason=None; px=None
            if lo<=p["stop"]:
                reason="STOP"; px=p["stop"]
            elif hi>=p["r1"]:
                reason="R1"; px=p["r1"]
            elif t-p["time"]>=pd.Timedelta(hours=24):
                reason="TIME"; px=cl
            if reason:
                ret=px/p["entry"]-1
                tr.append({**p,"exit_time":t,"exit_price":px,"exit_reason":reason,"ret_pct":ret*100})
                del pos[sym]; cool[sym]=t+pd.Timedelta(hours=2)
        if t>base.ENTRY_END: continue
        if t in by and len(pos)<base.MAX_POSITIONS:
            for _,s in by[t].iterrows():
                sym=s.symbol
                if len(pos)>=base.MAX_POSITIONS: break
                if sym in pos or (sym in cool and t<cool[sym]): continue
                pos[sym]={k:(float(s[k]) if k not in ("symbol","time") else s[k]) for k in ["symbol","time","entry","s1","major_s","stop","r1","rr","quality"]}
    for sym,p in pos.items():
        h=b15[sym]; hh=h[h.index<=base.FETCH_END]
        if len(hh)==0: continue
        t=hh.index[-1]; px=float(hh.close.iloc[-1]); ret=px/p["entry"]-1
        tr.append({**p,"exit_time":t,"exit_price":px,"exit_reason":"END","ret_pct":ret*100})
    return pd.DataFrame(tr)


def main():
    print("[1/6] Loading existing actual 15m cache (NO re-download)...", flush=True)
    b15=load_cached_bars()
    print(f"  cached symbols={len(b15)}", flush=True)
    print("[2/6] Building 1H / 4H bars...", flush=True)
    b1,b4=base.build_higher(b15)

    variants=[]; daily_all=[]; trade_files=[]; signal_files=[]

    for name,lookback in [("A_1H18",18),("B_1H36",36),("C_1H72",72)]:
        print(f"[3/6] {name}: states/signals/simulation", flush=True)
        states=base.make_sr_hourly(b1,b4,lookback=lookback)
        sig=base.make_signals(b15,states)
        T=base.simulate(b15,sig)
        r,d=add_metrics(name,T)
        variants.append(r)
        if len(d): d.insert(0,"variant",name); daily_all.append(d)
        tp=OUT/f"{name}_TRADES.csv"; sp=OUT/f"{name}_SIGNALS.csv"
        T.to_csv(tp,index=False); sig.to_csv(sp,index=False)
        trade_files.append(tp); signal_files.append(sp)

    print("[4/6] D_4H1H: states/signals/simulation", flush=True)
    statesD=make_states_D(b1,b4)
    sigD=make_signals_D(b15,statesD)
    TD=simulate_D(b15,sigD)
    r,d=add_metrics("D_4H1H",TD)
    variants.append(r)
    if len(d): d.insert(0,"variant","D_4H1H"); daily_all.append(d)
    tp=OUT/"D_4H1H_TRADES.csv"; sp=OUT/"D_4H1H_SIGNALS.csv"
    TD.to_csv(tp,index=False); sigD.to_csv(sp,index=False)
    trade_files.append(tp); signal_files.append(sp)

    print("[5/6] Writing comparison...", flush=True)
    S=pd.DataFrame(variants)
    # friendly column order
    cols=["variant","trades","wins","losses","win_rate_pct","avg_win_pct","avg_loss_pct",
          "profit_factor_ret","gross_usdt","net_fee_scenario_usdt","ending_equity_from_200_fee_scenario",
          "max_closed_equity_dd_usdt_fee_scenario","max_loss_streak","stop_count","target_count","time_count",
          "avg_target_dist_pct","median_target_dist_pct","positive_days_fee","negative_days_fee",
          "worst_day_fee_usdt","best_day_fee_usdt","sum_ret_pct","best_trade_pct","worst_trade_pct","avg_hold_hours"]
    cols=[c for c in cols if c in S.columns]
    S=S[cols]
    summary_path=OUT/"S_ABCD_ACTUAL15_SUMMARY.csv"
    daily_path=OUT/"S_ABCD_ACTUAL15_DAILY.csv"
    S.to_csv(summary_path,index=False)
    DLY=pd.concat(daily_all,ignore_index=True) if daily_all else pd.DataFrame()
    DLY.to_csv(daily_path,index=False)

    readme=OUT/"README_S_ABCD_ACTUAL15.txt"
    readme.write_text(
        "S형 A~D actual 15m comparison using previously cached Bybit data.\n"
        "A=1H18h, B=1H36h, C=1H72h, D=4H major SR(72h)+1H refine(36h)+15m rebound.\n"
        "Common: 4 slots, 27 USDT x5, no averaging, 24h max hold, 2h cooldown, same-bar STOP first.\n"
        "Fee scenario=0.055% per side.\n"
        "Note: cache starts 2026-08-30 KST, so full 72h warmup means C/D naturally begin later than A/B at the very start of Sep.\n",
        encoding="utf-8")

    zip_path=OUT/"S_ABCD_ACTUAL15_RESULTS.zip"
    with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as z:
        for p in [summary_path,daily_path,readme,*trade_files,*signal_files]:
            z.write(p,arcname=p.name)
    print("[6/6] DONE", flush=True)
    print(S.to_string(index=False), flush=True)
    print(f"RESULT_ZIP={zip_path}", flush=True)

if __name__=="__main__":
    main()
