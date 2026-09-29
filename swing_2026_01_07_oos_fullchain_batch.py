#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
2026년 1~7월 S형 월별 OOS 배치검증
- 8월 OOS 작업이 아직 실행 중이면 자동으로 기다렸다가 시작
- 터미널이 끊겨도 nohup으로 실행 가능
- 각 월별 실제 Bybit 15m 다운로드/캐시 + 4단계 비교
- 최종 ZIP은 /root/hyejin-trader/bybit_swing/ 에 바로 생성

월별 비교
F0_C72_RAW
  C형: 1H 72시간 지지/저항 + 기존 종목별 4H 3봉 연속하락 가드
F1_BTC_GAP050
  F0 + BTC 4H weak & EMA20 gap <= -0.50% 신규진입 차단
F2_Q2_RECLAIM010
  F1 + S1 대비 +0.10% 이상 회복 확인
F3_Q2_RR_BLOCK
  F2 + 1.5 <= RR < 2.0 진입 차단

공통
- 최대 4슬롯
- 슬롯당 27 USDT, 5x
- TP=R1
- STOP=S1*0.994
- 최대보유 24h
- 종료 후 2h cooldown
- 동일 15m봉 TP/STOP 동시 터치시 STOP 우선
- 수수료 시나리오 0.055%/side
- 실제 주문 없음

주의
- 9월 연구에서 정한 숫자는 1~7월에서 변경하지 않음.
- 종목 풀은 9월 검증과 동일한 SYMBOLS를 사용하며, 해당 월에 실제 캔들이 없는 종목은 자동 제외.
"""

from __future__ import annotations

import os
import time
import zipfile
import subprocess
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

try:
    import swing_actual15_recheck as sepbase
except Exception as e:
    raise SystemExit(f"Cannot import swing_actual15_recheck.py: {e}")

ROOT = Path("/root/hyejin-trader")
BATCH = ROOT / "swing_2026_01_07_batch"
FINAL = ROOT / "bybit_swing"
BATCH.mkdir(parents=True, exist_ok=True)
FINAL.mkdir(parents=True, exist_ok=True)

SYMBOLS = list(sepbase.SYMBOLS)
MAX_POSITIONS = 4
MARGIN_USDT = 27.0
LEVERAGE = 5.0
NOTIONAL = MARGIN_USDT * LEVERAGE
FEE_EACH_SIDE = 0.00055
WORKERS = int(os.environ.get("S_SWING_WORKERS", "4"))
API = "https://api.bybit.com/v5/market/kline"

MONTHS = list(range(1, 8))


def wait_for_august():
    """현재 실행 중인 8월 OOS가 있으면 CPU 경합 방지를 위해 종료될 때까지 대기."""
    me = os.getpid()
    print("[WAIT] 8월 OOS 작업 확인", flush=True)
    while True:
        try:
            p = subprocess.run(
                ["pgrep", "-f", "swing_aug_oos_q2_rr_actual15.py"],
                capture_output=True, text=True
            )
            pids = []
            for s in p.stdout.split():
                try:
                    pid = int(s)
                    if pid != me:
                        pids.append(pid)
                except Exception:
                    pass
            if not pids:
                print("[WAIT] 8월 작업 없음/완료 → 1~7월 시작", flush=True)
                return
            print(f"[WAIT] 8월 작업 진행중 PID={pids}. 60초 후 재확인", flush=True)
        except Exception as e:
            print(f"[WAIT] 확인 실패({e}) → 120초 후 재확인", flush=True)
        time.sleep(60)


def month_bounds(year, month):
    start = pd.Timestamp(year=year, month=month, day=1)
    if month == 12:
        nxt = pd.Timestamp(year=year+1, month=1, day=1)
    else:
        nxt = pd.Timestamp(year=year, month=month+1, day=1)
    end = nxt - pd.Timedelta(seconds=1)
    fetch_start = start - pd.Timedelta(days=6)
    fetch_end = nxt + pd.Timedelta(days=2)
    return start, end, fetch_start, fetch_end


def utc_ms_from_kst_naive(ts):
    return int(ts.tz_localize("Asia/Seoul").tz_convert("UTC").timestamp() * 1000)


def _request(params, tries=7):
    last = None
    for i in range(tries):
        try:
            r = requests.get(API, params=params, timeout=20)
            if r.status_code == 200:
                j = r.json()
                if int(j.get("retCode", -1)) == 0:
                    return j
                last = RuntimeError(f"retCode={j.get('retCode')} retMsg={j.get('retMsg')}")
            else:
                last = RuntimeError(f"HTTP {r.status_code} {r.text[:180]}")
        except Exception as e:
            last = e
        time.sleep(min(0.6 * (2 ** i), 8))
    raise last or RuntimeError("request failed")


def cluster_levels(vals, tol=.006):
    arr = np.sort(np.asarray(vals, float))
    if len(arr) == 0:
        return []
    groups = []
    for v in arr:
        if groups and abs(v - np.median(groups[-1])) / np.median(groups[-1]) <= tol:
            groups[-1].append(v)
        else:
            groups.append([v])
    return [(float(np.median(g)), len(g)) for g in groups if len(g) >= 2]


def fetch_month(year, month, out, cache, fetch_start, fetch_end):
    start_ms = utc_ms_from_kst_naive(fetch_start)
    end_ms = utc_ms_from_kst_naive(fetch_end)
    tag = f"{fetch_start:%Y%m%d}_{fetch_end:%Y%m%d}"

    def cpath(sym):
        return cache / f"{sym}_15m_{tag}.csv.gz"

    def one(sym):
        fp = cpath(sym)
        if fp.exists() and fp.stat().st_size > 200:
            try:
                d = pd.read_csv(fp, compression="gzip", parse_dates=["time"])
                if len(d) > 100:
                    return sym, len(d), "CACHE", ""
            except Exception:
                pass

        rows = []
        cursor_end = end_ms
        seen_min = None
        while cursor_end >= start_ms:
            params = {
                "category":"linear", "symbol":sym, "interval":"15",
                "limit":1000, "end":cursor_end
            }
            try:
                j = _request(params)
            except Exception as e:
                return sym, 0, "ERROR", repr(e)
            arr = (j.get("result") or {}).get("list") or []
            if not arr:
                break

            mins = []
            for x in arr:
                try:
                    ms = int(x[0]); mins.append(ms)
                    if start_ms <= ms <= end_ms:
                        rows.append((
                            ms, float(x[1]), float(x[2]), float(x[3]),
                            float(x[4]), float(x[5]), float(x[6])
                        ))
                except Exception:
                    continue
            if not mins:
                break
            mn = min(mins)
            if seen_min is not None and mn >= seen_min:
                break
            seen_min = mn
            if mn <= start_ms:
                break
            cursor_end = mn - 1
            time.sleep(0.04)

        if not rows:
            return sym, 0, "NO_DATA", ""

        d = pd.DataFrame(rows, columns=["ms","open","high","low","close","volume","turnover"])
        d = d.drop_duplicates("ms").sort_values("ms")
        t = pd.to_datetime(d.pop("ms"), unit="ms", utc=True).dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
        d.insert(0, "time", t)
        d = d[(d.time >= fetch_start) & (d.time <= fetch_end)]
        d.to_csv(fp, index=False, compression="gzip")
        return sym, len(d), "FETCH", ""

    requested = list(dict.fromkeys(SYMBOLS + ["BTCUSDT"]))
    status = []
    print(f"  [1/7] 실제 15m 다운로드/캐시: {len(requested)} symbols, workers={WORKERS}", flush=True)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(one, s): s for s in requested}
        done = 0
        for fut in as_completed(futs):
            done += 1
            sym = futs[fut]
            try:
                s, n, mode, err = fut.result()
            except Exception as e:
                s, n, mode, err = sym, 0, "ERROR", repr(e)
            status.append({"symbol":s,"bars":n,"mode":mode,"error":err})
            if done % 25 == 0 or done == len(requested):
                valid = sum(1 for x in status if x["bars"] >= 100)
                print(f"    {done}/{len(requested)} · 유효 {valid}", flush=True)

    pd.DataFrame(status).to_csv(out/"FETCH_STATUS.csv", index=False)
    return status, cpath


def load_month_bars(status, cpath):
    b15 = {}
    btc = None
    for x in status:
        if int(x["bars"]) < 100:
            continue
        fp = cpath(x["symbol"])
        try:
            d = pd.read_csv(fp, compression="gzip", parse_dates=["time"]).set_index("time").sort_index()
            d = d[~d.index.duplicated(keep="last")]
            d = d[["open","high","low","close","volume","turnover"]].astype(float)
            if x["symbol"] == "BTCUSDT":
                btc = d
            else:
                b15[x["symbol"]] = d
        except Exception:
            pass
    return b15, btc


def build_higher(b15):
    agg = {"open":"first","high":"max","low":"min","close":"last","volume":"sum","turnover":"sum"}
    b1, b4 = {}, {}
    for i, (sym, d) in enumerate(b15.items(), 1):
        h = d.resample("1h").agg(agg)
        h["n"] = d["close"].resample("1h").count()
        h = h.dropna(subset=["open","high","low","close"])
        q = d.resample("4h").agg(agg)
        q["n"] = d["close"].resample("4h").count()
        q = q.dropna(subset=["open","high","low","close"])
        if len(h) >= 20:
            b1[sym] = h
            b4[sym] = q
        if i % 75 == 0:
            print(f"    higher {i}/{len(b15)}", flush=True)
    return b1, b4


def make_states(b1, b4, lookback=72):
    states = {}
    total = len(b1)
    for i, (sym, h) in enumerate(b1.items(), 1):
        d = {}
        for k in range(10, len(h)+1):
            hist = h.iloc[max(0, k-lookback):k]
            hour = (h.index[k-1] + pd.Timedelta(hours=1)).floor("h")
            if len(hist) < 10 or (hist.n >= 4).mean() < .5:
                continue
            ref = float(hist.close.iloc[-1])
            supp = cluster_levels(hist.low.values)
            res = cluster_levels(hist.high.values)
            supp = sorted([(lv,n) for lv,n in supp if lv <= ref*1.015], reverse=True)
            res = sorted([(lv,n) for lv,n in res if lv >= ref*.985])
            if len(supp) < 2 or len(res) < 1:
                continue
            s1 = supp[0]
            s2s = [x for x in supp[1:] if x[0] <= s1[0]*.99 and x[0] >= s1[0]*.92]
            if not s2s:
                continue
            s2 = s2s[0]
            r1s = [x for x in res if x[0] >= ref*1.005]
            if not r1s:
                continue
            r1 = r1s[0]

            trend_ok = True
            q = b4.get(sym)
            if q is not None:
                qc = q[(q.index + pd.Timedelta(hours=4)) <= hour]
                if len(qc) >= 3:
                    z = qc.tail(3)
                    down = (
                        z.close.iloc[2] < z.close.iloc[1] < z.close.iloc[0]
                        and z.high.iloc[2] < z.high.iloc[1] < z.high.iloc[0]
                        and z.low.iloc[2] < z.low.iloc[1] < z.low.iloc[0]
                    )
                    trend_ok = not down
            if trend_ok:
                d[hour] = (s1[0], s2[0], r1[0], s1[1], s2[1], r1[1])
        states[sym] = d
        if i % 50 == 0 or i == total:
            print(f"    C72 states {i}/{total}", flush=True)
    return states


def make_signals(b15, states, fetch_start, entry_end):
    out = []
    total = len(b15)
    for i, (sym, h) in enumerate(b15.items(), 1):
        st = states.get(sym, {})
        if not st:
            continue
        prev = None
        hh = h[(h.index >= fetch_start) & (h.index <= entry_end)]
        for t, row in hh.iterrows():
            if prev is None:
                prev = row
                continue
            hour = t.floor("h")
            if hour not in st:
                prev = row
                continue
            s1, s2, r1, nt1, nt2, ntr = st[hour]
            cl, lo, op = float(row.close), float(row.low), float(row.open)
            if not (lo <= s1*1.004 and cl >= s1*.998 and cl > op and cl > float(prev.close)):
                prev = row
                continue
            reward = (r1-cl)/cl
            risk_filter_stop = s2*.994
            risk = (cl-risk_filter_stop)/cl
            if reward >= .01 and risk > 0 and reward/risk >= 1.15:
                quality = reward/risk + .1*(nt1+ntr-4)
                out.append((t,sym,cl,s1,s2,r1,reward/risk,quality))
            prev = row
        if i % 75 == 0 or i == total:
            print(f"    signals {i}/{total}", flush=True)
    return pd.DataFrame(out, columns=["time","symbol","entry","s1","s2","r1","rr","quality"])


def btc_features(btc15):
    q = btc15.resample("4h").agg({
        "open":"first","high":"max","low":"min","close":"last","volume":"sum","turnover":"sum"
    })
    q["n"] = btc15["close"].resample("4h").count()
    q = q.dropna(subset=["close"])
    q["ema20"] = q["close"].ewm(span=20, adjust=False, min_periods=20).mean()
    q["ema20_prev"] = q["ema20"].shift(1)
    q["btc_gap_pct"] = (q["close"]/q["ema20"]-1)*100
    q["btc_weak"] = (q["n"]>=16) & (q["close"]<q["ema20"]) & (q["ema20"]<q["ema20_prev"])
    q.index = q.index + pd.Timedelta(hours=4)
    return q[["btc_gap_pct","btc_weak"]].sort_index()


def attach_btc(sig, feat):
    if sig.empty:
        x = sig.copy()
        x["btc_gap_pct"] = np.nan
        x["btc_weak"] = False
        return x
    left = sig.copy().reset_index(drop=True)
    left["_order"] = np.arange(len(left))
    right = feat.reset_index().rename(columns={feat.reset_index().columns[0]:"time"})
    m = pd.merge_asof(left.sort_values("time"), right.sort_values("time"), on="time", direction="backward")
    return m.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)


def simulate(b15, sig, entry_start, entry_end, fetch_end):
    sig = sig[(sig.time>=entry_start)&(sig.time<=entry_end)].copy()
    by = {t:g.sort_values("quality",ascending=False) for t,g in sig.groupby("time")}
    times = pd.date_range(entry_start, fetch_end, freq="15min")
    pos, cool, tr = {}, {}, []

    for t in times:
        for sym,p in list(pos.items()):
            h = b15.get(sym)
            if h is None or t not in h.index:
                continue
            r = h.loc[t]
            lo,hi,cl = float(r.low),float(r.high),float(r.close)
            stop,target = p["s1"]*.994,p["r1"]
            reason=px=None
            if lo<=stop:
                reason,px="STOP",stop
            elif hi>=target:
                reason,px="R1",target
            elif t-p["time"]>=pd.Timedelta(hours=24):
                reason,px="TIME",cl
            if reason:
                ret=px/p["entry"]-1
                tr.append({**p,"exit_time":t,"exit_price":px,"exit_reason":reason,"ret_pct":ret*100})
                del pos[sym]
                cool[sym]=t+pd.Timedelta(hours=2)

        if t>entry_end:
            continue
        if t in by and len(pos)<MAX_POSITIONS:
            for _,s in by[t].iterrows():
                sym=s.symbol
                if len(pos)>=MAX_POSITIONS: break
                if sym in pos or (sym in cool and t<cool[sym]): continue
                pos[sym]={"symbol":sym,"time":t,"entry":float(s.entry),"s1":float(s.s1),
                          "s2":float(s.s2),"r1":float(s.r1),"rr":float(s.rr),"quality":float(s.quality)}

    return pd.DataFrame(tr)


def max_loss_streak(T):
    cur=best=0
    for x in T.sort_values(["time","symbol"]).ret_pct if len(T) else []:
        if x<0:
            cur+=1; best=max(best,cur)
        else:
            cur=0
    return best


def summarize(T, variant, entry_start, entry_end):
    if T.empty:
        return {"variant":variant,"trades":0}, pd.DataFrame()
    T=T.sort_values(["time","symbol"]).copy()
    T["date"]=T.time.dt.date
    T["gross_usdt"]=NOTIONAL*T.ret_pct/100
    T["fee_scenario_usdt"]=NOTIONAL*(FEE_EACH_SIDE*2)
    T["net_fee_scenario_usdt"]=T.gross_usdt-T.fee_scenario_usdt
    w=T[T.ret_pct>0]; l=T[T.ret_pct<0]
    eq=200+T.net_fee_scenario_usdt.cumsum()
    dd=eq-eq.cummax()
    r={
        "variant":variant,
        "trades":len(T),
        "wins":len(w),"losses":len(l),
        "win_rate_pct":100*len(w)/len(T),
        "avg_win_pct":w.ret_pct.mean() if len(w) else np.nan,
        "avg_loss_pct":l.ret_pct.mean() if len(l) else np.nan,
        "profit_factor_ret":w.ret_pct.sum()/(-l.ret_pct.sum()) if len(l) and -l.ret_pct.sum()>0 else np.nan,
        "gross_usdt":T.gross_usdt.sum(),
        "net_fee_scenario_usdt":T.net_fee_scenario_usdt.sum(),
        "max_closed_equity_dd_usdt_fee_scenario":dd.min(),
        "max_loss_streak":max_loss_streak(T),
        "stop_count":int((T.exit_reason=="STOP").sum()),
        "target_count":int((T.exit_reason=="R1").sum()),
        "time_count":int((T.exit_reason=="TIME").sum()),
    }
    daily=T.groupby("date").agg(
        trades=("ret_pct","size"),
        gross_usdt=("gross_usdt","sum"),
        net_fee_scenario_usdt=("net_fee_scenario_usdt","sum")
    ).reset_index()
    daily.insert(0,"variant",variant)
    return r,daily


def direct_selectivity(F0_trades, sig_features, variants):
    """F0 실제 체결건 기준 각 필터가 원래 STOP/R1을 얼마나 직접 막는지."""
    if F0_trades.empty:
        return pd.DataFrame()
    key = sig_features[["time","symbol","pass_F1","pass_F2","pass_F3"]].drop_duplicates(["time","symbol"])
    m = F0_trades.merge(key,on=["time","symbol"],how="left")
    out=[]
    stop=m.exit_reason.eq("STOP")
    r1=m.exit_reason.eq("R1")
    for name,col in [("F1_BTC_GAP050","pass_F1"),("F2_Q2_RECLAIM010","pass_F2"),("F3_Q2_RR_BLOCK","pass_F3")]:
        p=m[col].fillna(False)
        sb=100*(~p & stop).sum()/stop.sum() if stop.sum() else np.nan
        rb=100*(~p & r1).sum()/r1.sum() if r1.sum() else np.nan
        out.append({
            "variant":name,
            "F0_stop_total":int(stop.sum()),
            "F0_r1_total":int(r1.sum()),
            "F0_stop_blocked_pct":sb,
            "F0_r1_blocked_pct":rb,
            "selectivity_gap_pp":sb-rb
        })
    return pd.DataFrame(out)


def run_month(year, month):
    entry_start,entry_end,fetch_start,fetch_end=month_bounds(year,month)
    tag=f"{year}{month:02d}"
    out=BATCH/tag
    cache=out/"cache15"
    out.mkdir(parents=True,exist_ok=True)
    cache.mkdir(parents=True,exist_ok=True)

    print("\n"+"="*70,flush=True)
    print(f"[MONTH {tag}] {entry_start.date()} ~ {entry_end.date()}",flush=True)
    print("="*70,flush=True)

    status,cpath=fetch_month(year,month,out,cache,fetch_start,fetch_end)
    b15,btc15=load_month_bars(status,cpath)
    if btc15 is None or len(b15)<20:
        raise RuntimeError(f"{tag}: usable data too small: symbols={len(b15)}, btc={btc15 is not None}")

    print("  [2/7] 1H/4H 생성",flush=True)
    b1,b4=build_higher(b15)

    print("  [3/7] C72 지지/저항 계산",flush=True)
    states=make_states(b1,b4,72)

    print("  [4/7] 15m 신호 + BTC 시장상태 계산",flush=True)
    raw=make_signals(b15,states,fetch_start,entry_end)
    mf=btc_features(btc15)
    sx=attach_btc(raw,mf)
    sx["entry_s1_pct"]=(sx.entry/sx.s1-1)*100

    market_block=sx.btc_weak.fillna(False)&(sx.btc_gap_pct<=-0.50)
    sx["pass_F1"]=~market_block
    sx["pass_F2"]=sx["pass_F1"]&(sx.entry_s1_pct>=0.10)
    rr=pd.to_numeric(sx.rr,errors="coerce")
    sx["pass_F3"]=sx["pass_F2"]&~((rr>=1.5)&(rr<2.0))

    cols=["time","symbol","entry","s1","s2","r1","rr","quality"]
    variants={
        "F0_C72_RAW": sx[cols].copy(),
        "F1_BTC_GAP050": sx.loc[sx.pass_F1,cols].copy(),
        "F2_Q2_RECLAIM010": sx.loc[sx.pass_F2,cols].copy(),
        "F3_Q2_RR_BLOCK": sx.loc[sx.pass_F3,cols].copy(),
    }

    print("  [5/7] 4단계 4슬롯 재시뮬레이션",flush=True)
    summary_rows=[]; daily_parts=[]; trades={}
    for name,vsig in variants.items():
        print(f"    {name}: signals={len(vsig)}",flush=True)
        T=simulate(b15,vsig,entry_start,entry_end,fetch_end)
        trades[name]=T
        T.to_csv(out/f"{name}_TRADES.csv",index=False)
        r,d=summarize(T,name,entry_start,entry_end)
        summary_rows.append(r)
        if len(d): daily_parts.append(d)

    summary=pd.DataFrame(summary_rows)
    daily=pd.concat(daily_parts,ignore_index=True) if daily_parts else pd.DataFrame()
    selectivity=direct_selectivity(trades["F0_C72_RAW"],sx,variants)

    # BTC 월간 참고지표
    b=btc15[(btc15.index>=entry_start)&(btc15.index<=entry_end)]
    btc_row={}
    if len(b):
        first=float(b.close.iloc[0]); last=float(b.close.iloc[-1])
        rollmax=b.close.cummax()
        dd=(b.close/rollmax-1)*100
        dailybtc=b.close.resample("1D").last().dropna().pct_change()*100
        btc_row={
            "btc_month_return_pct":(last/first-1)*100,
            "btc_max_drawdown_pct":float(dd.min()),
            "btc_worst_day_pct":float(dailybtc.min()) if len(dailybtc.dropna()) else np.nan,
        }
        for k,v in btc_row.items():
            summary[k]=v

    summary.insert(0,"month",tag)
    if len(daily): daily.insert(0,"month",tag)
    if len(selectivity): selectivity.insert(0,"month",tag)

    summary.to_csv(out/"MONTH_SUMMARY.csv",index=False)
    daily.to_csv(out/"MONTH_DAILY.csv",index=False)
    selectivity.to_csv(out/"MONTH_DIRECT_SELECTIVITY.csv",index=False)
    sx.to_csv(out/"SIGNALS_WITH_FILTER_FLAGS.csv",index=False)

    print("  [6/7] 월별 ZIP",flush=True)
    zpath=out/f"S_{tag}_OOS_FULLCHAIN_RESULTS.zip"
    with zipfile.ZipFile(zpath,"w",compression=zipfile.ZIP_DEFLATED) as z:
        for p in out.glob("*.csv"):
            z.write(p,arcname=p.name)

    print("  [7/7] 완료",flush=True)
    cols_show=[c for c in ["variant","trades","win_rate_pct","profit_factor_ret",
                           "net_fee_scenario_usdt","max_closed_equity_dd_usdt_fee_scenario",
                           "max_loss_streak","stop_count","target_count",
                           "btc_month_return_pct","btc_max_drawdown_pct"] if c in summary.columns]
    print(summary[cols_show].to_string(index=False),flush=True)
    return summary, daily, selectivity, zpath


def main():
    wait_for_august()

    all_s=[]; all_d=[]; all_sel=[]; zips=[]; errors=[]
    for m in MONTHS:
        try:
            s,d,sel,z=run_month(2026,m)
            all_s.append(s); all_d.append(d); all_sel.append(sel); zips.append(z)
        except Exception as e:
            errors.append({"month":f"2026{m:02d}","error":repr(e)})
            print(f"[ERROR] 2026-{m:02d}: {e}",flush=True)

    master_s=pd.concat(all_s,ignore_index=True) if all_s else pd.DataFrame()
    master_d=pd.concat(all_d,ignore_index=True) if all_d else pd.DataFrame()
    master_sel=pd.concat(all_sel,ignore_index=True) if all_sel else pd.DataFrame()
    err=pd.DataFrame(errors)

    master_s.to_csv(BATCH/"S_2026_01_07_MONTHLY_SUMMARY.csv",index=False)
    master_d.to_csv(BATCH/"S_2026_01_07_MONTHLY_DAILY.csv",index=False)
    master_sel.to_csv(BATCH/"S_2026_01_07_DIRECT_SELECTIVITY.csv",index=False)
    err.to_csv(BATCH/"S_2026_01_07_ERRORS.csv",index=False)

    final_zip=FINAL/"S_2026_01_07_OOS_FULLCHAIN_RESULTS.zip"
    with zipfile.ZipFile(final_zip,"w",compression=zipfile.ZIP_DEFLATED) as z:
        for p in [
            BATCH/"S_2026_01_07_MONTHLY_SUMMARY.csv",
            BATCH/"S_2026_01_07_MONTHLY_DAILY.csv",
            BATCH/"S_2026_01_07_DIRECT_SELECTIVITY.csv",
            BATCH/"S_2026_01_07_ERRORS.csv",
        ]:
            if p.exists(): z.write(p,arcname=p.name)
        for zp in zips:
            if zp.exists():
                z.write(zp,arcname=f"monthly/{zp.name}")

    print("\n"+"="*70,flush=True)
    print("[BATCH COMPLETE]",flush=True)
    print(f"FINAL ZIP={final_zip}",flush=True)
    if len(master_s):
        show=[c for c in ["month","variant","trades","win_rate_pct","profit_factor_ret",
                          "net_fee_scenario_usdt","max_closed_equity_dd_usdt_fee_scenario",
                          "max_loss_streak","btc_month_return_pct","btc_max_drawdown_pct"] if c in master_s.columns]
        print(master_s[show].to_string(index=False),flush=True)


if __name__=="__main__":
    main()
