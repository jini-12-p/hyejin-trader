#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S형 V0 실제 Bybit 15분봉 재검증 — 2026-09-01~2026-09-28 KST
- 데이터: Bybit V5 public linear 15m OHLCV (실제 봉)
- 종목 풀: 기존 9/1~9/28 P형 스캔에서 한 번이라도 관측된 USDT 선물 398개
- 동시 슬롯: 4
- 진입금/레버리지 환산: 27 USDT x 5배 = 135 USDT notional/slot
- 전략 규칙: 기존 S_V0와 동일한 가격/구조 규칙을 실제 15m 봉으로 재계산

주의: 이 스크립트는 신규 S형 연구용 백테스트이며 실제 주문을 전혀 내지 않습니다.
"""
from __future__ import annotations
import os, sys, time, math, json, zipfile, traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import pandas as pd
import numpy as np

OUT = Path(os.environ.get("S_SWING_OUT", "/root/hyejin-trader/swing_actual15_recheck"))
CACHE = OUT / "cache15"
OUT.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)

SYMBOLS = ['0GUSDT', '1000000MOGUSDT', '1000BONKUSDT', '1000BTTUSDT', '1000FLOKIUSDT', '1000NEIROCTOUSDT', '1000RATSUSDT', '1000TOSHIUSDT', '2ZUSDT', '4STOCKUSDT', '4USDT', 'ACEUSDT', 'ACUUSDT', 'AEROUSDT', 'AGIUSDT', 'AGLDUSDT', 'AIOZUSDT', 'AIXBTUSDT', 'AKEUSDT', 'AKTUSDT', 'ALCHUSDT', 'ALGOUSDT', 'ALLOUSDT', 'ALTUSDT', 'AMCUSDT', 'ANIMEUSDT', 'ANKRUSDT', 'APEUSDT', 'APEXUSDT', 'API3USDT', 'APLDUSDT', 'APRUSDT', 'APTUSDT', 'ARBUSDT', 'ARCUSDT', 'ARIAUSDT', 'ARKMUSDT', 'ARKUSDT', 'ARMUSDT', 'ARPAUSDT', 'ARUSDT', 'ARXUSDT', 'ASTERUSDT', 'ASTRUSDT', 'ATHUSDT', 'AUSDT', 'AVAUSDT', 'AXLUSDT', 'AXSUSDT', 'AZTECUSDT', 'B2USDT', 'B3USDT', 'BBXUSDT', 'BEAMUSDT', 'BEATUSDT', 'BERAUSDT', 'BEUSDT', 'BICOUSDT', 'BIGTIMEUSDT', 'BILLUSDT', 'BIOUSDT', 'BIRBUSDT', 'BLASTUSDT', 'BLESSUSDT', 'BLUAIUSDT', 'BLURUSDT', 'BMNRUSDT', 'BMTUSDT', 'BNBUSDT', 'BNCUSDT', 'BOMEUSDT', 'BOTUSDT', 'BPUSDT', 'BRETTUSDT', 'BROCCOLIUSDT', 'BRUSDT', 'BSBUSDT', 'BSPUSDT', 'BTRUSDT', 'BTWUSDT', 'BUSDT', 'BZUSDT', 'CAKEUSDT', 'CAPUSDT', 'CBRSUSDT', 'CCUSDT', 'CELOUSDT', 'CFGUSDT', 'CFXUSDT', 'CHILLGUYUSDT', 'CHIPUSDT', 'CHRUSDT', 'CHZUSDT', 'CLOUSDT', 'CLUSDT', 'COMPUSDT', 'COOKIEUSDT', 'COREUSDT', 'COTIUSDT', 'COWUSDT', 'CPUSDT', 'CRCLUSDT', 'CROSSUSDT', 'CROUSDT', 'CUSDT', 'CYSUSDT', 'DASHUSDT', 'DATAUSDT', 'DBRUSDT', 'DEEPUSDT', 'DELLUSDT', 'DGAIUSDT', 'DOODUSDT', 'DOSUSDT', 'DRAMUSDT', 'DRIFTUSDT', 'DUSKUSDT', 'DYDXUSDT', 'EDENUSDT', 'EDGEUSDT', 'EGLDUSDT', 'EIGENUSDT', 'ELSAUSDT', 'ENAUSDT', 'ENJUSDT', 'ENSOUSDT', 'ENSUSDT', 'EPICUSDT', 'ESPORTSUSDT', 'ESPUSDT', 'ETHFIUSDT', 'EULUSDT', 'EWYUSDT', 'FFUSDT', 'FHEUSDT', 'FIDAUSDT', 'FIGHTUSDT', 'FLOCKUSDT', 'FLOWUSDT', 'FLRUSDT', 'FLYUSDT', 'FOLKSUSDT', 'FORMUSDT', 'FUSDT', 'FWDIUSDT', 'GALAUSDT', 'GASUSDT', 'GENIUSUSDT', 'GIGGLEUSDT', 'GLMUSDT', 'GMTUSDT', 'GMXUSDT', 'GOATUSDT', 'GPROUSDT', 'GPSUSDT', 'GRAMUSDT', 'GRASSUSDT', 'GRIFFAINUSDT', 'GRTUSDT', 'GUNUSDT', 'HAEDALUSDT', 'HAJIMIUSDT', 'HEIUSDT', 'HEMIUSDT', 'HIMSUSDT', 'HNTUSDT', 'HOLOUSDT', 'HOMEUSDT', 'HOODUSDT', 'HUSDT', 'HYPERUSDT', 'HYPEUSDT', 'ICNTUSDT', 'ICPUSDT', 'ICXUSDT', 'IDUSDT', 'IMXUSDT', 'INFQUSDT', 'INITUSDT', 'INJUSDT', 'INUSDT', 'INXUSDT', 'IONQUSDT', 'IOSTUSDT', 'IOTAUSDT', 'IOTXUSDT', 'IOUSDT', 'IRENUSDT', 'IRYSUSDT', 'JASMYUSDT', 'JSTUSDT', 'JTOUSDT', 'JUPUSDT', 'KAITOUSDT', 'KASUSDT', 'KATUSDT', 'KERNELUSDT', 'KITEUSDT', 'KMNOUSDT', 'KNCUSDT', 'KSMUSDT', 'LABUSDT', 'LAOPUUSDT', 'LAUSDT', 'LDOUSDT', 'LINEAUSDT', 'LITEUSDT', 'LITUSDT', 'LONGXIAUSDT', 'LPTUSDT', 'LRCUSDT', 'LSKUSDT', 'LUNA2USDT', 'LYNUSDT', 'MEGAUSDT', 'MELANIAUSDT', 'MERLUSDT', 'METISUSDT', 'METUSDT', 'MEWUSDT', 'MINAUSDT', 'MIRAUSDT', 'MMTUSDT', 'MNTUSDT', 'MONUSDT', 'MOODENGUSDT', 'MORPHOUSDT', 'MRNAUSDT', 'MSTUUSDT', 'MTLUSDT', 'MUBARAKUSDT', 'MUSDT', 'MYXUSDT', 'NAORISUSDT', 'NBISUSDT', 'NCLDUSDT', 'NEARUSDT', 'NESAUSDT', 'NEWTUSDT', 'NIGHTUSDT', 'NILUSDT', 'NIULAIUSDT', 'NMRUSDT', 'NOMUSDT', 'NOTUSDT', 'NXPCUSDT', 'OKTAUSDT', 'ONDOUSDT', 'ONGUSDT', 'ONTUSDT', 'OPGUSDT', 'OPNUSDT', 'OPUSDT', 'ORCAUSDT', 'ORDERUSDT', 'ORDIUSDT', 'PANWUSDT', 'PEAQUSDT', 'PENDLEUSDT', 'PENGUUSDT', 'PEOPLEUSDT', 'PHAROSUSDT', 'PHAUSDT', 'PIPPINUSDT', 'PIXELUSDT', 'PLAYSOUTUSDT', 'PLUMEUSDT', 'POETUSDT', 'POLUSDT', 'POLYXUSDT', 'PONSUSDT', 'PORTALUSDT', 'POWERUSDT', 'POWRUSDT', 'PRLUSDT', 'PTBUSDT', 'PUFFERUSDT', 'PUMPFUNUSDT', 'PUNDIXUSDT', 'PURRUSDT', 'PYTHUSDT', 'QNTUSDT', 'QUSDT', 'RAREUSDT', 'RAYDIUMUSDT', 'RDWUSDT', 'REDUSDT', 'RENDERUSDT', 'REUSDT', 'REZUSDT', 'RKLBUSDT', 'ROBOUSDT', 'ROSEUSDT', 'RUNEUSDT', 'SAFEUSDT', 'SAGAUSDT', 'SAHARAUSDT', 'SAMSUNGUSDT', 'SANDUSDT', 'SEIUSDT', 'SENTUSDT', 'SHIB1000USDT', 'SIGNUSDT', 'SIRENUSDT', 'SKHYUSDT', 'SKLUSDT', 'SKRUSDT', 'SKYAI1USDT', 'SKYUSDT', 'SLXUSDT', 'SMCIUSDT', 'SNOWUSDT', 'SNTUSDT', 'SNXUSDT', 'SNXXUSDT', 'SOLAYERUSDT', 'SOMIUSDT', 'SONICUSDT', 'SOONUSDT', 'SOPHUSDT', 'SOXSUSDT', 'SPCXUSDT', 'SPKUSDT', 'SPXUSDT', 'STABLEUSDT', 'STEEMUSDT', 'STGUSDT', 'STONKUSDT', 'STORJUSDT', 'STRKUSDT', 'STXUSDT', 'STXXUSDT', 'SUIUSDT', 'SUPERUSDT', 'SUSDT', 'SUSHIUSDT', 'SXTUSDT', 'SYRUPUSDT', 'TACUSDT', 'TAOUSDT', 'TAUSDT', 'TEAMUSDT', 'TEMUSDT', 'THETAUSDT', 'TIAUSDT', 'TLMUSDT', 'TMXUSDT', 'TNSRUSDT', 'TQQQUSDT', 'TRBUSDT', 'TREEUSDT', 'TRIAUSDT', 'TRUMPUSDT', 'TRUSTUSDT', 'TSEMUSDT', 'TSLLUSDT', 'TUSDT', 'TUTUSDT', 'TWTUSDT', 'TZAUSDT', 'UAIUSDT', 'UBUSDT', 'UNITREEUSDT', 'UNIUSDT', 'USELESSUSDT', 'USUALUSDT', 'USUSDT', 'WALUSDT', 'WAXPUSDT', 'WETUSDT', 'WIFUSDT', 'WLDUSDT', 'WLFIUSDT', 'WOOUSDT', 'WUSDT', 'XAIUSDT', 'XANUSDT', 'XCNUSDT', 'XDCUSDT', 'XLMUSDT', 'XMRUSDT', 'XNYUSDT', 'XPINUSDT', 'XPLUSDT', 'XTZUSDT', 'ZBTUSDT', 'ZECUSDT', 'ZENUSDT', 'ZEREBROUSDT', 'ZESTUSDT', 'ZETAUSDT', 'ZILUSDT', 'ZKCUSDT', 'ZKPUSDT', 'ZKUSDT', 'ZORAUSDT', 'ZROUSDT', 'ZRXUSDT']

ENTRY_START = pd.Timestamp("2026-09-01 00:00:00")   # KST naive
ENTRY_END   = pd.Timestamp("2026-09-28 23:59:59")   # KST naive
# 1H/4H 레벨 계산용 선행 데이터 + 마지막 진입 24h 청산용 후행 데이터
FETCH_START = pd.Timestamp("2026-08-30 00:00:00")   # KST naive
FETCH_END   = pd.Timestamp("2026-09-30 00:00:00")   # KST naive
MAX_POSITIONS = 4
MARGIN_USDT = 27.0
LEVERAGE = 5.0
NOTIONAL = MARGIN_USDT * LEVERAGE
FEE_EACH_SIDE = 0.00055   # 비교용 시나리오. 실제 수수료가 다르면 결과표에서 별도 수정 가능.
API = "https://api.bybit.com/v5/market/kline"
WORKERS = int(os.environ.get("S_SWING_WORKERS", "6"))


def kst_naive_to_utc_ms(ts: pd.Timestamp) -> int:
    return int(ts.tz_localize("Asia/Seoul").tz_convert("UTC").timestamp() * 1000)

START_MS = kst_naive_to_utc_ms(FETCH_START)
END_MS = kst_naive_to_utc_ms(FETCH_END)


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
                last = RuntimeError(f"HTTP {r.status_code} {r.text[:200]}")
        except Exception as e:
            last = e
        time.sleep(min(0.5 * (2 ** i), 8.0))
    raise last or RuntimeError("request failed")


def fetch_symbol(sym: str):
    fp = CACHE / f"{sym}_15m_20260830_20260930.csv.gz"
    if fp.exists() and fp.stat().st_size > 200:
        try:
            d = pd.read_csv(fp, compression="gzip", parse_dates=["time"])
            if len(d) > 100:
                return sym, len(d), "CACHE", None
        except Exception:
            pass
    rows = []
    cursor_end = END_MS
    seen_min = None
    while cursor_end >= START_MS:
        params = {"category":"linear","symbol":sym,"interval":"15","limit":1000,"end":cursor_end}
        j = _request(params)
        arr = (j.get("result") or {}).get("list") or []
        if not arr:
            break
        # [startTime, open, high, low, close, volume, turnover]
        for x in arr:
            try:
                ms = int(x[0])
                if START_MS <= ms <= END_MS:
                    rows.append((ms, float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5]), float(x[6])))
            except Exception:
                continue
        mins = [int(x[0]) for x in arr if x and str(x[0]).isdigit()]
        if not mins:
            break
        mn = min(mins)
        if seen_min is not None and mn >= seen_min:
            break
        seen_min = mn
        if mn <= START_MS:
            break
        cursor_end = mn - 1
        time.sleep(0.03)
    if not rows:
        return sym, 0, "NO_DATA", None
    d = pd.DataFrame(rows, columns=["ms","open","high","low","close","volume","turnover"])
    d = d.drop_duplicates("ms").sort_values("ms")
    t = pd.to_datetime(d.pop("ms"), unit="ms", utc=True).dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    d.insert(0, "time", t)
    d = d[(d.time >= FETCH_START) & (d.time <= FETCH_END)]
    d.to_csv(fp, index=False, compression="gzip")
    return sym, len(d), "FETCH", None


def fetch_all():
    status=[]
    total=len(SYMBOLS)
    print(f"[1/5] Bybit 실제 15분봉 다운로드 시작: {total} symbols, workers={WORKERS}", flush=True)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs={ex.submit(fetch_symbol,s):s for s in SYMBOLS}
        done=0
        for fut in as_completed(futs):
            s=futs[fut]; done+=1
            try:
                sym,n,mode,err=fut.result()
                status.append({"symbol":sym,"bars":n,"mode":mode,"error":"" if err is None else str(err)})
            except Exception as e:
                status.append({"symbol":s,"bars":0,"mode":"ERROR","error":repr(e)})
            if done % 20 == 0 or done==total:
                ok=sum(1 for x in status if x["bars"]>=100)
                print(f"  {done}/{total} 완료 · 유효 {ok}", flush=True)
    pd.DataFrame(status).sort_values(["bars","symbol"],ascending=[False,True]).to_csv(OUT/"S_ACTUAL15_FETCH_STATUS.csv",index=False)
    return status


def load_bars(status):
    b15={}
    for x in status:
        if x["bars"] < 100: continue
        sym=x["symbol"]; fp=CACHE/f"{sym}_15m_20260830_20260930.csv.gz"
        try:
            d=pd.read_csv(fp,compression="gzip",parse_dates=["time"])
            d=d.set_index("time").sort_index()
            d=d[~d.index.duplicated(keep="last")]
            if len(d)>=100:
                b15[sym]=d[["open","high","low","close","volume","turnover"]].astype(float)
        except Exception:
            pass
    print(f"[2/5] 로드 완료: {len(b15)} symbols", flush=True)
    return b15


def cluster_levels(vals,tol=.006):
    arr=np.sort(np.asarray(vals,float))
    if len(arr)==0:return []
    groups=[]
    for v in arr:
        if groups and abs(v-np.median(groups[-1]))/np.median(groups[-1])<=tol:
            groups[-1].append(v)
        else:
            groups.append([v])
    return [(float(np.median(g)),len(g)) for g in groups if len(g)>=2]


def build_higher(b15):
    b1={}; b4={}
    for sym,d in b15.items():
        # 실제 15m OHLCV -> 1H/4H. 라벨은 구간 시작시각.
        agg={"open":"first","high":"max","low":"min","close":"last","volume":"sum","turnover":"sum"}
        h=d.resample("1h").agg(agg)
        h["n"]=d["close"].resample("1h").count()
        h=h.dropna(subset=["open","high","low","close"])
        q=d.resample("4h").agg(agg)
        q["n"]=d["close"].resample("4h").count()
        q=q.dropna(subset=["open","high","low","close"])
        if len(h)>=20:
            b1[sym]=h; b4[sym]=q
    return b1,b4


def make_sr_hourly(b1,b4,lookback=18):
    states={}
    for sym,h in b1.items():
        d={}
        for k in range(10,len(h)+1):
            hist=h.iloc[max(0,k-lookback):k]
            hour=(h.index[k-1]+pd.Timedelta(hours=1)).floor("h")
            # 실제 15m 원본이므로 1H 완성도는 4개 봉 기준으로 검사
            if len(hist)<10 or (hist.n>=4).mean()<.5: continue
            ref=float(hist.close.iloc[-1])
            supp=cluster_levels(hist.low.values); res=cluster_levels(hist.high.values)
            supp=sorted([(lv,n) for lv,n in supp if lv<=ref*1.015],reverse=True)
            res=sorted([(lv,n) for lv,n in res if lv>=ref*.985])
            if len(supp)<2 or len(res)<1: continue
            s1=supp[0]
            s2s=[x for x in supp[1:] if x[0]<=s1[0]*.99 and x[0]>=s1[0]*.92]
            if not s2s: continue
            s2=s2s[0]
            r1s=[x for x in res if x[0]>=ref*1.005]
            if not r1s: continue
            r1=r1s[0]
            trend_ok=True; q=b4.get(sym)
            if q is not None:
                # 해당 hour 시점에 완전히 종료된 4H 봉만 사용
                qc=q[(q.index+pd.Timedelta(hours=4))<=hour]
                if len(qc)>=3:
                    z=qc.tail(3)
                    down=(z.close.iloc[2]<z.close.iloc[1]<z.close.iloc[0] and
                          z.high.iloc[2]<z.high.iloc[1]<z.high.iloc[0] and
                          z.low.iloc[2]<z.low.iloc[1]<z.low.iloc[0])
                    trend_ok=not down
            if trend_ok:
                d[hour]=(s1[0],s2[0],r1[0],s1[1],s2[1],r1[1])
        states[sym]=d
    return states


def make_signals(b15,states):
    out=[]
    for sym,h in b15.items():
        st=states.get(sym,{})
        if not st: continue
        prev=None
        # 엔트리 기간 주변만 순회
        hh=h[(h.index>=FETCH_START)&(h.index<=ENTRY_END)]
        for t,row in hh.iterrows():
            if prev is None:
                prev=row; continue
            hour=t.floor("h")
            if hour not in st:
                prev=row; continue
            s1,s2,r1,nt1,nt2,ntr=st[hour]
            cl,lo,op=float(row.close),float(row.low),float(row.open)
            # 기존 S_V0 진입 가격조건 그대로
            if not (lo<=s1*1.004 and cl>=s1*.998 and cl>op and cl>float(prev.close)):
                prev=row; continue
            reward=(r1-cl)/cl
            risk_filter_stop=s2*.994   # 기존 S_V0 후보 RR 필터 기준을 그대로 유지
            risk=(cl-risk_filter_stop)/cl
            if reward>=.01 and risk>0 and reward/risk>=1.15:
                quality=reward/risk + .1*(nt1+ntr-4)
                out.append((t,sym,cl,s1,s2,r1,reward/risk,quality))
            prev=row
    return pd.DataFrame(out,columns=["time","symbol","entry","s1","s2","r1","rr","quality"])


def simulate(b15,sig):
    sig=sig[(sig.time>=ENTRY_START)&(sig.time<=ENTRY_END)].copy()
    by={t:g.sort_values("quality",ascending=False) for t,g in sig.groupby("time")}
    # 실제 15m 시간축 전체
    times=pd.date_range(FETCH_START, FETCH_END, freq="15min")
    pos={}; cool={}; tr=[]
    for t in times:
        # exits first
        for sym,p in list(pos.items()):
            h=b15.get(sym)
            if h is None or t not in h.index: continue
            r=h.loc[t]
            lo,hi,cl=float(r.low),float(r.high),float(r.close)
            reason=None; px=None
            stop=p["s1"]*.994   # 기존 S_V0 실제 청산선 그대로
            target=p["r1"]
            # 같은 봉에서 둘 다 닿으면 보수적으로 STOP 우선
            if lo<=stop:
                reason="STOP"; px=stop
            elif hi>=target:
                reason="R1"; px=target
            elif t-p["time"]>=pd.Timedelta(hours=24):
                reason="TIME"; px=cl
            if reason:
                ret=px/p["entry"]-1
                tr.append({**p,"exit_time":t,"exit_price":px,"exit_reason":reason,"ret_pct":ret*100})
                del pos[sym]
                cool[sym]=t+pd.Timedelta(hours=2)
        if t>ENTRY_END: continue
        if t in by and len(pos)<MAX_POSITIONS:
            for _,s in by[t].iterrows():
                sym=s.symbol
                if len(pos)>=MAX_POSITIONS: break
                if sym in pos or (sym in cool and t<cool[sym]): continue
                pos[sym]={"symbol":sym,"time":t,"entry":float(s.entry),"s1":float(s.s1),"s2":float(s.s2),"r1":float(s.r1),"rr":float(s.rr),"quality":float(s.quality)}
    # fetch window 끝까지 미종료 시 마지막 close
    for sym,p in pos.items():
        h=b15[sym]; hh=h[h.index<=FETCH_END]
        if len(hh)==0: continue
        t=hh.index[-1]; px=float(hh.close.iloc[-1]); ret=px/p["entry"]-1
        tr.append({**p,"exit_time":t,"exit_price":px,"exit_reason":"END","ret_pct":ret*100})
    return pd.DataFrame(tr)


def summarize(T):
    if len(T)==0:
        return pd.DataFrame([{"trades":0}]), pd.DataFrame()
    T=T.sort_values(["time","symbol"]).copy()
    T["date"]=T["time"].dt.date
    T["gross_usdt"]=NOTIONAL*T.ret_pct/100
    T["fee_scenario_usdt"]=NOTIONAL*(FEE_EACH_SIDE*2)
    T["net_fee_scenario_usdt"]=T.gross_usdt-T.fee_scenario_usdt
    T["hold_hours"]=(T.exit_time-T.time).dt.total_seconds()/3600
    w=T[T.ret_pct>0]; l=T[T.ret_pct<0]
    gross=T.gross_usdt.sum(); net=T.net_fee_scenario_usdt.sum()
    eq=200+T.net_fee_scenario_usdt.cumsum()
    peak=eq.cummax(); dd=eq-peak
    s=pd.DataFrame([{
      "period":"2026-09-01~2026-09-28 KST",
      "source":"Bybit V5 actual 15m OHLCV",
      "symbols_with_data":T.symbol.nunique(),
      "max_positions":MAX_POSITIONS,
      "margin_per_slot_usdt":MARGIN_USDT,
      "leverage":LEVERAGE,
      "notional_per_slot_usdt":NOTIONAL,
      "trades":len(T),"wins":len(w),"losses":len(l),
      "win_rate_pct":100*len(w)/len(T),
      "sum_ret_pct":T.ret_pct.sum(),
      "avg_win_pct":w.ret_pct.mean() if len(w) else np.nan,
      "avg_loss_pct":l.ret_pct.mean() if len(l) else np.nan,
      "best_trade_pct":T.ret_pct.max(),"worst_trade_pct":T.ret_pct.min(),
      "profit_factor_ret":w.ret_pct.sum()/(-l.ret_pct.sum()) if len(l) and -l.ret_pct.sum()>0 else np.nan,
      "gross_usdt":gross,
      "fee_assumption_each_side_pct":FEE_EACH_SIDE*100,
      "net_fee_scenario_usdt":net,
      "ending_equity_from_200_fee_scenario":200+net,
      "max_closed_equity_dd_usdt_fee_scenario":dd.min(),
      "max_hold_hours":T.hold_hours.max(),
      "avg_hold_hours":T.hold_hours.mean(),
    }])
    daily=T.groupby("date").agg(
       trades=("ret_pct","size"),wins=("ret_pct",lambda x:(x>0).sum()),losses=("ret_pct",lambda x:(x<0).sum()),
       sum_ret_pct=("ret_pct","sum"),gross_usdt=("gross_usdt","sum"),net_fee_scenario_usdt=("net_fee_scenario_usdt","sum")
    ).reset_index()
    return s,daily


def main():
    status=fetch_all()
    b15=load_bars(status)
    if len(b15)<5:
        raise RuntimeError("유효한 실제 15분봉 종목이 너무 적습니다. S_ACTUAL15_FETCH_STATUS.csv를 확인하세요.")
    print("[3/5] 1H/4H 지지·저항 계산",flush=True)
    b1,b4=build_higher(b15)
    states=make_sr_hourly(b1,b4)
    print("[4/5] 15m 반등 신호 생성 + 4슬롯 시뮬레이션",flush=True)
    sig=make_signals(b15,states)
    T=simulate(b15,sig)
    summary,daily=summarize(T)
    trades_path=OUT/"S_ACTUAL15_0901_0928_TRADES.csv"
    daily_path=OUT/"S_ACTUAL15_0901_0928_DAILY.csv"
    summary_path=OUT/"S_ACTUAL15_0901_0928_SUMMARY.csv"
    sig_path=OUT/"S_ACTUAL15_0901_0928_SIGNALS.csv"
    T.to_csv(trades_path,index=False)
    daily.to_csv(daily_path,index=False)
    summary.to_csv(summary_path,index=False)
    sig.to_csv(sig_path,index=False)
    readme=OUT/"README_S_ACTUAL15.txt"
    readme.write_text(
       "S형 V0 실제 Bybit 15분봉 재검증\n"
       "기간: 2026-09-01~2026-09-28 KST (엔트리 기준)\n"
       "4슬롯 / 27 USDT / 5배\n"
       "TP: 계산된 1H R1, STOP: S1*0.994, 최대보유 24h, 종료 후 2h cooldown\n"
       "동일봉 STOP/R1 동시 터치 시 STOP 우선(보수적).\n"
       "수수료 0.055%/side는 비교용 시나리오이며 실제 체결 수수료를 의미하지 않음.\n",
       encoding="utf-8")
    zip_path=OUT/"S_ACTUAL15_0901_0928_RESULTS.zip"
    with zipfile.ZipFile(zip_path,"w",zipfile.ZIP_DEFLATED) as z:
       for p in [summary_path,daily_path,trades_path,sig_path,OUT/"S_ACTUAL15_FETCH_STATUS.csv",readme]:
          z.write(p,arcname=p.name)
    print("[5/5] 완료",flush=True)
    print(summary.to_string(index=False),flush=True)
    print("EXITS",flush=True); print(T.exit_reason.value_counts().to_string() if len(T) else "none",flush=True)
    print(f"RESULT_ZIP={zip_path}",flush=True)

if __name__=="__main__":
    main()
