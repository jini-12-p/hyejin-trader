#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S형 C(1H 72h SR) + 진입 전 시장필터 실제 Bybit 15m 비교

기준 C0: C_1H72_SIGNALS.csv 그대로, 4슬롯/27USDT/5x/기존 TP·STOP·24h TIME/cooldown.
필터는 '진입 시점 이전에 완전히 종료된 봉'만 사용해 look-ahead를 피한다.

C0_NONE       : 시장필터 없음
C1_BTC4H      : BTC 4H 약세면 신규롱 차단
C2_BTCETH4H   : BTC와 ETH가 동시에 4H 약세면 신규롱 차단
C3_BREADTH40  : 알트 1H breadth(<40%)면 신규롱 차단
C4_COMBO      : C2 또는 C3가 나쁘면 차단 (둘 다 통과해야 진입)

BTC/ETH 4H 약세 정의:
  latest completed 4H close < EMA20 AND EMA20 < previous EMA20
Breadth 정의:
  기존 397개 실제 15m 캐시를 1H로 묶어, latest completed 1H close > EMA20 인 종목 비율.
  20개 1H 히스토리 이후만 집계, 한 시점 유효 종목 80개 미만이면 breadth 필터 미발동.

추가 민감도: breadth 30/40/50%도 함께 산출.
결과 ZIP은 /root/hyejin-trader/bybit_swing/ 에 직접 생성.
연구/백테스트 전용. 주문 없음.
"""
from __future__ import annotations
import zipfile
from pathlib import Path
import pandas as pd
import numpy as np

try:
    import swing_actual15_recheck as base
except Exception as e:
    raise SystemExit(f"Cannot import swing_actual15_recheck.py: {e}")

ABCD = Path('/root/hyejin-trader/swing_abcd_actual15')
OUT = Path('/root/hyejin-trader/swing_c_market_filters')
FINAL = Path('/root/hyejin-trader/bybit_swing')
OUT.mkdir(parents=True, exist_ok=True)
FINAL.mkdir(parents=True, exist_ok=True)


def load_cached_universe():
    status_path = base.OUT / 'S_ACTUAL15_FETCH_STATUS.csv'
    if not status_path.exists():
        raise FileNotFoundError(f'Missing {status_path}')
    status = pd.read_csv(status_path).fillna('').to_dict('records')
    return base.load_bars(status)


def load_one(sym):
    # fetch_symbol first uses cache if already present; BTC/ETH only, so cheap even if fetch needed.
    s,n,mode,err = base.fetch_symbol(sym)
    if n < 100:
        raise RuntimeError(f'{sym} bars too few: {n}, err={err}')
    fp = base.CACHE / f'{sym}_15m_20260830_20260930.csv.gz'
    d = pd.read_csv(fp, compression='gzip', parse_dates=['time']).set_index('time').sort_index()
    d = d[~d.index.duplicated(keep='last')]
    return d[['open','high','low','close','volume','turnover']].astype(float), mode


def market_4h_weak(d15):
    # 4H bar labeled by start; move state to completion timestamp (+4h).
    q = d15.resample('4h').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum','turnover':'sum'})
    q['n'] = d15['close'].resample('4h').count()
    q = q.dropna(subset=['close'])
    q['ema20'] = q['close'].ewm(span=20, adjust=False, min_periods=20).mean()
    q['ema20_prev'] = q['ema20'].shift(1)
    q['weak'] = (q['n'] >= 16) & (q['close'] < q['ema20']) & (q['ema20'] < q['ema20_prev'])
    s = q['weak'].copy()
    s.index = s.index + pd.Timedelta(hours=4)
    return s.sort_index()


def breadth_1h(b15):
    series=[]
    for i,(sym,d) in enumerate(b15.items(),1):
        c = d['close'].resample('1h').last()
        n = d['close'].resample('1h').count()
        ema = c.ewm(span=20, adjust=False, min_periods=20).mean()
        v = ((c > ema) & (n >= 4)).astype(float)
        # Missing/not-enough-history must be NaN, not False.
        v[(ema.isna()) | (n < 4)] = np.nan
        v.index = v.index + pd.Timedelta(hours=1)  # completed-bar availability time
        v.name = sym
        series.append(v)
        if i % 75 == 0:
            print(f'    breadth prep {i}/{len(b15)}', flush=True)
    M = pd.concat(series, axis=1)
    breadth = M.mean(axis=1, skipna=True) * 100.0
    valid_n = M.notna().sum(axis=1)
    out = pd.DataFrame({'breadth_pct':breadth,'breadth_n':valid_n})
    out.loc[out.breadth_n < 80, 'breadth_pct'] = np.nan
    return out


def asof_values(times, series, default=np.nan):
    # Series indexed by state-available timestamps. For each signal t, use latest state <= t.
    idx = pd.DatetimeIndex(times)
    left = pd.DataFrame({'time':idx, '_order':np.arange(len(idx))}).sort_values('time')
    right = series.rename('value').dropna().reset_index().rename(columns={series.index.name or 'index':'time'})
    right = right.sort_values('time')
    m = pd.merge_asof(left, right, on='time', direction='backward')
    m = m.sort_values('_order')
    return m['value'].fillna(default).to_numpy()


def max_loss_streak(T):
    if T.empty: return 0
    z=T.sort_values(['time','symbol'])
    cur=best=0
    for x in z.ret_pct:
        if x < 0:
            cur += 1; best=max(best,cur)
        else:
            cur=0
    return best


def metrics(name, T, total_signals, allowed_signals, baseline_trades=None, block_mask_baseline=None):
    s,d = base.summarize(T)
    r=s.iloc[0].to_dict()
    r['variant']=name
    r['signals_total']=int(total_signals)
    r['signals_allowed']=int(allowed_signals)
    r['signals_blocked']=int(total_signals-allowed_signals)
    r['signal_block_pct']=100*(total_signals-allowed_signals)/total_signals if total_signals else np.nan
    r['max_loss_streak']=max_loss_streak(T)
    r['stop_count']=int((T.exit_reason=='STOP').sum()) if len(T) else 0
    r['target_count']=int((T.exit_reason=='R1').sum()) if len(T) else 0
    r['time_count']=int((T.exit_reason=='TIME').sum()) if len(T) else 0
    r['end_count']=int((T.exit_reason=='END').sum()) if len(T) else 0
    if len(d):
        r['positive_days_fee']=int((d.net_fee_scenario_usdt>0).sum())
        r['negative_days_fee']=int((d.net_fee_scenario_usdt<0).sum())
        r['worst_day_fee_usdt']=float(d.net_fee_scenario_usdt.min())
        r['best_day_fee_usdt']=float(d.net_fee_scenario_usdt.max())
    if baseline_trades is not None and block_mask_baseline is not None:
        bt=baseline_trades.copy(); bm=np.asarray(block_mask_baseline,bool)
        r['baseline_trades_blocked']=int(bm.sum())
        for reason,key in [('STOP','baseline_stop_blocked'),('R1','baseline_r1_blocked'),('TIME','baseline_time_blocked'),('END','baseline_end_blocked')]:
            mm=(bt.exit_reason.values==reason)
            r[key]=int((bm & mm).sum())
            den=int(mm.sum())
            r[key+'_pct']=100*r[key]/den if den else np.nan
    return r,d


def period_rows(name,T):
    parts=[('P1_0901_0917',pd.Timestamp('2026-09-01'),pd.Timestamp('2026-09-17 23:59:59')),
           ('P2_0918_0928',pd.Timestamp('2026-09-18'),pd.Timestamp('2026-09-28 23:59:59'))]
    out=[]
    for label,a,b in parts:
        x=T[(T.time>=a)&(T.time<=b)].copy()
        s,_=base.summarize(x) if len(x) else (pd.DataFrame([{'trades':0}]),pd.DataFrame())
        r=s.iloc[0].to_dict(); r.update({'variant':name,'segment':label}); out.append(r)
    return out


def main():
    print('[1/7] 기존 actual15 캐시 로드', flush=True)
    b15=load_cached_universe()
    print(f'  universe={len(b15)}', flush=True)

    c_sig_path=ABCD/'C_1H72_SIGNALS.csv'
    c_tr_path=ABCD/'C_1H72_TRADES.csv'
    if not c_sig_path.exists() or not c_tr_path.exists():
        raise FileNotFoundError('C_1H72 results missing. Run A-D actual15 first.')
    sig=pd.read_csv(c_sig_path,parse_dates=['time'])
    baseline=pd.read_csv(c_tr_path,parse_dates=['time','exit_time'])
    print(f'[2/7] C signals={len(sig)}, baseline executed={len(baseline)}', flush=True)

    print('[3/7] BTC/ETH 실제 15m 캐시 확인', flush=True)
    btc,mb=load_one('BTCUSDT'); eth,me=load_one('ETHUSDT')
    print(f'  BTC={mb}, ETH={me}', flush=True)
    bw=market_4h_weak(btc); ew=market_4h_weak(eth)

    print('[4/7] 알트 1H breadth 계산', flush=True)
    br=breadth_1h(b15)
    br.to_csv(OUT/'MARKET_BREADTH_1H.csv')

    # Annotate all C signals.
    sig=sig.copy().sort_values(['time','symbol']).reset_index(drop=True)
    sig['btc_weak']=asof_values(sig.time,bw,False).astype(bool)
    sig['eth_weak']=asof_values(sig.time,ew,False).astype(bool)
    sig['btceth_both_weak']=sig.btc_weak & sig.eth_weak
    sig['breadth_pct']=asof_values(sig.time,br['breadth_pct'],np.nan)
    sig['breadth_n']=asof_values(sig.time,br['breadth_n'],0).astype(int)
    sig.to_csv(OUT/'C_SIGNALS_WITH_MARKET.csv',index=False)

    # Annotate baseline C0 executed trades for exact blocked STOP/R1 counts.
    bt=baseline.copy().sort_values(['time','symbol']).reset_index(drop=True)
    bt['btc_weak']=asof_values(bt.time,bw,False).astype(bool)
    bt['eth_weak']=asof_values(bt.time,ew,False).astype(bool)
    bt['btceth_both_weak']=bt.btc_weak & bt.eth_weak
    bt['breadth_pct']=asof_values(bt.time,br['breadth_pct'],np.nan)
    bt['breadth_n']=asof_values(bt.time,br['breadth_n'],0).astype(int)
    bt.to_csv(OUT/'C0_BASELINE_TRADES_WITH_MARKET.csv',index=False)

    defs={
      'C0_NONE': pd.Series(False,index=sig.index),
      'C1_BTC4H': sig['btc_weak'],
      'C2_BTCETH4H': sig['btceth_both_weak'],
      'C3_BREADTH40': (sig['breadth_pct']<40),
      'C4_COMBO': sig['btceth_both_weak'] | (sig['breadth_pct']<40),
      # sensitivity only
      'SENS_BREADTH30': (sig['breadth_pct']<30),
      'SENS_BREADTH50': (sig['breadth_pct']<50),
    }
    bt_defs={
      'C0_NONE': pd.Series(False,index=bt.index),
      'C1_BTC4H': bt['btc_weak'],
      'C2_BTCETH4H': bt['btceth_both_weak'],
      'C3_BREADTH40': (bt['breadth_pct']<40),
      'C4_COMBO': bt['btceth_both_weak'] | (bt['breadth_pct']<40),
      'SENS_BREADTH30': (bt['breadth_pct']<30),
      'SENS_BREADTH50': (bt['breadth_pct']<50),
    }

    print('[5/7] C0~C4 4슬롯 재시뮬레이션', flush=True)
    rows=[]; daily=[]; periods=[]; files=[]
    for name,block in defs.items():
        allow=~block.fillna(False)
        fsig=sig.loc[allow, ['time','symbol','entry','s1','s2','r1','rr','quality']].copy()
        T=base.simulate(b15,fsig)
        T.to_csv(OUT/f'{name}_TRADES.csv',index=False); files.append(OUT/f'{name}_TRADES.csv')
        r,d=metrics(name,T,len(sig),len(fsig),bt,bt_defs[name].fillna(False).values)
        rows.append(r)
        if len(d): d.insert(0,'variant',name); daily.append(d)
        periods += period_rows(name,T)
        print(f"  {name}: trades={r.get('trades')} win={r.get('win_rate_pct',np.nan):.2f}% net={r.get('net_fee_scenario_usdt',np.nan):+.2f} PF={r.get('profit_factor_ret',np.nan):.3f}",flush=True)

    summary=pd.DataFrame(rows)
    priority=['variant','trades','wins','losses','win_rate_pct','avg_win_pct','avg_loss_pct','profit_factor_ret','gross_usdt','net_fee_scenario_usdt','ending_equity_from_200_fee_scenario','max_closed_equity_dd_usdt_fee_scenario','max_loss_streak','stop_count','target_count','time_count','signals_total','signals_allowed','signals_blocked','signal_block_pct','baseline_trades_blocked','baseline_stop_blocked','baseline_stop_blocked_pct','baseline_r1_blocked','baseline_r1_blocked_pct','positive_days_fee','negative_days_fee','worst_day_fee_usdt','best_day_fee_usdt']
    summary=summary[[c for c in priority if c in summary.columns]+[c for c in summary.columns if c not in priority]]
    sp=OUT/'S_C_MARKET_FILTER_SUMMARY.csv'; summary.to_csv(sp,index=False)
    dp=OUT/'S_C_MARKET_FILTER_DAILY.csv'; pd.concat(daily,ignore_index=True).to_csv(dp,index=False)
    pp=OUT/'S_C_MARKET_FILTER_SEGMENTS.csv'; pd.DataFrame(periods).to_csv(pp,index=False)

    # A compact selectivity table: baseline outcomes blocked by each market filter.
    select=[]
    for name,bm in bt_defs.items():
        bm=bm.fillna(False).values.astype(bool)
        rr={'variant':name,'baseline_trades':len(bt),'blocked':int(bm.sum())}
        for reason in ['STOP','R1','TIME','END']:
            m=(bt.exit_reason.values==reason); den=int(m.sum()); num=int((m&bm).sum())
            rr[f'{reason.lower()}_blocked']=num; rr[f'{reason.lower()}_blocked_pct']=100*num/den if den else np.nan
        select.append(rr)
    selp=OUT/'S_C_MARKET_FILTER_SELECTIVITY.csv'; pd.DataFrame(select).to_csv(selp,index=False)

    readme=OUT/'README_S_C_MARKET_FILTER.txt'
    readme.write_text(
      'S C-type market-filter test on actual Bybit 15m cache.\n'
      'C0 no filter; C1 BTC 4H weak; C2 BTC+ETH both 4H weak; C3 breadth<40%; C4 C2 OR C3.\n'
      '4H weak = latest completed 4H close<EMA20 and EMA20 slope<0.\n'
      'Breadth = share of cached alt universe whose latest completed 1H close>EMA20; breadth state uses only completed 1H bars.\n'
      'SENS_BREADTH30/50 are sensitivity checks, not preselected candidates.\n'
      'Common trading rules unchanged: C 1H72 support/resistance signals, 4 slots, 27 USDT x5, no averaging, same TP/STOP/TIME/cooldown, same fee scenario.\n', encoding='utf-8')

    print('[6/7] 결과 ZIP 생성',flush=True)
    zip_path=FINAL/'S_C_MARKET_FILTER_ACTUAL15_RESULTS.zip'
    include=[sp,dp,pp,selp,readme,OUT/'C_SIGNALS_WITH_MARKET.csv',OUT/'C0_BASELINE_TRADES_WITH_MARKET.csv',OUT/'MARKET_BREADTH_1H.csv',*files]
    with zipfile.ZipFile(zip_path,'w',zipfile.ZIP_DEFLATED) as z:
        for p in include:
            if p.exists(): z.write(p,arcname=p.name)
    print('[7/7] 완료',flush=True)
    print(summary[['variant','trades','win_rate_pct','avg_win_pct','avg_loss_pct','profit_factor_ret','net_fee_scenario_usdt','max_closed_equity_dd_usdt_fee_scenario','max_loss_streak','baseline_stop_blocked_pct','baseline_r1_blocked_pct']].to_string(index=False),flush=True)
    print(f'ZIP: {zip_path}',flush=True)

if __name__=='__main__':
    main()
