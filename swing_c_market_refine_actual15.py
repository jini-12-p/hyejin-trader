#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S형 C(1H 72h SR) 시장필터 정밀화 - 실제 Bybit 15m 캐시 재사용

목적:
  현재 C1(BTC 4H 약세 차단)은 STOP과 R1을 둘 다 많이 막는다.
  따라서 C형/TP/STOP/슬롯/수수료는 그대로 고정하고,
  더 '강한 BTC 약세'에서만 차단하는 후보를 비교해
  STOP 차단률 - R1 차단률의 선별력과 실제 순손익/DD를 함께 본다.

공통:
  - C형: 1H 72h 지지저항 + 15m 반등 신호
  - 4 slots, 27 USDT x 5x
  - no averaging
  - same TP/STOP/TIME/cooldown/fee scenario as swing_actual15_recheck.py
  - completed 4H/1H states only (no look-ahead)

후보:
  BASE_NONE               : 필터 없음
  CURR_BTC_WEAK           : 기존 C1 = BTC close<EMA20 & EMA20 하락
  BTC_WEAK_PERSIST2       : 위 약세가 2개 4H봉 연속
  BTC_GAP_025             : 기존 약세 + EMA20 대비 -0.25% 이하
  BTC_GAP_050             : 기존 약세 + EMA20 대비 -0.50% 이하
  BTC_GAP_100             : 기존 약세 + EMA20 대비 -1.00% 이하
  BTC_RET_N05             : 기존 약세 + 해당 4H 수익률 <= -0.50%
  BTC_RET_N10             : 기존 약세 + 해당 4H 수익률 <= -1.00%
  BTC_P2_GAP025           : 2봉 연속 약세 + EMA20 대비 -0.25% 이하
  BTC_AND_ETH_WEAK        : 기존 C2 = BTC/ETH 동시 약세
  BTC_AND_BREADTH40       : BTC 약세 AND breadth<40%
  BTC_CONFIRM_ANY         : BTC 약세 AND (ETH 약세 OR breadth<40%)

결과 ZIP:
  /root/hyejin-trader/bybit_swing/S_C_MARKET_REFINE_ACTUAL15_RESULTS.zip
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
PREV = Path('/root/hyejin-trader/swing_c_market_filters')
OUT = Path('/root/hyejin-trader/swing_c_market_refine')
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
    s, n, mode, err = base.fetch_symbol(sym)
    if n < 100:
        raise RuntimeError(f'{sym} bars too few: {n}, err={err}')
    fp = base.CACHE / f'{sym}_15m_20260830_20260930.csv.gz'
    d = pd.read_csv(fp, compression='gzip', parse_dates=['time']).set_index('time').sort_index()
    d = d[~d.index.duplicated(keep='last')]
    return d[['open','high','low','close','volume','turnover']].astype(float), mode


def market_4h_features(d15):
    q = d15.resample('4h').agg({
        'open':'first','high':'max','low':'min','close':'last','volume':'sum','turnover':'sum'
    })
    q['n'] = d15['close'].resample('4h').count()
    q = q.dropna(subset=['close'])
    q['ema20'] = q['close'].ewm(span=20, adjust=False, min_periods=20).mean()
    q['ema20_prev'] = q['ema20'].shift(1)
    q['gap_pct'] = (q['close'] / q['ema20'] - 1.0) * 100.0
    q['ret4h_pct'] = (q['close'] / q['open'] - 1.0) * 100.0
    q['weak'] = (q['n'] >= 16) & (q['close'] < q['ema20']) & (q['ema20'] < q['ema20_prev'])
    q['weak_prev'] = q['weak'].shift(1).fillna(False)
    q['weak2'] = q['weak'] & q['weak_prev']
    # completed 4H state becomes available only at bar end
    q.index = q.index + pd.Timedelta(hours=4)
    return q[['close','ema20','gap_pct','ret4h_pct','weak','weak2']].sort_index()


def breadth_1h(b15):
    series=[]
    for i,(sym,d) in enumerate(b15.items(),1):
        c = d['close'].resample('1h').last()
        n = d['close'].resample('1h').count()
        ema = c.ewm(span=20, adjust=False, min_periods=20).mean()
        v = ((c > ema) & (n >= 4)).astype(float)
        v[(ema.isna()) | (n < 4)] = np.nan
        v.index = v.index + pd.Timedelta(hours=1)
        v.name = sym
        series.append(v)
        if i % 75 == 0:
            print(f'    breadth prep {i}/{len(b15)}', flush=True)
    M = pd.concat(series, axis=1)
    out = pd.DataFrame({
        'breadth_pct': M.mean(axis=1, skipna=True) * 100.0,
        'breadth_n': M.notna().sum(axis=1)
    })
    out.loc[out.breadth_n < 80, 'breadth_pct'] = np.nan
    return out


def asof_frame(times, feat_df, cols):
    idx = pd.DatetimeIndex(times)
    left = pd.DataFrame({'time':idx, '_order':np.arange(len(idx))}).sort_values('time')
    right = feat_df[cols].reset_index().rename(columns={feat_df.index.name or 'index':'time'}).sort_values('time')
    m = pd.merge_asof(left, right, on='time', direction='backward')
    return m.sort_values('_order')[cols].reset_index(drop=True)


def asof_series(times, series, default=np.nan):
    idx = pd.DatetimeIndex(times)
    left = pd.DataFrame({'time':idx, '_order':np.arange(len(idx))}).sort_values('time')
    right = series.rename('value').dropna().reset_index().rename(columns={series.index.name or 'index':'time'}).sort_values('time')
    m = pd.merge_asof(left, right, on='time', direction='backward')
    return m.sort_values('_order')['value'].fillna(default).to_numpy()


def max_loss_streak(T):
    if T.empty:
        return 0
    z=T.sort_values(['time','symbol'])
    cur=best=0
    for x in z.ret_pct:
        if x < 0:
            cur += 1
            best=max(best,cur)
        else:
            cur=0
    return best


def metrics(name, T, total_signals, allowed_signals, baseline_trades, baseline_block_mask):
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

    bt=baseline_trades
    bm=np.asarray(baseline_block_mask,bool)
    stop=(bt.exit_reason.values=='STOP')
    r1=(bt.exit_reason.values=='R1')
    r['baseline_stop_blocked']=int((bm & stop).sum())
    r['baseline_r1_blocked']=int((bm & r1).sum())
    r['baseline_stop_blocked_pct']=100*r['baseline_stop_blocked']/int(stop.sum()) if stop.sum() else np.nan
    r['baseline_r1_blocked_pct']=100*r['baseline_r1_blocked']/int(r1.sum()) if r1.sum() else np.nan
    r['selectivity_gap_pp']=r['baseline_stop_blocked_pct']-r['baseline_r1_blocked_pct']
    r['r1_preserved_pct']=100-r['baseline_r1_blocked_pct']
    if len(d):
        r['positive_days_fee']=int((d.net_fee_scenario_usdt>0).sum())
        r['negative_days_fee']=int((d.net_fee_scenario_usdt<0).sum())
        r['worst_day_fee_usdt']=float(d.net_fee_scenario_usdt.min())
        r['best_day_fee_usdt']=float(d.net_fee_scenario_usdt.max())
    return r,d


def period_rows(name,T):
    parts=[
        ('P1_0901_0917',pd.Timestamp('2026-09-01'),pd.Timestamp('2026-09-17 23:59:59')),
        ('P2_0918_0928',pd.Timestamp('2026-09-18'),pd.Timestamp('2026-09-28 23:59:59')),
    ]
    out=[]
    for label,a,b in parts:
        x=T[(T.time>=a)&(T.time<=b)].copy()
        if len(x):
            s,_=base.summarize(x)
            r=s.iloc[0].to_dict()
        else:
            r={'trades':0}
        r.update({'variant':name,'segment':label})
        out.append(r)
    return out


def main():
    print('[1/7] 실제15m 캐시/기존 C 결과 로드', flush=True)
    b15=load_cached_universe()
    print(f'  universe={len(b15)}', flush=True)

    sig_path=ABCD/'C_1H72_SIGNALS.csv'
    tr_path=ABCD/'C_1H72_TRADES.csv'
    if not sig_path.exists() or not tr_path.exists():
        raise FileNotFoundError('C_1H72 results missing. Run A-D actual15 first.')
    sig=pd.read_csv(sig_path,parse_dates=['time']).sort_values(['time','symbol']).reset_index(drop=True)
    bt=pd.read_csv(tr_path,parse_dates=['time','exit_time']).sort_values(['time','symbol']).reset_index(drop=True)
    print(f'  C signals={len(sig)}, baseline executed={len(bt)}', flush=True)

    print('[2/7] BTC/ETH 4H 정밀 feature 계산', flush=True)
    btc,mb=load_one('BTCUSDT')
    eth,me=load_one('ETHUSDT')
    bf=market_4h_features(btc)
    ef=market_4h_features(eth)
    print(f'  BTC={mb}, ETH={me}', flush=True)

    print('[3/7] Breadth 재사용/필요시 계산', flush=True)
    br_path=PREV/'MARKET_BREADTH_1H.csv'
    if br_path.exists():
        br=pd.read_csv(br_path,index_col=0,parse_dates=True)
        print('  reuse existing MARKET_BREADTH_1H.csv', flush=True)
    else:
        br=breadth_1h(b15)
        br.to_csv(OUT/'MARKET_BREADTH_1H.csv')
        print('  breadth newly calculated', flush=True)

    print('[4/7] C signals/baseline에 시장상태 부착', flush=True)
    s_b=asof_frame(sig.time,bf,['gap_pct','ret4h_pct','weak','weak2'])
    s_e=asof_frame(sig.time,ef,['weak'])
    sig['btc_gap_pct']=pd.to_numeric(s_b['gap_pct'],errors='coerce')
    sig['btc_ret4h_pct']=pd.to_numeric(s_b['ret4h_pct'],errors='coerce')
    sig['btc_weak']=s_b['weak'].fillna(False).astype(bool)
    sig['btc_weak2']=s_b['weak2'].fillna(False).astype(bool)
    sig['eth_weak']=s_e['weak'].fillna(False).astype(bool)
    sig['breadth_pct']=asof_series(sig.time,br['breadth_pct'],np.nan)

    b_b=asof_frame(bt.time,bf,['gap_pct','ret4h_pct','weak','weak2'])
    b_e=asof_frame(bt.time,ef,['weak'])
    bt['btc_gap_pct']=pd.to_numeric(b_b['gap_pct'],errors='coerce')
    bt['btc_ret4h_pct']=pd.to_numeric(b_b['ret4h_pct'],errors='coerce')
    bt['btc_weak']=b_b['weak'].fillna(False).astype(bool)
    bt['btc_weak2']=b_b['weak2'].fillna(False).astype(bool)
    bt['eth_weak']=b_e['weak'].fillna(False).astype(bool)
    bt['breadth_pct']=asof_series(bt.time,br['breadth_pct'],np.nan)

    sig.to_csv(OUT/'C_SIGNALS_MARKET_REFINE_FEATURES.csv',index=False)
    bt.to_csv(OUT/'C_BASELINE_TRADES_MARKET_REFINE_FEATURES.csv',index=False)

    def make_defs(d):
        w=d['btc_weak'].fillna(False)
        w2=d['btc_weak2'].fillna(False)
        ethw=d['eth_weak'].fillna(False)
        gap=d['btc_gap_pct']
        ret=d['btc_ret4h_pct']
        br40=(d['breadth_pct']<40).fillna(False)
        return {
            'BASE_NONE': pd.Series(False,index=d.index),
            'CURR_BTC_WEAK': w,
            'BTC_WEAK_PERSIST2': w2,
            'BTC_GAP_025': w & (gap <= -0.25),
            'BTC_GAP_050': w & (gap <= -0.50),
            'BTC_GAP_100': w & (gap <= -1.00),
            'BTC_RET_N05': w & (ret <= -0.50),
            'BTC_RET_N10': w & (ret <= -1.00),
            'BTC_P2_GAP025': w2 & (gap <= -0.25),
            'BTC_AND_ETH_WEAK': w & ethw,
            'BTC_AND_BREADTH40': w & br40,
            'BTC_CONFIRM_ANY': w & (ethw | br40),
        }

    defs=make_defs(sig)
    bt_defs=make_defs(bt)

    print('[5/7] 후보별 4슬롯 재시뮬레이션', flush=True)
    rows=[]; daily=[]; periods=[]; trade_files=[]
    for i,(name,block) in enumerate(defs.items(),1):
        allow=~block.fillna(False)
        fsig=sig.loc[allow,['time','symbol','entry','s1','s2','r1','rr','quality']].copy()
        T=base.simulate(b15,fsig)
        tp=OUT/f'{name}_TRADES.csv'
        T.to_csv(tp,index=False)
        trade_files.append(tp)
        r,d=metrics(name,T,len(sig),len(fsig),bt,bt_defs[name].fillna(False).values)
        rows.append(r)
        if len(d):
            d.insert(0,'variant',name)
            daily.append(d)
        periods += period_rows(name,T)
        print(
            f"  {i:02d}/{len(defs)} {name}: trades={r.get('trades')} "
            f"win={r.get('win_rate_pct',np.nan):.2f}% net={r.get('net_fee_scenario_usdt',np.nan):+.2f} "
            f"DD={r.get('max_closed_equity_dd_usdt_fee_scenario',np.nan):+.2f} "
            f"STOPblk={r.get('baseline_stop_blocked_pct',np.nan):.1f}% "
            f"R1blk={r.get('baseline_r1_blocked_pct',np.nan):.1f}% "
            f"gap={r.get('selectivity_gap_pp',np.nan):+.1f}pp",
            flush=True
        )

    summary=pd.DataFrame(rows)
    priority=[
        'variant','trades','wins','losses','win_rate_pct','avg_win_pct','avg_loss_pct','profit_factor_ret',
        'gross_usdt','net_fee_scenario_usdt','ending_equity_from_200_fee_scenario',
        'max_closed_equity_dd_usdt_fee_scenario','max_loss_streak',
        'baseline_stop_blocked_pct','baseline_r1_blocked_pct','selectivity_gap_pp','r1_preserved_pct',
        'signals_total','signals_allowed','signals_blocked','signal_block_pct',
        'positive_days_fee','negative_days_fee','worst_day_fee_usdt','best_day_fee_usdt'
    ]
    summary=summary[[c for c in priority if c in summary.columns]+[c for c in summary.columns if c not in priority]]
    summary['stable_positive_both_segments']=False

    seg=pd.DataFrame(periods)
    if len(seg):
        piv=seg.pivot(index='variant',columns='segment',values='net_fee_scenario_usdt')
        for idx,row in summary.iterrows():
            v=row['variant']
            if v in piv.index:
                vals=piv.loc[v].dropna().values
                summary.loc[idx,'stable_positive_both_segments']=bool(len(vals)>=2 and np.all(vals>0))

    # Helpful ranking columns, but do not auto-select a winner.
    summary['net_minus_abs_dd']=summary['net_fee_scenario_usdt']-summary['max_closed_equity_dd_usdt_fee_scenario'].abs()
    summary['selectivity_efficiency']=np.where(
        summary['baseline_r1_blocked_pct']>0,
        summary['baseline_stop_blocked_pct']/summary['baseline_r1_blocked_pct'],
        np.nan
    )

    sp=OUT/'S_C_MARKET_REFINE_SUMMARY.csv'; summary.to_csv(sp,index=False)
    dp=OUT/'S_C_MARKET_REFINE_DAILY.csv'; pd.concat(daily,ignore_index=True).to_csv(dp,index=False)
    pp=OUT/'S_C_MARKET_REFINE_SEGMENTS.csv'; seg.to_csv(pp,index=False)

    # Compact selectivity-only table for fast review.
    selcols=['variant','trades','win_rate_pct','net_fee_scenario_usdt','max_closed_equity_dd_usdt_fee_scenario',
             'max_loss_streak','baseline_stop_blocked_pct','baseline_r1_blocked_pct','selectivity_gap_pp',
             'r1_preserved_pct','stable_positive_both_segments']
    selp=OUT/'S_C_MARKET_REFINE_SELECTIVITY.csv'
    summary[[c for c in selcols if c in summary.columns]].to_csv(selp,index=False)

    readme=OUT/'README_S_C_MARKET_REFINE.txt'
    readme.write_text(
        'S C-type market-filter refinement on actual Bybit 15m cache.\n'
        'Common C strategy is unchanged. Only entry-time market filter changes.\n'
        'All BTC/ETH 4H states use completed bars only.\n'
        'Primary review: STOP blocked %, R1 blocked %, selectivity gap, net fee PnL, DD, loss streak, and P1/P2 segment stability.\n'
        'Threshold candidates are predefined sensitivity checks; do not select by one metric alone.\n',
        encoding='utf-8'
    )

    print('[6/7] 결과 ZIP 생성', flush=True)
    zip_path=FINAL/'S_C_MARKET_REFINE_ACTUAL15_RESULTS.zip'
    include=[sp,dp,pp,selp,readme,OUT/'C_SIGNALS_MARKET_REFINE_FEATURES.csv',OUT/'C_BASELINE_TRADES_MARKET_REFINE_FEATURES.csv',*trade_files]
    with zipfile.ZipFile(zip_path,'w',zipfile.ZIP_DEFLATED) as z:
        for p in include:
            if p.exists():
                z.write(p,arcname=p.name)

    print('[7/7] 완료', flush=True)
    show=[c for c in ['variant','trades','win_rate_pct','net_fee_scenario_usdt','max_closed_equity_dd_usdt_fee_scenario','max_loss_streak','baseline_stop_blocked_pct','baseline_r1_blocked_pct','selectivity_gap_pp','stable_positive_both_segments'] if c in summary.columns]
    print(summary[show].to_string(index=False), flush=True)
    print(f'ZIP: {zip_path}', flush=True)


if __name__=='__main__':
    main()
