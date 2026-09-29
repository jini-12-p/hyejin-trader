#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S형 C + BTC_GAP_050 고정 후 지지품질 재검증 (실제 Bybit 15m 캐시 재사용)

목적
- 시장필터는 BTC_GAP_050으로 고정.
- C형 지지/저항 로직, TP/STOP/TIME, 4슬롯, 27 USDT x 5x, 수수료 시나리오는 그대로.
- 지지 진입 직전 이미 알 수 있는 단순 품질만 추가해서 4슬롯을 처음부터 재시뮬레이션.

후보
Q0_GAP050_BASE          : 시장필터만
Q1_RECLAIM_005         : entry >= S1 +0.05%
Q2_RECLAIM_010         : entry >= S1 +0.10%
Q3_R005_S1S2_250       : +0.05% 회복 & S1-S2 간격 <=2.50%
Q4_R010_S1S2_250       : +0.10% 회복 & S1-S2 간격 <=2.50%
Q5_R010_S1S2_200       : +0.10% 회복 & S1-S2 간격 <=2.00%

주의
- 연구/백테스트 전용. 주문 없음.
- 최종 ZIP은 /root/hyejin-trader/bybit_swing/ 에 바로 생성.
"""
from __future__ import annotations
from pathlib import Path
import zipfile
import numpy as np
import pandas as pd

try:
    import swing_actual15_recheck as base
except Exception as e:
    raise SystemExit(f"Cannot import swing_actual15_recheck.py: {e}")

REFINE = Path('/root/hyejin-trader/swing_c_market_refine')
OUT = Path('/root/hyejin-trader/swing_c_support_quality')
FINAL = Path('/root/hyejin-trader/bybit_swing')
OUT.mkdir(parents=True, exist_ok=True)
FINAL.mkdir(parents=True, exist_ok=True)


def load_cached_universe():
    status_path = base.OUT / 'S_ACTUAL15_FETCH_STATUS.csv'
    if not status_path.exists():
        raise FileNotFoundError(f'Missing {status_path}')
    status = pd.read_csv(status_path).fillna('').to_dict('records')
    return base.load_bars(status)


def max_loss_streak(T):
    if T.empty:
        return 0
    z = T.sort_values(['time','symbol'])
    cur = best = 0
    for x in z.ret_pct:
        if x < 0:
            cur += 1; best = max(best, cur)
        else:
            cur = 0
    return best


def period_rows(name, T):
    parts = [
        ('P1_0901_0917', pd.Timestamp('2026-09-01'), pd.Timestamp('2026-09-17 23:59:59')),
        ('P2_0918_0928', pd.Timestamp('2026-09-18'), pd.Timestamp('2026-09-28 23:59:59')),
    ]
    out=[]
    for label,a,b in parts:
        x=T[(T.time>=a)&(T.time<=b)].copy()
        if len(x):
            s,_=base.summarize(x); r=s.iloc[0].to_dict()
        else:
            r={'trades':0}
        r.update({'variant':name,'segment':label})
        out.append(r)
    return out


def make_support_features(d):
    x=d.copy()
    x['entry_s1_pct']=(x['entry']/x['s1']-1.0)*100.0
    x['s1_s2_gap_pct']=(x['s1']/x['s2']-1.0)*100.0
    x['r1_upside_pct']=(x['r1']/x['entry']-1.0)*100.0
    # 기존 quality 정의에서 총 touch 수 복원: quality = rr + 0.1*(nt1+ntr-4)
    x['touch_total_est']=np.rint((x['quality']-x['rr'])*10.0+4.0)
    return x


def support_defs(d):
    e=d['entry_s1_pct']
    g=d['s1_s2_gap_pct']
    return {
        'Q0_GAP050_BASE': pd.Series(True,index=d.index),
        'Q1_RECLAIM_005': e >= 0.05,
        'Q2_RECLAIM_010': e >= 0.10,
        'Q3_R005_S1S2_250': (e >= 0.05) & (g <= 2.50),
        'Q4_R010_S1S2_250': (e >= 0.10) & (g <= 2.50),
        'Q5_R010_S1S2_200': (e >= 0.10) & (g <= 2.00),
    }


def metrics(name,T,total_signals,allowed_signals,bt,bt_allow):
    s,d=base.summarize(T)
    r=s.iloc[0].to_dict()
    r['variant']=name
    r['signals_total_gap050']=int(total_signals)
    r['signals_allowed_support']=int(allowed_signals)
    r['signals_blocked_support']=int(total_signals-allowed_signals)
    r['signal_block_pct_support']=100*(total_signals-allowed_signals)/total_signals if total_signals else np.nan
    r['max_loss_streak']=max_loss_streak(T)
    r['stop_count']=int((T.exit_reason=='STOP').sum()) if len(T) else 0
    r['target_count']=int((T.exit_reason=='R1').sum()) if len(T) else 0
    r['time_count']=int((T.exit_reason=='TIME').sum()) if len(T) else 0

    allow=np.asarray(bt_allow,bool)
    stop=bt.exit_reason.eq('STOP').to_numpy()
    r1=bt.exit_reason.eq('R1').to_numpy()
    r['baseline_gap050_stop_total']=int(stop.sum())
    r['baseline_gap050_r1_total']=int(r1.sum())
    r['baseline_stop_preserved']=int((allow & stop).sum())
    r['baseline_r1_preserved']=int((allow & r1).sum())
    r['baseline_stop_preserved_pct']=100*r['baseline_stop_preserved']/stop.sum() if stop.sum() else np.nan
    r['baseline_r1_preserved_pct']=100*r['baseline_r1_preserved']/r1.sum() if r1.sum() else np.nan
    r['baseline_stop_blocked_pct']=100-r['baseline_stop_preserved_pct']
    r['baseline_r1_blocked_pct']=100-r['baseline_r1_preserved_pct']
    r['selectivity_gap_pp']=r['baseline_stop_blocked_pct']-r['baseline_r1_blocked_pct']
    if len(d):
        r['positive_days_fee']=int((d.net_fee_scenario_usdt>0).sum())
        r['negative_days_fee']=int((d.net_fee_scenario_usdt<0).sum())
        r['worst_day_fee_usdt']=float(d.net_fee_scenario_usdt.min())
        r['best_day_fee_usdt']=float(d.net_fee_scenario_usdt.max())
    return r,d


def main():
    print('[1/6] 실제 15m 캐시 로드', flush=True)
    b15=load_cached_universe()
    print(f'  universe={len(b15)}', flush=True)

    sig_path=REFINE/'C_SIGNALS_MARKET_REFINE_FEATURES.csv'
    bt_path=REFINE/'BTC_GAP_050_TRADES.csv'
    if not sig_path.exists() or not bt_path.exists():
        raise FileNotFoundError('시장필터 정밀화 결과가 없습니다. S_C_MARKET_REFINE을 먼저 실행하세요.')

    print('[2/6] C 신호 + GAP0.50 시장필터 고정', flush=True)
    sig=pd.read_csv(sig_path,parse_dates=['time']).sort_values(['time','symbol']).reset_index(drop=True)
    bt=pd.read_csv(bt_path,parse_dates=['time','exit_time']).sort_values(['time','symbol']).reset_index(drop=True)
    sig=make_support_features(sig)
    bt=make_support_features(bt)

    market_block=sig['btc_weak'].fillna(False) & (pd.to_numeric(sig['btc_gap_pct'],errors='coerce')<=-0.50)
    msig=sig.loc[~market_block.fillna(False)].copy().reset_index(drop=True)
    print(f'  all C signals={len(sig)}, GAP0.50 pass signals={len(msig)}, baseline trades={len(bt)}', flush=True)

    print('[3/6] 지지품질 후보 정의', flush=True)
    defs=support_defs(msig)
    bt_defs=support_defs(bt)

    rows=[]; daily=[]; periods=[]; trade_files=[]
    print('[4/6] 후보별 4슬롯 재시뮬레이션', flush=True)
    for i,(name,allow) in enumerate(defs.items(),1):
        fsig=msig.loc[allow.fillna(False),['time','symbol','entry','s1','s2','r1','rr','quality']].copy()
        T=base.simulate(b15,fsig)
        tp=OUT/f'{name}_TRADES.csv'; T.to_csv(tp,index=False); trade_files.append(tp)
        r,d=metrics(name,T,len(msig),len(fsig),bt,bt_defs[name].fillna(False).to_numpy())
        rows.append(r)
        if len(d):
            d.insert(0,'variant',name); daily.append(d)
        periods.extend(period_rows(name,T))
        print(f"  {i}/{len(defs)} {name}: trades={r.get('trades',0):.0f} win={r.get('win_rate_pct',np.nan):.2f}% net={r.get('net_fee_scenario_usdt',np.nan):+.2f} DD={r.get('max_closed_equity_dd_usdt_fee_scenario',np.nan):+.2f} STOPblk={r.get('baseline_stop_blocked_pct',np.nan):.1f}% R1blk={r.get('baseline_r1_blocked_pct',np.nan):.1f}% sel={r.get('selectivity_gap_pp',np.nan):+.1f}pp", flush=True)

    summary=pd.DataFrame(rows)
    seg=pd.DataFrame(periods)
    daily_df=pd.concat(daily,ignore_index=True) if daily else pd.DataFrame()

    # stable both segments
    if not seg.empty and 'net_fee_scenario_usdt' in seg.columns:
        pivot=seg.pivot(index='variant',columns='segment',values='net_fee_scenario_usdt')
        summary['P1_net_fee_usdt']=summary['variant'].map(pivot.get('P1_0901_0917',pd.Series(dtype=float)))
        summary['P2_net_fee_usdt']=summary['variant'].map(pivot.get('P2_0918_0928',pd.Series(dtype=float)))
        summary['stable_positive_both_segments']=(summary['P1_net_fee_usdt']>0)&(summary['P2_net_fee_usdt']>0)

    order=['variant','trades','win_rate_pct','avg_win_pct','avg_loss_pct','profit_factor','gross_usdt','net_fee_scenario_usdt','max_closed_equity_dd_usdt_fee_scenario','max_loss_streak','stop_count','target_count','time_count','baseline_stop_blocked_pct','baseline_r1_blocked_pct','selectivity_gap_pp','signals_total_gap050','signals_allowed_support','signal_block_pct_support','P1_net_fee_usdt','P2_net_fee_usdt','stable_positive_both_segments','worst_day_fee_usdt','best_day_fee_usdt']
    cols=[c for c in order if c in summary.columns]+[c for c in summary.columns if c not in order]
    summary=summary[cols]
    summary.to_csv(OUT/'S_C_SUPPORT_QUALITY_SUMMARY.csv',index=False)
    seg.to_csv(OUT/'S_C_SUPPORT_QUALITY_SEGMENTS.csv',index=False)
    if len(daily_df): daily_df.to_csv(OUT/'S_C_SUPPORT_QUALITY_DAILY.csv',index=False)
    msig.to_csv(OUT/'C_GAP050_SIGNALS_SUPPORT_FEATURES.csv',index=False)
    bt.to_csv(OUT/'C_GAP050_BASELINE_TRADES_SUPPORT_FEATURES.csv',index=False)

    readme=OUT/'README_S_C_SUPPORT_QUALITY.txt'
    readme.write_text(
        'Actual15 C + BTC_GAP_050 support-quality validation.\n'
        'Market filter, C SR logic, exits, 4 slots, 27 USDT x5, fee scenario are fixed.\n'
        'Primary review: net fee PnL, DD, loss streak, STOP blocked vs R1 blocked, and P1/P2 stability.\n'
        'Support rules use only information known at entry.\n',encoding='utf-8')

    print('[5/6] ZIP 생성', flush=True)
    zpath=FINAL/'S_C_SUPPORT_QUALITY_ACTUAL15_RESULTS.zip'
    files=[OUT/'S_C_SUPPORT_QUALITY_SUMMARY.csv',OUT/'S_C_SUPPORT_QUALITY_SEGMENTS.csv',OUT/'C_GAP050_SIGNALS_SUPPORT_FEATURES.csv',OUT/'C_GAP050_BASELINE_TRADES_SUPPORT_FEATURES.csv',readme,*trade_files]
    if (OUT/'S_C_SUPPORT_QUALITY_DAILY.csv').exists(): files.append(OUT/'S_C_SUPPORT_QUALITY_DAILY.csv')
    with zipfile.ZipFile(zpath,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for p in files:
            if p.exists(): z.write(p,arcname=p.name)

    print('[6/6] 완료', flush=True)
    show=[c for c in ['variant','trades','win_rate_pct','net_fee_scenario_usdt','max_closed_equity_dd_usdt_fee_scenario','max_loss_streak','baseline_stop_blocked_pct','baseline_r1_blocked_pct','selectivity_gap_pp','P1_net_fee_usdt','P2_net_fee_usdt'] if c in summary.columns]
    print(summary[show].to_string(index=False), flush=True)
    print(f'ZIP={zpath}', flush=True)

if __name__=='__main__':
    main()
