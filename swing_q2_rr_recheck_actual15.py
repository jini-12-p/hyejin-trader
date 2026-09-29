#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S형 Q2 고정 + RR 1.5~2.0 구간 차단 실제 15m 4슬롯 재검증

고정 기준
- C형: 1H 72시간 지지/저항
- 시장필터: BTC 4H GAP -0.50
- 지지품질: S1 대비 +0.10% 이상 회복 확인(Q2)
- 4슬롯 / 슬롯당 27 USDT / 5x
- TP/STOP/TIME 및 수수료 시나리오: swing_actual15_recheck.py와 동일
- 물타기 없음
- 손절 수정 없음

비교
Q2_BASE
Q2_RR_BLOCK_15_20 : 진입시 1.5 <= RR < 2.0 이면 차단

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
OUT = Path('/root/hyejin-trader/swing_q2_rr_recheck')
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
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def period_rows(name, T):
    parts = [
        ('P1_0901_0917', pd.Timestamp('2026-09-01'), pd.Timestamp('2026-09-17 23:59:59')),
        ('P2_0918_0928', pd.Timestamp('2026-09-18'), pd.Timestamp('2026-09-28 23:59:59')),
    ]
    out = []
    for label, a, b in parts:
        x = T[(T.time >= a) & (T.time <= b)].copy()
        if len(x):
            s, _ = base.summarize(x)
            r = s.iloc[0].to_dict()
        else:
            r = {'trades': 0}
        r.update({'variant': name, 'segment': label})
        out.append(r)
    return out


def make_features(d):
    x = d.copy()
    x['entry_s1_pct'] = (x['entry'] / x['s1'] - 1.0) * 100.0
    return x


def metrics(name, T, baseline_trades=None, baseline_allow=None):
    s, daily = base.summarize(T)
    r = s.iloc[0].to_dict()
    r['variant'] = name
    r['max_loss_streak'] = max_loss_streak(T)
    r['stop_count'] = int((T.exit_reason == 'STOP').sum()) if len(T) else 0
    r['target_count'] = int((T.exit_reason == 'R1').sum()) if len(T) else 0
    r['time_count'] = int((T.exit_reason == 'TIME').sum()) if len(T) else 0

    if len(daily):
        r['positive_days_fee'] = int((daily.net_fee_scenario_usdt > 0).sum())
        r['negative_days_fee'] = int((daily.net_fee_scenario_usdt < 0).sum())
        r['worst_day_fee_usdt'] = float(daily.net_fee_scenario_usdt.min())
        r['best_day_fee_usdt'] = float(daily.net_fee_scenario_usdt.max())

    if baseline_trades is not None and baseline_allow is not None:
        bt = baseline_trades
        allow = np.asarray(baseline_allow, bool)
        stop = bt.exit_reason.eq('STOP').to_numpy()
        r1 = bt.exit_reason.eq('R1').to_numpy()
        r['q2_stop_total'] = int(stop.sum())
        r['q2_r1_total'] = int(r1.sum())
        r['q2_stop_preserved'] = int((allow & stop).sum())
        r['q2_r1_preserved'] = int((allow & r1).sum())
        r['q2_stop_blocked_pct'] = 100 - (100 * r['q2_stop_preserved'] / stop.sum() if stop.sum() else np.nan)
        r['q2_r1_blocked_pct'] = 100 - (100 * r['q2_r1_preserved'] / r1.sum() if r1.sum() else np.nan)
        r['selectivity_gap_pp'] = r['q2_stop_blocked_pct'] - r['q2_r1_blocked_pct']
    return r, daily


def main():
    print('[1/6] 실제 15m 캐시 로드', flush=True)
    b15 = load_cached_universe()
    print(f'  universe={len(b15)}', flush=True)

    sig_path = REFINE / 'C_SIGNALS_MARKET_REFINE_FEATURES.csv'
    if not sig_path.exists():
        raise FileNotFoundError('시장필터 정밀화 결과가 없습니다.')

    print('[2/6] C + BTC GAP0.50 + Q2(+0.10% 회복) 신호 구성', flush=True)
    sig = pd.read_csv(sig_path, parse_dates=['time']).sort_values(['time','symbol']).reset_index(drop=True)
    sig = make_features(sig)

    btc_gap = pd.to_numeric(sig['btc_gap_pct'], errors='coerce')
    market_block = sig['btc_weak'].fillna(False) & (btc_gap <= -0.50)
    q2 = sig.loc[(~market_block.fillna(False)) & (sig['entry_s1_pct'] >= 0.10)].copy().reset_index(drop=True)
    print(f'  Q2 candidate signals={len(q2)}', flush=True)

    print('[3/6] Q2 baseline 4슬롯 재시뮬레이션', flush=True)
    cols = ['time','symbol','entry','s1','s2','r1','rr','quality']
    T0 = base.simulate(b15, q2[cols].copy())
    T0.to_csv(OUT/'Q2_BASE_TRADES.csv', index=False)
    print(f'  Q2 trades={len(T0)}', flush=True)

    print('[4/6] RR 1.5~2.0 차단 후 4슬롯 재시뮬레이션', flush=True)
    rr = pd.to_numeric(q2['rr'], errors='coerce')
    allow_sig = ~((rr >= 1.5) & (rr < 2.0))
    q2_rr = q2.loc[allow_sig.fillna(False), cols].copy()
    T1 = base.simulate(b15, q2_rr)
    T1.to_csv(OUT/'Q2_RR_BLOCK_15_20_TRADES.csv', index=False)
    print(f'  filtered signals={len(q2_rr)}, trades={len(T1)}', flush=True)

    # 기존 Q2 실제 체결건에서 RR 차단이 STOP/R1을 어떻게 가르는지도 함께 기록
    bt_rr = pd.to_numeric(T0['rr'], errors='coerce')
    bt_allow = ~((bt_rr >= 1.5) & (bt_rr < 2.0))

    rows = []
    daily_all = []
    periods = []

    r0, d0 = metrics('Q2_BASE', T0)
    rows.append(r0)
    if len(d0):
        d0.insert(0, 'variant', 'Q2_BASE')
        daily_all.append(d0)
    periods.extend(period_rows('Q2_BASE', T0))

    r1, d1 = metrics('Q2_RR_BLOCK_15_20', T1, T0, bt_allow.to_numpy())
    rows.append(r1)
    if len(d1):
        d1.insert(0, 'variant', 'Q2_RR_BLOCK_15_20')
        daily_all.append(d1)
    periods.extend(period_rows('Q2_RR_BLOCK_15_20', T1))

    summary = pd.DataFrame(rows)
    seg = pd.DataFrame(periods)
    daily_df = pd.concat(daily_all, ignore_index=True) if daily_all else pd.DataFrame()

    if not seg.empty and 'net_fee_scenario_usdt' in seg.columns:
        pivot = seg.pivot(index='variant', columns='segment', values='net_fee_scenario_usdt')
        summary['P1_net_fee_usdt'] = summary['variant'].map(pivot.get('P1_0901_0917', pd.Series(dtype=float)))
        summary['P2_net_fee_usdt'] = summary['variant'].map(pivot.get('P2_0918_0928', pd.Series(dtype=float)))
        summary['stable_positive_both_segments'] = (summary['P1_net_fee_usdt'] > 0) & (summary['P2_net_fee_usdt'] > 0)

    summary.to_csv(OUT/'S_Q2_RR_RECHECK_SUMMARY.csv', index=False)
    seg.to_csv(OUT/'S_Q2_RR_RECHECK_SEGMENTS.csv', index=False)
    if len(daily_df):
        daily_df.to_csv(OUT/'S_Q2_RR_RECHECK_DAILY.csv', index=False)
    q2.to_csv(OUT/'Q2_SIGNALS_BEFORE_RR_FILTER.csv', index=False)

    readme = OUT/'README_S_Q2_RR_RECHECK.txt'
    readme.write_text(
        'Actual15 Q2 fixed + RR 1.5<=RR<2.0 block validation.\n'
        'C72H + BTC GAP -0.50 + S1 reclaim >=0.10% are fixed.\n'
        'Compare only Q2 baseline vs RR-band exclusion using the same 4-slot simulator.\n'
        'Research/backtest only. No orders.\n',
        encoding='utf-8'
    )

    print('[5/6] 결과 ZIP 생성', flush=True)
    zpath = FINAL/'S_Q2_RR_RECHECK_ACTUAL15_RESULTS.zip'
    files = [
        OUT/'S_Q2_RR_RECHECK_SUMMARY.csv',
        OUT/'S_Q2_RR_RECHECK_SEGMENTS.csv',
        OUT/'S_Q2_RR_RECHECK_DAILY.csv',
        OUT/'Q2_BASE_TRADES.csv',
        OUT/'Q2_RR_BLOCK_15_20_TRADES.csv',
        OUT/'Q2_SIGNALS_BEFORE_RR_FILTER.csv',
        readme
    ]
    with zipfile.ZipFile(zpath, 'w', compression=zipfile.ZIP_DEFLATED) as z:
        for p in files:
            if p.exists():
                z.write(p, arcname=p.name)

    print('[6/6] 완료', flush=True)
    show = [c for c in [
        'variant','trades','win_rate_pct','avg_win_pct','avg_loss_pct','profit_factor',
        'net_fee_scenario_usdt','max_closed_equity_dd_usdt_fee_scenario','max_loss_streak',
        'q2_stop_blocked_pct','q2_r1_blocked_pct','selectivity_gap_pp',
        'P1_net_fee_usdt','P2_net_fee_usdt','worst_day_fee_usdt','best_day_fee_usdt'
    ] if c in summary.columns]
    print(summary[show].to_string(index=False), flush=True)
    print(f'ZIP={zpath}', flush=True)


if __name__ == '__main__':
    main()
