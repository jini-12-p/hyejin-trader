#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""JAN-AUG 2026 BTC+ETH 15m crash -> falling/rebound diagnostic.
READ-ONLY research. Does not regenerate V25 candidates and does not place orders.
Uses latest CURRENT_P_JANAUG result ZIP + public/cached BTC/ETH 1m klines.
"""
from __future__ import annotations
import csv, json, zipfile, importlib.util
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import Counter
import pandas as pd

ROOT=Path(__file__).resolve().parent
KST=timezone(timedelta(hours=9)); UTC=timezone.utc
START=datetime(2026,1,1,tzinfo=KST).astimezone(UTC)
END=datetime(2026,9,1,tzinfo=KST).astimezone(UTC)
TH=-0.50
MAX_EVENT_MIN=60
STAMP=datetime.now(KST).strftime('%Y%m%d_%H%M%S')
PREF=f'JANAUG_BTCETH_CRASH_REBOUND_{STAMP}'


def load_balanced():
    p=ROOT/'backtest_current_p_jan_aug_2026_balanced.py'
    if not p.exists(): raise SystemExit(f'missing {p}')
    sp=importlib.util.spec_from_file_location('BAL',str(p)); m=importlib.util.module_from_spec(sp); sp.loader.exec_module(m); return m


def latest_current_zip():
    zs=sorted(ROOT.glob('CURRENT_P_JANAUG_2026_*_RESULTS.zip'), key=lambda p:p.stat().st_mtime, reverse=True)
    if not zs: raise SystemExit('No CURRENT_P_JANAUG_2026_*_RESULTS.zip found in bybit_swing')
    return zs[0]


def load_trades(zp:Path):
    with zipfile.ZipFile(zp) as z:
        names=z.namelist(); cand=[n for n in names if n.endswith('_TRADES.csv') and 'NOFILTER' not in n and 'CRASH' not in n and 'BE_' not in n]
        if not cand: raise SystemExit(f'CURRENT trades csv not found in {zp.name}')
        with z.open(cand[0]) as f: df=pd.read_csv(f)
    df['entry_dt']=pd.to_datetime(df['entry_time_kst'], errors='coerce').dt.tz_localize(KST, nonexistent='shift_forward', ambiguous='NaT').dt.tz_convert(UTC)
    df['exit_dt']=pd.to_datetime(df['exit_time_kst'], errors='coerce').dt.tz_localize(KST, nonexistent='shift_forward', ambiguous='NaT').dt.tz_convert(UTC)
    df['net_pct']=pd.to_numeric(df['net_pct'],errors='coerce').fillna(0.0)
    return df.dropna(subset=['entry_dt']).copy(), cand[0]


def load_market(B):
    cache=B.HistCache(ROOT/'.janaug_hist_cache_v1')
    frames={}
    for sym in ('BTCUSDT','ETHUSDT'):
        parts=[]
        for m in range(1,9):
            st=datetime(2026,m,1,tzinfo=KST).astimezone(UTC)
            en=(datetime(2026,m+1,1,tzinfo=KST) if m<8 else datetime(2026,9,1,tzinfo=KST)).astimezone(UTC)
            print(f'[KLINE] {sym} {m}/8',flush=True)
            d=cache.get(sym,'1',st-timedelta(minutes=20),en)
            parts.append(d[['start_time','close']].copy())
            cache.clear_mem()
        x=pd.concat(parts,ignore_index=True).drop_duplicates('start_time').sort_values('start_time')
        x['start_time']=pd.to_datetime(x['start_time'],utc=True)
        x=x[(x.start_time>=START-timedelta(minutes=20))&(x.start_time<END)].copy()
        x=x.set_index('start_time')['close'].astype(float).rename(sym)
        frames[sym]=x
    m=pd.concat(frames.values(),axis=1).sort_index().ffill(limit=2).dropna()
    for sym in ('BTCUSDT','ETHUSDT'):
        m[sym+'_r15']=(m[sym]/m[sym].shift(15)-1)*100
        m[sym+'_r1']=(m[sym]/m[sym].shift(1)-1)*100
    m['joint_r15']=(m.BTCUSDT_r15+m.ETHUSDT_r15)/2
    m['comp']=(m.BTCUSDT/m.BTCUSDT.iloc[0]+m.ETHUSDT/m.ETHUSDT.iloc[0])/2
    return m


def build_states(m):
    # Causal state machine: trigger when BOTH 15m returns <= -0.50.
    # During an active event, every new composite low is FALLING. Time since last low labels rebound.
    active=False; event_id=0; low_comp=None; low_t=None; start_t=None
    rows=[]; events=[]
    for t,r in m.iterrows():
        trig=bool(r.BTCUSDT_r15<=TH and r.ETHUSDT_r15<=TH)
        if trig and not active:
            active=True; event_id+=1; start_t=t; low_t=t; low_comp=float(r.comp)
        if active:
            if float(r.comp) <= low_comp:
                low_comp=float(r.comp); low_t=t; state='FALLING'
            else:
                mins=(t-low_t).total_seconds()/60
                if mins<=5: state='REBOUND_0_5'
                elif mins<=10: state='REBOUND_5_10'
                elif mins<=15: state='REBOUND_10_15'
                elif mins<=30: state='REBOUND_15_30'
                else: state='REBOUND_30_60'
            since_low=(t-low_t).total_seconds()/60
            rows.append((t,event_id,state,start_t,low_t,r.BTCUSDT_r15,r.ETHUSDT_r15,r.joint_r15))
            # expire 60m after last low once immediate crash condition is gone
            if since_low>=MAX_EVENT_MIN and not trig:
                events.append({'event_id':event_id,'start_utc':start_t,'last_low_utc':low_t,'duration_min':(t-start_t).total_seconds()/60})
                active=False; low_comp=low_t=start_t=None
    st=pd.DataFrame(rows,columns=['ts','event_id','state','event_start','running_low_time','btc15','eth15','joint15']).set_index('ts')
    return st,pd.DataFrame(events)


def state_at(states,t):
    tt=pd.Timestamp(t).floor('min')
    if tt in states.index:
        r=states.loc[tt]
        if isinstance(r,pd.DataFrame): r=r.iloc[-1]
        return r
    return None


def summarize(df,key='market_state'):
    out=[]
    order=['FALLING','REBOUND_0_5','REBOUND_5_10','REBOUND_10_15','REBOUND_15_30','REBOUND_30_60','NORMAL']
    for k in order:
        g=df[df[key]==k]
        if len(g)==0: continue
        c=Counter(g.result.astype(str))
        out.append({'state':k,'entries':len(g),'TP':c['TP20_FULL'],'PP':c['PROFIT_PROTECT_EXIT'],'STOP':c['STOP'],'LATE':c['LATE_FAILURE_EXIT'],'TIME':c['TIME_EXIT'],'FLAT':c['FLAT_EXIT_75M'],'stop_rate_pct':round(c['STOP']/len(g)*100,2),'net_pct':round(g.net_pct.sum(),6),'avg_net_pct':round(g.net_pct.mean(),6)})
    return pd.DataFrame(out)


def main():
    B=load_balanced(); zp=latest_current_zip(); trades,trname=load_trades(zp)
    print(f'[SOURCE] {zp.name} :: {trname} trades={len(trades)}',flush=True)
    m=load_market(B); print(f'[MARKET] rows={len(m)} {m.index.min()} -> {m.index.max()}',flush=True)
    states,events=build_states(m); print(f'[EVENTS] {states.event_id.nunique() if len(states) else 0}',flush=True)
    vals=[]
    for _,r in trades.iterrows():
        s=state_at(states,r.entry_dt)
        vals.append('NORMAL' if s is None else str(s.state))
    trades['market_state']=vals
    summ=summarize(trades)

    # Positions entered before an event but STOP while event is active.
    stoprows=[]
    for _,r in trades[trades.result.astype(str).eq('STOP')].iterrows():
        s=state_at(states,r.exit_dt) if pd.notna(r.exit_dt) else None
        if s is not None:
            stoprows.append({**r.drop(labels=['entry_dt','exit_dt'],errors='ignore').to_dict(),'entry_dt_utc':r.entry_dt.isoformat(),'exit_dt_utc':r.exit_dt.isoformat(),'stop_market_state':str(s.state),'event_id':int(s.event_id),'btc15_at_stop':float(s.btc15),'eth15_at_stop':float(s.eth15)})
    stopdf=pd.DataFrame(stoprows)

    # For market-event STOPs, inspect ALT price after stop using cached/public 1m, no hypothetical P&L claims.
    rec=[]; kc=B.HistCache(ROOT/'.janaug_hist_cache_v1')
    for i,r in stopdf.iterrows():
        try:
            et=pd.Timestamp(r['exit_dt_utc']).to_pydatetime().astimezone(UTC); sym=str(r['symbol']); entry=float(r['entry_price'])
            x=kc.get(sym,'1',et-timedelta(minutes=1),et+timedelta(minutes=61))
            x=x.copy(); x['start_time']=pd.to_datetime(x['start_time'],utc=True); x=x[x.start_time>=pd.Timestamp(et).floor('min')]
            d={'setup_id':r.get('setup_id'),'symbol':sym,'event_id':r['event_id'],'stop_state':r['stop_market_state'],'entry_price':entry,'base_net_pct':r['net_pct']}
            for mm in (5,10,15,30,60):
                y=x[x.start_time<=pd.Timestamp(et+timedelta(minutes=mm))]
                if len(y):
                    d[f'max_recovery_{mm}m_pct']=round((float(y.high.max())/entry-1)*100,6)
                    d[f'min_after_{mm}m_pct']=round((float(y.low.min())/entry-1)*100,6)
            rec.append(d)
        except Exception as e:
            rec.append({'setup_id':r.get('setup_id'),'symbol':r.get('symbol'),'error':str(e)})
        if (i+1)%25==0: print(f'[STOP FOLLOW] {i+1}/{len(stopdf)}',flush=True)
    recdf=pd.DataFrame(rec)

    summ.to_csv(ROOT/f'{PREF}_ENTRY_STATE_SUMMARY.csv',index=False,encoding='utf-8-sig')
    trades.drop(columns=['entry_dt','exit_dt'],errors='ignore').to_csv(ROOT/f'{PREF}_TRADES_TAGGED.csv',index=False,encoding='utf-8-sig')
    events.to_csv(ROOT/f'{PREF}_EVENTS.csv',index=False,encoding='utf-8-sig')
    stopdf.to_csv(ROOT/f'{PREF}_STOPS_DURING_EVENT.csv',index=False,encoding='utf-8-sig')
    recdf.to_csv(ROOT/f'{PREF}_STOP_FOLLOWUP.csv',index=False,encoding='utf-8-sig')
    summary=[f'SOURCE={zp.name}',f'CURRENT_TRADES={len(trades)}',f'CRASH_THRESHOLD_BOTH_15M={TH}%',f'EVENTS={states.event_id.nunique() if len(states) else 0}','',summ.to_string(index=False),'',f'STOPS_EXITING_DURING_CRASH_EVENT={len(stopdf)}']
    (ROOT/f'{PREF}_SUMMARY.txt').write_text('\n'.join(summary)+'\n',encoding='utf-8')
    files=[ROOT/f'{PREF}_{x}' for x in ['ENTRY_STATE_SUMMARY.csv','TRADES_TAGGED.csv','EVENTS.csv','STOPS_DURING_EVENT.csv','STOP_FOLLOWUP.csv','SUMMARY.txt']]
    zpo=ROOT/f'{PREF}_RESULTS.zip'
    with zipfile.ZipFile(zpo,'w',zipfile.ZIP_DEFLATED) as z:
        for p in files: z.write(p,p.name)
    print('\n'+summ.to_string(index=False),flush=True)
    print(f'RESULT_ZIP={zpo}',flush=True)

if __name__=='__main__': main()
