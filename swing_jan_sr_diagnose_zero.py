#!/usr/bin/env python3
from pathlib import Path
import importlib.util, json, pandas as pd, numpy as np

ROOT=Path('/root/hyejin-trader')
SRC=ROOT/'swing_jan_sr_reset_v1.py'
CACHE=ROOT/'swing_2026_01_07_batch'/'202601'/'cache15'
OUT=ROOT/'bybit_swing'/'S_JAN_SR_ZERO_DIAG.txt'

spec=importlib.util.spec_from_file_location('janreset', SRC)
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
# Symbols known to have Jan trades/signals in the earlier actual-15m batch.
PREF=['POLYXUSDT','CHZUSDT','CLOUSDT','4USDT','SUSHIUSDT','1000BONKUSDT']
lines=[]
lines.append(f'source={SRC}')
lines.append(f'cache={CACHE}')
lines.append(f'version={m.VERSION}')
for sym in PREF:
    files=list(CACHE.glob(f'{sym}_15m_*.csv.gz'))
    if not files:
        lines.append(f'\n[{sym}] CACHE_MISSING'); continue
    d,a=m.read_cache(files[0])
    st=m.make_states(d)
    sig=m.make_signals(sym,d,st)
    lines.append(f'\n[{sym}] bars={len(d)} jan={a["january_bars"]} offgrid={a["off_grid_rows_dropped"]}')
    lines.append('states '+ ' '.join(f'{v}={len(st[v])}' for v in m.VARIANTS))
    lines.append('signals '+ ' '.join(f'{v}={int((sig.variant==v).sum()) if len(sig) else 0}' for v in m.VARIANTS))

    vals=d[m.OHLCV].to_numpy(); it=d.index
    for v in m.VARIANTS:
        counts=dict(adjacent=0,bull=0,state=0,touch=0,recover=0,inside=0)
        for i in range(1,len(d)):
            t=it[i]; dec=t+m.STEP
            if dec < m.START or dec >= m.END or t-it[i-1] != m.STEP: continue
            counts['adjacent']+=1
            op,hi,lo,cl=vals[i,:4]; prev=vals[i-1,3]
            if not (cl>op and cl>prev): continue
            counts['bull']+=1
            state=st[v].get(t.floor('h').value)
            if state is None: continue
            counts['state']+=1
            s1,stop,r1=state['s1'],state['stop'],state['r1']
            if not (lo <= s1*1.004): continue
            counts['touch']+=1
            if not (cl >= s1*.998): continue
            counts['recover']+=1
            if not (stop < cl < r1): continue
            counts['inside']+=1
        lines.append(f'gate {v}: '+ ' '.join(f'{k}={x}' for k,x in counts.items()))

# Global fast audit: number of files with any state/signal, capped enough to diagnose.
any_state=any_sig=checked=0
for p in sorted(CACHE.glob('*_15m_*.csv.gz')):
    sym=p.name.split('_15m_')[0]
    if sym=='BTCUSDT': continue
    d,a=m.read_cache(p)
    if a['january_bars']==0: continue
    checked+=1
    st=m.make_states(d)
    if any(len(st[v]) for v in m.VARIANTS): any_state+=1
    sg=m.make_signals(sym,d,st)
    if len(sg): any_sig+=1
    if checked>=40: break
lines.append(f'\n[first40] checked={checked} symbols_with_any_state={any_state} symbols_with_any_signal={any_sig}')
OUT.write_text('\n'.join(lines),encoding='utf-8')
print('\n'.join(lines))
print(f'\nWROTE {OUT}')
