#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FINAL P STOP RECOVERY GATE — 2026-10-05

Purpose
-------
Use the FINAL exact entry path (C1~C11 + C14 ABDE + C12 + C13) and ONLY its
1,547 STOP trades.  Test whether information visible at the STOP moment can
separate:
  - recovery-eligible STOPs (max-2 DCA recovery succeeds)
  - recovery-failure STOPs (better to keep the original STOP)
Then repeat the separation at the exact low+1.5% rebound trigger, before adding.

Frozen hypothetical recovery
----------------------------
1) Ignore the original STOP; original core 100% remains open.
2) From the NEXT 1m bar after STOP, track a new swing low.
3) When price rebounds +1.5% from that swing low, add 100% equal quantity.
4) New average = (original entry + add price) / 2.
5) Conservative same-bar handling after add: old swing-low rebreak wins over
   average recovery.
6) If old low rebreaks first, added 100% is removed at the old low; then search
   for a second low+1.5% rebound.
7) Maximum 2 add attempts.
8) Success = new average recovered within 24h after STOP.
9) Non-success within 24h is kept as a separate failure/unresolved category.

Anti-lookahead
--------------
- STOP-point features use bars COMPLETED strictly before the STOP minute.
- Rebound-point features use bars COMPLETED strictly before the rebound trigger
  minute.  Trigger-minute final close/volume is NOT used.
- Future bars are used only to assign the success/failure label.

Rule discovery split
--------------------
- DISCOVERY: 2026-01 ~ 2026-06
- VALIDATION: 2026-07 ~ 2026-08
- OOS: 2026-09

Safety
------
- No bot.py edit
- No DB write
- No order
- Public Bybit kline read only + local cache files
"""

from __future__ import annotations

from pathlib import Path
from collections import defaultdict, deque, Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import bisect
import csv
import gzip
import io
import json
import math
import os
import re
import runpy
import sqlite3
import statistics
import sys
import time
import zipfile

import numpy as np
import pandas as pd

ROOT = Path('/root/hyejin-trader/bybit_swing')
STAMP = '20261005'
OUTDIR = ROOT / f'FINAL_STOP_RECOVERY_GATE_{STAMP}'
OUTDIR.mkdir(exist_ok=True)
OUTZIP = ROOT / f'FINAL_STOP_RECOVERY_GATE_{STAMP}_RESULTS.zip'
CACHE = ROOT / '.final_stop_recovery_gate_cache'
CACHE.mkdir(exist_ok=True)
KST = timezone(timedelta(hours=9))
UTC = timezone.utc
REBOUND_PCT = 1.50
MAX_ATTEMPTS = 2
HORIZON_H = 24
LOG = []


def say(*a):
    s = ' '.join(str(x) for x in a)
    print(s, flush=True)
    LOG.append(s)


def fv(v, d=None):
    try:
        if v is None or str(v).strip() == '':
            return d
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return d
        return x
    except Exception:
        return d


def dt_utc(v):
    if v is None or str(v).strip() == '':
        return None
    try:
        t = pd.Timestamp(v)
        if t.tzinfo is None:
            t = t.tz_localize(UTC)
        else:
            t = t.tz_convert(UTC)
        return t.to_pydatetime()
    except Exception:
        return None


def dt_kst_text(t):
    if t is None:
        return ''
    return t.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S')


def pct(a, b):
    return (a / b - 1.0) * 100.0 if b not in (None, 0) else None


def mean(xs):
    z = [float(x) for x in xs if x is not None and math.isfinite(float(x))]
    return sum(z) / len(z) if z else None


def ema(vals, n):
    z = [float(x) for x in vals if x is not None]
    if not z:
        return None
    alpha = 2.0 / (n + 1.0)
    e = z[0]
    for x in z[1:]:
        e = alpha * x + (1.0 - alpha) * e
    return e


def rsi(vals, n=14):
    if len(vals) < n + 1:
        return None
    gains, losses = [], []
    for a, b in zip(vals[-(n+1):-1], vals[-n:]):
        d = b - a
        gains.append(max(d, 0.0))
        losses.append(max(-d, 0.0))
    ag = sum(gains) / n
    al = sum(losses) / n
    if al == 0:
        return 100.0
    rs = ag / al
    return 100.0 - (100.0 / (1.0 + rs))


def close_pos(b):
    if not b or b['h'] <= b['l']:
        return None
    return (b['c'] - b['l']) / (b['h'] - b['l'])


def safe(v, nd=6):
    x = fv(v)
    return '' if x is None else round(x, nd)


def write_csv(path: Path, rows):
    if isinstance(rows, pd.DataFrame):
        rows.to_csv(path, index=False)
        return
    rows = list(rows)
    if not rows:
        path.write_text('', encoding='utf-8-sig')
        return
    keys, seen = [], set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with path.open('w', newline='', encoding='utf-8-sig') as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
        w.writeheader(); w.writerows(rows)


def split_name(month):
    m = str(month)
    if m <= '2026-06':
        return 'DISCOVERY_0106'
    if m <= '2026-08':
        return 'VALIDATION_0708'
    return 'OOS_09'


# ---------------------------------------------------------------------------
# 1) Load validated engine and FINAL trade ledger
# ---------------------------------------------------------------------------
BASE = ROOT / 'breadth_persistence_diag.py'
if not BASE.exists():
    BASE = Path('/tmp/breadth_persistence_diag.py')
if not BASE.exists():
    raise SystemExit('MISSING breadth_persistence_diag.py')

say('=== LOAD VALIDATED ENGINE ===')
NS = runpy.run_path(str(BASE))
U = NS['U']
ja = NS['ja'].copy()
se = NS['se'].copy()
getsim = NS['getsim']
se_actual = NS.get('se_actual', pd.DataFrame())

# use a dedicated persistent cache for this analysis
U.CACHE_DIR = CACHE
U.CACHE_DIR.mkdir(exist_ok=True)
U.KC = U.KlineCache()

rz = sorted(ROOT.glob('C14_SEP_C12_C13_EXACT_*_RESULTS.zip'), key=lambda p:p.stat().st_mtime, reverse=True)
if not rz:
    raise SystemExit('MISSING C14_SEP_C12_C13_EXACT_*_RESULTS.zip')
FINALZIP = rz[0]
say('FINAL LEDGER ZIP', FINALZIP)
with zipfile.ZipFile(FINALZIP) as z:
    if 'TRADES_C14_C12_C13.csv' not in z.namelist():
        raise SystemExit('FINAL ZIP missing TRADES_C14_C12_C13.csv')
    final = pd.read_csv(z.open('TRADES_C14_C12_C13.csv'), low_memory=False)

final['entry_dt'] = pd.to_datetime(final['entry_dt'], utc=True, errors='coerce')
final_stops = final[final.result.astype(str) == 'STOP'].copy().reset_index(drop=True)
say('FINAL TRADES', len(final), 'FINAL STOPS', len(final_stops), 'EXPECTED 1547')
if len(final_stops) != 1547:
    raise SystemExit(f'FINAL STOP COUNT AUDIT FAIL: {len(final_stops)} != 1547')

ja_idx = {str(r['setup_id']): r for _, r in ja.iterrows()}
se_idx = {str(r['setup_id']): r for _, r in se.iterrows()}

# ---------------------------------------------------------------------------
# 2) Detailed Jan-Aug sim source (terminal_price / stop_stage)
# ---------------------------------------------------------------------------
packs = sorted(ROOT.glob('P_RESEARCH_PACK_*.zip'), key=lambda p:p.stat().st_mtime, reverse=True)
if not packs:
    raise SystemExit('MISSING P_RESEARCH_PACK_*.zip')
PACK = packs[0]
say('RESEARCH PACK', PACK)
with zipfile.ZipFile(PACK) as z:
    simdf = pd.read_csv(gzip.GzipFile(fileobj=io.BytesIO(z.read('JAN_AUG_SIM_RESULTS.csv.gz'))), low_memory=False)

sim_idx = {str(r['setup_id']): r for _, r in simdf.iterrows()}
se_actual_idx = {str(r['setup_id']): r for _, r in se_actual.iterrows()} if len(se_actual) else {}

# ---------------------------------------------------------------------------
# 3) Recover exact base trade metadata for final path
# ---------------------------------------------------------------------------
@dataclass
class BaseDetail:
    setup_id: str
    symbol: str
    period: str
    entry: datetime
    entry_price: float
    result: str
    exit_time: datetime
    terminal_price: float
    net_pct: float
    mfe_pct: float | None
    mae_pct: float | None
    stop_stage: str


def candidate_for(sid, period):
    return ja_idx.get(sid) if period == 'JA' else se_idx.get(sid)


def simulate_detail(sid, period, r):
    entry = pd.Timestamp(r['_dt']).to_pydatetime() if period == 'JA' else r['_dt']
    s = dict(setup_id=sid, symbol=str(r['symbol']), entry=entry, entry_price=float(r['entry_price']))
    sim = U.simulate_base(s)
    return BaseDetail(
        setup_id=sid, symbol=str(r['symbol']), period=period, entry=entry,
        entry_price=float(r['entry_price']), result=str(sim.result), exit_time=sim.exit_time,
        terminal_price=float(sim.terminal_price), net_pct=float(sim.net_pct),
        mfe_pct=fv(sim.mfe_pct), mae_pct=fv(sim.mae_pct), stop_stage=str(sim.stop_stage or '')
    )


def detail_for_final_trade(tr):
    sid = str(tr['setup_id']); period = str(tr['period'])
    r = candidate_for(sid, period)
    if r is None:
        raise RuntimeError('candidate missing '+sid)
    entry = pd.Timestamp(r['_dt']).to_pydatetime() if period == 'JA' else r['_dt']
    ep = float(r['entry_price'])

    if period == 'JA':
        q = sim_idx.get(sid)
        if q is not None and str(q.get('result')) == str(tr['result']):
            et = dt_utc(q.get('exit_time_utc'))
            if et is not None:
                return BaseDetail(
                    sid, str(r['symbol']), period, entry, ep, str(q.get('result')), et,
                    float(q.get('terminal_price')), float(q.get('net_pct')),
                    fv(q.get('mfe_pct')), fv(q.get('mae_pct')), str(q.get('stop_stage') or '')
                )
    else:
        q = se_actual_idx.get(sid)
        # Sep source rows do not retain terminal price reliably. Re-simulate STOPs;
        # for non-STOP final-shadow events getsim is enough below.
        if str(tr['result']) != 'STOP' and q is not None:
            et = dt_utc(q.get('exit_time_kst'))
            if et is not None:
                return BaseDetail(
                    sid, str(r['symbol']), period, entry, ep, str(q.get('result')), et,
                    ep, float(q.get('net_pct')), fv(q.get('mfe_pct')), fv(q.get('mae_pct')),
                    str(q.get('stop_stage') or '')
                )

    return simulate_detail(sid, period, r)


# Build final-path exit event stream for STOP-time rolling performance context.
say('=== BUILD FINAL EXIT STREAM ===')
exit_events = []
exit_errors = []
for n, tr in enumerate(final.to_dict('records'), 1):
    sid = str(tr['setup_id']); period = str(tr['period']); r = candidate_for(sid, period)
    try:
        if r is None:
            raise RuntimeError('candidate missing')
        if period == 'JA':
            q = sim_idx.get(sid)
            et = dt_utc(q.get('exit_time_utc')) if q is not None else None
        else:
            # exact getsim returns exit_time even for scheduler replacement trades
            s = getsim(r, period)
            et = s.exit_time
        if et is None:
            raise RuntimeError('exit time missing')
        exit_events.append(dict(
            setup_id=sid, symbol=str(tr['symbol']), exit_time=et,
            net_pct=float(tr['net_pct']), result=str(tr['result'])
        ))
    except Exception as e:
        exit_errors.append((sid, str(e)))
    if n % 500 == 0 or n == len(final):
        say('EXIT STREAM', n, '/', len(final), 'ok', len(exit_events), 'err', len(exit_errors))

if len(exit_events) < len(final) * 0.98:
    raise SystemExit(f'EXIT STREAM COVERAGE TOO LOW {len(exit_events)}/{len(final)}')
exit_events.sort(key=lambda x: (x['exit_time'], x['setup_id']))
exit_times = [e['exit_time'].timestamp() for e in exit_events]


def recent_exit_features(t: datetime):
    j = bisect.bisect_left(exit_times, t.timestamp())
    prior = exit_events[:j]
    last9 = prior[-9:]
    out = {
        'shadow9_n': len(last9),
        'shadow9_sum': sum(x['net_pct'] for x in last9) if last9 else None,
        'shadow9_stop_n': sum(1 for x in last9 if x['result'] == 'STOP'),
    }
    for h in (1, 2, 6, 12, 24):
        cut = t.timestamp() - h * 3600
        k = bisect.bisect_left(exit_times, cut, 0, j)
        z = exit_events[k:j]
        out[f'exit_n_{h}h'] = len(z)
        out[f'exit_net_{h}h'] = sum(x['net_pct'] for x in z) if z else 0.0
        out[f'exit_stop_n_{h}h'] = sum(1 for x in z if x['result'] == 'STOP')
    return out

# ---------------------------------------------------------------------------
# 4) Candidate activity at arbitrary checkpoint (market participation proxy)
# ---------------------------------------------------------------------------
cand = []
for period, d in [('JA', ja), ('SE', se)]:
    for _, r in d.iterrows():
        try:
            t = pd.Timestamp(r['_dt']).to_pydatetime() if period == 'JA' else r['_dt']
            cand.append((t.timestamp(), str(r['symbol'])))
        except Exception:
            pass
cand.sort()
cand_times = [x[0] for x in cand]


def cand_window(t: datetime, h: int):
    b = t.timestamp(); a = b - h * 3600
    i = bisect.bisect_left(cand_times, a); j = bisect.bisect_right(cand_times, b)
    z = cand[i:j]
    return len(z), len(set(sym for _, sym in z))


def candidate_features(t: datetime):
    out = {}
    for h in (1, 2, 6, 12, 24):
        n, br = cand_window(t, h)
        out[f'v25_count_{h}h'] = n
        out[f'v25_breadth_{h}h'] = br
    # non-overlapping previous-window delta
    for h in (2, 6, 12, 24):
        now_n, now_b = cand_window(t, h)
        prev_t = t - timedelta(hours=h)
        prev_n, prev_b = cand_window(prev_t, h)
        out[f'v25_count_delta_{h}h'] = now_n - prev_n
        out[f'v25_breadth_delta_{h}h'] = now_b - prev_b
    return out

# ---------------------------------------------------------------------------
# 5) Price feature helpers
# ---------------------------------------------------------------------------
def to_bars(df):
    out=[]
    if df is None or len(df)==0:
        return out
    for r in df.itertuples(index=False):
        out.append({'ts': pd.Timestamp(r.ts).to_pydatetime().astimezone(UTC),
                    'o':float(r.open),'h':float(r.high),'l':float(r.low),'c':float(r.close),'v':float(r.volume)})
    return out


def five_bars(one):
    buckets = {}
    for b in one:
        t = b['ts'].replace(minute=(b['ts'].minute//5)*5, second=0, microsecond=0)
        x = buckets.get(t)
        if x is None:
            buckets[t] = {'ts':t,'o':b['o'],'h':b['h'],'l':b['l'],'c':b['c'],'v':b['v'],'n':1}
        else:
            x['h']=max(x['h'],b['h']); x['l']=min(x['l'],b['l']); x['c']=b['c']; x['v']+=b['v']; x['n']+=1
    return [buckets[k] for k in sorted(buckets)]


def feature_pack(one, checkpoint, entry_price=None, entry_ts=None):
    cp = checkpoint.astimezone(UTC).replace(second=0,microsecond=0)
    prev1 = [b for b in one if b['ts'] < cp]
    five = five_bars(one)
    prev5 = [b for b in five if b['ts'] + timedelta(minutes=5) <= cp]
    p1 = prev1[-1] if prev1 else None
    p3 = prev1[-3:] if len(prev1)>=3 else prev1
    p5m = prev5[-1] if prev5 else None
    vol10 = [b['v'] for b in prev1[-11:-1]] if len(prev1)>=2 else []
    m10 = mean(vol10)
    vol5 = [b['v'] for b in prev5[-6:-1]] if len(prev5)>=2 else []
    m5 = mean(vol5)
    closes5 = [b['c'] for b in prev5]
    e9 = ema(closes5[-60:],9) if closes5 else None
    e20 = ema(closes5[-80:],20) if closes5 else None
    e20p = ema(closes5[-81:-1],20) if len(closes5)>=2 else None
    out = {
        'prev1m_ret': pct(p1['c'],p1['o']) if p1 else None,
        'prev3m_ret': pct(p3[-1]['c'],p3[0]['o']) if p3 else None,
        'prev1m_close_pos': close_pos(p1),
        'prev1m_vol_ratio10': (p1['v']/m10) if p1 and m10 not in (None,0) else None,
        'last5m_ret': pct(p5m['c'],p5m['o']) if p5m else None,
        'last5m_close_pos': close_pos(p5m),
        'last5m_vol_ratio5': (p5m['v']/m5) if p5m and m5 not in (None,0) else None,
        'ema9_20_gap': (e9/e20-1)*100 if e9 and e20 else None,
        'ema20_slope': (e20/e20p-1)*100 if e20 and e20p else None,
        'rsi14_5m': rsi(closes5,14),
    }
    # realized recent move based on completed 1m closes
    for m in (5,10,15,30,60,120):
        if prev1:
            cur = prev1[-1]['c']; target = cp - timedelta(minutes=m); old=None
            for b in reversed(prev1):
                if b['ts'] <= target:
                    old=b['c']; break
            out[f'ret_{m}m'] = pct(cur,old) if old else None
        else:
            out[f'ret_{m}m'] = None
    if entry_price and entry_ts:
        ep = entry_ts.astimezone(UTC).replace(second=0,microsecond=0)
        path = [b for b in one if ep <= b['ts'] < cp]
        if path:
            out['mfe_to_cp'] = pct(max(b['h'] for b in path),entry_price)
            out['mae_to_cp'] = pct(min(b['l'] for b in path),entry_price)
            out['low_break_count_30m'] = sum(1 for k in range(max(1,len(path)-30),len(path)) if path[k]['l'] < min(x['l'] for x in path[max(0,k-10):k]))
        else:
            out['mfe_to_cp']=None; out['mae_to_cp']=None; out['low_break_count_30m']=None
    return out

# short BTC/ETH day cache
market_1m_cache = {}

def market_1m(sym, t):
    d0 = t.astimezone(UTC).replace(hour=0,minute=0,second=0,microsecond=0)
    key=(sym,d0)
    if key not in market_1m_cache:
        df=U.KC.get(sym,'1m',d0-timedelta(hours=6),d0+timedelta(hours=26))
        market_1m_cache[key]=to_bars(df)
    return market_1m_cache[key]


def market_short(sym, t, minutes):
    one=market_1m(sym,t); cp=t.astimezone(UTC).replace(second=0,microsecond=0)
    prev=[b for b in one if b['ts']<cp]
    if not prev: return None
    cur=prev[-1]['c']; target=cp-timedelta(minutes=minutes); old=None
    for b in reversed(prev):
        if b['ts']<=target:
            old=b['c']; break
    return pct(cur,old) if old else None

say('=== LOAD BTC/ETH 1H MARKET SERIES ===')
MARKET_START=datetime(2025,11,25,tzinfo=UTC)
MARKET_END=datetime(2026,10,2,tzinfo=UTC)
market_hour={}
for sym in ('BTCUSDT','ETHUSDT'):
    d=U.KC.get(sym,'60',MARKET_START,MARKET_END).copy()
    d['ts']=pd.to_datetime(d['ts'],utc=True)
    d=d.sort_values('ts').drop_duplicates('ts').reset_index(drop=True)
    market_hour[sym]=d
    say(sym,'1H rows',len(d),'range',d.ts.min(),d.ts.max())


def hourly_change(sym,t,hours):
    d=market_hour[sym]; ts=pd.Timestamp(t).tz_convert(UTC) if pd.Timestamp(t).tzinfo else pd.Timestamp(t,tz=UTC)
    q=d[d.ts<ts]
    if len(q)==0:return None
    cur=float(q.iloc[-1].close); target=ts-pd.Timedelta(hours=hours)
    p=q[q.ts<=target]
    if len(p)==0:return None
    return pct(cur,float(p.iloc[-1].close))


def hourly_pos(sym,t,hours):
    d=market_hour[sym]; ts=pd.Timestamp(t).tz_convert(UTC) if pd.Timestamp(t).tzinfo else pd.Timestamp(t,tz=UTC)
    q=d[(d.ts<ts)&(d.ts>=ts-pd.Timedelta(hours=hours))]
    if len(q)<2:return None
    lo=float(q.low.min()); hi=float(q.high.max()); cur=float(q.iloc[-1].close)
    return (cur-lo)/(hi-lo) if hi>lo else None


def market_features(t):
    out={}
    for sym,pfx in [('BTCUSDT','btc'),('ETHUSDT','eth')]:
        for m in (5,15,30,60,240):
            out[f'{pfx}_{m}m'] = market_short(sym,t,m)
        for h,name in [(24,'24h'),(48,'48h'),(168,'7d'),(720,'30d')]:
            out[f'{pfx}_{name}'] = hourly_change(sym,t,h)
        out[f'{pfx}_pos24']=hourly_pos(sym,t,24)
    for name in ('15m','60m','240m','24h','48h','7d','30d'):
        a=out.get('btc_'+name); b=out.get('eth_'+name)
        out[f'btc_eth_spread_{name}'] = (a-b) if a is not None and b is not None else None
        out[f'btc_eth_absavg_{name}'] = (abs(a)+abs(b))/2 if a is not None and b is not None else None
        out[f'btc_eth_both_down_{name}'] = int(a<0 and b<0) if a is not None and b is not None else None
    return out

# ---------------------------------------------------------------------------
# 6) Optional actual alt UP24 breadth from existing 60m caches
# ---------------------------------------------------------------------------
def build_up24_breadth_optional():
    patterns=[
        '.janaug_hist_cache_v1/*_60_*.csv.gz',
        'JANAUG_FIXED_2026_WORK/*_60_*.csv.gz',
        '.*/*_60_*.csv.gz',
    ]
    files=[]
    seen=set()
    for pat in patterns:
        for p in ROOT.glob(pat):
            rp=str(p.resolve())
            if rp not in seen:
                seen.add(rp); files.append(p)
    rx=re.compile(r'^(?P<sym>.+)_60_(?P<s>\d{9,10})_(?P<e>\d{9,10})\.csv\.gz$')
    groups=defaultdict(list)
    for p in files:
        m=rx.match(p.name)
        if not m: continue
        sym=m.group('sym')
        if sym in ('BTCUSDT','ETHUSDT'): continue
        groups[sym].append(p)
    if len(groups)<20:
        say('UP24 CACHE unavailable/too small; symbols',len(groups))
        return pd.DataFrame(columns=['ts','up24','up24_n'])
    say('UP24 CACHE symbols',len(groups),'files',sum(len(v) for v in groups.values()))
    agg=defaultdict(lambda:[0,0])
    for i,(sym,ps) in enumerate(groups.items(),1):
        parts=[]
        for p in ps:
            try:
                d=pd.read_csv(p,compression='gzip',usecols=lambda c:c in ('ts','close','timestamp','time'),low_memory=False)
                tc='ts' if 'ts' in d.columns else ('timestamp' if 'timestamp' in d.columns else ('time' if 'time' in d.columns else None))
                if tc is None or 'close' not in d.columns: continue
                q=pd.DataFrame({'ts':pd.to_datetime(d[tc],utc=True,errors='coerce'),'close':pd.to_numeric(d.close,errors='coerce')}).dropna()
                parts.append(q)
            except Exception:
                pass
        if not parts: continue
        d=pd.concat(parts,ignore_index=True).drop_duplicates('ts').sort_values('ts')
        d['r24']=d.close/d.close.shift(24)-1
        for r in d.dropna(subset=['r24']).itertuples(index=False):
            hr=pd.Timestamp(r.ts).floor('h')
            a=agg[hr]; a[1]+=1; a[0]+=int(float(r.r24)>0)
        if i%50==0: say('UP24',i,'/',len(groups))
    rows=[{'ts':k,'up24':100*v[0]/v[1] if v[1] else np.nan,'up24_n':v[1]} for k,v in agg.items()]
    out=pd.DataFrame(rows).sort_values('ts').reset_index(drop=True) if rows else pd.DataFrame(columns=['ts','up24','up24_n'])
    if len(out):
        out['up24_delta6']=out.up24-out.up24.shift(6)
        out['up24_delta12']=out.up24-out.up24.shift(12)
        out['up24_delta24']=out.up24-out.up24.shift(24)
        say('UP24 rows',len(out),'range',out.ts.min(),out.ts.max(),'median_n',round(float(out.up24_n.median()),1))
    return out

UP24 = build_up24_breadth_optional()

def up24_features(t):
    if len(UP24)==0:return {}
    ts=pd.Timestamp(t).tz_convert(UTC) if pd.Timestamp(t).tzinfo else pd.Timestamp(t,tz=UTC)
    q=UP24[UP24.ts<ts]
    if len(q)==0:return {}
    r=q.iloc[-1]
    return {k:fv(r.get(k)) for k in ['up24','up24_n','up24_delta6','up24_delta12','up24_delta24']}

# ---------------------------------------------------------------------------
# 7) MAX2 recovery simulation + exact attempt checkpoints
# ---------------------------------------------------------------------------
def simulate_max2(one, stop_ts, entry, initial_low):
    floor=stop_ts.astimezone(UTC).replace(second=0,microsecond=0)
    end=floor+timedelta(hours=HORIZON_H)
    post=[b for b in one if floor+timedelta(minutes=1)<=b['ts']<=end]
    if not post:
        return {'category':'NO_DATA','success24':0,'attempts':0}
    swing_low=float(initial_low if initial_low and initial_low>0 else entry)
    low_ts=floor
    attempts=[]
    for attempt_no in (1,2):
        trigger=None; add_price=None; this_low=swing_low; this_low_ts=low_ts
        # find next rebound from continuously updated low
        for b in post:
            if b['ts']<=low_ts: continue
            if b['l']<swing_low:
                swing_low=b['l']; low_ts=b['ts']; this_low=swing_low; this_low_ts=low_ts
                continue
            trg=swing_low*(1+REBOUND_PCT/100)
            if b['ts']>low_ts and b['h']>=trg:
                trigger=b['ts']; add_price=trg; this_low=swing_low; this_low_ts=low_ts
                break
        if trigger is None:
            cat='NO_REBOUND' if attempt_no==1 else 'FAIL1_NO_SECOND_REBOUND'
            return {'category':cat,'success24':0,'attempts':attempt_no-1,
                    'last_low':swing_low,'last_low_ts':low_ts,'attempt_rows':attempts}
        avg=(entry+add_price)/2.0
        rec=None; brk=None; worst=this_low
        for b in post:
            if b['ts']<=trigger: continue
            worst=min(worst,b['l'])
            # adverse first
            if b['l']<=this_low:
                brk=b['ts']; break
            if b['h']>=avg:
                rec=b['ts']; break
        attempts.append({
            'attempt':attempt_no,'low':this_low,'low_ts':this_low_ts,
            'trigger_ts':trigger,'add_price':add_price,'avg':avg,
            'recover_ts':rec,'false_break_ts':brk,'worst_after_add':worst,
            'stop_to_low_min':(this_low_ts-floor).total_seconds()/60,
            'low_to_trigger_min':(trigger-this_low_ts).total_seconds()/60,
            'stop_to_trigger_min':(trigger-floor).total_seconds()/60,
            'trigger_to_recover_min':((rec-trigger).total_seconds()/60 if rec else None),
        })
        if rec is not None:
            cat='SUCCESS_1' if attempt_no==1 else 'SUCCESS_2'
            return {'category':cat,'success24':1,'attempts':attempt_no,
                    'success_attempt':attempt_no,'attempt_rows':attempts,
                    'recover_ts':rec,'recover_from_stop_min':(rec-floor).total_seconds()/60,
                    'max_additional_drawdown_pct':pct(min(x['worst_after_add'] for x in attempts), entry)}
        if brk is None:
            cat='UNRESOLVED_AFTER_ADD1' if attempt_no==1 else 'UNRESOLVED_AFTER_ADD2'
            return {'category':cat,'success24':0,'attempts':attempt_no,'attempt_rows':attempts,
                    'max_additional_drawdown_pct':pct(worst,entry)}
        # false break: continue from the broken/updated low after trigger
        # restrict future scan to bars after break to avoid reusing prior bars
        post=[b for b in post if b['ts']>=brk]
        swing_low=min(b['l'] for b in post[:1]) if post else this_low
        low_ts=brk
    return {'category':'FAIL_2','success24':0,'attempts':2,'attempt_rows':attempts,
            'max_additional_drawdown_pct':pct(min(x['worst_after_add'] for x in attempts),entry)}

# ---------------------------------------------------------------------------
# 8) Process final 1,547 STOPs
# ---------------------------------------------------------------------------
rows=[]; errors=[]
say('=== FINAL STOP RECOVERY LABEL + FEATURES ===')
for n,tr in enumerate(final_stops.to_dict('records'),1):
    sid=str(tr['setup_id']); period=str(tr['period'])
    try:
        bd=detail_for_final_trade(tr)
        if bd.result!='STOP':
            raise RuntimeError('base detail not STOP: '+bd.result)
        # verify final ledger net reasonably matches base sim net
        if abs(float(tr['net_pct'])-bd.net_pct)>0.15:
            # re-simulate once, because some old cached rows may have legacy detail
            r=candidate_for(sid,period); bd=simulate_detail(sid,period,r)
        start=bd.exit_time-timedelta(hours=3)
        end=bd.exit_time+timedelta(hours=HORIZON_H,minutes=3)
        df=U.KC.get(bd.symbol,'1m',start,end)
        one=to_bars(df)
        if len(one)<30:
            raise RuntimeError('too few symbol 1m bars')
        rec=simulate_max2(one,bd.exit_time,bd.entry_price,bd.terminal_price)

        row={
            'setup_id':sid,'symbol':bd.symbol,'period':period,'month':str(tr['month']),
            'split':split_name(tr['month']),'entry_time_utc':bd.entry.isoformat(),
            'entry_time_kst':dt_kst_text(bd.entry),'stop_time_utc':bd.exit_time.isoformat(),
            'stop_time_kst':dt_kst_text(bd.exit_time),'entry_price':bd.entry_price,
            'terminal_price':bd.terminal_price,'base_net_pct':float(tr['net_pct']),
            'base_mfe_pct':bd.mfe_pct,'base_mae_pct':bd.mae_pct,'stop_stage':bd.stop_stage,
            'entry_to_stop_min':(bd.exit_time-bd.entry).total_seconds()/60,
            'recovery_category':rec.get('category'),'recovery_success24':int(rec.get('success24',0)),
            'recovery_attempts':int(rec.get('attempts',0) or 0),
            'success_attempt':rec.get('success_attempt',''),
            'recover_from_stop_min':rec.get('recover_from_stop_min',''),
            'max_additional_drawdown_pct':rec.get('max_additional_drawdown_pct',''),
        }
        sf=feature_pack(one,bd.exit_time,bd.entry_price,bd.entry)
        for k,v in sf.items():row['STOP_'+k]=safe(v)
        for k,v in market_features(bd.exit_time).items():row['STOP_'+k]=safe(v)
        for k,v in candidate_features(bd.exit_time).items():row['STOP_'+k]=safe(v)
        for k,v in recent_exit_features(bd.exit_time).items():row['STOP_'+k]=safe(v)
        for k,v in up24_features(bd.exit_time).items():row['STOP_'+k]=safe(v)

        attempts=rec.get('attempt_rows') or []
        for a in attempts:
            j=int(a['attempt'])
            for k in ['low','add_price','avg','stop_to_low_min','low_to_trigger_min','stop_to_trigger_min','trigger_to_recover_min']:
                row[f'A{j}_{k}']=safe(a.get(k))
            row[f'A{j}_trigger_time_kst']=dt_kst_text(a.get('trigger_ts'))
            row[f'A{j}_recover_time_kst']=dt_kst_text(a.get('recover_ts'))
            row[f'A{j}_false_break_time_kst']=dt_kst_text(a.get('false_break_ts'))
            row[f'A{j}_success']=int(a.get('recover_ts') is not None)
            row[f'A{j}_false_break']=int(a.get('false_break_ts') is not None)
            if a.get('trigger_ts') is not None:
                tf=a['trigger_ts']
                rf=feature_pack(one,tf,bd.entry_price,bd.entry)
                for k,v in rf.items():row[f'RB{j}_'+k]=safe(v)
                for k,v in market_features(tf).items():row[f'RB{j}_'+k]=safe(v)
                for k,v in candidate_features(tf).items():row[f'RB{j}_'+k]=safe(v)
                for k,v in recent_exit_features(tf).items():row[f'RB{j}_'+k]=safe(v)
                for k,v in up24_features(tf).items():row[f'RB{j}_'+k]=safe(v)
                row[f'RB{j}_swing_low_pct']=safe(pct(a.get('low'),bd.entry_price))
                row[f'RB{j}_add_price_pct']=safe(pct(a.get('add_price'),bd.entry_price))
                lt=fv(a.get('low_to_trigger_min'))
                row[f'RB{j}_rebound_speed_pct_per_min']=safe(REBOUND_PCT/lt if lt not in (None,0) else None)
        # explicit first-rebound label: was first +1.5% rebound a true avg recovery?
        row['RB1_true_recovery']=int(bool(attempts and attempts[0].get('recover_ts') is not None)) if attempts else ''
        rows.append(row)
    except Exception as e:
        errors.append({'setup_id':sid,'period':period,'symbol':tr.get('symbol'),'error':repr(e)})
        say('ERR',sid,tr.get('symbol'),repr(e))
    if n%25==0 or n==len(final_stops):
        say('STOP',n,'/',len(final_stops),'ok',len(rows),'err',len(errors))

D=pd.DataFrame(rows)
if len(D)<1500:
    raise SystemExit(f'RECOVERY FEATURE COVERAGE TOO LOW {len(D)}/1547')
D.to_csv(OUTDIR/'FINAL_STOP_RECOVERY_DETAIL.csv',index=False)
if errors: pd.DataFrame(errors).to_csv(OUTDIR/'ERRORS.csv',index=False)

# ---------------------------------------------------------------------------
# 9) Descriptive tables
# ---------------------------------------------------------------------------
def grp_table(col):
    out=[]
    for key,g in D.groupby(col,dropna=False):
        out.append({col:key,'N':len(g),'SUCCESS':int(g.recovery_success24.sum()),
                    'FAIL':int(len(g)-g.recovery_success24.sum()),
                    'SUCCESS_RATE':float(g.recovery_success24.mean()),
                    'BASE_STOP_NET':float(g.base_net_pct.sum())})
    return pd.DataFrame(out).sort_values('N',ascending=False)

grp_table('month').to_csv(OUTDIR/'RECOVERY_BY_MONTH.csv',index=False)
grp_table('stop_stage').to_csv(OUTDIR/'RECOVERY_BY_STOP_STAGE.csv',index=False)
grp_table('recovery_category').to_csv(OUTDIR/'RECOVERY_CATEGORIES.csv',index=False)

# priority families used in current research

def stop_family_row(r):
    s=str(r.get('stop_stage') or '')
    ep=fv(r.get('entry_price')); tp=fv(r.get('terminal_price'))
    # Direct -3% disaster often has blank stop_stage in the saved SimResult,
    # so identify it by the actual terminal price as well.
    if ep and tp and tp <= ep*0.9705:
        return 'HARD_-3'
    if 'STOP15_FULL' in s:return 'STOP15_FULL'
    if 'P_CATASTROPHIC' in s:return 'V27_P_CATA'
    if s.startswith('STOP') or 'STOP30' in s:return 'FINAL4_OTHER'
    if s.startswith('V271_STAGE1_'):return 'V27_OTHER'
    return 'OTHER'
D['stop_family']=D.apply(stop_family_row,axis=1)
grp_table('stop_family').to_csv(OUTDIR/'RECOVERY_BY_STOP_FAMILY.csv',index=False)
D.to_csv(OUTDIR/'FINAL_STOP_RECOVERY_DETAIL.csv',index=False)

# ---------------------------------------------------------------------------
# 10) Stable rule search: single thresholds + top pair AND rules
# ---------------------------------------------------------------------------
IGNORE_PREFIX=('A1_','A2_','RB2_')
STOP_FEATURES=[]
for c in D.columns:
    if not c.startswith('STOP_'): continue
    x=pd.to_numeric(D[c],errors='coerce')
    if x.notna().sum()>=max(100,int(len(D)*0.50)) and x.nunique(dropna=True)>=6:
        STOP_FEATURES.append(c)
RB1_FEATURES=[]
for c in D.columns:
    if not c.startswith('RB1_'): continue
    if c in ('RB1_true_recovery',): continue
    x=pd.to_numeric(D[c],errors='coerce')
    if x.notna().sum()>=100 and x.nunique(dropna=True)>=6:
        RB1_FEATURES.append(c)


def eval_cond(df, conds, target):
    q=df.copy()
    mask=pd.Series(True,index=q.index)
    for feat,op,th in conds:
        x=pd.to_numeric(q[feat],errors='coerce')
        mask &= x.notna() & ((x>=th) if op=='>=' else (x<=th))
    usable=q.copy()
    # base target includes all rows with target observed; all rows have stop target
    if target not in q.columns:return None
    y=pd.to_numeric(q[target],errors='coerce')
    valid=y.notna()
    q=q[valid]; y=y[valid]; mask=mask[valid]
    n=len(q); hit=int(mask.sum())
    if n==0:return None
    base=float(y.mean())
    if hit==0:return dict(n=n,hit=0,base=base,precision=np.nan,keep=0.0,other_block=0.0)
    yy=y[mask]
    precision=float(yy.mean())
    total_pos=float(y.sum()); hit_pos=float(yy.sum()); total_neg=n-total_pos; hit_neg=hit-hit_pos
    keep=hit_pos/total_pos if total_pos else 0.0
    other_block=(total_neg-hit_neg)/total_neg if total_neg else 0.0
    return dict(n=n,hit=hit,base=base,precision=precision,keep=keep,other_block=other_block)


def split_frames(df):
    return {
        'DISC':df[df.split=='DISCOVERY_0106'],
        'VAL':df[df.split=='VALIDATION_0708'],
        'OOS':df[df.split=='OOS_09'],
    }


def quantiles(xs):
    x=np.array(sorted(set(float(v) for v in xs if pd.notna(v))),dtype=float)
    if len(x)<6:return []
    return sorted(set(float(np.quantile(x,q)) for q in (.10,.20,.30,.40,.50,.60,.70,.80,.90)))


def search_single(df,features,target,label):
    sp=split_frames(df); disc=sp['DISC']; out=[]
    for feat in features:
        vals=pd.to_numeric(disc[feat],errors='coerce').dropna().values
        for th in quantiles(vals):
            for op in ('>=','<='):
                cond=[(feat,op,th)]
                ed=eval_cond(sp['DISC'],cond,target); ev=eval_cond(sp['VAL'],cond,target); eo=eval_cond(sp['OOS'],cond,target)
                if not ed or ed['hit']<30:continue
                # target precision improvement over each split's own baseline
                stable_val=bool(ev and ev['hit']>=10 and ev['precision']>=ev['base'])
                stable_oos=bool(eo and eo['hit']>=8 and eo['precision']>=eo['base'])
                lift=ed['precision']-ed['base']
                score=lift + 0.35*ed['keep'] + (0 if not ev or math.isnan(ev['precision']) else ev['precision']-ev['base']) + (0 if not eo or math.isnan(eo['precision']) else eo['precision']-eo['base'])
                out.append({
                    'checkpoint':label,'target':target,'feature':feat,'op':op,'threshold':th,
                    'stable_val':int(stable_val),'stable_oos':int(stable_oos),'stable_both':int(stable_val and stable_oos),'score':score,
                    **{f'DISC_{k}':v for k,v in ed.items()},
                    **{f'VAL_{k}':v for k,v in (ev or {}).items()},
                    **{f'OOS_{k}':v for k,v in (eo or {}).items()},
                })
    return pd.DataFrame(out).sort_values(['stable_both','stable_val','stable_oos','score'],ascending=False) if out else pd.DataFrame()


def search_pairs(df,singles,target,label):
    if len(singles)==0:return pd.DataFrame()
    top=singles.head(16).to_dict('records')
    sp=split_frames(df); out=[]
    for i in range(len(top)):
        for j in range(i+1,len(top)):
            a,b=top[i],top[j]
            if a['feature']==b['feature']:continue
            cond=[(a['feature'],a['op'],float(a['threshold'])),(b['feature'],b['op'],float(b['threshold']))]
            ed=eval_cond(sp['DISC'],cond,target); ev=eval_cond(sp['VAL'],cond,target); eo=eval_cond(sp['OOS'],cond,target)
            if not ed or ed['hit']<20:continue
            stable_val=bool(ev and ev['hit']>=8 and ev['precision']>=ev['base'])
            stable_oos=bool(eo and eo['hit']>=5 and eo['precision']>=eo['base'])
            score=(ed['precision']-ed['base'])+0.25*ed['keep']+(0 if not ev or math.isnan(ev['precision']) else ev['precision']-ev['base'])+(0 if not eo or math.isnan(eo['precision']) else eo['precision']-eo['base'])
            out.append({
                'checkpoint':label,'target':target,
                'rule':f"{a['feature']} {a['op']} {float(a['threshold']):.6g} AND {b['feature']} {b['op']} {float(b['threshold']):.6g}",
                'stable_val':int(stable_val),'stable_oos':int(stable_oos),'stable_both':int(stable_val and stable_oos),'score':score,
                **{f'DISC_{k}':v for k,v in ed.items()},**{f'VAL_{k}':v for k,v in (ev or {}).items()},**{f'OOS_{k}':v for k,v in (eo or {}).items()},
            })
    return pd.DataFrame(out).sort_values(['stable_both','stable_val','stable_oos','score'],ascending=False) if out else pd.DataFrame()

# Stop checkpoint: success-gate and failure-guard targets
D['STOP_TARGET_SUCCESS']=D.recovery_success24.astype(int)
D['STOP_TARGET_FAILURE']=1-D.recovery_success24.astype(int)
Ssucc=search_single(D,STOP_FEATURES,'STOP_TARGET_SUCCESS','STOP_POINT_SUCCESS')
Sfail=search_single(D,STOP_FEATURES,'STOP_TARGET_FAILURE','STOP_POINT_FAILURE')
Psucc=search_pairs(D,Ssucc,'STOP_TARGET_SUCCESS','STOP_POINT_SUCCESS')
Pfail=search_pairs(D,Sfail,'STOP_TARGET_FAILURE','STOP_POINT_FAILURE')
for name,x in [('STOP_SINGLE_SUCCESS_RULES.csv',Ssucc),('STOP_SINGLE_FAILURE_RULES.csv',Sfail),('STOP_PAIR_SUCCESS_RULES.csv',Psucc),('STOP_PAIR_FAILURE_RULES.csv',Pfail)]:
    x.to_csv(OUTDIR/name,index=False)

# First rebound checkpoint only: true vs false first rebound
RB=D[pd.to_numeric(D.RB1_true_recovery,errors='coerce').notna()].copy()
if len(RB):
    RB['RB_TARGET_SUCCESS']=pd.to_numeric(RB.RB1_true_recovery,errors='coerce').astype(int)
    RB['RB_TARGET_FAILURE']=1-RB.RB_TARGET_SUCCESS
    Rsucc=search_single(RB,RB1_FEATURES,'RB_TARGET_SUCCESS','REBOUND1_SUCCESS')
    Rfail=search_single(RB,RB1_FEATURES,'RB_TARGET_FAILURE','REBOUND1_FAILURE')
    RPsucc=search_pairs(RB,Rsucc,'RB_TARGET_SUCCESS','REBOUND1_SUCCESS')
    RPfail=search_pairs(RB,Rfail,'RB_TARGET_FAILURE','REBOUND1_FAILURE')
else:
    Rsucc=Rfail=RPsucc=RPfail=pd.DataFrame()
for name,x in [('RB1_SINGLE_SUCCESS_RULES.csv',Rsucc),('RB1_SINGLE_FAILURE_RULES.csv',Rfail),('RB1_PAIR_SUCCESS_RULES.csv',RPsucc),('RB1_PAIR_FAILURE_RULES.csv',RPfail)]:
    x.to_csv(OUTDIR/name,index=False)

# ---------------------------------------------------------------------------
# 11) Summary
# ---------------------------------------------------------------------------
lines=[]
lines.append('FINAL STOP RECOVERY GATE — SUMMARY')
lines.append(f'FINAL_STOP_N={len(final_stops)}')
lines.append(f'ANALYZED_N={len(D)} ERRORS={len(errors)}')
lines.append('RULE=ignore STOP -> low+1.5% -> add 100% equal qty -> avg recovery; max 2 attempts; 24h horizon')
lines.append('SPLIT=DISC Jan-Jun / VALID Jul-Aug / OOS Sep')
lines.append('')
lines.append('[OVERALL]')
lines.append(f"SUCCESS={int(D.recovery_success24.sum())}/{len(D)} ({D.recovery_success24.mean():.2%})")
lines.append('CATEGORIES='+json.dumps(D.recovery_category.value_counts().to_dict(),ensure_ascii=False))
lines.append('')
lines.append('[BY SPLIT]')
for sp,g in D.groupby('split'):
    lines.append(f"{sp}: N={len(g)} SUCCESS={int(g.recovery_success24.sum())} RATE={g.recovery_success24.mean():.2%}")
lines.append('')
lines.append('[BY STOP FAMILY]')
for r in grp_table('stop_family').itertuples(index=False):
    lines.append(f"{r.stop_family}: N={r.N} SUCCESS={r.SUCCESS} RATE={r.SUCCESS_RATE:.2%} BASE_STOP_NET={r.BASE_STOP_NET:.3f}")


def top_lines(title,df,n=8):
    lines.append(''); lines.append(title)
    if df is None or len(df)==0:
        lines.append('NONE'); return
    for r in df.head(n).to_dict('records'):
        rule=r.get('rule') or f"{r.get('feature')} {r.get('op')} {float(r.get('threshold')):.6g}"
        lines.append(
            f"{rule} | stable={r.get('stable_both')} | "
            f"DISC hit={r.get('DISC_hit')} prec={fv(r.get('DISC_precision'),0):.3f} base={fv(r.get('DISC_base'),0):.3f} | "
            f"VAL hit={r.get('VAL_hit','')} prec={safe(r.get('VAL_precision'),3)} base={safe(r.get('VAL_base'),3)} | "
            f"OOS hit={r.get('OOS_hit','')} prec={safe(r.get('OOS_precision'),3)} base={safe(r.get('OOS_base'),3)}"
        )

top_lines('[TOP STOP FAILURE GUARDS — prefer original STOP]',Pfail if len(Pfail) else Sfail)
top_lines('[TOP STOP SUCCESS GATES — allow recovery watch]',Psucc if len(Psucc) else Ssucc)
top_lines('[TOP REBOUND FALSE-GUARDS — do NOT add at +1.5%]',RPfail if len(RPfail) else Rfail)
top_lines('[TOP REBOUND TRUE-GATES — add 100% at +1.5%]',RPsucc if len(RPsucc) else Rsucc)

# exact answers to user's central question
stable_stop_fail = (Pfail[Pfail.stable_both==1] if len(Pfail) else pd.DataFrame())
stable_stop_succ = (Psucc[Psucc.stable_both==1] if len(Psucc) else pd.DataFrame())
stable_rb_fail = (RPfail[RPfail.stable_both==1] if len(RPfail) else pd.DataFrame())
stable_rb_succ = (RPsucc[RPsucc.stable_both==1] if len(RPsucc) else pd.DataFrame())
lines.append('')
lines.append('[DECISION SIGNAL]')
lines.append(f'STABLE_STOP_FAILURE_PAIR_RULES={len(stable_stop_fail)}')
lines.append(f'STABLE_STOP_SUCCESS_PAIR_RULES={len(stable_stop_succ)}')
lines.append(f'STABLE_RB_FAILURE_PAIR_RULES={len(stable_rb_fail)}')
lines.append(f'STABLE_RB_SUCCESS_PAIR_RULES={len(stable_rb_succ)}')
lines.append('Interpretation: if STOP rules are weak but rebound rules are stable, use 2-stage gating. If both are weak, do not force a recovery classifier.')

(OUTDIR/'SUMMARY.txt').write_text('\n'.join(lines)+'\n',encoding='utf-8')
(OUTDIR/'RUN_LOG.txt').write_text('\n'.join(LOG)+'\n',encoding='utf-8')

with zipfile.ZipFile(OUTZIP,'w',zipfile.ZIP_DEFLATED) as z:
    for p in sorted(OUTDIR.iterdir()):
        if p.is_file():z.write(p,p.name)

say('=== COMPLETE ===')
say('ZIP',OUTZIP)
print('\n'.join(lines[:80]))
