#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FINAL P PP12 SELECTIVE RECOVERY EXACT V21 — 2026-10-06

Purpose
-------
Use the validated V18 winner (DYN_HARD_STOP) as the exact control and test
selective PP12 cyclic Recovery gates discovered in V20.  The V18 STOP/Hard
logic, entry stack, scheduler, cooldowns and replacement behavior remain frozen.
The comparison is:
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
STAMP = '20261005_V3'
OUTDIR = ROOT / 'FINAL_P_HARD_EXTEND_EXACT_V25_20261006'
OUTDIR.mkdir(exist_ok=True)
OUTZIP = ROOT / 'FINAL_P_HARD_EXTEND_EXACT_V25_20261006_RESULTS.zip'
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
pre = NS['pre']
blocked = NS['blocked']
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
    if 'TRADES_C14_C12_C13.csv' not in z.namelist() or 'BLOCKS_C14_C12_C13.csv' not in z.namelist():
        raise SystemExit('FINAL ZIP missing TRADES/BLOCKS_C14_C12_C13.csv')
    final = pd.read_csv(z.open('TRADES_C14_C12_C13.csv'), low_memory=False)
    final_extra_blocks = pd.read_csv(z.open('BLOCKS_C14_C12_C13.csv'), low_memory=False)

final['entry_dt'] = pd.to_datetime(final['entry_dt'], utc=True, errors='coerce')
final_stops = final[final.result.astype(str) == 'STOP'].copy().reset_index(drop=True)
say('FINAL TRADES', len(final), 'FINAL STOPS', len(final_stops), 'EXPECTED 1547')
if len(final_stops) != 1547:
    raise SystemExit(f'FINAL STOP COUNT AUDIT FAIL: {len(final_stops)} != 1547')

# V6: canonical final-ledger lookup used to reconstruct STOP SimResult fields
# required by simulate_r100/gated recovery (terminal_price, exit_time, MFE/MAE).
final_idx = {str(r['setup_id']): r for _, r in final.iterrows()}

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
# V13: residual feature lookup keyed by setup_id.
# The scheduler candidate rows (ja/se) do NOT carry the full confirmation
# feature set used by the direct residual scan.  Build that exact feature
# snapshot from P_RESEARCH_PACK (Jan-Aug) and research_pv25_setups (Sep).
# This prevents a silent no-op where every residual flag evaluates False.
# ---------------------------------------------------------------------------
_resid_feat = {}

def _jdict(x):
    if isinstance(x, dict):
        return x
    try:
        if x is None or (isinstance(x,float) and np.isnan(x)):
            return {}
    except Exception:
        pass
    try:
        return json.loads(str(x))
    except Exception:
        return {}

def _put_resid_feat(sid, q, entry_price=None, low_price=None):
    if not sid:
        return
    ep=fv(entry_price)
    lp=fv(low_price)
    entry_low=None
    if ep is not None and lp is not None and lp>0:
        entry_low=(ep/lp-1.0)*100.0
    _resid_feat[str(sid)] = {
        'rsi': fv(q.get('rsi')),
        'rsi_delta': fv(q.get('rsi_delta')),
        'ema20_slope_pct': fv(q.get('ema20_slope_pct')),
        'p_v21_persistence_score': fv(q.get('p_v21_persistence_score')),
        'volume_ratio': fv(q.get('volume_ratio')),
        'entry_low_pct': entry_low,
    }

with zipfile.ZipFile(PACK) as z:
    zn=set(z.namelist())
    for _m in range(1,9):
        _n=f'2026-{_m:02d}_CANDIDATES.csv.gz'
        if _n not in zn:
            raise SystemExit('V13 MISSING RESID FEATURE SOURCE '+_n)
        _d=pd.read_csv(gzip.GzipFile(fileobj=io.BytesIO(z.read(_n))),low_memory=False)
        for _r in _d.to_dict('records'):
            _sid=str(_r.get('setup_id') or '')
            _q=_jdict(_r.get('details_json'))
            _put_resid_feat(
                _sid,_q,
                _r.get('entry_price'),
                _r.get('lowest_price_before_confirm')
            )

# September confirmed setup snapshots.
_con=sqlite3.connect(ROOT/'bybit_swing_bot.db')
_con.row_factory=sqlite3.Row
try:
    _rows=_con.execute(
        """SELECT setup_id, confirmed_price, lowest_price, snapshot_json
           FROM research_pv25_setups
           WHERE confirmed_at IS NOT NULL"""
    ).fetchall()
finally:
    _con.close()

for _rr in _rows:
    _r=dict(_rr)
    _sid=str(_r.get('setup_id') or '')
    _put_resid_feat(
        _sid,
        _jdict(_r.get('snapshot_json')),
        _r.get('confirmed_price'),
        _r.get('lowest_price')
    )

say('V13 RESID FEATURE LOOKUP',len(_resid_feat),'setups')

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
        # Prefer the saved Jan-Aug simulation row when it exists, but final-path
        # replacement trades are not guaranteed to exist in sim_idx.  In V1 those
        # rows became false exit-stream errors (244/4993 in the observed run).
        # Fall back to the exact simulator for BOTH periods so every final-path
        # trade gets its own exit time.
        et = None
        if period == 'JA':
            q = sim_idx.get(sid)
            if q is not None:
                et = dt_utc(q.get('exit_time_utc'))
        if et is None:
            s = getsim(r, period)
            et = s.exit_time
        if et is None:
            raise RuntimeError('exit time missing after exact fallback')
        exit_events.append(dict(
            setup_id=sid, symbol=str(tr['symbol']), exit_time=et,
            net_pct=float(tr['net_pct']), result=str(tr['result'])
        ))
    except Exception as e:
        exit_errors.append((sid, str(e)))
    if n % 500 == 0 or n == len(final):
        say('EXIT STREAM', n, '/', len(final), 'ok', len(exit_events), 'err', len(exit_errors))

# Do not silently lower the coverage requirement.  The rolling Shadow/context
# features are time-order sensitive, so missing exits can bias the Recovery gate.
# After the exact fallback above, require essentially complete reconstruction and
# emit the unresolved IDs before stopping if anything material is still missing.
exit_cov = len(exit_events) / max(1, len(final))
if exit_errors:
    say('EXIT STREAM UNRESOLVED', len(exit_errors), 'COVERAGE', f'{exit_cov:.4%}')
    for sid, err in exit_errors[:30]:
        say('  EXIT_ERR', sid, err)
if exit_cov < 0.998:
    raise SystemExit(f'EXIT STREAM COVERAGE STILL TOO LOW {len(exit_events)}/{len(final)}; unresolved={len(exit_errors)}')
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
# 6) ACTUAL alt UP24 breadth — fixed timestamp parsing + Sep extension
# ---------------------------------------------------------------------------
def _parse_any_ts(x):
    """Parse cache timestamps robustly. Numeric epochs are NOT assumed ns."""
    z=pd.Series(x)
    num=pd.to_numeric(z,errors='coerce')
    if num.notna().mean()>=0.90:
        med=float(num.dropna().abs().median()) if num.notna().any() else 0.0
        if med>=1e17: unit='ns'
        elif med>=1e14: unit='us'
        elif med>=1e11: unit='ms'
        elif med>=1e8: unit='s'
        else: return pd.to_datetime(z,utc=True,errors='coerce')
        return pd.to_datetime(num,unit=unit,utc=True,errors='coerce')
    return pd.to_datetime(z,utc=True,errors='coerce')


def _read_60m_file(p):
    try:
        d=pd.read_csv(p,compression='gzip',low_memory=False)
        tc=next((c for c in ('ts','timestamp','time','open_time','startTime','start_time') if c in d.columns),None)
        cc=next((c for c in ('close','c') if c in d.columns),None)
        if tc is None or cc is None:return pd.DataFrame(columns=['ts','close'])
        q=pd.DataFrame({'ts':_parse_any_ts(d[tc]),'close':pd.to_numeric(d[cc],errors='coerce')}).dropna()
        return q
    except Exception:
        return pd.DataFrame(columns=['ts','close'])


def build_up24_breadth_fixed():
    # V4: actual VPS Jan-Aug 60m cache path confirmed on 2026-10-05.
    # Example: AUSDT_60_2025122315_2026013115.csv.gz
    roots=[
        Path('/root/hyejin-trader/bybit_swing/.janaug_hist_cache_v1'),
        ROOT/'.janaug_hist_cache_v1',
        ROOT/'JANAUG_FIXED_2026_WORK',
    ]
    # V5: regex를 쓰지 않고 실제 파일명 '<SYMBOL>_60_<START>_<END>.csv.gz'를 직접 파싱한다.
    # 예: DEEPUSDT_60_2026072315_2026083115.csv.gz
    groups=defaultdict(list)
    seen=set()
    parse_bad=[]
    for root in roots:
        if not root.exists():
            continue
        for p in root.glob('*_60_*.csv.gz'):
            rp=str(p.resolve())
            if rp in seen:
                continue
            seen.add(rp)
            name=p.name
            if '_60_' not in name or not name.endswith('.csv.gz'):
                parse_bad.append(name)
                continue
            sym, tail = name.split('_60_', 1)
            core = tail[:-7]  # remove '.csv.gz'
            try:
                start_tag, end_tag = core.rsplit('_', 1)
            except ValueError:
                parse_bad.append(name)
                continue
            # 날짜 태그는 YYYYMMDDHH(10자리) 등 숫자 토큰이면 충분하다.
            if (not sym) or (not start_tag.isdigit()) or (not end_tag.isdigit()):
                parse_bad.append(name)
                continue
            if sym in ('BTCUSDT','ETHUSDT'):
                continue
            groups[sym].append(p)
    if parse_bad:
        say('UP24 FIX parser skipped', len(parse_bad), 'sample', parse_bad[:5])
    say('UP24 FIX roots', ' | '.join(f'{r}:{"YES" if r.exists() else "NO"}' for r in roots))
    say('UP24 FIX cache universe',len(groups),'symbols',sum(len(v) for v in groups.values()),'files')
    if len(groups)<20:
        samples=[]
        for root in roots:
            if root.exists():
                samples.extend([p.name for p in list(root.glob('*_60_*.csv.gz'))[:8]])
        raise SystemExit(
            f'UP24 FIX: expected Jan-Aug 60m cache universe, found only {len(groups)} symbols; '
            f'sample_files={samples}'
        )

    # Extend the SAME historical universe through Sep with public 60m data.
    sep_start=datetime(2026,8,30,tzinfo=UTC)
    sep_end=datetime(2026,10,1,23,0,tzinfo=UTC)
    ups=[]; cnts=[]
    for i,(sym,ps) in enumerate(sorted(groups.items()),1):
        parts=[]
        for p in ps:
            q=_read_60m_file(p)
            if len(q):parts.append(q)
        try:
            q=U.KC.get(sym,'60',sep_start,sep_end)
            if q is not None and len(q):
                parts.append(pd.DataFrame({'ts':pd.to_datetime(q.ts,utc=True,errors='coerce'),
                                           'close':pd.to_numeric(q.close,errors='coerce')}).dropna())
        except Exception as e:
            if i<=10:say('UP24 SEP fetch warn',sym,repr(e))
        if not parts:continue
        d=pd.concat(parts,ignore_index=True).dropna().drop_duplicates('ts',keep='last').sort_values('ts')
        if len(d)<25:continue
        # Exact hourly grid: shift(24) now means 24 clock-hours, not 24 arbitrary rows.
        ser=d.set_index('ts')['close'].resample('1h').last()
        r24=ser/ser.shift(24)-1.0
        valid=r24.notna()
        if not valid.any():continue
        ups.append((r24[valid]>0).astype('int16').rename(sym))
        cnts.append(valid[valid].astype('int16').rename(sym))
        if i%50==0: say('UP24 FIX',i,'/',len(groups))

    if len(ups)<20:
        raise SystemExit(f'UP24 FIX: usable symbols too small {len(ups)}')
    up_df=pd.concat(ups,axis=1)
    cnt_df=pd.concat(cnts,axis=1)
    n=cnt_df.sum(axis=1)
    upn=up_df.sum(axis=1,min_count=1)
    out=pd.DataFrame({'ts':n.index,'up24_n':n.astype(float),'up24':100*upn/n.replace(0,np.nan)}).reset_index(drop=True)
    out=out[(out.ts>=pd.Timestamp('2026-01-01',tz=UTC)) & (out.ts<=pd.Timestamp('2026-10-01 23:00',tz=UTC))].copy()
    out=out.sort_values('ts').drop_duplicates('ts').reset_index(drop=True)
    # Time-based deltas on full hourly grid.
    full=out.set_index('ts').reindex(pd.date_range(out.ts.min(),out.ts.max(),freq='1h',tz=UTC))
    for h in (6,12,24): full[f'up24_delta{h}']=full.up24-full.up24.shift(h)
    full['ts']=full.index
    out=full.reset_index(drop=True)

    # Hard sanity checks: the old bug collapsed epoch seconds into one 1970 hour.
    med_n=float(pd.to_numeric(out.up24_n,errors='coerce').median())
    max_n=float(pd.to_numeric(out.up24_n,errors='coerce').max())
    nunique=int(pd.to_numeric(out.up24,errors='coerce').nunique())
    sep_cov=int(out[(out.ts>=pd.Timestamp('2026-09-01',tz=UTC))&(out.ts<pd.Timestamp('2026-10-01',tz=UTC))].up24.notna().sum())
    say('UP24 FIX rows',len(out),'median_n',round(med_n,1),'max_n',round(max_n,1),'unique_up24',nunique,'SepHours',sep_cov)
    if max_n>len(groups)+2 or med_n<20 or nunique<100 or sep_cov<24*20:
        raise SystemExit(f'UP24 SANITY FAIL median_n={med_n} max_n={max_n} universe={len(groups)} unique={nunique} sep_hours={sep_cov}')
    out.to_csv(OUTDIR/'UP24_BREADTH_SERIES.csv',index=False)
    return out

UP24 = build_up24_breadth_fixed()


def up24_features(t):
    if len(UP24)==0:return {}
    ts=pd.Timestamp(t).tz_convert(UTC) if pd.Timestamp(t).tzinfo else pd.Timestamp(t,tz=UTC)
    q=UP24[(UP24.ts<ts)&UP24.up24.notna()]
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
# V21 FAST PATH: reuse the already-validated V18 generic STOP Recovery gates
# ---------------------------------------------------------------------------
# V18 already mined the 1,547 STOP cohort and exact-replayed DYN_HARD_STOP.
# Re-running that mining is unnecessary for the PP12 question.  Load the frozen
# V18 selected gates and keep every entry/STOP rule identical.
V18_RESULT_ZIP = ROOT / 'FINAL_P_HARD_SELECTIVE_EXACT_V18_20261006_RESULTS.zip'
if not V18_RESULT_ZIP.exists():
    raise SystemExit('V21 missing '+str(V18_RESULT_ZIP))
with zipfile.ZipFile(V18_RESULT_ZIP) as _zv18:
    if 'SELECTED_GATES.json' not in _zv18.namelist():
        raise SystemExit('V21 V18 result zip missing SELECTED_GATES.json')
    _sel_json = json.loads(_zv18.read('SELECTED_GATES.json').decode('utf-8'))
selected = {}
for _k,_rr in _sel_json.items():
    if _rr is None:
        selected[_k] = None
    else:
        _rr = dict(_rr)
        _rr['_conds'] = _rr.get('conds') or []
        selected[_k] = _rr
say('=== V21 LOAD FROZEN V18 GATES ===')
for _k,_rr in selected.items():
    say(_k, 'NONE' if _rr is None else (_rr.get('rule') or f"{_rr.get('feature')} {_rr.get('op')} {_rr.get('threshold')}"))

def cond_hit(feat, rr):
    if rr is None:return False
    conds=rr.get('_conds') or []
    if not conds:return False
    for k,op,th in conds:
        v=fv(feat.get(k))
        if v is None:return False
        if op=='>=' and not (v>=th):return False
        if op=='<=' and not (v<=th):return False
    return True

# ---------------------------------------------------------------------------
# 11) Exact scheduler comparison: baseline vs unconditional vs 2-stage gate
# ---------------------------------------------------------------------------
# Freeze final entry guards exactly as already validated.
c11z=sorted(ROOT.glob('C8_C11_EXACT_REPLACEMENT_*_RESULTS.zip'),key=lambda p:p.stat().st_mtime,reverse=True)
if not c11z: raise SystemExit('EXACT replay missing C8_C11 result zip')
with zipfile.ZipFile(c11z[0]) as z:
    b11=pd.read_csv(z.open('BLOCKS_C11.csv'),low_memory=False)
FINAL_BLOCK_IDS=set(b11.setup_id.astype(str)) | set(final_extra_blocks.setup_id.astype(str))
# V14: split C14-added blocks from the always-frozen block set so C14 can be
# bypassed only in strong alt-market re-expansion states. C12/C13 and C1-C11 remain frozen.
with zipfile.ZipFile(FINALZIP) as _z14:
    _c14df=pd.read_csv(_z14.open('BLOCKS_C14_ABDE.csv'),low_memory=False)
C14_BLOCK_IDS=set(_c14df.setup_id.astype(str))
NON_C14_BLOCK_IDS=FINAL_BLOCK_IDS-C14_BLOCK_IDS
say('V14 BLOCK SPLIT','ALL',len(FINAL_BLOCK_IDS),'C14',len(C14_BLOCK_IDS),'NON_C14',len(NON_C14_BLOCK_IDS))
say('FINAL FROZEN ENTRY BLOCK IDS',len(FINAL_BLOCK_IDS))

# Cache exogenous checkpoint features across policy replays.
_point_cache={}
def point_features(prefix,symbol,one,t,entry_price,entry_time,base,stop_time,low=None,add_price=None,low_to_trigger=None):
    key=(prefix,symbol,int(t.timestamp()),round(entry_price,10),int(stop_time.timestamp()))
    if key in _point_cache:
        d=dict(_point_cache[key])
    else:
        d={}
        ff=feature_pack(one,t,entry_price,entry_time)
        # Correct MFE/MAE using exact base path before STOP + post-STOP 1m path.
        post=[b for b in one if stop_time.astimezone(UTC).replace(second=0,microsecond=0)<b['ts']<t]
        emfe=(pct(max(b['h'] for b in post),entry_price) if post else None)
        emae=(pct(min(b['l'] for b in post),entry_price) if post else None)
        ff['mfe_to_cp']=max([x for x in (fv(base.mfe_pct),emfe) if x is not None],default=None)
        ff['mae_to_cp']=min([x for x in (fv(base.mae_pct),emae) if x is not None],default=None)
        for k,v in ff.items():d[prefix+k]=v
        for k,v in market_features(t).items():d[prefix+k]=v
        for k,v in candidate_features(t).items():d[prefix+k]=v
        for k,v in up24_features(t).items():d[prefix+k]=v
        _point_cache[key]=dict(d)
    if low is not None:
        d[prefix+'swing_low_pct']=pct(low,entry_price)
    if add_price is not None:
        d[prefix+'add_price_pct']=pct(add_price,entry_price)
    if low_to_trigger not in (None,0):
        d[prefix+'rebound_speed_pct_per_min']=REBOUND_PCT/float(low_to_trigger)
    return d


def gate_sim(symbol,entry_time,entry,base,watch_cut_pct=None):
    # Stage 1 STOP failure guard is only active if it was strong enough in DISC+VAL.
    stop=base.exit_time
    st=stop.astimezone(UTC).replace(second=0,microsecond=0)
    end=st+timedelta(hours=HORIZON_H)
    start=st-timedelta(hours=3)
    df=U.KC.get(symbol,'1m',start,end+timedelta(minutes=3))
    one=to_bars(df)
    post=[b for b in one if st+timedelta(minutes=1)<=b['ts']<=end]
    if not post:
        return base,'NO_DATA_BASESTOP'
    sf=point_features('STOP_',symbol,one,stop,entry,entry_time,base,stop)
    if cond_hit(sf,selected['STOP_FAIL']):
        return base,'STOP_FAILURE_GUARD'
    # V9 BASE GATE: the STOP-success gate is the first-stage permission to defer the
    # original stop. V7 selected this rule but never used it, so every STOP was
    # forced into rebound watching and 1,133 later became GATE_REJECT_EXIT.
    # If the STOP-success condition is not met, keep the original validated STOP.
    if selected.get('STOP_SUCCESS') is not None and not cond_hit(sf,selected['STOP_SUCCESS']):
        return base,'STOP_SUCCESS_NOT_MET'

    swing_low=float(base.terminal_price if base.terminal_price and base.terminal_price>0 else entry)
    low_ts=st
    attempt=0; add_open=False; add_price=0.; add_low=0.; add_bar_ts=None
    prior_add_realized=0.; extra_entries=[]; extra_exits=[]
    mfe=fv(base.mfe_pct,0.); mae=fv(base.mae_pct,0.)
    result=''; core_exit=0.; exit_t=stop; terminal=swing_low; reason=''

    def finish(px,res,why,stop_like=True):
        core_gross=(px/entry-1)*100.0
        gross=core_gross+prior_add_realized
        fee=U.FEE_PCT
        for x in extra_entries: fee += U.FEE_PCT*x.qty_mult*(x.price/entry)
        for x in extra_exits: fee += U.FEE_PCT*x.qty_mult*(x.price/entry)
        fee += U.FEE_PCT*(px/entry)
        net=gross-fee
        fills=[U.Fill(1.0,px,'GATE_CORE_EXIT')]+list(extra_exits)
        sim=U.SimResult(res,exit_t,px,fills,gross,fee,net,mfe,mae,
                        stop_stage=f'GATE_ATTEMPTS_{attempt}',detail=why)
        return sim,why

    for b in post:
        bt=b['ts']; now=bt+timedelta(minutes=1)
        lo=float(b['l']); hi=float(b['h']); cl=float(b['c'])
        mfe=max(mfe,(hi/entry-1)*100); mae=min(mae,(lo/entry-1)*100)
        if not add_open:
            # V9 PRE-RB1 WATCH SAFETY CUT:
            # Only before the first add.  If price deteriorates X% further than
            # the original validated STOP terminal price, abandon Recovery
            # observation immediately instead of waiting for a +1.5% rebound.
            # This is intentionally referenced to the original STOP price so
            # different STOP families are treated on the same additional-risk basis.
            if attempt==0 and watch_cut_pct is not None:
                cut_price=float(base.terminal_price)*(1.0-float(watch_cut_pct)/100.0)
                if lo<=cut_price:
                    exit_t=now; terminal=cut_price
                    return finish(
                        cut_price,'GATE_WATCH_CUT',
                        f'PRE_RB1_WATCH_CUT_{float(watch_cut_pct):.2f}PCT'
                    )
            if lo<swing_low:
                swing_low=lo; low_ts=bt; continue
            trigger=swing_low*(1+REBOUND_PCT/100)
            if bt<=low_ts or hi<trigger:continue
            attempt+=1
            low_to=(bt-low_ts).total_seconds()/60
            prefix='RB1_' if attempt==1 else 'RB2_'
            feat=point_features(prefix,symbol,one,bt,entry,entry_time,base,stop,
                                low=swing_low,add_price=trigger,low_to_trigger=low_to)
            succ_rule=selected['RB1_SUCCESS'] if attempt==1 else selected['RB2_SUCCESS']
            fail_rule=selected['RB1_FAIL'] if attempt==1 else selected['RB2_FAIL']
            # Conservative priority: a stable FALSE guard overrides GREEN.
            bad=cond_hit(feat,fail_rule)
            good=cond_hit(feat,succ_rule)
            if bad or not good:
                exit_t=now; terminal=trigger
                return finish(trigger,'GATE_REJECT_EXIT',f'RB{attempt}_BAD={int(bad)}_GOOD={int(good)}')
            add_open=True; add_price=trigger; add_low=swing_low; add_bar_ts=bt
            extra_entries.append(U.Fill(1.0,add_price,f'GATE_ADD{attempt}'))
            continue
        else:
            if add_bar_ts is not None and bt<=add_bar_ts:continue
            avg=(entry+add_price)/2.0
            # Conservative same-minute order: old-low rebreak wins.
            if lo<=add_low:
                prior_add_realized += (add_low-add_price)/entry*100.0
                extra_exits.append(U.Fill(1.0,add_low,f'GATE_ADD{attempt}_FALSE_EXIT'))
                add_open=False
                if attempt>=MAX_ATTEMPTS:
                    exit_t=now; terminal=add_low
                    return finish(add_low,'GATE_MAX2_EXIT','SECOND_FALSE_BREAK')
                swing_low=min(lo,add_low); low_ts=bt
                continue
            if hi>=avg:
                prior_add_realized += (avg-add_price)/entry*100.0
                extra_exits.append(U.Fill(1.0,avg,f'GATE_ADD{attempt}_AVG_EXIT'))
                add_open=False; exit_t=now; terminal=avg
                return finish(avg,'GATE_AVG_EXIT',f'AVG_RECOVERY_ATTEMPT_{attempt}',stop_like=False)

    # 24h timeout.
    last=post[-1]; cl=float(last['c']); exit_t=last['ts']+timedelta(minutes=1); terminal=cl
    if add_open:
        prior_add_realized += (cl-add_price)/entry*100.0
        extra_exits.append(U.Fill(1.0,cl,f'GATE_ADD{attempt}_24H_EXIT'))
        add_open=False
    return finish(cl,'GATE_24H_EXIT','NO_COMPLETION_WITHIN_24H')



# ---------------------------------------------------------------------------
# V18 HARD_-3 selective Recovery (Hard210 study; Sep OOS untouched in selection)
# ---------------------------------------------------------------------------
# Stage-1 GREEN OR was selected using Jan-Jun discovery + Jul-Aug validation only.
# Precheck on the DYN_CUT_200 Hard210 cohort: 140 hits / 124 recoveries.
# A post-STOP strong-red signal removes 2 Stage-1 hits (both failures), leaving
# 138 / 124. Gray trades are judged again at the first +1.5% rebound; the
# selected RB1 GREEN OR adds 28 / 26. Combined precheck = 166 / 150 (90.36%).
# IMPORTANT: these are classification prechecks only. Exact economics below
# still apply the -2.00% pre-RB1 watch cut, scheduler, cooldowns and replacements.

HARD_STOP_GREEN_RULES = [
    [('STOP_btc_eth_spread_48h','>=',0.183884), ('STOP_btc_eth_spread_240m','>=',0.0136165)],
    [('STOP_ema20_slope','>=',-0.0450572), ('STOP_v25_count_2h','<=',7.0)],
    [('STOP_btc_eth_spread_240m','>=',0.0136165), ('STOP_btc_eth_spread_30d','<=',1.65127)],
    [('STOP_up24_delta12','<=',4.27702), ('STOP_btc_eth_spread_48h','>=',0.418013)],
    [('STOP_v25_breadth_delta_2h','<=',1.4), ('STOP_ret_60m','>=',-0.765476)],
]
HARD_RB1_GREEN_RULES = [
    [('RB1_prev1m_close_pos','>=',0.584102), ('RB1_btc_eth_spread_30d','<=',-3.30235)],
    [('RB1_shadow9_stop_n','<=',2.0), ('RB1_v25_count_24h','<=',93.5)],
    [('RB1_prev1m_ret','>=',-0.298403), ('RB1_eth_5m','<=',-0.041242)],
    [('RB1_exit_n_2h','>=',3.0), ('RB1_v25_breadth_6h','<=',15.0)],
]

def _rule_and_hit(feat, conds):
    for k,op,th in conds:
        v=fv(feat.get(k))
        if v is None:
            return False
        if op=='>=' and not (v>=th):
            return False
        if op=='<=' and not (v<=th):
            return False
    return True

def _rule_or_hit(feat, rules):
    return any(_rule_and_hit(feat,r) for r in rules)

def hard_stop_green(feat):
    return _rule_or_hit(feat,HARD_STOP_GREEN_RULES)

def hard_rb1_green(feat):
    return _rule_or_hit(feat,HARD_RB1_GREEN_RULES)

def hard_strong_red(stop_feat, rb1_feat):
    # STOP RED candidate (24 hits / 17 failures in Hard210) becomes decisive only
    # after price has already made a <= -5% swing low before the first +1.5% rebound.
    # In the frozen Hard210 precheck this conjunction was 11/11 failures.
    return bool(
        fv(stop_feat.get('STOP_ret_120m')) is not None
        and fv(stop_feat.get('STOP_ret_120m')) <= 0.07
        and fv(stop_feat.get('STOP_eth_30m')) is not None
        and fv(stop_feat.get('STOP_eth_30m')) >= 0.0
        and fv(stop_feat.get('STOP_btc_eth_spread_240m')) is not None
        and fv(stop_feat.get('STOP_btc_eth_spread_240m')) <= 0.075
        and fv(rb1_feat.get('RB1_swing_low_pct')) is not None
        and fv(rb1_feat.get('RB1_swing_low_pct')) <= -5.0
    )

def is_hard3_stop(entry,base):
    tp=fv(getattr(base,'terminal_price',None))
    return bool(entry and tp is not None and tp <= float(entry)*0.9705)

def hard_selective_sim(symbol,entry_time,entry,base,watch_cut_pct=2.00,use_gray=True,extend_ctx=None,extend_mode=None):
    """Causal Hard_-3 two-stage Recovery exact replay.

    Stage 1 at original STOP:
      - GREEN => defer STOP and watch.
      - non-GREEN => if use_gray, also watch until first rebound for stage-2 verdict;
        otherwise keep original STOP (STOP-only comparison policy).
    Before first rebound: retain V17 -2.00% additional-loss safety cut.
    First +1.5% rebound:
      - strong RED overrides GREEN/GRAY and rejects Recovery.
      - Stage-1 GREEN may add immediately.
      - GRAY must pass RB1 GREEN OR to add.
    Once accepted, the frozen max-2 Recovery mechanics run without the old
    all-STOP generic RB2 gate; attempt 2 is part of the already-selected Hard
    recovery path.
    """
    stop=base.exit_time
    st=stop.astimezone(UTC).replace(second=0,microsecond=0)
    end=st+timedelta(hours=HORIZON_H)
    start=st-timedelta(hours=3)
    df=U.KC.get(symbol,'1m',start,end+timedelta(minutes=3))
    one=to_bars(df)
    post=[b for b in one if st+timedelta(minutes=1)<=b['ts']<=end]
    if not post:
        return base,'HARD_NO_DATA_BASESTOP'

    sf=point_features('STOP_',symbol,one,stop,entry,entry_time,base,stop)
    s1=hard_stop_green(sf)
    if not s1 and not use_gray:
        return base,'HARD_STOP_GREEN_NOT_MET'
    route='S1' if s1 else 'GRAY'

    swing_low=float(base.terminal_price if base.terminal_price and base.terminal_price>0 else entry)
    low_ts=st
    attempt=0; add_open=False; add_price=0.; add_low=0.; add_bar_ts=None
    prior_add_realized=0.; extra_entries=[]; extra_exits=[]
    mfe=fv(base.mfe_pct,0.); mae=fv(base.mae_pct,0.)
    exit_t=stop; terminal=swing_low
    extension_tag=''

    def finish(px,res,why):
        nonlocal extension_tag
        if extension_tag:
            why=f'{extension_tag};{why}'
        core_gross=(px/entry-1)*100.0
        gross=core_gross+prior_add_realized
        fee=U.FEE_PCT
        for x in extra_entries: fee += U.FEE_PCT*x.qty_mult*(x.price/entry)
        for x in extra_exits: fee += U.FEE_PCT*x.qty_mult*(x.price/entry)
        fee += U.FEE_PCT*(px/entry)
        net=gross-fee
        fills=[U.Fill(1.0,px,'HARD_CORE_EXIT')]+list(extra_exits)
        sim=U.SimResult(res,exit_t,px,fills,gross,fee,net,mfe,mae,
                        stop_stage=f'HARD_GATE_ATTEMPTS_{attempt}',detail=why)
        return sim,why

    for b in post:
        bt=b['ts']; now=bt+timedelta(minutes=1)
        lo=float(b['l']); hi=float(b['h']); cl=float(b['c'])
        mfe=max(mfe,(hi/entry-1)*100); mae=min(mae,(lo/entry-1)*100)
        if not add_open:
            if attempt==0 and watch_cut_pct is not None:
                cut_price=float(base.terminal_price)*(1.0-float(watch_cut_pct)/100.0)
                if lo<=cut_price:
                    # V25 selective extension is evaluated only at the -3.50% pre-RB1 watch cut,
                    # using PRIOR realized exits from the previous exact pass (strictly before now).
                    if extend_mode in ('EXT2','EXT3') and extend_ctx is not None and abs(float(watch_cut_pct)-3.50)<1e-9:
                        cx=extend_ctx(now)
                        n24=fv(cx.get('net24'),0.0); n6=fv(cx.get('net6'),0.0)
                        sl6=fv(cx.get('stoplike6'),0.0)
                        pass2=(n24>=2.0 and sl6<=1.0)
                        pass3=(pass2 and n6>=-2.0)
                        ext_ok=pass2 if extend_mode=='EXT2' else pass3
                        ctxs=f'net24={n24:.6f},stoplike6={sl6:.0f},net6={n6:.6f}'
                        if ext_ok:
                            extension_tag=f'HCUT350_{extend_mode}_PASS({ctxs})'
                            # Do not cut. Treat this minute's low as the new swing low and
                            # continue from the next minute (conservative same-minute ordering).
                            watch_cut_pct=None
                            if lo<swing_low:
                                swing_low=lo; low_ts=bt
                            continue
                        exit_t=now; terminal=cut_price
                        return finish(cut_price,'GATE_WATCH_CUT',
                                      f'HARD_{route}_PRE_RB1_WATCH_CUT_3.50PCT;HCUT350_{extend_mode}_FAIL({ctxs})')
                    exit_t=now; terminal=cut_price
                    return finish(cut_price,'GATE_WATCH_CUT',
                                  f'HARD_{route}_PRE_RB1_WATCH_CUT_{float(watch_cut_pct):.2f}PCT')
            if lo<swing_low:
                swing_low=lo; low_ts=bt; continue
            trigger=swing_low*(1+REBOUND_PCT/100)
            if bt<=low_ts or hi<trigger:
                continue
            attempt+=1
            low_to=(bt-low_ts).total_seconds()/60
            prefix='RB1_' if attempt==1 else 'RB2_'
            feat=point_features(prefix,symbol,one,bt,entry,entry_time,base,stop,
                                low=swing_low,add_price=trigger,low_to_trigger=low_to)

            if attempt==1:
                if hard_strong_red(sf,feat):
                    exit_t=now; terminal=trigger
                    return finish(trigger,'GATE_REJECT_EXIT',f'HARD_{route}_STRONG_RED_RB1')
                if (not s1) and (not hard_rb1_green(feat)):
                    exit_t=now; terminal=trigger
                    return finish(trigger,'GATE_REJECT_EXIT','HARD_GRAY_RB1_GREEN_NOT_MET')
                gate_tag='HARD_S1' if s1 else 'HARD_GRAY_RB1_GREEN'
            else:
                # Attempt 2 is unconditional once the Hard trade passed its first
                # selection gate and attempt 1 false-broke.
                gate_tag='HARD_S1' if s1 else 'HARD_GRAY_RB1_GREEN'

            add_open=True; add_price=trigger; add_low=swing_low; add_bar_ts=bt
            extra_entries.append(U.Fill(1.0,add_price,f'HARD_ADD{attempt}'))
            continue
        else:
            if add_bar_ts is not None and bt<=add_bar_ts:
                continue
            avg=(entry+add_price)/2.0
            # Conservative same-minute ordering retained from V17.
            if lo<=add_low:
                prior_add_realized += (add_low-add_price)/entry*100.0
                extra_exits.append(U.Fill(1.0,add_low,f'HARD_ADD{attempt}_FALSE_EXIT'))
                add_open=False
                if attempt>=MAX_ATTEMPTS:
                    exit_t=now; terminal=add_low
                    return finish(add_low,'GATE_MAX2_EXIT',f'{gate_tag}_ADD2_FALSE_BREAK')
                swing_low=min(lo,add_low); low_ts=bt
                continue
            if hi>=avg:
                prior_add_realized += (avg-add_price)/entry*100.0
                extra_exits.append(U.Fill(1.0,avg,f'HARD_ADD{attempt}_AVG_EXIT'))
                add_open=False; exit_t=now; terminal=avg
                return finish(avg,'GATE_AVG_EXIT',f'{gate_tag}_AVG_RECOVERY_ATTEMPT_{attempt}')

    last=post[-1]; cl=float(last['c']); exit_t=last['ts']+timedelta(minutes=1); terminal=cl
    if add_open:
        prior_add_realized += (cl-add_price)/entry*100.0
        extra_exits.append(U.Fill(1.0,cl,f'HARD_ADD{attempt}_24H_EXIT'))
        add_open=False
    return finish(cl,'GATE_24H_EXIT',f'HARD_{route}_NO_COMPLETION_WITHIN_24H')



# ---------------------------------------------------------------------------
# V21 PP12 selective Recovery — V20 frozen GREEN gates, V19 mechanics
# ---------------------------------------------------------------------------
# V20 selection used Jan-Jun discovery + Jul-Aug validation; Sep was OOS only.
# Economic label was PP Recovery improvement over immediate PP12 exit.
# Exact replay uses KEEP_REMAINING economics: preserve any pre-PP partial fills,
# cancel only the terminal PP12 fill, and add the same quantity as the remaining core.
PP_WATCH_CUT_PCT = 1.25
PP_GATE_RULES = {
    'A': [
        ('PP_v25_breadth_delta_12h','>=',9.0),
        ('PP_up24_n','>=',409.5),
    ],
    'B': [('PP_low_break_count_30m','>=',10.0)],
    'C': [('PP_btc_eth_spread_240m','>=',0.392273)],
    'D': [('PP_prev1m_ret','>=',0.154482)],
}

_pp_checkpoint_cache={}
_policy_pp_full_cache={}

def _pp_cond_hit(feat, conds):
    for k,op,th in conds:
        v=fv(feat.get(k))
        if v is None:return False
        if op=='>=' and not (v>=th):return False
        if op=='<=' and not (v<=th):return False
    return True

def pp_policy_mode(policy):
    if policy == 'DYN_HARD_STOP_PPA': return 'A'
    if policy in ('DYN_HARD_STOP_PPABC','DYN_HARD_STOP_PPABC_NOSL'): return 'ABC'
    if policy.startswith('DYN_HARD_STOP_PPABC_HCUT_'): return 'ABC'
    if policy == 'DYN_HARD_STOP_PPABCD': return 'ABCD'
    return None

def pp_checkpoint_features(symbol, entry_time, entry, pp_time):
    key=(symbol,int(entry_time.timestamp()),int(pp_time.timestamp()),round(float(entry),10))
    if key in _pp_checkpoint_cache:
        return dict(_pp_checkpoint_cache[key])
    start=min(entry_time.astimezone(UTC)-timedelta(minutes=5), pp_time.astimezone(UTC)-timedelta(hours=12))
    raw=U.KC.get(symbol,'1m',start,pp_time.astimezone(UTC)+timedelta(minutes=2))
    one=to_bars(raw)
    ff=feature_pack(one,pp_time,entry,entry_time)
    mf=market_features(pp_time)
    cf=candidate_features(pp_time)
    uf=up24_features(pp_time)
    out={
        'PP_prev1m_ret':fv(ff.get('prev1m_ret')),
        'PP_low_break_count_30m':fv(ff.get('low_break_count_30m')),
        'PP_btc_eth_spread_240m':fv(mf.get('btc_eth_spread_240m')),
        'PP_v25_breadth_delta_12h':fv(cf.get('v25_breadth_delta_12h')),
        'PP_up24_n':fv(uf.get('up24_n')),
    }
    _pp_checkpoint_cache[key]=dict(out)
    return out

def pp_gate_eval(mode, feat):
    hits={k:_pp_cond_hit(feat,v) for k,v in PP_GATE_RULES.items()}
    if mode=='A': ok=hits['A']
    elif mode=='ABC': ok=hits['A'] or hits['B'] or hits['C']
    elif mode=='ABCD': ok=hits['A'] or hits['B'] or hits['C'] or hits['D']
    else: ok=False
    tag=''.join(k for k in 'ABCD' if hits[k]) or 'NONE'
    return bool(ok),tag,hits

def pp_seed_from_full_sim(sim, entry):
    fills=list(getattr(sim,'fills',[]) or [])
    pp_fills=[x for x in fills if str(getattr(x,'kind',''))=='PP12']
    pre=[x for x in fills if str(getattr(x,'kind',''))!='PP12']
    if not pp_fills and fills:
        pp_fills=[fills[-1]]; pre=fills[:-1]
    qrem=sum(float(getattr(x,'qty_mult',0.0)) for x in pp_fills)
    prior_gross=sum(float(x.qty_mult)*(float(x.price)/entry-1.0)*100.0 for x in pre)
    prior_exit_fee=sum(U.FEE_PCT*float(x.qty_mult)*(float(x.price)/entry) for x in pre)
    return qrem,prior_gross,prior_exit_fee,pre

def pp_selective_recovery_sim(symbol,entry_time,entry,full_base,watch_cut_pct=PP_WATCH_CUT_PCT):
    """Cancel the terminal PP12 fill and apply V19 KEEP_REMAINING cyclic Recovery."""
    if str(full_base.result)!='PROFIT_PROTECT_EXIT':
        return full_base,'PP_BASE_NOT_PP'
    pp_time=full_base.exit_time
    pp_px=float(full_base.terminal_price)
    qcore,prior_gross,prior_exit_fee,pre_fills=pp_seed_from_full_sim(full_base,entry)
    if qcore<=1e-12:
        return full_base,'PP_NO_REMAINING_CORE'
    st=pp_time.astimezone(UTC).replace(second=0,microsecond=0)
    end=st+timedelta(hours=HORIZON_H)
    raw=U.KC.get(symbol,'1m',st-timedelta(minutes=2),end+timedelta(minutes=3))
    one=to_bars(raw)
    post=[b for b in one if b['ts']>st and b['ts']<=pp_time.astimezone(UTC)+timedelta(hours=HORIZON_H)]
    if not post:
        return full_base,'PP_REC_NO_DATA_BASE'

    swing_low=pp_px; low_ts=st
    attempt=0; add_open=False; add_price=0.0; add_low=0.0; add_bar_ts=None
    add_realized=0.0; add_entries=[]; add_exits=[]
    mfe=fv(full_base.mfe_pct,0.0); mae=fv(full_base.mae_pct,0.0)
    exit_t=pp_time; core_exit=pp_px

    def finish(px,res,why):
        core_gross=qcore*(px/entry-1.0)*100.0
        gross=prior_gross+core_gross+add_realized
        fee=U.FEE_PCT + prior_exit_fee
        for q,p in add_entries: fee += U.FEE_PCT*q*(p/entry)
        for q,p in add_exits: fee += U.FEE_PCT*q*(p/entry)
        fee += U.FEE_PCT*qcore*(px/entry)
        net=gross-fee
        fills=list(pre_fills)+[U.Fill(q,p,'PP_REC_ADD_EXIT') for q,p in add_exits]
        fills.append(U.Fill(qcore,px,'PP_REC_CORE_EXIT'))
        sim=U.SimResult(res,exit_t,px,fills,gross,fee,net,mfe,mae,
                        stop_stage=f'PP_REC_ATTEMPTS_{attempt}',detail=why)
        return sim,why

    for b in post:
        bt=b['ts']; now=bt+timedelta(minutes=1)
        lo=float(b['l']); hi=float(b['h'])
        mfe=max(mfe,(hi/entry-1.0)*100.0); mae=min(mae,(lo/entry-1.0)*100.0)
        if not add_open:
            if attempt==0 and watch_cut_pct is not None:
                cut_px=pp_px*(1.0-float(watch_cut_pct)/100.0)
                if lo<=cut_px:
                    exit_t=now; core_exit=cut_px
                    return finish(cut_px,'PP_REC_WATCH_CUT',f'PP_PRE_RB1_WATCH_CUT_{float(watch_cut_pct):.2f}PCT')
            if lo<swing_low:
                swing_low=lo; low_ts=bt; continue
            trigger=swing_low*(1.0+REBOUND_PCT/100.0)
            if bt<=low_ts or hi<trigger: continue
            attempt+=1; add_open=True; add_price=trigger; add_low=swing_low; add_bar_ts=bt
            add_entries.append((qcore,add_price))
            continue
        if add_bar_ts is not None and bt<=add_bar_ts:
            continue
        avg=(entry+add_price)/2.0
        # Conservative same-minute priority: rebreak old low before average recovery.
        if lo<=add_low:
            add_realized += qcore*(add_low-add_price)/entry*100.0
            add_exits.append((qcore,add_low)); add_open=False
            if attempt>=MAX_ATTEMPTS:
                exit_t=now; core_exit=add_low
                return finish(add_low,'PP_REC_MAX2_EXIT','PP_SECOND_FALSE_BREAK')
            swing_low=min(lo,add_low); low_ts=bt
            continue
        if hi>=avg:
            add_realized += qcore*(avg-add_price)/entry*100.0
            add_exits.append((qcore,avg)); add_open=False; exit_t=now; core_exit=avg
            return finish(avg,'PP_REC_AVG_EXIT',f'PP_AVG_RECOVERY_ATTEMPT_{attempt}')

    last=post[-1]; cl=float(last['c']); exit_t=last['ts']+timedelta(minutes=1); core_exit=cl
    if add_open:
        add_realized += qcore*(cl-add_price)/entry*100.0
        add_exits.append((qcore,cl)); add_open=False
    return finish(cl,'PP_REC_24H_EXIT','PP_NO_COMPLETION_WITHIN_24H')

# V7: policy replay can choose replacement setups absent from BASELINE final ledger.
# Keep a full base STOP reconstruction cache independent of final_idx.
_policy_stop_detail_cache={}


# ---------------------------------------------------------------------------
# V11 Fine-grained P-ON guard — NO whole-month OFF
# Discovery: Jan-Jun only. Validation: Jul-Aug. OOS report: Sep.
# Uses only information available at each candidate timestamp.
# ---------------------------------------------------------------------------
_fine_pon_cache = {}

def fine_pon_snapshot(t):
    key=int(t.timestamp())
    if key in _fine_pon_cache:
        return _fine_pon_cache[key]
    cf=candidate_features(t)
    uf=up24_features(t)
    out={
        'btc4h': fv(hourly_change('BTCUSDT', t, 4)),
        'dbr24': fv(cf.get('v25_breadth_delta_24h')),
        'up_d6': fv(uf.get('up24_delta6')),
        'up_d24': fv(uf.get('up24_delta24')),
    }
    _fine_pon_cache[key]=out
    return out

def fine_pon_pass(name,t):
    f=fine_pon_snapshot(t)
    btc=f.get('btc4h')
    db=f.get('dbr24')
    d6=f.get('up_d6')
    d24=f.get('up_d24')

    # Missing exogenous market feature -> fail closed.
    if btc is None or db is None or d6 is None or d24 is None:
        return False

    # Main rule selected using Jan-Jun only:
    # A) BTC 4H is not materially weak AND V25 24H breadth is expanding
    # OR
    # B) Alt UP24 breadth has turned up over 6H while still contracted vs 24H ago.
    if name=='PON_FINE_MAIN':
        A=(btc >= -0.30 and db >= 3)
        B=(d6 >= 0 and d24 <= -3)
        return A or B

    # Neighbor checks for threshold robustness.
    if name=='PON_FINE_DBR2':
        A=(btc >= -0.30 and db >= 2)
        B=(d6 >= 0 and d24 <= -3)
        return A or B

    if name=='PON_FINE_BTC35':
        A=(btc >= -0.35 and db >= 3)
        B=(d6 >= 0 and d24 <= -3)
        return A or B

    if name=='PON_FINE_WIDE_B':
        A=(btc >= -0.30 and db >= 3)
        B=(d6 >= -2 and d24 <= -3)
        return A or B

    if name=='PON_FINE_DBR4':
        A=(btc >= -0.30 and db >= 4)
        B=(d6 >= 0 and d24 <= -3)
        return A or B

    raise RuntimeError('UNKNOWN FINE PON POLICY '+str(name))


# ---------------------------------------------------------------------------
# V12 residual-risk guards on top of PON_FINE_DBR2
# Derived from DBR2 trades using Jan-Jun discovery only; Jul-Aug validation;
# Sep is untouched OOS reporting.
#
# R1: weak RSI but entry already chased >1.0695% from pre-confirm low
# R2: RSI expansion + strong EMA20 slope (late acceleration / exhaustion)
# R3: high persistence but very weak volume support
# ---------------------------------------------------------------------------
def residual_risk_flags_by_sid(sid):
    f=_resid_feat.get(str(sid),{})
    rsi=fv(f.get('rsi'))
    rsi_delta=fv(f.get('rsi_delta'))
    ema20=fv(f.get('ema20_slope_pct'))
    pers=fv(f.get('p_v21_persistence_score'))
    vol=fv(f.get('volume_ratio'))
    entry_low=fv(f.get('entry_low_pct'))

    R1=(rsi is not None and entry_low is not None
        and rsi <= 64.733371 and entry_low >= 1.069512)
    R2=(rsi_delta is not None and ema20 is not None
        and rsi_delta >= 8.048741 and ema20 >= 0.337831)
    R3=(pers is not None and vol is not None
        and pers >= 62.478654 and vol <= 0.508608)
    return R1,R2,R3

def residual_block(policy,sid):
    R1,R2,R3=residual_risk_flags_by_sid(sid)
    if policy=='PON_RESID_R1': return R1
    if policy=='PON_RESID_R2': return R2
    if policy=='PON_RESID_R3': return R3
    if policy=='PON_RESID_R12': return R1 or R2
    if policy=='PON_RESID_R13': return R1 or R3
    if policy=='PON_RESID_R23': return R2 or R3
    if policy=='PON_RESID_R123': return R1 or R2 or R3
    return False

# Audit the exact DBR2-eligible candidate stream BEFORE scheduler replay.
# Abort rather than silently produce another no-op result.
_resid_audit=Counter()
_resid_eligible=0
for _period,_d in [('JA',ja),('SE',se)]:
    for _,_r in _d.iterrows():
        if not pre(_r,_period) or blocked(_r,_period):
            continue
        _sid=str(_r['setup_id'])
        if _sid in FINAL_BLOCK_IDS:
            continue
        _now=pd.Timestamp(_r['_dt']).to_pydatetime() if _period=='JA' else _r['_dt']
        if not fine_pon_pass('PON_FINE_DBR2',_now):
            continue
        _resid_eligible += 1
        _a,_b,_c=residual_risk_flags_by_sid(_sid)
        _resid_audit['R1'] += int(_a)
        _resid_audit['R2'] += int(_b)
        _resid_audit['R3'] += int(_c)
        _resid_audit['R123'] += int(_a or _b or _c)
say('V13 RESID AUDIT eligible',_resid_eligible,
    'R1',_resid_audit['R1'],'R2',_resid_audit['R2'],
    'R3',_resid_audit['R3'],'R123',_resid_audit['R123'])
if _resid_audit['R123'] <= 0:
    raise SystemExit('V13 RESID AUDIT FAIL: all residual guards are zero-hit')



# ---------------------------------------------------------------------------
# V14 dynamic protection / bypass helpers
# ---------------------------------------------------------------------------
def _v14_up(t):
    u=up24_features(t)
    return (fv(u.get('up24')),fv(u.get('up24_delta6')),fv(u.get('up24_delta12')),fv(u.get('up24_delta24')))

def v14_dbr2_pass(t):
    # Base DBR2 remains the default.  Bypass only in the strong re-expansion
    # states that were directly positive in Jan-Aug AND Sep.
    if fine_pon_pass('PON_FINE_DBR2',t):
        return True
    up,d6,d12,d24=_v14_up(t)
    by1=(d6 is not None and -11.0 <= d6 <= -5.0)
    by2=(d12 is not None and d24 is not None and d12 <= 22.0 and d24 >= 51.0)
    return bool(by1 or by2)

def v14_c14_bypass(t):
    up,d6,_,_=_v14_up(t)
    return bool(up is not None and d6 is not None and up >= 61.0 and d6 >= 12.5)

def v14_r1_bypass(t):
    e=recent_exit_features(t)
    _,_,d12,_=_v14_up(t)
    b1=(fv(e.get('exit_n_12h')) is not None and fv(e.get('exit_n_24h')) is not None
        and fv(e.get('exit_n_12h')) <= 7 and fv(e.get('exit_n_24h')) >= 8)
    b2=(d12 is not None and d12 >= 12.0 and fv(e.get('exit_stop_n_12h')) is not None
        and fv(e.get('exit_stop_n_12h')) >= 4)
    return bool(b1 or b2)

def v14_common_loss_guard(t):
    # Market-only common loss guard: this direct cohort was negative in every
    # month Jan-Sep.  Keep it exogenous so scheduler replacements can be audited cleanly.
    _,d6,_,_=_v14_up(t)
    return bool(d6 is not None and d6 <= -30.0)

def v14_residual_block(policy,sid,r,t):
    r1,r2,r3=residual_risk_flags_by_sid(sid)
    # R2/R3 are the low-9M-damage residual guards and stay on in every V14 policy.
    if r2 or r3:
        return True
    # R1 is only added conditionally; strong/recovering states bypass it.
    if (policy in ('DYN_R1_COND','DYN_ALL') or policy.startswith('DYN_CUT_') or policy.startswith('DYN_HARD_')) and r1 and not v14_r1_bypass(t):
        return True
    return False

def v14_market_pass(policy,t):
    if policy in ('DYN_DBR2_BYPASS','DYN_ALL') or policy.startswith('DYN_CUT_') or policy.startswith('DYN_HARD_'):
        return v14_dbr2_pass(t)
    return fine_pon_pass('PON_FINE_DBR2',t)

def v14_c14_blocked(policy,sid,t):
    if sid in NON_C14_BLOCK_IDS:
        return True
    if sid not in C14_BLOCK_IDS:
        return False
    if (policy in ('DYN_C14_BYPASS','DYN_ALL') or policy.startswith('DYN_CUT_') or policy.startswith('DYN_HARD_')) and v14_c14_bypass(t):
        return False
    return True

def run_exact_policy(policy, extend_ctx=None):
    trades=[]
    for period,d in [('JA',ja),('SE',se)]:
        sched=U.Scheduler()
        for _,r in d.iterrows():
            if not pre(r,period) or blocked(r,period):continue
            sid=str(r['setup_id']); sym=str(r['symbol'])
            now=pd.Timestamp(r['_dt']).to_pydatetime() if period=='JA' else r['_dt']
            if policy.startswith('DYN_'):
                if v14_c14_blocked(policy,sid,now):
                    continue
                if not v14_market_pass(policy,now):
                    continue
                if v14_residual_block(policy,sid,r,now):
                    continue
                if (policy in ('DYN_LOSS_GUARD','DYN_ALL') or policy.startswith('DYN_CUT_') or policy.startswith('DYN_HARD_')) and v14_common_loss_guard(now):
                    continue
            else:
                if sid in FINAL_BLOCK_IDS:continue
                if policy.startswith('PON_FINE_') and not fine_pon_pass(policy,now):
                    continue
                if policy.startswith('PON_RESID_'):
                    # DBR2 market gate is frozen; only the residual trade-quality block varies.
                    if not fine_pon_pass('PON_FINE_DBR2',now):
                        continue
                    if residual_block(policy,sid):
                        continue
            ok,_=sched.can_open(now,sym)
            if not ok:continue
            base=getsim(r,period)
            sim=base; why='BASE'
            pp_gate_hit=0; pp_gate_tag=''; pp_recovery_applied=0
            if base.result=='STOP' and policy!='BASELINE':
                # V7 FIX:
                # Alternate recovery policies can free a slot and select a setup that
                # never existed in the frozen BASELINE final ledger.  Therefore the
                # recovery base MUST NOT depend on final_idx.
                #
                # Re-simulate the CURRENT candidate r with the validated base engine.
                # Cache by (period, setup_id) so UNCONDITIONAL and GATED replays share
                # the exact same reconstructed STOP state.
                cache_key=(period,sid)
                bd=_policy_stop_detail_cache.get(cache_key)
                if bd is None:
                    bd=simulate_detail(sid,period,r)
                    _policy_stop_detail_cache[cache_key]=bd
                if str(bd.result)!='STOP':
                    raise RuntimeError(
                        f'POLICY STOP RESIM MISMATCH {sid} period={period} '
                        f'getsim={base.result} resim={bd.result}'
                    )
                base_recovery=U.SimResult(
                    result=str(bd.result), exit_time=bd.exit_time,
                    terminal_price=float(bd.terminal_price), fills=[],
                    gross_pct=float(bd.net_pct), fee_pct=0.0, net_pct=float(bd.net_pct),
                    mfe_pct=float(bd.mfe_pct or 0.0), mae_pct=float(bd.mae_pct or 0.0),
                    stop_stage=str(bd.stop_stage or ''), detail='V7_RECONSTRUCTED_CANDIDATE_STOP'
                )
                required=('result','exit_time','terminal_price','mfe_pct','mae_pct','net_pct')
                missing=[x for x in required if not hasattr(base_recovery,x)]
                if missing:
                    raise RuntimeError('RECOVERY BASE MISSING '+','.join(missing)+' '+sid)
                if policy=='UNCONDITIONAL_R100':
                    sobj={'setup_id':sid,'symbol':sym,'entry':now,'entry_price':float(r['entry_price'])}
                    sim=U.simulate_r100({'setup_obj':sobj,'sim_obj':base_recovery})
                    if getattr(sim,'data_error','') or str(sim.result)=='R100_DATA_ERROR':
                        sim=base; why='R100_DATA_FALLBACK_BASE'
                    else:
                        why='R100'
                elif policy=='GATED_2STAGE':
                    sim,why=gate_sim(sym,now,float(r['entry_price']),base_recovery,watch_cut_pct=None)
                elif policy.startswith('GATED_CUT_'):
                    watch_cut=float(policy.rsplit('_',1)[1])/100.0
                    sim,why=gate_sim(sym,now,float(r['entry_price']),base_recovery,watch_cut_pct=watch_cut)
                elif policy.startswith('PON_FINE_'):
                    # Keep the already-selected V9 Recovery structure fixed.
                    sim,why=gate_sim(sym,now,float(r['entry_price']),base_recovery,watch_cut_pct=2.25)
                elif policy.startswith('PON_RESID_'):
                    # Same DBR2 + V9 Recovery. Only entry residual guards are changed.
                    sim,why=gate_sim(sym,now,float(r['entry_price']),base_recovery,watch_cut_pct=2.25)
                elif policy.startswith('DYN_HARD_'):
                    # V25: freeze Hard STOP-GREEN + PPABC. Baseline uses -3.50%.
                    # EXT2/EXT3 may continue only selected trades beyond -3.50%.
                    _hard_watch_cut=2.00
                    _extend_mode=None
                    if policy.startswith('DYN_HARD_STOP_PPABC_HCUT_350_EXT2'):
                        _hard_watch_cut=3.50; _extend_mode='EXT2'
                    elif policy.startswith('DYN_HARD_STOP_PPABC_HCUT_350_EXT3'):
                        _hard_watch_cut=3.50; _extend_mode='EXT3'
                    elif policy.startswith('DYN_HARD_STOP_PPABC_HCUT_'):
                        _tag=policy.split('_HCUT_',1)[1]
                        _hard_watch_cut=None if _tag=='NONE' else float(_tag)/100.0
                    if is_hard3_stop(float(r['entry_price']),base_recovery):
                        sim,why=hard_selective_sim(
                            sym,now,float(r['entry_price']),base_recovery,watch_cut_pct=_hard_watch_cut,
                            use_gray=False,extend_ctx=extend_ctx,extend_mode=_extend_mode
                        )
                    else:
                        sim,why=gate_sim(sym,now,float(r['entry_price']),base_recovery,watch_cut_pct=2.00)
                elif policy.startswith('DYN_'):
                    # V17 frozen behavior for control policies.
                    if policy.startswith('DYN_CUT_'):
                        watch_cut=float(policy.rsplit('_',1)[1])/100.0
                    else:
                        watch_cut=2.25
                    sim,why=gate_sim(sym,now,float(r['entry_price']),base_recovery,watch_cut_pct=watch_cut)
            # V21 PP12 selective Recovery is evaluated only after the frozen Hard-STOP policy.
            _ppm=pp_policy_mode(policy)
            if str(base.result)=='PROFIT_PROTECT_EXIT' and _ppm is not None:
                _pk=(period,sid)
                _full=_policy_pp_full_cache.get(_pk)
                if _full is None:
                    _sobj=dict(setup_id=sid,symbol=sym,entry=now,entry_price=float(r['entry_price']))
                    _full=U.simulate_base(_sobj)
                    _policy_pp_full_cache[_pk]=_full
                if str(_full.result)!='PROFIT_PROTECT_EXIT':
                    raise RuntimeError(f'PP RESIM MISMATCH {sid} period={period} getsim={base.result} resim={_full.result}')
                _pf=pp_checkpoint_features(sym,now,float(r['entry_price']),_full.exit_time)
                _ok,_tag,_hits=pp_gate_eval(_ppm,_pf)
                pp_gate_hit=int(_ok); pp_gate_tag=_tag
                if _ok:
                    sim,why=pp_selective_recovery_sim(sym,now,float(r['entry_price']),_full,watch_cut_pct=PP_WATCH_CUT_PCT)
                    pp_recovery_applied=1
                    why=f'PP_{_ppm}_GATE_{_tag};'+str(why)
                else:
                    sim=base; why=f'PP_{_ppm}_GATE_NOT_MET_{_tag}'

            if pp_recovery_applied:
                # Sensitivity: default treats failed PP Recovery as stop-like; _NOSL preserves original PP semantics.
                if policy.endswith('_NOSL'):
                    stop_like=False
                else:
                    stop_like=(str(sim.result)!='PP_REC_AVG_EXIT')
            elif policy=='UNCONDITIONAL_R100':
                stop_like=sim.result in ('STOP','LATE_FAILURE_EXIT','MAX2_EXIT','CUT3','CUT6','RECOVERY_24H_EXIT')
            elif (policy.startswith('GATED_') or policy.startswith('PON_FINE_') or policy.startswith('PON_RESID_') or policy.startswith('DYN_')) and base.result=='STOP':
                stop_like=(sim.result!='GATE_AVG_EXIT')
            else:
                stop_like=sim.result in ('STOP','LATE_FAILURE_EXIT')
            sched.add(now,sym,sim,stop_like)
            _hard3=0
            if str(base.result)=='STOP':
                try:
                    _hard3=int(is_hard3_stop(float(r['entry_price']),base_recovery))
                except Exception:
                    _hard3=0
            trades.append(dict(policy=policy,period=period,setup_id=sid,symbol=sym,
                               month=str(r['entry_time_kst'])[:7],day=str(r['entry_time_kst'])[:10],
                               entry_dt=pd.Timestamp(now),base_result=str(base.result),result=str(sim.result),
                               net_pct=float(sim.net_pct),exit_time_utc=sim.exit_time.isoformat(),
                               stop_like=int(bool(stop_like)),policy_reason=why,hard3=int(_hard3),
                               pp_gate_hit=int(pp_gate_hit),pp_gate_tag=str(pp_gate_tag),
                               pp_recovery_applied=int(pp_recovery_applied)))
    return pd.DataFrame(trades)




say('V25 HARD -3.5 SELECTIVE EXTENSION EXACT')
say('Frozen structure: Hard STOP-GREEN + PPABC. Baseline Hard pre-RB1 cut=-3.50%.')
say('EXT2 gate at -3.50%: prior 24H realized net >= +2.0%p AND prior 6H stop-like exits <= 1.')
say('EXT3 adds: prior 6H realized net >= -2.0%p.')
say('Context is strictly PRIOR realized exits. Policies are iterated to convergence so extension-altered exits feed the next pass.')
say('=== EXACT CONTROL REPLAYS ===')

def make_context_func(tdf):
    z=tdf[['exit_time_utc','net_pct','stop_like']].copy()
    z['_t']=pd.to_datetime(z['exit_time_utc'],utc=True,errors='coerce')
    z=z[z['_t'].notna()].sort_values('_t').reset_index(drop=True)
    times=[x.timestamp() for x in z['_t']]
    nets=np.asarray(pd.to_numeric(z['net_pct'],errors='coerce').fillna(0.0),dtype=float)
    stops=np.asarray(pd.to_numeric(z['stop_like'],errors='coerce').fillna(0).astype(int),dtype=float)
    pnet=np.concatenate([[0.0],np.cumsum(nets)])
    pstop=np.concatenate([[0.0],np.cumsum(stops)])
    def ctx(t):
        tt=pd.Timestamp(t)
        if tt.tzinfo is None:
            tt=tt.tz_localize(UTC)
        else:
            tt=tt.tz_convert(UTC)
        x=tt.timestamp()
        j=bisect.bisect_left(times,x)   # STRICTLY prior exits only
        out={}
        for h in (6,24):
            k=bisect.bisect_left(times,x-h*3600,0,j)
            out[f'net{h}']=float(pnet[j]-pnet[k])
            out[f'stoplike{h}']=float(pstop[j]-pstop[k])
            out[f'n{h}']=int(j-k)
        return out
    return ctx

POLICY_TRADES={}

say('RUN CURRENT CONTROL DYN_HARD_STOP_PPABC')
current=run_exact_policy('DYN_HARD_STOP_PPABC')
POLICY_TRADES['DYN_HARD_STOP_PPABC']=current
current.to_csv(OUTDIR/'EXACT_TRADES_DYN_HARD_STOP_PPABC.csv',index=False)
current_net=float(current.net_pct.sum())
say('CURRENT N',len(current),'NET',round(current_net,6))
if len(current)!=2836 or abs(current_net-852.920018)>0.08:
    raise SystemExit(f'V25 CURRENT CONTROL AUDIT FAIL N={len(current)} NET={current_net:.6f}')

say('RUN -3.50 BASELINE DYN_HARD_STOP_PPABC_HCUT_350')
h350=run_exact_policy('DYN_HARD_STOP_PPABC_HCUT_350')
POLICY_TRADES['DYN_HARD_STOP_PPABC_HCUT_350']=h350
h350.to_csv(OUTDIR/'EXACT_TRADES_DYN_HARD_STOP_PPABC_HCUT_350.csv',index=False)
h350_net=float(h350.net_pct.sum())
say('H350 N',len(h350),'NET',round(h350_net,6))
if len(h350)!=2834 or abs(h350_net-933.978325)>0.08:
    raise SystemExit(f'V25 H350 AUDIT FAIL N={len(h350)} NET={h350_net:.6f}')

ITER_ROWS=[]
FINAL_BY_MODE={}
PASSES_BY_MODE={}

def iterate_mode(mode,max_iter=5):
    prev_sig=None
    ctx=make_context_func(h350)
    passes=[]
    for p in range(1,max_iter+1):
        pol=f'DYN_HARD_STOP_PPABC_HCUT_350_{mode}_P{p}'
        say('RUN',pol)
        t=run_exact_policy(pol,extend_ctx=ctx)
        POLICY_TRADES[pol]=t
        passes.append(pol)
        t.to_csv(OUTDIR/f'EXACT_TRADES_{pol}.csv',index=False)
        rs=t.policy_reason.astype(str)
        passmask=rs.str.contains(f'HCUT350_{mode}_PASS',regex=False)
        failmask=rs.str.contains(f'HCUT350_{mode}_FAIL',regex=False)
        passids=tuple(sorted(t.loc[passmask,'setup_id'].astype(str).tolist()))
        net=float(t.net_pct.sum())
        sig=(passids,len(t),round(net,9))
        stable=(prev_sig==sig)
        hard=t[pd.to_numeric(t.hard3,errors='coerce').fillna(0).astype(int).eq(1)]
        say(pol,'N',len(t),'NET',round(net,6),'DELTA_H350',round(net-h350_net,6),
            'EXT_PASS',int(passmask.sum()),'EXT_FAIL',int(failmask.sum()),
            'HARD_NET',round(float(hard.net_pct.sum()),6),'STABLE',int(stable))
        ITER_ROWS.append(dict(
            mode=mode,iteration=p,policy=pol,N=len(t),NET=net,
            DELTA_VS_CURRENT=net-current_net,DELTA_VS_H350=net-h350_net,
            EXT_PASS=int(passmask.sum()),EXT_FAIL=int(failmask.sum()),
            HARD_N=len(hard),HARD_NET=float(hard.net_pct.sum()),
            STOPLIKE=int(pd.to_numeric(t.stop_like,errors='coerce').fillna(0).sum()),
            STABLE=int(stable)
        ))
        if stable and p>=2:
            return t,passes
        prev_sig=sig
        ctx=make_context_func(t)
    return t,passes

for mode in ('EXT2','EXT3'):
    final_t,passes=iterate_mode(mode)
    FINAL_BY_MODE[mode]=final_t
    PASSES_BY_MODE[mode]=passes

pd.DataFrame(ITER_ROWS).to_csv(OUTDIR/'ITERATION_AUDIT.csv',index=False)

# Totals/monthly for current, H350 and converged selective policies.
summary_trades={
    'CURRENT_PPABC':current,
    'H350_BASE':h350,
    'H350_EXT2_FINAL':FINAL_BY_MODE['EXT2'],
    'H350_EXT3_FINAL':FINAL_BY_MODE['EXT3'],
}
total_rows=[]; monthly_rows=[]
for name,t in summary_trades.items():
    hard=t[pd.to_numeric(t.hard3,errors='coerce').fillna(0).astype(int).eq(1)]
    rs=t.policy_reason.astype(str)
    total_rows.append(dict(
        policy=name,N=len(t),NET=float(t.net_pct.sum()),
        DELTA_VS_CURRENT=float(t.net_pct.sum())-current_net,
        DELTA_VS_H350=float(t.net_pct.sum())-h350_net,
        STOPLIKE=int(pd.to_numeric(t.stop_like,errors='coerce').fillna(0).sum()),
        HARD_N=len(hard),HARD_NET=float(hard.net_pct.sum()),
        EXT2_PASS=int(rs.str.contains('HCUT350_EXT2_PASS',regex=False).sum()),
        EXT3_PASS=int(rs.str.contains('HCUT350_EXT3_PASS',regex=False).sum()),
        HARD_AVG1=int(rs.str.contains('HARD_S1_AVG_RECOVERY_ATTEMPT_1',regex=False).sum()),
        HARD_AVG2=int(rs.str.contains('HARD_S1_AVG_RECOVERY_ATTEMPT_2',regex=False).sum()),
        HARD_MAX2=int(rs.str.contains('HARD_S1_ADD2_FALSE_BREAK',regex=False).sum()),
        WORST_MONTH_NET=float(t.groupby('month').net_pct.sum().min()),
        POS_MONTHS=int((t.groupby('month').net_pct.sum()>0).sum()),
    ))
    for mo,g in t.groupby('month'):
        monthly_rows.append(dict(
            policy=name,month=mo,N=len(g),NET=float(g.net_pct.sum()),
            DELTA_VS_CURRENT=float(g.net_pct.sum())-float(current[current.month.eq(mo)].net_pct.sum()),
            DELTA_VS_H350=float(g.net_pct.sum())-float(h350[h350.month.eq(mo)].net_pct.sum())
        ))

totals=pd.DataFrame(total_rows)
monthly=pd.DataFrame(monthly_rows)
totals.to_csv(OUTDIR/'EXACT_POLICY_TOTALS.csv',index=False)
monthly.to_csv(OUTDIR/'EXACT_POLICY_MONTHLY.csv',index=False)

# Gate details from the converged policies.
gate_rows=[]
for mode,t in FINAL_BY_MODE.items():
    rs=t.policy_reason.astype(str)
    g=t[rs.str.contains(f'HCUT350_{mode}_',regex=False)].copy()
    for _,r in g.iterrows():
        gate_rows.append(dict(
            mode=mode,setup_id=str(r.setup_id),symbol=str(r.symbol),month=str(r.month),
            passed=int(f'HCUT350_{mode}_PASS' in str(r.policy_reason)),
            result=str(r.result),net_pct=float(r.net_pct),policy_reason=str(r.policy_reason),
            exit_time_utc=str(r.exit_time_utc)
        ))
pd.DataFrame(gate_rows).to_csv(OUTDIR/'SELECTIVE_GATE_DETAIL.csv',index=False)

# Ledger diffs versus H350 baseline for slot/replacement effects.
base_idx=h350.set_index('setup_id',drop=False)
for mode,t in FINAL_BY_MODE.items():
    idx2=t.set_index('setup_id',drop=False)
    add_ids=idx2.index.difference(base_idx.index)
    drop_ids=base_idx.index.difference(idx2.index)
    common=idx2.index.intersection(base_idx.index)
    added=idx2.loc[add_ids].copy() if len(add_ids) else idx2.iloc[:0].copy()
    dropped=base_idx.loc[drop_ids].copy() if len(drop_ids) else base_idx.iloc[:0].copy()
    changed=[]
    for sid in common:
        a=base_idx.loc[sid]; b=idx2.loc[sid]
        if isinstance(a,pd.DataFrame) or isinstance(b,pd.DataFrame):
            raise RuntimeError('DUPLICATE setup_id '+str(sid))
        if abs(float(a.net_pct)-float(b.net_pct))>1e-9 or str(a.result)!=str(b.result) or str(a.policy_reason)!=str(b.policy_reason):
            changed.append(dict(
                setup_id=sid,symbol=str(b.symbol),month=str(b.month),
                base_result=str(a.result),new_result=str(b.result),
                base_net=float(a.net_pct),new_net=float(b.net_pct),
                delta=float(b.net_pct)-float(a.net_pct),
                base_reason=str(a.policy_reason),new_reason=str(b.policy_reason),
            ))
    added.to_csv(OUTDIR/f'LEDGER_ADDED_{mode}.csv',index=False)
    dropped.to_csv(OUTDIR/f'LEDGER_DROPPED_{mode}.csv',index=False)
    pd.DataFrame(changed).to_csv(OUTDIR/f'LEDGER_COMMON_CHANGED_{mode}.csv',index=False)
    say('LEDGER',mode,'ADDED',len(added),'DROP',len(dropped),'CHANGED',len(changed),
        'ADDED_NET',round(float(added.net_pct.sum()) if len(added) else 0.0,6),
        'DROP_NET',round(float(dropped.net_pct.sum()) if len(dropped) else 0.0,6))

# Focused audit: the original H350 cut cohort and what final EXT2/EXT3 do.
cut_ids=set(h350.loc[
    h350.policy_reason.astype(str).str.contains('HARD_S1_PRE_RB1_WATCH_CUT_3.50PCT',regex=False),
    'setup_id'
].astype(str))
audit=[]
for sid in sorted(cut_ids):
    b=h350[h350.setup_id.astype(str).eq(sid)]
    if len(b)!=1: continue
    br=b.iloc[0]
    row=dict(setup_id=sid,symbol=str(br.symbol),month=str(br.month),
             H350_result=str(br.result),H350_net=float(br.net_pct),H350_reason=str(br.policy_reason))
    for mode,t in FINAL_BY_MODE.items():
        q=t[t.setup_id.astype(str).eq(sid)]
        if len(q)==1:
            rr=q.iloc[0]
            row[mode+'_present']=1
            row[mode+'_result']=str(rr.result)
            row[mode+'_net']=float(rr.net_pct)
            row[mode+'_delta']=float(rr.net_pct)-float(br.net_pct)
            row[mode+'_reason']=str(rr.policy_reason)
        else:
            row[mode+'_present']=0
            row[mode+'_result']=''
            row[mode+'_net']=np.nan
            row[mode+'_delta']=np.nan
            row[mode+'_reason']='DROPPED_BY_SCHEDULER'
    audit.append(row)
pd.DataFrame(audit).to_csv(OUTDIR/'H350_CUT15_SELECTIVE_AUDIT.csv',index=False)

lines=[]
lines.append('FINAL P HARD -3.5 SELECTIVE EXTENSION EXACT V25 — 2026-10-06')
lines.append('EXT2: prior24H net >= +2.0%p AND prior6H stop-like <=1.')
lines.append('EXT3: EXT2 AND prior6H net >= -2.0%p.')
lines.append('Context uses strictly prior realized exits and is iterated until stable (max 5 passes).')
lines.append('')
lines.append('TOTALS')
for _,r in totals.iterrows():
    lines.append(
        f"{r.policy}: N={int(r.N)} NET={r.NET:.6f} "
        f"DELTA_CURRENT={r.DELTA_VS_CURRENT:+.6f} DELTA_H350={r.DELTA_VS_H350:+.6f} "
        f"HARD_NET={r.HARD_NET:.6f} WORST_MONTH={r.WORST_MONTH_NET:.6f}"
    )
lines.append('')
lines.append('ITERATIONS')
for r in ITER_ROWS:
    lines.append(
        f"{r['policy']}: NET={r['NET']:.6f} DELTA_H350={r['DELTA_VS_H350']:+.6f} "
        f"PASS={r['EXT_PASS']} FAIL={r['EXT_FAIL']} STABLE={r['STABLE']}"
    )
lines.append('')
lines.append('MONTHLY')
for name in summary_trades:
    lines.append('['+name+']')
    for _,r in monthly[monthly.policy.eq(name)].iterrows():
        lines.append(
            f"{r.month}: NET={r.NET:.6f} DELTA_CURRENT={r.DELTA_VS_CURRENT:+.6f} "
            f"DELTA_H350={r.DELTA_VS_H350:+.6f}"
        )

(OUTDIR/'SUMMARY.txt').write_text('\n'.join(lines),encoding='utf-8')
(OUTDIR/'RUN_LOG.txt').write_text('\n'.join(LOG),encoding='utf-8')

with zipfile.ZipFile(OUTZIP,'w',zipfile.ZIP_DEFLATED) as z:
    for p in sorted(OUTDIR.iterdir()):
        if p.is_file():
            z.write(p,p.name)
say('RESULT ZIP',OUTZIP,'SIZE',OUTZIP.stat().st_size)
say('=== V25 DONE ===')
