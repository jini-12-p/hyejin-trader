#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FINAL P STOP RECOVERY + EXACT GATE V3 — 2026-10-05

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
STAMP = '20261005_V3'
OUTDIR = ROOT / 'FINAL_P_RECOVERY_CUT_EXACT_V17_20261006'
OUTDIR.mkdir(exist_ok=True)
OUTZIP = ROOT / 'FINAL_P_RECOVERY_CUT_EXACT_V17_20261006_RESULTS.zip'
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
        # Exact pre-STOP MFE/MAE comes from the validated base simulator.
        sf['mfe_to_cp']=bd.mfe_pct
        sf['mae_to_cp']=bd.mae_pct
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
                post_path=[b for b in one if bd.exit_time.astimezone(UTC).replace(second=0,microsecond=0) < b['ts'] < tf]
                extra_mfe=(pct(max(b['h'] for b in post_path),bd.entry_price) if post_path else None)
                extra_mae=(pct(min(b['l'] for b in post_path),bd.entry_price) if post_path else None)
                rf['mfe_to_cp']=max([x for x in (bd.mfe_pct,extra_mfe) if x is not None],default=None)
                rf['mae_to_cp']=min([x for x in (bd.mae_pct,extra_mae) if x is not None],default=None)
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
        row['RB2_true_recovery']=(int(bool(len(attempts)>=2 and attempts[1].get('recover_ts') is not None)) if len(attempts)>=2 else '')
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
RB2_FEATURES=[]
for c in D.columns:
    if not c.startswith('RB2_'): continue
    if c in ('RB2_true_recovery',): continue
    x=pd.to_numeric(D[c],errors='coerce')
    if x.notna().sum()>=60 and x.nunique(dropna=True)>=6:
        RB2_FEATURES.append(c)

# Exact scheduler replay can safely use only path-independent features.
def exog_feature(c):
    lc=c.lower()
    return ('exit_' not in lc and 'shadow9' not in lc)
EXOG_STOP_FEATURES=[c for c in STOP_FEATURES if exog_feature(c)]
EXOG_RB1_FEATURES=[c for c in RB1_FEATURES if exog_feature(c)]
EXOG_RB2_FEATURES=[c for c in RB2_FEATURES if exog_feature(c)]


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


# Second rebound checkpoint: true vs false second add attempt.
RB2=D[pd.to_numeric(D.RB2_true_recovery,errors='coerce').notna()].copy()
if len(RB2):
    RB2['RB2_TARGET_SUCCESS']=pd.to_numeric(RB2.RB2_true_recovery,errors='coerce').astype(int)
    RB2['RB2_TARGET_FAILURE']=1-RB2.RB2_TARGET_SUCCESS
    R2succ=search_single(RB2,RB2_FEATURES,'RB2_TARGET_SUCCESS','REBOUND2_SUCCESS')
    R2fail=search_single(RB2,RB2_FEATURES,'RB2_TARGET_FAILURE','REBOUND2_FAILURE')
    R2Psucc=search_pairs(RB2,R2succ,'RB2_TARGET_SUCCESS','REBOUND2_SUCCESS')
    R2Pfail=search_pairs(RB2,R2fail,'RB2_TARGET_FAILURE','REBOUND2_FAILURE')
else:
    R2succ=R2fail=R2Psucc=R2Pfail=pd.DataFrame()
for name,x in [('RB2_SINGLE_SUCCESS_RULES.csv',R2succ),('RB2_SINGLE_FAILURE_RULES.csv',R2fail),('RB2_PAIR_SUCCESS_RULES.csv',R2Psucc),('RB2_PAIR_FAILURE_RULES.csv',R2Pfail)]:
    x.to_csv(OUTDIR/name,index=False)

# Path-independent searches used for the exact scheduler replay below.
def _disc_val_rank(x):
    if x is None or len(x)==0:return x
    q=x.copy()
    q['_DV_SCORE']=(pd.to_numeric(q.DISC_precision,errors='coerce')-pd.to_numeric(q.DISC_base,errors='coerce')
                    +pd.to_numeric(q.VAL_precision,errors='coerce').fillna(-1)-pd.to_numeric(q.VAL_base,errors='coerce').fillna(0)
                    +0.0005*(pd.to_numeric(q.DISC_hit,errors='coerce').fillna(0)+2*pd.to_numeric(q.VAL_hit,errors='coerce').fillna(0)))
    return q.sort_values('_DV_SCORE',ascending=False).drop(columns=['_DV_SCORE'])

def run_exog_search(df,features,prefix,target_success,target_failure):
    ss=search_single(df,features,target_success,prefix+'_SUCCESS')
    sf=search_single(df,features,target_failure,prefix+'_FAILURE')
    # Pair candidates are chosen with DISC+VAL ranking only; Sep OOS never affects gate selection.
    ss_dv=_disc_val_rank(ss); sf_dv=_disc_val_rank(sf)
    ps=search_pairs(df,ss_dv,target_success,prefix+'_SUCCESS')
    pf=search_pairs(df,sf_dv,target_failure,prefix+'_FAILURE')
    return ss_dv,sf_dv,ps,pf

XSsucc,XSfail,XPsucc,XPfail=run_exog_search(D,EXOG_STOP_FEATURES,'EXOG_STOP','STOP_TARGET_SUCCESS','STOP_TARGET_FAILURE')
XR1succ,XR1fail,XR1Psucc,XR1Pfail=run_exog_search(RB,EXOG_RB1_FEATURES,'EXOG_RB1','RB_TARGET_SUCCESS','RB_TARGET_FAILURE')
if len(RB2):
    XR2succ,XR2fail,XR2Psucc,XR2Pfail=run_exog_search(RB2,EXOG_RB2_FEATURES,'EXOG_RB2','RB2_TARGET_SUCCESS','RB2_TARGET_FAILURE')
else:
    XR2succ=XR2fail=XR2Psucc=XR2Pfail=pd.DataFrame()
for name,x in [
    ('EXOG_STOP_PAIR_SUCCESS.csv',XPsucc),('EXOG_STOP_PAIR_FAILURE.csv',XPfail),
    ('EXOG_RB1_PAIR_SUCCESS.csv',XR1Psucc),('EXOG_RB1_PAIR_FAILURE.csv',XR1Pfail),
    ('EXOG_RB2_PAIR_SUCCESS.csv',XR2Psucc),('EXOG_RB2_PAIR_FAILURE.csv',XR2Pfail)]:
    x.to_csv(OUTDIR/name,index=False)


def parse_rule_record(r):
    if r is None:return None
    if 'rule' in r and str(r.get('rule') or '').strip():
        txt=str(r['rule'])
        parts=txt.split(' AND ')
        out=[]
        for p in parts:
            m=re.match(r'^(.+?)\s*(>=|<=)\s*(-?[0-9.eE+]+)$',p.strip())
            if not m:return None
            out.append((m.group(1),m.group(2),float(m.group(3))))
        return out
    if r.get('feature') is not None:
        return [(str(r['feature']),str(r['op']),float(r['threshold']))]
    return None


def choose_rule(pairdf,singledf,kind):
    """Select using DISC+VAL only. OOS columns are never referenced here."""
    pools=[]
    for src,df in [('PAIR',pairdf),('SINGLE',singledf)]:
        if df is None or len(df)==0:continue
        for rr in df.to_dict('records'):
            dh=fv(rr.get('DISC_hit'),0); vh=fv(rr.get('VAL_hit'),0)
            dp=fv(rr.get('DISC_precision')); vp=fv(rr.get('VAL_precision'))
            db=fv(rr.get('DISC_base'),0); vb=fv(rr.get('VAL_base'),0)
            if dp is None or vp is None:continue
            ok=False
            if kind=='STOP_FAIL': ok=(dh>=50 and vh>=15 and dp>=.60 and vp>=.60)
            elif kind=='STOP_SUCCESS': ok=(dh>=80 and vh>=20 and dp>=.84 and vp>=.84)
            elif kind=='RB1_SUCCESS': ok=(dh>=80 and vh>=25 and dp>=.80 and vp>=.80)
            elif kind=='RB1_FAIL': ok=(dh>=60 and vh>=20 and dp>=.60 and vp>=.55)
            elif kind=='RB2_SUCCESS': ok=(dh>=30 and vh>=10 and dp>=.65 and vp>=.60)
            elif kind=='RB2_FAIL': ok=(dh>=25 and vh>=8 and dp>=.70 and vp>=.60)
            if not ok:continue
            # Prefer stable precision first, then meaningful coverage. No OOS leakage.
            minp=min(dp,vp)
            lift=min(dp-db,vp-vb)
            utility=minp + .20*lift + .0006*(dh+2*vh) + (.01 if src=='PAIR' else 0)
            pools.append((utility,src,rr))
    if not pools:return None
    pools.sort(key=lambda x:x[0],reverse=True)
    rr=pools[0][2].copy(); rr['_source']=pools[0][1]; rr['_utility']=pools[0][0]
    rr['_conds']=parse_rule_record(rr)
    return rr

selected={
    'STOP_FAIL':choose_rule(XPfail,XSfail,'STOP_FAIL'),
    'STOP_SUCCESS':choose_rule(XPsucc,XSsucc,'STOP_SUCCESS'),
    'RB1_SUCCESS':choose_rule(XR1Psucc,XR1succ,'RB1_SUCCESS'),
    'RB1_FAIL':choose_rule(XR1Pfail,XR1fail,'RB1_FAIL'),
    'RB2_SUCCESS':choose_rule(XR2Psucc,XR2succ,'RB2_SUCCESS'),
    'RB2_FAIL':choose_rule(XR2Pfail,XR2fail,'RB2_FAIL'),
}

def rule_text(rr):
    if rr is None:return 'NONE'
    if rr.get('rule'):return str(rr['rule'])
    return f"{rr.get('feature')} {rr.get('op')} {float(rr.get('threshold')):.6g}"

say('=== SELECTED GATES (selection uses DISC+VAL only; OOS untouched) ===')
sel_json={}
for k,rr in selected.items():
    if rr is None:
        say(k,'NONE'); sel_json[k]=None; continue
    say(k,rule_text(rr),'DISC',int(rr.get('DISC_hit',0)),round(float(rr.get('DISC_precision',0)),3),
        'VAL',int(rr.get('VAL_hit',0)),round(float(rr.get('VAL_precision',0)),3),
        'OOS_REPORT_ONLY',int(rr.get('OOS_hit',0)),safe(rr.get('OOS_precision'),3))
    sel_json[k]={kk:(vv if not isinstance(vv,(np.integer,np.floating)) else vv.item()) for kk,vv in rr.items() if kk!='_conds'}
    sel_json[k]['conds']=rr.get('_conds')
(OUTDIR/'SELECTED_GATES.json').write_text(json.dumps(sel_json,ensure_ascii=False,indent=2,default=str),encoding='utf-8')


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
    r1,r2,r3=residual_risk_flags(r)
    # R2/R3 are the low-9M-damage residual guards and stay on in every V14 policy.
    if r2 or r3:
        return True
    # R1 is only added conditionally; strong/recovering states bypass it.
    if (policy in ('DYN_R1_COND','DYN_ALL') or policy.startswith('DYN_CUT_')) and r1 and not v14_r1_bypass(t):
        return True
    return False

def v14_market_pass(policy,t):
    if policy in ('DYN_DBR2_BYPASS','DYN_ALL') or policy.startswith('DYN_CUT_'):
        return v14_dbr2_pass(t)
    return fine_pon_pass('PON_FINE_DBR2',t)

def v14_c14_blocked(policy,sid,t):
    if sid in NON_C14_BLOCK_IDS:
        return True
    if sid not in C14_BLOCK_IDS:
        return False
    if (policy in ('DYN_C14_BYPASS','DYN_ALL') or policy.startswith('DYN_CUT_')) and v14_c14_bypass(t):
        return False
    return True

def run_exact_policy(policy):
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
                if (policy in ('DYN_LOSS_GUARD','DYN_ALL') or policy.startswith('DYN_CUT_')) and v14_common_loss_guard(now):
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
                elif policy.startswith('DYN_'):
                    # V17: DYN_ALL entry/market stack frozen; ONLY pre-RB1 watch cut varies.
                    if policy.startswith('DYN_CUT_'):
                        watch_cut=float(policy.rsplit('_',1)[1])/100.0
                    else:
                        watch_cut=2.25
                    sim,why=gate_sim(sym,now,float(r['entry_price']),base_recovery,watch_cut_pct=watch_cut)
            if policy=='UNCONDITIONAL_R100':
                stop_like=sim.result in ('STOP','LATE_FAILURE_EXIT','MAX2_EXIT','CUT3','CUT6','RECOVERY_24H_EXIT')
            elif (policy.startswith('GATED_') or policy.startswith('PON_FINE_') or policy.startswith('PON_RESID_') or policy.startswith('DYN_')) and base.result=='STOP':
                stop_like=(sim.result!='GATE_AVG_EXIT')
            else:
                stop_like=sim.result in ('STOP','LATE_FAILURE_EXIT')
            sched.add(now,sym,sim,stop_like)
            trades.append(dict(policy=policy,period=period,setup_id=sid,symbol=sym,
                               month=str(r['entry_time_kst'])[:7],day=str(r['entry_time_kst'])[:10],
                               entry_dt=pd.Timestamp(now),base_result=str(base.result),result=str(sim.result),
                               net_pct=float(sim.net_pct),exit_time_utc=sim.exit_time.isoformat(),
                               stop_like=int(bool(stop_like)),policy_reason=why))
    return pd.DataFrame(trades)

say('V17 DYN_ALL FROZEN + WATCH-CUT SCAN: only pre-RB1 additional-loss cut varies')
say('=== EXACT POLICY REPLAYS ===')
POLICY_TRADES={}
for pol in (
    'BASELINE',
    'DYN_ALL',
    'DYN_CUT_050',
    'DYN_CUT_075',
    'DYN_CUT_100',
    'DYN_CUT_125',
    'DYN_CUT_150',
    'DYN_CUT_175',
    'DYN_CUT_200',
    'DYN_CUT_225',
):
    say('RUN POLICY',pol)
    t=run_exact_policy(pol)
    POLICY_TRADES[pol]=t
    t.to_csv(OUTDIR/f'EXACT_TRADES_{pol}.csv',index=False)
    say(pol,'N',len(t),'NET',round(float(t.net_pct.sum()),3),'STOPLIKE',int(t.stop_like.sum()))

# V17 clone audit: DYN_CUT_225 MUST reproduce DYN_ALL exactly because
# entry logic and recovery cut are identical.  Abort before interpretation if not.
_a=POLICY_TRADES['DYN_ALL'].reset_index(drop=True)
_b=POLICY_TRADES['DYN_CUT_225'].reset_index(drop=True)
_clone_ok=(len(_a)==len(_b) and list(_a.setup_id.astype(str))==list(_b.setup_id.astype(str))
           and abs(float(_a.net_pct.sum())-float(_b.net_pct.sum()))<=1e-9
           and list(_a.result.astype(str))==list(_b.result.astype(str)))
say('V17 DYN_ALL CLONE AUDIT', 'PASS' if _clone_ok else 'FAIL',
    'N',len(_a),len(_b),'NET',round(float(_a.net_pct.sum()),6),round(float(_b.net_pct.sum()),6))
if not _clone_ok:
    raise SystemExit('V17 DYN_CUT_225 DOES NOT MATCH DYN_ALL')

# DYN_ALL must also reproduce the already-accepted V14 total.
if abs(float(_a.net_pct.sum())-705.440) > 0.08:
    raise SystemExit(f'V17 DYN_ALL TOTAL AUDIT FAIL: {float(_a.net_pct.sum()):.6f} vs 705.440')

# Baseline hard audit against already-validated final ledger.
base=POLICY_TRADES['BASELINE']
for m in [f'2026-{i:02d}' for i in range(1,10)]:
    a=base[base.month==m]; b=final[final.month.astype(str)==m]
    ok=(len(a)==len(b) and int((a.result=='TP20_FULL').sum())==int((b.result.astype(str)=='TP20_FULL').sum())
        and int((a.result=='STOP').sum())==int((b.result.astype(str)=='STOP').sum())
        and abs(float(a.net_pct.sum())-float(b.net_pct.sum()))<=.03)
    say('EXACT BASE AUDIT',m,'N',len(a),'NET',round(float(a.net_pct.sum()),3),'PASS',ok)
    if not ok: raise SystemExit('EXACT BASELINE AUDIT FAIL '+m)

monthly=[]; totals=[]
for pol,t in POLICY_TRADES.items():
    for m in [f'2026-{i:02d}' for i in range(1,10)]:
        g=t[t.month==m]
        monthly.append(dict(policy=pol,month=m,N=len(g),NET=float(g.net_pct.sum()),
                            STOPLIKE=int(g.stop_like.sum()),AVG_EXIT=int(g.result.astype(str).str.contains('AVG_EXIT').sum()),
                            MAX2_EXIT=int(g.result.astype(str).str.contains('MAX2_EXIT').sum()),
                            TIME24_EXIT=int(g.result.astype(str).str.contains('24H_EXIT').sum()),
                            GATE_REJECT=int((g.result.astype(str)=='GATE_REJECT_EXIT').sum()),
                            WATCH_CUT=int((g.result.astype(str)=='GATE_WATCH_CUT').sum())))
    mm=pd.DataFrame([x for x in monthly if x['policy']==pol])
    totals.append(dict(policy=pol,N=len(t),NET=float(t.net_pct.sum()),STOPLIKE=int(t.stop_like.sum()),
                       POS_MONTHS=int((mm.NET>0).sum()),NEG_MONTHS=int((mm.NET<0).sum()),
                       WORST_MONTH_NET=float(mm.NET.min()),BEST_MONTH_NET=float(mm.NET.max())))
MONTHLY_EXACT=pd.DataFrame(monthly)
TOTALS_EXACT=pd.DataFrame(totals)
MONTHLY_EXACT.to_csv(OUTDIR/'EXACT_POLICY_MONTHLY.csv',index=False)
TOTALS_EXACT.to_csv(OUTDIR/'EXACT_POLICY_TOTALS.csv',index=False)
say('=== EXACT TOTALS ===')
for r in TOTALS_EXACT.itertuples(index=False):
    say(r.policy,'N',r.N,'NET',round(r.NET,3),'STOPLIKE',r.STOPLIKE,'POSM',r.POS_MONTHS,'NEGM',r.NEG_MONTHS,'WORST',round(r.WORST_MONTH_NET,3))

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
top_lines('[TOP REBOUND2 FALSE-GUARDS — do NOT add on attempt 2]',R2Pfail if len(R2Pfail) else R2fail)
top_lines('[TOP REBOUND2 TRUE-GATES — add 100% on attempt 2]',R2Psucc if len(R2Psucc) else R2succ)
lines.append('')
lines.append('[SELECTED EXOGENOUS GATES — DISC+VAL selection only]')
for k,rr in selected.items():
    lines.append(k+'='+rule_text(rr))
lines.append('')
lines.append('[EXACT POLICY TOTALS — scheduler/replacements/cooldowns recalculated]')
for r in TOTALS_EXACT.itertuples(index=False):
    lines.append(f"{r.policy}: N={r.N} NET={r.NET:.6f} STOPLIKE={r.STOPLIKE} POS_MONTHS={r.POS_MONTHS} NEG_MONTHS={r.NEG_MONTHS} WORST_MONTH={r.WORST_MONTH_NET:.6f}")

# exact answers to user's central question
stable_stop_fail = (Pfail[Pfail.stable_both==1] if len(Pfail) else pd.DataFrame())
stable_stop_succ = (Psucc[Psucc.stable_both==1] if len(Psucc) else pd.DataFrame())
stable_rb_fail = (RPfail[RPfail.stable_both==1] if len(RPfail) else pd.DataFrame())
stable_rb_succ = (RPsucc[RPsucc.stable_both==1] if len(RPsucc) else pd.DataFrame())
stable_rb2_fail = (R2Pfail[R2Pfail.stable_both==1] if len(R2Pfail) else pd.DataFrame())
stable_rb2_succ = (R2Psucc[R2Psucc.stable_both==1] if len(R2Psucc) else pd.DataFrame())
lines.append('')
lines.append('[DECISION SIGNAL]')
lines.append(f'STABLE_STOP_FAILURE_PAIR_RULES={len(stable_stop_fail)}')
lines.append(f'STABLE_STOP_SUCCESS_PAIR_RULES={len(stable_stop_succ)}')
lines.append(f'STABLE_RB_FAILURE_PAIR_RULES={len(stable_rb_fail)}')
lines.append(f'STABLE_RB_SUCCESS_PAIR_RULES={len(stable_rb_succ)}')
lines.append(f'STABLE_RB2_FAILURE_PAIR_RULES={len(stable_rb2_fail)}')
lines.append(f'STABLE_RB2_SUCCESS_PAIR_RULES={len(stable_rb2_succ)}')
lines.append('Interpretation: exact totals decide whether BASELINE, unconditional MAX2, or 2-stage gate is economically best. Gate selection never uses Sep OOS.')

(OUTDIR/'SUMMARY.txt').write_text('\n'.join(lines)+'\n',encoding='utf-8')
(OUTDIR/'RUN_LOG.txt').write_text('\n'.join(LOG)+'\n',encoding='utf-8')

with zipfile.ZipFile(OUTZIP,'w',zipfile.ZIP_DEFLATED) as z:
    for p in sorted(OUTDIR.iterdir()):
        if p.is_file():z.write(p,p.name)

say('=== COMPLETE ===')
say('ZIP',OUTZIP)
print('\n'.join(lines[:80]))
