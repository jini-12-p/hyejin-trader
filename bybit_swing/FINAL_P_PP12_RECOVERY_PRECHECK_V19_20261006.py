#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FINAL P PP12 RECOVERY PRECHECK V19 — 2026-10-06

Purpose
-------
Target ONLY the final exact DYN_HARD_STOP ledger's PROFIT_PROTECT_EXIT (PP12)
trades and answer the question:

    "If PP12 had NOT fully exited, could the same cyclic +1.5% rebound / max-2
     recovery structure used for STOP trades reduce PP12 losses?"

This is a COHORT PRECHECK, not a portfolio exact replay.
It intentionally leaves the final entry stack / scheduler untouched.

Frozen recovery mechanics
-------------------------
- PP12 exit is counterfactually cancelled.
- Track a new swing low from the PP12 exit point using future 1m bars.
- When price rebounds +1.5% from that swing low, add equal quantity.
- New average = (original entry + add price) / 2 for equal-size core/add.
- If old swing low rebreaks before average recovery, close the add at old low.
- Maximum 2 add attempts.
- Conservative same-minute ordering: old-low rebreak wins over avg recovery.
- 24h horizon after PP12 exit.
- Watch-cut grid is applied ONLY before the first rebound/add, matching the
  existing STOP Recovery safety-cut convention.

Two counterfactual position models are reported
-----------------------------------------------
RESET100:
    Exact analogue of the STOP Recovery research convention: ignore any prior
    partial exits and assume the original core 100% is still open at PP12.

KEEP_REMAINING:
    More literal PP-only replacement: preserve any fills that occurred before
    the PP12 fill and keep only the quantity that PP12 would have closed.
    Recovery add size equals that remaining core quantity.

Anti-lookahead / split
----------------------
- No feature/gate mining in this script.
- Discovery report: Jan-Jun
- Validation report: Jul-Aug
- OOS report: Sep
- Future bars are used only to label the counterfactual PP outcomes.

Safety
------
- No bot.py edit
- No DB write
- No order
- Public Bybit kline read only + local cache files
"""
from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from collections import Counter, defaultdict
import csv
import json
import math
import runpy
import zipfile

import pandas as pd

ROOT = Path('/root/hyejin-trader/bybit_swing')
OUTDIR = ROOT / 'FINAL_P_PP12_RECOVERY_PRECHECK_V19_20261006'
OUTDIR.mkdir(exist_ok=True)
OUTZIP = ROOT / 'FINAL_P_PP12_RECOVERY_PRECHECK_V19_20261006_RESULTS.zip'
CACHE = ROOT / '.final_stop_recovery_gate_cache'  # reuse V18 cache
UTC = timezone.utc
KST = timezone(timedelta(hours=9))
REBOUND_PCT = 1.50
MAX_ATTEMPTS = 2
HORIZON_H = 24
CUT_GRID = [None, 0.50, 0.75, 1.00, 1.25, 1.50, 1.75, 2.00, 2.25, 2.50, 3.00]
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
    t = pd.Timestamp(v)
    if t.tzinfo is None:
        t = t.tz_localize(UTC)
    else:
        t = t.tz_convert(UTC)
    return t.to_pydatetime()


def split_name(month):
    m = str(month)
    if m <= '2026-06': return 'DISCOVERY_0106'
    if m <= '2026-08': return 'VALIDATION_0708'
    return 'OOS_09'


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


def cut_tag(c):
    return 'NONE' if c is None else f'{c:.2f}'


# ---------------------------------------------------------------------------
# 1) Load validated engine + V18 exact winner ledger
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

U.CACHE_DIR = CACHE
U.CACHE_DIR.mkdir(exist_ok=True)
U.KC = U.KlineCache()

V18ZIP = ROOT / 'FINAL_P_HARD_SELECTIVE_EXACT_V18_20261006_RESULTS.zip'
if not V18ZIP.exists():
    raise SystemExit('MISSING '+str(V18ZIP))
with zipfile.ZipFile(V18ZIP) as z:
    ledger = pd.read_csv(z.open('EXACT_TRADES_DYN_HARD_STOP.csv'), low_memory=False)

pp = ledger[ledger['result'].astype(str).eq('PROFIT_PROTECT_EXIT')].copy().reset_index(drop=True)
pp['month'] = pp['month'].astype(str)
base_pp_n = len(pp)
base_pp_net = float(pd.to_numeric(pp['net_pct'], errors='coerce').sum())
say('V18 PP12 COHORT', base_pp_n, 'BASE_NET', f'{base_pp_net:.6f}')
if base_pp_n != 251:
    raise SystemExit(f'PP12 COUNT AUDIT FAIL {base_pp_n} != 251')
if abs(base_pp_net - (-407.12125229407195)) > 0.10:
    raise SystemExit(f'PP12 NET AUDIT FAIL {base_pp_net:.6f}')

ja_idx = {str(r['setup_id']): r for _, r in ja.iterrows()}
se_idx = {str(r['setup_id']): r for _, r in se.iterrows()}


def candidate_row(period, sid):
    return ja_idx.get(str(sid)) if str(period) == 'JA' else se_idx.get(str(sid))


def entry_dt(period, r):
    if str(period) == 'JA':
        return pd.Timestamp(r['_dt']).to_pydatetime()
    return r['_dt']


def post_bars(symbol, start, hours=HORIZON_H):
    st = start.astimezone(UTC).replace(second=0, microsecond=0)
    en = st + timedelta(hours=hours, minutes=3)
    df = U.KC.get(symbol, '1m', st - timedelta(minutes=2), en)
    if df is None or len(df) == 0:
        return pd.DataFrame()
    q = df.copy()
    q['ts'] = pd.to_datetime(q['ts'], utc=True, format='mixed')
    q = q[(q['ts'] > pd.Timestamp(st)) & (q['ts'] <= pd.Timestamp(start + timedelta(hours=hours)))].copy()
    q = q.sort_values('ts').drop_duplicates('ts')
    return q


@dataclass
class SeedState:
    model: str
    core_qty: float
    prior_gross: float
    prior_exit_fee: float
    original_entry_fee: float


def seed_from_base(sim, entry):
    # The PP12 fill closes what remains. Anything before it is a pre-PP fill.
    pp_fills = [x for x in list(sim.fills or []) if str(getattr(x, 'kind', '')) == 'PP12']
    pre = [x for x in list(sim.fills or []) if str(getattr(x, 'kind', '')) != 'PP12']
    if not pp_fills:
        # Defensive fallback: terminal fill is expected to be PP12.
        if sim.fills:
            pp_fills = [sim.fills[-1]]
            pre = list(sim.fills[:-1])
    qrem = sum(float(getattr(x, 'qty_mult', 0.0)) for x in pp_fills)
    prior_gross = sum(float(x.qty_mult) * (float(x.price)/entry - 1.0) * 100.0 for x in pre)
    prior_exit_fee = sum(U.FEE_PCT * float(x.qty_mult) * (float(x.price)/entry) for x in pre)
    return (
        SeedState('RESET100', 1.0, 0.0, 0.0, U.FEE_PCT),
        SeedState('KEEP_REMAINING', qrem, prior_gross, prior_exit_fee, U.FEE_PCT),
        qrem, pre, pp_fills,
    )


def raw_path_labels(bars, entry, pp_px, start):
    out = {
        'entry_recovered_24h': 0, 'tp20_reached_24h': 0,
        'entry_recovery_min': None, 'tp20_min': None,
        'post_low_entry_pct': None, 'post_high_entry_pct': None,
        'post_low_from_pp_pct': None, 'post_high_from_pp_pct': None,
        'close24_entry_pct': None,
    }
    if bars is None or len(bars) == 0:
        return out
    lows = pd.to_numeric(bars['low'], errors='coerce')
    highs = pd.to_numeric(bars['high'], errors='coerce')
    closes = pd.to_numeric(bars['close'], errors='coerce')
    lo = float(lows.min()); hi = float(highs.max()); cl = float(closes.iloc[-1])
    out['post_low_entry_pct'] = (lo/entry-1)*100
    out['post_high_entry_pct'] = (hi/entry-1)*100
    out['post_low_from_pp_pct'] = (lo/pp_px-1)*100
    out['post_high_from_pp_pct'] = (hi/pp_px-1)*100
    out['close24_entry_pct'] = (cl/entry-1)*100
    hit = bars[highs >= entry]
    if len(hit):
        t = pd.Timestamp(hit.iloc[0]['ts']).to_pydatetime().astimezone(UTC) + timedelta(minutes=1)
        out['entry_recovered_24h'] = 1
        out['entry_recovery_min'] = (t-start).total_seconds()/60
    tp = entry*1.02
    hit = bars[highs >= tp]
    if len(hit):
        t = pd.Timestamp(hit.iloc[0]['ts']).to_pydatetime().astimezone(UTC) + timedelta(minutes=1)
        out['tp20_reached_24h'] = 1
        out['tp20_min'] = (t-start).total_seconds()/60
    return out


def simulate_cycle(bars, entry, pp_px, start, seed: SeedState, watch_cut_pct):
    qcore = float(seed.core_qty)
    if qcore <= 1e-12:
        return dict(result='NO_CORE', attempts=0, net_pct=seed.prior_gross-seed.original_entry_fee-seed.prior_exit_fee,
                    gross_pct=seed.prior_gross, fee_pct=seed.original_entry_fee+seed.prior_exit_fee,
                    exit_time=start, terminal_price=pp_px, add_realized=0.0)

    swing_low = float(pp_px if pp_px > 0 else entry)
    low_ts = start.astimezone(UTC).replace(second=0, microsecond=0)
    attempt = 0
    add_open = False
    add_price = 0.0
    add_low = 0.0
    add_bar_ts = None
    add_realized = 0.0  # %p vs original initial notional
    add_entries = []     # (qty, price)
    add_exits = []       # (qty, price)
    result = ''
    exit_t = start
    core_exit = pp_px

    if bars is None or len(bars) == 0:
        return dict(result='DATA_ERROR', attempts=0, net_pct=None, gross_pct=None, fee_pct=None,
                    exit_time=start, terminal_price=pp_px, add_realized=0.0)

    for _, bar in bars.iterrows():
        bt = pd.Timestamp(bar['ts']).to_pydatetime().astimezone(UTC)
        now = bt + timedelta(minutes=1)
        lo = float(bar['low']); hi = float(bar['high']); cl = float(bar['close'])

        if not add_open:
            # Same safety-cut convention as V17/V18: only before first add attempt.
            if attempt == 0 and watch_cut_pct is not None:
                cut_px = pp_px * (1.0 - float(watch_cut_pct)/100.0)
                if lo <= cut_px:
                    core_exit = cut_px; exit_t = now; result = 'WATCH_CUT'
                    break

            if lo < swing_low:
                swing_low = lo; low_ts = bt
                continue
            trigger = swing_low * (1.0 + REBOUND_PCT/100.0)
            if bt <= low_ts or hi < trigger:
                continue
            attempt += 1
            add_open = True
            add_price = trigger
            add_low = swing_low
            add_bar_ts = bt
            add_entries.append((qcore, add_price))
            continue

        if add_bar_ts is not None and bt <= add_bar_ts:
            continue
        avg = (entry + add_price)/2.0
        # Conservative same-minute order: low rebreak first.
        if lo <= add_low:
            add_realized += qcore * (add_low-add_price)/entry*100.0
            add_exits.append((qcore, add_low))
            add_open = False
            if attempt >= MAX_ATTEMPTS:
                core_exit = add_low; exit_t = now; result = 'MAX2_EXIT'
                break
            swing_low = min(lo, add_low); low_ts = bt
            continue
        if hi >= avg:
            add_realized += qcore * (avg-add_price)/entry*100.0
            add_exits.append((qcore, avg))
            add_open = False
            core_exit = avg; exit_t = now; result = 'AVG_EXIT'
            break

    if not result:
        last = bars.iloc[-1]
        cl = float(last['close'])
        exit_t = pd.Timestamp(last['ts']).to_pydatetime().astimezone(UTC) + timedelta(minutes=1)
        if add_open:
            add_realized += qcore * (cl-add_price)/entry*100.0
            add_exits.append((qcore, cl))
            add_open = False
        core_exit = cl
        result = 'RECOVERY_24H_EXIT'

    core_gross = qcore * (core_exit/entry - 1.0)*100.0
    gross = seed.prior_gross + core_gross + add_realized
    fee = seed.original_entry_fee + seed.prior_exit_fee
    for q, px in add_entries:
        fee += U.FEE_PCT * q * (px/entry)
    for q, px in add_exits:
        fee += U.FEE_PCT * q * (px/entry)
    fee += U.FEE_PCT * qcore * (core_exit/entry)
    net = gross - fee
    return dict(result=result, attempts=attempt, net_pct=net, gross_pct=gross, fee_pct=fee,
                exit_time=exit_t, terminal_price=core_exit, add_realized=add_realized)


# ---------------------------------------------------------------------------
# 2) Build PP12 trajectories once; evaluate all cut levels in-memory
# ---------------------------------------------------------------------------
detail_rows = []
policy_rows = []
errors = []

say('=== PP12 POST-EXIT LABEL + RECOVERY GRID ===')
for i, tr in enumerate(pp.to_dict('records'), 1):
    sid = str(tr['setup_id']); period = str(tr['period']); sym = str(tr['symbol'])
    r = candidate_row(period, sid)
    if r is None:
        errors.append((sid, 'candidate missing')); continue
    ent = entry_dt(period, r)
    ep = float(r['entry_price'])
    try:
        sim = U.simulate_base(dict(setup_id=sid, symbol=sym, entry=ent, entry_price=ep))
        if str(sim.result) != 'PROFIT_PROTECT_EXIT':
            raise RuntimeError('base resim='+str(sim.result))
        ledger_net = float(tr['net_pct'])
        if abs(float(sim.net_pct)-ledger_net) > 0.08:
            raise RuntimeError(f'net mismatch sim={sim.net_pct:.6f} ledger={ledger_net:.6f}')
        start = sim.exit_time
        pp_px = float(sim.terminal_price)
        bars = post_bars(sym, start, HORIZON_H)
        if len(bars) == 0:
            raise RuntimeError('no post bars')

        s_reset, s_keep, qrem, pre_fills, pp_fills = seed_from_base(sim, ep)
        raw = raw_path_labels(bars, ep, pp_px, start)
        base_rec = {
            'setup_id':sid,'symbol':sym,'period':period,'month':str(tr['month']),
            'split':split_name(tr['month']),'entry_dt':str(ent),'entry_price':ep,
            'pp_exit_time_utc':start.isoformat(),'pp_exit_price':pp_px,
            'base_pp_net_pct':ledger_net,'base_pp_mfe_pct':fv(sim.mfe_pct),'base_pp_mae_pct':fv(sim.mae_pct),
            'pp_remaining_qty':qrem,'pre_pp_fill_n':len(pre_fills),
            'pre_pp_fill_kinds':'|'.join(str(x.kind) for x in pre_fills),
            **raw,
        }
        detail_rows.append(base_rec)

        for seed in (s_reset, s_keep):
            for cut in CUT_GRID:
                rr = simulate_cycle(bars, ep, pp_px, start, seed, cut)
                policy_rows.append({
                    'setup_id':sid,'symbol':sym,'period':period,'month':str(tr['month']),
                    'split':split_name(tr['month']),'model':seed.model,'cut_pct':cut_tag(cut),
                    'base_pp_net_pct':ledger_net,'result':rr['result'],'attempts':rr['attempts'],
                    'net_pct':rr['net_pct'],'delta_vs_pp_pct':None if rr['net_pct'] is None else rr['net_pct']-ledger_net,
                    'gross_pct':rr['gross_pct'],'fee_pct':rr['fee_pct'],
                    'exit_time_utc':rr['exit_time'].isoformat() if rr.get('exit_time') else '',
                    'terminal_price':rr['terminal_price'],'add_realized_pct':rr['add_realized'],
                })
    except Exception as e:
        errors.append((sid, str(e)))

    if i % 25 == 0 or i == len(pp):
        say('PP', i, '/', len(pp), 'ok', len(detail_rows), 'err', len(errors))

if errors:
    write_csv(OUTDIR/'PP12_ERRORS.csv', [dict(setup_id=a,error=b) for a,b in errors])
if len(detail_rows) != base_pp_n:
    raise SystemExit(f'PP DETAIL COUNT FAIL ok={len(detail_rows)} err={len(errors)} expected={base_pp_n}')

write_csv(OUTDIR/'PP12_FUTURE_PATH.csv', detail_rows)
write_csv(OUTDIR/'PP12_RECOVERY_ALL_TRADES.csv', policy_rows)

# ---------------------------------------------------------------------------
# 3) Summaries
# ---------------------------------------------------------------------------
det = pd.DataFrame(detail_rows)
pol = pd.DataFrame(policy_rows)
for c in ['base_pp_net_pct','net_pct','delta_vs_pp_pct']:
    if c in pol: pol[c] = pd.to_numeric(pol[c], errors='coerce')

path_summary = []
for sp, g in [('ALL',det)] + [(s,det[det['split'].eq(s)]) for s in ['DISCOVERY_0106','VALIDATION_0708','OOS_09']]:
    path_summary.append({
        'split':sp,'n':len(g),
        'entry_recovered_n':int(pd.to_numeric(g['entry_recovered_24h'],errors='coerce').fillna(0).sum()),
        'entry_recovered_rate':float(pd.to_numeric(g['entry_recovered_24h'],errors='coerce').mean()),
        'tp20_reached_n':int(pd.to_numeric(g['tp20_reached_24h'],errors='coerce').fillna(0).sum()),
        'tp20_reached_rate':float(pd.to_numeric(g['tp20_reached_24h'],errors='coerce').mean()),
        'median_entry_recovery_min':float(pd.to_numeric(g['entry_recovery_min'],errors='coerce').dropna().median()) if pd.to_numeric(g['entry_recovery_min'],errors='coerce').notna().any() else None,
        'median_post_low_from_pp_pct':float(pd.to_numeric(g['post_low_from_pp_pct'],errors='coerce').median()),
        'median_post_low_entry_pct':float(pd.to_numeric(g['post_low_entry_pct'],errors='coerce').median()),
    })
write_csv(OUTDIR/'PP12_PATH_SUMMARY.csv', path_summary)

grid_rows = []
for (model,cut), g0 in pol.groupby(['model','cut_pct'], dropna=False):
    for sp, g in [('ALL',g0)] + [(s,g0[g0['split'].eq(s)]) for s in ['DISCOVERY_0106','VALIDATION_0708','OOS_09']]:
        cnt = Counter(g['result'].astype(str))
        grid_rows.append({
            'model':model,'cut_pct':cut,'split':sp,'n':len(g),
            'base_pp_net':float(g['base_pp_net_pct'].sum()),
            'recovery_net':float(g['net_pct'].sum()),
            'delta_vs_pp':float(g['delta_vs_pp_pct'].sum()),
            'avg_delta':float(g['delta_vs_pp_pct'].mean()),
            'improved_n':int((g['delta_vs_pp_pct']>0).sum()),
            'worse_n':int((g['delta_vs_pp_pct']<0).sum()),
            'avg_exit_n':cnt.get('AVG_EXIT',0),
            'watch_cut_n':cnt.get('WATCH_CUT',0),
            'max2_n':cnt.get('MAX2_EXIT',0),
            'exit24_n':cnt.get('RECOVERY_24H_EXIT',0),
        })
write_csv(OUTDIR/'PP12_RECOVERY_CUT_GRID.csv', grid_rows)

grid = pd.DataFrame(grid_rows)
allg = grid[grid['split'].eq('ALL')].copy().sort_values(['model','delta_vs_pp'], ascending=[True,False])
write_csv(OUTDIR/'PP12_RECOVERY_CUT_GRID_ALL.csv', allg)

monthly_rows=[]
for (model,cut,month), g in pol.groupby(['model','cut_pct','month']):
    monthly_rows.append({
        'model':model,'cut_pct':cut,'month':month,'n':len(g),
        'base_pp_net':float(g['base_pp_net_pct'].sum()),
        'recovery_net':float(g['net_pct'].sum()),
        'delta_vs_pp':float(g['delta_vs_pp_pct'].sum()),
        'avg_exit_n':int(g['result'].eq('AVG_EXIT').sum()),
        'watch_cut_n':int(g['result'].eq('WATCH_CUT').sum()),
        'max2_n':int(g['result'].eq('MAX2_EXIT').sum()),
        'exit24_n':int(g['result'].eq('RECOVERY_24H_EXIT').sum()),
    })
write_csv(OUTDIR/'PP12_RECOVERY_MONTHLY.csv', monthly_rows)

# Dedicated details for direct analogue of current Hard safety cut -2.00.
sel = pol[pol['cut_pct'].eq('2.00')].copy()
write_csv(OUTDIR/'PP12_RECOVERY_CUT200_DETAIL.csv', sel)

# Audit how often PP had pre-fills / reduced remaining qty.
q = pd.to_numeric(det['pp_remaining_qty'], errors='coerce')
prefill_audit = [{
    'pp_n':len(det),
    'no_pre_fill_n':int((pd.to_numeric(det['pre_pp_fill_n'],errors='coerce')==0).sum()),
    'has_pre_fill_n':int((pd.to_numeric(det['pre_pp_fill_n'],errors='coerce')>0).sum()),
    'remaining_qty_1_n':int((q.round(8)==1.0).sum()),
    'remaining_qty_lt1_n':int((q<0.999999).sum()),
    'remaining_qty_min':float(q.min()),
    'remaining_qty_median':float(q.median()),
}]
write_csv(OUTDIR/'PP12_PREFILL_AUDIT.csv', prefill_audit)

# Human-readable summary
lines=[]
lines.append('FINAL P PP12 RECOVERY PRECHECK V19 — 2026-10-06')
lines.append('')
lines.append(f'V18 DYN_HARD_STOP PP12: {base_pp_n} trades / {base_pp_net:+.6f}%p')
pa=path_summary[0]
lines.append(f"24h raw entry recovery: {pa['entry_recovered_n']}/{pa['n']} = {100*pa['entry_recovered_rate']:.2f}%")
lines.append(f"24h TP+2.0 reach after PP exit: {pa['tp20_reached_n']}/{pa['n']} = {100*pa['tp20_reached_rate']:.2f}%")
lines.append(f"PP with pre-PP partial fill: {prefill_audit[0]['has_pre_fill_n']}/{base_pp_n}")
lines.append('')
for model in ['RESET100','KEEP_REMAINING']:
    lines.append('=== '+model+' ===')
    z = allg[allg['model'].eq(model)].copy()
    for _,r in z.head(6).iterrows():
        lines.append(
            f"cut={r['cut_pct']} n={int(r['n'])} recovery={r['recovery_net']:+.3f} "
            f"delta={r['delta_vs_pp']:+.3f} AVG={int(r['avg_exit_n'])} CUT={int(r['watch_cut_n'])} "
            f"MAX2={int(r['max2_n'])} 24H={int(r['exit24_n'])}"
        )
    lines.append('')
lines.append('NOTE: cohort precheck only. Scheduler/slot/replacement-entry effects require a later exact replay.')
(OUTDIR/'SUMMARY.txt').write_text('\n'.join(lines)+'\n', encoding='utf-8')

say('=== TOP GRID ===')
for model in ['RESET100','KEEP_REMAINING']:
    z=allg[allg['model'].eq(model)].head(5)
    say(model)
    for _,r in z.iterrows():
        say(' cut',r['cut_pct'],'net',f"{r['recovery_net']:+.3f}",'delta',f"{r['delta_vs_pp']:+.3f}",
            'AVG',int(r['avg_exit_n']),'CUT',int(r['watch_cut_n']),'MAX2',int(r['max2_n']),'24H',int(r['exit24_n']))

(OUTDIR/'RUN_LOG.txt').write_text('\n'.join(LOG)+'\n', encoding='utf-8')

with zipfile.ZipFile(OUTZIP, 'w', zipfile.ZIP_DEFLATED) as z:
    for p in sorted(OUTDIR.iterdir()):
        if p.is_file(): z.write(p, arcname=p.name)

say('RESULT ZIP', OUTZIP)
say('DONE')
