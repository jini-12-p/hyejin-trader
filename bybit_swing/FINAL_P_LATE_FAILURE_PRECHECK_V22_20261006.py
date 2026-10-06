#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FINAL P LATE FAILURE PRECHECK V22 — 2026-10-06

Goal
----
Starting from the frozen V21 winner:
  DYN_HARD_STOP + PPABC selective recovery
analyze the 68 exact LATE_FAILURE_EXIT trades without changing the entry stack.

For each exact Late Failure trade:
1) Reproduce the current base Late exit.
2) Disable only pv26_late_failure_signal and replay the same trade causally.
3) If the no-Late replay later becomes STOP, apply the frozen V18 winner:
   - Hard -3: Hard STOP-GREEN selective Recovery, -2.00 watch cut.
   - Non-Hard STOP: frozen generic gated Recovery, -2.00 watch cut.
4) If the no-Late replay later becomes PP12, apply frozen V21 PPABC selective Recovery,
   - PP watch cut -1.25%, +1.5 rebound, max 2, KEEP_REMAINING.
5) Record raw no-Late and frozen-downstream economics.
6) Attach causal features at the original Late exit for later GREEN/RED research.
7) Record 24h future path diagnostics only as labels/diagnostics, never as gate features.

This is a cohort precheck, NOT a scheduler exact.  If a useful selective Late gate exists,
a later exact replay must recalculate slots/cooldowns/replacement entries.
"""
from __future__ import annotations
from pathlib import Path
from datetime import timedelta
import json, math, zipfile, sys, types
import numpy as np
import pandas as pd

ROOT = Path('/root/hyejin-trader/bybit_swing')
V21 = ROOT / 'FINAL_P_PP12_SELECTIVE_EXACT_V21_20261006.py'
V21ZIP = ROOT / 'FINAL_P_PP12_SELECTIVE_EXACT_V21_20261006_RESULTS.zip'
OUTDIR = ROOT / 'FINAL_P_LATE_FAILURE_PRECHECK_V22_20261006'
OUTZIP = ROOT / 'FINAL_P_LATE_FAILURE_PRECHECK_V22_20261006_RESULTS.zip'
OUTDIR.mkdir(exist_ok=True)

if not V21.exists():
    raise SystemExit(f'MISSING {V21}')
if not V21ZIP.exists():
    raise SystemExit(f'MISSING {V21ZIP}')

# Load only the validated V21 engine/definitions; never execute its policy replay bottom block.
src = V21.read_text(encoding='utf-8')
marker = "\nsay('V21 PP12 SELECTIVE EXACT: frozen DYN_HARD_STOP + PP Gate A/ABC/ABCD')"
if marker not in src:
    raise SystemExit('V21 PREFIX MARKER NOT FOUND')
prefix = src.split(marker, 1)[0]
_mod_name = '__v21_prefix__'
_mod = types.ModuleType(_mod_name)
_mod.__file__ = str(V21)
sys.modules[_mod_name] = _mod
exec(compile(prefix, str(V21), 'exec'), _mod.__dict__)
ns = _mod.__dict__

U = ns['U']; ja_idx = ns['ja_idx']; se_idx = ns['se_idx']
KST = ns['KST']; UTC = ns['UTC']
dt_utc = ns['dt_utc']; fv = ns['fv']; pct = ns['pct']; safe = ns['safe']
to_bars = ns['to_bars']; feature_pack = ns['feature_pack']; market_features = ns['market_features']
candidate_features = ns['candidate_features']; up24_features = ns['up24_features']
point_features = ns['point_features']; is_hard3_stop = ns['is_hard3_stop']
gate_sim = ns['gate_sim']; hard_selective_sim = ns['hard_selective_sim']
pp_checkpoint_features = ns['pp_checkpoint_features']; pp_gate_eval = ns['pp_gate_eval']
pp_selective_recovery_sim = ns['pp_selective_recovery_sim']
PP_WATCH_CUT_PCT = ns['PP_WATCH_CUT_PCT']

LOG=[]
def say(*a):
    s=' '.join(str(x) for x in a); print(s, flush=True); LOG.append(s)

def split_name(month):
    m=str(month)
    if m <= '2026-06': return 'DISCOVERY_0106'
    if m <= '2026-08': return 'VALIDATION_0708'
    return 'OOS_09'

def entry_time_from_candidate(r, period):
    x=r['_dt']
    if period=='JA':
        t=pd.Timestamp(x)
        if t.tzinfo is None: t=t.tz_localize(UTC)
        else: t=t.tz_convert(UTC)
        return t.to_pydatetime()
    if isinstance(x, pd.Timestamp):
        if x.tzinfo is None: x=x.tz_localize(UTC)
        else: x=x.tz_convert(UTC)
        return x.to_pydatetime()
    return x

def future_path_diag(symbol, late_t, entry):
    st=late_t.astimezone(UTC).replace(second=0,microsecond=0)
    en=late_t.astimezone(UTC)+timedelta(hours=24)
    try:
        raw=U.KC.get(symbol,'1m',st-timedelta(minutes=1),en+timedelta(minutes=2))
        one=to_bars(raw)
        post=[b for b in one if b['ts']>st and b['ts']<=en]
    except Exception as e:
        return {'future_data_error':str(e)}
    if not post: return {'future_data_error':'NO_POST_BARS'}
    maxh=max(float(b['h']) for b in post); minl=min(float(b['l']) for b in post)
    rec=None; tp=None; hard=None
    for b in post:
        now=b['ts']+timedelta(minutes=1)
        if rec is None and float(b['h'])>=entry: rec=now
        if tp is None and float(b['h'])>=entry*1.02: tp=now
        if hard is None and float(b['l'])<=entry*0.97: hard=now
    def mins(t): return None if t is None else (t-late_t).total_seconds()/60.0
    return {
        'future_max_pct':pct(maxh,entry),'future_min_pct':pct(minl,entry),
        'future_entry_recovered':int(rec is not None),'future_entry_recover_min':mins(rec),
        'future_tp20':int(tp is not None),'future_tp20_min':mins(tp),
        'future_hard3_touched':int(hard is not None),'future_hard3_min':mins(hard),
        'future_data_error':''
    }

def late_features(symbol, entry_time, entry, late_base):
    t=late_base.exit_time
    start=t.astimezone(UTC)-timedelta(hours=3)
    try:
        raw=U.KC.get(symbol,'1m',start,t.astimezone(UTC)+timedelta(minutes=2))
        one=to_bars(raw)
        d=point_features('LATE_',symbol,one,t,entry,entry_time,late_base,t)
    except Exception as e:
        d={'LATE_feature_error':str(e)}
    d['LATE_age_min']=(t-entry_time).total_seconds()/60.0
    d['LATE_exit_pct']=pct(float(late_base.terminal_price),entry)
    d['LATE_mfe_pct']=fv(getattr(late_base,'mfe_pct',None))
    d['LATE_mae_pct']=fv(getattr(late_base,'mae_pct',None))
    if d['LATE_mfe_pct'] is not None and d['LATE_exit_pct'] is not None:
        d['LATE_giveback_from_mfe_pp']=d['LATE_mfe_pct']-d['LATE_exit_pct']
    return d

def apply_frozen_downstream(symbol, entry_time, entry, sim):
    res=str(sim.result)
    if res=='STOP':
        if is_hard3_stop(entry,sim):
            out,why=hard_selective_sim(symbol,entry_time,entry,sim,watch_cut_pct=2.00,use_gray=False)
            return out,'NO_LATE->STOP->HARD_STOP_GREEN:'+str(why)
        out,why=gate_sim(symbol,entry_time,entry,sim,watch_cut_pct=2.00)
        return out,'NO_LATE->STOP->GENERIC_CUT200:'+str(why)
    if res=='PROFIT_PROTECT_EXIT':
        pf=pp_checkpoint_features(symbol,entry_time,entry,sim.exit_time)
        ok,tag,hits=pp_gate_eval('ABC',pf)
        if ok:
            out,why=pp_selective_recovery_sim(symbol,entry_time,entry,sim,watch_cut_pct=PP_WATCH_CUT_PCT)
            return out,'NO_LATE->PPABC_'+tag+':'+str(why)
        return sim,'NO_LATE->PPABC_GATE_NOT_MET_'+tag
    return sim,'NO_LATE->'+res

say('=== LOAD V21 WINNER LEDGER ===')
with zipfile.ZipFile(V21ZIP) as z:
    winner=pd.read_csv(z.open('EXACT_TRADES_DYN_HARD_STOP_PPABC.csv'),low_memory=False)
late=winner[winner.result.astype(str).eq('LATE_FAILURE_EXIT')].copy().reset_index(drop=True)
say('V21 WINNER N',len(winner),'NET',round(float(pd.to_numeric(winner.net_pct).sum()),6))
say('LATE N',len(late),'NET',round(float(pd.to_numeric(late.net_pct).sum()),6))
if len(late)!=68:
    raise SystemExit(f'LATE COUNT AUDIT FAIL {len(late)} != 68')

orig_late_fn=U.BOT.pv26_late_failure_signal
rows=[]
for i,lr in late.iterrows():
    sid=str(lr.setup_id); period=str(lr.period); sym=str(lr.symbol)
    cand=(ja_idx.get(sid) if period=='JA' else se_idx.get(sid))
    if cand is None:
        rows.append({'setup_id':sid,'period':period,'symbol':sym,'error':'CANDIDATE_NOT_FOUND'}); continue
    et=entry_time_from_candidate(cand,period); entry=float(cand['entry_price'])
    sobj={'setup_id':sid,'symbol':sym,'entry':et,'entry_price':entry}

    # Reproduce current base late result.
    current=U.simulate_base(sobj)
    audit_ok=(str(current.result)=='LATE_FAILURE_EXIT')

    # Counterfactual: disable only V26 late failure, preserve every other base rule.
    try:
        U.BOT.pv26_late_failure_signal=lambda *a,**k:(False,{})
        no_late=U.simulate_base(sobj)
    finally:
        U.BOT.pv26_late_failure_signal=orig_late_fn

    frozen,route=apply_frozen_downstream(sym,et,entry,no_late)
    cur_net=float(lr.net_pct)
    raw_delta=float(no_late.net_pct)-cur_net
    frozen_delta=float(frozen.net_pct)-cur_net

    rec={
        'setup_id':sid,'period':period,'symbol':sym,'month':str(lr.month),'split':split_name(lr.month),
        'entry_time_utc':et.isoformat(),'entry_price':entry,
        'current_exact_net_pct':cur_net,'current_resim_result':str(current.result),
        'current_resim_net_pct':float(current.net_pct),'current_audit_ok':int(audit_ok),
        'late_exit_time_utc':current.exit_time.isoformat(),'late_terminal_price':float(current.terminal_price),
        'no_late_raw_result':str(no_late.result),'no_late_raw_net_pct':float(no_late.net_pct),
        'no_late_raw_delta_pct':raw_delta,
        'no_late_frozen_result':str(frozen.result),'no_late_frozen_net_pct':float(frozen.net_pct),
        'no_late_frozen_delta_pct':frozen_delta,'no_late_frozen_route':route,
        'no_late_better':int(frozen_delta>0),'no_late_worse':int(frozen_delta<0),
        'no_late_exit_time_utc':frozen.exit_time.isoformat(),
        'no_late_extra_hold_min':(frozen.exit_time-current.exit_time).total_seconds()/60.0,
        'error':''
    }
    rec.update(late_features(sym,et,entry,current))
    rec.update(future_path_diag(sym,current.exit_time,entry))
    rows.append(rec)
    if (i+1)%10==0 or i+1==len(late):
        good=sum(int(x.get('no_late_better',0)) for x in rows)
        delta=sum(float(x.get('no_late_frozen_delta_pct',0) or 0) for x in rows if not x.get('error'))
        say('LATE',i+1,'/',len(late),'BETTER',good,'COHORT_DELTA',round(delta,3))

R=pd.DataFrame(rows)
R.to_csv(OUTDIR/'LATE_FAILURE_NO_LATE_DETAIL.csv',index=False)

# Audits and summaries.
aud_bad=R[pd.to_numeric(R.current_audit_ok,errors='coerce').fillna(0).astype(int)!=1]
if len(aud_bad):
    say('WARNING CURRENT RESIM AUDIT MISMATCH',len(aud_bad))

monthly=(R.groupby('month',dropna=False)
    .agg(N=('setup_id','count'),CURRENT_NET=('current_exact_net_pct','sum'),
         NO_LATE_RAW_NET=('no_late_raw_net_pct','sum'),NO_LATE_FROZEN_NET=('no_late_frozen_net_pct','sum'),
         RAW_DELTA=('no_late_raw_delta_pct','sum'),FROZEN_DELTA=('no_late_frozen_delta_pct','sum'),
         BETTER=('no_late_better','sum'),WORSE=('no_late_worse','sum'),
         AVG_EXTRA_HOLD_MIN=('no_late_extra_hold_min','mean'))
    .reset_index())
monthly.to_csv(OUTDIR/'LATE_FAILURE_MONTHLY.csv',index=False)

split=(R.groupby('split',dropna=False)
    .agg(N=('setup_id','count'),CURRENT_NET=('current_exact_net_pct','sum'),
         FROZEN_NET=('no_late_frozen_net_pct','sum'),DELTA=('no_late_frozen_delta_pct','sum'),
         BETTER=('no_late_better','sum'),WORSE=('no_late_worse','sum'))
    .reset_index())
split.to_csv(OUTDIR/'LATE_FAILURE_SPLIT.csv',index=False)

trans=(R.groupby(['no_late_raw_result','no_late_frozen_result'],dropna=False)
    .agg(N=('setup_id','count'),CURRENT_NET=('current_exact_net_pct','sum'),
         FROZEN_NET=('no_late_frozen_net_pct','sum'),DELTA=('no_late_frozen_delta_pct','sum'),BETTER=('no_late_better','sum'))
    .reset_index().sort_values(['N','DELTA'],ascending=[False,False]))
trans.to_csv(OUTDIR/'LATE_FAILURE_TRANSITIONS.csv',index=False)

# Causal feature matrix only (no future columns) + label for next-turn rule analysis.
future_cols=[c for c in R.columns if c.startswith('future_')]
meta_keep=['setup_id','period','symbol','month','split','current_exact_net_pct','no_late_frozen_result',
           'no_late_frozen_net_pct','no_late_frozen_delta_pct','no_late_better','no_late_worse','no_late_extra_hold_min']
feat_cols=[c for c in R.columns if c.startswith('LATE_')]
R[[c for c in meta_keep+feat_cols if c in R.columns]].to_csv(OUTDIR/'LATE_FAILURE_GATE_FEATURES.csv',index=False)

summary={
    'winner_total_net':float(pd.to_numeric(winner.net_pct).sum()),
    'late_n':int(len(R)),
    'late_current_net':float(R.current_exact_net_pct.sum()),
    'no_late_raw_net':float(R.no_late_raw_net_pct.sum()),
    'no_late_raw_delta':float(R.no_late_raw_delta_pct.sum()),
    'no_late_frozen_net':float(R.no_late_frozen_net_pct.sum()),
    'no_late_frozen_delta':float(R.no_late_frozen_delta_pct.sum()),
    'better_n':int(R.no_late_better.sum()),'worse_n':int(R.no_late_worse.sum()),
    'entry_recovered_24h':int(pd.to_numeric(R.get('future_entry_recovered',0),errors='coerce').fillna(0).sum()),
    'tp20_24h':int(pd.to_numeric(R.get('future_tp20',0),errors='coerce').fillna(0).sum()),
    'hard3_touched_24h':int(pd.to_numeric(R.get('future_hard3_touched',0),errors='coerce').fillna(0).sum()),
}
(OUTDIR/'SUMMARY.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')

say('=== SUMMARY ===')
for k,v in summary.items(): say(k,v)
say('=== SPLIT ===')
for r in split.itertuples(index=False): say(r)
say('=== MONTHLY ===')
for r in monthly.itertuples(index=False): say(r)
(OUTDIR/'RUN_LOG.txt').write_text('\n'.join(LOG)+'\n',encoding='utf-8')

with zipfile.ZipFile(OUTZIP,'w',zipfile.ZIP_DEFLATED) as z:
    for p in sorted(OUTDIR.iterdir()):
        if p.is_file(): z.write(p,p.name)
say('=== COMPLETE ===')
say('ZIP',OUTZIP)
