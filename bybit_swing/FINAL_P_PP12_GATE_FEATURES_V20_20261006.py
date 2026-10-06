#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FINAL P PP12 SELECTIVE RECOVERY FEATURE/GATE MINING V20 — 2026-10-06

Purpose
-------
V19 showed that unconditional PP12 cyclic recovery is not robust. This script
extracts information that is causally available at the PP12 exit moment and
asks whether PP trades whose recovery IMPROVES P&L can be separated from
trades better left at the original PP12 exit.

Main economic label (RESET100, watch cut -1.25% from V19):
    IMPROVE = recovery delta_vs_pp_pct > 0
Secondary label:
    AVG_EXIT = recovery eventually exited at the new average

Anti-lookahead
--------------
- All PP checkpoint features use bars completed at or before the PP exit.
- Future V19 outcomes are used only as labels.
- Rule selection uses Jan-Jun DISCOVERY and Jul-Aug VALIDATION only.
- Sep OOS is displayed only; never used to select thresholds/rules.

This is a PRECHECK / feature-mining run only.
It does NOT alter bot.py and does NOT perform the final scheduler exact replay.
"""
from __future__ import annotations

from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import bisect, csv, io, json, math, runpy, zipfile

import numpy as np
import pandas as pd

ROOT = Path('/root/hyejin-trader/bybit_swing')
OUTDIR = ROOT / 'FINAL_P_PP12_GATE_FEATURES_V20_20261006'
OUTDIR.mkdir(exist_ok=True)
OUTZIP = ROOT / 'FINAL_P_PP12_GATE_FEATURES_V20_20261006_RESULTS.zip'
KST = timezone(timedelta(hours=9)); UTC = timezone.utc
LOG=[]

def say(*a):
    s=' '.join(str(x) for x in a); print(s,flush=True); LOG.append(s)

def fv(v,d=None):
    try:
        if v is None or str(v).strip()=='': return d
        x=float(v)
        if not math.isfinite(x): return d
        return x
    except Exception: return d

def pct(a,b):
    return (float(a)/float(b)-1.0)*100.0 if b not in (None,0) else None

def mean(xs):
    z=[float(x) for x in xs if x is not None and math.isfinite(float(x))]
    return sum(z)/len(z) if z else None

def ema(vals,n):
    z=[float(x) for x in vals if x is not None]
    if not z:return None
    alpha=2/(n+1); e=z[0]
    for x in z[1:]: e=alpha*x+(1-alpha)*e
    return e

def rsi(vals,n=14):
    if len(vals)<n+1:return None
    ds=np.diff(np.asarray(vals[-(n+1):],dtype=float))
    ag=np.maximum(ds,0).mean(); al=np.maximum(-ds,0).mean()
    if al==0:return 100.0
    rs=ag/al; return 100-100/(1+rs)

def close_pos(b):
    if not b or b['h']<=b['l']:return None
    return (b['c']-b['l'])/(b['h']-b['l'])

def write_csv(path,df):
    if isinstance(df,pd.DataFrame):df.to_csv(path,index=False)
    else:pd.DataFrame(df).to_csv(path,index=False)

def split_name(month):
    m=str(month)
    if m<='2026-06':return 'DISCOVERY_0106'
    if m<='2026-08':return 'VALIDATION_0708'
    return 'OOS_09'

# ------------------------------------------------------------------
# Load validated data engine, V18 exact winner ledger, V19 labels
# ------------------------------------------------------------------
BASE=ROOT/'breadth_persistence_diag.py'
if not BASE.exists(): BASE=Path('/tmp/breadth_persistence_diag.py')
if not BASE.exists(): raise SystemExit('MISSING breadth_persistence_diag.py')
say('=== LOAD VALIDATED ENGINE ===')
NS=runpy.run_path(str(BASE)); U=NS['U']; ja=NS['ja'].copy(); se=NS['se'].copy()

v18=ROOT/'FINAL_P_HARD_SELECTIVE_EXACT_V18_20261006_RESULTS.zip'
v19=ROOT/'FINAL_P_PP12_RECOVERY_PRECHECK_V19_20261006_RESULTS.zip'
if not v18.exists(): raise SystemExit('MISSING '+str(v18))
if not v19.exists(): raise SystemExit('MISSING '+str(v19))

with zipfile.ZipFile(v18) as z:
    ledger=pd.read_csv(z.open('EXACT_TRADES_DYN_HARD_STOP.csv'),low_memory=False)
    up24=pd.read_csv(z.open('UP24_BREADTH_SERIES.csv'),low_memory=False)
with zipfile.ZipFile(v19) as z:
    fut=pd.read_csv(z.open('PP12_FUTURE_PATH.csv'),low_memory=False)
    rr=pd.read_csv(z.open('PP12_RECOVERY_ALL_TRADES.csv'),low_memory=False)

rr['cut_num']=pd.to_numeric(rr.cut_pct,errors='coerce')
lab=rr[(rr.model.astype(str)=='RESET100') & (rr.cut_num.sub(1.25).abs()<1e-9)].copy()
lab=lab[['setup_id','result','attempts','net_pct','delta_vs_pp_pct','gross_pct','fee_pct','exit_time_utc','add_realized_pct']]
P=fut.merge(lab,on='setup_id',how='inner',validate='one_to_one')
if len(P)!=251: raise SystemExit(f'PP LABEL COUNT {len(P)} != 251')
P['split']=P['month'].astype(str).map(split_name)
P['target_improve']=(pd.to_numeric(P.delta_vs_pp_pct,errors='coerce')>0).astype(int)
P['target_avg_exit']=(P.result.astype(str)=='AVG_EXIT').astype(int)
say('PP COHORT',len(P),'IMPROVE',int(P.target_improve.sum()),'AVG_EXIT',int(P.target_avg_exit.sum()))
for sp,g in P.groupby('split'):
    say(sp,'N',len(g),'IMPROVE',int(g.target_improve.sum()),f'RATE={g.target_improve.mean():.2%}')

# ------------------------------------------------------------------
# Exact prior-exit stream from V18 winner
# ------------------------------------------------------------------
ledger['exit_dt']=pd.to_datetime(ledger.exit_time_utc,utc=True,errors='coerce')
ledger['net_pct']=pd.to_numeric(ledger.net_pct,errors='coerce')
lex=ledger.dropna(subset=['exit_dt']).sort_values(['exit_dt','setup_id']).copy()
exit_events=[{'t':r.exit_dt.to_pydatetime(),'net':float(r.net_pct),'result':str(r.result),'sid':str(r.setup_id)} for r in lex.itertuples(index=False)]
exit_times=[x['t'].timestamp() for x in exit_events]

def recent_exit_features(t):
    j=bisect.bisect_left(exit_times,t.timestamp()); prior=exit_events[:j]; last9=prior[-9:]
    out={'shadow9_n':len(last9),'shadow9_sum':sum(x['net'] for x in last9) if last9 else None,
         'shadow9_stop_n':sum(1 for x in last9 if x['result'] in ('STOP','GATE_WATCH_CUT','GATE_REJECT_EXIT','LATE_FAILURE_EXIT'))}
    for h in (1,2,6,12,24):
        cut=t.timestamp()-h*3600; k=bisect.bisect_left(exit_times,cut,0,j); z=exit_events[k:j]
        out[f'exit_n_{h}h']=len(z); out[f'exit_net_{h}h']=sum(x['net'] for x in z) if z else 0.0
        out[f'exit_stop_n_{h}h']=sum(1 for x in z if x['result'] in ('STOP','GATE_WATCH_CUT','GATE_REJECT_EXIT','LATE_FAILURE_EXIT'))
    return out

# ------------------------------------------------------------------
# V25 candidate activity
# ------------------------------------------------------------------
cand=[]
for period,d in [('JA',ja),('SE',se)]:
    for _,r in d.iterrows():
        try:
            t=pd.Timestamp(r['_dt']).to_pydatetime() if period=='JA' else r['_dt']
            if t.tzinfo is None:t=t.replace(tzinfo=UTC)
            else:t=t.astimezone(UTC)
            cand.append((t.timestamp(),str(r['symbol'])))
        except Exception:pass
cand.sort(); cand_times=[x[0] for x in cand]

def cand_window(t,h):
    b=t.timestamp(); a=b-h*3600; i=bisect.bisect_left(cand_times,a); j=bisect.bisect_right(cand_times,b)
    z=cand[i:j]; return len(z),len(set(s for _,s in z))

def candidate_features(t):
    out={}
    for h in (1,2,6,12,24):
        n,b=cand_window(t,h); out[f'v25_count_{h}h']=n; out[f'v25_breadth_{h}h']=b
    for h in (2,6,12,24):
        n,b=cand_window(t,h); pn,pb=cand_window(t-timedelta(hours=h),h)
        out[f'v25_count_delta_{h}h']=n-pn; out[f'v25_breadth_delta_{h}h']=b-pb
    return out

# ------------------------------------------------------------------
# Price/market checkpoint features
# ------------------------------------------------------------------
def to_bars(df):
    out=[]
    if df is None or len(df)==0:return out
    for r in df.itertuples(index=False):
        out.append({'ts':pd.Timestamp(r.ts).to_pydatetime().astimezone(UTC),'o':float(r.open),'h':float(r.high),'l':float(r.low),'c':float(r.close),'v':float(r.volume)})
    return out

def five_bars(one):
    buckets={}
    for b in one:
        t=b['ts'].replace(minute=(b['ts'].minute//5)*5,second=0,microsecond=0)
        x=buckets.get(t)
        if x is None:buckets[t]={'ts':t,'o':b['o'],'h':b['h'],'l':b['l'],'c':b['c'],'v':b['v'],'n':1}
        else:x['h']=max(x['h'],b['h']);x['l']=min(x['l'],b['l']);x['c']=b['c'];x['v']+=b['v'];x['n']+=1
    return [buckets[k] for k in sorted(buckets)]

def feature_pack(one,checkpoint,entry_price=None,entry_ts=None):
    cp=checkpoint.astimezone(UTC).replace(second=0,microsecond=0)
    prev1=[b for b in one if b['ts']<cp]; five=five_bars(one); prev5=[b for b in five if b['ts']+timedelta(minutes=5)<=cp]
    p1=prev1[-1] if prev1 else None; p3=prev1[-3:] if len(prev1)>=3 else prev1; p5=prev5[-1] if prev5 else None
    v10=[b['v'] for b in prev1[-11:-1]] if len(prev1)>=2 else []; m10=mean(v10)
    v5=[b['v'] for b in prev5[-6:-1]] if len(prev5)>=2 else []; m5=mean(v5)
    c5=[b['c'] for b in prev5]; e9=ema(c5[-60:],9) if c5 else None; e20=ema(c5[-80:],20) if c5 else None; e20p=ema(c5[-81:-1],20) if len(c5)>=2 else None
    out={'prev1m_ret':pct(p1['c'],p1['o']) if p1 else None,'prev3m_ret':pct(p3[-1]['c'],p3[0]['o']) if p3 else None,
         'prev1m_close_pos':close_pos(p1),'prev1m_vol_ratio10':p1['v']/m10 if p1 and m10 not in (None,0) else None,
         'last5m_ret':pct(p5['c'],p5['o']) if p5 else None,'last5m_close_pos':close_pos(p5),'last5m_vol_ratio5':p5['v']/m5 if p5 and m5 not in (None,0) else None,
         'ema9_20_gap':(e9/e20-1)*100 if e9 and e20 else None,'ema20_slope':(e20/e20p-1)*100 if e20 and e20p else None,'rsi14_5m':rsi(c5,14)}
    for m in (5,10,15,30,60,120):
        if prev1:
            cur=prev1[-1]['c']; target=cp-timedelta(minutes=m); old=None
            for b in reversed(prev1):
                if b['ts']<=target:old=b['c'];break
            out[f'ret_{m}m']=pct(cur,old) if old else None
        else:out[f'ret_{m}m']=None
    if entry_price and entry_ts:
        ep=entry_ts.astimezone(UTC).replace(second=0,microsecond=0); path=[b for b in one if ep<=b['ts']<cp]
        if path:
            out['mfe_to_cp']=pct(max(b['h'] for b in path),entry_price); out['mae_to_cp']=pct(min(b['l'] for b in path),entry_price)
            out['low_break_count_30m']=sum(1 for k in range(max(1,len(path)-30),len(path)) if path[k]['l']<min(x['l'] for x in path[max(0,k-10):k]))
        else: out['mfe_to_cp']=None;out['mae_to_cp']=None;out['low_break_count_30m']=None
    return out

market_1m_cache={}
def market_1m(sym,t):
    d0=t.astimezone(UTC).replace(hour=0,minute=0,second=0,microsecond=0); key=(sym,d0)
    if key not in market_1m_cache:
        market_1m_cache[key]=to_bars(U.KC.get(sym,'1m',d0-timedelta(hours=6),d0+timedelta(hours=26)))
    return market_1m_cache[key]

def market_short(sym,t,minutes):
    one=market_1m(sym,t); cp=t.astimezone(UTC).replace(second=0,microsecond=0); prev=[b for b in one if b['ts']<cp]
    if not prev:return None
    cur=prev[-1]['c']; target=cp-timedelta(minutes=minutes); old=None
    for b in reversed(prev):
        if b['ts']<=target:old=b['c'];break
    return pct(cur,old) if old else None

say('=== LOAD BTC/ETH 1H MARKET SERIES ===')
market_hour={}
for sym in ('BTCUSDT','ETHUSDT'):
    d=U.KC.get(sym,'60',datetime(2025,11,25,tzinfo=UTC),datetime(2026,10,2,tzinfo=UTC)).copy()
    d['ts']=pd.to_datetime(d.ts,utc=True); d=d.sort_values('ts').drop_duplicates('ts').reset_index(drop=True); market_hour[sym]=d
    say(sym,'1H',len(d))

def hourly_change(sym,t,h):
    d=market_hour[sym]; ts=pd.Timestamp(t).tz_convert(UTC); q=d[d.ts<ts]
    if len(q)==0:return None
    cur=float(q.iloc[-1].close); p=q[q.ts<=ts-pd.Timedelta(hours=h)]
    return pct(cur,float(p.iloc[-1].close)) if len(p) else None

def hourly_pos(sym,t,h):
    d=market_hour[sym]; ts=pd.Timestamp(t).tz_convert(UTC); q=d[(d.ts<ts)&(d.ts>=ts-pd.Timedelta(hours=h))]
    if len(q)<2:return None
    lo=float(q.low.min());hi=float(q.high.max());cur=float(q.iloc[-1].close);return (cur-lo)/(hi-lo) if hi>lo else None

def market_features(t):
    out={}
    for sym,p in [('BTCUSDT','btc'),('ETHUSDT','eth')]:
        for m in (5,15,30,60,240):out[f'{p}_{m}m']=market_short(sym,t,m)
        for h,n in [(24,'24h'),(48,'48h'),(168,'7d'),(720,'30d')]:out[f'{p}_{n}']=hourly_change(sym,t,h)
        out[f'{p}_pos24']=hourly_pos(sym,t,24)
    for n in ('15m','60m','240m','24h','48h','7d','30d'):
        a=out.get('btc_'+n);b=out.get('eth_'+n)
        out[f'btc_eth_spread_{n}']=(a-b) if a is not None and b is not None else None
        out[f'btc_eth_absavg_{n}']=(abs(a)+abs(b))/2 if a is not None and b is not None else None
        out[f'btc_eth_both_down_{n}']=int(a<0 and b<0) if a is not None and b is not None else None
    return out

up24['ts']=pd.to_datetime(up24.ts,utc=True,errors='coerce')
def up24_features(t):
    q=up24[(up24.ts<pd.Timestamp(t).tz_convert(UTC)) & up24.up24.notna()]
    if len(q)==0:return {}
    r=q.iloc[-1];return {k:fv(r.get(k)) for k in ['up24','up24_n','up24_delta6','up24_delta12','up24_delta24']}

# ------------------------------------------------------------------
# PP-specific causal features
# ------------------------------------------------------------------
def pp_specific(one,entry_t,pp_t,entry,pp_px,base_mfe,base_mae):
    ep=entry_t.astimezone(UTC).replace(second=0,microsecond=0); cp=pp_t.astimezone(UTC).replace(second=0,microsecond=0)
    path=[b for b in one if ep<=b['ts']<cp]
    pp_pct=pct(pp_px,entry)
    out={'entry_to_pp_min':(pp_t-entry_t).total_seconds()/60.0,'pp_close_entry_pct':pp_pct,
         'pp_mfe_pct':base_mfe,'pp_mae_pct':base_mae,'mfe_giveback_to_pp':(base_mfe-pp_pct if base_mfe is not None and pp_pct is not None else None),
         'pp_close_overshoot_below_m115':(-1.15-pp_pct if pp_pct is not None else None)}
    if not path:return out
    arm_thr=entry*1.012
    arm_b=next((b for b in path if b['h']>=arm_thr),None)
    peak=max(path,key=lambda b:b['h']); peak_t=peak['ts']+timedelta(minutes=1)
    arm_t=(arm_b['ts']+timedelta(minutes=1)) if arm_b else None
    out['time_to_peak_min']=(peak_t-entry_t).total_seconds()/60.0
    out['peak_to_pp_min']=(pp_t-peak_t).total_seconds()/60.0
    out['peak_mfe_pct']=pct(peak['h'],entry)
    if arm_t:
        out['arm_to_pp_min']=(pp_t-arm_t).total_seconds()/60.0
        out['arm_to_peak_min']=(peak_t-arm_t).total_seconds()/60.0
        after=[b for b in path if b['ts']>=arm_b['ts']]
        if after:
            out['post_arm_low_pct']=pct(min(b['l'] for b in after),entry)
            out['post_arm_high_pct']=pct(max(b['h'] for b in after),entry)
            out['post_arm_range_pct']=out['post_arm_high_pct']-out['post_arm_low_pct']
    else:
        out['arm_to_pp_min']=None;out['arm_to_peak_min']=None
    give=out.get('mfe_giveback_to_pp'); mins=out.get('peak_to_pp_min')
    out['giveback_speed_pp_per_min']=give/max(mins,1.0) if give is not None and mins is not None else None
    return out

# ------------------------------------------------------------------
# Extract 251 checkpoints
# ------------------------------------------------------------------
say('=== PP12 CHECKPOINT FEATURES ===')
rows=[]; errs=[]
for i,r in enumerate(P.itertuples(index=False),1):
    try:
        et=pd.Timestamp(r.entry_dt).to_pydatetime(); et=et.replace(tzinfo=UTC) if et.tzinfo is None else et.astimezone(UTC)
        pt=pd.Timestamp(r.pp_exit_time_utc).to_pydatetime(); pt=pt.replace(tzinfo=UTC) if pt.tzinfo is None else pt.astimezone(UTC)
        sym=str(r.symbol); entry=float(r.entry_price); pp_px=float(r.pp_exit_price)
        start=min(et-timedelta(minutes=5),pt-timedelta(hours=12)); raw=U.KC.get(sym,'1m',start,pt+timedelta(minutes=2)); one=to_bars(raw)
        d={k:getattr(r,k) for k in P.columns if hasattr(r,k)}
        d.update({f'PP_{k}':v for k,v in feature_pack(one,pt,entry,et).items()})
        d.update({f'PP_{k}':v for k,v in market_features(pt).items()})
        d.update({f'PP_{k}':v for k,v in candidate_features(pt).items()})
        d.update({f'PP_{k}':v for k,v in recent_exit_features(pt).items()})
        d.update({f'PP_{k}':v for k,v in up24_features(pt).items()})
        d.update({f'PPX_{k}':v for k,v in pp_specific(one,et,pt,entry,pp_px,fv(r.base_pp_mfe_pct),fv(r.base_pp_mae_pct)).items()})
        rows.append(d)
    except Exception as e:errs.append((getattr(r,'setup_id','?'),repr(e)))
    if i%25==0 or i==len(P):say('PPF',i,'/',len(P),'ok',len(rows),'err',len(errs))
if errs:
    for x in errs[:20]:say('ERR',*x)
if len(rows)<249:raise SystemExit(f'FEATURE COVERAGE TOO LOW {len(rows)}/251')
F=pd.DataFrame(rows)
write_csv(OUTDIR/'PP12_GATE_FEATURES.csv',F)

# ------------------------------------------------------------------
# Rule mining - selection on DISC + VAL, OOS display only
# ------------------------------------------------------------------
id_cols={'setup_id','symbol','period','month','split','entry_dt','pp_exit_time_utc','result','exit_time_utc'}
target_cols={'target_improve','target_avg_exit','delta_vs_pp_pct','net_pct','gross_pct','fee_pct','add_realized_pct','attempts'}
future_cols={c for c in F.columns if c.startswith('post_') or c in ('entry_recovered_24h','tp20_reached_24h','entry_recovery_min','tp20_min','close24_entry_pct')}
features=[]
for c in F.columns:
    if c in id_cols or c in target_cols or c in future_cols:continue
    x=pd.to_numeric(F[c],errors='coerce')
    if x.notna().sum()>=220 and x.nunique(dropna=True)>=4:features.append(c)
say('NUMERIC CAUSAL FEATURES',len(features))

D=F[F.split=='DISCOVERY_0106'].copy(); V=F[F.split=='VALIDATION_0708'].copy(); O=F[F.split=='OOS_09'].copy()

def atom_mask(g,f,op,th):
    x=pd.to_numeric(g[f],errors='coerce')
    return (x>=th)&x.notna() if op=='>=' else (x<=th)&x.notna()

def stats(g,m,target='target_improve'):
    h=g[m]
    if len(h)==0:return (0,np.nan)
    return len(h),float(h[target].mean())

single=[]; atoms=[]
for f in features:
    x=pd.to_numeric(D[f],errors='coerce').dropna()
    if len(x)<50:continue
    ths=np.unique(np.quantile(x,np.linspace(.05,.95,19)))
    for op in ('>=','<='):
        for th in ths:
            md=atom_mask(D,f,op,float(th)); nd,pd_=stats(D,md)
            if nd<12:continue
            mv=atom_mask(V,f,op,float(th)); nv,pv=stats(V,mv)
            mo=atom_mask(O,f,op,float(th)); no,po=stats(O,mo)
            row={'feature':f,'op':op,'threshold':float(th),'D_hit':nd,'D_precision':pd_,'D_base':float(D.target_improve.mean()),
                 'V_hit':nv,'V_precision':pv,'V_base':float(V.target_improve.mean()),'OOS_hit':no,'OOS_precision':po,'OOS_base':float(O.target_improve.mean())}
            single.append(row)
            # keep atoms with at least mild DISC discrimination for pair search
            if abs(pd_-float(D.target_improve.mean()))>=0.08:atoms.append((f,op,float(th)))
S=pd.DataFrame(single)
Ssucc=S[(S.D_precision>=0.65)&(S.V_hit>=5)&(S.V_precision>=0.65)].copy().sort_values(['V_precision','D_precision','D_hit'],ascending=[False,False,False])
Sfail=S[(S.D_precision<=0.35)&(S.V_hit>=5)&(S.V_precision<=0.35)].copy().sort_values(['V_precision','D_precision','D_hit'],ascending=[True,True,False])
write_csv(OUTDIR/'PP12_SINGLE_SUCCESS.csv',Ssucc);write_csv(OUTDIR/'PP12_SINGLE_FAILURE.csv',Sfail)
# de-duplicate atoms; cap by strongest discovery deviation/coverage
atoms=list(dict.fromkeys(atoms))
def atom_rank(a):
    m=atom_mask(D,*a); n,p=stats(D,m);return (abs(p-D.target_improve.mean()),n)
atoms=sorted(atoms,key=atom_rank,reverse=True)[:180]
say('PAIR ATOMS',len(atoms))

pairs=[]
for i,a in enumerate(atoms):
    for b in atoms[i+1:]:
        if a[0]==b[0]:continue
        md=atom_mask(D,*a)&atom_mask(D,*b); nd,pd_=stats(D,md)
        if nd<10 or not (pd_>=0.72 or pd_<=0.28):continue
        mv=atom_mask(V,*a)&atom_mask(V,*b); nv,pv=stats(V,mv)
        if nv<5:continue
        mo=atom_mask(O,*a)&atom_mask(O,*b); no,po=stats(O,mo)
        pairs.append({'rule':f'{a[0]} {a[1]} {a[2]:.8g} AND {b[0]} {b[1]} {b[2]:.8g}',
                      'f1':a[0],'op1':a[1],'th1':a[2],'f2':b[0],'op2':b[1],'th2':b[2],
                      'D_hit':nd,'D_precision':pd_,'D_base':float(D.target_improve.mean()),
                      'V_hit':nv,'V_precision':pv,'V_base':float(V.target_improve.mean()),
                      'OOS_hit':no,'OOS_precision':po,'OOS_base':float(O.target_improve.mean())})
Q=pd.DataFrame(pairs)
if len(Q):
    Qsucc=Q[(Q.D_precision>=.75)&(Q.V_precision>=.75)].copy().sort_values(['V_precision','D_precision','D_hit'],ascending=[False,False,False])
    Qfail=Q[(Q.D_precision<=.25)&(Q.V_precision<=.25)].copy().sort_values(['V_precision','D_precision','D_hit'],ascending=[True,True,False])
else:Qsucc=Q.copy();Qfail=Q.copy()
write_csv(OUTDIR/'PP12_PAIR_SUCCESS.csv',Qsucc);write_csv(OUTDIR/'PP12_PAIR_FAILURE.csv',Qfail)

# Econ summaries and causal-simple sanity table
simple=['PPX_entry_to_pp_min','PPX_pp_close_entry_pct','PPX_pp_mfe_pct','PPX_pp_mae_pct','PPX_mfe_giveback_to_pp','PPX_arm_to_pp_min','PPX_peak_to_pp_min','PPX_giveback_speed_pp_per_min']
sm=[]
for f in simple:
    if f not in F:continue
    for sp,g in [('DISC',D),('VAL',V),('OOS',O),('ALL',F)]:
        for y in (1,0):
            z=pd.to_numeric(g[g.target_improve==y][f],errors='coerce').dropna()
            sm.append({'feature':f,'split':sp,'class':'IMPROVE' if y else 'NO_IMPROVE','n':len(z),'median':z.median() if len(z) else np.nan,'mean':z.mean() if len(z) else np.nan,'q25':z.quantile(.25) if len(z) else np.nan,'q75':z.quantile(.75) if len(z) else np.nan})
write_csv(OUTDIR/'PP12_CAUSAL_SIMPLE_SUMMARY.csv',pd.DataFrame(sm))

lines=[]
lines.append('FINAL P PP12 GATE FEATURES V20 — 2026-10-06')
lines.append(f'PP N={len(F)} ECON_IMPROVE={int(F.target_improve.sum())}/{len(F)} ({F.target_improve.mean():.2%}) AVG_EXIT={int(F.target_avg_exit.sum())}/{len(F)} ({F.target_avg_exit.mean():.2%})')
lines.append('Economic label is delta_vs_original_PP > 0. This is stricter and more relevant than AVG_EXIT alone.')
for sp,g in [('DISCOVERY_0106',D),('VALIDATION_0708',V),('OOS_09',O)]:lines.append(f'{sp}: N={len(g)} IMPROVE={int(g.target_improve.sum())} RATE={g.target_improve.mean():.2%}')
lines.append(f'CAUSAL_FEATURES={len(features)} SINGLE_SUCCESS={len(Ssucc)} SINGLE_FAILURE={len(Sfail)} PAIR_SUCCESS={len(Qsucc)} PAIR_FAILURE={len(Qfail)}')
lines.append('Selection used DISC+VAL only. Sep OOS was never used to choose rules.')
lines.append('')
lines.append('[TOP SUCCESS PAIRS]')
if len(Qsucc):
    for r in Qsucc.head(12).itertuples(index=False):lines.append(f'{r.rule} | D {r.D_hit}/{r.D_precision:.3f} V {r.V_hit}/{r.V_precision:.3f} OOS {r.OOS_hit}/{r.OOS_precision if pd.notna(r.OOS_precision) else float("nan"):.3f}')
else:lines.append('NONE')
lines.append('')
lines.append('[TOP FAILURE PAIRS]')
if len(Qfail):
    for r in Qfail.head(12).itertuples(index=False):lines.append(f'{r.rule} | D {r.D_hit}/{r.D_precision:.3f} V {r.V_hit}/{r.V_precision:.3f} OOS {r.OOS_hit}/{r.OOS_precision if pd.notna(r.OOS_precision) else float("nan"):.3f}')
else:lines.append('NONE')
(OUTDIR/'SUMMARY.txt').write_text('\n'.join(lines)+'\n',encoding='utf-8')
(OUTDIR/'RUN_LOG.txt').write_text('\n'.join(LOG)+'\n',encoding='utf-8')
with zipfile.ZipFile(OUTZIP,'w',zipfile.ZIP_DEFLATED) as z:
    for p in sorted(OUTDIR.iterdir()):
        if p.is_file():z.write(p,p.name)
say('=== COMPLETE ===');say('ZIP',OUTZIP)
print('\n'.join(lines),flush=True)
