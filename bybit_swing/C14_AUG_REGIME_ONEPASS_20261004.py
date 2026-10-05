#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
P형 C14 8월 시장-regime ONE-PASS 연구
2026-10-04

목표
1) C1~C11 exact baseline 재현 감사
2) 8월 STOP 군집의 시장 feature 비교
3) 고정 C14 후보 A/B/C/D/E를 대체진입 포함 exact replay
4) .janaug_hist_cache_v1가 있으면 실제 alt UP24 breadth 6/12/24h 변화 +
   BTC/ETH 7D/30D를 복구해 direct 일괄검색
5) 장기에서도 살아있는 true-market 후보 상위 최대 5개를 exact replay

안전
- bot.py 수정 없음
- DB write 없음
- 주문 없음
- 결과 CSV/ZIP만 생성
"""
from pathlib import Path
from collections import Counter, deque, defaultdict
import runpy, sqlite3, json, math, zipfile, gzip, io, re, time
import pandas as pd
import numpy as np

R=Path('/root/hyejin-trader/bybit_swing')
STAMP='20261004'
OUTDIR=R/f'C14_AUG_REGIME_ONEPASS_{STAMP}'
OUTDIR.mkdir(exist_ok=True)
LOG=[]
def say(*a):
    s=' '.join(str(x) for x in a); print(s,flush=True); LOG.append(s)

def fv(v,d=None):
    try:
        x=float(v)
        return d if math.isnan(x) or math.isinf(x) else x
    except Exception:
        return d

def jl(v):
    if isinstance(v,dict): return dict(v)
    if not v: return {}
    try:
        x=json.loads(str(v)); return x if isinstance(x,dict) else {}
    except Exception: return {}

# ------------------------------------------------------------
# 1. Load validated C5 engine + exact candidate streams.
# ------------------------------------------------------------
BASE_SCRIPT=R/'breadth_persistence_diag.py'
if not BASE_SCRIPT.exists(): BASE_SCRIPT=Path('/tmp/breadth_persistence_diag.py')
if not BASE_SCRIPT.exists(): raise SystemExit('MISSING breadth_persistence_diag.py')
say('=== LOAD VALIDATED C5 ENGINE ===')
ns=runpy.run_path(str(BASE_SCRIPT))
U=ns['U']; ja=ns['ja'].copy(); pre=ns['pre']; blocked=ns['blocked']; getsim=ns['getsim']

# C6 frozen exact IDs.
C6_IDS={
'P25SET-AKEUSDT-1964292','P25SET-AGLDUSDT-1968650','P25SET-SIGNUSDT-1969756','P25SET-KITEUSDT-1969768',
'P25SET-XANUSDT-1971016','P25SET-TRIAUSDT-1971329','P25SET-STOUSDT-1971719','P25SET-ZBTUSDT-1974617',
'P25SET-BSBUSDT-1979575','P25SET-EPICUSDT-1981325','P25SET-LDOUSDT-1981649','P25SET-BILLUSDT-1982088',
'P25SET-FHEUSDT-1982101','P25SET-JCTUSDT-1982161','P25SET-REUSDT-1983051','P25SET-BILLUSDT-1983199',
'P25SET-REUSDT-1983199','P25SET-CAPUSDT-1983218','P25SET-SAGAUSDT-1988348'}

packs=sorted(R.glob('P_RESEARCH_PACK_*.zip'),key=lambda p:p.stat().st_mtime,reverse=True)
if not packs: raise SystemExit('MISSING P_RESEARCH_PACK_*.zip')
PACK=packs[0]; say('RESEARCH PACK',PACK)
feat={}
def add_features(d):
    for r in d.to_dict('records'):
        sid=str(r.get('setup_id') or '')
        if not sid: continue
        w=jl(r.get('watch_details_json')); q=jl(r.get('details_json'))
        ep=fv(r.get('entry_price')); trig=fv(r.get('trigger_price'))
        if trig is None: trig=fv(q.get('price'))
        low=fv(r.get('lowest_price_before_confirm'))
        ws=fv(r.get('watch_p_v2_score'))
        if ws is None: ws=fv(w.get('p_v2_score'))
        wb=fv(r.get('watch_btc15'))
        if wb is None: wb=fv(w.get('btc_15m_change_pct'))
        feat[sid]=dict(
            rsi_delta=fv(q.get('rsi_delta')), live_gain=fv(q.get('live_candle_gain_pct')),
            gap=fv(q.get('ema9_ema20_gap_pct')), slope=fv(q.get('ema9_slope_prev1_pct')),
            entry=ep, trigger=trig, watch_score=ws, final_score=fv(q.get('p_v2_score')),
            rsi=fv(q.get('rsi')), watch_btc15=wb, final_btc15=fv(q.get('btc_15m_change_pct')),
            entry_from_low=((ep/low-1)*100 if ep is not None and low is not None and low>0 else None),
            eth4h=fv(q.get('eth_4h_change_pct')), btc4h=fv(q.get('btc_4h_change_pct')),
            pullback=fv(q.get('pullback_from_high_pct')), ema9_prev1=fv(q.get('ema9_slope_prev1_pct')),
        )
with zipfile.ZipFile(PACK) as z:
    for m in range(1,9):
        n=f'2026-{m:02d}_CANDIDATES.csv.gz'
        d=pd.read_csv(gzip.GzipFile(fileobj=io.BytesIO(z.read(n))),low_memory=False)
        add_features(d)
if len(feat)<20000: raise SystemExit(f'FEATURE LOAD TOO SMALL {len(feat)}')

# ------------------------------------------------------------
# 2. Existing C7~C11 guards.
# ------------------------------------------------------------
def mi(r):
    return dict(eth24=fv(r.get('eth24_pct')),btc48=fv(r.get('btc48_pct')),abs4=fv(r.get('abs4h_avg')),
                v25=fv(r.get('v25_2h_count')),shadow=fv(r.get('market_shadow_sum')))
def c7(sid):
    q=feat.get(str(sid),{}); vals=[q.get('rsi_delta'),q.get('live_gain'),q.get('gap'),q.get('slope'),q.get('entry'),q.get('trigger')]
    if any(v is None for v in vals) or (q.get('trigger') or 0)<=0: return False
    jump=(q['entry']/q['trigger']-1)*100
    return q['rsi_delta']<=1.5 and q['live_gain']>=.5 and q['gap']<=1 and q['slope']>=.3 and jump>=.5
def c8(sid,r):
    q=feat.get(str(sid),{}); m=mi(r)
    ws=q.get('watch_score'); fin=q.get('final_score'); live=q.get('live_gain'); rsi=q.get('rsi'); wb=q.get('watch_btc15'); fb=q.get('final_btc15'); lowj=q.get('entry_from_low')
    sd=(fin-ws if fin is not None and ws is not None else None); bd=(fb-wb if fb is not None and wb is not None else None)
    M=((m['abs4'] is not None and m['v25'] is not None and m['abs4']>=.30 and m['v25']<=5) or
       (m['eth24'] is not None and m['v25'] is not None and m['eth24']>=.75 and m['v25']<=8))
    A=(M and ws is not None and ws>=87.7 and sd is not None and sd<=-4 and fin is not None and fin<95 and lowj is not None and lowj>=.25 and m['btc48'] is not None and
       (m['btc48']>=-2 or (m['btc48']<-2 and rsi is not None and rsi>=68 and live is not None and live<2)))
    B1=(live is not None and live>=1.25 and rsi is not None and rsi>=78 and m['abs4'] is not None and m['abs4']>=.18)
    B2=(bd is not None and bd<=-.22 and lowj is not None and lowj<=.28 and m['abs4'] is not None and m['abs4']>=.30 and ws is not None and ws<87)
    return bool(A or (M and (B1 or B2)))
def c9(sid,r):
    q=feat.get(str(sid),{}); m=mi(r)
    return bool(q.get('eth4h') is not None and q['eth4h']>=.92 and m['abs4'] is not None and m['abs4']<=.82 and q.get('rsi') is not None and q['rsi']>=65)
def c10(sid,r):
    q=feat.get(str(sid),{}); m=mi(r)
    A=(m['btc48'] is not None and m['btc48']<=-7 and q.get('final_score') is not None and q['final_score']<=85 and m['abs4'] is not None and m['abs4']<=.40)
    B=(m['btc48'] is not None and m['btc48']>=1 and m['v25'] is not None and m['v25']>=8 and m['shadow'] is not None and m['shadow']<=-6 and q.get('pullback') is not None and q['pullback']<=1.5 and q.get('ema9_prev1') is not None and q['ema9_prev1']<=.20 and q.get('final_score') is not None and q['final_score']<95)
    return bool(A or B)
def c11(r):
    m=mi(r); return bool(m['v25'] is not None and m['v25']>=17 and m['shadow'] is not None and m['shadow']<=-9)

def blocked_c11(sid,r):
    return sid in C6_IDS or c7(sid) or c8(sid,r) or c9(sid,r) or c10(sid,r) or c11(r)

# ------------------------------------------------------------
# 3. C5-eligible trailing candidate breadth proxy (path-independent).
# ------------------------------------------------------------
say('=== BUILD C5 ELIGIBLE TRAILING BREADTH ===')
el=[]
for _,r in ja.iterrows():
    if pre(r,'JA') and not blocked(r,'JA'):
        el.append((pd.Timestamp(r['_dt']),str(r['setup_id']),str(r['symbol'])))
el.sort(key=lambda z:(z[0],z[1]))
proxy={}
for hours in (6,12,24):
    dq=deque(); cnt=Counter(); key=f'cand_breadth_{hours}h'
    for t,sid,sym in el:
        dq.append((t,sym)); cnt[sym]+=1; cutoff=t-pd.Timedelta(hours=hours)
        while dq and dq[0][0]<cutoff:
            _,ss=dq.popleft(); cnt[ss]-=1
            if cnt[ss]<=0: del cnt[ss]
        proxy.setdefault(sid,{})[key]=len(cnt)
say('PROXY FEATURES',len(proxy))

# ------------------------------------------------------------
# 4. Generic exact replay on Jan-Aug, after C11.
# ------------------------------------------------------------
def run(extra_guard=None,label='S11'):
    sched=U.Scheduler(); trades=[]; blocks=[]
    for _,r in ja.iterrows():
        if not pre(r,'JA') or blocked(r,'JA'): continue
        sid=str(r['setup_id']); sym=str(r['symbol'])
        if blocked_c11(sid,r): continue
        if extra_guard is not None and extra_guard(sid,r):
            blocks.append(dict(setup_id=sid,symbol=sym,month=str(r['entry_time_kst'])[:7],day=str(r['entry_time_kst'])[:10],guard=label))
            continue
        now=pd.Timestamp(r['_dt']).to_pydatetime(); ok,_=sched.can_open(now,sym)
        if not ok: continue
        s=getsim(r,'JA'); sched.add(now,sym,s,s.result in ('STOP','LATE_FAILURE_EXIT'))
        trades.append(dict(setup_id=sid,symbol=sym,month=str(r['entry_time_kst'])[:7],day=str(r['entry_time_kst'])[:10],entry_dt=pd.Timestamp(now),result=str(s.result),net_pct=float(s.net_pct)))
    return pd.DataFrame(trades),pd.DataFrame(blocks)
def summary(t,label):
    out=[]
    for m in [f'2026-{i:02d}' for i in range(1,9)]:
        g=t[t.month==m]; rc=Counter(g.result.astype(str))
        out.append(dict(scenario=label,month=m,N=len(g),TP=rc['TP20_FULL'],STOP=rc['STOP'],PP=rc['PROFIT_PROTECT_EXIT'],TIME=rc['TIME_EXIT'],LATE=rc['LATE_FAILURE_EXIT'],NET=float(g.net_pct.sum())))
    return pd.DataFrame(out)

say('=== RUN S11 BASELINE AUDIT ===')
base,base_blocks=run(None,'S11')
bs=summary(base,'S11')
EXPECTED={
'2026-01':(746,370,253,-12.373023),'2026-02':(458,226,149,-32.201862),'2026-03':(592,285,200,-61.114202),'2026-04':(727,391,218,50.423912),
'2026-05':(740,399,225,100.679087),'2026-06':(520,256,185,-53.333746),'2026-07':(599,283,195,-59.068632),'2026-08':(733,358,245,-52.507294)}
for r in bs.itertuples(index=False):
    e=EXPECTED[r.month]; ok=(r.N==e[0] and r.TP==e[1] and r.STOP==e[2] and abs(r.NET-e[3])<.03)
    say(r.month,'N',r.N,'TP',r.TP,'STOP',r.STOP,'NET',round(r.NET,3),'AUDIT',ok)
    if not ok: raise SystemExit('S11 BASELINE AUDIT FAIL '+r.month)
say('S11 BASELINE AUDIT = PASS')

# ------------------------------------------------------------
# 5. Fixed robust candidates found from direct Jan-Aug C11 path.
# ------------------------------------------------------------
def gA(sid,r): return fv(r.get('eth24_pct'),999)>=1.0 and fv(r.get('eth48_pct'),999)<=0.0
def gB(sid,r): return fv(r.get('market_shadow_sum'),999)<=-3.0 and fv((feat.get(sid) or {}).get('eth4h'),999)<=-0.90
def gC(sid,r): return fv(r.get('btc48_pct'),999)<=-0.50 and fv(r.get('eth24_pct'),-999)>=1.0
def gD(sid,r):
    p=proxy.get(sid,{})
    return p.get('cand_breadth_12h',999)<=10 and abs(fv(r.get('btc48_pct'),999)-fv(r.get('eth48_pct'),-999))<=0.10
def gE(sid,r):
    p=proxy.get(sid,{})
    return p.get('cand_breadth_24h',999)<=15 and abs(fv(r.get('btc24_pct'),999)-fv(r.get('eth24_pct'),-999))<=0.27
FIXED={
'A_ETH24UP_ETH48NONPOS':gA,
'B_SHADOWNEG_ETH4HDOWN':gB,
'C_BTC48DOWN_ETH24UP':gC,
'D_LOW12H_SYNC48':gD,
'E_LOW24H_SYNC24':gE,
'AB':lambda sid,r:gA(sid,r) or gB(sid,r),
'AD':lambda sid,r:gA(sid,r) or gD(sid,r),
'BD':lambda sid,r:gB(sid,r) or gD(sid,r),
'ABD':lambda sid,r:gA(sid,r) or gB(sid,r) or gD(sid,r),
'ABDE':lambda sid,r:gA(sid,r) or gB(sid,r) or gD(sid,r) or gE(sid,r),
}

all_summ=[bs]; scen={'S11':base}; blocksets={}
for name,fn in FIXED.items():
    say('=== EXACT FIXED',name,'===')
    t,b=run(fn,name); s=summary(t,name); all_summ.append(s); scen[name]=t; blocksets[name]=b
    say(name,'TOTAL',round(t.net_pct.sum(),3),'DELTA',round(t.net_pct.sum()-base.net_pct.sum(),3),'AUG',round(s.loc[s.month=='2026-08','NET'].iloc[0],3),'AUG_DELTA',round(s.loc[s.month=='2026-08','NET'].iloc[0]-EXPECTED['2026-08'][3],3))

# ------------------------------------------------------------
# 6. Recover TRUE alt UP24 breadth + BTC/ETH 7D/30D from historical 60m cache.
# ------------------------------------------------------------
TRUE_READY=False; true_by_sid={}; true_direct=pd.DataFrame(); auto_defs=[]
CACHE=R/'.janaug_hist_cache_v1'
if CACHE.exists():
    say('=== BUILD TRUE MARKET 60M FEATURES ===')
    pat=re.compile(r'^(?P<sym>.+)_60_(?P<s>\d{10})_(?P<e>\d{10})\.csv\.gz$')
    idx=defaultdict(list)
    for p in CACHE.glob('*_60_*.csv.gz'):
        m=pat.match(p.name)
        if m: idx[m.group('sym')].append(p)
    say('CACHE SYMBOLS',len(idx),'FILES',sum(len(v) for v in idx.values()))
    def read_symbol(sym):
        parts=[]
        for p in idx.get(sym,[]):
            try:
                d=pd.read_csv(p,usecols=lambda c:c in ('start_time','ts','close'),compression='gzip',low_memory=False)
                if 'start_time' in d.columns: d['ts2']=pd.to_datetime(d.start_time,utc=True,errors='coerce')
                else:
                    z=pd.to_numeric(d['ts'],errors='coerce'); d['ts2']=pd.to_datetime(z,unit='ms',utc=True,errors='coerce') if z.dropna().median()>1e12 else pd.to_datetime(d['ts'],utc=True,errors='coerce')
                d['close']=pd.to_numeric(d['close'],errors='coerce'); parts.append(d[['ts2','close']])
            except Exception: pass
        if not parts: return pd.DataFrame(columns=['ts','close'])
        d=pd.concat(parts,ignore_index=True).dropna().drop_duplicates('ts2').sort_values('ts2').rename(columns={'ts2':'ts'})
        return d
    # BTC/ETH trend series.
    refs=[]
    for sym,prefix in [('BTCUSDT','btc'),('ETHUSDT','eth')]:
        d=read_symbol(sym)
        if len(d)<100: continue
        for h in (24,48,168,720): d[f'{prefix}{h}h']=(d.close/d.close.shift(h)-1)*100
        d['available_time']=d.ts+pd.Timedelta(hours=1)
        refs.append(d[['available_time',f'{prefix}24h',f'{prefix}48h',f'{prefix}168h',f'{prefix}720h']])
    if len(refs)==2:
        ref=pd.merge(refs[0],refs[1],on='available_time',how='outer').sort_values('available_time')
    else: ref=pd.DataFrame()
    # True alt breadth from same historical universe; exclude BTC/ETH.
    pieces=[]; alts=[s for s in idx if s not in ('BTCUSDT','ETHUSDT')]
    for i,sym in enumerate(alts,1):
        d=read_symbol(sym)
        if len(d)>=30:
            d['r24']=(d.close/d.close.shift(24)-1)*100
            q=d.dropna(subset=['r24'])[['ts','r24']].copy(); q['up']=(q.r24>0).astype('int8'); pieces.append(q[['ts','up']])
        if i%100==0: say('BREADTH READ',i,'/',len(alts))
    if pieces and len(ref):
        L=pd.concat(pieces,ignore_index=True)
        B=L.groupby('ts').agg(up_n=('up','sum'),n=('up','size')).reset_index().sort_values('ts')
        B['up24']=100*B.up_n/B.n
        for h in (6,12,24): B[f'up24_delta{h}']=B.up24-B.up24.shift(h)
        B['available_time']=B.ts+pd.Timedelta(hours=1)
        market=B[['available_time','up24','up24_delta6','up24_delta12','up24_delta24','n']].sort_values('available_time')
        # Attach to every JA candidate using last completed hourly row.
        q=ja[['setup_id','_dt']].copy(); q['_dt']=pd.to_datetime(q['_dt'],utc=True); q=q.sort_values('_dt')
        q=pd.merge_asof(q,market,left_on='_dt',right_on='available_time',direction='backward')
        q=pd.merge_asof(q.sort_values('_dt'),ref.sort_values('available_time'),left_on='_dt',right_on='available_time',direction='backward',suffixes=('','_ref'))
        for rr in q.to_dict('records'):
            true_by_sid[str(rr['setup_id'])]={k:fv(rr.get(k)) for k in ['up24','up24_delta6','up24_delta12','up24_delta24','btc24h','btc48h','btc168h','btc720h','eth24h','eth48h','eth168h','eth720h']}
        TRUE_READY=True; say('TRUE MARKET FEATURES ATTACHED',len(true_by_sid))
else:
    say('TRUE MARKET CACHE NOT FOUND - fixed exact only')

# ------------------------------------------------------------
# 7. Direct true-market grid on current exact S11 path, then exact replay top distinct <=5.
# ------------------------------------------------------------
if TRUE_READY:
    bx=base.copy(); rows=[]
    for sid in bx.setup_id.astype(str): rows.append(true_by_sid.get(sid,{}))
    ff=pd.DataFrame(rows); bx=pd.concat([bx.reset_index(drop=True),ff.reset_index(drop=True)],axis=1)
    bx.to_csv(OUTDIR/'S11_TRUE_MARKET_ENRICHED.csv',index=False)
    # simple interpretable primitives only
    prim=[]
    specs={
      'up24':([25,35,45,55,65],['<=','>=']),
      'up24_delta6':([-15,-10,-5,5,10,15],['<=','>=']),
      'up24_delta12':([-20,-15,-10,-5,5,10,15,20],['<=','>=']),
      'up24_delta24':([-25,-20,-15,-10,-5,5,10,15,20,25],['<=','>=']),
      'btc168h':([-5,-2,0,2,5],['<=','>=']), 'eth168h':([-5,-2,0,2,5],['<=','>=']),
      'btc720h':([-10,-5,0,5,10],['<=','>=']), 'eth720h':([-10,-5,0,5,10],['<=','>=']),
    }
    def mk_mask(df,f,op,v):
        s=pd.to_numeric(df[f],errors='coerce'); return (s<=v) if op=='<=' else (s>=v)
    for f,(vals,ops) in specs.items():
        for op in ops:
            for v in vals: prim.append((f,op,v,mk_mask(bx,f,op,v).to_numpy()))
    months=bx.month.astype(str).to_numpy(); res=bx.result.astype(str).to_numpy(); net=bx.net_pct.to_numpy(float)
    augm=months=='2026-08'; prev=months<'2026-08'
    def st(mask,scope):
        z=mask&scope; return int(z.sum()),int((z&(res=='TP20_FULL')).sum()),int((z&(res=='STOP')).sum()),float(net[z].sum()) if z.any() else 0.0
    cand=[]
    # singles + pairs
    defs=[]
    for p in prim: defs.append((p[0]+' '+p[1]+' '+str(p[2]),[p[:3]],p[3]))
    for i in range(len(prim)):
        for j in range(i+1,len(prim)):
            if prim[i][0]==prim[j][0]: continue
            defs.append((f'{prim[i][0]} {prim[i][1]} {prim[i][2]} AND {prim[j][0]} {prim[j][1]} {prim[j][2]}',[prim[i][:3],prim[j][:3]],prim[i][3]&prim[j][3]))
    for name,terms,mask in defs:
        A=st(mask,augm); P=st(mask,prev); T=st(mask,months<='2026-08')
        if A[0]<5 or A[2]<4 or A[3]>-8 or P[3]>0 or T[3]>-12: continue
        mn=[]
        for m in [f'2026-{i:02d}' for i in range(1,8)]: mn.append(st(mask,months==m)[3])
        pos=sum(max(0,z) for z in mn)
        if pos>5: continue
        score=(-A[3])+.5*(-P[3])+10*(A[2]/A[0])-6*(A[1]/A[0])-.01*T[0]-pos
        cand.append(dict(rule=name,terms=json.dumps(terms),score=score,aug_n=A[0],aug_tp=A[1],aug_stop=A[2],aug_net=A[3],prev_n=P[0],prev_tp=P[1],prev_stop=P[2],prev_net=P[3],total_n=T[0],total_tp=T[1],total_stop=T[2],total_net=T[3],mask=mask))
    if cand:
        cs=pd.DataFrame([{k:v for k,v in q.items() if k!='mask'} for q in cand]).sort_values('score',ascending=False)
        cs.to_csv(OUTDIR/'TRUE_MARKET_DIRECT_SCAN.csv',index=False)
        selected=[]; masks=[]
        for q0 in sorted(cand,key=lambda z:z['score'],reverse=True):
            m=q0['mask']
            if any((np.sum(m&z)/max(1,np.sum(m|z)))>.85 for z in masks): continue
            selected.append(q0); masks.append(m)
            if len(selected)>=5: break
        say('TRUE AUTO SELECTED',len(selected))
        def make_guard(terms):
            def fn(sid,r):
                f=true_by_sid.get(sid,{})
                for col,op,val in terms:
                    z=fv(f.get(col))
                    if z is None: return False
                    if op=='<=' and not (z<=val): return False
                    if op=='>=' and not (z>=val): return False
                return True
            return fn
        true_exact=[]
        for k,q0 in enumerate(selected,1):
            terms=json.loads(q0['terms']); name=f'TRUE{k}'
            say('=== EXACT',name,q0['rule'],'===')
            t,b=run(make_guard(terms),name); s=summary(t,name); all_summ.append(s); scen[name]=t; blocksets[name]=b
            augnet=float(s.loc[s.month=='2026-08','NET'].iloc[0]); delta=float(t.net_pct.sum()-base.net_pct.sum())
            true_exact.append(dict(name=name,rule=q0['rule'],total_net=float(t.net_pct.sum()),delta_total=delta,aug_net=augnet,delta_aug=augnet-EXPECTED['2026-08'][3]))
            say(name,'TOTAL',round(t.net_pct.sum(),3),'DELTA',round(delta,3),'AUG',round(augnet,3),'AUG_DELTA',round(augnet-EXPECTED['2026-08'][3],3))
        pd.DataFrame(true_exact).to_csv(OUTDIR/'TRUE_MARKET_EXACT_TOP.csv',index=False)
    else:
        pd.DataFrame(columns=['rule']).to_csv(OUTDIR/'TRUE_MARKET_DIRECT_SCAN.csv',index=False)
        say('NO ROBUST TRUE-MARKET DIRECT CANDIDATE')

# ------------------------------------------------------------
# 8. Save outputs.
# ------------------------------------------------------------
SUM=pd.concat(all_summ,ignore_index=True)
SUM.to_csv(OUTDIR/'MONTHLY_ALL_SCENARIOS.csv',index=False)
# compact delta table
base_net=base.net_pct.sum(); delta=[]
for name,t in scen.items():
    s=summary(t,name); augnet=float(s.loc[s.month=='2026-08','NET'].iloc[0])
    delta.append(dict(scenario=name,total_net=float(t.net_pct.sum()),delta_total=float(t.net_pct.sum()-base_net),aug_net=augnet,delta_aug=augnet-EXPECTED['2026-08'][3]))
pd.DataFrame(delta).sort_values(['delta_total','delta_aug'],ascending=False).to_csv(OUTDIR/'SCENARIO_DELTAS.csv',index=False)
for name,b in blocksets.items():
    if len(b): b.to_csv(OUTDIR/f'BLOCKS_{name}.csv',index=False)
# save full trades only baseline and top fixed unions to keep zip compact
for name in ['S11','AB','ABD','ABDE']:
    if name in scen: scen[name].to_csv(OUTDIR/f'TRADES_{name}.csv',index=False)
(OUTDIR/'RUN_LOG.txt').write_text('\n'.join(LOG)+'\n',encoding='utf-8')
zip_path=R/f'C14_AUG_REGIME_ONEPASS_{STAMP}_RESULTS.zip'
with zipfile.ZipFile(zip_path,'w',zipfile.ZIP_DEFLATED) as z:
    for p in sorted(OUTDIR.iterdir()):
        if p.is_file(): z.write(p,p.name)
say('RESULT ZIP',zip_path)
say('=== DONE ===')
