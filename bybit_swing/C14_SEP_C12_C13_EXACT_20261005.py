#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
P형 C14(ABDE) 9월 + C12/C13 exact 통합검증 — 2026-10-05

검증 순서
1) C1~C11 baseline exact audit (기존 결과 ZIP과 일치 확인)
2) C14=ABDE를 1~9월에 대체진입 포함 exact replay
3) C12, C13 direct 정의 audit (기존 Jan-Aug 연구 수치 재현)
4) C1~C11+C14 위에서 C12 / C13 / C12 OR C13를 각각 exact replay
5) 비교용으로 C12/C13 단독 경로도 exact replay

C14 ABDE
 A: ETH24 >= +1% AND ETH48 <= 0%
 B: market_shadow_sum <= -3 AND confirmed ETH4H <= -0.90%
 D: trailing C5-eligible distinct-symbol breadth 12H <= 10 AND |BTC48-ETH48| <= 0.10%p
 E: trailing C5-eligible distinct-symbol breadth 24H <= 15 AND |BTC24-ETH24| <= 0.27%p

C12
 - confirmed P_V21 persistence score <= 47
 - WATCH RSI delta >= 8.75
 - confirmed volume ratio <= 1.0

C13 (C12 미해당만)
 - confirmed EMA9 prev1 slope <= +0.18%
 - WATCH -> confirmed BTC4H change >= +0.24%p
 - confirmed BTC4H <= -0.90%

안전
- bot.py 수정 없음
- DB write 없음
- 주문 없음
- 결과 CSV/ZIP만 생성
"""

from pathlib import Path
from collections import Counter, deque
from datetime import timedelta, timezone
import runpy, sqlite3, json, math, zipfile, gzip, io
import pandas as pd
import numpy as np

R = Path('/root/hyejin-trader/bybit_swing')
STAMP = '20261005'
OUTDIR = R / f'C14_SEP_C12_C13_EXACT_{STAMP}'
OUTDIR.mkdir(exist_ok=True)
LOG = []
KST = timezone(timedelta(hours=9))
UTC = timezone.utc


def say(*a):
    s = ' '.join(str(x) for x in a)
    print(s, flush=True)
    LOG.append(s)


def fv(v, d=None):
    try:
        if v is None or str(v).strip() == '':
            return d
        x = float(v)
        return d if math.isnan(x) or math.isinf(x) else x
    except Exception:
        return d


def jl(v):
    if isinstance(v, dict):
        return dict(v)
    if not v:
        return {}
    try:
        x = json.loads(str(v))
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def dt_utc(v):
    if v is None or str(v).strip() == '':
        return None
    try:
        t = pd.Timestamp(v)
        if t.tzinfo is None:
            t = t.tz_localize(UTC)
        else:
            t = t.tz_convert(UTC)
        return t
    except Exception:
        return None


# ------------------------------------------------------------------
# 1) Validated C5 engine / exact candidate streams
# ------------------------------------------------------------------
BASE = R / 'breadth_persistence_diag.py'
if not BASE.exists():
    BASE = Path('/tmp/breadth_persistence_diag.py')
if not BASE.exists():
    raise SystemExit('MISSING breadth_persistence_diag.py')

say('=== LOAD VALIDATED C5 ENGINE ===')
ns = runpy.run_path(str(BASE))
U = ns['U']
ja = ns['ja'].copy()
se = ns['se'].copy()
pre = ns['pre']
blocked = ns['blocked']
getsim = ns['getsim']

# ------------------------------------------------------------------
# 2) Load frozen C1~C11 block IDs + prior exact S11 trade ledger.
#    We deliberately use the already validated result ZIP so C6~C11
#    condition interpretation cannot drift in this follow-up test.
# ------------------------------------------------------------------
resz = sorted(
    R.glob('C8_C11_EXACT_REPLACEMENT_*_RESULTS.zip'),
    key=lambda p: p.stat().st_mtime,
    reverse=True,
)
if not resz:
    raise SystemExit('MISSING C8_C11_EXACT_REPLACEMENT_*_RESULTS.zip')
C11ZIP = resz[0]
say('C11 EXACT RESULT', C11ZIP)

with zipfile.ZipFile(C11ZIP) as z:
    names = set(z.namelist())
    if 'BLOCKS_C11.csv' not in names or 'TRADES_C11.csv' not in names:
        raise SystemExit('C11 RESULT ZIP missing BLOCKS_C11/TRADES_C11')
    b11 = pd.read_csv(z.open('BLOCKS_C11.csv'), low_memory=False)
    old_s11 = pd.read_csv(z.open('TRADES_C11.csv'), low_memory=False)

BASE_C11_IDS = set(b11.setup_id.astype(str))

EXPECTED_S11 = {
    '2026-01': (746,370,253,-12.3730225911),
    '2026-02': (458,226,149,-32.2018615746),
    '2026-03': (592,285,200,-61.1142015830),
    '2026-04': (727,391,218, 50.4239117530),
    '2026-05': (740,399,225,100.6790869660),
    '2026-06': (520,256,185,-53.3337456923),
    '2026-07': (599,283,195,-59.0686319361),
    '2026-08': (733,358,245,-52.5072938740),
    '2026-09': (678,392,186,179.8926567402),
}

# prior ledger audit before any new work
say('=== AUDIT PRIOR C1~C11 LEDGER ===')
for m, e in EXPECTED_S11.items():
    g = old_s11[old_s11.month.astype(str) == m]
    n = len(g); tp = int((g.result.astype(str) == 'TP20_FULL').sum())
    st = int((g.result.astype(str) == 'STOP').sum()); net = float(g.net_pct.sum())
    ok = n == e[0] and tp == e[1] and st == e[2] and abs(net-e[3]) <= .02
    say(m, 'N', n, 'TP', tp, 'STOP', st, 'NET', round(net,3), 'AUDIT', ok)
    if not ok:
        raise SystemExit('PRIOR C11 LEDGER AUDIT FAIL '+m)

# ------------------------------------------------------------------
# 3) Feature snapshots for C14 + C12/C13
# ------------------------------------------------------------------
packs = sorted(R.glob('P_RESEARCH_PACK_*.zip'), key=lambda p:p.stat().st_mtime, reverse=True)
if not packs:
    raise SystemExit('MISSING P_RESEARCH_PACK_*.zip')
PACK = packs[0]
say('RESEARCH PACK', PACK)

feat = {}

def put_final_features(sid, w, q):
    feat[sid] = dict(
        eth4h=fv(q.get('eth_4h_change_pct')),
        final_pv21=fv(q.get('p_v21_persistence_score')),
        final_volume=fv(q.get('volume_ratio')),
        final_ema9_prev1=fv(q.get('ema9_slope_prev1_pct')),
        final_btc4h=fv(q.get('btc_4h_change_pct')),
        watch_rsi_delta=fv(w.get('rsi_delta')),
        watch_btc4h=fv(w.get('btc_4h_change_pct')),
    )

with zipfile.ZipFile(PACK) as z:
    names = set(z.namelist())
    for m in range(1,9):
        n = f'2026-{m:02d}_CANDIDATES.csv.gz'
        if n not in names:
            raise SystemExit('MISSING '+n)
        d = pd.read_csv(gzip.GzipFile(fileobj=io.BytesIO(z.read(n))), low_memory=False)
        for r in d.to_dict('records'):
            sid = str(r.get('setup_id') or '')
            if not sid:
                continue
            put_final_features(sid, jl(r.get('watch_details_json')), jl(r.get('details_json')))

# September: final-confirm snapshot from DB + exact WATCH RSI from validated replay row.
se_lookup = {str(r['setup_id']):r for _,r in se.iterrows()}
con = sqlite3.connect(R/'bybit_swing_bot.db')
con.row_factory = sqlite3.Row
try:
    dbrows = con.execute(
        "SELECT setup_id,first_seen_at,snapshot_json FROM research_pv25_setups WHERE confirmed_at IS NOT NULL"
    ).fetchall()
finally:
    con.close()

sep_watch_time = {}
for rr in dbrows:
    r = dict(rr); sid = str(r.get('setup_id') or '')
    sr = se_lookup.get(sid)
    if sr is None:
        continue
    q = jl(r.get('snapshot_json'))
    # v22_rsi_delta is the first WATCH observation preserved by final fixed replay.
    w = {'rsi_delta': fv(sr.get('v22_rsi_delta'))}
    put_final_features(sid, w, q)
    t = dt_utc(r.get('first_seen_at'))
    if t is not None:
        sep_watch_time[sid] = t

# ------------------------------------------------------------------
# 4) Recover WATCH BTC4H for September from first_seen_at.
#    The bot defines first_seen_at exactly when P_V25 WATCH is created.
#    Use merged existing BTC 1m market caches; if absent, fetch one public
#    historical BTC series through the existing unified KlineCache.
# ------------------------------------------------------------------
def load_btc_market_1m():
    files = []
    for pat in [
        '.unified_kline_cache/MARKET_BTCUSDT_1m_*.csv.gz',
        '.baseline_fixed_0901_0928_fwd_clean_v1/MARKET_BTCUSDT_1m_*.csv.gz',
        '.pure_forward_v22q_kline_cache/MARKET_BTCUSDT_1m_*.csv.gz',
        '.baseline_fixed_0901_0928_fwd_clean_v1/*.csv.gz',
    ]:
        files += list(R.glob(pat))
    # Also catch equivalent cached market files in nearby research cache dirs.
    files += list(R.glob('.*/*BTCUSDT*1m*.csv.gz'))
    uniq = []
    seen = set()
    for p in files:
        rp = str(p.resolve())
        if rp not in seen:
            seen.add(rp); uniq.append(p)
    parts = []
    for p in uniq:
        try:
            d = pd.read_csv(p, compression='gzip', low_memory=False)
            if 'ts' not in d.columns or 'close' not in d.columns:
                continue
            d = d[['ts','close']].copy()
            d['ts'] = pd.to_datetime(d['ts'], utc=True, errors='coerce')
            d['close'] = pd.to_numeric(d['close'], errors='coerce')
            d = d.dropna()
            if len(d): parts.append(d)
        except Exception:
            pass
    if parts:
        out = pd.concat(parts, ignore_index=True).drop_duplicates('ts').sort_values('ts').reset_index(drop=True)
        say('BTC MARKET CACHE rows', len(out), 'files', len(parts), 'range', out.ts.min(), out.ts.max())
        # Need coverage from 4h before first Sep watch through last Sep watch.
        if sep_watch_time:
            lo = min(sep_watch_time.values()) - pd.Timedelta(hours=5)
            hi = max(sep_watch_time.values())
            if out.ts.min() <= lo and out.ts.max() >= hi:
                return out
            say('BTC MARKET CACHE coverage incomplete; public historical fallback will be used')
    # Public-data fallback via the already installed unified engine. No orders/DB writes.
    if not sep_watch_time:
        return pd.DataFrame(columns=['ts','close'])
    start = min(sep_watch_time.values()).to_pydatetime() - timedelta(hours=5)
    end = max(sep_watch_time.values()).to_pydatetime() + timedelta(minutes=2)
    try:
        U.CACHE_DIR = R/'.c14_c13_watch_btc_cache'
        U.CACHE_DIR.mkdir(exist_ok=True)
        U.KC = U.KlineCache()
        d = U.KC._api('BTCUSDT','1m',start,end)
        d = d[['ts','close']].copy()
        d['ts'] = pd.to_datetime(d['ts'], utc=True, errors='coerce')
        d['close'] = pd.to_numeric(d['close'], errors='coerce')
        d = d.dropna().drop_duplicates('ts').sort_values('ts').reset_index(drop=True)
        say('BTC PUBLIC FALLBACK rows', len(d), 'range', d.ts.min(), d.ts.max())
        return d
    except Exception as e:
        say('BTC PUBLIC FALLBACK ERROR', type(e).__name__, str(e)[:300])
        return pd.DataFrame(columns=['ts','close'])

btc1m = load_btc_market_1m()
if len(btc1m):
    arr = btc1m.ts.astype('int64').to_numpy()
    close = btc1m.close.to_numpy(float)
    def close_at_or_before(t):
        target = int(pd.Timestamp(t).value)
        i = arr.searchsorted(target, side='right') - 1
        return None if i < 0 else float(close[int(i)])
    def watch_btc4h_at(t):
        # same anti-lookahead convention as unified_current.market_at
        cur_t = pd.Timestamp(t).tz_convert(UTC).floor('min') - pd.Timedelta(minutes=1)
        cur = close_at_or_before(cur_t)
        p4 = close_at_or_before(cur_t - pd.Timedelta(hours=4))
        return ((cur/p4)-1)*100 if cur and p4 else None
    for sid,t in sep_watch_time.items():
        if sid in feat:
            feat[sid]['watch_btc4h'] = watch_btc4h_at(t)

# ------------------------------------------------------------------
# 5) Market helpers + path-independent C5-eligible trailing breadth.
#    Build one continuous Jan-Sep feature stream so Sep 1 correctly has
#    Aug 31 history for 12/24h breadth.
# ------------------------------------------------------------------
def market_info(r, period):
    if period == 'JA':
        return dict(
            btc24=fv(r.get('btc24_pct')), eth24=fv(r.get('eth24_pct')),
            btc48=fv(r.get('btc48_pct')), eth48=fv(r.get('eth48_pct')),
            shadow=fv(r.get('market_shadow_sum')),
        )
    return dict(
        btc24=fv(r.get('downsel_btc24')), eth24=fv(r.get('downsel_eth24')),
        btc48=fv(r.get('downsel_btc48')), eth48=fv(r.get('downsel_eth48')),
        shadow=fv(r.get('market_shadow_sum')),
    )

elig = []
for d, period in [(ja,'JA'),(se,'SE')]:
    for _,r in d.iterrows():
        if not pre(r, period) or blocked(r, period):
            continue
        t = pd.Timestamp(r['_dt'])
        if t.tzinfo is None:
            t = t.tz_localize(UTC)
        else:
            t = t.tz_convert(UTC)
        elig.append((t, str(r['setup_id']), str(r['symbol'])))
elig.sort(key=lambda z:(z[0],z[1]))
proxy = {}
for hours in (12,24):
    dq = deque(); cnt = Counter(); key=f'cand_breadth_{hours}h'
    for t,sid,sym in elig:
        dq.append((t,sym)); cnt[sym]+=1
        cutoff=t-pd.Timedelta(hours=hours)
        while dq and dq[0][0] < cutoff:
            _,ss=dq.popleft(); cnt[ss]-=1
            if cnt[ss] <= 0: del cnt[ss]
        proxy.setdefault(sid,{})[key]=len(cnt)
say('TRAILING BREADTH FEATURES', len(proxy))

# ------------------------------------------------------------------
# 6) Guards
# ------------------------------------------------------------------
def c14_A(sid,r,period):
    m=market_info(r,period)
    return m['eth24'] is not None and m['eth48'] is not None and m['eth24']>=1.0 and m['eth48']<=0.0

def c14_B(sid,r,period):
    m=market_info(r,period); q=feat.get(sid,{})
    return m['shadow'] is not None and m['shadow']<=-3.0 and q.get('eth4h') is not None and q['eth4h']<=-0.90

def c14_D(sid,r,period):
    m=market_info(r,period); p=proxy.get(sid,{})
    return p.get('cand_breadth_12h',999)<=10 and m['btc48'] is not None and m['eth48'] is not None and abs(m['btc48']-m['eth48'])<=0.10

def c14_E(sid,r,period):
    m=market_info(r,period); p=proxy.get(sid,{})
    return p.get('cand_breadth_24h',999)<=15 and m['btc24'] is not None and m['eth24'] is not None and abs(m['btc24']-m['eth24'])<=0.27

def c14(sid,r,period):
    return c14_A(sid,r,period) or c14_B(sid,r,period) or c14_D(sid,r,period) or c14_E(sid,r,period)

def c12(sid,r,period):
    q=feat.get(sid,{})
    return bool(
        q.get('final_pv21') is not None and q['final_pv21']<=47.0 and
        q.get('watch_rsi_delta') is not None and q['watch_rsi_delta']>=8.75 and
        q.get('final_volume') is not None and q['final_volume']<=1.0
    )

def c13(sid,r,period):
    if c12(sid,r,period):
        return False
    q=feat.get(sid,{})
    return bool(
        q.get('final_ema9_prev1') is not None and q['final_ema9_prev1']<=0.18 and
        q.get('watch_btc4h') is not None and q.get('final_btc4h') is not None and
        (q['final_btc4h']-q['watch_btc4h'])>=0.24 and
        q['final_btc4h']<=-0.90
    )

# ------------------------------------------------------------------
# 7) Generic exact replay after frozen C1~C11 guards.
# ------------------------------------------------------------------
def run_period(d,period,extras,label):
    sched=U.Scheduler(); trades=[]; blocks=[]
    for _,r in d.iterrows():
        if not pre(r,period) or blocked(r,period):
            continue
        sid=str(r['setup_id']); sym=str(r['symbol'])
        if sid in BASE_C11_IDS:
            continue
        hit=None
        for gname,fn in extras:
            if fn(sid,r,period):
                hit=gname; break
        if hit:
            blocks.append(dict(period=period,setup_id=sid,symbol=sym,month=str(r['entry_time_kst'])[:7],day=str(r['entry_time_kst'])[:10],guard=hit))
            continue
        now=pd.Timestamp(r['_dt']).to_pydatetime() if period=='JA' else r['_dt']
        ok,_=sched.can_open(now,sym)
        if not ok:
            continue
        s=getsim(r,period)
        sched.add(now,sym,s,s.result in ('STOP','LATE_FAILURE_EXIT'))
        trades.append(dict(
            period=period, setup_id=sid, symbol=sym,
            month=str(r['entry_time_kst'])[:7], day=str(r['entry_time_kst'])[:10],
            entry_dt=pd.Timestamp(now), result=str(s.result), net_pct=float(s.net_pct)
        ))
    return pd.DataFrame(trades),pd.DataFrame(blocks)

def run_both(extras,label):
    a,ab=run_period(ja,'JA',extras,label)
    b,bb=run_period(se,'SE',extras,label)
    return pd.concat([a,b],ignore_index=True),pd.concat([ab,bb],ignore_index=True)

def summarize(x,label):
    rows=[]
    for m in [f'2026-{i:02d}' for i in range(1,10)]:
        g=x[x.month.astype(str)==m]; rc=Counter(g.result.astype(str))
        rows.append(dict(
            scenario=label,month=m,N=len(g),TP=rc['TP20_FULL'],STOP=rc['STOP'],
            PP=rc['PROFIT_PROTECT_EXIT'],TIME=rc['TIME_EXIT'],LATE=rc['LATE_FAILURE_EXIT'],
            NET=float(g.net_pct.sum())
        ))
    return pd.DataFrame(rows)

# Rebuild S11 from engine + frozen guard IDs, not merely load old ledger.
say('=== REBUILD C1~C11 BASELINE EXACT ===')
t_s11,b_s11=run_both([], 'S11')
s_s11=summarize(t_s11,'S11')
for r in s_s11.itertuples(index=False):
    e=EXPECTED_S11[r.month]
    ok=r.N==e[0] and r.TP==e[1] and r.STOP==e[2] and abs(r.NET-e[3])<=.03
    say(r.month,'N',r.N,'TP',r.TP,'STOP',r.STOP,'NET',round(r.NET,3),'AUDIT',ok)
    if not ok:
        raise SystemExit('REBUILT C11 BASELINE AUDIT FAIL '+r.month)
say('REBUILT C11 BASELINE AUDIT = PASS')

# ------------------------------------------------------------------
# 8) C12/C13 original direct-definition audit on S11 Jan-Aug path.
# ------------------------------------------------------------------
ja_rows={str(r['setup_id']):r for _,r in ja.iterrows()}
d12=[]; d13=[]
for tr in t_s11[t_s11.period=='JA'].to_dict('records'):
    sid=str(tr['setup_id']); r=ja_rows.get(sid)
    if r is None: continue
    if c12(sid,r,'JA'):
        d12.append(tr)
    elif c13(sid,r,'JA'):
        d13.append(tr)
D12=pd.DataFrame(d12); D13=pd.DataFrame(d13)

def direct_line(name,d,exp_n,exp_tp,exp_stop,exp_net):
    rc=Counter(d.result.astype(str)) if len(d) else Counter(); net=float(d.net_pct.sum()) if len(d) else 0.0
    ok=len(d)==exp_n and rc['TP20_FULL']==exp_tp and rc['STOP']==exp_stop and abs(net-exp_net)<=.03
    say(name,'N',len(d),'TP',rc['TP20_FULL'],'STOP',rc['STOP'],'OTHER',len(d)-rc['TP20_FULL']-rc['STOP'],'NET',round(net,6),'AUDIT',ok)
    if not ok: raise SystemExit(name+' DIRECT AUDIT FAIL')

direct_line('C12 DIRECT JANAUG',D12,10,0,10,-14.062368)
direct_line('C13 DIRECT JANAUG',D13,9,0,8,-17.872884)
# 3월 sub-audit
for name,d,n,net in [('C12 MAR',D12,5,-6.666561),('C13 MAR',D13,4,-8.965163)]:
    g=d[d.month.astype(str)=='2026-03']; ok=len(g)==n and abs(float(g.net_pct.sum())-net)<=.03
    say(name,'N',len(g),'NET',round(float(g.net_pct.sum()),6),'AUDIT',ok)
    if not ok: raise SystemExit(name+' AUDIT FAIL')
say('C12/C13 DIRECT DEFINITIONS = PASS')

# Sep feature completeness diagnostics (do not silently guess C13).
sep_ids=set(se.setup_id.astype(str))
qsep=[feat.get(sid,{}) for sid in sep_ids]
watch_btc_cov=sum(1 for q in qsep if q.get('watch_btc4h') is not None)
watch_rsi_cov=sum(1 for q in qsep if q.get('watch_rsi_delta') is not None)
say('SEP FEATURE COVERAGE','IDs',len(sep_ids),'WATCH_RSI',watch_rsi_cov,'WATCH_BTC4H',watch_btc_cov)
if watch_btc_cov < max(1,int(len(sep_ids)*0.90)):
    raise SystemExit('SEP WATCH BTC4H COVERAGE <90% — C13 exact would be incomplete')

# ------------------------------------------------------------------
# 9) Exact scenarios.
# ------------------------------------------------------------------
SCENARIOS=[
    ('S11',[]),
    ('C14_ABDE',[('C14',c14)]),
    ('C12_ONLY',[('C12',c12)]),
    ('C13_ONLY',[('C13',c13)]),
    ('C12_C13',[('C12',c12),('C13',c13)]),
    ('C14_C12',[('C14',c14),('C12',c12)]),
    ('C14_C13',[('C14',c14),('C13',c13)]),
    ('C14_C12_C13',[('C14',c14),('C12',c12),('C13',c13)]),
]

trades={}; blocks_out={}; sums=[]
for name,extras in SCENARIOS:
    if name=='S11':
        t,b,s=t_s11,b_s11,s_s11
    else:
        say('=== EXACT',name,'===')
        t,b=run_both(extras,name); s=summarize(t,name)
    trades[name]=t; blocks_out[name]=b; sums.append(s)
    sepnet=float(s.loc[s.month=='2026-09','NET'].iloc[0])
    total=float(t.net_pct.sum())
    say(name,'TOTAL',round(total,3),'SEP',round(sepnet,3),'N',len(t),'TP',int((t.result=='TP20_FULL').sum()),'STOP',int((t.result=='STOP').sum()))

SUM=pd.concat(sums,ignore_index=True)
SUM.to_csv(OUTDIR/'MONTHLY_ALL_SCENARIOS.csv',index=False)

# Hard audit C14 Jan-Aug against prior exact run.
EXPECTED_C14_JA={
'2026-01':(660,329,223,-1.130199),
'2026-02':(388,198,124,6.948358),
'2026-03':(496,243,164,-28.104803),
'2026-04':(654,360,182,85.206306),
'2026-05':(660,360,198,105.891429),
'2026-06':(410,197,144,-55.217959),
'2026-07':(515,255,159,2.794043),
'2026-08':(640,326,202,14.661556),
}
c14s=SUM[SUM.scenario=='C14_ABDE']
say('=== C14 JAN-AUG PRIOR-RESULT AUDIT ===')
for r in c14s[c14s.month<='2026-08'].itertuples(index=False):
    e=EXPECTED_C14_JA[r.month]
    ok=r.N==e[0] and r.TP==e[1] and r.STOP==e[2] and abs(r.NET-e[3])<=.04
    say(r.month,'N',r.N,'TP',r.TP,'STOP',r.STOP,'NET',round(r.NET,3),'AUDIT',ok)
    if not ok: raise SystemExit('C14 JANAUG AUDIT FAIL '+r.month)
say('C14 JAN-AUG AUDIT = PASS')

# Scenario totals + deltas vs S11 / vs C14.
base_total=float(trades['S11'].net_pct.sum())
c14_total=float(trades['C14_ABDE'].net_pct.sum())
base_sep=float(s_s11.loc[s_s11.month=='2026-09','NET'].iloc[0])
c14_sep=float(c14s.loc[c14s.month=='2026-09','NET'].iloc[0])
rows=[]
for name,t in trades.items():
    s=SUM[SUM.scenario==name]
    sepnet=float(s.loc[s.month=='2026-09','NET'].iloc[0])
    total=float(t.net_pct.sum())
    rows.append(dict(
        scenario=name,total_net=total,delta_vs_S11=total-base_total,
        delta_vs_C14=(total-c14_total if name!='S11' else np.nan),
        sep_net=sepnet,delta_sep_vs_S11=sepnet-base_sep,
        delta_sep_vs_C14=(sepnet-c14_sep if name!='S11' else np.nan),
        N=len(t),TP=int((t.result=='TP20_FULL').sum()),STOP=int((t.result=='STOP').sum())
    ))
TOTALS=pd.DataFrame(rows).sort_values('total_net',ascending=False)
TOTALS.to_csv(OUTDIR/'SCENARIO_TOTALS.csv',index=False)

# Monthly compact compare table.
wide=[]
for m in [f'2026-{i:02d}' for i in range(1,10)]:
    row={'month':m}
    for name in ['S11','C14_ABDE','C14_C12','C14_C13','C14_C12_C13']:
        g=SUM[(SUM.scenario==name)&(SUM.month==m)].iloc[0]
        row[f'{name}_NET']=float(g.NET);row[f'{name}_TP']=int(g.TP);row[f'{name}_STOP']=int(g.STOP);row[f'{name}_N']=int(g.N)
    wide.append(row)
W=pd.DataFrame(wide)
W.to_csv(OUTDIR/'MONTHLY_KEY_COMPARE.csv',index=False)

# Save direct audit lists and all block lists; key full trade ledgers.
if len(D12): D12.to_csv(OUTDIR/'DIRECT_C12_JANAUG.csv',index=False)
if len(D13): D13.to_csv(OUTDIR/'DIRECT_C13_JANAUG.csv',index=False)
for name,b in blocks_out.items():
    if len(b): b.to_csv(OUTDIR/f'BLOCKS_{name}.csv',index=False)
for name in ['S11','C14_ABDE','C14_C12_C13']:
    trades[name].to_csv(OUTDIR/f'TRADES_{name}.csv',index=False)

# Final concise console decision data.
say('')
say('='*110)
say('FINAL KEY RESULTS')
say('='*110)
for name in ['S11','C14_ABDE','C14_C12','C14_C13','C14_C12_C13']:
    r=TOTALS[TOTALS.scenario==name].iloc[0]
    say(name,'TOTAL',round(r.total_net,3),'D_S11',round(r.delta_vs_S11,3),'SEP',round(r.sep_net,3),'D_SEP_S11',round(r.delta_sep_vs_S11,3),'N',int(r.N),'TP',int(r.TP),'STOP',int(r.STOP))

# March-specific C12/C13 effect after C14.
for name in ['C14_ABDE','C14_C12','C14_C13','C14_C12_C13']:
    g=SUM[(SUM.scenario==name)&(SUM.month=='2026-03')].iloc[0]
    say('MARCH',name,'NET',round(float(g.NET),3),'N',int(g.N),'TP',int(g.TP),'STOP',int(g.STOP))

(OUTDIR/'RUN_LOG.txt').write_text('\n'.join(LOG)+'\n',encoding='utf-8')
zip_path=R/f'C14_SEP_C12_C13_EXACT_{STAMP}_RESULTS.zip'
with zipfile.ZipFile(zip_path,'w',zipfile.ZIP_DEFLATED) as z:
    for p in sorted(OUTDIR.iterdir()):
        if p.is_file(): z.write(p,p.name)
say('RESULT ZIP',zip_path)
say('=== DONE ===')
