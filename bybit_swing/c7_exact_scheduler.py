from pathlib import Path
from collections import Counter
from types import SimpleNamespace
import runpy, sqlite3, json, math, zipfile
import pandas as pd

R = Path('/root/hyejin-trader/bybit_swing')
BASE_SCRIPT = R / 'breadth_persistence_diag.py'
if not BASE_SCRIPT.exists():
    BASE_SCRIPT = Path('/tmp/breadth_persistence_diag.py')
if not BASE_SCRIPT.exists():
    raise SystemExit('MISSING breadth_persistence_diag.py')

print('=== C7 EXACT SCHEDULER — LOAD C5 ENGINE ===', flush=True)
ns = runpy.run_path(str(BASE_SCRIPT))
U = ns['U']; ja = ns['ja']; se = ns['se']; pre = ns['pre']; blocked = ns['blocked']; getsim = ns['getsim']

# Frozen C6 direct targets from the completed exact C6 audit.
c6_ids = {
'P25SET-AKEUSDT-1964292',
'P25SET-AGLDUSDT-1968650',
'P25SET-SIGNUSDT-1969756',
'P25SET-KITEUSDT-1969768',
'P25SET-XANUSDT-1971016',
'P25SET-TRIAUSDT-1971329',
'P25SET-STOUSDT-1971719',
'P25SET-ZBTUSDT-1974617',
'P25SET-BSBUSDT-1979575',
'P25SET-EPICUSDT-1981325',
'P25SET-LDOUSDT-1981649',
'P25SET-BILLUSDT-1982088',
'P25SET-FHEUSDT-1982101',
'P25SET-JCTUSDT-1982161',
'P25SET-REUSDT-1983051',
'P25SET-BILLUSDT-1983199',
'P25SET-REUSDT-1983199',
'P25SET-CAPUSDT-1983218',
'P25SET-SAGAUSDT-1988348',
}
if len(c6_ids) != 19:
    raise SystemExit(f'C6 ID COUNT FAIL: {len(c6_ids)} != 19')

# ---------- C7 feature loader ----------
def fv(v, d=None):
    try:
        x = float(v)
        return d if math.isnan(x) or math.isinf(x) else x
    except Exception:
        return d

def jl(v):
    if isinstance(v, dict): return dict(v)
    if not v: return {}
    try:
        x = json.loads(str(v)); return x if isinstance(x, dict) else {}
    except Exception:
        return {}

feat = {}
# Jan-Aug: exact confirmed candidate snapshots already saved in monthly candidate files.
for f in sorted(R.glob('2026-??_CANDIDATES.csv.gz')):
    try:
        d = pd.read_csv(f, low_memory=False)
    except Exception:
        continue
    for r in d.to_dict('records'):
        sid = str(r.get('setup_id') or '')
        if not sid: continue
        q = jl(r.get('details_json'))
        ep = fv(r.get('entry_price'))
        trig = fv(r.get('trigger_price'))
        if trig is None: trig = fv(q.get('price'))
        feat[sid] = dict(
            rsi_delta=fv(q.get('rsi_delta')),
            live_gain=fv(q.get('live_candle_gain_pct')),
            gap=fv(q.get('ema9_ema20_gap_pct')),
            slope=fv(q.get('ema9_slope_prev1_pct')),
            entry=ep,
            trigger=trig,
            source='MONTHLY_CANDIDATE'
        )

# September: exact confirmed snapshots from DB.
db = R / 'bybit_swing_bot.db'
con = sqlite3.connect(db); con.row_factory = sqlite3.Row
try:
    rows = con.execute('SELECT * FROM research_pv25_setups WHERE confirmed_at IS NOT NULL').fetchall()
finally:
    con.close()
for rr in rows:
    r = dict(rr); sid = str(r.get('setup_id') or '')
    if not sid or sid not in set(se.setup_id.astype(str)): continue
    q = jl(r.get('snapshot_json'))
    ep = fv(r.get('confirmed_price'))
    trig = None
    for k in ('trigger_price','watch_price','reference_price','price'):
        if k in r:
            trig = fv(r.get(k))
            if trig is not None: break
    if trig is None: trig = fv(q.get('price'))
    feat[sid] = dict(
        rsi_delta=fv(q.get('rsi_delta')),
        live_gain=fv(q.get('live_candle_gain_pct')),
        gap=fv(q.get('ema9_ema20_gap_pct')),
        slope=fv(q.get('ema9_slope_prev1_pct')),
        entry=ep,
        trigger=trig,
        source='SEP_DB'
    )

def c7_info(sid):
    q = feat.get(str(sid)) or {}
    vals = [q.get('rsi_delta'), q.get('live_gain'), q.get('gap'), q.get('slope'), q.get('entry'), q.get('trigger')]
    complete = all(v is not None for v in vals) and q.get('trigger',0) > 0
    jump = ((q['entry']/q['trigger'] - 1.0) * 100.0) if complete else None
    hit = bool(
        complete and
        q['rsi_delta'] <= 1.5 and
        q['live_gain'] >= 0.5 and
        q['gap'] <= 1.0 and
        q['slope'] >= 0.3 and
        jump >= 0.5
    )
    z = dict(q); z['complete'] = complete; z['entry_vs_trigger_pct'] = jump; z['hit'] = hit
    return z

# ---------- exact scheduler replay ----------
def run_guard(d, period, use_c7):
    sched = U.Scheduler()
    trades = []
    direct = []
    c7_missing_eligible = []
    for _, r in d.iterrows():
        if not pre(r, period) or blocked(r, period):
            continue
        sid = str(r['setup_id']); sym = str(r['symbol'])
        now = pd.Timestamp(r['_dt']).to_pydatetime() if period == 'JA' else r['_dt']
        month = str(r['entry_time_kst'])[:7]
        day = str(r['entry_time_kst'])[:10]
        if sid in c6_ids:
            direct.append(dict(period=period, setup_id=sid, symbol=sym, month=month, day=day, guard='C6'))
            continue
        ci = c7_info(sid)
        if use_c7 and not ci['complete']:
            c7_missing_eligible.append(sid)
        if use_c7 and ci['hit']:
            direct.append(dict(period=period, setup_id=sid, symbol=sym, month=month, day=day, guard='C7', **{k:ci.get(k) for k in ['rsi_delta','live_gain','gap','slope','entry_vs_trigger_pct','source']}))
            continue
        ok, why = sched.can_open(now, sym)
        if not ok:
            continue
        s = getsim(r, period)
        sched.add(now, sym, s, s.result in ('STOP','LATE_FAILURE_EXIT'))
        trades.append(dict(
            period=period, setup_id=sid, symbol=sym, month=month, day=day,
            entry_dt=pd.Timestamp(now), result=str(s.result), net_pct=float(s.net_pct)
        ))
    return pd.DataFrame(trades), pd.DataFrame(direct), sorted(set(c7_missing_eligible))

def summarize(x):
    rows=[]
    for m in [f'2026-{i:02d}' for i in range(1,10)]:
        g=x[x.month==m]
        rc=Counter(g.result.astype(str))
        rows.append(dict(month=m, N=len(g), TP=rc['TP20_FULL'], STOP=rc['STOP'], PP=rc['PROFIT_PROTECT_EXIT'], LATE=rc['LATE_FAILURE_EXIT'], NET=float(g.net_pct.sum())))
    return pd.DataFrame(rows)

print('\n=== EXACT C6 BASELINE REPLAY ===', flush=True)
c6_ja, c6d_ja, miss0a = run_guard(ja, 'JA', False)
c6_se, c6d_se, miss0b = run_guard(se, 'SE', False)
c6 = pd.concat([c6_ja,c6_se], ignore_index=True)
sm6 = summarize(c6)
expected = {
'2026-01':(779,383,268,-35.468),'2026-02':(478,239,155,-38.122),'2026-03':(613,290,211,-83.803),
'2026-04':(748,394,234,15.146),'2026-05':(775,408,239,60.780),'2026-06':(565,265,212,-115.494),
'2026-07':(629,282,219,-123.735),'2026-08':(757,367,253,-58.742),'2026-09':(703,410,192,196.869)}
fail=[]
for r in sm6.itertuples(index=False):
    e=expected[r.month]
    ok=(r.N==e[0] and r.TP==e[1] and r.STOP==e[2] and abs(r.NET-e[3])<=0.02)
    print(r.month,'N',r.N,'TP',r.TP,'STOP',r.STOP,'NET',round(r.NET,3),'EXPECTED',e,'OK',ok)
    if not ok: fail.append(r.month)
if fail:
    raise SystemExit('C6 BASELINE AUDIT FAIL: '+','.join(fail))
print('C6 BASELINE AUDIT = PASS', flush=True)

print('\n=== C7 DIRECT AUDIT ON C6 PATH ===', flush=True)
# Direct C7 hits among the actually accepted C6 path before rescheduling.
c6_ids_path=set(c6.setup_id.astype(str))
direct_rows=[]
for _,r in pd.concat([ja,se],ignore_index=True).iterrows():
    sid=str(r['setup_id'])
    if sid not in c6_ids_path: continue
    ci=c7_info(sid)
    if ci['hit']:
        tr=c6[c6.setup_id.astype(str)==sid].iloc[0]
        direct_rows.append(dict(setup_id=sid,symbol=str(r['symbol']),month=tr['month'],result=tr['result'],net_pct=tr['net_pct'],**{k:ci.get(k) for k in ['rsi_delta','live_gain','gap','slope','entry_vs_trigger_pct','source']}))
direct_df=pd.DataFrame(direct_rows)
if len(direct_df):
    for m,g in direct_df.groupby('month'):
        rc=Counter(g.result.astype(str)); print(m,'N',len(g),'TP',rc['TP20_FULL'],'STOP',rc['STOP'],'PP',rc['PROFIT_PROTECT_EXIT'],'NET',round(g.net_pct.sum(),3))
    j=direct_df[direct_df.month=='2026-07']; rc=Counter(j.result.astype(str))
    print('JULY C7 DIRECT:',len(j),'TP',rc['TP20_FULL'],'STOP',rc['STOP'],'PP',rc['PROFIT_PROTECT_EXIT'],'NET',round(j.net_pct.sum(),3))
else:
    print('NO C7 DIRECT HITS — FEATURE LOAD ERROR LIKELY')

print('\n=== RUN C6 + C7 WITH REAL REPLACEMENT ===', flush=True)
c7_ja, d7ja, missja = run_guard(ja, 'JA', True)
c7_se, d7se, missse = run_guard(se, 'SE', True)
c7 = pd.concat([c7_ja,c7_se], ignore_index=True)
sm7 = summarize(c7)

cmp=sm6.merge(sm7,on='month',suffixes=('_C6','_C7'))
cmp['DELTA']=cmp.NET_C7-cmp.NET_C6
print('\nMONTHLY C6 -> C6+C7 EXACT')
for r in cmp.itertuples(index=False):
    print(f"{r.month} | C6 N {r.N_C6} TP {r.TP_C6} STOP {r.STOP_C6} NET {r.NET_C6:.3f} | C7 N {r.N_C7} TP {r.TP_C7} STOP {r.STOP_C7} NET {r.NET_C7:.3f} | DELTA {r.DELTA:+.3f}")
print('TOTAL | C6',round(c6.net_pct.sum(),3),'| C6+C7',round(c7.net_pct.sum(),3),'| DELTA',round(c7.net_pct.sum()-c6.net_pct.sum(),3))

# Path changes: removed and added trades caused by C7 + downstream scheduler changes.
key6=set(c6.setup_id.astype(str)); key7=set(c7.setup_id.astype(str))
removed=c6[c6.setup_id.astype(str).isin(key6-key7)].copy(); removed['change']='REMOVED'
added=c7[c7.setup_id.astype(str).isin(key7-key6)].copy(); added['change']='ADDED'
changes=pd.concat([removed,added],ignore_index=True)
print('\nPATH CHANGES')
for m in [f'2026-{i:02d}' for i in range(1,10)]:
    rr=removed[removed.month==m]; aa=added[added.month==m]
    rrc=Counter(rr.result.astype(str)); aac=Counter(aa.result.astype(str))
    print(m,'REM',len(rr),'TP',rrc['TP20_FULL'],'STOP',rrc['STOP'],'NET',round(rr.net_pct.sum(),3),'| ADD',len(aa),'TP',aac['TP20_FULL'],'STOP',aac['STOP'],'NET',round(aa.net_pct.sum(),3))

print('\nC7 FEATURE MISSING AMONG C5-ELIGIBLE: JA',len(missja),'SEP',len(missse))
if missja[:10]: print('JA SAMPLE',missja[:10])
if missse[:10]: print('SEP SAMPLE',missse[:10])

# Save compact exact result pack.
stamp='20261003'
outdir=R/f'C7_EXACT_{stamp}'
outdir.mkdir(exist_ok=True)
cmp.to_csv(outdir/'MONTHLY.csv',index=False)
c6.to_csv(outdir/'C6_TRADES.csv',index=False)
c7.to_csv(outdir/'C6_C7_TRADES.csv',index=False)
changes.to_csv(outdir/'PATH_CHANGES.csv',index=False)
direct_df.to_csv(outdir/'C7_DIRECT_ON_C6_PATH.csv',index=False)
pd.concat([d7ja,d7se],ignore_index=True).to_csv(outdir/'GUARD_BLOCKS.csv',index=False)
summary = []
summary.append('C7 EXACT SCHEDULER 2026-10-03')
summary.append('C6 baseline audit PASS')
for r in cmp.itertuples(index=False):
    summary.append(f"{r.month} C6_N={r.N_C6} C6_TP={r.TP_C6} C6_STOP={r.STOP_C6} C6_NET={r.NET_C6:.6f} C7_N={r.N_C7} C7_TP={r.TP_C7} C7_STOP={r.STOP_C7} C7_NET={r.NET_C7:.6f} DELTA={r.DELTA:+.6f}")
summary.append(f"TOTAL_C6={c6.net_pct.sum():.6f} TOTAL_C7={c7.net_pct.sum():.6f} DELTA={c7.net_pct.sum()-c6.net_pct.sum():+.6f}")
summary.append(f"C7_FEATURE_MISSING_JA={len(missja)} SEP={len(missse)}")
(outdir/'SUMMARY.txt').write_text('\n'.join(summary)+'\n',encoding='utf-8')
zip_path=R/f'C7_EXACT_SCHEDULER_{stamp}_RESULTS.zip'
with zipfile.ZipFile(zip_path,'w',zipfile.ZIP_DEFLATED) as z:
    for f in sorted(outdir.iterdir()): z.write(f,f.name)
print('\nRESULT ZIP:',zip_path)
print('DONE', flush=True)
