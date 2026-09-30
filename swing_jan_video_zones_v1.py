#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""January research only: video-inspired 4H boxes, role changes and retests.
No network, API keys, live orders, or bot changes. Not a reproduction of the
leader's discretionary trading. Uses existing January 15-minute cache.
Run from the repository root. Requires the already-delivered
swing_jan_sr_reset_v1_2.py (time-unit-fixed accounting engine).
"""
from __future__ import annotations
import argparse
from dataclasses import dataclass, asdict
import fcntl
import hashlib
import html
import json
import math
import os
from pathlib import Path
import sys
import threading
import time
import traceback
import zipfile
import numpy as np
import pandas as pd

# A normal import registers the module in sys.modules; no dynamic-import
# dataclass failure. Importing the research engine does not run its main().
try:
    import swing_jan_sr_reset_v1_2 as E
except ImportError as exc:
    raise SystemExit('같은 GitHub 최상위에 기존 swing_jan_sr_reset_v1_2.py가 필요합니다: '+str(exc))

VERSION = 'JAN_VIDEO_ZONES_V1_20260930'
NEW = 'VIDEO4H_BOX_RETEST'
OLD = 'D_4H72_1H36'
MODES = ('NEXT_OPEN', 'SIGNAL_CLOSE_DIAGNOSTIC')
STAMP_COLS = ('signal_bar_open','signal_time','sr_known_at','box_born_at',
              'support_role_at','target_born_at','target_role_at','source_start','source_end')
EXTRA_COLS = ['support_id','target_id','support_lower','support_upper',
 'support_body_lower','support_body_upper','target_lower','target_upper',
 'target_body_lower','target_body_upper','box_born_at','support_role_at',
 'target_born_at','target_role_at','support_origin','support_flips',
 'source_start','source_end','source_bars','box_width_pct','gap_to_upper_pct',
 'planned_reward_pct','planned_loss_pct','target_fee_only_net_usdt']
COLUMNS = list(E.SIG_COLS) + EXTRA_COLS

@dataclass(frozen=True)
class BoxAssumptions:
    # ALL numbers here are research assumptions, not numbers specified by the videos.
    timeframe_hours: int = 4
    consecutive_bodies: int = 3
    atr_period: int = 14
    maximum_box_atr: float = 2.0
    max_age_days: int = 30

CFG = BoxAssumptions()
RULES = '''영상 기반 1월 지지·저항 V1 — 규칙과 한계

1. 영상에서 관찰한 것 / 이번에 정한 가정
5번: 캔들 밀집 가격을 박스로 표시하고 몸통/꼬리 부근의 경계를 확인.
8번: 큰 시간봉에서 작은 시간봉으로 보기, 돌파 후 지지/저항 역할 전환 및 리테스트.
영상은 완전한 수치 알고리즘/매매 규칙을 제공하지 않는다. 이 파일은 영상 원리를
참고한 별도 연구 모델이며, 리더 원본/검증된 수익 전략이라는 뜻이 아니다.

이번 첫 시험은 4시간봉 예시에 한정한다. 월봉/주봉/일봉 전체 작도를 재현하지 않는다.
1H는 직전 완성봉 거래대금 우선순위와 형성과정 확인자료용이다. 1H에서 박스를 재조정하지 않는다.
기존 1월 캐시(대략 12월말~2월초)만 사용한다. 과거 장기 주요 매물대가 빠질 수 있다.

2. 박스 형성: 다음은 모두 검증용 가정
완성 4H봉 3개가 연속이고, 세 몸통의 공통 겹침이 있을 때만 후보.
몸통 하단=min(open,close), 몸통 상단=max(open,close).
세 몸통의 공통교집합=[max(body_low), min(body_high)]. 교집합이 없으면 후보 아님.
코어=[세 몸통 하단 최솟값, 세 몸통 상단 최댓값].
외곽=[세 봉 저가 최솟값, 세 봉 고가 최댓값]. 중앙값 한 줄로 바꾸지 않는다.
3개 봉 외곽 폭은 그 시점까지 완성된 4H 14개 TR 평균(단순 ATR)의 2배 이하.
0폭/비정상 OHLC는 제외. 연속된 같은 밀집구간에서 매봉 새 박스를 만들지 않는다.
처음 인정된 경계는 고정; 과거 경계 수정 없음. 최대 보관 30일, 종료 후 과거 레벨을 재사용하지 않음.
4H 데이터 공백이 있으면 기존 상태를 무효화하고 연속 데이터로 다시 형성.

3. 역할 전환(미래 데이터 사용 금지)
형성 직후는 NEUTRAL. 이후 완성 4H 종가가 외곽 상단보다 높아지면 SUPPORT,
외곽 하단보다 낮아지면 RESISTANCE. 기존 RESISTANCE가 위로 돌파되면 SUPPORT로 전환.
한 봉 꼬리만 넘었다고 역할을 뒤집지 않는다. 박스 내부 종가에서는 마지막 역할을 유지.
형성시각/역할확정시각/지지원점/전환횟수 전부 기록.
15분 신호봉 '시작' 이전에 확정된 박스/역할만 사용. 신호봉으로 박스를 소급 생성하지 않는다.

4. 진입: 리더의 확정 규칙이 아닌 공통 검증용 실행 가정
롱만, 물타기 없음. 유효 SUPPORT 박스 위에 완성 15m봉 하나가 완전히 위치한 이력이 필요.
그 후 가격범위가 박스와 겹치고, 양봉/직전종가보다 높은 종가/박스 상단 위 종가로
회복하면 후보. 같은 접촉 에피소드에서는 신호 1개만 생성.
다시 완성봉 저가가 박스 상단 위로 떨어져 나와야 새 접촉을 허용.
조건 미충족을 '반등한 것'으로 추정하지 않는다. 이 15m 마감확인은 영상의 의무조건이 아니다.
동시에 여러 지지가 닿으면 상단이 현재가에 가장 가까운(가장 높은) 박스를 사용.
목표는 현재가 위에 있는 유효 RESISTANCE 중 가장 가까운 외곽 하단.
저항 박스 안에서 신규진입/지지 상단보다 아래 목표/목표 위 체결은 허용하지 않는다.
가까운 저항을 수익이 작다는 이유로 건너뛰지 않는다. 저항이 없으면 거래 없음.
NEXT_OPEN은 신호 종료시각의 다음 15m 시가. 그 시가가 지지박스 상단 이하이면 진입불가.
SIGNAL_CLOSE_DIAGNOSTIC은 같은 시각에 신호종가로 체결 가능했다고 가정한 비교용.
어느 경우도 라이브 체결평단이 아니며, 신호확인 전 시간으로 진입을 소급하지 않는다.

5. 비교 / 고정 자금·청산
이전 D(4H72+1H36)의 저장 신호를 같은 엔진으로 다시 실행하는 대조군 1개.
새 VIDEO4H_BOX_RETEST 1개. 각 NEXT_OPEN/신호종가 비교, 총 4개 시나리오.
이전 A/B/C/D의 최적을 선택하는 재탐색 아님. 기존 D가 같은 4H 계열이라 대조용.
200 USDT, 증거금 27 USDT, 5배, 동시 4종목, 거래대금순, 종료 뒤 동일종목 2h대기.
손절=지지 박스 외곽 하단*(1-0.006); 0.6% 여유폭 자체는 기존 공통 검증가정.
목표=다음 저항 외곽 하단; 최대보유 24h. 청산선은 진입시 고정.
같은봉 STOP/TP 동시 접촉은 STOP 우선. 갭 손절은 더 불리한 시가.
진입봉 고저가부터 관리. 자금 부족 차단, 임의 추가입금/물타기/복리 증액 없음.
BTC/ETH/Breadth/종목추세/Q2/RR 구간/최소목표수익 필터 모두 없음.
단, 박스 인식 조건과 리테스트/가격순서 조건은 존재한다. '아무 규칙도 없음'은 아님.

6. 비용과 검사
편도 0.055%는 가정치. 청산수수료는 실제 가상수량*청산가 기준.
펀딩, 스프레드, 주문지연, 틱/최소수량, 거래소 강제청산 엔진은 미반영.
양방향 각0.05% 불리한 가격의 비용 민감도는 같은 거래에 대한 참고치, 슬롯 재실행 아님.
NEXT_OPEN/신호종가 차이는 별도; 수익 비교는 NEXT_OPEN이 우선.
DD는 진입순서가 아닌 청산/현금흐름 및 미실현 포함 15m 종가평가; 봉중 최악낙폭 아님.
증거금 초과 손실/공백 포지션은 실전 재현 불충분 경고. R1도 수수료 후 손실일 수 있어 기록.

7. 증거·선정·범위
박스 생성에 쓰인 봉/형성시간/역할변화/선택한 지지와 저항 ID 기록.
초기 날짜의 각기 다른 종목 첫 신호 최대12개를 성과를 보지 않고 선택하여
진입 전 4H/1H/15m 차트(HTML)와 원자료 동봉. 차트는 수익 결과를 가림.
자동 작도가 리더 예시와 같은지 확인하는 자료이며, 자동으로 '같음' 판정하지 않는다.
종목집합은 기존 캐시 그대로. 당시 전체 상장/상폐/유동성 풀 복원이 아니며 BTC 거래는 제외.
1~9월 성과를 이미 봤으므로 1월 새 버전은 재탐색. 완전히 독립적인 OOS 아님.
2~9월 데이터로 숫자를 고르거나 이 스크립트에서 2~9월 성과를 계산하지 않는다.
이 파일의 self-test는 소프트웨어 검사일 뿐 실제 1월 성과 검증이 아니다.
'''

_STATE={'stage':'START','detail':''}
def log(s):
    now=pd.Timestamp.now(tz='Asia/Seoul').strftime('%Y-%m-%d %H:%M:%S KST')
    print(f'[{now}] {s}',flush=True)
def beat(done):
    while not done.wait(60): log(f"HEARTBEAT {_STATE['stage']} | {_STATE['detail']}")
def stage(n,text):
    _STATE.update(stage=n,detail=text); log(n+' '+text)
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def jwrite(p,x): E.write_json(Path(p),x)
def cwrite(p,x): E.write_csv(Path(p),x)
def empty_signal(): return pd.DataFrame(columns=COLUMNS)
def read_signal(p):
    x=pd.read_csv(p,compression='infer')
    for c in STAMP_COLS:
        if c in x: x[c]=pd.to_datetime(x[c],utc=True).dt.as_unit('ns')
    return x


def advance_role(z, close, when):
    """Returns a NEW record; previous snapshots may never be mutated."""
    side='SUPPORT' if close>z['upper'] else ('RESISTANCE' if close<z['lower'] else None)
    if side is None or side==z['role']: return z,None
    nz=dict(z)
    nz.update(role=side,role_at=when,origin='RESISTANCE_FLIP' if side=='SUPPORT' and z['role']=='RESISTANCE' else ('BOX_BREAKOUT' if side=='SUPPORT' else 'BOX_BREAKDOWN'))
    if z['role']!='NEUTRAL': nz['flips']=z['flips']+1
    return nz,dict(zone_id=z['zone_id'],time=when,event='ROLE',old_role=z['role'],new_role=side,close=float(close))


def build_boxes(q: pd.DataFrame, symbol: str, cfg=CFG):
    """q index = closed-bar availability, UTC. No pivot labels from the future."""
    snapshots={}; births=[]; events=[]; sources=[]; active=[]; episode=None
    prev=None; tr_history=[]; period=pd.Timedelta(hours=cfg.timeframe_hours)
    vals=q[E.OHLCV].to_numpy(float)
    for i, when in enumerate(q.index):
        if when>=E.END: break
        a=vals[i]
        if not np.isfinite(a).all():
            events.extend(dict(zone_id=z['zone_id'],time=when,event='DATA_GAP_INVALIDATED') for z in active)
            active=[]; episode=None; prev=None; tr_history=[]; snapshots[when.value]=(); continue
        op,hi,lo,cl=a[:4]
        tr=max(hi-lo,abs(hi-prev),abs(lo-prev)) if prev is not None else hi-lo
        prev=cl; tr_history.append(float(tr))
        tr_history=tr_history[-cfg.atr_period:]
        nextactive=[]
        for z in active:
            if when-z['born_at']>pd.Timedelta(days=cfg.max_age_days):
                events.append(dict(zone_id=z['zone_id'],time=when,event='EXPIRED')); continue
            nz,event=advance_role(z,cl,when)
            if event: events.append(event)
            nextactive.append(nz)
        active=nextactive
        n=cfg.consecutive_bodies
        candidate=None
        if i>=n-1 and len(tr_history)>=cfg.atr_period:
            window=vals[i-n+1:i+1]
            timestamps=q.index[i-n+1:i+1]
            contiguous=all(timestamps[j]-timestamps[j-1]==period for j in range(1,n))
            if contiguous and np.isfinite(window).all():
                bl=np.minimum(window[:,0],window[:,3]); bh=np.maximum(window[:,0],window[:,3])
                lower=float(window[:,2].min()); upper=float(window[:,1].max())
                shared_lo=float(bl.max()); shared_hi=float(bh.min())
                atr=float(np.mean(tr_history))
                if lower>0 and upper>lower and shared_lo<=shared_hi and upper-lower<=cfg.maximum_box_atr*atr and atr>0:
                    candidate=dict(lower=lower,upper=upper,body_lower=float(bl.min()),body_upper=float(bh.max()),shared_lo=shared_lo,shared_hi=shared_hi,atr_at_birth=atr)
        if candidate is None:
            episode=None
        else:
            # A continuously overlapping episode keeps its FIRST fixed box.
            same=(episode is not None and when-episode['last_at']==period and
                  max(candidate['shared_lo'],episode['shared_lo'])<=min(candidate['shared_hi'],episode['shared_hi']))
            if same:
                episode={**episode,'last_at':when}
            else:
                zid=f'{symbol}_4H_{when.value}'
                z=dict(zone_id=zid,symbol=symbol,tf='4H',born_at=when,
                       role='NEUTRAL',role_at=when,origin='UNCONFIRMED',flips=0,
                       source_start=q.index[i-n+1]-period,source_end=when,source_bars=n,**candidate)
                active.append(z); births.append(dict(z))
                events.append(dict(zone_id=zid,time=when,event='FORMED',old_role='',new_role='NEUTRAL',close=cl))
                for k in range(i-n+1,i+1):
                    sources.append(dict(zone_id=zid,symbol=symbol,bar_open=q.index[k]-period,known_at=q.index[k],**dict(zip(E.OHLCV,map(float,vals[k])))))
                episode=dict(last_at=when,shared_lo=candidate['shared_lo'],shared_hi=candidate['shared_hi'])
        snapshots[when.value]=tuple(dict(z) for z in active)
    return snapshots,pd.DataFrame(births),pd.DataFrame(events),pd.DataFrame(sources)


def closest_resistance(zones, price):
    """Do not skip a nearby overlapping obstruction to pick a nicer far target."""
    r=sorted((z for z in zones if z['role']=='RESISTANCE' and z['upper']>price),key=lambda z:(z['lower'],z['born_at'],z['zone_id']))
    if not r: return None,'NO_RESISTANCE'
    if r[0]['lower']<=price: return None,'INSIDE_RESISTANCE'
    return r[0],None


def make_video_signals(symbol,d,snapshots,h1):
    result=[]; retest={}; reasons={}; vals=d[E.OHLCV].to_numpy(float); times=d.index
    h1rank=h1['turnover'].to_dict()
    def count(k): reasons[k]=reasons.get(k,0)+1
    for i in range(1,len(d)):
        t=times[i]; dec=t+E.STEP
        if dec>=E.END: break
        if t-times[i-1]!=E.STEP:
            retest.clear(); count('DATA_GAP_RESET'); continue
        zones=snapshots.get(t.floor('4h').value,())
        candidates=[]
        op,hi,lo,cl=vals[i,:4]; prevlow=vals[i-1,2]; prevcl=vals[i-1,3]
        for z in zones:
            key=(z['zone_id'],z['role_at'].value)
            if z['role']!='SUPPORT': continue
            st=retest.setdefault(key,dict(armed=False,touch=False))
            if prevlow>z['upper']:
                st.update(armed=True,touch=False)
            overlap=lo<=z['upper'] and hi>=z['lower']
            if overlap and st['armed']:
                st.update(armed=False,touch=True); count('TOUCH_EPISODE')
            if cl<z['lower']:
                st.update(armed=False,touch=False); count('CLOSE_BELOW_BOX'); continue
            confirm=st['touch'] and overlap and cl>z['upper'] and cl>op and cl>prevcl
            if confirm:
                st['touch']=False; count('CONFIRMED_RETEST')
                candidates.append(z)
            elif lo>z['upper']:
                st.update(armed=True,touch=False)
        # Drop expired/changed tokens, while preserving current episode state.
        valid={(z['zone_id'],z['role_at'].value) for z in zones if z['role']=='SUPPORT'}
        retest={k:v for k,v in retest.items() if k in valid}
        if dec<E.START or not candidates: continue
        count('IN_MONTH_RETEST')
        z=max(candidates,key=lambda a:(a['upper'],a['born_at'],a['zone_id']))
        target,error=closest_resistance(zones,cl)
        if error: count(error); continue
        stop=z['lower']*(1-E.RULES.stop_buffer); r1=target['lower']
        if not (0<stop<z['lower']<=z['body_lower']<=z['body_upper']<=z['upper']<cl<r1):
            count('INVALID_GEOMETRY'); continue
        rank=h1rank.get(t.floor('h'),np.nan)
        if not np.isfinite(rank): count('NO_COMPLETED_H1'); continue
        # sr_known_at records the latest role confirmation, not a future bar.
        known=max(z['role_at'],target['role_at'],z['born_at'],target['born_at'])
        assert known<=t
        row=dict(variant=NEW,symbol=symbol,signal_bar_open=t,signal_time=dec,sr_known_at=known,
            signal_close=float(cl),signal_open=float(op),signal_low=float(lo),signal_high=float(hi),
            previous_close=float(prevcl),s1=z['upper'],s2=np.nan,major_s=z['lower'],stop=stop,r1=r1,
            s1_count=z['source_bars'],r1_count=target['source_bars'],rank_turnover=float(rank),
            entry_s1_pct=(cl/z['upper']-1)*100,target_dist_pct=(r1/cl-1)*100,
            rr_actual_stop=(r1-cl)/(cl-stop),support_id=z['zone_id'],target_id=target['zone_id'],
            support_lower=z['lower'],support_upper=z['upper'],support_body_lower=z['body_lower'],
            support_body_upper=z['body_upper'],target_lower=target['lower'],target_upper=target['upper'],
            target_body_lower=target['body_lower'],target_body_upper=target['body_upper'],
            box_born_at=z['born_at'],support_role_at=z['role_at'],target_born_at=target['born_at'],
            target_role_at=target['role_at'],support_origin=z['origin'],support_flips=z['flips'],
            source_start=z['source_start'],source_end=z['source_end'],source_bars=z['source_bars'],
            box_width_pct=(z['upper']/z['lower']-1)*100,gap_to_upper_pct=(cl/z['upper']-1)*100,
            planned_reward_pct=(r1/cl-1)*100,planned_loss_pct=(1-stop/cl)*100,
            target_fee_only_net_usdt=E.RULES.notional*((r1/cl-1)-E.RULES.fee*(1+r1/cl)))
        result.append(row); count('SIGNAL')
    return pd.DataFrame(result,columns=COLUMNS),reasons


def entry_guard(bars,signals,mode):
    kept=[]; bad=[]
    for row in signals.to_dict('records'):
        t=row['signal_time']; d=bars[row['symbol']]; reason=None
        if mode=='NEXT_OPEN':
            if t not in d.index: reason='NO_NEXT_OPEN_DATA'; p=np.nan
            else: p=float(d.at[t,'open'])
        else: p=float(row['signal_close'])
        if reason is None and p<=row['support_upper']: reason='NEXT_PRICE_NOT_ABOVE_SUPPORT_BOX'
        if reason is None and p>=row['target_lower']: reason='NEXT_PRICE_AT_OR_ABOVE_TARGET_BOX'
        if reason is not None: bad.append(dict(signal_time=t,symbol=row['symbol'],reason=reason,entry_price=p))
        else: kept.append(row)
    return pd.DataFrame(kept,columns=signals.columns),pd.DataFrame(bad,columns=['signal_time','symbol','reason','entry_price'])


def raw_chart_svg(d,levels):
    """Static black/white data chart, no external assets; before-entry bars only."""
    if d.empty: return '<p>No complete bars.</p>'
    w,h=960,290; left,right,top,bottom=70,155,22,38
    prices=list(d.low)+list(d.high)+[float(v) for _,v in levels]
    low,high=min(prices),max(prices); pad=max((high-low)*.06,high*.001); low-=pad; high+=pad
    def y(p): return top+(high-float(p))/(high-low)*(h-top-bottom)
    dx=(w-left-right)/len(d)
    out=[f'<svg viewBox="0 0 {w} {h}" role="img"><rect width="100%" height="100%" fill="white"/>']
    for i,(t,r) in enumerate(d.iterrows()):
        x=left+(i+.5)*dx; yy=min(y(r.open),y(r.close)); bh=max(abs(y(r.open)-y(r.close)),.5)
        out.append(f'<path d="M{x:.2f},{y(r.high):.2f} V{y(r.low):.2f}" stroke="black" fill="none"/>')
        out.append(f'<rect x="{x-dx*.32:.2f}" y="{yy:.2f}" width="{dx*.64:.2f}" height="{bh:.2f}" stroke="black" fill="'+('white' if r.close>=r.open else 'black')+'"/>')
    for i,(label,p) in enumerate(levels):
        yp=y(p)
        out.append(f'<path d="M{left},{yp:.2f} H{w-right}" stroke="black" stroke-dasharray="{3+i*2},3" fill="none"/>')
        out.append(f'<text x="{w-right+5}" y="{yp-3:.2f}" font-size="11">{html.escape(label)} {p:.7g}</text>')
    for idx in (0,len(d)//2,len(d)-1):
        t=d.index[idx].tz_convert(E.KST)
        out.append(f'<text x="{left+(idx+.5)*dx:.1f}" y="{h-10}" font-size="11" text-anchor="middle">{t:%m/%d %H:%M}</text>')
    out.append('</svg>'); return ''.join(out)


def evidence(out,bars,new_sig):
    """Choose first signals in distinct symbols, never inspect future returns."""
    edir=out/'PRE_ENTRY_REVIEW'; edir.mkdir(exist_ok=True)
    selected=new_sig.sort_values(['signal_time','symbol']).drop_duplicates('symbol').head(12)
    cwrite(edir/'SELECTED_SIGNALS.csv',selected)
    pages=['<!doctype html><meta charset="utf-8"><title>Before-entry zone review</title><style>body{max-width:1100px;margin:24px auto;font-family:sans-serif;line-height:1.6}svg{width:100%;border:1px solid}section{margin:40px 0}table{border-collapse:collapse}td{padding:4px 10px;border:1px solid}</style><h1>진입 전 박스 확인 — 이후 수익 결과 숨김</h1><p>처음 발생한 서로 다른 종목 최대12개. 선은 신호봉 시작 전에 이미 알려진 값. 캔들 시각은 완성시각(KST). 리더 작도와 일치한다는 판정 아님.</p>']
    for n,row in enumerate(selected.to_dict('records'),1):
        s=row['symbol']; t=row['signal_time']; d=bars[s]
        before=d[(d.index+E.STEP)<=t].copy()
        name=f'{n:02}_{s}'
        cwrite(edir/f'{name}_15M_PRE.csv.gz',before.tail(96).rename_axis('bar_open').reset_index())
        levels=[('S lower',row['support_lower']),('S upper',row['support_upper']),('STOP',row['stop']),('R lower',row['r1'])]
        pages.append(f'<section><h2>{name} | {t.tz_convert(E.KST)}</h2><p>Support origin: {row["support_origin"]}; born {row["box_born_at"]}; role confirmed {row["support_role_at"]}.<br>지지 코어 {row["support_body_lower"]:.8g}~{row["support_body_upper"]:.8g}; 외곽 {row["support_lower"]:.8g}~{row["support_upper"]:.8g}.</p>')
        for tf,num in ((4,90),(1,96)):
            q=E.aggregate(before,tf); q=q[(q.index<=t)&q.close.notna()].tail(num)
            cwrite(edir/f'{name}_{tf}H_PRE.csv.gz',q.rename_axis('available_at').reset_index())
            pages.append(f'<h3>{tf}H — 진입시까지 완성된 봉</h3>'+raw_chart_svg(q,levels))
        fine=before.tail(32).copy(); fine.index=fine.index+E.STEP
        pages.append('<h3>15m — 신호봉 종료까지만 표시</h3>'+raw_chart_svg(fine,levels)+'</section>')
    if selected.empty: pages.append('<p>신호 0개: 성과 판정 불가. ZONE/GATE 감사기록 확인 필요.</p>')
    (out/'PRE_ENTRY_ZONE_REVIEW.html').write_text('\n'.join(pages),encoding='utf-8')


def self_test():
    base_tests=E.self_test()
    # No observations from January used. Deliberately artificial bars.
    idx=pd.date_range(E.START-pd.Timedelta(days=6),periods=32,freq='4h').as_unit('ns')
    vals=[]
    for i in range(32):
        if i<16: a=[110,112,107,109]
        elif i<19: a=[101,103,98,100]
        elif i<23: a=[104,106,103.5,105]
        else: a=[104,105,103,104.5]
        vals.append(a+[10,1000])
    q=pd.DataFrame(vals,index=idx,columns=E.OHLCV)
    snap,births,ev,sources=build_boxes(q,'TEST')
    assert len(births)>0 and (ev.event=='ROLE').any()
    when=idx[21]; zs=snap[when.value]
    supp=[z for z in zs if z['role']=='SUPPORT']; resist=[z for z in zs if z['role']=='RESISTANCE']
    assert supp and resist
    z=max(supp,key=lambda z:z['upper']); r=closest_resistance(zs,105)[0]; assert r and r['lower']>105
    assert all(z['born_at']<=when and z['role_at']<=when for z in zs)
    # Prefix invariance: appending future bars cannot redraw earlier zones.
    ps,pb,pe,_=build_boxes(q.iloc[:22],'TEST')
    assert ps=={k:v for k,v in snap.items() if k<=q.index[21].value}
    z0=dict(zone_id='FLIP',lower=98.,upper=102.,born_at=E.START-pd.Timedelta(days=1),
        role='RESISTANCE',role_at=E.START-pd.Timedelta(hours=4),origin='BOX_BREAKDOWN',flips=0)
    changed,event=advance_role(z0,103,E.START)
    assert changed['role']=='SUPPORT' and changed['origin']=='RESISTANCE_FLIP' and z0['role']=='RESISTANCE'
    assert advance_role(changed,100,E.START+E.STEP)[0] is changed
    assert closest_resistance([dict(role='RESISTANCE',lower=100,upper=110,born_at=E.START,zone_id='R')],105)[1]=='INSIDE_RESISTANCE'
    # A manufactured retest with roles known at t, then a distinct next-open fill.
    t=E.START.ceil('4h')+pd.Timedelta(hours=4)
    z=dict(zone_id='S',symbol='TEST',tf='4H',lower=98.,upper=102.,body_lower=99.,body_upper=101.,
        born_at=t-pd.Timedelta(days=1),role_at=t-pd.Timedelta(hours=4),role='SUPPORT',origin='BOX_BREAKOUT',
        flips=0,source_start=t-pd.Timedelta(days=2),source_end=t-pd.Timedelta(days=1),source_bars=3)
    rz={**z,'zone_id':'R','lower':108.,'upper':112.,'body_lower':109.,'body_upper':111.,'role':'RESISTANCE'}
    di=pd.date_range(t-pd.Timedelta(hours=1),periods=12,freq='15min').as_unit('ns')
    v=np.array([[102.2,102.5,102.1,102.3]]*12,float)
    v[4]=[102.2,103.0,101.9,102.8]
    v[5]=[103.5,110.,103.2,109.]
    d=pd.DataFrame(v,index=di,columns=E.OHLCV[:4]); d['volume']=10.;d['turnover']=1000.
    h1=E.aggregate(d,1)
    ss={t.value:(z,rz)}
    sig,gates=make_video_signals('TEST',d,ss,h1)
    assert len(sig)>=1 and sig.iloc[0].signal_time==t+E.STEP
    guard,_=entry_guard({'TEST':d},sig,'NEXT_OPEN')
    sm,tr,*_=E.simulate({'TEST':d},guard,'NEXT_OPEN',start=di[0],end=di[7],horizon=di[-1]+E.STEP)
    assert len(tr) and tr.iloc[0].entry_price==103.5 and tr.iloc[0].exit_reason=='R1'
    assert tr.iloc[0].exit_time>tr.iloc[0].entry_time
    bad=d.copy();bad.loc[t+E.STEP,'open']=101.0
    gs,gr=entry_guard({'TEST':bad},sig,'NEXT_OPEN')
    assert len(gr) and gr.iloc[0].reason=='NEXT_PRICE_NOT_ABOVE_SUPPORT_BOX'
    # Repeated touches in the same episode cannot repeatedly emit entries.
    touch=d.copy();touch.iloc[5:8,0]=102.2;touch.iloc[5:8,1]=103.1
    touch.iloc[5:8,2]=101.9;touch.iloc[5:8,3]=[102.85,102.9,102.95]
    s2,_=make_video_signals('TEST',touch,ss,E.aggregate(touch,1))
    assert len(s2)==1
    log('VIDEO_SELF_TEST_PASS: box formation / role flip / non-repainting / retest / entry geometry / next-open fill')
    return dict(software_only=True,actual_january_results_tested=False,base_tests=base_tests,
        video_tests=['body_overlap_box','closed_bar_role_flip','prefix_invariance','one_signal_per_touch',
                     'overlapping_resistance_blocks','next_open_above_support','next_open_fill','no_future_bar'])


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--root',type=Path,default=Path('/root/hyejin-trader'))
    ap.add_argument('--cache',type=Path)
    ap.add_argument('--self-test',action='store_true')
    ap.add_argument('--skip-legacy',action='store_true',help='Only for isolated software tests. Default includes stored D control.')
    args=ap.parse_args()
    tests=self_test()
    if args.self_test: return
    root=args.root.resolve(); cache=(args.cache or root/'swing_2026_01_07_batch'/'202601'/'cache15').resolve()
    final=root/'bybit_swing'; work=root/'swing_jan_video_zones_v1'
    final.mkdir(parents=True,exist_ok=True);work.mkdir(parents=True,exist_ok=True)
    lock=open(work/'RUN.lock','a+')
    try: fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        log('이미 같은 검증이 실행 중입니다. 중복 실행하지 않습니다.'); return
    done=threading.Event();threading.Thread(target=beat,args=(done,),daemon=True).start()
    try:
        lock.seek(0);lock.truncate();lock.write(str(os.getpid()));lock.flush()
        paths=sorted(cache.glob('*_15m_*.csv.gz'))
        if not paths: raise FileNotFoundError(f'원본 1월 15m 캐시 없음: {cache}')
        symfiles={}
        for p in paths:
            s=p.name.split('_15m_')[0]
            if s=='BTCUSDT':continue
            if s in symfiles: raise ValueError(f'중복 캐시: {s}')
            symfiles[s]=p
        legacy=root/'swing_jan_sr_reset_v1_2'/'ALL_SIGNALS.csv.gz'
        if not args.skip_legacy and not legacy.exists():
            raise FileNotFoundError(f'이전 D 대조군 신호파일 없음: {legacy}; 이전 결과를 덮어쓰지 않습니다.')
        inp=[(s,p.name,p.stat().st_size,p.stat().st_mtime_ns) for s,p in symfiles.items()]
        run_key=hashlib.sha256((sha(__file__)+sha(E.__file__)+json.dumps(inp)+str(args.skip_legacy)+(sha(legacy) if legacy.exists() else '')).encode()).hexdigest()[:16]
        out=work/run_key; out.mkdir(exist_ok=True); check=out/'checkpoints';check.mkdir(exist_ok=True)
        status=final/'S_JAN_VIDEO_ZONES_V1_STATUS.json'
        jwrite(status,dict(status='RUNNING',run_key=run_key,pid=os.getpid()))
        manifest=dict(version=VERSION,run_key=run_key,engine_version=E.VERSION,source_sha=sha(__file__),engine_sha=sha(E.__file__),
            box_assumptions=asdict(CFG),money_assumptions=asdict(E.RULES),entry_modes=MODES,cache=str(cache),
            actual_january_execution=True,network_calls=False,leader_exact_replication=False,independent_oos=False,
            scope='4H-only initial interpretation; monthly/weekly/daily zones not reproduced',
            old_control_available=not args.skip_legacy,inputs=inp,started_at=str(pd.Timestamp.now(tz='UTC')))
        jwrite(out/'MANIFEST.json',manifest);jwrite(out/'SOFTWARE_TESTS.json',tests)
        (out/'RULES_AND_LIMITATIONS.txt').write_text(RULES,encoding='utf-8')
        log(f'VERSION={VERSION}; ENGINE={E.VERSION}; run={run_key}; pandas={pd.__version__}; no network/no orders')
        stage('[1/5]','기존 1월 캐시 → 4H 박스/역할/리테스트 (종목별 중간저장)')
        bars={}; audits=[]; signalparts=[]; birthparts=[]; eventparts=[]; sourceparts=[]; gateparts=[]
        for i,(s,p) in enumerate(symfiles.items(),1):
            _STATE['detail']=f'{i}/{len(symfiles)} {s}'
            d,a=E.read_cache(p);a['symbol']=s
            if d.empty or not a['january_bars']:
                a['status']='NO_JANUARY';audits.append(a);continue
            bars[s]=d
            prefix=check/s
            marker=prefix.with_name(s+'_DONE.json')
            files={k:prefix.with_name(s+'_'+k+'.csv.gz') for k in ('SIGNALS','BOXES','EVENTS','SOURCE_BARS')}
            if marker.exists() and all(x.exists() for x in files.values()):
                m=json.loads(marker.read_text());sg=read_signal(files['SIGNALS']);local={}
                for k in ('BOXES','EVENTS','SOURCE_BARS'):
                    try:local[k]=pd.read_csv(files[k])
                    except pd.errors.EmptyDataError:local[k]=pd.DataFrame()
                b,ev,src=local['BOXES'],local['EVENTS'],local['SOURCE_BARS'];g=m['gates'];a['status']='CHECKPOINT'
            else:
                q=E.aggregate(d,4);h=E.aggregate(d,1)
                snap,b,ev,src=build_boxes(q,s);sg,g=make_video_signals(s,d,snap,h)
                for k,x in [('SIGNALS',sg),('BOXES',b),('EVENTS',ev),('SOURCE_BARS',src)]:cwrite(files[k],x)
                jwrite(marker,dict(gates=g));a['status']='NEW'
            a['boxes']=len(b);a['signals']=len(sg);audits.append(a)
            signalparts.append(sg);birthparts.append(b);eventparts.append(ev);sourceparts.append(src);gateparts.append(dict(symbol=s,**g))
            if i%10==0 or i==len(symfiles):
                log(f'완료 {i}/{len(symfiles)} | 봉 {len(d)} | 박스 {sum(len(x) for x in birthparts):,} | 신호 {sum(len(x) for x in signalparts):,}')
                cwrite(out/'DATA_AUDIT.csv',pd.DataFrame(audits));jwrite(status,dict(status='RUNNING',run_key=run_key,stage='ZONES',done=i,total=len(symfiles),pid=os.getpid()))
        if not bars:raise RuntimeError('1월 유효봉 없음. 수익 검증 불가.')
        def concat(items,empty=None):
            parts=[x for x in items if len(x)]
            return pd.concat(parts,ignore_index=True) if parts else (pd.DataFrame() if empty is None else empty)
        new=concat(signalparts,empty_signal())
        for c in STAMP_COLS:
            if c in new:new[c]=pd.to_datetime(new[c],utc=True).dt.as_unit('ns')
        cwrite(out/'NEW_SIGNALS.csv.gz',new);cwrite(out/'BOXES.csv.gz',concat(birthparts));cwrite(out/'ROLE_EVENTS.csv.gz',concat(eventparts))
        cwrite(out/'BOX_SOURCE_BARS.csv.gz',concat(sourceparts));cwrite(out/'GATE_AUDIT.csv',pd.DataFrame(gateparts));cwrite(out/'DATA_AUDIT.csv',pd.DataFrame(audits))
        stage('[2/5]',f'4슬롯 재시뮬레이션 준비 | 새 후보 신호 {len(new):,}')
        groups=[]
        if not args.skip_legacy:
            old=read_signal(legacy);old=old[(old.variant==OLD)&old.symbol.isin(bars)].copy()
            if old.empty:raise RuntimeError('기존 D 대조 신호가 0입니다. 대조군 파일 확인 필요.')
            groups.append(('OLD_D_CONTROL',old))
        groups.append((NEW,new));summaries=[]
        for name,sig in groups:
            for mode in MODES:
                stage('[3/5]',name+' / '+mode)
                if name==NEW:go,guard=entry_guard(bars,sig,mode)
                else:go,guard=sig,pd.DataFrame()
                sm,tr,ev,eq,rej,opn=E.simulate(bars,go,mode)
                daily,cohort=E.daily_tables(tr,ev,eq)
                sm.update(variant=name,raw_candidate_signals=len(sig),geometry_guard_blocked=len(guard))
                if name==NEW and new.empty:sm['validation_status']='NO_SIGNALS_REQUIRES_REVIEW'
                if len(tr):
                    sm['r1_fee_loss_count']=int((tr.exit_reason.str.startswith('R1')&(tr.net_fee_only_usdt<=0)).sum())
                    sm['median_target_distance_pct']=float(((tr.r1/tr.entry_price-1)*100).median())
                    sm['median_risk_distance_pct']=float(((1-tr.stop/tr.entry_price)*100).median())
                    sm['first_entry_kst']=str(tr.entry_time.min().tz_convert(E.KST));sm['last_entry_kst']=str(tr.entry_time.max().tz_convert(E.KST))
                    if name==NEW:
                        assert (tr.support_upper<tr.entry_price).all() and (tr.entry_price<tr.r1).all()
                        assert (tr.sr_known_at<=tr.signal_bar_open).all()
                sm['january_cashflow_usdt']=float(daily.loc[daily.date_kst.str.startswith('2026-01'),'cashflow_net_usdt'].sum())
                sm['post_january_tail_cashflow_usdt']=float(daily.loc[~daily.date_kst.str.startswith('2026-01'),'cashflow_net_usdt'].sum())
                summaries.append(sm)
                tag=name+'__'+mode
                for label,x in [('TRADES',tr),('DAILY_BY_EXIT',daily),('ENTRY_COHORT',cohort),('EVENTS',ev),('EQUITY',eq),('REJECTED',rej),('OPEN_POSITIONS',opn),('ENTRY_GUARD',guard)]:
                    cwrite(out/(tag+'_'+label+'.csv.gz'),x)
                cwrite(out/'SUMMARY.csv',pd.DataFrame(summaries))
                log(f"{name} {mode}: trades={sm['trades_closed']} net={sm['net_fee_only_closed_usdt']:.4f} status={sm['validation_status']}")
        stage('[4/5]','수익결과를 가린 진입 전 차트·원자료·박스 감사자료 작성')
        evidence(out,bars,new)
        manifest.update(finished_at=str(pd.Timestamp.now(tz='UTC')),symbols_observed=len(bars),signal_count=len(new),
                        warning_no_signals=new.empty,simulation_status='BAR_MODEL_NOT_LIVE')
        jwrite(out/'MANIFEST.json',manifest)
        dst=final/'S_JAN_VIDEO_ZONES_V1_RESULTS.zip';temp=dst.with_suffix('.zip.tmp')
        with zipfile.ZipFile(temp,'w',compression=zipfile.ZIP_DEFLATED) as z:
            for p in sorted(out.rglob('*')):
                if p.is_file() and 'checkpoints' not in p.parts and not p.name.endswith('.tmp'):z.write(p,str(p.relative_to(out)))
            z.write(Path(__file__),'SOURCE_swing_jan_video_zones_v1.py');z.write(Path(E.__file__),'SOURCE_accounting_engine_v1_2.py')
        os.replace(temp,dst)
        jwrite(status,dict(status='COMPLETE_REVIEW_REQUIRED' if new.empty else 'COMPLETE',run_key=run_key,zip=str(dst)))
        stage('[5/5]','완료 '+str(dst))
        show=['variant','entry_mode','trades_closed','net_fee_only_closed_usdt','profit_factor_net','max_15m_close_equity_dd_usdt','insufficient_equity_signals']
        print(pd.DataFrame(summaries).reindex(columns=show).to_string(index=False),flush=True)
    except Exception as exc:
        (final/'S_JAN_VIDEO_ZONES_V1_ERROR.txt').write_text(traceback.format_exc(),encoding='utf-8')
        jwrite(final/'S_JAN_VIDEO_ZONES_V1_STATUS.json',dict(status='FAILED',error=str(exc)))
        raise
    finally:
        done.set();fcntl.flock(lock.fileno(),fcntl.LOCK_UN);lock.close()

if __name__=='__main__':main()
