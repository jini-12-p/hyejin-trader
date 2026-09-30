#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""S형 2026-01 SR reset v1. RESEARCH ONLY: no network, keys or orders.

Uses ONLY the existing January 15m cache. Does not import or edit any bot.
Four SR definitions, two entry price assumptions, same funded 4-slot simulator.
All extra market / trend / minimum reward / RR / reclaim filters removed.
Read RULES_AND_LIMITATIONS.txt in the result ZIP before interpreting results.
"""
from __future__ import annotations
import argparse
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import threading
import time
import traceback
import zipfile
from dataclasses import dataclass

import numpy as np
import pandas as pd

VERSION = "JAN_SR_RESET_V1_2_TIMEUNIT_FIX_20260930"
KST = "Asia/Seoul"
STEP = pd.Timedelta(minutes=15)
START = pd.Timestamp("2026-01-01", tz=KST).tz_convert("UTC")
END = pd.Timestamp("2026-02-01", tz=KST).tz_convert("UTC")  # exclusive
DATA_START = START - pd.Timedelta(days=6)
HORIZON = END + pd.Timedelta(days=2)  # exits only after END
VARIANTS = ("A_1H18", "B_1H36", "C_1H72", "D_4H72_1H36")
MODES = ("NEXT_OPEN", "SIGNAL_CLOSE_DIAGNOSTIC")
OHLCV = ["open", "high", "low", "close", "volume", "turnover"]
SIG_COLS = ["variant", "symbol", "signal_bar_open", "signal_time", "sr_known_at",
            "signal_close", "s1", "s2", "major_s", "stop", "r1", "s1_count",
            "r1_count", "rank_turnover", "signal_open", "signal_low", "signal_high",
            "previous_close", "entry_s1_pct", "target_dist_pct", "rr_actual_stop"]

@dataclass(frozen=True)
class Rules:
    equity: float = 200.0
    margin: float = 27.0
    leverage: float = 5.0
    slots: int = 4
    fee: float = 0.00055  # assumed, not an assertion about an account's actual tier
    stop_buffer: float = 0.006
    cooldown_minutes: int = 120
    max_hold_minutes: int = 1440
    @property
    def notional(self) -> float:
        return self.margin * self.leverage

RULES = Rules()
RULES_TEXT = """S형 1월 재시작 — JAN_SR_RESET_V1_2_TIMEUNIT_FIX
[기술 수정]
pandas datetime의 s/ms/us/ns 저장단위를 UTC 나노초로 명시 통일.
시각/가격 반올림, 보간, 전략 임계값 변경 없음. 원본 캐시 파일 수정 없음.
정상 15분봉을 단위혼용으로 제외하던 V1_1 오류를 수정.
기존 0거래 V1 ZIP과 구분하려고 V1_2 작업폴더/ZIP 사용.
매 실행시 합성 데이터로 CSV 로딩/4개 시간단위/신호생성을 검사.

연구용 가상체결. 주문/API키/서버 봇 수정 없음. 네트워크 호출도 없음.

[범위]
실제 진입시각 KST 2026-01-01 00:00 <= t < 2026-02-01 00:00.
1월 캐시만 읽음. 12월말은 워밍업, 2월초는 1월 진입분 청산용.
1~9월 결과를 이미 보았으므로 이번 작업은 재탐색이지 새로운 독립 OOS가 아님.

[공통 고정 실행/청산 규칙 — 최적이라고 주장하지 않음]
200 USDT 시작, 거래당 증거금 27, 레버리지 5, 명목금액 135 고정, 최대 4종목.
재투입금 확대/복리사이징 없음. 같은 종목 중복 보유 없음. 물타기 없음.
신규 진입은 추정 equity - 사용 증거금이 새 증거금+진입수수료보다 작으면 차단.
이 자금 검사는 시장필터가 아닌 실행 가능성 검사임. 자동 추가입금 없음.
청산 이후 해당 종목 2시간 대기, 최대 24시간 보유 후 시장청산 가정.
R1/지지/STOP은 신호 당시 값을 고정. A/B/C STOP=S1*0.994;
D STOP=4H major support*0.994, 진입 접촉은 1H fine support 기준.
지지 아래 0.6%는 비교를 위한 기존 공통값이며 최적 손절폭이 아님.
수수료는 진입 135*0.00055, 청산 실제 가상명목금액*0.00055.
기본 슬리피지 0, 펀딩/호가 스프레드/호가단위/수량단위/최소주문금액/청산엔진 미반영.
거래별 왕복 0.10%(각 방향 0.05%) 불리한 가격을 가정한 사후 비용 민감도도 출력.
그 민감도는 거래/슬롯을 재실행한 것이 아니며, 최종 순수익으로 부르면 안 됨.

[지지/저항 정의]
A=1H 최근 18개, B=36개, C=72개. 1H 고저가를 0.6% 군집으로 묶음.
군집 내 2개 이상의 봉이 있어야 레벨로 인정. 독립 재접촉 2회와 동의어는 아님.
D=4H 최근 18개(72h) 고저가 1% 군집 + 1H36 fine support(0.6% 군집).
D fine support는 major support와 1.2% 안에서 가장 가까운 것.
A/B/C는 최근 완성 1H 종가의 1.5% 위까지 포함한 지지후보 중 가장 높은 S1.
저항은 최근 완성 1H 종가보다 높은 것 중 가장 가까운 군집.
신호/실행가격이 target 위이면 그 거래는 실행불가로 기록; 먼 R2로 임의 교체 안 함.
S2는 진단용으로만 기록: 없다고 진입 차단하지 않음. S1-S2 간격 제한 없음.
최소 목표수익 1%, 최소 RR 1.15, RR 제외대역, Q2 회복하한 추가 모두 없음.
BTC/ETH/Breadth/종목별 4H 연속하락가드 전부 없음.
옛 quality=RR+접촉수 점수도 사용 안 함. 동시 신호는 직전 완성 1H 거래대금순,
동률은 symbol 오름차순. 자리가 없으면 기다리게 하지 않고 해당 신호를 건너뜀.

[기본 15분 반등 — 모든 후보 공통]
low<=S1*1.004 AND close>=S1*0.998 AND close>open AND close>previous_close.
추가 Q2 +0.10% 조건 없음. 완성봉 정보만 사용.
이는 '지지 접촉 즉시 지정가매수' 전략이 아닌 마감확인형 전략임.
이 규칙의 허용폭 자체도 하나의 가정이며 순수 지지저항에 유일한 정답은 아님.

[시간/데이터]
캐시의 naive time은 원래 생성 코드대로 KST '봉 시작'으로 해석, 내부는 UTC.
15m 시작+15m=신호확정시간. 시간 표시를 15분 앞당겨 entry로 기록하지 않음.
1H/4H는 UTC 경계의 완성봉만 사용. 종전 naive KST 4H 재묶음과 다른 정의임.
이번은 새 기준선이므로 이전 A~D 수익과 직접 동일조건 비교 아님.
S/R은 신호봉 시작 전에 알려진 상태만 사용; 신호봉이 레벨을 소급해 만들지 않음.
A/B/C 모두 72시간 완전한 1H 히스토리가 있을 때부터 비교. D는 18개 완전한 4H도 필요.
캔들 공백은 보간하지 않음. 필요한 과거봉이 비면 신호 안 만듦.
15분 정각이 아닌 비정규/부분 봉은 가격이나 시각을 반올림해 만들지 않고 제외하며 감사표에 기록.
보유 중 공백은 별도 기록, 낡은 가격에 임의로 STOP 체결시키지 않음.

[진입가격 2개 시나리오]
NEXT_OPEN: 신호봉 종료 직후 다음 15m 시가; 라이브 체결에 대한 근사치일 뿐.
SIGNAL_CLOSE_DIAGNOSTIC: 신호봉 종가를 종료시점에 얻을 수 있다고 가정한 낙관적 비교.
두 모드 모두 신호봉 내부 고저가로 신규 포지션을 소급 청산하지 않음.
NEXT_OPEN은 진입한 봉의 고저가부터 즉시 청산 판정. 신호봉과 달리 스킵하지 않음.
시장 갭으로 기존 stop를 넘었으면 stop가 아닌 더 불리한 시가로 STOP_GAP.
목표 갭은 target가격만 인정(추가 유리한 갭수익 없음).
봉 안에서 STOP/R1 동시접촉이면 STOP 우선; 발생시각은 봉 종료로만 기록(정확한 tick아님).
SIGNAL_CLOSE 진입은 다음 시가의 갭 정보를 보기 전; NEXT_OPEN은 시가에서 재확인.
Shadow에는 실제 주문 체결평단이 존재하지 않음. LIVE 실제평단과 혼동 금지.

[출력]
SUMMARY: 8개 비교, gross, fee-only net, net PF, equity, 비용 민감도.
DAILY_BY_EXIT: 청산/수수료 발생일 KST 기준 실제 현금흐름. ENTRY_COHORT는 별도 참고.
EQUITY: 15분 종가평가 포함 계좌추정. CLOSED_EQUITY는 진입수수료를 즉시 반영한 잔고.
DD는 시작 200을 포함해 계산, 청산순서 및 같은시각 묶음 기준, 진입순서 아님.
MDD는 15분 종가 근사; 봉중 최저낙폭/Mark price/거래소 강제청산 재현 아님.
동일봉 exit들 사이의 실제 순서도 알 수 없으므로 같은시간 합산.
한 거래 손실이 투입증거금을 넘으면 liquidation_model_required 표시.
유효 완료월/월말오픈/공백/부족자금은 반드시 별도로 확인.

[범위 한계]
캐시는 과거에 선정한 종목목록의 부분집합. 1월 상장 전체, 당시 유동성 순위,
상장폐지된 종목 누락까지 복원했다고 주장할 수 없음. FETCH_STATUS 및 데이터 감사 동봉.
ETH/BTC 시장분석은 이번 결과에 필터로 들어가지 않음. 결과 후 별도 연구.
"""

_PROGRESS = {"stage": "starting", "detail": ""}
def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)
def update(stage: str, detail: str = "") -> None:
    _PROGRESS.update(stage=stage, detail=detail)
    log(stage + (" " + detail if detail else ""))
def heartbeat(stop: threading.Event) -> None:
    while not stop.wait(60):
        log(f"HEARTBEAT {_PROGRESS['stage']} | {_PROGRESS['detail']}")
def write_json(path: Path, data: dict) -> None:
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temp, path)
def write_csv(path: Path, df: pd.DataFrame) -> None:
    x = df.copy()
    for c in x.columns:
        if isinstance(x[c].dtype, pd.DatetimeTZDtype):
            x[c] = x[c].dt.tz_convert(KST).dt.strftime("%Y-%m-%d %H:%M:%S%z")
    temp = path.with_name(path.name + ".tmp")
    x.to_csv(temp, index=False, encoding="utf-8-sig",
             compression="gzip" if path.name.endswith(".gz") else None)
    os.replace(temp, path)

def utc_index(values) -> pd.DatetimeIndex:
    idx = pd.DatetimeIndex(pd.to_datetime(values, errors="raise"))
    if idx.tz is None:
        idx = idx.tz_localize(KST)
    if idx.hasnans:
        raise ValueError("NaT timestamps in candle input")
    # pandas 3.x can preserve/infer seconds or microseconds.  Timestamp.value
    # and Timedelta.value below are nanoseconds, so use the same integer unit.
    # This converts storage units only, not clock times or prices.
    return idx.tz_convert("UTC").as_unit("ns")

def read_cache(path: Path) -> tuple[pd.DataFrame, dict]:
    raw = pd.read_csv(path, compression="infer")
    if not set(["time"] + OHLCV).issubset(raw.columns):
        raise ValueError(f"{path.name}: missing columns {set(['time']+OHLCV)-set(raw.columns)}")
    d = raw[OHLCV].apply(pd.to_numeric, errors="coerce")
    parsed_index = pd.DatetimeIndex(pd.to_datetime(raw["time"], errors="raise"))
    input_time_dtype = str(parsed_index.dtype)
    d.index = utc_index(parsed_index)
    d = d.sort_index()
    if d.index.duplicated().any():
        groups = d.groupby(level=0).nunique(dropna=False)
        if (groups > 1).any().any():
            raise ValueError(f"{path.name}: conflicting duplicate timestamps")
        d = d[~d.index.duplicated(keep="first")]
    d = d[(d.index >= DATA_START) & (d.index < HORIZON)]

    # Some newly listed instruments can contain a partial first candle whose
    # timestamp is not aligned to the exchange's normal 15m grid.  Never round
    # or shift such a bar because that would manufacture a price at a time that
    # did not exist.  Drop it and keep an explicit audit count instead.
    in_window_before_grid = len(d)
    # Compare real timestamps, not potentially mixed-unit integers.
    on_grid = (d.index == d.index.floor("15min"))
    off_grid = int((~on_grid).sum())
    d = d.loc[on_grid].copy()

    finite = np.isfinite(d.to_numpy()).all(axis=1)
    valid = (finite & (d[["open","high","low","close"]] > 0).all(axis=1)
             & (d.high >= d[["open","close","low"]].max(axis=1))
             & (d.low <= d[["open","close","high"]].min(axis=1))
             & (d.volume >= 0) & (d.turnover >= 0))
    invalid = int((~valid).sum())
    d = d.loc[valid].copy()
    jan = d[(d.index >= START) & (d.index < END)]
    gaps = int(np.maximum(np.diff(d.index.as_unit("ns").asi8)//STEP.value - 1, 0).sum()) if len(d)>1 else 0
    audit = dict(file=path.name, raw_rows=len(raw), in_window_before_grid=in_window_before_grid,
                 input_time_dtype=input_time_dtype, normalized_time_dtype=str(d.index.dtype),
                 usable_bars=len(d), january_bars=len(jan),
                 off_grid_rows_dropped=off_grid, invalid_rows=invalid,
                 observed_first=d.index.min() if len(d) else None,
                 observed_last=d.index.max() if len(d) else None,
                 missing_intervals_inside_observed_span=gaps)
    return d, audit

def aggregate(d: pd.DataFrame, hours: int) -> pd.DataFrame:
    rule = f"{hours}h"
    h = d.resample(rule, origin="epoch", closed="left", label="left").agg(
        {"open":"first","high":"max","low":"min","close":"last","volume":"sum","turnover":"sum"})
    h["count"] = d.close.resample(rule, origin="epoch", closed="left", label="left").count()
    h.loc[h["count"] != hours*4, OHLCV] = np.nan
    h.index = h.index + pd.Timedelta(hours=hours)  # availability / completion
    return h

def cluster_levels(vals, tol: float = 0.006) -> list[tuple[float, int]]:
    # Input is sorted, so current group median is O(1), no repeated np.median calls.
    a = np.sort(np.asarray(vals, dtype=float))
    groups: list[list[float]] = []
    for v in a:
        if not math.isfinite(v) or v <= 0:
            continue
        if groups:
            g = groups[-1]; n = len(g)
            med = g[n//2] if n%2 else (g[n//2-1]+g[n//2])/2
            if abs(v-med)/med <= tol:
                g.append(float(v)); continue
        groups.append([float(v)])
    out = []
    for g in groups:
        n = len(g)
        if n >= 2:
            out.append((g[n//2] if n%2 else (g[n//2-1]+g[n//2])/2, n))
    return out

def level_state(lo, hi, ref, turnover, known_at):
    su = sorted([g for g in lo if g[0] <= ref*1.015], reverse=True)
    re = sorted([g for g in hi if g[0] > ref])
    if not su or not re:
        return None
    s1, ns = su[0]; r1, nr = re[0]
    if r1 <= s1*0.994:
        return None
    return dict(s1=s1, s2=su[1][0] if len(su)>1 else np.nan, major_s=s1,
                stop=s1*(1-RULES.stop_buffer), r1=r1, s1_count=ns, r1_count=nr,
                rank_turnover=turnover, sr_known_at=known_at)

def make_states(d: pd.DataFrame) -> dict[str, dict]:
    h, q = aggregate(d, 1), aggregate(d, 4)
    a = h[["low","high","close","turnover"]].to_numpy()
    qa = q[["low","high","close"]].to_numpy()
    ht, qt = h.index.as_unit("ns").asi8, q.index.as_unit("ns").asi8
    states = {v:{} for v in VARIANTS}
    qmemo: dict = {}
    for k in range(71, len(h)):
        # Equal historical eligibility for A/B/C. Missing bars never filled.
        if not np.isfinite(a[k-71:k+1]).all():
            continue
        known = h.index[k]
        if known < START-pd.Timedelta(hours=1) or known >= END:
            continue
        ref, tv = float(a[k,2]), float(a[k,3])
        clusters = {}
        for v, lb in zip(VARIANTS[:3], (18,36,72)):
            z = a[k-lb+1:k+1]
            sl, rl = cluster_levels(z[:,0]), cluster_levels(z[:,1])
            clusters[lb] = (sl, rl)
            st = level_state(sl, rl, ref, tv, known)
            if st is not None:
                states[v][known.value] = st
        j = int(np.searchsorted(qt, ht[k], side="right") - 1)
        if j < 17:
            continue
        if j not in qmemo:
            z = qa[j-17:j+1]
            qmemo[j] = (cluster_levels(z[:,0], .01), cluster_levels(z[:,1], .01)) if np.isfinite(z).all() else None
        pair = qmemo[j]
        if pair is None:
            continue
        sl, rl = pair
        su = sorted([g for g in sl if g[0] <= ref*1.015], reverse=True)
        re = sorted([g for g in rl if g[0] > ref])
        if not su or not re:
            continue
        major, ns4 = su[0]; r1, nr4 = re[0]
        nearby = [g for g in clusters[36][0] if abs(g[0]-major)/major <= .012]
        if not nearby:
            continue
        fine, ns1 = min(nearby, key=lambda x:(abs(x[0]-major), x[0]))
        stop = major*(1-RULES.stop_buffer)
        if fine <= stop or r1 <= stop:
            continue
        states[VARIANTS[3]][known.value] = dict(
            s1=fine, s2=su[1][0] if len(su)>1 else np.nan, major_s=major,
            stop=stop, r1=r1, s1_count=ns1, r1_count=nr4,
            rank_turnover=tv, sr_known_at=known)
    return states

def make_signals(symbol: str, d: pd.DataFrame, states: dict) -> pd.DataFrame:
    out = []
    vals = d[OHLCV].to_numpy()
    it = d.index
    for i in range(1, len(d)):
        t = it[i]; decision = t+STEP
        if decision < START or decision >= END or t-it[i-1] != STEP:
            continue
        op, hi, lo, cl = vals[i,:4]
        prev = vals[i-1,3]
        if not (cl > op and cl > prev):
            continue
        known_key = t.floor("h").value  # known BEFORE the signal candle
        for v in VARIANTS:
            st = states[v].get(known_key)
            if st is None:
                continue
            s1, stop, r1 = st["s1"], st["stop"], st["r1"]
            if not (lo <= s1*1.004 and cl >= s1*.998 and stop < cl < r1):
                continue
            out.append(dict(variant=v, symbol=symbol, signal_bar_open=t, signal_time=decision,
                            signal_close=cl, signal_open=op, signal_high=hi, signal_low=lo,
                            previous_close=prev, entry_s1_pct=(cl/s1-1)*100,
                            target_dist_pct=(r1/cl-1)*100, rr_actual_stop=(r1-cl)/(cl-stop), **st))
    return pd.DataFrame(out, columns=SIG_COLS)

def drawdown(values: np.ndarray, initial: float) -> tuple[float, float]:
    a = np.r_[initial, np.asarray(values, float)]
    peaks = np.maximum.accumulate(a)
    diffs = a-peaks
    pct = np.divide(diffs, peaks, out=np.full_like(diffs, np.nan), where=peaks>0)*100
    return float(diffs.min()), float(np.nanmin(pct))

def net_streak(tr: pd.DataFrame) -> int:
    if tr.empty:
        return 0
    # All simultaneous/interval-tied exits are a single event; no invented within-bar ordering.
    vals = tr.groupby("exit_time")["net_fee_only_usdt"].sum().sort_index()
    best = cur = 0
    for v in vals:
        cur = cur+1 if v < 0 else 0
        best = max(best, cur)
    return best

def simulate(bars: dict[str, pd.DataFrame], sig: pd.DataFrame, mode: str,
             rules: Rules = RULES, start=START, end=END, horizon=HORIZON):
    if mode not in MODES:
        raise ValueError(mode)
    grid = pd.date_range(start, horizon, freq="15min", inclusive="left").as_unit("ns")
    arrays = {s:d.reindex(grid)[["open","high","low","close"]].to_numpy() for s,d in bars.items()}
    by = {}
    if not sig.empty:
        ordered = sig[(sig.signal_time>=start)&(sig.signal_time<end)].sort_values(
            ["signal_time","rank_turnover","symbol"], ascending=[True,False,True], kind="stable")
        for s in ordered.to_dict("records"):
            by.setdefault(pd.Timestamp(s["signal_time"]).value, []).append(s)
    pos: dict = {}; cooldown: dict = {}; records=[]; events=[]; equity=[]; rejected=[]
    cash = rules.equity
    max_active=0; missing_position_bars=0; stale_valuation_steps=0
    active_gap_count=0; funding_blocks=0; min_available=float("inf")
    def close(s, p, raw_price, when, reason, ambiguous=False, exit_bar=None):
        nonlocal cash
        qty = p["quantity"]
        gross = qty*(raw_price-p["entry_price"])
        exit_fee = qty*raw_price*rules.fee
        net = gross-p["entry_fee"]-exit_fee
        cash += gross-exit_fee
        # Deterministic adverse +/-5 bps per side, same trades, no re-scheduling.
        se, sx = p["entry_price"]*1.0005, raw_price*.9995
        sqty = rules.notional/se
        stress = sqty*(sx-se) - rules.notional*rules.fee - sqty*sx*rules.fee
        records.append({**p, "exit_time":when, "exit_bar_open":exit_bar,
            "exit_price":raw_price, "exit_reason":reason, "both_touched":bool(ambiguous),
            "exit_fee":exit_fee, "gross_usdt":gross, "net_fee_only_usdt":net,
            "ret_price_pct":(raw_price/p["entry_price"]-1)*100,
            "stress_net_same_trades_005pct_side_usdt":stress,
            "hold_hours":(when-p["entry_time"]).total_seconds()/3600,
            "loss_exceeds_initial_margin":net < -rules.margin})
        events.append(dict(time=when, symbol=s, event="EXIT", pnl_cash_usdt=gross-exit_fee))
        del pos[s]
        cooldown[s] = when+pd.Timedelta(minutes=rules.cooldown_minutes)
    def entries(t, j, opening):
        nonlocal cash, max_active, funding_blocks, min_available
        for row in by.get(t.value, []):
            s=row["symbol"]
            reason=None
            if s in pos:
                reason="ALREADY_OPEN"
            elif t < cooldown.get(s, start):
                reason="SYMBOL_COOLDOWN"
            elif len(pos)>=rules.slots:
                reason="SLOTS_FULL"
            arr=arrays.get(s)
            has_bar=arr is not None and np.isfinite(arr[j]).all()
            if reason is None and opening and not has_bar:
                reason="NO_NEXT_OPEN_DATA"
            price = (float(arr[j,0]) if opening and has_bar else float(row["signal_close"]))
            if reason is None and not (row["stop"] < price < row["r1"]):
                reason="ENTRY_OUTSIDE_STOP_TARGET"
            if reason is None:
                unreal = sum(p["quantity"]*(p["last_price"]-p["entry_price"]) for p in pos.values())
                available=cash+unreal-len(pos)*rules.margin
                min_available=min(min_available, available)
                if available < rules.margin+rules.notional*rules.fee:
                    reason="INSUFFICIENT_EQUITY"
                    funding_blocks+=1
            if reason is not None:
                rejected.append(dict(signal_time=t, symbol=s, reason=reason))
                continue
            assert row["sr_known_at"] <= row["signal_bar_open"] < t
            fee=rules.notional*rules.fee
            p={**row, "entry_time":t, "entry_price":price, "entry_mode":mode,
               "quantity":rules.notional/price, "margin_usdt":rules.margin,
               "entry_fee":fee, "last_price":price,
               "entry_vs_signal_close_pct":(price/row["signal_close"]-1)*100}
            cash-=fee
            pos[s]=p
            events.append(dict(time=t, symbol=s, event="ENTRY_FEE", pnl_cash_usdt=-fee))
            max_active=max(max_active,len(pos))
    for j,t in enumerate(grid):
        # Hypothetical signal-close fills cannot use information in the next open.
        if mode=="SIGNAL_CLOSE_DIAGNOSTIC":
            entries(t,j,False)
        # At open: old positions can gap through barriers. Do not execute at a stale stop.
        for s,p in list(pos.items()):
            r=arrays[s][j]
            if not np.isfinite(r).all():
                continue
            op=float(r[0]); p["last_price"]=op
            if op<=p["stop"]:
                active_gap_count+=1
                close(s,p,op,t,"STOP_GAP",exit_bar=t)
            elif op>=p["r1"]:
                close(s,p,p["r1"],t,"R1_GAP",exit_bar=t)
            elif t-p["entry_time"]>=pd.Timedelta(minutes=rules.max_hold_minutes):
                close(s,p,op,t,"TIME_NEXT_AVAILABLE",exit_bar=t)
        if mode=="NEXT_OPEN":
            entries(t,j,True)
        # Now process this candle, including newly opened NEXT_OPEN positions.
        interval_end=t+STEP
        stale=False
        for s,p in list(pos.items()):
            r=arrays[s][j]
            if not np.isfinite(r).all():
                missing_position_bars+=1; stale=True
                continue
            op,hi,lo,cl=map(float,r)
            stop_hit=lo<=p["stop"]; target_hit=hi>=p["r1"]
            if stop_hit:
                close(s,p,p["stop"],interval_end,"STOP",target_hit,t)
            elif target_hit:
                close(s,p,p["r1"],interval_end,"R1",False,t)
            elif interval_end-p["entry_time"]>=pd.Timedelta(minutes=rules.max_hold_minutes):
                close(s,p,cl,interval_end,"TIME",False,t)
            else:
                p["last_price"]=cl
        if stale: stale_valuation_steps+=1
        unreal=sum(p["quantity"]*(p["last_price"]-p["entry_price"]) for p in pos.values())
        eq=cash+unreal
        equity.append(dict(time=interval_end,cash_equity=cash,unrealized_pnl=unreal,
                           close_price_equity_proxy=eq,open_positions=len(pos),
                           used_margin=len(pos)*rules.margin,free_equity_proxy=eq-len(pos)*rules.margin,
                           stale_position_prices=stale))
    tr=pd.DataFrame(records)
    ev=pd.DataFrame(events,columns=["time","symbol","event","pnl_cash_usdt"])
    eq=pd.DataFrame(equity)
    opn=pd.DataFrame(list(pos.values()))
    rej=pd.DataFrame(rejected,columns=["signal_time","symbol","reason"])
    closed_dd,closed_dd_pct=drawdown(eq.cash_equity.to_numpy(),rules.equity)
    mtm_dd,mtm_dd_pct=drawdown(eq.close_price_equity_proxy.to_numpy(),rules.equity)
    # Event balance includes actual initial 200 and ties are aggregated, not sorted by entry.
    cash_events=ev.groupby("time").pnl_cash_usdt.sum().sort_index().cumsum()+rules.equity if len(ev) else pd.Series(dtype=float)
    event_dd,event_dd_pct=drawdown(cash_events.to_numpy(),rules.equity)
    def pf(v):
        gain=float(v[v>0].sum()); loss=-float(v[v<0].sum())
        return gain/loss if loss>0 else (float("inf") if gain>0 else np.nan)
    n=len(tr)
    if n:
        net=tr.net_fee_only_usdt; ret=tr.ret_price_pct; gross=tr.gross_usdt
        w=ret[net>0]; l=ret[net<0]
        jan_ev=ev.assign(date=ev.time.dt.tz_convert(KST).dt.strftime("%Y-%m-%d")).groupby("date").pnl_cash_usdt.sum()
        losses_margin=int(tr.loss_exceeds_initial_margin.sum())
        summary=dict(trades_closed=n,entries_total=int((ev.event=="ENTRY_FEE").sum()),
             wins_net=int((net>0).sum()),losses_net=int((net<0).sum()),
             win_rate_net_pct=100*float((net>0).mean()),avg_win_price_pct=float(w.mean()),
             avg_loss_price_pct=float(l.mean()),profit_factor_gross=pf(gross),profit_factor_net=pf(net),
             gross_usdt=float(gross.sum()),fees_closed_trades=float((tr.entry_fee+tr.exit_fee).sum()),
             net_fee_only_closed_usdt=float(net.sum()),
             stress_net_same_trades_005pct_side_usdt=float(tr.stress_net_same_trades_005pct_side_usdt.sum()),
             stop_count=int(tr.exit_reason.str.startswith("STOP").sum()),
             r1_count=int(tr.exit_reason.str.startswith("R1").sum()),
             time_count=int(tr.exit_reason.str.startswith("TIME").sum()),
             same_bar_ambiguous_count=int(tr.both_touched.sum()),
             worst_trade_net_usdt=float(net.min()),best_trade_net_usdt=float(net.max()),
             max_hold_hours=float(tr.hold_hours.max()),loss_exceeds_margin_count=losses_margin,
             max_net_loss_exit_event_streak=net_streak(tr))
    else:
        summary=dict(trades_closed=0,entries_total=int((ev.event=="ENTRY_FEE").sum()),
                     net_fee_only_closed_usdt=0.,profit_factor_net=np.nan,
                     win_rate_net_pct=np.nan,stop_count=0,r1_count=0,time_count=0,
                     loss_exceeds_margin_count=0)
    summary.update(entry_mode=mode,initial_equity=rules.equity,margin_per_slot=rules.margin,
        leverage=rules.leverage,max_slots=rules.slots,max_active_observed=max_active,
        ending_cash_equity=float(cash),ending_equity_close_proxy=float(eq.close_price_equity_proxy.iloc[-1]),
        max_closed_dd_usdt=event_dd,max_closed_dd_pct=event_dd_pct,
        max_15m_close_equity_dd_usdt=mtm_dd,max_15m_close_equity_dd_pct=mtm_dd_pct,
        open_at_end=len(opn),missing_active_position_bars=missing_position_bars,
        stale_equity_steps=stale_valuation_steps,opening_stop_gap_count=active_gap_count,
        insufficient_equity_signals=funding_blocks,signals_total=len(sig),
        validation_status="INCOMPLETE_OR_EXECUTION_RISK" if (len(opn) or missing_position_bars or summary["loss_exceeds_margin_count"]) else "BAR_MODEL_ONLY_NOT_LIVE_VALIDATED")
    return summary,tr,ev,eq,rej,opn

def daily_tables(tr,ev,eq):
    dates=pd.date_range(START.tz_convert(KST).normalize(),HORIZON.tz_convert(KST).normalize(),freq="D",inclusive="left").strftime("%Y-%m-%d")
    d=pd.DataFrame(index=pd.Index(dates,name="date_kst"))
    if len(ev):
        e=ev.assign(date_kst=ev.time.dt.tz_convert(KST).dt.strftime("%Y-%m-%d"))
        d["cashflow_net_usdt"]=e.groupby("date_kst").pnl_cash_usdt.sum()
        d["entries"]=e[e.event=="ENTRY_FEE"].groupby("date_kst").size()
    if len(tr):
        t=tr.assign(date_kst=tr.exit_time.dt.tz_convert(KST).dt.strftime("%Y-%m-%d"))
        d["closed_trades"]=t.groupby("date_kst").size()
        d["closed_trade_net_usdt"]=t.groupby("date_kst").net_fee_only_usdt.sum()
        d["stop"]=t[t.exit_reason.str.startswith("STOP")].groupby("date_kst").size()
        d["r1"]=t[t.exit_reason.str.startswith("R1")].groupby("date_kst").size()
    d=d.fillna(0)
    if "cashflow_net_usdt" not in d:
        d["cashflow_net_usdt"]=0.
    d["cum_cashflow_net_usdt"]=d.cashflow_net_usdt.cumsum()
    cohort=pd.DataFrame()
    if len(tr):
        t=tr.assign(entry_date_kst=tr.entry_time.dt.tz_convert(KST).dt.strftime("%Y-%m-%d"))
        cohort=t.groupby("entry_date_kst").agg(trades=("symbol","size"), eventual_net_usdt=("net_fee_only_usdt","sum")).reset_index()
    return d.reset_index(),cohort

def self_test():
    # Fixed toy data tests, NOT historical performance results.
    idx=pd.date_range(START,periods=9,freq="15min")
    vals=np.array([[100,103,90,102],[110,112,108,111],[111,125,109,121]]+[[121,122,120,121]]*6,float)
    d=pd.DataFrame(vals,columns=["open","high","low","close"],index=idx)
    d["volume"]=1.; d["turnover"]=100.
    rec=dict(variant=VARIANTS[0],symbol="TEST",signal_bar_open=idx[0],signal_time=idx[1],sr_known_at=idx[0],
             signal_close=102.,s1=100.,s2=98.,major_s=100.,stop=99.4,r1=120.,
             s1_count=2,r1_count=2,rank_turnover=100.,signal_open=100.,signal_low=90.,signal_high=103.,
             previous_close=99.,entry_s1_pct=2.,target_dist_pct=17.647,rr_actual_stop=6.7)
    sig=pd.DataFrame([rec])
    s,tr,*_=simulate({"TEST":d},sig,"NEXT_OPEN",start=idx[0],end=idx[4],horizon=idx[-1]+STEP)
    assert len(tr)==1 and tr.iloc[0].entry_price==110 and tr.iloc[0].exit_reason=="R1"
    assert tr.iloc[0].exit_time==idx[3]  # touch occurs in 2nd post-signal candle
    # Signal candle low=90 must NOT be used to stop out a new position retrospectively.
    _,tc,*_=simulate({"TEST":d},sig,"SIGNAL_CLOSE_DIAGNOSTIC",start=idx[0],end=idx[4],horizon=idx[-1]+STEP)
    assert tc.iloc[0].entry_price==102 and tc.iloc[0].exit_reason=="R1"
    # Entry candle after signal must not be skipped; both sides -> STOP.
    dd=d.copy(); dd.loc[idx[1],["open","high","low","close"]]=[110,125,90,111]
    _,tt,*_=simulate({"TEST":dd},sig,"NEXT_OPEN",start=idx[0],end=idx[4],horizon=idx[-1]+STEP)
    assert tt.iloc[0].exit_reason=="STOP" and bool(tt.iloc[0].both_touched)
    # Adverse opening gap fills at opening price, not the planned stop.
    dd=d.copy(); dd.loc[idx[2],["open","high","low","close"]]=[90,92,88,91]
    _,tt,*_=simulate({"TEST":dd},sig,"NEXT_OPEN",start=idx[0],end=idx[4],horizon=idx[-1]+STEP)
    assert tt.iloc[0].exit_reason=="STOP_GAP" and tt.iloc[0].exit_price==90
    # Starting high-water mark must include initial balance.
    assert drawdown(np.array([197,196,198]),200)[0]==-4
    # Funded entries cannot continue at full size with insufficient collateral.
    small=Rules(equity=20)
    s,tt,*_=simulate({"TEST":d},sig,"NEXT_OPEN",small,start=idx[0],end=idx[4],horizon=idx[-1]+STEP)
    assert len(tt)==0 and s["insufficient_equity_signals"]==1
    # Five simultaneous candidates: max four, known turnover ranking only.
    sig5=pd.concat([sig.assign(symbol=f"T{i}",rank_turnover=i+1.) for i in range(5)],ignore_index=True)
    s,tt,*_=simulate({f"T{i}":d for i in range(5)},sig5,"NEXT_OPEN",start=idx[0],end=idx[4],horizon=idx[-1]+STEP)
    assert s["max_active_observed"]==4 and len(tt)==4 and "T0" not in set(tt.symbol)
    # Fast clustered medians exactly preserve the original grouping rule.
    rng=np.random.default_rng(42)
    for _ in range(50):
        a=np.sort(100+rng.normal(size=72)); groups=[]
        for v in a:
            if groups and abs(v-np.median(groups[-1]))/np.median(groups[-1])<=.006:
                groups[-1].append(v)
            else: groups.append([v])
        ref=[(float(np.median(g)),len(g)) for g in groups if len(g)>=2]
        assert cluster_levels(a)==ref
    # Availability timestamps: 1H assembled from 4 quarter-hour bars ends at +1h.
    h=aggregate(d,1)
    assert h.index[0]==idx[0].floor("h")+pd.Timedelta(hours=1)
    time_checks = self_test_time_units()
    log("SELF_TEST_PASS: entries, signal timing, entry-bar exits, gaps, slots, capital, DD, clustering, s/ms/us/ns time units, CSV-to-signal pipeline")
    return {"self_test":"PASS","synthetic_only":True,"time_unit_tests":time_checks,
            "checks":["next_open_fill","signal_close_diagnostic","no_signal_bar_exit",
                      "entry_bar_included","stop_first_ambiguous","adverse_open_gap",
                      "initial_peak_DD","insufficient_funds","four_slots","fast_cluster_equivalence","bar_availability"]}

def self_test_time_units():
    """Reproduce the failure using non-ns inputs and verify the real read path.

    Toy prices are only for testing software.  They are never included in the
    historical cache or performance reports.
    """
    import tempfile
    idx = pd.date_range(START-pd.Timedelta(days=4), periods=96*7, freq="15min").as_unit("ns")
    x = np.arange(len(idx), dtype=float)
    op = 100.0 + 0.65*np.sin(2*np.pi*x/24.0)
    cl = 100.0 + 0.65*np.sin(2*np.pi*(x+1)/24.0)
    hi = np.maximum(op, cl) + 0.45
    lo = np.minimum(op, cl) - 0.45
    sample = pd.DataFrame({"open":op,"high":hi,"low":lo,"close":cl,
                           "volume":np.ones(len(idx)),"turnover":cl*100}, index=idx)
    # Both ns and non-ns indexes must produce identical histories and signals.
    ref_states = make_states(sample)
    ref_signals = make_signals("UNIT_TEST", sample, ref_states)
    if ref_signals.empty:
        raise AssertionError("time-unit fixture should produce at least one signal")
    results = {}
    for unit in ("s", "ms", "us", "ns"):
        source_idx = idx.as_unit(unit)
        normalized = utc_index(source_idx)
        assert normalized.equals(idx)
        assert normalized.dtype.unit == "ns"
        assert (normalized.asi8 % STEP.value == 0).all()
        alternate = sample.copy()
        alternate.index = source_idx
        states = make_states(alternate)
        signals = make_signals("UNIT_TEST", alternate, states)
        assert {v:len(states[v]) for v in VARIANTS} == {v:len(ref_states[v]) for v in VARIANTS}
        pd.testing.assert_frame_equal(signals, ref_signals, check_dtype=False)
        results[unit] = {"normal_bars_preserved":len(idx),"signals":len(signals)}
    # Exercise read_csv -> parse -> localize -> normalize -> grid -> signal flow.
    raw = sample.copy()
    raw.insert(0, "time", idx.tz_convert(KST).tz_localize(None).strftime("%Y-%m-%d %H:%M:%S"))
    raw = raw.reset_index(drop=True)
    with tempfile.TemporaryDirectory(prefix="swing_jan_timeunit_test_") as temp:
        fp = Path(temp)/"UNIT_TEST_15m.csv.gz"
        raw.to_csv(fp,index=False,compression="gzip")
        loaded,audit = read_cache(fp)
        assert len(loaded) == len(raw) and audit["off_grid_rows_dropped"] == 0
        assert loaded.index.equals(idx)
        pd.testing.assert_frame_equal(loaded.reset_index(drop=True),sample.reset_index(drop=True),
                                      check_dtype=False,check_exact=False,rtol=1e-12,atol=1e-12)
        csv_signals = make_signals("UNIT_TEST",loaded,make_states(loaded))
        assert len(csv_signals)==len(ref_signals)
        # One genuinely off-grid row is excluded; no price/timestamp rounding.
        extra = raw.iloc[[0]].copy()
        extra["time"] = (idx[0].tz_convert(KST).tz_localize(None)+pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
        pd.concat([raw,extra],ignore_index=True).to_csv(fp,index=False,compression="gzip")
        loaded2,audit2 = read_cache(fp)
        assert len(loaded2)==len(raw) and audit2["off_grid_rows_dropped"] == 1
        assert loaded2.index.equals(idx)
    log(f"TIME_UNIT_TEST_PASS: s/ms/us/ns preserve {len(idx)} regular bars; "
        f"synthetic signals={len(ref_signals)}; actual off-grid test=1 row excluded")
    return results


def main():
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root",type=Path,default=Path("/root/hyejin-trader"))
    ap.add_argument("--cache",type=Path,default=None)
    ap.add_argument("--self-test",action="store_true")
    args=ap.parse_args()
    log(f"VERSION={VERSION}; Python={sys.version.split()[0]}; pandas={pd.__version__}; numpy={np.__version__}")
    tests=self_test()
    if args.self_test: return
    root=args.root.resolve()
    cache=(args.cache or root/"swing_2026_01_07_batch"/"202601"/"cache15").resolve()
    out=root/"swing_jan_sr_reset_v1_2"; final=root/"bybit_swing"
    out.mkdir(parents=True,exist_ok=True); final.mkdir(parents=True,exist_ok=True)
    lock=open(out/"RUN.lock","a+")
    try: fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        log("이미 같은 1월 검증이 실행 중입니다. 중복 실행하지 않습니다."); return
    lock.seek(0); lock.truncate(); lock.write(str(os.getpid())); lock.flush()
    done=threading.Event(); threading.Thread(target=heartbeat,args=(done,),daemon=True).start()
    start_wall=time.time()
    try:
        update("[1/5] 1월 캐시 확인 (다운로드/매매 없음)",str(cache))
        if not cache.is_dir():
            raise FileNotFoundError(f"1월 캐시 경로 없음: {cache}. 결과ZIP만으로 재구성하지 않습니다.")
        paths=sorted(cache.glob("*_15m_*.csv.gz"))
        if not paths: raise FileNotFoundError(f"실제 15분봉 캐시 없음: {cache}")
        symfiles={}
        for p in paths:
            symbol=p.name.split("_15m_")[0]
            if symbol in symfiles:
                raise ValueError(f"{symbol}: 캐시가 2개 이상입니다. 기간을 확인한 뒤 단일 캐시폴더를 지정하세요.")
            symfiles[symbol]=p
        code_sha=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        fingerprint=hashlib.sha256(json.dumps([(p.name,p.stat().st_size,p.stat().st_mtime_ns) for p in paths]).encode()).hexdigest()
        run_key=hashlib.sha256((code_sha+fingerprint).encode()).hexdigest()[:16]
        checkdir=out/"checkpoints"/run_key; checkdir.mkdir(parents=True,exist_ok=True)
        manifest=dict(version=VERSION,code_sha256=code_sha,input_fingerprint=fingerprint,
              cache=str(cache),start_kst=str(START.tz_convert(KST)),end_exclusive_kst=str(END.tz_convert(KST)),
              variants=VARIANTS,entry_modes=MODES,params=RULES.__dict__,
              higher_bar_anchor="UTC",inputs=[p.name for p in paths],
              independent_oos=False,only_january_used=True)
        write_json(out/"MANIFEST.json",manifest)
        (out/"RULES_AND_LIMITATIONS.txt").write_text(RULES_TEXT,encoding="utf-8")
        write_json(out/"SELF_TEST.json",tests)
        b15={}; audits=[]; sig_parts=[]
        update("[2/5] 종목별 1H/4H 한 번 생성 → A/B/C/D 신호 + 중간저장")
        strategy_files=[(s,p) for s,p in symfiles.items() if s!="BTCUSDT"]
        for i,(sym,path) in enumerate(strategy_files,1):
            _PROGRESS["detail"]=f"{i}/{len(strategy_files)} {sym}"
            d,audit=read_cache(path); audit["symbol"]=sym
            if i <= 3 or audit["off_grid_rows_dropped"]:
                log(f"INPUT_AUDIT {sym}: {audit['input_time_dtype']} -> {audit['normalized_time_dtype']}; "
                    f"before_grid={audit['in_window_before_grid']} retained={len(d)} "
                    f"offgrid_dropped={audit['off_grid_rows_dropped']} january={audit['january_bars']}")
            if len(d)==0 or audit["january_bars"]==0:
                audit["status"]="NO_JANUARY_OBSERVATIONS"; audits.append(audit); continue
            b15[sym]=d
            cp=checkdir/f"{sym}_SIGNALS.csv.gz"
            if cp.exists():
                sig=pd.read_csv(cp,compression="gzip")
                for c in ("signal_bar_open","signal_time","sr_known_at"):
                    sig[c]=pd.to_datetime(sig[c],utc=True).dt.as_unit("ns")
                audit["status"]="CHECKPOINT_REUSED"
            else:
                sig=make_signals(sym,d,make_states(d))
                write_csv(cp,sig); audit["status"]="GENERATED"
            audit["all_variant_signals"]=len(sig)
            audits.append(audit); sig_parts.append(sig)
            if i%25==0 or i==len(strategy_files):
                log(f"  완료 {i}/{len(strategy_files)} 종목 · 신호 {sum(len(x) for x in sig_parts):,}")
                write_csv(out/"DATA_AUDIT.csv",pd.DataFrame(audits))
                write_json(final/"S_JAN_SR_RESET_V1_2_STATUS.json",dict(status="RUNNING",stage="signals",done=i,total=len(strategy_files),pid=os.getpid()))
        write_csv(out/"DATA_AUDIT.csv",pd.DataFrame(audits))
        if not b15: raise RuntimeError("1월 관측 데이터가 있는 종목이 없습니다.")
        sig=pd.concat(sig_parts,ignore_index=True) if sig_parts else pd.DataFrame(columns=SIG_COLS)
        write_csv(out/"ALL_SIGNALS.csv.gz",sig)
        signal_counts = {v:int(sig.variant.eq(v).sum()) for v in VARIANTS}
        log(f"SIGNALS_BY_VARIANT={signal_counts}")
        if sig.empty:
            raise RuntimeError(
                "NO_SIGNALS_DIAGNOSTIC_REQUIRED: all four variants produced zero signals. "
                "No successful result ZIP was written. Inspect DATA_AUDIT.csv and the log."
            )
        rows=[]
        update("[3/5] 4슬롯 / 200불 자금제약 / 두 가지 진입가격 비교")
        for v in VARIANTS:
            sub=sig.loc[sig.variant==v].copy()
            for mode in MODES:
                name=f"{v}__{mode}"
                update("[3/5] 시뮬레이션",name)
                result_path=out/f"{name}_SUMMARY.json"
                if result_path.exists():
                    prior=json.loads(result_path.read_text(encoding="utf-8"))
                    if (prior.get("run_key")==run_key and prior.get("complete")
                            and all((out/f"{name}_{label}.csv.gz").exists() for label in
                                    ("TRADES","DAILY_BY_EXIT","ENTRY_COHORT","EQUITY","REJECTED_SIGNALS","OPEN_POSITIONS"))):
                        rows.append(prior["summary"]); log("  완성된 시나리오 재사용"); continue
                sm,tr,ev,eq,rej,opn=simulate(b15,sub,mode)
                sm["variant"]=v
                daily,cohort=daily_tables(tr,ev,eq)
                jan_days=daily.date_kst.str.startswith("2026-01")
                sm["january_calendar_cashflow_net_usdt"]=float(daily.loc[jan_days,"cashflow_net_usdt"].sum())
                sm["february_tail_cashflow_net_usdt"]=float(daily.loc[~jan_days,"cashflow_net_usdt"].sum())
                sm["worst_cashflow_day_usdt"]=float(daily.cashflow_net_usdt.min())
                sm["best_cashflow_day_usdt"]=float(daily.cashflow_net_usdt.max())
                for label,df in [("TRADES",tr),("DAILY_BY_EXIT",daily),("ENTRY_COHORT",cohort),
                                 ("EQUITY",eq),("REJECTED_SIGNALS",rej),("OPEN_POSITIONS",opn)]:
                    write_csv(out/f"{name}_{label}.csv.gz",df)
                write_json(result_path,dict(run_key=run_key,complete=True,summary=sm))
                rows.append(sm)
                log(f"  {v}/{mode}: trades={sm['trades_closed']} fee-only net={sm['net_fee_only_closed_usdt']:.2f}; status={sm['validation_status']}")
        update("[4/5] 결과표/감사기록/ZIP 작성")
        summary=pd.DataFrame(rows)
        write_csv(out/"S_JAN_SR_RESET_SUMMARY.csv",summary)
        manifest.update(finished_at_utc=str(pd.Timestamp.now(tz="UTC")),elapsed_seconds=time.time()-start_wall,
                        symbols_observed=len(b15),signal_count=len(sig))
        write_json(out/"MANIFEST.json",manifest)
        zp=final/"S_JAN_SR_RESET_V1_2_RESULTS.zip"; tmp=zp.with_suffix(".zip.tmp")
        with zipfile.ZipFile(tmp,"w",zipfile.ZIP_DEFLATED) as z:
            for p in sorted(out.iterdir()):
                if p.is_file() and p.suffix not in (".lock",".tmp"):
                    z.write(p,p.name)
            z.write(Path(__file__),"SOURCE_swing_jan_sr_reset_v1_2.py")
            src_status=cache.parent/"FETCH_STATUS.csv"
            if src_status.exists(): z.write(src_status,"SOURCE_JAN_FETCH_STATUS.csv")
        os.replace(tmp,zp)
        write_json(final/"S_JAN_SR_RESET_V1_2_STATUS.json",dict(status="COMPLETE",zip=str(zp),elapsed_seconds=time.time()-start_wall))
        update("[5/5] 완료",str(zp))
        cols=["variant","entry_mode","trades_closed","net_fee_only_closed_usdt","profit_factor_net","max_15m_close_equity_dd_usdt","insufficient_equity_signals"]
        print(summary[cols].to_string(index=False),flush=True)
    except Exception as exc:
        text=traceback.format_exc()
        (final/"S_JAN_SR_RESET_V1_2_ERROR.txt").write_text(text,encoding="utf-8")
        write_json(final/"S_JAN_SR_RESET_V1_2_STATUS.json",dict(status="FAILED",error=str(exc)))
        raise
    finally:
        done.set()
        fcntl.flock(lock.fileno(),fcntl.LOCK_UN)
        lock.close()

if __name__=="__main__":
    main()
