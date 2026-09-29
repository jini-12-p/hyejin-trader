#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S형 2026년 8월 OOS(Out-of-Sample) 실제 Bybit 15분봉 검증

9월에서 확정/후보로 남긴 규칙을 8월 데이터에서는 절대 튜닝하지 않고 그대로 적용:
  Q2_BASE
    - C형: 1H 최근 최대 72시간 지지/저항
    - 기존 C형의 4H 3봉 연속 하락 가드 유지
    - BTC 4H 시장필터: weak(close<EMA20 & EMA20 하락) AND gap<=-0.50% 이면 신규진입 차단
    - S1 도달 후 진입종가가 S1 대비 +0.10% 이상 회복한 신호만
    - 4슬롯 / 슬롯당 27 USDT / 5x
    - TP=R1, STOP=S1*0.994, 최대보유 24h, 종료 후 2h cooldown
    - 동일 15m봉에서 TP/STOP 동시 터치 시 STOP 우선
  Q2_RR_BLOCK_15_20
    - 위 Q2_BASE와 동일
    - 추가로 1.5 <= RR < 2.0 이면 진입 차단

데이터:
  - 진입기간: 2026-08-01 00:00 ~ 2026-08-31 23:59 KST
  - 워밍업/청산용 실제봉: 2026-07-27 ~ 2026-09-02 KST
  - 종목 풀: 9월 실제봉 검증에서 사용한 동일 SYMBOLS 목록
  - BTCUSDT는 시장필터 계산용으로 별도 다운로드

최종 ZIP:
  /root/hyejin-trader/bybit_swing/S_AUG_OOS_Q2_RR_ACTUAL15_RESULTS.zip

연구/백테스트 전용. 실제 주문 없음.
"""
from __future__ import annotations

import os
import time
import zipfile
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

try:
    import swing_actual15_recheck as sepbase
except Exception as e:
    raise SystemExit(f"Cannot import swing_actual15_recheck.py: {e}")

OUT = Path("/root/hyejin-trader/swing_aug_oos_q2_rr")
CACHE = OUT / "cache15"
FINAL = Path("/root/hyejin-trader/bybit_swing")
OUT.mkdir(parents=True, exist_ok=True)
CACHE.mkdir(parents=True, exist_ok=True)
FINAL.mkdir(parents=True, exist_ok=True)

SYMBOLS = list(sepbase.SYMBOLS)
ENTRY_START = pd.Timestamp("2026-08-01 00:00:00")
ENTRY_END   = pd.Timestamp("2026-08-31 23:59:59")
FETCH_START = pd.Timestamp("2026-07-27 00:00:00")
FETCH_END   = pd.Timestamp("2026-09-02 00:00:00")

MAX_POSITIONS = 4
MARGIN_USDT = 27.0
LEVERAGE = 5.0
NOTIONAL = MARGIN_USDT * LEVERAGE
FEE_EACH_SIDE = 0.00055
WORKERS = int(os.environ.get("S_SWING_WORKERS", "6"))
API = "https://api.bybit.com/v5/market/kline"

CACHE_TAG = "20260727_20260902"


def kst_naive_to_utc_ms(ts: pd.Timestamp) -> int:
    return int(ts.tz_localize("Asia/Seoul").tz_convert("UTC").timestamp() * 1000)


START_MS = kst_naive_to_utc_ms(FETCH_START)
END_MS = kst_naive_to_utc_ms(FETCH_END)


def _request(params, tries=7):
    last = None
    for i in range(tries):
        try:
            r = requests.get(API, params=params, timeout=20)
            if r.status_code == 200:
                j = r.json()
                if int(j.get("retCode", -1)) == 0:
                    return j
                last = RuntimeError(f"retCode={j.get('retCode')} retMsg={j.get('retMsg')}")
            else:
                last = RuntimeError(f"HTTP {r.status_code} {r.text[:200]}")
        except Exception as e:
            last = e
        time.sleep(min(0.5 * (2 ** i), 8.0))
    raise last or RuntimeError("request failed")


def cache_path(sym: str) -> Path:
    return CACHE / f"{sym}_15m_{CACHE_TAG}.csv.gz"


def fetch_symbol(sym: str):
    fp = cache_path(sym)
    if fp.exists() and fp.stat().st_size > 200:
        try:
            d = pd.read_csv(fp, compression="gzip", parse_dates=["time"])
            if len(d) > 100:
                return sym, len(d), "CACHE", ""
        except Exception:
            pass

    rows = []
    cursor_end = END_MS
    seen_min = None

    while cursor_end >= START_MS:
        params = {
            "category": "linear",
            "symbol": sym,
            "interval": "15",
            "limit": 1000,
            "end": cursor_end,
        }
        try:
            j = _request(params)
        except Exception as e:
            return sym, 0, "ERROR", repr(e)

        arr = (j.get("result") or {}).get("list") or []
        if not arr:
            break

        for x in arr:
            try:
                ms = int(x[0])
                if START_MS <= ms <= END_MS:
                    rows.append((
                        ms, float(x[1]), float(x[2]), float(x[3]),
                        float(x[4]), float(x[5]), float(x[6])
                    ))
            except Exception:
                continue

        mins = []
        for x in arr:
            try:
                mins.append(int(x[0]))
            except Exception:
                pass
        if not mins:
            break

        mn = min(mins)
        if seen_min is not None and mn >= seen_min:
            break
        seen_min = mn
        if mn <= START_MS:
            break
        cursor_end = mn - 1
        time.sleep(0.03)

    if not rows:
        return sym, 0, "NO_DATA", ""

    d = pd.DataFrame(rows, columns=["ms","open","high","low","close","volume","turnover"])
    d = d.drop_duplicates("ms").sort_values("ms")
    t = pd.to_datetime(d.pop("ms"), unit="ms", utc=True).dt.tz_convert("Asia/Seoul").dt.tz_localize(None)
    d.insert(0, "time", t)
    d = d[(d.time >= FETCH_START) & (d.time <= FETCH_END)]
    d.to_csv(fp, index=False, compression="gzip")
    return sym, len(d), "FETCH", ""


def fetch_all():
    requested = list(dict.fromkeys(SYMBOLS + ["BTCUSDT"]))
    status = []
    print(f"[1/8] 8월 OOS 실제 15m 다운로드/캐시: {len(requested)} symbols, workers={WORKERS}", flush=True)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_symbol, s): s for s in requested}
        done = 0
        for fut in as_completed(futs):
            sym = futs[fut]
            done += 1
            try:
                s, n, mode, err = fut.result()
            except Exception as e:
                s, n, mode, err = sym, 0, "ERROR", repr(e)
            status.append({"symbol": s, "bars": n, "mode": mode, "error": err})
            if done % 20 == 0 or done == len(requested):
                valid = sum(1 for x in status if x["bars"] >= 100)
                print(f"  {done}/{len(requested)} 완료 · 유효 {valid}", flush=True)

    S = pd.DataFrame(status).sort_values(["bars","symbol"], ascending=[False, True])
    S.to_csv(OUT / "S_AUG_OOS_FETCH_STATUS.csv", index=False)
    return status


def load_bars(status):
    b15 = {}
    for x in status:
        if x["symbol"] == "BTCUSDT" or int(x["bars"]) < 100:
            continue
        fp = cache_path(x["symbol"])
        try:
            d = pd.read_csv(fp, compression="gzip", parse_dates=["time"]).set_index("time").sort_index()
            d = d[~d.index.duplicated(keep="last")]
            if len(d) >= 100:
                b15[x["symbol"]] = d[["open","high","low","close","volume","turnover"]].astype(float)
        except Exception:
            pass
    return b15


def load_btc():
    fp = cache_path("BTCUSDT")
    if not fp.exists():
        raise FileNotFoundError("BTCUSDT cache missing")
    d = pd.read_csv(fp, compression="gzip", parse_dates=["time"]).set_index("time").sort_index()
    d = d[~d.index.duplicated(keep="last")]
    return d[["open","high","low","close","volume","turnover"]].astype(float)


def cluster_levels(vals, tol=.006):
    arr = np.sort(np.asarray(vals, float))
    if len(arr) == 0:
        return []
    groups = []
    for v in arr:
        if groups and abs(v - np.median(groups[-1])) / np.median(groups[-1]) <= tol:
            groups[-1].append(v)
        else:
            groups.append([v])
    return [(float(np.median(g)), len(g)) for g in groups if len(g) >= 2]


def build_higher(b15):
    b1, b4 = {}, {}
    agg = {"open":"first","high":"max","low":"min","close":"last","volume":"sum","turnover":"sum"}
    for i, (sym, d) in enumerate(b15.items(), 1):
        h = d.resample("1h").agg(agg)
        h["n"] = d["close"].resample("1h").count()
        h = h.dropna(subset=["open","high","low","close"])

        q = d.resample("4h").agg(agg)
        q["n"] = d["close"].resample("4h").count()
        q = q.dropna(subset=["open","high","low","close"])

        if len(h) >= 20:
            b1[sym] = h
            b4[sym] = q

        if i % 75 == 0:
            print(f"    higher bars {i}/{len(b15)}", flush=True)
    return b1, b4


def make_sr_hourly(b1, b4, lookback=72):
    states = {}
    total = len(b1)

    for i, (sym, h) in enumerate(b1.items(), 1):
        d = {}
        for k in range(10, len(h) + 1):
            hist = h.iloc[max(0, k-lookback):k]
            hour = (h.index[k-1] + pd.Timedelta(hours=1)).floor("h")

            if len(hist) < 10 or (hist.n >= 4).mean() < .5:
                continue

            ref = float(hist.close.iloc[-1])
            supp = cluster_levels(hist.low.values)
            res = cluster_levels(hist.high.values)

            supp = sorted([(lv,n) for lv,n in supp if lv <= ref * 1.015], reverse=True)
            res = sorted([(lv,n) for lv,n in res if lv >= ref * .985])
            if len(supp) < 2 or len(res) < 1:
                continue

            s1 = supp[0]
            s2s = [x for x in supp[1:] if x[0] <= s1[0] * .99 and x[0] >= s1[0] * .92]
            if not s2s:
                continue
            s2 = s2s[0]

            r1s = [x for x in res if x[0] >= ref * 1.005]
            if not r1s:
                continue
            r1 = r1s[0]

            # 9월 C형과 동일한 해당 종목 4H 3봉 연속 하락 가드
            trend_ok = True
            q = b4.get(sym)
            if q is not None:
                qc = q[(q.index + pd.Timedelta(hours=4)) <= hour]
                if len(qc) >= 3:
                    z = qc.tail(3)
                    down = (
                        z.close.iloc[2] < z.close.iloc[1] < z.close.iloc[0]
                        and z.high.iloc[2] < z.high.iloc[1] < z.high.iloc[0]
                        and z.low.iloc[2] < z.low.iloc[1] < z.low.iloc[0]
                    )
                    trend_ok = not down

            if trend_ok:
                d[hour] = (s1[0], s2[0], r1[0], s1[1], s2[1], r1[1])

        states[sym] = d
        if i % 50 == 0 or i == total:
            print(f"    C72 states {i}/{total}", flush=True)
    return states


def make_signals(b15, states):
    out = []
    total = len(b15)

    for i, (sym, h) in enumerate(b15.items(), 1):
        st = states.get(sym, {})
        if not st:
            continue

        prev = None
        hh = h[(h.index >= FETCH_START) & (h.index <= ENTRY_END)]
        for t, row in hh.iterrows():
            if prev is None:
                prev = row
                continue

            hour = t.floor("h")
            if hour not in st:
                prev = row
                continue

            s1, s2, r1, nt1, nt2, ntr = st[hour]
            cl, lo, op = float(row.close), float(row.low), float(row.open)

            if not (lo <= s1 * 1.004 and cl >= s1 * .998 and cl > op and cl > float(prev.close)):
                prev = row
                continue

            reward = (r1 - cl) / cl
            risk_filter_stop = s2 * .994
            risk = (cl - risk_filter_stop) / cl

            if reward >= .01 and risk > 0 and reward / risk >= 1.15:
                quality = reward / risk + .1 * (nt1 + ntr - 4)
                out.append((t, sym, cl, s1, s2, r1, reward/risk, quality))

            prev = row

        if i % 75 == 0 or i == total:
            print(f"    signals {i}/{total}", flush=True)

    return pd.DataFrame(out, columns=["time","symbol","entry","s1","s2","r1","rr","quality"])


def btc_market_features(btc15):
    q = btc15.resample("4h").agg({
        "open":"first","high":"max","low":"min","close":"last","volume":"sum","turnover":"sum"
    })
    q["n"] = btc15["close"].resample("4h").count()
    q = q.dropna(subset=["close"])
    q["ema20"] = q["close"].ewm(span=20, adjust=False, min_periods=20).mean()
    q["ema20_prev"] = q["ema20"].shift(1)
    q["gap_pct"] = (q["close"] / q["ema20"] - 1.0) * 100.0
    q["weak"] = (q["n"] >= 16) & (q["close"] < q["ema20"]) & (q["ema20"] < q["ema20_prev"])
    # 완성된 4H봉은 종료시각부터 사용
    q.index = q.index + pd.Timedelta(hours=4)
    return q[["gap_pct","weak"]].sort_index()


def attach_btc_market(sig, btc_feat):
    if sig.empty:
        x = sig.copy()
        x["btc_gap_pct"] = np.nan
        x["btc_weak"] = False
        return x

    left = sig.copy().reset_index(drop=True)
    left["_order"] = np.arange(len(left))
    left2 = left.sort_values("time")

    right = btc_feat.reset_index()
    right = right.rename(columns={right.columns[0]:"time"}).sort_values("time")

    m = pd.merge_asof(left2, right, on="time", direction="backward")
    m = m.sort_values("_order").drop(columns=["_order"]).reset_index(drop=True)
    m["btc_gap_pct"] = pd.to_numeric(m["gap_pct"], errors="coerce")
    m["btc_weak"] = m["weak"].fillna(False).astype(bool)
    return m.drop(columns=["gap_pct","weak"])


def prepare_q2_signals(sig_with_market):
    x = sig_with_market.copy()
    x["entry_s1_pct"] = (x["entry"] / x["s1"] - 1.0) * 100.0

    # 9월 확정 시장필터: weak 상태 + EMA20 대비 -0.50% 이하에서만 차단
    market_block = x["btc_weak"].fillna(False) & (x["btc_gap_pct"] <= -0.50)

    # Q2: 시장 통과 + S1 대비 +0.10% 이상 회복
    q2 = x.loc[(~market_block) & (x["entry_s1_pct"] >= 0.10)].copy()
    q2 = q2.sort_values(["time","symbol"]).reset_index(drop=True)
    return q2


def simulate(b15, sig):
    sig = sig[(sig.time >= ENTRY_START) & (sig.time <= ENTRY_END)].copy()
    by = {t: g.sort_values("quality", ascending=False) for t, g in sig.groupby("time")}

    times = pd.date_range(FETCH_START, FETCH_END, freq="15min")
    pos = {}
    cool = {}
    tr = []

    for t in times:
        # exits first
        for sym, p in list(pos.items()):
            h = b15.get(sym)
            if h is None or t not in h.index:
                continue
            r = h.loc[t]
            lo, hi, cl = float(r.low), float(r.high), float(r.close)
            stop = p["s1"] * .994
            target = p["r1"]

            reason = None
            px = None
            if lo <= stop:
                reason, px = "STOP", stop
            elif hi >= target:
                reason, px = "R1", target
            elif t - p["time"] >= pd.Timedelta(hours=24):
                reason, px = "TIME", cl

            if reason:
                ret = px / p["entry"] - 1
                tr.append({
                    **p,
                    "exit_time": t,
                    "exit_price": px,
                    "exit_reason": reason,
                    "ret_pct": ret * 100,
                })
                del pos[sym]
                cool[sym] = t + pd.Timedelta(hours=2)

        if t > ENTRY_END:
            continue

        if t in by and len(pos) < MAX_POSITIONS:
            for _, s in by[t].iterrows():
                sym = s.symbol
                if len(pos) >= MAX_POSITIONS:
                    break
                if sym in pos or (sym in cool and t < cool[sym]):
                    continue
                pos[sym] = {
                    "symbol": sym,
                    "time": t,
                    "entry": float(s.entry),
                    "s1": float(s.s1),
                    "s2": float(s.s2),
                    "r1": float(s.r1),
                    "rr": float(s.rr),
                    "quality": float(s.quality),
                }

    # 마지막 미종료 처리
    for sym, p in pos.items():
        h = b15[sym]
        hh = h[h.index <= FETCH_END]
        if len(hh) == 0:
            continue
        t = hh.index[-1]
        px = float(hh.close.iloc[-1])
        ret = px / p["entry"] - 1
        tr.append({
            **p,
            "exit_time": t,
            "exit_price": px,
            "exit_reason": "END",
            "ret_pct": ret * 100,
        })

    return pd.DataFrame(tr)


def max_loss_streak(T):
    if T.empty:
        return 0
    z = T.sort_values(["time","symbol"])
    cur = best = 0
    for x in z.ret_pct:
        if x < 0:
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best


def summarize(T, variant):
    if T.empty:
        return pd.DataFrame([{"variant":variant, "trades":0}]), pd.DataFrame()

    T = T.sort_values(["time","symbol"]).copy()
    T["date"] = T["time"].dt.date
    T["gross_usdt"] = NOTIONAL * T.ret_pct / 100
    T["fee_scenario_usdt"] = NOTIONAL * (FEE_EACH_SIDE * 2)
    T["net_fee_scenario_usdt"] = T.gross_usdt - T.fee_scenario_usdt
    T["hold_hours"] = (T.exit_time - T.time).dt.total_seconds() / 3600

    w = T[T.ret_pct > 0]
    l = T[T.ret_pct < 0]

    eq = 200 + T.net_fee_scenario_usdt.cumsum()
    peak = eq.cummax()
    dd = eq - peak

    s = pd.DataFrame([{
        "variant": variant,
        "period": "2026-08-01~2026-08-31 KST",
        "source": "Bybit V5 actual 15m OHLCV",
        "max_positions": MAX_POSITIONS,
        "margin_per_slot_usdt": MARGIN_USDT,
        "leverage": LEVERAGE,
        "notional_per_slot_usdt": NOTIONAL,
        "trades": len(T),
        "wins": len(w),
        "losses": len(l),
        "win_rate_pct": 100 * len(w) / len(T),
        "sum_ret_pct": T.ret_pct.sum(),
        "avg_win_pct": w.ret_pct.mean() if len(w) else np.nan,
        "avg_loss_pct": l.ret_pct.mean() if len(l) else np.nan,
        "best_trade_pct": T.ret_pct.max(),
        "worst_trade_pct": T.ret_pct.min(),
        "profit_factor_ret": w.ret_pct.sum() / (-l.ret_pct.sum()) if len(l) and -l.ret_pct.sum() > 0 else np.nan,
        "gross_usdt": T.gross_usdt.sum(),
        "fee_assumption_each_side_pct": FEE_EACH_SIDE * 100,
        "net_fee_scenario_usdt": T.net_fee_scenario_usdt.sum(),
        "ending_equity_from_200_fee_scenario": 200 + T.net_fee_scenario_usdt.sum(),
        "max_closed_equity_dd_usdt_fee_scenario": dd.min(),
        "max_hold_hours": T.hold_hours.max(),
        "avg_hold_hours": T.hold_hours.mean(),
        "max_loss_streak": max_loss_streak(T),
        "stop_count": int((T.exit_reason == "STOP").sum()),
        "target_count": int((T.exit_reason == "R1").sum()),
        "time_count": int((T.exit_reason == "TIME").sum()),
        "end_count": int((T.exit_reason == "END").sum()),
    }])

    daily = T.groupby("date").agg(
        trades=("ret_pct","size"),
        wins=("ret_pct", lambda x: (x > 0).sum()),
        losses=("ret_pct", lambda x: (x < 0).sum()),
        sum_ret_pct=("ret_pct","sum"),
        gross_usdt=("gross_usdt","sum"),
        net_fee_scenario_usdt=("net_fee_scenario_usdt","sum"),
    ).reset_index()

    return s, daily


def segment_summary(T, variant):
    parts = [
        ("AUG_01_15", pd.Timestamp("2026-08-01"), pd.Timestamp("2026-08-15 23:59:59")),
        ("AUG_16_31", pd.Timestamp("2026-08-16"), pd.Timestamp("2026-08-31 23:59:59")),
    ]
    rows = []
    for label, a, b in parts:
        x = T[(T.time >= a) & (T.time <= b)].copy()
        s, _ = summarize(x, variant)
        r = s.iloc[0].to_dict()
        r["segment"] = label
        rows.append(r)
    return rows


def main():
    status = fetch_all()

    print("[2/8] 실제봉 로드", flush=True)
    b15 = load_bars(status)
    btc15 = load_btc()
    print(f"  strategy universe with data={len(b15)}", flush=True)
    if len(b15) < 20:
        raise RuntimeError("8월 실제봉 유효 종목이 너무 적습니다.")

    print("[3/8] 1H/4H 생성", flush=True)
    b1, b4 = build_higher(b15)

    print("[4/8] C형 72H 지지/저항 상태 계산", flush=True)
    states = make_sr_hourly(b1, b4, lookback=72)

    print("[5/8] 15m 반등 신호 + BTC GAP0.50 + Q2 구성", flush=True)
    raw_sig = make_signals(b15, states)
    btc_feat = btc_market_features(btc15)
    sigm = attach_btc_market(raw_sig, btc_feat)
    q2 = prepare_q2_signals(sigm)

    raw_sig.to_csv(OUT / "AUG_C72_RAW_SIGNALS.csv", index=False)
    sigm.to_csv(OUT / "AUG_C72_SIGNALS_WITH_BTC_MARKET.csv", index=False)
    q2.to_csv(OUT / "AUG_Q2_SIGNALS.csv", index=False)

    print(f"  raw C signals={len(raw_sig)}, Q2 signals={len(q2)}", flush=True)

    print("[6/8] Q2_BASE / Q2+RR 실제 4슬롯 재시뮬레이션", flush=True)
    cols = ["time","symbol","entry","s1","s2","r1","rr","quality"]

    T0 = simulate(b15, q2[cols].copy())
    rr = pd.to_numeric(q2["rr"], errors="coerce")
    q2rr = q2.loc[~((rr >= 1.5) & (rr < 2.0)), cols].copy()
    T1 = simulate(b15, q2rr)

    T0.to_csv(OUT / "AUG_Q2_BASE_TRADES.csv", index=False)
    T1.to_csv(OUT / "AUG_Q2_RR_BLOCK_15_20_TRADES.csv", index=False)

    rows = []
    daily_all = []
    segrows = []

    for name, T in [
        ("Q2_BASE", T0),
        ("Q2_RR_BLOCK_15_20", T1),
    ]:
        s, d = summarize(T, name)
        rows.append(s.iloc[0].to_dict())
        if len(d):
            d.insert(0, "variant", name)
            daily_all.append(d)
        segrows.extend(segment_summary(T, name))

    summary = pd.DataFrame(rows)
    daily = pd.concat(daily_all, ignore_index=True) if daily_all else pd.DataFrame()
    segments = pd.DataFrame(segrows)

    # 9월 결과가 서버에 있으면 비교용으로 함께 묶기
    sep_path = Path("/root/hyejin-trader/swing_q2_rr_recheck/S_Q2_RR_RECHECK_SUMMARY.csv")
    compare = pd.DataFrame()
    if sep_path.exists():
        try:
            sep = pd.read_csv(sep_path)
            keep = [c for c in [
                "variant","trades","win_rate_pct","avg_win_pct","avg_loss_pct",
                "profit_factor_ret","net_fee_scenario_usdt",
                "max_closed_equity_dd_usdt_fee_scenario","max_loss_streak",
                "stop_count","target_count","time_count"
            ] if c in sep.columns]
            sep2 = sep[keep].copy()
            sep2.insert(0, "month", "2026-09")

            aug2 = summary[[c for c in keep if c in summary.columns]].copy()
            aug2.insert(0, "month", "2026-08")
            compare = pd.concat([aug2, sep2], ignore_index=True, sort=False)
        except Exception as e:
            print(f"  warning: September comparison skipped: {e}", flush=True)

    summary.to_csv(OUT / "S_AUG_OOS_Q2_RR_SUMMARY.csv", index=False)
    daily.to_csv(OUT / "S_AUG_OOS_Q2_RR_DAILY.csv", index=False)
    segments.to_csv(OUT / "S_AUG_OOS_Q2_RR_SEGMENTS.csv", index=False)
    if len(compare):
        compare.to_csv(OUT / "S_AUG_VS_SEP_COMPARE.csv", index=False)

    readme = OUT / "README_S_AUG_OOS_Q2_RR.txt"
    readme.write_text(
        "2026-08 OOS validation using actual Bybit 15m OHLCV.\n"
        "Rules were frozen from September research; August is not used to tune thresholds.\n"
        "Q2_BASE = C72H + existing per-symbol 4H downtrend guard + BTC weak&gap<=-0.50 block + S1 reclaim>=0.10%.\n"
        "Q2_RR_BLOCK_15_20 additionally blocks 1.5<=RR<2.0.\n"
        "4 slots / 27 USDT x5 / TP=R1 / STOP=S1*0.994 / max hold 24h / cooldown 2h.\n"
        "Same-bar STOP and R1 -> STOP first. Fee scenario=0.055% per side.\n"
        "Universe is the same symbol pool used in the September validation; symbols without August history naturally contribute no trades.\n",
        encoding="utf-8"
    )

    print("[7/8] 결과 ZIP 생성", flush=True)
    zpath = FINAL / "S_AUG_OOS_Q2_RR_ACTUAL15_RESULTS.zip"
    files = [
        OUT / "S_AUG_OOS_Q2_RR_SUMMARY.csv",
        OUT / "S_AUG_OOS_Q2_RR_DAILY.csv",
        OUT / "S_AUG_OOS_Q2_RR_SEGMENTS.csv",
        OUT / "S_AUG_VS_SEP_COMPARE.csv",
        OUT / "AUG_Q2_BASE_TRADES.csv",
        OUT / "AUG_Q2_RR_BLOCK_15_20_TRADES.csv",
        OUT / "AUG_Q2_SIGNALS.csv",
        OUT / "AUG_C72_RAW_SIGNALS.csv",
        OUT / "AUG_C72_SIGNALS_WITH_BTC_MARKET.csv",
        OUT / "S_AUG_OOS_FETCH_STATUS.csv",
        readme,
    ]
    with zipfile.ZipFile(zpath, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in files:
            if p.exists():
                z.write(p, arcname=p.name)

    print("[8/8] 완료", flush=True)
    show = [c for c in [
        "variant","trades","win_rate_pct","avg_win_pct","avg_loss_pct",
        "profit_factor_ret","gross_usdt","net_fee_scenario_usdt",
        "max_closed_equity_dd_usdt_fee_scenario","max_loss_streak",
        "stop_count","target_count","time_count"
    ] if c in summary.columns]
    print(summary[show].to_string(index=False), flush=True)
    print(f"ZIP={zpath}", flush=True)


if __name__ == "__main__":
    main()
