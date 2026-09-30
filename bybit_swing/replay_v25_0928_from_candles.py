#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
P V25 historical-candle reproducibility test — 2026-09-28 KST

Purpose
-------
Before extending the current P logic to Jan-Aug 2026, test whether the current
V22 -> V25 candidate engine can be reconstructed from historical Bybit candles.

This PHASE-A test intentionally reuses the *actual scan time/symbol universe*
from SCAN_FULL_20260928_0000_2359_KST.csv. It does NOT reconstruct the dynamic
universe yet. That isolates two questions first:
  1) can current candidate_signal() reproduce stored p_v22_candidate telemetry?
  2) can the V25 WATCH -> confirmed-5m-break state machine reproduce the actual
     research_pv25_setups confirmations?

If this passes reasonably, Phase B can reconstruct the historical dynamic
universe itself and then extend the same engine to Jan-Aug.

No DB writes. No orders. Public Bybit market-data reads only.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import importlib.util
import io
import json
import math
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

KST = timezone(timedelta(hours=9))
UTC = timezone.utc
DAY_START_KST = datetime(2026, 9, 28, 0, 0, 0, tzinfo=KST)
DAY_END_KST = datetime(2026, 9, 29, 0, 0, 0, tzinfo=KST)
DAY_START_UTC = DAY_START_KST.astimezone(UTC)
DAY_END_UTC = DAY_END_KST.astimezone(UTC)


def fnum(v, default=None):
    try:
        if v is None or str(v).strip() == "":
            return default
        x = float(v)
        return default if math.isnan(x) else x
    except Exception:
        return default


def bval(v) -> bool:
    s = str(v).strip().lower()
    return s in {"1", "true", "t", "yes", "y"}


def parse_kst(v: Any) -> datetime | None:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=KST)
        return d.astimezone(KST)
    except Exception:
        try:
            d = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST)
            return d
        except Exception:
            return None


def parse_db_utc(v: Any) -> datetime | None:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except Exception:
        return None


def kst_text(d: datetime | None) -> str:
    return "" if d is None else d.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot import {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def api_rows(symbol: str, interval: str, start: datetime, end: datetime, cache_file: Path) -> pd.DataFrame:
    """Fetch Bybit linear kline history, cached as CSV. [start,end) UTC."""
    if cache_file.exists() and cache_file.stat().st_size > 50:
        df = pd.read_csv(cache_file)
        if not df.empty:
            df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
        return df

    start_ms = int(start.timestamp() * 1000)
    cursor_end = int((end - timedelta(milliseconds=1)).timestamp() * 1000)
    got: dict[int, list[Any]] = {}
    tries_total = 0

    while cursor_end >= start_ms:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "start": start_ms,
            "end": cursor_end,
            "limit": 1000,
        }
        url = "https://api.bybit.com/v5/market/kline?" + urllib.parse.urlencode(params)
        last_exc = None
        payload = None
        for attempt in range(7):
            tries_total += 1
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "HJ-PV25-HIST-REPLAY/1.0"})
                with urllib.request.urlopen(req, timeout=25) as resp:
                    j = json.loads(resp.read().decode("utf-8"))
                if int(j.get("retCode", -1)) != 0:
                    raise RuntimeError(f"{j.get('retCode')} {j.get('retMsg')}")
                payload = j.get("result", {}).get("list", [])
                break
            except Exception as e:
                last_exc = e
                time.sleep(min(7.0, 0.7 * (attempt + 1)))
        if payload is None:
            raise RuntimeError(f"Bybit kline failed {symbol} {interval}: {last_exc}")
        if not payload:
            break

        earliest = None
        for r in payload:
            try:
                ts = int(r[0])
                if start_ms <= ts <= cursor_end:
                    got[ts] = r
                    earliest = ts if earliest is None else min(earliest, ts)
            except Exception:
                pass
        if earliest is None or earliest <= start_ms:
            break
        cursor_end = earliest - 1
        time.sleep(0.035)

    cols = ["ts", "open", "high", "low", "close", "volume", "turnover"]
    rows = [got[k] for k in sorted(got)]
    df = pd.DataFrame(rows, columns=cols if rows else cols)
    if not df.empty:
        for c in ["open", "high", "low", "close", "volume", "turnover"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df["ts"] = pd.to_numeric(df["ts"], errors="coerce").astype("Int64")
        df["start_time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df[(df["start_time"] >= pd.Timestamp(start)) & (df["start_time"] < pd.Timestamp(end))]
        df = df.sort_values("start_time").drop_duplicates("start_time", keep="last").reset_index(drop=True)
    else:
        df["start_time"] = pd.to_datetime(pd.Series([], dtype="datetime64[ns, UTC]"))
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_file, index=False)
    return df


def aggregate_partial(one: pd.DataFrame, start: datetime, now: datetime, minutes: int) -> dict[str, Any] | None:
    # Historical 1m candles are final candles. Including the current minute therefore
    # has up-to-one-minute look-ahead. We expose this explicitly in the report.
    st = pd.Timestamp(start)
    en = pd.Timestamp(now.replace(second=0, microsecond=0) + timedelta(minutes=1))
    z = one[(one["start_time"] >= st) & (one["start_time"] < en)].copy()
    if z.empty:
        return None
    first = z.iloc[0]
    last = z.iloc[-1]
    return {
        "ts": int(pd.Timestamp(start).timestamp() * 1000),
        "open": float(first["open"]),
        "high": float(z["high"].max()),
        "low": float(z["low"].min()),
        "close": float(last["close"]),
        "volume": float(z["volume"].sum()),
        "turnover": float(z["turnover"].sum()),
        "start_time": pd.Timestamp(start),
        "confirm": "1",
    }


@dataclass
class SymbolData:
    one: pd.DataFrame
    m15: pd.DataFrame
    h1: pd.DataFrame


class HistoricalClient:
    def __init__(self, data: dict[str, SymbolData]):
        self.data = data
        self.now: datetime = DAY_START_UTC

    def set_now(self, now: datetime):
        self.now = now.astimezone(UTC)

    def _frame(self, symbol: str, interval: str, limit: int) -> pd.DataFrame:
        sd = self.data[symbol]
        if interval in ("15", "15m"):
            mins, base = 15, sd.m15
        elif interval in ("60", "1H", "1h"):
            mins, base = 60, sd.h1
        elif interval in ("5", "5m"):
            mins, base = 5, None
        else:
            raise ValueError(f"unsupported interval={interval}")

        bucket = self.now.replace(second=0, microsecond=0)
        bucket = bucket - timedelta(minutes=(bucket.minute % mins))

        if mins == 5:
            # Generate closed 5m bars from 1m, plus current partial 5m bar.
            z = sd.one[sd.one["start_time"] < pd.Timestamp(bucket)].copy()
            if z.empty:
                closed = pd.DataFrame(columns=["ts","open","high","low","close","volume","turnover","start_time","confirm"])
            else:
                z = z.set_index("start_time")
                agg = z.resample("5min", label="left", closed="left").agg({
                    "open":"first", "high":"max", "low":"min", "close":"last", "volume":"sum", "turnover":"sum"
                }).dropna(subset=["open","close"]).reset_index()
                # Only complete 5m buckets.
                agg = agg[agg["start_time"] + pd.Timedelta(minutes=5) <= pd.Timestamp(bucket)]
                agg["ts"] = (agg["start_time"].astype("int64") // 1_000_000).astype("int64")
                agg["confirm"] = "1"
                closed = agg[["ts","open","high","low","close","volume","turnover","start_time","confirm"]]
        else:
            closed = base[base["start_time"] < pd.Timestamp(bucket)].copy()
            if not closed.empty:
                closed["confirm"] = "1"

        part = aggregate_partial(sd.one, bucket, self.now, mins)
        frames = [closed]
        if part is not None:
            frames.append(pd.DataFrame([part]))
        out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if out.empty:
            return out
        out = out.sort_values("start_time").drop_duplicates("start_time", keep="last")
        return out.tail(int(limit)).reset_index(drop=True)

    def candles(self, symbol: str, interval: str = "15", limit: int = 200) -> pd.DataFrame:
        return self._frame(symbol, str(interval), int(limit))


@dataclass
class Watch:
    setup_id: str
    symbol: str
    first_seen: datetime
    trigger_price: float
    lowest_price: float
    last_price: float
    status: str = "WATCH"
    last_5m_bucket: str = ""
    confirmed_at: datetime | None = None
    confirmed_price: float | None = None
    drop_reason: str = ""


def load_scan_points(scan: Path) -> pd.DataFrame:
    hdr = pd.read_csv(scan, nrows=0).columns.tolist()
    need = {"time_kst", "symbol"}
    if not need.issubset(hdr):
        raise SystemExit(f"SCAN missing {sorted(need-set(hdr))}")
    use = [c for c in ["time_kst","symbol","p_v22_candidate","live_price","price","p_v2_score"] if c in hdr]
    chunks = []
    for ch in pd.read_csv(scan, usecols=use, chunksize=200_000, low_memory=False):
        t = pd.to_datetime(ch["time_kst"], errors="coerce")
        mask = (t >= pd.Timestamp("2026-09-28 00:00:00")) & (t < pd.Timestamp("2026-09-29 00:00:00"))
        ch = ch.loc[mask].copy()
        if not ch.empty:
            chunks.append(ch)
    if not chunks:
        raise SystemExit("NO 9/28 scan rows")
    df = pd.concat(chunks, ignore_index=True)
    df["time_dt"] = pd.to_datetime(df["time_kst"], errors="coerce")
    df = df[df["time_dt"].notna() & df["symbol"].fillna("").astype(str).ne("")].copy()
    df["symbol"] = df["symbol"].astype(str)
    # merged full-day files can contain duplicate copies; a scan point is time+symbol.
    df = df.sort_values("time_dt").drop_duplicates(["time_kst","symbol"], keep="last").reset_index(drop=True)
    return df


def actual_v25(db_path: Path) -> pd.DataFrame:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute("""
        SELECT setup_id,symbol,first_seen_at,confirmed_at,confirmed_price,status,note
        FROM research_pv25_setups
        WHERE status='CONFIRMED' AND confirmed_at IS NOT NULL
        ORDER BY confirmed_at
    """).fetchall()]
    con.close()
    out = []
    for r in rows:
        ct = parse_db_utc(r.get("confirmed_at"))
        ft = parse_db_utc(r.get("first_seen_at"))
        if ct is None:
            continue
        if DAY_START_UTC <= ct < DAY_END_UTC:
            out.append({
                **r,
                "first_seen_kst": kst_text(ft),
                "confirmed_at_kst": kst_text(ct),
                "confirmed_dt": ct,
            })
    return pd.DataFrame(out)


def greedy_time_match(actual: pd.DataFrame, replay: pd.DataFrame, tol_min: float) -> tuple[int, list[dict[str,Any]]]:
    used = set(); matched = 0; detail = []
    if actual.empty or replay.empty:
        return 0, detail
    rr = replay.reset_index(drop=True)
    for _, a in actual.iterrows():
        at = a["confirmed_dt"]
        sym = str(a["symbol"])
        best = None
        for j, r in rr.iterrows():
            if j in used or str(r["symbol"]) != sym:
                continue
            dtm = abs((r["confirmed_dt"] - at).total_seconds()) / 60.0
            if dtm <= tol_min and (best is None or dtm < best[0]):
                best = (dtm, j, r)
        if best:
            used.add(best[1]); matched += 1
            detail.append({"actual_setup_id":a["setup_id"],"replay_setup_id":best[2]["setup_id"],"symbol":sym,"delta_min":best[0]})
    return matched, detail


def write_csv(path: Path, df: pd.DataFrame):
    df.to_csv(path, index=False, encoding="utf-8-sig")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="/root/hyejin-trader/bybit_swing")
    ap.add_argument("--scan", default="SCAN_FULL_20260928_0000_2359_KST.csv")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    scan = root / args.scan
    bot_path = root / "bot.py"
    db_path = root / "bybit_swing_bot.db"
    for p in [scan, bot_path, db_path]:
        if not p.exists():
            raise SystemExit(f"MISSING: {p}")

    # bot.py imports bybit_swing.bybit_api
    for p in (str(root.parent), str(root)):
        if p not in sys.path:
            sys.path.insert(0, p)
    BOT = load_module("BOT_PV25_HIST_REPLAY", bot_path)
    cfg = BOT.DailyConfig.load()
    cfg.research_shadow_enabled = True

    print("=== PHASE A: 2026-09-28 V22/V25 CANDLE REPLAY ===", flush=True)
    print("Universe/cadence source = actual 9/28 scan time+symbol only", flush=True)
    print("Signal inputs = historical Bybit candles", flush=True)
    print("NOTE: historical final 1m candle proxies each live scan minute (<=1m look-ahead)", flush=True)

    print("[1/6] scan cadence / actual V22 telemetry", flush=True)
    scans = load_scan_points(scan)
    syms = sorted(scans["symbol"].unique().tolist())
    print(f"scan points={len(scans)} unique symbols={len(syms)}", flush=True)

    print("[2/6] actual V25 ground truth", flush=True)
    av25 = actual_v25(db_path)
    print(f"actual confirmed V25={len(av25)}", flush=True)

    print("[3/6] download/cache historical candles", flush=True)
    cache_root = root / ".pv25_hist_replay_0928_cache_v1"
    cache_root.mkdir(exist_ok=True)
    data: dict[str, SymbolData] = {}
    fetch_errors = []
    one_start = DAY_START_UTC - timedelta(hours=1)
    # 220x15m ~= 55h; give extra history. 140x1H ~= 140h.
    m15_start = DAY_START_UTC - timedelta(hours=65)
    h1_start = DAY_START_UTC - timedelta(hours=160)
    for i, sym in enumerate(syms, 1):
        try:
            safe = "".join(c for c in sym if c.isalnum() or c in "_-.")
            one = api_rows(sym,"1",one_start,DAY_END_UTC,cache_root/f"{safe}_1m.csv")
            m15 = api_rows(sym,"15",m15_start,DAY_END_UTC,cache_root/f"{safe}_15m.csv")
            h1 = api_rows(sym,"60",h1_start,DAY_END_UTC,cache_root/f"{safe}_60m.csv")
            if one.empty or m15.empty or h1.empty:
                raise RuntimeError(f"empty data 1m={len(one)} 15m={len(m15)} 60m={len(h1)}")
            data[sym] = SymbolData(one=one,m15=m15,h1=h1)
        except Exception as e:
            fetch_errors.append({"symbol":sym,"error":str(e)})
        if i % 10 == 0 or i == len(syms):
            print(f"  candles {i}/{len(syms)} ok={len(data)} errors={len(fetch_errors)}", flush=True)
    if not data:
        raise SystemExit("NO CANDLE DATA")

    print("[4/6] replay exact bot candidate_signal on scan points", flush=True)
    hc = HistoricalClient(data)
    watches: dict[str, Watch] = {}
    last_setup_by_symbol: dict[str, Watch] = {}
    replay_conf = []
    cmp_rows = []
    signal_errors = []

    # Replay in chronological order. Scan timestamps are KST text without timezone.
    scans = scans.sort_values(["time_dt","symbol"]).reset_index(drop=True)
    for idx, r in scans.iterrows():
        sym = str(r["symbol"])
        if sym not in data:
            continue
        t_kst = r["time_dt"].to_pydatetime().replace(tzinfo=KST)
        now = t_kst.astimezone(UTC)
        hc.set_now(now)
        try:
            strategy, score, d = BOT.candidate_signal(hc, sym, cfg)
            replay_v22 = bool(d.get("p_v22_candidate"))
            # candidate_signal creates p_setup_id using wall clock; historical replay must use historical bucket.
            bucket = int(now.timestamp()) // 900
            d["p_setup_id"] = f"P65SET-{sym}-{bucket}"
            live_price = float(d.get("live_price") or d.get("price") or 0.0)
        except Exception as e:
            signal_errors.append({"time_kst":str(r["time_kst"]),"symbol":sym,"error":str(e)})
            continue

        actual_v22 = bval(r.get("p_v22_candidate")) if "p_v22_candidate" in scans.columns else None
        actual_live = fnum(r.get("live_price"), fnum(r.get("price")))
        cmp_rows.append({
            "time_kst": str(r["time_kst"]),
            "symbol": sym,
            "actual_v22": "" if actual_v22 is None else int(actual_v22),
            "replay_v22": int(replay_v22),
            "v22_match": "" if actual_v22 is None else int(actual_v22 == replay_v22),
            "actual_live_price": actual_live,
            "replay_live_price": live_price,
            "live_abs_err_pct": "" if not actual_live else abs(live_price/actual_live-1)*100,
            "replay_p_v2_score": fnum(d.get("p_v2_score")),
            "replay_persistence": fnum(d.get("p_v21_persistence_score")),
            "replay_signal_pass": fnum(d.get("p_v2_signal_pass_count")),
        })

        # Exact V25 state-machine semantics, without DB writes.
        recent = last_setup_by_symbol.get(sym)
        if recent is not None and recent.first_seen < now - timedelta(minutes=max(1,int(cfg.research_pv25_same_setup_lock_minutes))):
            recent = None

        if recent is None:
            if replay_v22 and live_price > 0:
                sid = f"P25SET-{sym}-{bucket}"
                w = Watch(sid,sym,now,live_price,live_price,live_price)
                watches[sid] = w
                last_setup_by_symbol[sym] = w
            continue

        if recent.status != "WATCH" or live_price <= 0:
            continue

        low = min(recent.lowest_price, live_price)
        recent.lowest_price = low
        recent.last_price = live_price
        age = (now - recent.first_seen).total_seconds()/60.0
        adverse = (low/recent.trigger_price-1)*100 if recent.trigger_price>0 else 0.0
        if age > float(cfg.research_pv25_confirm_max_minutes) or adverse <= -abs(float(cfg.research_pv25_max_adverse_before_confirm_pct)):
            recent.status="DROPPED"; recent.drop_reason="expired_or_adverse"
            continue

        current_5m_bucket = str(int(now.timestamp())//300)
        if recent.last_5m_bucket == current_5m_bucket:
            continue
        recent.last_5m_bucket = current_5m_bucket
        if age < float(cfg.research_pv25_confirm_min_minutes):
            continue
        try:
            m5 = BOT.confirmed(BOT.indicators(hc.candles(sym,"5m",8)))
            if len(m5)<2:
                continue
            row5, prev5 = m5.iloc[-1],m5.iloc[-2]
            five_bullish = float(row5.close)>float(row5.open)
            high_break = float(row5.close)>float(prev5.high)
        except Exception as e:
            signal_errors.append({"time_kst":str(r["time_kst"]),"symbol":sym,"error":"V25_5M: "+str(e)})
            continue
        if five_bullish and high_break:
            recent.status="CONFIRMED"
            recent.confirmed_at=now
            recent.confirmed_price=live_price
            replay_conf.append({
                "setup_id":recent.setup_id,"symbol":sym,
                "first_seen_kst":kst_text(recent.first_seen),
                "confirmed_at_kst":kst_text(now),
                "confirmed_price":live_price,
                "trigger_price":recent.trigger_price,
                "lowest_price":recent.lowest_price,
                "confirmed_dt":now,
            })

        if (idx+1)%5000==0:
            print(f"  replay {idx+1}/{len(scans)} confirmations={len(replay_conf)}", flush=True)

    cmpdf = pd.DataFrame(cmp_rows)
    rv25 = pd.DataFrame(replay_conf)

    print("[5/6] compare", flush=True)
    # V22 telemetry metrics
    if "actual_v22" in cmpdf.columns and not cmpdf.empty:
        known = cmpdf[cmpdf["actual_v22"].astype(str).ne("")].copy()
        if not known.empty:
            known["actual_v22"] = pd.to_numeric(known["actual_v22"],errors="coerce").fillna(0).astype(int)
            known["replay_v22"] = pd.to_numeric(known["replay_v22"],errors="coerce").fillna(0).astype(int)
            tp = int(((known.actual_v22==1)&(known.replay_v22==1)).sum())
            fn = int(((known.actual_v22==1)&(known.replay_v22==0)).sum())
            fp = int(((known.actual_v22==0)&(known.replay_v22==1)).sum())
            tn = int(((known.actual_v22==0)&(known.replay_v22==0)).sum())
            recall = tp/(tp+fn) if tp+fn else None
            precision = tp/(tp+fp) if tp+fp else None
            acc = (tp+tn)/len(known) if len(known) else None
        else:
            tp=fn=fp=tn=0; recall=precision=acc=None
    else:
        tp=fn=fp=tn=0; recall=precision=acc=None

    actual_ids = set(av25["setup_id"].astype(str)) if not av25.empty else set()
    replay_ids = set(rv25["setup_id"].astype(str)) if not rv25.empty else set()
    exact_ids = actual_ids & replay_ids
    actual_only = actual_ids - replay_ids
    replay_only = replay_ids - actual_ids

    # Setup-ID exact is strongest. Time-tolerance is a secondary diagnostic if first scan minute differs.
    m2, d2 = greedy_time_match(av25, rv25, 2.0) if not rv25.empty else (0,[])
    m5, d5 = greedy_time_match(av25, rv25, 5.0) if not rv25.empty else (0,[])

    # Price error for exact setup matches
    price_err=[]
    if exact_ids:
        amap={str(r["setup_id"]):r for _,r in av25.iterrows()}
        rmap={str(r["setup_id"]):r for _,r in rv25.iterrows()}
        for sid in exact_ids:
            ap=fnum(amap[sid].get("confirmed_price")); rp=fnum(rmap[sid].get("confirmed_price"))
            if ap and rp:
                price_err.append(abs(rp/ap-1)*100)

    stamp=datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    pref=f"PV25_CANDLE_REPLAY_0928_{stamp}"
    out_cmp=root/f"{pref}_V22_COMPARE.csv"
    out_rep=root/f"{pref}_V25_REPLAY.csv"
    out_act=root/f"{pref}_V25_ACTUAL.csv"
    out_err=root/f"{pref}_ERRORS.csv"
    out_match=root/f"{pref}_TIME_MATCH_5M.csv"
    out_sum=root/f"{pref}_SUMMARY.txt"
    out_zip=root/f"{pref}_RESULTS.zip"

    write_csv(out_cmp,cmpdf)
    repout=rv25.drop(columns=["confirmed_dt"],errors="ignore")
    actout=av25.drop(columns=["confirmed_dt"],errors="ignore")
    write_csv(out_rep,repout)
    write_csv(out_act,actout)
    write_csv(out_err,pd.DataFrame(fetch_errors+signal_errors))
    write_csv(out_match,pd.DataFrame(d5))

    lines=[
        "P V25 HISTORICAL-CANDLE REPLAY — 2026-09-28 KST — PHASE A",
        "",
        "[SCOPE]",
        "Actual 9/28 scan time+symbol pairs are reused; dynamic-universe reconstruction is NOT tested yet.",
        "candidate_signal() is executed from current bot.py using historical Bybit candles.",
        "Historical final 1m candle proxies each live scan minute, so <=1 minute intrabar look-ahead remains.",
        "",
        "[INPUT]",
        f"scan_points={len(scans)}",
        f"unique_symbols={len(syms)}",
        f"symbols_with_candles={len(data)}",
        f"fetch_errors={len(fetch_errors)}",
        f"signal_errors={len(signal_errors)}",
        f"actual_V25_confirmed={len(av25)}",
        "",
        "[V22 TELEMETRY REPRODUCTION]",
        f"TP={tp} FN={fn} FP={fp} TN={tn}",
        f"recall={'' if recall is None else f'{recall:.6f}'}",
        f"precision={'' if precision is None else f'{precision:.6f}'}",
        f"accuracy={'' if acc is None else f'{acc:.6f}'}",
        "",
        "[V25 CONFIRM REPRODUCTION]",
        f"replay_confirmed={len(rv25)}",
        f"exact_setup_id_matches={len(exact_ids)}",
        f"actual_only_setup_ids={len(actual_only)}",
        f"replay_only_setup_ids={len(replay_only)}",
        f"same_symbol_time_match_within_2m={m2}/{len(av25)}",
        f"same_symbol_time_match_within_5m={m5}/{len(av25)}",
        f"exact_match_price_abs_error_pct_median={'' if not price_err else f'{pd.Series(price_err).median():.6f}'}",
        f"exact_match_price_abs_error_pct_max={'' if not price_err else f'{max(price_err):.6f}'}",
        "",
        "[INTERPRETATION]",
        "Do NOT extend to Jan-Aug based on candidate count alone.",
        "First inspect V22 recall/precision, V25 setup/time matching, and mismatch clusters.",
        "If Phase A is acceptable, Phase B must separately reconstruct the dynamic 15-symbol universe from historical market data.",
        "",
        "[SOURCE]",
        f"bot.py sha256={sha256_file(bot_path)}",
        f"scan={scan.name} sha256={sha256_file(scan)}",
    ]
    out_sum.write_text("\n".join(lines)+"\n",encoding="utf-8")
    with zipfile.ZipFile(out_zip,"w",zipfile.ZIP_DEFLATED) as z:
        for p in [out_cmp,out_rep,out_act,out_err,out_match,out_sum]:
            z.write(p,arcname=p.name)

    print("[6/6] result", flush=True)
    print("\n".join(lines), flush=True)
    print("RESULT_ZIP="+str(out_zip), flush=True)

if __name__=="__main__":
    main()
