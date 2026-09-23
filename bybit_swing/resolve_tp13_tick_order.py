#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Resolve TP2.0 + ARM1.5 + PROTECT1.3 material 1-minute ambiguities
using Bybit public historical trade tick archives.

Input:
  TP20_ARM15_P13_COHORT_TRADES.csv

Selects only material ambiguities where WORST_NET != BEST_NET.
These are the trades that can change the final cohort NET.

Bybit linear archive:
  https://public.bybit.com/trading/{SYMBOL}/{SYMBOL}{YYYY-MM-DD}.csv.gz

Outputs:
  TP13_TICK_RESOLUTION_71.csv
  TP13_TICK_EXACT_DAILY.csv
  TP13_TICK_SUMMARY.txt
  TP13_TICK_RESOLUTION_RESULTS.zip

Decision:
  ARM_THEN_P13  -> choose BEST branch (same first ARM bar protection really triggers)
  LOW_FIRST_NO_RETEST -> choose WORST branch (the <=1.3 touch happened before ARM and did not retest)
  SAME_TS_UNRESOLVED / ARCHIVE_MISSING / DATA_MISMATCH -> unresolved range remains

The script never guesses same-timestamp ordering.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import math
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent
INFILE = ROOT / "TP20_ARM15_P13_COHORT_TRADES.csv"
OUT_DETAIL = ROOT / "TP13_TICK_RESOLUTION_71.csv"
OUT_DAILY = ROOT / "TP13_TICK_EXACT_DAILY.csv"
OUT_SUMMARY = ROOT / "TP13_TICK_SUMMARY.txt"
OUT_ZIP = ROOT / "TP13_TICK_RESOLUTION_RESULTS.zip"
CACHE = ROOT / "tp13_tick_minute_cache"
CACHE.mkdir(exist_ok=True)

ARM_PCT = 1.50
P13_PCT = 1.30
UA = "Mozilla/5.0 (compatible; BybitBacktestResearch/1.0)"
UTC = timezone.utc

JSON_TAIL = re.compile(r'(\{"mode".*\})\s*$', re.S)

def pfloat(v, default=None):
    try:
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default

def parse_ts(v: Any) -> float | None:
    try:
        x = float(str(v).strip())
    except Exception:
        return None
    # Normalize common seconds / ms / us / ns encodings.
    ax = abs(x)
    if ax > 1e17:
        x /= 1e9
    elif ax > 1e14:
        x /= 1e6
    elif ax > 1e11:
        x /= 1e3
    return x

def tail_meta(detail: Any) -> dict[str, Any]:
    s = str(detail or "")
    m = JSON_TAIL.search(s)
    if not m:
        # fallback: locate last JSON object starting at {"mode"
        i = s.rfind('{"mode"')
        if i >= 0:
            try:
                return json.loads(s[i:])
            except Exception:
                return {}
        return {}
    try:
        return json.loads(m.group(1))
    except Exception:
        return {}

def write_csv(path: Path, rows: list[dict[str, Any]]):
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    keys = []
    seen = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k); keys.append(k)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader(); w.writerows(rows)

def compact_cache_path(symbol: str, date: str) -> Path:
    return CACHE / f"{symbol}_{date}_targets.json"

def url_for(symbol: str, date: str) -> str:
    return f"https://public.bybit.com/trading/{symbol}/{symbol}{date}.csv.gz"

def stream_target_minutes(symbol: str, date: str, targets: list[dict[str, Any]]) -> tuple[str, dict[str, list[dict[str, Any]]], str]:
    """
    Return (status, ticks_by_setup_id, note).
    Downloads and scans the Bybit daily gzip without retaining the full file.
    A compact cache containing only target-minute trades is saved.
    """
    cp = compact_cache_path(symbol, date)
    target_ids = {str(t["setup_id"]) for t in targets}

    if cp.exists():
        try:
            obj = json.loads(cp.read_text(encoding="utf-8"))
            if set(obj.get("target_ids", [])) == target_ids:
                return obj.get("status", "OK"), obj.get("ticks", {}), obj.get("note", "cache")
        except Exception:
            pass

    windows = []
    for t in targets:
        st = datetime.fromisoformat(t["bar_start_utc"]).timestamp()
        en = datetime.fromisoformat(t["bar_end_utc"]).timestamp()
        windows.append((str(t["setup_id"]), st, en))
    min_start = min(x[1] for x in windows)
    max_end = max(x[2] for x in windows)

    out = {sid: [] for sid, _, _ in windows}
    url = url_for(symbol, date)
    req = urllib.request.Request(url, headers={"User-Agent": UA})

    try:
        resp = urllib.request.urlopen(req, timeout=60)
    except urllib.error.HTTPError as e:
        status = "ARCHIVE_MISSING" if e.code == 404 else f"HTTP_{e.code}"
        obj = {"status": status, "note": str(e), "target_ids": sorted(target_ids), "ticks": out}
        cp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        return status, out, str(e)
    except Exception as e:
        return "DOWNLOAD_ERROR", out, repr(e)

    valid_ts = []
    direction = None
    rows_seen = 0
    hits = 0
    note = ""
    try:
        with resp:
            with gzip.GzipFile(fileobj=resp) as gz:
                txt = io.TextIOWrapper(gz, encoding="utf-8", errors="replace", newline="")
                reader = csv.DictReader(txt)
                fields = [str(x) for x in (reader.fieldnames or [])]
                # Case-insensitive actual header lookup.
                lowmap = {x.lower(): x for x in fields}
                ts_col = lowmap.get("timestamp") or lowmap.get("time") or lowmap.get("ts")
                pr_col = lowmap.get("price") or lowmap.get("execprice")
                side_col = lowmap.get("side")
                id_col = lowmap.get("trdmatchid") or lowmap.get("execid") or lowmap.get("id")
                if not ts_col or not pr_col:
                    raise RuntimeError(f"unexpected columns: {fields}")

                for ordinal, row in enumerate(reader):
                    rows_seen += 1
                    ts = parse_ts(row.get(ts_col))
                    px = pfloat(row.get(pr_col))
                    if ts is None or px is None:
                        continue

                    if len(valid_ts) < 40:
                        valid_ts.append(ts)
                        if len(valid_ts) >= 3 and direction is None:
                            diffs = [b-a for a,b in zip(valid_ts, valid_ts[1:]) if b != a]
                            if diffs:
                                direction = "ASC" if sum(d > 0 for d in diffs) >= sum(d < 0 for d in diffs) else "DESC"

                    # Save only rows inside requested target minute(s).
                    for sid, st, en in windows:
                        if st <= ts < en:
                            out[sid].append({
                                "ts": ts,
                                "price": px,
                                "side": row.get(side_col, "") if side_col else "",
                                "id": row.get(id_col, "") if id_col else "",
                                "ord": ordinal,
                            })
                            hits += 1

                    # Safe early break after monotonic direction is known.
                    if direction == "ASC" and ts >= max_end + 1:
                        break
                    if direction == "DESC" and ts < min_start - 1:
                        break

        note = f"rows_seen={rows_seen}; target_ticks={hits}; direction={direction or 'UNKNOWN'}"
        obj = {"status": "OK", "note": note, "target_ids": sorted(target_ids), "ticks": out}
        cp.write_text(json.dumps(obj, ensure_ascii=False), encoding="utf-8")
        return "OK", out, note
    except Exception as e:
        return "PARSE_ERROR", out, f"{type(e).__name__}: {e}; rows_seen={rows_seen}"

def resolve_one(t: dict[str, Any], ticks: list[dict[str, Any]], source_status: str, source_note: str) -> dict[str, Any]:
    arm_px = float(t["arm_price"])
    p13_px = float(t["p13_price"])

    res = dict(t)
    res.update({
        "archive_status": source_status,
        "archive_note": source_note,
        "tick_count": len(ticks),
        "resolution": "",
        "chosen_branch": "",
        "chosen_net_pct": "",
        "first_arm_ts_utc": "",
        "first_p13_before_ts_utc": "",
        "first_p13_after_ts_utc": "",
        "same_ts_conflict": 0,
    })

    if source_status != "OK":
        res["resolution"] = source_status
        return res
    if not ticks:
        res["resolution"] = "NO_TICKS_IN_MINUTE"
        return res

    # Timestamp order only. Same timestamp with opposite threshold events is intentionally unresolved.
    ticks = sorted(ticks, key=lambda x: (float(x["ts"]), int(x.get("ord", 0))))
    arms = [x for x in ticks if float(x["price"]) >= arm_px]
    if not arms:
        res["resolution"] = "ARM_NOT_FOUND_TICK_MISMATCH"
        return res

    first_arm_ts = min(float(x["ts"]) for x in arms)
    res["first_arm_ts_utc"] = datetime.fromtimestamp(first_arm_ts, UTC).isoformat()

    p13_before = [x for x in ticks if float(x["price"]) <= p13_px and float(x["ts"]) < first_arm_ts]
    p13_same = [x for x in ticks if float(x["price"]) <= p13_px and float(x["ts"]) == first_arm_ts]
    p13_after = [x for x in ticks if float(x["price"]) <= p13_px and float(x["ts"]) > first_arm_ts]

    if p13_before:
        ts0 = min(float(x["ts"]) for x in p13_before)
        res["first_p13_before_ts_utc"] = datetime.fromtimestamp(ts0, UTC).isoformat()

    if p13_after:
        ts1 = min(float(x["ts"]) for x in p13_after)
        res["first_p13_after_ts_utc"] = datetime.fromtimestamp(ts1, UTC).isoformat()
        res["resolution"] = "ARM_THEN_P13"
        res["chosen_branch"] = "BEST"
        res["chosen_net_pct"] = float(t["best_net_pct"])
        return res

    if p13_same:
        res["same_ts_conflict"] = 1
        res["resolution"] = "SAME_TS_UNRESOLVED"
        return res

    # No <=P13 trade after ARM during the first ARM minute.
    # If candle low was <=P13 (which created the ambiguity), that touch necessarily happened before ARM.
    res["resolution"] = "LOW_FIRST_NO_RETEST"
    res["chosen_branch"] = "WORST"
    res["chosen_net_pct"] = float(t["worst_net_pct"])
    return res

def segment_range(df_all: pd.DataFrame, resolved_map: dict[str, dict[str, Any]], start: str, end: str):
    z = df_all[(df_all["entry_time_kst"].astype(str).str[:10] >= start) &
               (df_all["entry_time_kst"].astype(str).str[:10] <= end)].copy()
    lo = hi = exact_fixed = 0.0
    unresolved = 0
    for _, r in z.iterrows():
        sid = str(r["setup_id"])
        w = float(r["worst_net_pct"])
        b = float(r["best_net_pct"])
        if abs(w-b) < 1e-9:
            lo += w; hi += w; exact_fixed += w
            continue
        rr = resolved_map.get(sid, {})
        if rr.get("chosen_branch") in {"BEST","WORST"}:
            v = float(rr["chosen_net_pct"])
            lo += v; hi += v; exact_fixed += v
        else:
            lo += min(w,b); hi += max(w,b); unresolved += 1
    return len(z), lo, hi, unresolved

def main():
    if not INFILE.exists():
        raise SystemExit(f"missing {INFILE}")

    df = pd.read_csv(INFILE)
    for c in ["worst_net_pct","best_net_pct","entry_price"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    material = df[(df["worst_net_pct"] - df["best_net_pct"]).abs() > 1e-9].copy()
    print(f"material ambiguous = {len(material)} (expected 71)", flush=True)

    targets = []
    for _, r in material.iterrows():
        meta = tail_meta(r.get("worst_detail"))
        arm_time_s = meta.get("arm_time")
        if not arm_time_s:
            continue
        arm_end = datetime.fromisoformat(str(arm_time_s))
        if arm_end.tzinfo is None:
            arm_end = arm_end.replace(tzinfo=UTC)
        arm_end = arm_end.astimezone(UTC)
        bar_start = arm_end - timedelta(minutes=1)

        entry = float(r["entry_price"])
        targets.append({
            "setup_id": str(r["setup_id"]),
            "symbol": str(r["symbol"]),
            "entry_time_kst": str(r["entry_time_kst"]),
            "entry_ts_utc": str(r.get("entry_ts_utc","")),
            "entry_price": entry,
            "archive_date_utc": bar_start.date().isoformat(),
            "bar_start_utc": bar_start.isoformat(),
            "bar_end_utc": arm_end.isoformat(),
            "arm_price": entry * (1 + ARM_PCT/100),
            "p13_price": entry * (1 + P13_PCT/100),
            "base_result": str(r.get("base_result","")),
            "base_net_pct": float(r.get("base_net_pct") or 0),
            "worst_result": str(r.get("worst_result","")),
            "worst_net_pct": float(r["worst_net_pct"]),
            "best_result": str(r.get("best_result","")),
            "best_net_pct": float(r["best_net_pct"]),
        })

    groups = defaultdict(list)
    for t in targets:
        groups[(t["symbol"], t["archive_date_utc"])].append(t)

    print(f"archive files = {len(groups)}", flush=True)
    results = []
    for i, ((symbol,date), ts) in enumerate(sorted(groups.items()), 1):
        print(f"[{i}/{len(groups)}] {symbol} {date} targets={len(ts)}", flush=True)
        status, tickmap, note = stream_target_minutes(symbol, date, ts)
        for t in ts:
            rr = resolve_one(t, tickmap.get(t["setup_id"], []), status, note)
            results.append(rr)

    write_csv(OUT_DETAIL, results)

    rmap = {str(r["setup_id"]): r for r in results}
    # Daily exact/range from full 761 cohort.
    days = sorted(df["entry_time_kst"].astype(str).str[:10].unique())
    daily = []
    for day in days:
        n, lo, hi, un = segment_range(df, rmap, day, day)
        base = float(df[df["entry_time_kst"].astype(str).str.startswith(day)]["base_net_pct"].sum())
        daily.append({
            "date": day, "trades": n, "BASE_NET": round(base,6),
            "TICK_NET_LOW": round(lo,6), "TICK_NET_HIGH": round(hi,6),
            "DELTA_LOW": round(lo-base,6), "DELTA_HIGH": round(hi-base,6),
            "unresolved_material": un,
        })
    write_csv(OUT_DAILY, daily)

    cnt = Counter(r["resolution"] for r in results)
    branch = Counter(r["chosen_branch"] for r in results if r.get("chosen_branch"))
    nall, loall, hiall, unall = segment_range(df, rmap, "2026-09-01", "2026-09-22")
    n1, lo1, hi1, un1 = segment_range(df, rmap, "2026-09-01", "2026-09-17")
    n2, lo2, hi2, un2 = segment_range(df, rmap, "2026-09-18", "2026-09-22")
    base_all = float(df["base_net_pct"].sum())
    base1 = float(df[df["entry_time_kst"].astype(str).str[:10] <= "2026-09-17"]["base_net_pct"].sum())
    base2 = float(df[df["entry_time_kst"].astype(str).str[:10] >= "2026-09-18"]["base_net_pct"].sum())

    exact_label = lambda lo,hi,un: f"{lo:.6f}%p" if un==0 else f"{lo:.6f} ~ {hi:.6f}%p"

    lines = [
        "TP2.0 + ARM1.5 + P1.3 TICK ORDER RESOLUTION",
        f"material_ambiguities={len(targets)}",
        f"archive_files={len(groups)}",
        f"resolution_counts={dict(cnt)}",
        f"chosen_branches={dict(branch)}",
        "",
        "[FULL 09/01~09/22]",
        f"BASE_NET={base_all:.6f}%p",
        f"TICK_RESOLVED_NET={exact_label(loall,hiall,unall)}",
        f"DELTA_RANGE={loall-base_all:+.6f} ~ {hiall-base_all:+.6f}%p",
        f"unresolved_material={unall}",
        "",
        "[09/01~09/17]",
        f"BASE_NET={base1:.6f}%p",
        f"TICK_RESOLVED_NET={exact_label(lo1,hi1,un1)}",
        f"DELTA_RANGE={lo1-base1:+.6f} ~ {hi1-base1:+.6f}%p",
        f"unresolved_material={un1}",
        "",
        "[09/18~09/22]",
        f"BASE_NET={base2:.6f}%p",
        f"TICK_RESOLVED_NET={exact_label(lo2,hi2,un2)}",
        f"DELTA_RANGE={lo2-base2:+.6f} ~ {hi2-base2:+.6f}%p",
        f"unresolved_material={un2}",
        "",
        "Interpretation:",
        "ARM_THEN_P13 => +1.5 was reached before a later <=+1.3 trade in that minute; P1.3 protection is real.",
        "LOW_FIRST_NO_RETEST => <=+1.3 occurred before ARM and did not retest after ARM in that minute; use WORST continuation.",
        "SAME_TS_UNRESOLVED => public tick timestamps cannot establish ordering; kept as a range.",
        "ARCHIVE_MISSING => daily archive not published/available yet; rerun later.",
        "",
        f"detail={OUT_DETAIL.name}",
        f"daily={OUT_DAILY.name}",
    ]
    OUT_SUMMARY.write_text("\n".join(lines)+"\n", encoding="utf-8")
    print("\n".join(lines), flush=True)

    with zipfile.ZipFile(OUT_ZIP, "w", zipfile.ZIP_DEFLATED) as z:
        for p in [OUT_DETAIL, OUT_DAILY, OUT_SUMMARY]:
            z.write(p, arcname=p.name)
    print("DONE:", OUT_ZIP, flush=True)

if __name__ == "__main__":
    main()
