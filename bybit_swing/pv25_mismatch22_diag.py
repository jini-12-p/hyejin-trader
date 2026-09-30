#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnose the 22 V25 mismatches from the 2026-09-28 historical-candle replay.

Read-only:
- latest PV25_CANDLE_REPLAY_0928_*_RESULTS.zip
- bybit_swing_bot.db / research_pv25_setups
- SCAN_FULL_20260928_0000_2359_KST.csv

Outputs:
- mismatch summary
- DB rows around affected symbols/setups
- actual scan V25 confirmation telemetry if present
- compact ZIP
No DB writes, no orders.
"""
from __future__ import annotations

import csv, io, json, sqlite3, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

KST = timezone(timedelta(hours=9))
UTC = timezone.utc

ROOT = Path("/root/hyejin-trader/bybit_swing")
DB = ROOT / "bybit_swing_bot.db"
SCAN = ROOT / "SCAN_FULL_20260928_0000_2359_KST.csv"

def parse_utc(v):
    s = str(v or "").strip()
    if not s or s.lower() == "nan":
        return None
    try:
        d = datetime.fromisoformat(s.replace("Z","+00:00"))
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except Exception:
        return None

def kst_text(v):
    d = parse_utc(v)
    return "" if d is None else d.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")

def latest_replay_zip():
    zs = sorted(ROOT.glob("PV25_CANDLE_REPLAY_0928_*_RESULTS.zip"), key=lambda p:p.stat().st_mtime)
    if not zs:
        raise SystemExit("NO PV25_CANDLE_REPLAY_0928 RESULTS ZIP")
    return zs[-1]

def read_member(z, suffix):
    names = [n for n in z.namelist() if n.endswith(suffix)]
    if not names:
        raise SystemExit("MISSING ZIP MEMBER: "+suffix)
    return pd.read_csv(io.BytesIO(z.read(names[-1])), low_memory=False)

def write_csv(path, rows):
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")

def main():
    for p in [DB, SCAN]:
        if not p.exists():
            raise SystemExit("MISSING: "+str(p))

    zpath = latest_replay_zip()
    with zipfile.ZipFile(zpath) as z:
        actual = read_member(z, "_V25_ACTUAL.csv")
        replay = read_member(z, "_V25_REPLAY.csv")

    A = set(actual["setup_id"].astype(str))
    R = set(replay["setup_id"].astype(str))
    ao = actual[~actual["setup_id"].astype(str).isin(R)].copy()
    ro = replay[~replay["setup_id"].astype(str).isin(A)].copy()

    mismatch_syms = sorted(set(ao["symbol"].astype(str)) | set(ro["symbol"].astype(str)))
    mismatch_ids = sorted(set(ao["setup_id"].astype(str)) | set(ro["setup_id"].astype(str)))

    print("=== V25 MISMATCH DIAGNOSTIC ===", flush=True)
    print("replay_zip =", zpath.name, flush=True)
    print("actual_only =", len(ao), "replay_only =", len(ro), "symbols =", len(mismatch_syms), flush=True)

    # 1) DB: all V25 rows for affected symbols around 9/28, including DROPPED rows.
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    placeholders = ",".join("?" for _ in mismatch_syms)
    q = f"""
        SELECT id,setup_id,symbol,first_seen_at,last_seen_at,confirmed_at,
               confirmed_price,trigger_price,lowest_price,last_price,status,
               last_5m_bucket,expires_at,note,snapshot_json
        FROM research_pv25_setups
        WHERE symbol IN ({placeholders})
          AND first_seen_at >= ?
          AND first_seen_at < ?
        ORDER BY first_seen_at,symbol
    """
    dbrows = [dict(r) for r in con.execute(
        q, (*mismatch_syms, "2026-09-27T14:00:00+00:00", "2026-09-28T15:00:00+00:00")
    ).fetchall()]
    con.close()

    dbout = []
    for r in dbrows:
        snap = {}
        try:
            snap = json.loads(r.get("snapshot_json") or "{}")
        except Exception:
            pass
        dbout.append({
            "setup_id": r.get("setup_id"),
            "symbol": r.get("symbol"),
            "status": r.get("status"),
            "note": r.get("note"),
            "first_seen_kst": kst_text(r.get("first_seen_at")),
            "last_seen_kst": kst_text(r.get("last_seen_at")),
            "confirmed_kst": kst_text(r.get("confirmed_at")),
            "confirmed_price": r.get("confirmed_price"),
            "trigger_price": r.get("trigger_price"),
            "lowest_price": r.get("lowest_price"),
            "last_price": r.get("last_price"),
            "last_5m_bucket": r.get("last_5m_bucket"),
            "expires_kst": kst_text(r.get("expires_at")),
            "snap_p_v22_candidate": snap.get("p_v22_candidate"),
            "snap_p_v2_score": snap.get("p_v2_score"),
            "snap_persistence": snap.get("p_v21_persistence_score"),
            "snap_signal_pass": snap.get("p_v2_signal_pass_count"),
            "snap_live_price": snap.get("live_price"),
        })

    # 2) Scan rows: extract actual V25 telemetry around affected symbols.
    hdr = pd.read_csv(SCAN, nrows=0).columns.tolist()
    wanted = [
        "time_kst","symbol","result","p_v25_setup_id","p_v25_confirm_state",
        "p_v25_5m_bullish","p_v25_prev_high_break","p_v25_closed_5m_price",
        "p_v22_candidate","p_v2_score","p_v21_persistence_score",
        "p_v2_signal_pass_count","live_price","price"
    ]
    use = [c for c in wanted if c in hdr]
    scan_parts = []
    for ch in pd.read_csv(SCAN, usecols=use, chunksize=200_000, low_memory=False):
        if "symbol" not in ch:
            continue
        ch = ch[ch["symbol"].astype(str).isin(mismatch_syms)].copy()
        if "time_kst" in ch:
            t = pd.to_datetime(ch["time_kst"], errors="coerce")
            ch = ch[(t >= pd.Timestamp("2026-09-27 23:30:00")) &
                    (t <  pd.Timestamp("2026-09-29 00:00:00"))]
        if not ch.empty:
            scan_parts.append(ch)
    sc = pd.concat(scan_parts, ignore_index=True) if scan_parts else pd.DataFrame(columns=use)

    # Keep rows that contain V25 state/event info, plus exact mismatch setup IDs.
    if not sc.empty:
        keep = pd.Series(False, index=sc.index)
        if "p_v25_setup_id" in sc:
            keep |= sc["p_v25_setup_id"].astype(str).isin(mismatch_ids)
            keep |= sc["p_v25_setup_id"].fillna("").astype(str).ne("")
        if "result" in sc:
            keep |= sc["result"].fillna("").astype(str).str.contains("P_CONFIRM|P_V25", regex=True)
        sc = sc[keep].copy()
        if "time_kst" in sc:
            sc = sc.sort_values(["time_kst","symbol"])

    # 3) Compact mismatch table with nearest DB row / state.
    dbdf = pd.DataFrame(dbout)
    compact = []
    for src, df in [("ACTUAL_ONLY", ao), ("REPLAY_ONLY", ro)]:
        for _, x in df.iterrows():
            sid = str(x["setup_id"]); sym = str(x["symbol"])
            hit = dbdf[dbdf["setup_id"].astype(str).eq(sid)] if not dbdf.empty else pd.DataFrame()
            row = {
                "class": src,
                "setup_id": sid,
                "symbol": sym,
                "first_seen_kst": x.get("first_seen_kst",""),
                "confirmed_at_kst": x.get("confirmed_at_kst",""),
                "confirmed_price": x.get("confirmed_price",""),
                "db_exact_exists": int(len(hit)>0),
                "db_status": "" if hit.empty else hit.iloc[-1].get("status",""),
                "db_note": "" if hit.empty else hit.iloc[-1].get("note",""),
                "db_last_seen_kst": "" if hit.empty else hit.iloc[-1].get("last_seen_kst",""),
                "db_confirmed_kst": "" if hit.empty else hit.iloc[-1].get("confirmed_kst",""),
            }
            compact.append(row)

    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    prefix = f"PV25_MISMATCH22_DIAG_{stamp}"
    out_compact = ROOT / f"{prefix}_COMPACT.csv"
    out_db = ROOT / f"{prefix}_DB_ROWS.csv"
    out_scan = ROOT / f"{prefix}_SCAN_V25_ROWS.csv"
    out_txt = ROOT / f"{prefix}_SUMMARY.txt"
    out_zip = ROOT / f"{prefix}_RESULTS.zip"

    write_csv(out_compact, compact)
    write_csv(out_db, dbout)
    sc.to_csv(out_scan, index=False, encoding="utf-8-sig")

    summary = [
        "PV25 9/28 MISMATCH-22 DIAGNOSTIC",
        f"source={zpath.name}",
        f"actual_only={len(ao)} replay_only={len(ro)} affected_symbols={len(mismatch_syms)}",
        f"db_rows={len(dbout)} scan_v25_rows={len(sc)}",
        "",
        "Purpose:",
        "Separate warm-start/state-history mismatch from 5m-confirmation mismatch.",
        "No thresholds are changed. No strategy tuning is performed.",
    ]
    out_txt.write_text("\n".join(summary)+"\n", encoding="utf-8")

    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        for p in [out_compact,out_db,out_scan,out_txt]:
            z.write(p, arcname=p.name)

    print("\n".join(summary), flush=True)
    print("RESULT_ZIP="+str(out_zip), flush=True)

if __name__ == "__main__":
    main()
