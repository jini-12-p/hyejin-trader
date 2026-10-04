#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
C6(frozen validated IDs) + C7 + C8 + C9 + C10 + C11
EXACT scheduler replay with replacement entries, Jan-Sep 2026.

Scenarios:
  S7  = C1~C7 baseline
  S8  = S7 + C8
  S9  = S8 + C9
  S10 = S9 + C10
  S11 = S10 + C11

Safety:
- no bot.py modification
- no DB writes
- no orders
"""

from pathlib import Path
from collections import Counter
import runpy, sqlite3, json, math, zipfile, gzip, io
import pandas as pd

R = Path("/root/hyejin-trader/bybit_swing")

BASE_SCRIPT = R / "breadth_persistence_diag.py"
if not BASE_SCRIPT.exists():
    BASE_SCRIPT = Path("/tmp/breadth_persistence_diag.py")
if not BASE_SCRIPT.exists():
    raise SystemExit("MISSING breadth_persistence_diag.py")

print("=== LOAD VALIDATED C5 ENGINE ===", flush=True)
ns = runpy.run_path(str(BASE_SCRIPT))
U = ns["U"]
ja = ns["ja"].copy()
se = ns["se"].copy()
pre = ns["pre"]
blocked = ns["blocked"]
getsim = ns["getsim"]

def fv(v, d=None):
    try:
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

# ------------------------------------------------------------------
# C6 = original validated frozen direct-target set.
# Jan-Aug 18 + Sep SAGA 1.
# ------------------------------------------------------------------
C6_IDS = {
    "P25SET-AKEUSDT-1964292",
    "P25SET-AGLDUSDT-1968650",
    "P25SET-SIGNUSDT-1969756",
    "P25SET-KITEUSDT-1969768",
    "P25SET-XANUSDT-1971016",
    "P25SET-TRIAUSDT-1971329",
    "P25SET-STOUSDT-1971719",
    "P25SET-ZBTUSDT-1974617",
    "P25SET-BSBUSDT-1979575",
    "P25SET-EPICUSDT-1981325",
    "P25SET-LDOUSDT-1981649",
    "P25SET-BILLUSDT-1982088",
    "P25SET-FHEUSDT-1982101",
    "P25SET-JCTUSDT-1982161",
    "P25SET-REUSDT-1983051",
    "P25SET-BILLUSDT-1983199",
    "P25SET-REUSDT-1983199",
    "P25SET-CAPUSDT-1983218",
    "P25SET-SAGAUSDT-1988348",
}
if len(C6_IDS) != 19:
    raise SystemExit(f"C6 ID COUNT FAIL: {len(C6_IDS)}")

# ------------------------------------------------------------------
# Load exact candidate feature snapshots.
# ------------------------------------------------------------------
packs = sorted(
    R.glob("P_RESEARCH_PACK_*.zip"),
    key=lambda p: p.stat().st_mtime,
    reverse=True,
)
if not packs:
    raise SystemExit("MISSING P_RESEARCH_PACK_*.zip")
PACK = packs[0]
print("RESEARCH PACK:", PACK, flush=True)

feat = {}

def add_ja_features(d, source):
    for r in d.to_dict("records"):
        sid = str(r.get("setup_id") or "")
        if not sid:
            continue

        w = jl(r.get("watch_details_json"))
        q = jl(r.get("details_json"))

        ep = fv(r.get("entry_price"))
        trig = fv(r.get("trigger_price"))
        low = fv(r.get("lowest_price_before_confirm"))

        if trig is None:
            trig = fv(q.get("price"))

        watch_score = fv(r.get("watch_p_v2_score"))
        if watch_score is None:
            watch_score = fv(w.get("p_v2_score"))

        watch_btc15 = fv(r.get("watch_btc15"))
        if watch_btc15 is None:
            watch_btc15 = fv(w.get("btc_15m_change_pct"))

        feat[sid] = dict(
            # C7
            rsi_delta=fv(q.get("rsi_delta")),
            live_gain=fv(q.get("live_candle_gain_pct")),
            gap=fv(q.get("ema9_ema20_gap_pct")),
            slope=fv(q.get("ema9_slope_prev1_pct")),
            entry=ep,
            trigger=trig,

            # C8~C11
            watch_score=watch_score,
            final_score=fv(q.get("p_v2_score")),
            rsi=fv(q.get("rsi")),
            watch_btc15=watch_btc15,
            final_btc15=fv(q.get("btc_15m_change_pct")),
            entry_from_low=(
                (ep / low - 1.0) * 100.0
                if ep is not None and low is not None and low > 0
                else None
            ),
            eth4h=fv(q.get("eth_4h_change_pct")),
            pullback=fv(q.get("pullback_from_high_pct")),
            ema9_prev1=fv(q.get("ema9_slope_prev1_pct")),
            source=source,
        )

loaded = 0
with zipfile.ZipFile(PACK) as z:
    names = set(z.namelist())
    for m in range(1, 9):
        name = f"2026-{m:02d}_CANDIDATES.csv.gz"
        if name not in names:
            continue
        raw = z.read(name)
        d = pd.read_csv(
            gzip.GzipFile(fileobj=io.BytesIO(raw)),
            low_memory=False,
        )
        add_ja_features(d, "PACK:" + name)
        loaded += 1

if loaded != 8:
    raise SystemExit(f"JAN-AUG FEATURE FILE COUNT FAIL: {loaded}/8")

# September exact confirmed snapshot from DB.
db = R / "bybit_swing_bot.db"
con = sqlite3.connect(db)
con.row_factory = sqlite3.Row
try:
    dbrows = con.execute(
        "SELECT * FROM research_pv25_setups "
        "WHERE confirmed_at IS NOT NULL"
    ).fetchall()
finally:
    con.close()

se_lookup = {
    str(r["setup_id"]): r
    for _, r in se.iterrows()
}

for rr in dbrows:
    r = dict(rr)
    sid = str(r.get("setup_id") or "")
    sr = se_lookup.get(sid)
    if sr is None:
        continue

    q = jl(r.get("snapshot_json"))
    ep = fv(r.get("confirmed_price"))
    trig = fv(r.get("trigger_price"))
    low = fv(r.get("lowest_price"))

    feat[sid] = dict(
        # C7
        rsi_delta=fv(q.get("rsi_delta")),
        live_gain=fv(q.get("live_candle_gain_pct")),
        gap=fv(q.get("ema9_ema20_gap_pct")),
        slope=fv(q.get("ema9_slope_prev1_pct")),
        entry=ep,
        trigger=(trig if trig is not None else fv(q.get("price"))),

        # C8~C11
        watch_score=fv(sr.get("v22_score")),
        final_score=fv(q.get("p_v2_score")),
        rsi=fv(q.get("rsi")),
        watch_btc15=fv(sr.get("v22_btc15")),
        final_btc15=fv(q.get("btc_15m_change_pct")),
        entry_from_low=(
            (ep / low - 1.0) * 100.0
            if ep is not None and low is not None and low > 0
            else None
        ),
        eth4h=fv(q.get("eth_4h_change_pct")),
        pullback=fv(q.get("pullback_from_high_pct")),
        ema9_prev1=fv(q.get("ema9_slope_prev1_pct")),
        source="SEP_DB",
    )

# ------------------------------------------------------------------
# Row-level market features.
# ------------------------------------------------------------------
def market_info(r, period):
    if period == "JA":
        return dict(
            eth24=fv(r.get("eth24_pct")),
            btc48=fv(r.get("btc48_pct")),
            abs4=fv(r.get("abs4h_avg")),
            v25=fv(r.get("v25_2h_count")),
            shadow=fv(r.get("market_shadow_sum")),
        )
    return dict(
        eth24=fv(r.get("downsel_eth24")),
        btc48=fv(r.get("downsel_btc48")),
        abs4=fv(r.get("abs4h_avg")),
        v25=fv(r.get("v25_2h_count")),
        shadow=fv(r.get("market_shadow_sum")),
    )

# ------------------------------------------------------------------
# Guards
# ------------------------------------------------------------------
def c7_info(sid):
    q = feat.get(str(sid)) or {}
    vals = [
        q.get("rsi_delta"), q.get("live_gain"), q.get("gap"),
        q.get("slope"), q.get("entry"), q.get("trigger"),
    ]
    complete = (
        all(v is not None for v in vals)
        and (q.get("trigger") or 0) > 0
    )
    jump = (
        (q["entry"] / q["trigger"] - 1.0) * 100.0
        if complete else None
    )
    hit = bool(
        complete
        and q["rsi_delta"] <= 1.5
        and q["live_gain"] >= 0.5
        and q["gap"] <= 1.0
        and q["slope"] >= 0.3
        and jump >= 0.5
    )
    return dict(
        hit=hit,
        complete=complete,
        entry_vs_trigger_pct=jump,
        **q,
    )

def c8_info(sid, r, period):
    q = feat.get(str(sid)) or {}
    m = market_info(r, period)

    watch = q.get("watch_score")
    final = q.get("final_score")
    live = q.get("live_gain")
    rsi = q.get("rsi")
    wb = q.get("watch_btc15")
    fb = q.get("final_btc15")
    lowjump = q.get("entry_from_low")

    eth24 = m.get("eth24")
    btc48 = m.get("btc48")
    abs4 = m.get("abs4")
    v25 = m.get("v25")

    score_delta = (
        final - watch
        if final is not None and watch is not None
        else None
    )
    btc15_delta = (
        fb - wb
        if fb is not None and wb is not None
        else None
    )

    # C8 market regime M
    regime_m = bool(
        (
            abs4 is not None and v25 is not None
            and abs4 >= 0.30 and v25 <= 5
        )
        or
        (
            eth24 is not None and v25 is not None
            and eth24 >= 0.75 and v25 <= 8
        )
    )

    # C8-A refined
    a = bool(
        regime_m
        and watch is not None and watch >= 87.7
        and score_delta is not None and score_delta <= -4.0
        and final is not None and final < 95
        and lowjump is not None and lowjump >= 0.25
        and btc48 is not None
        and (
            btc48 >= -2.0
            or (
                btc48 < -2.0
                and rsi is not None and rsi >= 68
                and live is not None and live < 2.0
            )
        )
    )

    # C8-B refined
    b1 = bool(
        live is not None and live >= 1.25
        and rsi is not None and rsi >= 78
        and abs4 is not None and abs4 >= 0.18
    )
    b2 = bool(
        btc15_delta is not None and btc15_delta <= -0.22
        and lowjump is not None and lowjump <= 0.28
        and abs4 is not None and abs4 >= 0.30
        and watch is not None and watch < 87
    )
    b = bool(regime_m and (b1 or b2))

    return dict(
        hit=bool(a or b),
        A=a, B=b, B1=b1, B2=b2, M=regime_m,
        score_delta=score_delta,
        btc15_delta=btc15_delta,
        **q, **m,
    )

def c9_info(sid, r, period):
    q = feat.get(str(sid)) or {}
    m = market_info(r, period)
    hit = bool(
        q.get("eth4h") is not None and q["eth4h"] >= 0.92
        and m.get("abs4") is not None and m["abs4"] <= 0.82
        and q.get("rsi") is not None and q["rsi"] >= 65
    )
    return dict(hit=hit, **q, **m)

def c10_info(sid, r, period):
    q = feat.get(str(sid)) or {}
    m = market_info(r, period)

    # C10-A: deep 48h BTC decline + weak rebound
    a = bool(
        m.get("btc48") is not None and m["btc48"] <= -7.0
        and q.get("final_score") is not None and q["final_score"] <= 85
        and m.get("abs4") is not None and m["abs4"] <= 0.40
    )

    # C10-B: rebound exhaustion inside weak market
    b = bool(
        m.get("btc48") is not None and m["btc48"] >= 1.0
        and m.get("v25") is not None and m["v25"] >= 8
        and m.get("shadow") is not None and m["shadow"] <= -6
        and q.get("pullback") is not None and q["pullback"] <= 1.5
        and q.get("ema9_prev1") is not None and q["ema9_prev1"] <= 0.20
        and q.get("final_score") is not None and q["final_score"] < 95
    )

    return dict(hit=bool(a or b), A=a, B=b, **q, **m)

def c11_info(sid, r, period):
    m = market_info(r, period)
    hit = bool(
        m.get("v25") is not None and m["v25"] >= 17
        and m.get("shadow") is not None and m["shadow"] <= -9
    )
    return dict(hit=hit, **m)

# ------------------------------------------------------------------
# Hard C7 feature audit for Jan-Aug.
# ------------------------------------------------------------------
ja_ids = set(ja.setup_id.astype(str))
miss_c7 = sorted(
    sid for sid in ja_ids
    if not c7_info(sid)["complete"]
)
print("C7 JA FEATURE AUDIT missing", len(miss_c7), flush=True)
if miss_c7:
    print("SAMPLE", miss_c7[:20], flush=True)
    raise SystemExit("C7 JA FEATURE AUDIT FAIL")

# ------------------------------------------------------------------
# Exact scheduler replay.
# level=7/8/9/10/11
# ------------------------------------------------------------------
def run_stack(d, period, level):
    sched = U.Scheduler()
    trades = []
    blocks = []

    for _, r in d.iterrows():
        if not pre(r, period) or blocked(r, period):
            continue

        sid = str(r["setup_id"])
        sym = str(r["symbol"])
        now = (
            pd.Timestamp(r["_dt"]).to_pydatetime()
            if period == "JA"
            else r["_dt"]
        )
        month = str(r["entry_time_kst"])[:7]
        day = str(r["entry_time_kst"])[:10]

        if sid in C6_IDS:
            blocks.append(dict(
                period=period, setup_id=sid, symbol=sym,
                month=month, day=day, guard="C6"
            ))
            continue

        if c7_info(sid)["hit"]:
            blocks.append(dict(
                period=period, setup_id=sid, symbol=sym,
                month=month, day=day, guard="C7"
            ))
            continue

        if level >= 8:
            g = c8_info(sid, r, period)
            if g["hit"]:
                blocks.append(dict(
                    period=period, setup_id=sid, symbol=sym,
                    month=month, day=day, guard="C8",
                    subtype=("A" if g["A"] else "B"),
                ))
                continue

        if level >= 9:
            g = c9_info(sid, r, period)
            if g["hit"]:
                blocks.append(dict(
                    period=period, setup_id=sid, symbol=sym,
                    month=month, day=day, guard="C9"
                ))
                continue

        if level >= 10:
            g = c10_info(sid, r, period)
            if g["hit"]:
                blocks.append(dict(
                    period=period, setup_id=sid, symbol=sym,
                    month=month, day=day, guard="C10",
                    subtype=("A" if g["A"] else "B"),
                ))
                continue

        if level >= 11:
            g = c11_info(sid, r, period)
            if g["hit"]:
                blocks.append(dict(
                    period=period, setup_id=sid, symbol=sym,
                    month=month, day=day, guard="C11"
                ))
                continue

        ok, _ = sched.can_open(now, sym)
        if not ok:
            continue

        s = getsim(r, period)
        sched.add(
            now, sym, s,
            s.result in ("STOP", "LATE_FAILURE_EXIT")
        )

        trades.append(dict(
            period=period,
            setup_id=sid,
            symbol=sym,
            month=month,
            day=day,
            entry_dt=pd.Timestamp(now),
            result=str(s.result),
            net_pct=float(s.net_pct),
        ))

    return pd.DataFrame(trades), pd.DataFrame(blocks)

def run_both(level):
    a, ab = run_stack(ja, "JA", level)
    b, bb = run_stack(se, "SE", level)
    return (
        pd.concat([a, b], ignore_index=True),
        pd.concat([ab, bb], ignore_index=True),
    )

def summarize(x, label):
    rows = []
    for m in [f"2026-{i:02d}" for i in range(1, 10)]:
        g = x[x.month == m]
        rc = Counter(g.result.astype(str))
        rows.append(dict(
            scenario=label,
            month=m,
            N=len(g),
            TP=rc["TP20_FULL"],
            STOP=rc["STOP"],
            PP=rc["PROFIT_PROTECT_EXIT"],
            TIME=rc["TIME_EXIT"],
            LATE=rc["LATE_FAILURE_EXIT"],
            NET=float(g.net_pct.sum()),
        ))
    return pd.DataFrame(rows)

# ------------------------------------------------------------------
# Baseline exact audit: C6+C7 must exactly reproduce the accepted result.
# ------------------------------------------------------------------
print("\n=== RUN S7 = C6+C7 EXACT BASELINE ===", flush=True)
t7, b7 = run_both(7)
s7 = summarize(t7, "S7_C6_C7")

EXPECTED_C7 = {
    "2026-01": (758, 373, 261, -28.563236),
    "2026-02": (467, 228, 151, -44.926877),
    "2026-03": (601, 286, 207, -75.673168),
    "2026-04": (732, 389, 223,  34.220388),
    "2026-05": (753, 398, 234,  66.176575),
    "2026-06": (542, 255, 205, -106.886033),
    "2026-07": (620, 282, 212, -103.414786),
    "2026-08": (742, 360, 248, -61.957130),
    "2026-09": (686, 399, 185, 192.784161),
}

bad = []
for r in s7.itertuples(index=False):
    e = EXPECTED_C7[r.month]
    ok = (
        r.N == e[0]
        and r.TP == e[1]
        and r.STOP == e[2]
        and abs(r.NET - e[3]) <= 0.02
    )
    print(
        r.month,
        "N", r.N, "TP", r.TP, "STOP", r.STOP,
        "NET", round(r.NET, 3),
        "AUDIT", ok,
        flush=True,
    )
    if not ok:
        bad.append(r.month)

if bad:
    raise SystemExit(
        "C6+C7 BASELINE AUDIT FAIL: " + ",".join(bad)
    )
print("C6+C7 BASELINE AUDIT = PASS", flush=True)

# ------------------------------------------------------------------
# Direct-condition audits on the exact Jan-Aug S7 accepted path.
# These make sure the intended C8~C11 conditions are exactly the ones
# researched before the replacement scheduler runs.
# ------------------------------------------------------------------
ja_row = {
    str(r["setup_id"]): r
    for _, r in ja.iterrows()
}
t7ja = t7[t7.period == "JA"].copy()

direct_rows = []
for tr in t7ja.to_dict("records"):
    sid = str(tr["setup_id"])
    r = ja_row.get(sid)
    if r is None:
        continue

    if c8_info(sid, r, "JA")["hit"]:
        guard = "C8"
    elif c9_info(sid, r, "JA")["hit"]:
        guard = "C9"
    elif c10_info(sid, r, "JA")["hit"]:
        guard = "C10"
    elif c11_info(sid, r, "JA")["hit"]:
        guard = "C11"
    else:
        continue

    direct_rows.append(dict(
        guard=guard,
        setup_id=sid,
        symbol=tr["symbol"],
        month=tr["month"],
        result=tr["result"],
        net_pct=float(tr["net_pct"]),
    ))

direct = pd.DataFrame(direct_rows)

EXPECTED_DIRECT = {
    "C8":  (50, 4, 30, -83.47045567330528),
    "C9":  (40, 8, 22, -51.52623751574038),
    "C10": (26, 0, 21, -54.48036448211290),
    "C11": (10, 0,  8, -24.37019200000000),
}

print("\n=== DIRECT GUARD AUDITS ON S7 JAN-AUG PATH ===")
for guard, e in EXPECTED_DIRECT.items():
    g = direct[direct.guard == guard]
    rc = Counter(g.result.astype(str))
    net = float(g.net_pct.sum()) if len(g) else 0.0
    ok = (
        len(g) == e[0]
        and rc["TP20_FULL"] == e[1]
        and rc["STOP"] == e[2]
        and abs(net - e[3]) <= 0.03
    )
    print(
        guard,
        "N", len(g),
        "TP", rc["TP20_FULL"],
        "STOP", rc["STOP"],
        "NET", round(net, 6),
        "AUDIT", ok,
        flush=True,
    )
    if not ok:
        raise SystemExit(f"{guard} DIRECT AUDIT FAIL")

# Key month audits.
def audit_month(guard, month, n, tp, stop, net, tol=0.03):
    g = direct[
        (direct.guard == guard)
        & (direct.month == month)
    ]
    rc = Counter(g.result.astype(str))
    val = float(g.net_pct.sum()) if len(g) else 0.0
    ok = (
        len(g) == n
        and rc["TP20_FULL"] == tp
        and rc["STOP"] == stop
        and abs(val - net) <= tol
    )
    print(
        guard, month,
        "N", len(g), "TP", rc["TP20_FULL"],
        "STOP", rc["STOP"], "NET", round(val, 6),
        "AUDIT", ok,
        flush=True,
    )
    if not ok:
        raise SystemExit(f"{guard} {month} DIRECT AUDIT FAIL")

audit_month("C8",  "2026-07", 17, 0, 13, -32.125285)
audit_month("C9",  "2026-07",  6, 0,  5, -12.406184)
audit_month("C10", "2026-06", 14, 0, 11, -30.235735482112897)
audit_month("C11", "2026-06",  5, 0,  5, -11.455699)

print("C8~C11 DIRECT AUDITS = PASS", flush=True)

# ------------------------------------------------------------------
# Exact replacement scenarios.
# ------------------------------------------------------------------
scenarios = {}
summaries = {}
blocks = {}

for level, label in [
    (7,  "S7_C6_C7"),
    (8,  "S8_PLUS_C8"),
    (9,  "S9_PLUS_C9"),
    (10, "S10_PLUS_C10"),
    (11, "S11_PLUS_C11"),
]:
    if level == 7:
        tx, bx = t7, b7
    else:
        print(f"\n=== RUN {label} WITH REAL REPLACEMENT ===", flush=True)
        tx, bx = run_both(level)

    scenarios[level] = tx
    blocks[level] = bx
    summaries[level] = summarize(tx, label)

# Monthly comparison.
cmp = summaries[7][["month","N","TP","STOP","NET"]].rename(
    columns={
        "N":"N_C7","TP":"TP_C7","STOP":"STOP_C7","NET":"NET_C7"
    }
)

for level in [8,9,10,11]:
    q = summaries[level][["month","N","TP","STOP","NET"]].rename(
        columns={
            "N":f"N_C{level}",
            "TP":f"TP_C{level}",
            "STOP":f"STOP_C{level}",
            "NET":f"NET_C{level}",
        }
    )
    cmp = cmp.merge(q, on="month")

for level in [8,9,10,11]:
    prev = level - 1
    cmp[f"DELTA_C{level}_vs_C{prev}"] = (
        cmp[f"NET_C{level}"] - cmp[f"NET_C{prev}"]
    )

print("\n" + "="*140)
print("MONTHLY EXACT — C7 -> C8 -> C9 -> C10 -> C11 (REAL REPLACEMENT)")
print("="*140)
for r in cmp.itertuples(index=False):
    print(
        r.month,
        f"| C7 {r.NET_C7:+.3f}",
        f"| C8 {r.NET_C8:+.3f} D8 {r.DELTA_C8_vs_C7:+.3f}",
        f"| C9 {r.NET_C9:+.3f} D9 {r.DELTA_C9_vs_C8:+.3f}",
        f"| C10 {r.NET_C10:+.3f} D10 {r.DELTA_C10_vs_C9:+.3f}",
        f"| C11 {r.NET_C11:+.3f} D11 {r.DELTA_C11_vs_C10:+.3f}",
        flush=True,
    )

tot = {
    level: float(scenarios[level].net_pct.sum())
    for level in [7,8,9,10,11]
}

print("\nTOTALS")
print("C7 :", round(tot[7],3))
print("C8 :", round(tot[8],3), "DELTA", round(tot[8]-tot[7],3))
print("C9 :", round(tot[9],3), "DELTA", round(tot[9]-tot[8],3))
print("C10:", round(tot[10],3), "DELTA", round(tot[10]-tot[9],3))
print("C11:", round(tot[11],3), "DELTA", round(tot[11]-tot[10],3))

# Path changes between each successive level.
def path_changes(a, b, label):
    ka = set(a.setup_id.astype(str))
    kb = set(b.setup_id.astype(str))

    rem = a[a.setup_id.astype(str).isin(ka-kb)].copy()
    add = b[b.setup_id.astype(str).isin(kb-ka)].copy()

    rem["change"] = "REMOVED"
    add["change"] = "ADDED"

    z = pd.concat([rem, add], ignore_index=True)
    z["compare"] = label
    return z

path_frames = []
for a,b,label in [
    (7,8,"C8_vs_C7"),
    (8,9,"C9_vs_C8"),
    (9,10,"C10_vs_C9"),
    (10,11,"C11_vs_C10"),
]:
    path_frames.append(
        path_changes(
            scenarios[a], scenarios[b], label
        )
    )

paths = pd.concat(path_frames, ignore_index=True)

# ------------------------------------------------------------------
# Save compact exact result pack.
# ------------------------------------------------------------------
stamp = "20261004"
outdir = R / f"C8_C11_EXACT_REPLACEMENT_{stamp}"
outdir.mkdir(exist_ok=True)

cmp.to_csv(outdir/"MONTHLY_COMPARE.csv", index=False)
direct.to_csv(outdir/"DIRECT_GUARD_AUDIT_JANAUG.csv", index=False)
paths.to_csv(outdir/"PATH_CHANGES.csv", index=False)

for level in [7,8,9,10,11]:
    scenarios[level].to_csv(
        outdir/f"TRADES_C{level}.csv",
        index=False,
    )
    blocks[level].to_csv(
        outdir/f"BLOCKS_C{level}.csv",
        index=False,
    )

summary = [
    "C8~C11 EXACT REPLACEMENT 2026-10-04",
    "C6+C7 baseline audit: PASS",
    "Direct audits: C8/C9/C10/C11 PASS",
    f"TOTAL_C7={tot[7]:.6f}",
    f"TOTAL_C8={tot[8]:.6f} DELTA={tot[8]-tot[7]:+.6f}",
    f"TOTAL_C9={tot[9]:.6f} DELTA={tot[9]-tot[8]:+.6f}",
    f"TOTAL_C10={tot[10]:.6f} DELTA={tot[10]-tot[9]:+.6f}",
    f"TOTAL_C11={tot[11]:.6f} DELTA={tot[11]-tot[10]:+.6f}",
]

for r in cmp.itertuples(index=False):
    summary.append(
        f"{r.month} "
        f"C7={r.NET_C7:.6f} "
        f"C8={r.NET_C8:.6f} "
        f"C9={r.NET_C9:.6f} "
        f"C10={r.NET_C10:.6f} "
        f"C11={r.NET_C11:.6f}"
    )

(outdir/"SUMMARY.txt").write_text(
    "\n".join(summary) + "\n",
    encoding="utf-8",
)

zip_path = R / f"C8_C11_EXACT_REPLACEMENT_{stamp}_RESULTS.zip"
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
    for p in sorted(outdir.iterdir()):
        if p.is_file():
            z.write(p, p.name)

print("\nRESULT ZIP:", zip_path, flush=True)
print("=== DONE ===", flush=True)
