#!/usr/bin/env python3
import importlib.util,sys,csv,io,zipfile
from pathlib import Path
from datetime import datetime,timedelta,timezone
R=Path("/root/hyejin-trader/bybit_swing");S=R/"SCAN_FULL_20260928_0000_2359_KST.csv";F=R/"forward_full_0924_0925.py";Z=R/"MKT_REGIME_EXACT_RESULTS.zip";K=timezone(timedelta(hours=9))
sp=importlib.util.spec_from_file_location("L",str(F));M=importlib.util.module_from_spec(sp);sys.modules["L"]=M;sp.loader.exec_module(M)
M.SCAN=S;M.EVAL_START_KST=datetime(2026,9,28,tzinfo=K);M.WARM_START_KST=datetime(2026,9,27,tzinfo=K)
df,a,b,_=M.load_scan_window();M.configure_unified(datetime(2026,9,29,tzinfo=K));M.U.load_market_series()
ss=M.U.load_setups();cache={};print("[SETUPS]",len(ss),flush=True)
gate={}
with zipfile.ZipFile(Z) as z:
 n=[n for n in z.namelist() if "PERF_B_N9_P9_N6" in n and n.endswith("_TRADES.csv")][0]
 with z.open(n) as q:
  for r in csv.DictReader(io.TextIOWrapper(q,encoding="utf-8-sig")):
   if str(r.get("entry_time_kst","")).startswith("2026-09-28"):gate[r["setup_id"]]=r
rows=[]
for st in ss:
 if st["setup_id"] not in gate or int(float(gate[st["setup_id"]].get("accepted") or 0))!=1:continue
 x=M.get_sim(st,cache);rows.append((st,x))
print("[FINAL] entries",len(rows),"base_net",sum(x.net_pct for st,x in rows))
print("NOTE: this is exact entry-integrated BASE exit; DCA45 overlay requires prior final-candidate table.")
