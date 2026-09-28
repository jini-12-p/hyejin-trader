#!/usr/bin/env python3
# Clean stale Forward cache, rerun existing validators, bundle results.
from pathlib import Path
import shutil,subprocess,sys,zipfile,time
R=Path("/root/hyejin-trader/bybit_swing")
old=R/"pure_forward_v22q_kline_cache"
bak=R/f"pure_forward_v22q_kline_cache_STALE_BACKUP_{int(time.time())}"
if old.exists():
    print("BACKUP",old,"->",bak,flush=True);old.rename(bak)
old.mkdir(parents=True,exist_ok=True)
print("NEW EMPTY CACHE",old,flush=True)

# Existing scripts will repopulate the exact cache path they are configured to use.
for s in ("market_regime_exact_replay.py","exit_integrity_audit_0901_0928.py","final_0901_0928_integrated.py"):
    p=R/s
    if not p.exists():
        print("MISSING",s,flush=True);continue
    print("\\n=== RUN",s,"===",flush=True)
    rc=subprocess.call([sys.executable,str(p)],cwd=R)
    if rc: raise SystemExit(f"{s} failed rc={rc}")

out=R/"CLEAN_FORWARD_FINAL_0901_0928_RESULTS.zip"
names=[
"MKT_REGIME_EXACT_SUMMARY.csv","MKT_REGIME_EXACT_DAILY.csv","MKT_REGIME_TRANSITIONS.csv",
"EXIT_INTEGRITY_AUDIT_0901_0928_SUMMARY.csv","EXIT_INTEGRITY_AUDIT_0901_0928_MISMATCH.csv",
"EXIT_INTEGRITY_AUDIT_0901_0928_DETAIL.csv","EXIT_INTEGRITY_AUDIT_0901_0928_SUMMARY.txt",
"FINAL_0901_0928_DAILY.csv","FINAL_0901_0928_STOP_DETAIL.csv","FINAL_0901_0928_SUMMARY.txt"]
with zipfile.ZipFile(out,"w",zipfile.ZIP_DEFLATED) as z:
    for n in names:
        p=R/n
        if p.exists():z.write(p,arcname=n)
print("\\nDONE",out)
