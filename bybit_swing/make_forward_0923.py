from pathlib import Path
import csv, gzip, zipfile, io, json

ROOT = Path("/root/hyejin-trader/bybit_swing")
START = "2026-09-23 22:10:44"
OUT = ROOT / "scan_FORWARD_20260923_221044_to_20260924_NOW_KST.csv"
ZIP = ROOT / "scan_FORWARD_20260923_221044_to_20260924_NOW_KST.zip"

sources = []
arc = ROOT / "scan_archive"
if arc.exists():
    for p in arc.rglob("*"):
        if p.is_file() and (p.suffix.lower() == ".csv" or p.name.lower().endswith(".csv.gz") or p.suffix.lower() == ".zip"):
            sources.append(p)
for p in ROOT.glob("scan*"):
    if p.is_file() and not p.name.startswith("scan_FORWARD_20260923_221044"):
        if p.name == "scan_rejected.csv" or "20260923" in p.name or "20260924" in p.name:
            if p.suffix.lower() in (".csv", ".zip") or p.name.lower().endswith(".csv.gz"):
                sources.append(p)
sources = list(dict.fromkeys(sources))

fields, seen_fields, rows, seen = [], set(), [], set()

def add_text(text):
    rd = csv.DictReader(io.StringIO(text))
    if not rd.fieldnames:
        return
    for f in rd.fieldnames:
        if f not in seen_fields:
            seen_fields.add(f); fields.append(f)
    tk = next((k for k in ("time_kst","timestamp_kst","datetime_kst","time","timestamp") if k in rd.fieldnames), rd.fieldnames[0])
    for r in rd:
        ts = str(r.get(tk, "")).strip().strip('"')
        if not ts or ts < START:
            continue
        key = json.dumps(r, sort_keys=True, ensure_ascii=False, separators=(",",":"))
        if key in seen:
            continue
        seen.add(key)
        r["__TS__"] = ts
        rows.append(r)

for i,p in enumerate(sources,1):
    try:
        if p.name.lower().endswith(".csv.gz"):
            with gzip.open(p, "rt", encoding="utf-8-sig", errors="replace") as f:
                add_text(f.read())
        elif p.suffix.lower() == ".zip":
            with zipfile.ZipFile(p) as z:
                for n in z.namelist():
                    if n.lower().endswith(".csv"):
                        add_text(z.read(n).decode("utf-8-sig", errors="replace"))
        elif p.suffix.lower() == ".csv":
            add_text(p.read_text(encoding="utf-8-sig", errors="replace"))
        if i % 20 == 0:
            print(f"{i}/{len(sources)} files...")
    except Exception as e:
        print("WARN", p.name, e)

if not rows:
    raise SystemExit("NO DATA AFTER START")

rows.sort(key=lambda r: r["__TS__"])
first_ts, last_ts = rows[0]["__TS__"], rows[-1]["__TS__"]
for r in rows:
    r.pop("__TS__", None)

with OUT.open("w", newline="", encoding="utf-8-sig") as f:
    w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
    w.writeheader(); w.writerows(rows)

with zipfile.ZipFile(ZIP, "w", zipfile.ZIP_DEFLATED) as z:
    z.write(OUT, arcname=OUT.name)

print("=== DONE ===")
print("sources =", len(sources))
print("rows =", len(rows))
print("first =", first_ts)
print("last =", last_ts)
print("zip =", ZIP)
